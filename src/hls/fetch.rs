//! Segment fetcher: parallel downloads with strictly ordered write-out.
//!
//! The hook is consulted sequentially (in queue order) so segment
//! substitution is deterministic; downloads then run concurrently via
//! spawned tasks, while response bodies are drained in queue order.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};

use crate::buffer::ByteSender;
use crate::hls::hook::{SegmentAction, SegmentHook};
use crate::hls::m3u8::MediaSegment;
use crate::hls::worker::RecoveryRequest;

pub struct FetcherConfig {
    /// Parallel downloads.
    pub threads: usize,
    /// Attempts per segment.
    pub attempts: u32,
    /// Per-attempt timeout in seconds.
    pub timeout: f64,
    pub persist_stream: bool,
}

pub struct SegmentFetcher {
    client: reqwest::Client,
    config: FetcherConfig,
    hook: Arc<Mutex<dyn SegmentHook>>,
    recovery: RecoveryRequest,
}

enum FetchOutcome {
    Response {
        segment: MediaSegment,
        response: reqwest::Response,
        is_substitute: bool,
    },
    Skipped,
    Failed(MediaSegment, anyhow::Error),
}

impl SegmentFetcher {
    pub fn new(
        client: reqwest::Client,
        config: FetcherConfig,
        hook: Arc<Mutex<dyn SegmentHook>>,
        recovery: RecoveryRequest,
    ) -> Self {
        Self {
            client,
            config,
            hook,
            recovery,
        }
    }

    async fn fetch_url(&self, url: &str) -> Result<reqwest::Response> {
        let mut last_err = None;
        for attempt in 1..=self.config.attempts.max(1) {
            match self.try_fetch(url).await {
                Ok(response) => return Ok(response),
                Err(err) => {
                    tracing::debug!("Segment fetch attempt {attempt} failed: {err:#}");
                    last_err = Some(err);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
        Err(last_err.expect("at least one attempt was made"))
    }

    async fn try_fetch(&self, url: &str) -> Result<reqwest::Response> {
        let resp = self
            .client
            .get(url)
            .timeout(Duration::from_secs_f64(self.config.timeout))
            .send()
            .await
            .context("segment request failed")?;
        if !resp.status().is_success() {
            anyhow::bail!("segment request failed with status {}", resp.status());
        }
        Ok(resp)
    }

    pub async fn run(self, mut rx: mpsc::Receiver<MediaSegment>, out: ByteSender) {
        let threads = self.config.threads.max(1);
        // Pending write-out queue of 20 (streamlink's writer queue size);
        // the semaphore limits concurrent downloads to `threads`.
        let (fetch_tx, fetch_rx) = mpsc::channel::<tokio::task::JoinHandle<FetchOutcome>>(20);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(threads));
        let this = Arc::new(self);

        // Stage 1 (sequential): consult the hook in queue order and spawn the
        // download task.
        let producer = {
            let this = this.clone();
            let semaphore = semaphore.clone();
            tokio::spawn(async move {
                while let Some(segment) = rx.recv().await {
                    let action = this.hook.lock().await.segment_action(&segment).await;
                    let handle = {
                        let this = this.clone();
                        let semaphore = semaphore.clone();
                        tokio::spawn(async move {
                            let _permit = semaphore.acquire_owned().await;
                            this.fetch_segment(segment, action).await
                        })
                    };
                    if fetch_tx.send(handle).await.is_err() {
                        return;
                    }
                }
            })
        };

        // Stage 2 (ordered write-out): await downloads in submission order.
        let mut fetch_rx = fetch_rx;
        let mut wrote_discontinuity_warning = false;
        while let Some(handle) = fetch_rx.recv().await {
            let Ok(outcome) = handle.await else {
                continue;
            };
            match outcome {
                FetchOutcome::Response {
                    segment,
                    response,
                    is_substitute,
                } => {
                    match this
                        .stream_response(&out, &segment, response, is_substitute)
                        .await
                    {
                        Ok(StreamWrite::Complete) => {}
                        Ok(StreamWrite::OutputClosed) => {
                            tracing::debug!("Output closed, stopping fetcher");
                            break;
                        }
                        Err(err) => {
                            tracing::error!("Failed to download segment {}: {err:#}", segment.num);
                            if this.config.persist_stream {
                                this.recovery.request(&format!(
                                    "download failed for segment {}",
                                    segment.num
                                ));
                            }
                            if !wrote_discontinuity_warning {
                                tracing::warn!(
                                    "Skipped segment data will result in a stream discontinuity"
                                );
                                wrote_discontinuity_warning = true;
                            }
                        }
                    }
                }
                FetchOutcome::Skipped => {}
                FetchOutcome::Failed(segment, err) => {
                    tracing::error!("Failed to fetch segment {}: {err:#}", segment.num);
                    if this.config.persist_stream {
                        this.recovery
                            .request(&format!("fetch failed for segment {}", segment.num));
                    }
                    if !wrote_discontinuity_warning {
                        tracing::warn!(
                            "Skipped segment data will result in a stream discontinuity"
                        );
                        wrote_discontinuity_warning = true;
                    }
                }
            }
        }
        producer.abort();
        tracing::debug!("Fetcher finished");
    }

    async fn stream_response(
        &self,
        out: &ByteSender,
        segment: &MediaSegment,
        response: reqwest::Response,
        is_substitute: bool,
    ) -> Result<StreamWrite> {
        let mut bytes_written = 0usize;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("segment download failed")?;
            if chunk.is_empty() {
                continue;
            }
            bytes_written += chunk.len();
            if out.send(chunk).await.is_err() {
                return Ok(StreamWrite::OutputClosed);
            }
        }
        tracing::debug!(
            "Segment {} complete ({} bytes{})",
            segment.num,
            bytes_written,
            if is_substitute { ", substituted" } else { "" },
        );
        Ok(StreamWrite::Complete)
    }

    async fn fetch_segment(&self, segment: MediaSegment, action: SegmentAction) -> FetchOutcome {
        let (url, is_substitute) = match action {
            SegmentAction::Fetch => (segment.uri.clone(), false),
            SegmentAction::Substitute(url) => (url, true),
            SegmentAction::Skip => {
                tracing::debug!("Discarding segment {}", segment.num);
                return FetchOutcome::Skipped;
            }
        };
        match self.fetch_url(&url).await {
            Ok(response) => FetchOutcome::Response {
                segment,
                response,
                is_substitute,
            },
            Err(err) => FetchOutcome::Failed(segment, err),
        }
    }
}

enum StreamWrite {
    Complete,
    OutputClosed,
}
