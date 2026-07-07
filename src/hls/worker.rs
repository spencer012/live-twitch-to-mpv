//! Playlist reload loop: the port of HLSStreamWorker / TwitchHLSStreamWorker.
//!
//! Reloads the media playlist on a segment-duration cadence (low-latency) or
//! targetduration cadence, queues new segments in order for the fetcher, and
//! implements the fork's persist/recovery behavior. Adblock is only touched
//! through the `SegmentHook::on_playlist` call.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::{Mutex, mpsc};

use crate::hls::hook::SegmentHook;
use crate::hls::m3u8::{MediaSegment, parse_media_playlist};

pub const LOW_LATENCY_MAX_LIVE_EDGE: usize = 2;
const RELOAD_TIME_DEFAULT: f64 = 6.0;
const RECOVERY_RETRY_INTERVAL: f64 = 10.0;
const QUEUE_DEADLINE_FACTOR: f64 = 3.0;
const QUEUE_DEADLINE_MIN: f64 = 5.0;

pub struct WorkerConfig {
    pub low_latency: bool,
    pub live_edge: usize,
    pub live_restart: bool,
    pub persist_stream: bool,
    /// Seconds; 0 = wait forever.
    pub recovery_timeout: f64,
    pub reload_attempts: u32,
}

/// Signals from the fetcher back to the worker (segment fetch failures
/// trigger recovery when persist_stream is enabled).
#[derive(Clone, Default)]
pub struct RecoveryRequest {
    flag: Arc<AtomicBool>,
}

impl RecoveryRequest {
    pub fn request(&self, reason: &str) {
        if !self.flag.swap(true, Ordering::SeqCst) {
            tracing::warn!("[persist] {reason}, entering recovery mode...");
        }
    }

    fn take(&self) -> bool {
        self.flag.swap(false, Ordering::SeqCst)
    }
}

pub struct PlaylistWorker {
    client: reqwest::Client,
    url: String,
    config: WorkerConfig,
    hook: Arc<Mutex<dyn SegmentHook>>,
    tx: mpsc::Sender<MediaSegment>,
    recovery: RecoveryRequest,

    sequence: i64,
    targetduration: f64,
    reload_time: f64,
    last_segment_nums: Vec<i64>,
    playlist_end: Option<i64>,
    queue_last: Instant,
    /// Mark the next queued segment with a discontinuity after recovery.
    recovery_discontinuity: bool,
    low_latency_checked: bool,
}

impl PlaylistWorker {
    pub fn new(
        client: reqwest::Client,
        url: String,
        config: WorkerConfig,
        hook: Arc<Mutex<dyn SegmentHook>>,
        tx: mpsc::Sender<MediaSegment>,
        recovery: RecoveryRequest,
    ) -> Self {
        Self {
            client,
            url,
            config,
            hook,
            tx,
            recovery,
            sequence: -1,
            targetduration: 0.0,
            reload_time: RELOAD_TIME_DEFAULT,
            last_segment_nums: Vec::new(),
            playlist_end: None,
            queue_last: Instant::now(),
            recovery_discontinuity: false,
            low_latency_checked: false,
        }
    }

    /// Effective live edge: low latency clamps to 1..=2.
    fn live_edge(&self) -> usize {
        if self.config.low_latency {
            self.config.live_edge.clamp(1, LOW_LATENCY_MAX_LIVE_EDGE)
        } else {
            self.config.live_edge.max(1)
        }
    }

    async fn fetch_playlist(&self) -> Result<String> {
        let mut last_err = None;
        for attempt in 0..self.config.reload_attempts.max(1) {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            match self.try_fetch_playlist().await {
                Ok(text) => return Ok(text),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.expect("at least one attempt was made"))
    }

    async fn try_fetch_playlist(&self) -> Result<String> {
        let resp = self
            .client
            .get(&self.url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .context("playlist request failed")?;
        if !resp.status().is_success() {
            anyhow::bail!("playlist request failed with status {}", resp.status());
        }
        resp.text().await.context("failed to read playlist body")
    }

    /// Fetch + parse + process one playlist reload.
    /// Returns the segments that are new since the last reload.
    async fn reload(&mut self) -> Result<Vec<MediaSegment>> {
        tracing::debug!("Reloading playlist");
        let text = self.fetch_playlist().await?;
        let mut playlist = parse_media_playlist(&text, Some(&self.url));

        if playlist.segments.is_empty() && playlist.media_sequence == 0 && !playlist.is_endlist {
            anyhow::bail!("received an invalid or empty media playlist");
        }

        self.targetduration = playlist.targetduration.unwrap_or(0.0);

        // Ignore prefetch segments when not streaming in low-latency mode.
        if !self.config.low_latency {
            playlist.segments.retain(|s| !s.prefetch);
        }

        // Adblock hook: drives the ad-block state machine (no-op by default).
        self.hook.lock().await.on_playlist(&playlist).await;

        // Zero ad segment durations so they don't skew reload timing.
        for segment in &mut playlist.segments {
            if segment.ad {
                segment.duration = 0.0;
            }
        }

        if self.config.low_latency && !self.low_latency_checked {
            let has_content = playlist.segments.iter().any(|s| !s.ad);
            if has_content {
                self.low_latency_checked = true;
                if !playlist.segments.iter().any(|s| s.prefetch) {
                    tracing::info!("This is not a low latency stream");
                }
            }
        }

        // Reload time: last segment duration in LL mode, else targetduration,
        // else keep the current value; halve (floor 1s) when unchanged.
        let new_reload_time = if self.config.low_latency {
            playlist.segments.last().map(|s| s.duration).filter(|d| *d > 0.0)
        } else {
            None
        }
        .or(playlist.targetduration.filter(|d| *d > 0.0))
        .unwrap_or(self.reload_time.max(RELOAD_TIME_DEFAULT));
        self.reload_time = new_reload_time;

        let nums: Vec<i64> = playlist.segments.iter().map(|s| s.num).collect();
        let changed = nums != self.last_segment_nums;
        self.last_segment_nums = nums;
        if !changed {
            self.reload_time = (self.reload_time / 2.0).max(1.0);
        }

        if playlist.is_endlist {
            self.playlist_end = playlist.segments.last().map(|s| s.num);
        }

        // Initial live-edge positioning.
        if self.sequence < 0 && !playlist.segments.is_empty() {
            if self.playlist_end.is_none() && !self.config.live_restart {
                let edge = self.live_edge().min(playlist.segments.len());
                self.sequence = playlist.segments[playlist.segments.len() - edge].num;
            } else {
                self.sequence = playlist.segments[0].num;
            }
            tracing::debug!(
                "First Sequence: {}; Last Sequence: {}; Start Sequence: {}",
                playlist.segments[0].num,
                playlist.segments.last().map(|s| s.num).unwrap_or(-1),
                self.sequence,
            );
        }

        Ok(playlist
            .segments
            .into_iter()
            .filter(|s| s.num >= self.sequence)
            .collect())
    }

    fn queue_deadline(&self) -> f64 {
        (self.targetduration * QUEUE_DEADLINE_FACTOR).max(QUEUE_DEADLINE_MIN)
    }

    /// Poll the playlist until segments at/after the current sequence appear,
    /// every 10s, up to recovery_timeout (0 = forever).
    async fn recover_stream(&mut self) -> bool {
        tracing::info!("[persist] Stream appears offline, waiting for recovery...");
        let started = Instant::now();
        let mut attempt = 0u64;

        loop {
            if self.config.recovery_timeout > 0.0 {
                let remaining = self.config.recovery_timeout - started.elapsed().as_secs_f64();
                if remaining <= 0.0 {
                    break;
                }
                tokio::time::sleep(Duration::from_secs_f64(
                    RECOVERY_RETRY_INTERVAL.min(remaining),
                ))
                .await;
            } else {
                tokio::time::sleep(Duration::from_secs_f64(RECOVERY_RETRY_INTERVAL)).await;
            }

            if self.tx.is_closed() {
                return false;
            }

            attempt += 1;
            tracing::debug!("[persist] Checking for stream recovery (attempt {attempt})");
            match self.reload().await {
                Ok(segments) => {
                    if !segments.is_empty() {
                        tracing::info!(
                            "[persist] Stream recovered! Found {} new segments",
                            segments.len()
                        );
                        self.recovery_discontinuity = true;
                        return true;
                    }
                    tracing::debug!("[persist] No new segments yet, continuing to wait...");
                }
                Err(err) => {
                    tracing::debug!("[persist] Playlist reload failed: {err:#}");
                }
            }
        }

        tracing::warn!("[persist] Stream recovery timed out");
        false
    }

    /// Send new segments to the fetcher; returns false if the receiver is gone
    /// or updates `queued` when at least one segment was sent.
    async fn queue_segments(&mut self, segments: Vec<MediaSegment>) -> Option<bool> {
        let mut queued = false;
        for mut segment in segments {
            if self.recovery.take() {
                // A fetch failure was reported: stop queueing and recover.
                self.recovery.request("segment fetch failure");
                return Some(queued);
            }
            if segment.num < self.sequence {
                continue;
            }
            if self.recovery_discontinuity {
                segment.discontinuity = true;
                self.recovery_discontinuity = false;
                tracing::info!("[persist] Marked first segment with discontinuity after recovery");
            }

            let num = segment.num;
            if num > self.sequence && self.sequence >= 0 {
                tracing::warn!(
                    "Sequence gap of {} segment(s) at position {}",
                    num - self.sequence,
                    self.sequence
                );
            }
            tracing::debug!(
                "Queuing segment {num} (duration={:.3}, title={:?}, ad={}, prefetch={})",
                segment.duration,
                segment.title,
                segment.ad,
                segment.prefetch
            );
            if self.tx.send(segment).await.is_err() {
                return None;
            }
            self.sequence = num + 1;
            queued = true;
        }
        Some(queued)
    }

    pub async fn run(mut self) {
        let mut reload_last = Instant::now();

        // Initial load.
        let segments = match self.reload().await {
            Ok(segments) => segments,
            Err(err) => {
                tracing::error!("Failed to load playlist: {err:#}");
                return;
            }
        };
        self.queue_last = Instant::now();
        let Some(mut queued) = self.queue_segments(segments).await else {
            return;
        };
        if queued {
            self.queue_last = Instant::now();
        }

        loop {
            // Recovery requested by the fetcher (segment failures)?
            let fetch_recovery = self.recovery.take();
            if fetch_recovery && self.config.persist_stream {
                if self.recover_stream().await {
                    self.queue_last = Instant::now();
                    tracing::info!("Stream recovered, resuming...");
                } else {
                    return;
                }
            }

            // End of stream (VOD-style endlist).
            if let Some(end) = self.playlist_end
                && self.sequence > end
            {
                tracing::debug!("Reached playlist end");
                return;
            }

            // Implicit end of stream: no new segments within the deadline.
            if !queued && self.queue_last.elapsed().as_secs_f64() > self.queue_deadline() {
                tracing::warn!(
                    "No new segments for more than {:.2}s",
                    self.queue_deadline()
                );
                if self.config.persist_stream {
                    if self.recover_stream().await {
                        self.queue_last = Instant::now();
                        tracing::info!("Stream recovered, resuming...");
                    } else {
                        return;
                    }
                } else {
                    tracing::warn!("Stopping stream...");
                    return;
                }
            }

            // Strict reload interval, excluding fetch and processing time.
            let elapsed = reload_last.elapsed().as_secs_f64();
            let wait = (self.reload_time - elapsed).max(0.0);
            if wait > 0.0 {
                tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            }
            reload_last = Instant::now();

            if self.tx.is_closed() {
                return;
            }

            match self.reload().await {
                Ok(segments) => match self.queue_segments(segments).await {
                    Some(q) => {
                        queued = q;
                        if q {
                            self.queue_last = Instant::now();
                        }
                        if let Some(end) = self.playlist_end
                            && !q
                        {
                            let _ = end;
                            tracing::debug!("Reached playlist end without new segments");
                            return;
                        }
                    }
                    None => return,
                },
                Err(err) => {
                    tracing::warn!("Reloading failed: {err:#}");
                    queued = false;
                }
            }
        }
    }
}
