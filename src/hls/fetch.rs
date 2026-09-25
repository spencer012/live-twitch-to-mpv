//! Segment fetcher: parallel downloads with strictly ordered write-out.
//!
//! The hook is consulted sequentially (in queue order) so segment
//! substitution is deterministic. Each segment then downloads in its own
//! task, emitting chunks into an unbounded per-segment channel. The write-out
//! stage drains those channels in queue order:
//!
//! - The segment at the front of the queue streams to the output as its
//!   chunks arrive (lowest latency at the live edge, where Twitch delivers
//!   prefetch segments in real time).
//! - Segments queued behind it buffer inside their channels while they
//!   download concurrently, so their bytes are complete-with-retries by the
//!   time their turn comes.
//!
//! Downloads retry with `Range` resume: on a mid-body failure the task
//! re-requests the same URL from the byte offset it already emitted, so the
//! output byte stream continues seamlessly even for chunks that were already
//! forwarded to the player.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use futures::StreamExt;
use reqwest::StatusCode;
use tokio::sync::{Mutex, mpsc};

use crate::buffer::ByteSender;
use crate::hls::hook::{SegmentAction, SegmentHook};
use crate::hls::m3u8::MediaSegment;
use crate::hls::worker::RecoveryRequest;

const RETRY_BACKOFF_INITIAL: Duration = Duration::from_millis(250);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(2);

pub struct FetcherConfig {
    /// Parallel downloads.
    pub threads: usize,
    /// Attempts per segment (Range-resumed after mid-body failures).
    pub attempts: u32,
    pub persist_stream: bool,
}

pub struct SegmentFetcher {
    client: reqwest::Client,
    config: FetcherConfig,
    hook: Arc<Mutex<dyn SegmentHook>>,
    recovery: RecoveryRequest,
}

/// A queued segment download: chunks arrive on `rx` while the task runs.
struct SegmentJob {
    segment: MediaSegment,
    is_substitute: bool,
    /// The init-segment URL this job emits ahead of its media bytes, if any.
    emitted_map: Option<String>,
    rx: mpsc::UnboundedReceiver<Result<Bytes>>,
    task: tokio::task::JoinHandle<()>,
}

/// Errors from a single download attempt.
enum AttemptError {
    /// Not worth retrying (expired/forbidden segment): fail immediately.
    Fatal(anyhow::Error),
    /// Transient (network error, 5xx, timeout): retry with Range resume.
    Retryable(anyhow::Error),
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

    pub async fn run(self, mut rx: mpsc::Receiver<MediaSegment>, out: ByteSender) {
        let threads = self.config.threads.max(1);
        // Pending write-out queue of 20 (streamlink's writer queue size);
        // the semaphore limits concurrent downloads to `threads`.
        let (job_tx, mut job_rx) = mpsc::channel::<SegmentJob>(20);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(threads));
        // fMP4 (CMAF) streams declare an EXT-X-MAP initialization segment
        // which must be written before any media segment; re-emit it whenever
        // it changes. Shared with the write-out stage so a failed job resets
        // it (its init bytes never reached the output).
        let written_map: Arc<std::sync::Mutex<Option<String>>> =
            Arc::new(std::sync::Mutex::new(None));
        let this = Arc::new(self);

        // Stage 1 (sequential): consult the hook in queue order and spawn the
        // download task with its own chunk channel.
        let producer = {
            let this = this.clone();
            let semaphore = semaphore.clone();
            let written_map = written_map.clone();
            tokio::spawn(async move {
                while let Some(segment) = rx.recv().await {
                    let action = this.hook.lock().await.segment_action(&segment).await;
                    let (url, is_substitute) = match action {
                        SegmentAction::Fetch => (segment.uri.clone(), false),
                        SegmentAction::Substitute(url) => (url, true),
                        SegmentAction::Skip => {
                            tracing::debug!("Discarding segment {}", segment.num);
                            continue;
                        }
                    };
                    let map_url = {
                        let mut written = written_map.lock().unwrap();
                        if !is_substitute && segment.map_uri != *written {
                            *written = segment.map_uri.clone();
                            segment.map_uri.clone()
                        } else {
                            None
                        }
                    };
                    let (chunk_tx, chunk_rx) = mpsc::unbounded_channel();
                    let task = {
                        let this = this.clone();
                        let semaphore = semaphore.clone();
                        let map_url = map_url.clone();
                        tokio::spawn(async move {
                            let _permit = semaphore.acquire_owned().await;
                            if let Some(map_url) = map_url {
                                tracing::debug!("Downloading initialization segment");
                                if !this.download(&map_url, &chunk_tx).await {
                                    return;
                                }
                            }
                            this.download(&url, &chunk_tx).await;
                        })
                    };
                    let job = SegmentJob {
                        segment,
                        is_substitute,
                        emitted_map: map_url,
                        rx: chunk_rx,
                        task,
                    };
                    if job_tx.send(job).await.is_err() {
                        return;
                    }
                }
            })
        };

        // Stage 2 (ordered write-out): drain each segment's chunks in
        // submission order. The front job streams live; the rest buffer.
        let mut wrote_discontinuity_warning = false;
        'jobs: while let Some(mut job) = job_rx.recv().await {
            let mut bytes_written = 0usize;
            let mut failure: Option<anyhow::Error> = None;

            while let Some(item) = job.rx.recv().await {
                match item {
                    Ok(chunk) => {
                        bytes_written += chunk.len();
                        if out.send(chunk).await.is_err() {
                            tracing::debug!("Output closed, stopping fetcher");
                            job.task.abort();
                            break 'jobs;
                        }
                    }
                    Err(err) => {
                        failure = Some(err);
                        break;
                    }
                }
            }

            if failure.is_none() {
                // The chunk channel closed without an error marker: either the
                // download completed or the task panicked.
                if let Err(join_err) = job.task.await
                    && join_err.is_panic()
                {
                    failure = Some(anyhow!("segment download task panicked: {join_err}"));
                }
            }

            match failure {
                None => {
                    if let Some(date) = job.segment.date {
                        let end_ms = date.timestamp_millis()
                            + (job.segment.duration * 1000.0).round() as i64;
                        if out.segment_end(end_ms).is_err() {
                            break 'jobs;
                        }
                    }
                    tracing::debug!(
                        "Segment {} complete ({} bytes{})",
                        job.segment.num,
                        bytes_written,
                        if job.is_substitute {
                            ", substituted"
                        } else {
                            ""
                        },
                    );
                }
                Some(err) => {
                    tracing::error!("Failed to download segment {}: {err:#}", job.segment.num);
                    // This job may have failed before its init bytes reached
                    // the output: force a re-emit on the next segment.
                    if job.emitted_map.is_some() {
                        let mut written = written_map.lock().unwrap();
                        if *written == job.emitted_map {
                            *written = None;
                        }
                    }
                    if this.config.persist_stream {
                        this.recovery
                            .request(&format!("download failed for segment {}", job.segment.num));
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

    /// Download a segment with retries. Chunks are emitted into `tx` as they
    /// arrive; after a mid-body failure, the next attempt resumes from the
    /// byte offset already emitted via a `Range` request so previously
    /// forwarded data stays valid. Returns false when the download failed.
    async fn download(&self, url: &str, tx: &mpsc::UnboundedSender<Result<Bytes>>) -> bool {
        let attempts = self.config.attempts.max(1);
        let mut offset: u64 = 0;
        let mut backoff = RETRY_BACKOFF_INITIAL;

        for attempt in 1..=attempts {
            match self.attempt(url, &mut offset, tx).await {
                Ok(()) => return true,
                Err(AttemptError::Fatal(err)) => {
                    let _ = tx.send(Err(err));
                    return false;
                }
                Err(AttemptError::Retryable(err)) => {
                    if attempt == attempts {
                        let _ = tx.send(Err(
                            err.context(format!("giving up after {attempts} attempts"))
                        ));
                        return false;
                    }
                    tracing::debug!(
                        "Segment fetch attempt {attempt} failed (resuming at byte {offset}): {err:#}"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
                    // The consumer is gone (output closed / fetcher stopped):
                    // don't keep retrying an abandoned segment.
                    if tx.is_closed() {
                        return false;
                    }
                }
            }
        }
        false
    }

    /// One download attempt, resuming from `*offset` when it is non-zero.
    /// `*offset` advances as chunks are emitted. Timeouts are enforced by the
    /// client's connect/read timeouts, not a total deadline, so slow but
    /// live transfers are never cut off.
    async fn attempt(
        &self,
        url: &str,
        offset: &mut u64,
        tx: &mpsc::UnboundedSender<Result<Bytes>>,
    ) -> Result<(), AttemptError> {
        let mut request = self.client.get(url);
        if *offset > 0 {
            request = request.header(reqwest::header::RANGE, format!("bytes={offset}-"));
        }
        let response = request
            .send()
            .await
            .context("segment request failed")
            .map_err(AttemptError::Retryable)?;

        let status = response.status();
        // Bytes to discard when the server ignored our Range request and
        // replied 200 with the full body.
        let mut skip: u64 = 0;
        if *offset > 0 {
            match status {
                StatusCode::PARTIAL_CONTENT => {}
                // The requested offset is at/past the end: everything we
                // already emitted was the complete segment.
                StatusCode::RANGE_NOT_SATISFIABLE => return Ok(()),
                s if s.is_success() => skip = *offset,
                s => return Err(classify_status(s)),
            }
        } else if !status.is_success() {
            return Err(classify_status(status));
        }

        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let mut chunk = chunk
                .context("segment download failed")
                .map_err(AttemptError::Retryable)?;
            if skip > 0 {
                let discard = skip.min(chunk.len() as u64) as usize;
                let _ = chunk.split_to(discard);
                skip -= discard as u64;
            }
            if chunk.is_empty() {
                continue;
            }
            *offset += chunk.len() as u64;
            if tx.send(Ok(chunk)).is_err() {
                // Consumer dropped the segment (fetcher shutting down).
                return Ok(());
            }
        }

        if skip > 0 {
            return Err(AttemptError::Retryable(anyhow!(
                "resumed response ended before reaching previously delivered offset"
            )));
        }
        Ok(())
    }
}

/// 4xx responses (other than 429) mean the segment is gone or inaccessible:
/// retrying only delays the stream further. Everything else is transient.
fn classify_status(status: StatusCode) -> AttemptError {
    let err = anyhow!("segment request failed with status {status}");
    if status.is_client_error() && status != StatusCode::TOO_MANY_REQUESTS {
        AttemptError::Fatal(err)
    } else {
        AttemptError::Retryable(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::byte_channel;
    use crate::hls::hook::AdFilterHook;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Minimal HTTP/1.1 server: serves fixed bodies by path. Enough for
    /// reqwest to talk to without pulling in a mock-server dependency.
    async fn spawn_server(routes: Vec<(&'static str, &'static [u8])>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let routes = routes.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let mut read = 0;
                    // Read until the end of the request headers.
                    loop {
                        let Ok(n) = socket.read(&mut buf[read..]).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        read += n;
                        if buf[..read].windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8_lossy(&buf[..read]);
                    let path = request.split_whitespace().nth(1).unwrap_or("/").to_string();
                    let response = match routes.iter().find(|(p, _)| *p == path) {
                        Some((_, body)) => {
                            let mut r = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            )
                            .into_bytes();
                            r.extend_from_slice(body);
                            r
                        }
                        None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_vec(),
                    };
                    let _ = socket.write_all(&response).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        format!("http://{addr}")
    }

    fn segment(base: &str, num: i64, path: &str, map: Option<String>) -> MediaSegment {
        MediaSegment {
            uri: format!("{base}{path}"),
            num,
            duration: 2.0,
            title: None,
            date: None,
            discontinuity: false,
            map_uri: map,
            ad: false,
            prefetch: false,
        }
    }

    /// Run the fetcher over `segments` and collect the full output stream.
    async fn run_fetcher(segments: Vec<MediaSegment>) -> Vec<u8> {
        // no_proxy: skip Windows system proxy auto-detection, which stalls
        // for ~10s when multiple clients are created concurrently.
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let fetcher = SegmentFetcher::new(
            client,
            FetcherConfig {
                threads: 2,
                attempts: 2,
                persist_stream: false,
            },
            Arc::new(Mutex::new(AdFilterHook)),
            RecoveryRequest::default(),
        );
        let (seg_tx, seg_rx) = mpsc::channel(20);
        let (byte_tx, mut byte_rx) = byte_channel(1024 * 1024);
        let run = tokio::spawn(fetcher.run(seg_rx, byte_tx));
        for s in segments {
            seg_tx.send(s).await.unwrap();
        }
        drop(seg_tx);
        let mut out = Vec::new();
        while let Some(chunk) = byte_rx.recv().await {
            if let crate::buffer::Chunk::Data(data) = chunk {
                out.extend_from_slice(&data);
            }
        }
        run.await.unwrap();
        out
    }

    /// fMP4/CMAF: the EXT-X-MAP initialization segment must be written to the
    /// output before the first media segment, or the player cannot demux the
    /// stream ("no tfhd was found").
    #[tokio::test]
    async fn init_segment_written_before_first_media_segment() {
        let base = spawn_server(vec![
            ("/init.mp4", b"INIT".as_slice()),
            ("/seg1.mp4", b"SEG1".as_slice()),
            ("/seg2.mp4", b"SEG2".as_slice()),
        ])
        .await;

        let map = Some(format!("{base}/init.mp4"));
        let out = run_fetcher(vec![
            segment(&base, 1, "/seg1.mp4", map.clone()),
            segment(&base, 2, "/seg2.mp4", map.clone()),
        ])
        .await;

        // Init exactly once, before any media bytes, and not repeated
        // between segments that share the same map.
        assert_eq!(out, b"INITSEG1SEG2");
    }

    /// A changed EXT-X-MAP (e.g. after a recovery onto a fresh URL) must be
    /// re-emitted before the segment that introduced it.
    #[tokio::test]
    async fn changed_map_is_reemitted() {
        let base = spawn_server(vec![
            ("/init_a.mp4", b"INIT_A.".as_slice()),
            ("/init_b.mp4", b"INIT_B.".as_slice()),
            ("/seg1.mp4", b"SEG1.".as_slice()),
            ("/seg2.mp4", b"SEG2.".as_slice()),
        ])
        .await;

        let out = run_fetcher(vec![
            segment(&base, 1, "/seg1.mp4", Some(format!("{base}/init_a.mp4"))),
            segment(&base, 2, "/seg2.mp4", Some(format!("{base}/init_b.mp4"))),
        ])
        .await;

        assert_eq!(out, b"INIT_A.SEG1.INIT_B.SEG2.");
    }

    /// MPEG-TS segments have no map; nothing extra may be emitted.
    #[tokio::test]
    async fn no_map_means_no_extra_bytes() {
        let base = spawn_server(vec![
            ("/seg1.ts", b"AAAA".as_slice()),
            ("/seg2.ts", b"BBBB".as_slice()),
        ])
        .await;

        let out = run_fetcher(vec![
            segment(&base, 1, "/seg1.ts", None),
            segment(&base, 2, "/seg2.ts", None),
        ])
        .await;

        assert_eq!(out, b"AAAABBBB");
    }

    /// A failed init download must not let the media segment through without
    /// its header (the player would reject the whole stream).
    #[tokio::test]
    async fn failed_init_skips_media_segment() {
        let base = spawn_server(vec![
            ("/seg1.mp4", b"SEG1".as_slice()),
            ("/init2.mp4", b"INIT2.".as_slice()),
            ("/seg2.mp4", b"SEG2.".as_slice()),
        ])
        .await;

        // Segment 1's init 404s (fatal): its media bytes must be dropped.
        // Segment 2 carries a working init and must still play.
        let out = run_fetcher(vec![
            segment(
                &base,
                1,
                "/seg1.mp4",
                Some(format!("{base}/missing_init.mp4")),
            ),
            segment(&base, 2, "/seg2.mp4", Some(format!("{base}/init2.mp4"))),
        ])
        .await;

        assert_eq!(out, b"INIT2.SEG2.");
    }
}
