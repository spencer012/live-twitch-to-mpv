//! Disk-backed HLS timeline shared by the HTTP server and session engine.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use tokio::sync::Mutex;

pub const DEFAULT_BUFFER_CAP_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentSource {
    Live,
    Vod,
}

#[derive(Debug, Clone)]
pub struct StagedSegment {
    id: u64,
    path: PathBuf,
    duration: f64,
    size: u64,
    discontinuity: bool,
    source: SegmentSource,
    unix_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct TimelineSegment {
    id: u64,
    path: PathBuf,
    duration: f64,
    size: u64,
    discontinuity: bool,
    source: SegmentSource,
    unix_ms: Option<i64>,
    offset_sec: f64,
}

#[derive(Default)]
struct State {
    segments: VecDeque<TimelineSegment>,
    pending_live: Option<Vec<StagedSegment>>,
    media_sequence: u64,
    committed_duration: f64,
    playback_cursor_sec: f64,
    total_bytes: u64,
    ended: bool,
}

struct Inner {
    root: PathBuf,
    cap_bytes: u64,
    next_id: AtomicU64,
    state: Mutex<State>,
}

#[derive(Clone)]
pub struct Timeline {
    inner: Arc<Inner>,
}

impl Timeline {
    pub async fn new(root: impl AsRef<Path>, cap_bytes: u64) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&root)
            .await
            .with_context(|| format!("failed to create buffer directory {}", root.display()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                root,
                cap_bytes: cap_bytes.max(1),
                next_id: AtomicU64::new(0),
                state: Mutex::new(State::default()),
            }),
        })
    }

    pub async fn stage(
        &self,
        bytes: &[u8],
        duration: f64,
        discontinuity: bool,
        source: SegmentSource,
        unix_ms: Option<i64>,
    ) -> Result<StagedSegment> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let path = self.inner.root.join(format!("{id}.ts"));
        tokio::fs::write(&path, bytes)
            .await
            .with_context(|| format!("failed to write buffered segment {}", path.display()))?;
        let staged = StagedSegment {
            id,
            path,
            duration: duration.max(0.001),
            size: bytes.len() as u64,
            discontinuity,
            source,
            unix_ms,
        };
        let mut state = self.inner.state.lock().await;
        state.total_bytes = state.total_bytes.saturating_add(staged.size);
        drop(state);
        self.evict().await;
        Ok(staged)
    }

    pub async fn append_live(
        &self,
        bytes: &[u8],
        duration: f64,
        discontinuity: bool,
        unix_ms: Option<i64>,
    ) -> Result<f64> {
        let staged = self
            .stage(bytes, duration, discontinuity, SegmentSource::Live, unix_ms)
            .await?;
        let mut state = self.inner.state.lock().await;
        if let Some(pending) = &mut state.pending_live {
            pending.push(staged);
            return Ok(state.committed_duration);
        }
        let offset = Self::commit(&mut state, staged);
        drop(state);
        self.evict().await;
        Ok(offset)
    }

    pub async fn begin_gap(&self) -> bool {
        let mut state = self.inner.state.lock().await;
        if state.pending_live.is_some() {
            false
        } else {
            state.pending_live = Some(Vec::new());
            true
        }
    }

    pub async fn finish_gap(&self, mut backfill: Vec<StagedSegment>) -> f64 {
        let mut state = self.inner.state.lock().await;
        let mut pending = state.pending_live.take().unwrap_or_default();
        if let Some(first) = backfill.first_mut() {
            first.discontinuity = true;
        }
        for staged in backfill {
            Self::commit(&mut state, staged);
        }
        if let Some(first) = pending.first_mut() {
            first.discontinuity = true;
        }
        for staged in pending {
            Self::commit(&mut state, staged);
        }
        let duration = state.committed_duration;
        drop(state);
        self.evict().await;
        duration
    }

    pub async fn report_position(&self, stream_offset_sec: f64) {
        let mut state = self.inner.state.lock().await;
        state.playback_cursor_sec = stream_offset_sec.max(0.0);
        drop(state);
        self.evict().await;
    }

    pub async fn note_segment_request(&self, id: u64) {
        let mut state = self.inner.state.lock().await;
        if let Some(segment) = state.segments.iter().find(|segment| segment.id == id) {
            state.playback_cursor_sec = state.playback_cursor_sec.max(segment.offset_sec);
        }
        drop(state);
        self.evict().await;
    }

    pub async fn segment_path(&self, id: u64) -> Option<PathBuf> {
        let state = self.inner.state.lock().await;
        state
            .segments
            .iter()
            .find(|segment| segment.id == id)
            .map(|segment| segment.path.clone())
    }

    pub async fn playlist(&self) -> String {
        let state = self.inner.state.lock().await;
        let target = state
            .segments
            .iter()
            .map(|segment| segment.duration)
            .fold(1.0_f64, f64::max)
            .ceil() as u64;
        let mut out = format!(
            "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:{target}\n\
             #EXT-X-MEDIA-SEQUENCE:{}\n#EXT-X-PLAYLIST-TYPE:EVENT\n",
            state.media_sequence
        );
        for segment in &state.segments {
            if segment.discontinuity {
                out.push_str("#EXT-X-DISCONTINUITY\n");
            }
            if let Some(unix_ms) = segment.unix_ms
                && let Some(date) = chrono::DateTime::from_timestamp_millis(unix_ms)
            {
                out.push_str(&format!(
                    "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                    date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                ));
            }
            out.push_str(&format!(
                "#EXTINF:{:.3},{}\n/seg/{}.ts\n",
                segment.duration,
                match segment.source {
                    SegmentSource::Live => "live",
                    SegmentSource::Vod => "vod-backfill",
                },
                segment.id
            ));
        }
        if state.ended {
            out.push_str("#EXT-X-ENDLIST\n");
        }
        out
    }

    pub async fn end(&self) {
        self.inner.state.lock().await.ended = true;
    }

    pub async fn committed_duration(&self) -> f64 {
        self.inner.state.lock().await.committed_duration
    }

    fn commit(state: &mut State, staged: StagedSegment) -> f64 {
        let offset = state.committed_duration;
        state.committed_duration += staged.duration;
        state.segments.push_back(TimelineSegment {
            id: staged.id,
            path: staged.path,
            duration: staged.duration,
            size: staged.size,
            discontinuity: staged.discontinuity,
            source: staged.source,
            unix_ms: staged.unix_ms,
            offset_sec: offset,
        });
        offset
    }

    async fn evict(&self) {
        let mut removed = Vec::new();
        {
            let mut state = self.inner.state.lock().await;
            while state.total_bytes > self.inner.cap_bytes {
                let evictable = state.segments.front().is_some_and(|segment| {
                    segment.offset_sec + segment.duration <= state.playback_cursor_sec
                });
                if !evictable {
                    break;
                }
                if let Some(segment) = state.segments.pop_front() {
                    state.total_bytes = state.total_bytes.saturating_sub(segment.size);
                    state.media_sequence += 1;
                    removed.push(segment.path);
                }
            }
        }
        for path in removed {
            if let Err(err) = tokio::fs::remove_file(&path).await {
                tracing::debug!("failed to remove evicted segment {}: {err}", path.display());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("twitchpipe-{name}-{}", std::process::id()))
    }

    #[tokio::test]
    async fn evicts_only_watched_head_segments() {
        let dir = temp_dir("eviction");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let timeline = Timeline::new(&dir, 8).await.unwrap();
        timeline
            .append_live(b"1111", 1.0, false, None)
            .await
            .unwrap();
        timeline
            .append_live(b"2222", 1.0, false, None)
            .await
            .unwrap();
        timeline
            .append_live(b"3333", 1.0, false, None)
            .await
            .unwrap();
        assert!(timeline.segment_path(0).await.is_some());

        timeline.report_position(1.0).await;
        assert!(timeline.segment_path(0).await.is_none());
        assert!(timeline.segment_path(1).await.is_some());
        assert!(timeline.segment_path(2).await.is_some());
        assert!(
            timeline
                .playlist()
                .await
                .contains("#EXT-X-MEDIA-SEQUENCE:1")
        );
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn gap_backfill_precedes_live_capture() {
        let dir = temp_dir("backfill");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let timeline = Timeline::new(&dir, 1024).await.unwrap();
        timeline
            .append_live(b"before", 2.0, false, None)
            .await
            .unwrap();
        timeline.begin_gap().await;
        timeline
            .append_live(b"after", 2.0, false, None)
            .await
            .unwrap();
        let vod = timeline
            .stage(b"missing", 2.0, false, SegmentSource::Vod, None)
            .await
            .unwrap();
        timeline.finish_gap(vec![vod]).await;
        let playlist = timeline.playlist().await;
        let vod_pos = playlist.find("vod-backfill").unwrap();
        let live_after_pos = playlist.rfind(",live").unwrap();
        assert!(vod_pos < live_after_pos);
        assert_eq!(playlist.matches("#EXT-X-DISCONTINUITY").count(), 2);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
