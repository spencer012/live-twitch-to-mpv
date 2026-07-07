//! The narrow hook surface between the HLS pipeline and the adblock module.
//!
//! The pipeline only ever talks to a `SegmentHook`; the default implementation
//! simply filters out ad segments. The adblock module provides a richer
//! implementation, but its internals can never destabilize the normal
//! pipeline beyond what these two calls allow.

use async_trait::async_trait;

use crate::hls::m3u8::{MediaPlaylist, MediaSegment};

/// What the fetcher should do with a queued segment slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentAction {
    /// Fetch the segment normally.
    Fetch,
    /// Fetch this substitute URL instead of the segment's own URL.
    Substitute(String),
    /// Silently drop the segment slot (no data written).
    Skip,
}

#[async_trait]
pub trait SegmentHook: Send {
    /// Called by the worker after every playlist reload, before queueing.
    async fn on_playlist(&mut self, playlist: &MediaPlaylist);

    /// Called by the fetcher for each segment, in queue order.
    async fn segment_action(&mut self, segment: &MediaSegment) -> SegmentAction;
}

/// Default hook: skip ad segments, pass everything else through.
pub struct AdFilterHook;

#[async_trait]
impl SegmentHook for AdFilterHook {
    async fn on_playlist(&mut self, _playlist: &MediaPlaylist) {}

    async fn segment_action(&mut self, segment: &MediaSegment) -> SegmentAction {
        if segment.ad {
            SegmentAction::Skip
        } else {
            SegmentAction::Fetch
        }
    }
}
