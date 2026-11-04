//! The narrow hook surface between the HLS pipeline and the adblock module.
//!
//! The pipeline only ever talks to a `SegmentHook`; the default implementation
//! simply filters out ad segments. The adblock module provides a richer
//! implementation, but its internals can never destabilize the normal
//! pipeline beyond what these calls allow.
//!
//! Besides the two driving calls, a hook can report what it did through
//! [`SegmentHook::drain_events`] and describe substitute segments via
//! [`Substitute`]. The pipeline only uses that information for recorder-mode
//! reporting; playback never depends on it.

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};

use crate::hls::m3u8::{MediaPlaylist, MediaSegment};

/// Strategy name reported for the default ad filter (`ad_block = false`).
pub const STRATEGY_FILTER: &str = "filter";

/// A segment fetched in place of a live playlist slot.
#[derive(Debug, Clone, PartialEq)]
pub struct Substitute {
    /// URL to fetch instead of the slot's own URL. Never reported.
    pub url: String,
    /// Strategy that supplied it, e.g. "delayed_playback" or "backup_player".
    pub strategy: &'static str,
    /// Player type of the token the substitute playlist was fetched with.
    pub player_type: Option<String>,
    /// Media sequence number of the substitute in its own playlist.
    pub sequence: Option<i64>,
    /// EXT-X-PROGRAM-DATE-TIME of the substitute segment, if known.
    pub date: Option<DateTime<FixedOffset>>,
    /// EXTINF duration of the substitute segment, if known.
    pub duration: Option<f64>,
}

impl Substitute {
    /// A substitute described only by its URL.
    #[cfg(test)]
    pub fn url_only(url: impl Into<String>, strategy: &'static str) -> Self {
        Self {
            url: url.into(),
            strategy,
            player_type: None,
            sequence: None,
            date: None,
            duration: None,
        }
    }
}

/// What the fetcher should do with a queued segment slot.
#[derive(Debug, Clone, PartialEq)]
pub enum SegmentAction {
    /// Fetch the segment normally.
    Fetch,
    /// Fetch this substitute instead of the segment's own URL.
    Substitute(Substitute),
    /// Silently drop the segment slot (no data written).
    Skip,
}

/// Something a hook did that is worth reporting (recorder events).
// Variant names mirror the recorder event types (ad_start, ad_end, ...).
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq)]
pub enum HookEvent {
    /// Ads appeared at the live edge.
    AdStart {
        strategy: &'static str,
        /// Ads before any content was seen (pre-roll).
        preroll: bool,
        /// Last media sequence number of the playlist that showed the ads.
        sequence: i64,
        /// Ad break duration advertised by Twitch, if known.
        break_duration: Option<f64>,
    },
    /// Ads disappeared from the live edge.
    AdEnd {
        strategy: &'static str,
        sequence: i64,
        /// Delayed playback is still substituting until it reaches live.
        catching_up: bool,
    },
    /// Delayed playback caught back up to the live edge.
    AdCatchupComplete {
        strategy: &'static str,
        /// Last substituted sequence number.
        sequence: i64,
        /// Substitute segments supplied during this ad episode.
        substituted: usize,
    },
}

// async-trait expands to boxed futures that carry `#[must_use]`; newer
// clippy flags that as a double must-use on the generated methods.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait SegmentHook: Send {
    /// Called by the worker after every playlist reload, before queueing.
    async fn on_playlist(&mut self, playlist: &MediaPlaylist);

    /// Called by the fetcher for each segment, in queue order.
    async fn segment_action(&mut self, segment: &MediaSegment) -> SegmentAction;

    /// Take the events recorded since the last call. The pipeline calls this
    /// after every `on_playlist`/`segment_action`, so buffers stay small.
    fn drain_events(&mut self) -> Vec<HookEvent> {
        Vec::new()
    }
}

/// Default hook: skip ad segments, pass everything else through.
#[derive(Default)]
pub struct AdFilterHook {
    showing_ad: bool,
    events: Vec<HookEvent>,
}

#[async_trait]
impl SegmentHook for AdFilterHook {
    async fn on_playlist(&mut self, playlist: &MediaPlaylist) {
        let has_ads = playlist.segments.iter().any(|s| s.ad);
        let sequence = playlist.segments.last().map(|s| s.num).unwrap_or(0);
        if has_ads != self.showing_ad {
            self.showing_ad = has_ads;
            self.events.push(if has_ads {
                HookEvent::AdStart {
                    strategy: STRATEGY_FILTER,
                    preroll: false,
                    sequence,
                    break_duration: ad_break_duration(playlist),
                }
            } else {
                HookEvent::AdEnd {
                    strategy: STRATEGY_FILTER,
                    sequence,
                    catching_up: false,
                }
            });
        }
    }

    async fn segment_action(&mut self, segment: &MediaSegment) -> SegmentAction {
        if segment.ad {
            SegmentAction::Skip
        } else {
            SegmentAction::Fetch
        }
    }

    fn drain_events(&mut self) -> Vec<HookEvent> {
        std::mem::take(&mut self.events)
    }
}

/// Longest ad break duration advertised by the playlist's ad dateranges.
pub fn ad_break_duration(playlist: &MediaPlaylist) -> Option<f64> {
    playlist
        .dateranges_ads
        .iter()
        .filter_map(|dr| {
            dr.x.get("X-TV-TWITCH-AD-POD-FILLED-DURATION")
                .and_then(|v| v.parse::<f64>().ok())
                .or(dr.duration)
                .or(dr.planned_duration)
        })
        .reduce(f64::max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hls::m3u8::parse_media_playlist;

    #[tokio::test]
    async fn filter_hook_reports_ad_start_and_end_once() {
        let ads = parse_media_playlist(
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:10\n\
             #EXT-X-DATERANGE:ID=\"stitched-ad-1\",CLASS=\"twitch-stitched-ad\",START-DATE=\"2026-01-01T00:00:00.000Z\",DURATION=30.000\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z\n#EXTINF:2.000,\nad.ts\n",
            None,
        );
        let live = parse_media_playlist(
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:11\n#EXTINF:2.000,live\nseg.ts\n",
            None,
        );
        let mut hook = AdFilterHook::default();
        hook.on_playlist(&live).await;
        assert!(hook.drain_events().is_empty());
        hook.on_playlist(&ads).await;
        hook.on_playlist(&ads).await;
        assert_eq!(
            hook.drain_events(),
            vec![HookEvent::AdStart {
                strategy: STRATEGY_FILTER,
                preroll: false,
                sequence: 10,
                break_duration: Some(30.0),
            }]
        );
        assert_eq!(
            hook.segment_action(&ads.segments[0]).await,
            SegmentAction::Skip
        );
        hook.on_playlist(&live).await;
        assert_eq!(
            hook.drain_events(),
            vec![HookEvent::AdEnd {
                strategy: STRATEGY_FILTER,
                sequence: 11,
                catching_up: false,
            }]
        );
    }
}
