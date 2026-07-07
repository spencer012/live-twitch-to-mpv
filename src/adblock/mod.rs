//! Ad-block system, fully segmented from the normal HLS pipeline.
//!
//! The pipeline only interacts with this module through the
//! [`SegmentHook`](crate::hls::hook::SegmentHook) trait. `TwitchAdBlock`
//! coordinates two strategies ported from the Python fork:
//!
//! - [`delayed`]: the primary mid-roll strategy. On ads at the live edge it
//!   fetches fresh tokens/playlists and plays slightly delayed ad-free
//!   segments, then catches back up to live.
//! - [`backup`]: the pre-roll fallback. It tries alternate player types
//!   (embed/popout/autoplay) which often serve ad-free streams.
//!
//! When neither can supply a segment, ad slots are silently dropped.

pub mod backup;
pub mod delayed;

use std::collections::{HashSet, VecDeque};

use async_trait::async_trait;

use crate::hls::hook::{SegmentAction, SegmentHook};
use crate::hls::m3u8::{MediaPlaylist, MediaSegment};
use crate::twitch::api::TwitchApi;
use crate::twitch::usher::UsherService;

use backup::BackupStreamManager;
use delayed::DelayedStreamManager;

pub struct TwitchAdBlock {
    delayed: DelayedStreamManager,
    backup: BackupStreamManager,

    is_showing_ad: bool,
    had_content: bool,
    /// True while the backup strategy handles pre-roll ads.
    preroll_fallback: bool,

    backup_segments: VecDeque<String>,
    consumed_backup_urls: HashSet<String>,

    logged_ads: VecDeque<String>,
}

impl TwitchAdBlock {
    pub fn new(
        client: reqwest::Client,
        api: TwitchApi,
        usher: UsherService,
        channel: &str,
        resolution: Option<(u32, u32)>,
    ) -> Self {
        tracing::info!("[AdBlock] Delayed playback ad-blocking enabled");
        Self {
            delayed: DelayedStreamManager::new(
                client.clone(),
                api.clone(),
                usher.clone(),
                channel,
                resolution,
            ),
            backup: BackupStreamManager::new(client, api, usher, channel, resolution),
            is_showing_ad: false,
            had_content: false,
            preroll_fallback: false,
            backup_segments: VecDeque::new(),
            consumed_backup_urls: HashSet::new(),
            logged_ads: VecDeque::new(),
        }
    }

    fn log_ad_break_durations(&mut self, playlist: &MediaPlaylist) {
        for daterange in &playlist.dateranges_ads {
            let Some(ads_id) = daterange
                .x
                .get("X-TV-TWITCH-AD-COMMERCIAL-ID")
                .or_else(|| daterange.x.get("X-TV-TWITCH-AD-ROLL-TYPE"))
                .cloned()
            else {
                continue;
            };
            if self.logged_ads.contains(&ads_id) {
                continue;
            }
            self.logged_ads.push_back(ads_id);
            while self.logged_ads.len() > 10 {
                self.logged_ads.pop_front();
            }

            // Prefer Twitch's own ads duration metadata if available.
            let duration = daterange
                .x
                .get("X-TV-TWITCH-AD-POD-FILLED-DURATION")
                .and_then(|v| v.parse::<f64>().ok())
                .or(daterange.duration)
                .or(daterange.planned_duration);
            if let Some(duration) = duration {
                tracing::info!(
                    "[AdBlock] Advertisement break duration: {} seconds",
                    duration.ceil() as u64
                );
            }
        }
    }

    async fn prepare_backup_segments(&mut self) {
        let Some((player_type, content)) = self.backup.get_ad_free_playlist().await else {
            tracing::debug!("[AdBlock] No ad-free backup available, will filter segments");
            return;
        };
        tracing::debug!("[AdBlock] Using backup playlist from {player_type}");

        // Extract segment URLs, skipping already-consumed ones so overlapping
        // reloads don't replay content.
        let mut segments = VecDeque::new();
        let mut skipped = 0usize;
        let lines: Vec<&str> = content.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if line.starts_with("#EXTINF")
                && let Some(uri) = lines.get(i + 1)
            {
                let uri = uri.trim();
                if uri.is_empty() || uri.starts_with('#') {
                    continue;
                }
                if self.consumed_backup_urls.contains(uri) {
                    skipped += 1;
                } else {
                    segments.push_back(uri.to_string());
                }
            }
        }

        if segments.is_empty() {
            tracing::debug!("[AdBlock] Backup playlist has no new segments (all consumed)");
            return;
        }
        tracing::debug!(
            "[AdBlock] Backup playlist has {} new segments ({skipped} already consumed)",
            segments.len()
        );
        self.backup_segments = segments;
    }

    fn next_backup_url(&mut self) -> Option<String> {
        let url = self.backup_segments.pop_front()?;
        self.consumed_backup_urls.insert(url.clone());
        Some(url)
    }
}

#[async_trait]
impl SegmentHook for TwitchAdBlock {
    async fn on_playlist(&mut self, playlist: &MediaPlaylist) {
        let ad_count = playlist.segments.iter().filter(|s| s.ad).count();
        let total = playlist.segments.len();
        if ad_count > 0 {
            tracing::debug!("[AdBlock] Detected {ad_count}/{total} ad segments in playlist");
        }
        let has_ads = ad_count > 0;
        let current_sequence = playlist.segments.last().map(|s| s.num).unwrap_or(0);

        self.log_ad_break_durations(playlist);

        if has_ads {
            if !self.is_showing_ad {
                self.is_showing_ad = true;
                tracing::info!("[AdBlock] Ad break started");

                // Pre-roll: ads before any real content has been seen. Delayed
                // playback can't help (fresh tokens also get pre-rolls), so
                // fall back to the backup player-type method.
                if !self.had_content {
                    self.preroll_fallback = true;
                    tracing::info!(
                        "[AdBlock] Pre-roll ads detected, falling back to backup player method"
                    );
                    self.backup.on_ad_start();
                } else {
                    self.delayed.on_ad_detected(current_sequence);
                }
            }

            if self.preroll_fallback {
                self.prepare_backup_segments().await;
            } else {
                self.delayed.update_live_sequence(current_sequence);
                self.delayed.try_get_delayed_segments().await;
            }
        } else {
            if self.is_showing_ad {
                self.is_showing_ad = false;
                tracing::info!("[AdBlock] Ad break ended");

                if self.preroll_fallback {
                    tracing::info!("[AdBlock] Pre-roll ads finished, switching to normal playback");
                    self.backup.on_ad_end();
                    self.backup_segments.clear();
                    self.consumed_backup_urls.clear();
                    self.preroll_fallback = false;
                } else {
                    self.delayed.on_ads_ended(current_sequence);
                }
            }

            // Ads have ended at the live edge, but the delayed manager may
            // still be catching up: keep its target at the live edge and keep
            // its queue topped up.
            if self.delayed.is_active() {
                self.delayed.update_catchup_target(current_sequence);
                self.delayed.update_live_sequence(current_sequence);
                self.delayed.try_get_delayed_segments().await;
            }
        }

        if !self.had_content {
            self.had_content = playlist.segments.iter().any(|s| !s.ad);
        }
    }

    async fn segment_action(&mut self, segment: &MediaSegment) -> SegmentAction {
        // Delayed playback mode: substitute every live-edge slot with delayed
        // content; if none is available, skip the slot entirely — never mix
        // live ad content into delayed playback.
        if !self.preroll_fallback && self.delayed.is_active() {
            if let Some((url, seq)) = self.delayed.next_segment().await {
                tracing::debug!("[AdBlock] Substituting delayed segment seq={seq}");
                return SegmentAction::Substitute(url);
            }
            tracing::debug!(
                "[AdBlock] No delayed segment available, skipping live segment (ad={})",
                segment.ad
            );
            return SegmentAction::Skip;
        }

        if !segment.ad {
            return SegmentAction::Fetch;
        }

        // Pre-roll fallback: replace ad segments with backup-stream segments.
        if let Some(url) = self.next_backup_url() {
            self.backup.count_stripped();
            tracing::debug!("[AdBlock] Substituting backup segment");
            return SegmentAction::Substitute(url);
        }

        tracing::debug!("[AdBlock] Backup segment unavailable, filtering ad segment");
        SegmentAction::Skip
    }
}
