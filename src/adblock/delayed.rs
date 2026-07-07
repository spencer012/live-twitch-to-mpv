//! DelayedStreamManager port: delayed playback to skip mid-roll ads.
//!
//! State machine:
//! ```text
//! NORMAL -> DELAYED_PLAYBACK -> CATCHING_UP -> NORMAL
//! ```
//! On ads at the live edge, a fresh popout/web token + usher playlist is
//! fetched; older segments behind the live edge are usually ad-free. When ads
//! end, delayed content keeps playing until it catches up to where ads ended.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use crate::hls::m3u8::{parse_media_playlist, parse_multivariant_playlist};
use crate::twitch::api::{TokenResult, TwitchApi};
use crate::twitch::usher::UsherService;

const PREROLL_SEQUENCE_THRESHOLD: i64 = 15;
const FETCH_INTERVAL_DELAYED: Duration = Duration::from_secs(8);
const FETCH_INTERVAL_CATCHUP: Duration = Duration::from_secs(4);
const MIN_REMAINING_BEFORE_FETCH: usize = 5;
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Normal,
    DelayedPlayback,
    CatchingUp,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            State::Normal => "NORMAL",
            State::DelayedPlayback => "DELAYED_PLAYBACK",
            State::CatchingUp => "CATCHING_UP",
        };
        f.write_str(name)
    }
}

pub struct DelayedStreamManager {
    client: reqwest::Client,
    api: TwitchApi,
    usher: UsherService,
    channel: String,
    resolution: Option<(u32, u32)>,

    state: State,
    state_entered_at: Instant,

    catchup_target_sequence: Option<i64>,
    min_delayed_sequence: Option<i64>,
    last_live_sequence: Option<i64>,

    /// Queued (url, sequence) pairs, deduplicated by sequence number.
    delayed_segments: Vec<(String, i64)>,
    delayed_index: usize,
    consumed_sequences: HashSet<i64>,
    known_sequences: HashSet<i64>,

    last_fetch: Option<Instant>,
    segments_recovered: usize,
}

impl DelayedStreamManager {
    pub fn new(
        client: reqwest::Client,
        api: TwitchApi,
        usher: UsherService,
        channel: &str,
        resolution: Option<(u32, u32)>,
    ) -> Self {
        tracing::debug!(
            "[AdBlock] DelayedStreamManager initialized for channel={channel}, resolution={resolution:?}"
        );
        Self {
            client,
            api,
            usher,
            channel: channel.to_string(),
            resolution,
            state: State::Normal,
            state_entered_at: Instant::now(),
            catchup_target_sequence: None,
            min_delayed_sequence: None,
            last_live_sequence: None,
            delayed_segments: Vec::new(),
            delayed_index: 0,
            consumed_sequences: HashSet::new(),
            known_sequences: HashSet::new(),
            last_fetch: None,
            segments_recovered: 0,
        }
    }

    pub fn is_active(&self) -> bool {
        self.state != State::Normal
    }

    fn transition_to(&mut self, new_state: State) {
        let duration = self.state_entered_at.elapsed().as_secs_f64();
        tracing::info!(
            "[AdBlock] State transition: {} -> {} (was in {} for {:.1}s)",
            self.state,
            new_state,
            self.state,
            duration
        );
        self.state = new_state;
        self.state_entered_at = Instant::now();
    }

    pub fn update_live_sequence(&mut self, sequence: i64) {
        self.last_live_sequence = Some(sequence);
    }

    /// Called when an ad segment is detected at the live edge.
    pub fn on_ad_detected(&mut self, current_sequence: i64) {
        if self.state != State::Normal {
            return;
        }
        tracing::info!(
            "[AdBlock] Ad detected at sequence {current_sequence}, switching to delayed playback"
        );

        // Avoid replaying content already watched live — but only for
        // mid-stream ads; pre-rolls (low sequence) have no watched content.
        if current_sequence > PREROLL_SEQUENCE_THRESHOLD {
            self.min_delayed_sequence = Some(current_sequence - 1);
        } else {
            self.min_delayed_sequence = None;
        }

        self.transition_to(State::DelayedPlayback);
        self.delayed_segments.clear();
        self.delayed_index = 0;
        self.segments_recovered = 0;
    }

    /// Called when ads end at the live edge.
    pub fn on_ads_ended(&mut self, current_sequence: i64) {
        match self.state {
            State::DelayedPlayback => {
                self.catchup_target_sequence = Some(current_sequence);
                tracing::info!(
                    "[AdBlock] Ads ended at sequence {current_sequence}, will catch up to live edge"
                );
                self.transition_to(State::CatchingUp);
            }
            State::CatchingUp => {
                if self.catchup_target_sequence.is_none_or(|t| current_sequence > t) {
                    self.catchup_target_sequence = Some(current_sequence);
                }
            }
            State::Normal => {}
        }
    }

    /// Keep the catch-up target at the current live edge.
    pub fn update_catchup_target(&mut self, current_live_sequence: i64) {
        if self.state == State::CatchingUp
            && self
                .catchup_target_sequence
                .is_none_or(|t| current_live_sequence > t)
        {
            self.catchup_target_sequence = Some(current_live_sequence);
        }
    }

    fn remaining(&self) -> usize {
        self.delayed_segments.len().saturating_sub(self.delayed_index)
    }

    /// Fetch more delayed segments if the rate limiter and queue level allow.
    /// Returns true if segments are available afterwards.
    pub async fn try_get_delayed_segments(&mut self) -> bool {
        if self.state == State::Normal {
            return false;
        }

        let (min_interval, min_remaining) = match self.state {
            State::CatchingUp => (FETCH_INTERVAL_CATCHUP, MIN_REMAINING_BEFORE_FETCH + 2),
            _ => (FETCH_INTERVAL_DELAYED, MIN_REMAINING_BEFORE_FETCH),
        };

        let elapsed_ok = self
            .last_fetch
            .is_none_or(|t| t.elapsed() >= min_interval);
        let remaining = self.remaining();
        if !elapsed_ok && remaining >= min_remaining {
            return remaining > 0;
        }

        tracing::debug!(
            "[AdBlock] Fetching delayed segments (state={}, {remaining} remaining)",
            self.state
        );
        let fetched_at = Instant::now();
        let Some(playlist_content) = self.fetch_fresh_playlist().await else {
            tracing::debug!("[AdBlock] Failed to fetch fresh playlist");
            return self.remaining() > 0;
        };
        self.last_fetch = Some(fetched_at);

        let playlist = parse_media_playlist(&playlist_content, None);
        let mut new_segments = Vec::new();
        let mut skipped_too_old = 0usize;
        for segment in &playlist.segments {
            if segment.ad || segment.prefetch {
                continue;
            }
            if self.min_delayed_sequence.is_some_and(|min| segment.num < min) {
                skipped_too_old += 1;
                continue;
            }
            if self.consumed_sequences.contains(&segment.num)
                || self.known_sequences.contains(&segment.num)
            {
                continue;
            }
            self.known_sequences.insert(segment.num);
            new_segments.push((segment.uri.clone(), segment.num));
        }

        if new_segments.is_empty() {
            if skipped_too_old > 0 {
                tracing::debug!(
                    "[AdBlock] Waiting for newer segments ({skipped_too_old} skipped, need seq >= {:?})",
                    self.min_delayed_sequence
                );
            }
            return self.remaining() > 0;
        }

        let first = new_segments.first().map(|(_, n)| *n).unwrap_or(0);
        let last = new_segments.last().map(|(_, n)| *n).unwrap_or(0);
        tracing::info!(
            "[AdBlock] Found {} new delayed segments (seq {first}-{last}), {skipped_too_old} skipped",
            new_segments.len()
        );
        self.delayed_segments.extend(new_segments);
        true
    }

    /// Pop the next delayed segment; refetches when the queue runs dry.
    pub async fn next_segment(&mut self) -> Option<(String, i64)> {
        if self.state == State::Normal {
            return None;
        }

        if self.delayed_index >= self.delayed_segments.len() {
            tracing::debug!("[AdBlock] Out of delayed segments, attempting to fetch more...");
            self.try_get_delayed_segments().await;
        }

        let (url, seq) = self.delayed_segments.get(self.delayed_index)?.clone();
        self.delayed_index += 1;
        self.consumed_sequences.insert(seq);
        self.segments_recovered += 1;

        if self.state == State::CatchingUp
            && let Some(target) = self.catchup_target_sequence
        {
            let behind = target - seq;
            tracing::info!(
                "[AdBlock] CATCHING_UP: seq={seq}, {behind} behind live edge ({target}), {} queued",
                self.remaining()
            );
            if seq >= target {
                tracing::info!(
                    "[AdBlock] Catchup complete! Recovered {} segments, resuming live playback",
                    self.segments_recovered
                );
                self.transition_to(State::Normal);
                self.reset_for_next_ad_break();
            }
        } else if self.state == State::DelayedPlayback {
            tracing::info!(
                "[AdBlock] Playing delayed seq={seq}, {} queued",
                self.remaining()
            );
        }

        Some((url, seq))
    }

    fn reset_for_next_ad_break(&mut self) {
        self.catchup_target_sequence = None;
        self.min_delayed_sequence = None;
        self.delayed_segments.clear();
        self.delayed_index = 0;
        self.consumed_sequences.clear();
        self.known_sequences.clear();
        self.last_fetch = None;
        self.segments_recovered = 0;
    }

    /// Fetch a fresh token (popout/web), the usher multivariant playlist and
    /// finally the media playlist matching our resolution.
    async fn fetch_fresh_playlist(&self) -> Option<String> {
        let token = match self.api.access_token(&self.channel, "popout", "web").await {
            Ok(TokenResult::Token(token)) => token,
            Ok(other) => {
                tracing::debug!("[AdBlock] Fresh token request failed: {other:?}");
                return None;
            }
            Err(err) => {
                tracing::debug!("[AdBlock] Exception getting fresh token: {err:#}");
                return None;
            }
        };

        let url = self.usher.channel_url(&self.channel, &token, true);
        let multivariant = self.http_get(url.as_str()).await?;

        let variants = parse_multivariant_playlist(&multivariant, Some(url.as_str()));
        let stream_url = select_by_resolution(&variants.variants, self.resolution)?;

        self.http_get(&stream_url).await
    }

    async fn http_get(&self, url: &str) -> Option<String> {
        let resp = self
            .client
            .get(url)
            .timeout(HTTP_TIMEOUT)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            tracing::debug!("[AdBlock] Request failed with status {}", resp.status());
            return None;
        }
        resp.text().await.ok()
    }
}

/// Pick the variant whose pixel area is closest to the target resolution;
/// falls back to the first variant.
pub(crate) fn select_by_resolution(
    variants: &[crate::hls::m3u8::VariantStream],
    target: Option<(u32, u32)>,
) -> Option<String> {
    if variants.is_empty() {
        return None;
    }
    let Some((tw, th)) = target else {
        return Some(variants[0].uri.clone());
    };
    let target_pixels = tw as i64 * th as i64;

    variants
        .iter()
        .filter(|v| v.resolution.is_some())
        .min_by_key(|v| (v.pixels() as i64 - target_pixels).abs())
        .map(|v| v.uri.clone())
        .or_else(|| Some(variants[0].uri.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hls::m3u8::VariantStream;

    fn v(name: &str, res: Option<(u32, u32)>) -> VariantStream {
        VariantStream {
            name: name.to_string(),
            uri: format!("https://example.com/{name}.m3u8"),
            bandwidth: 0,
            resolution: res,
            framerate: None,
        }
    }

    #[test]
    fn resolution_matching_by_pixel_area() {
        let variants = vec![
            v("1080p", Some((1920, 1080))),
            v("720p", Some((1280, 720))),
            v("480p", Some((852, 480))),
            v("audio", None),
        ];
        assert_eq!(
            select_by_resolution(&variants, Some((1920, 1080))).unwrap(),
            "https://example.com/1080p.m3u8"
        );
        assert_eq!(
            select_by_resolution(&variants, Some((1280, 720))).unwrap(),
            "https://example.com/720p.m3u8"
        );
        // closest match by pixel area wins when exact is unavailable
        // (1600x900 = 1.44MP is closer to 720p's 0.92MP than 1080p's 2.07MP)
        assert_eq!(
            select_by_resolution(&variants, Some((1600, 900))).unwrap(),
            "https://example.com/720p.m3u8"
        );
        assert_eq!(
            select_by_resolution(&variants, Some((1920, 1200))).unwrap(),
            "https://example.com/1080p.m3u8"
        );
        // no target -> first
        assert_eq!(
            select_by_resolution(&variants, None).unwrap(),
            "https://example.com/1080p.m3u8"
        );
    }
}
