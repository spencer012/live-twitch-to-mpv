//! BackupStreamManager port: pre-roll fallback using alternate player types.
//!
//! Twitch serves different ad decisions per player type; embed/popout/autoplay
//! tokens often skip pre-roll ads entirely. Playlists are checked for ads via
//! a case-insensitive "stitched" substring, matching the Python fork.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::hls::m3u8::parse_multivariant_playlist;
use crate::twitch::api::{TokenResult, TwitchApi};
use crate::twitch::usher::UsherService;

const BACKUP_PLAYER_TYPES: [&str; 3] = ["embed", "popout", "autoplay"];
const TOKEN_MIN_INTERVAL: Duration = Duration::from_secs(2);
const ENCODINGS_CACHE_TTL: Duration = Duration::from_secs(30);
const STICKY_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

fn platform_for(player_type: &str) -> &'static str {
    if player_type == "autoplay" {
        "android"
    } else {
        "web"
    }
}

fn has_ads(content: &str) -> bool {
    content.to_lowercase().contains("stitched")
}

/// Per-player-type rate limiter with a permanent per-session blacklist.
struct RateLimiter {
    last_request: HashMap<String, Instant>,
    failed: HashSet<String>,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            last_request: HashMap::new(),
            failed: HashSet::new(),
        }
    }

    fn can_request(&self, player_type: &str) -> bool {
        if self.failed.contains(player_type) {
            return false;
        }
        self.last_request
            .get(player_type)
            .is_none_or(|t| t.elapsed() >= TOKEN_MIN_INTERVAL)
    }

    fn mark_requested(&mut self, player_type: &str) {
        self.last_request
            .insert(player_type.to_string(), Instant::now());
    }

    fn mark_failed(&mut self, player_type: &str) {
        tracing::debug!("[AdBlock] {player_type} marked as failed, will not retry this session");
        self.failed.insert(player_type.to_string());
    }

    fn is_failed(&self, player_type: &str) -> bool {
        self.failed.contains(player_type)
    }
}

/// Mutable state shared between the manager and background fetch tasks.
/// Guarded by a std Mutex: never held across an await point.
struct SharedState {
    rate_limiter: RateLimiter,
    /// player_type -> (multivariant playlist content, base URL, fetched at)
    encodings_cache: HashMap<String, (String, String, Instant)>,

    // Sticky source selection: keep using a working source for a while.
    preferred_type: Option<String>,
    preferred_at: Option<Instant>,
}

/// The network half of the backup manager: runs the whole
/// "find an ad-free playlist" flow in a background task so the segment
/// pipeline is never blocked on token/usher/playlist round trips.
struct BackupFetcher {
    client: reqwest::Client,
    api: TwitchApi,
    usher: UsherService,
    channel: String,
    resolution: Option<(u32, u32)>,
    state: Mutex<SharedState>,
}

impl BackupFetcher {
    async fn get_backup_token(&self, player_type: &str) -> Option<crate::twitch::api::AccessToken> {
        {
            let mut state = self.state.lock().unwrap();
            if !state.rate_limiter.can_request(player_type) {
                return None;
            }
            state.rate_limiter.mark_requested(player_type);
        }

        tracing::debug!("[AdBlock] Requesting backup token with playerType={player_type}");
        match self
            .api
            .access_token(&self.channel, player_type, platform_for(player_type))
            .await
        {
            Ok(TokenResult::Token(token)) => Some(token),
            Ok(other) => {
                tracing::debug!("[AdBlock] Backup token failed for {player_type}: {other:?}");
                self.state
                    .lock()
                    .unwrap()
                    .rate_limiter
                    .mark_failed(player_type);
                None
            }
            Err(err) => {
                tracing::debug!(
                    "[AdBlock] Exception getting backup token for {player_type}: {err:#}"
                );
                self.state
                    .lock()
                    .unwrap()
                    .rate_limiter
                    .mark_failed(player_type);
                None
            }
        }
    }

    async fn fetch_encodings(&self, player_type: &str) -> Option<(String, String)> {
        {
            let state = self.state.lock().unwrap();
            if let Some((content, base_url, _)) = state
                .encodings_cache
                .get(player_type)
                .filter(|(_, _, fetched_at)| fetched_at.elapsed() < ENCODINGS_CACHE_TTL)
            {
                tracing::debug!("[AdBlock] Using cached encodings for {player_type}");
                return Some((content.clone(), base_url.clone()));
            }
        }

        let token = self.get_backup_token(player_type).await?;
        let url = self.usher.channel_url(&self.channel, &token, true);

        tracing::debug!("[AdBlock] Fetching backup encodings from usher for {player_type}");
        let content = self.http_get(url.as_str()).await?;
        self.state.lock().unwrap().encodings_cache.insert(
            player_type.to_string(),
            (content.clone(), url.to_string(), Instant::now()),
        );
        Some((content, url.to_string()))
    }

    /// Fetch the media playlist for a backup player type at the resolution
    /// closest to the current stream.
    async fn fetch_backup_playlist(&self, player_type: &str) -> Option<String> {
        let (encodings, base_url) = self.fetch_encodings(player_type).await?;
        let variants = parse_multivariant_playlist(&encodings, Some(base_url.as_str()));
        let stream_url = super::delayed::select_by_resolution(&variants.variants, self.resolution)?;

        tracing::debug!("[AdBlock] Fetching backup segment playlist for {player_type}");
        self.http_get(&stream_url).await
    }

    /// Try to get an ad-free media playlist from any backup player type,
    /// preferring the sticky source while it keeps working.
    async fn get_ad_free_playlist(&self) -> Option<(String, String)> {
        let sticky = {
            let state = self.state.lock().unwrap();
            state
                .preferred_type
                .clone()
                .filter(|preferred| !state.rate_limiter.is_failed(preferred))
                .filter(|_| {
                    state
                        .preferred_at
                        .is_some_and(|t| t.elapsed() < STICKY_TIMEOUT)
                })
        };
        if let Some(preferred) = sticky
            && let Some(content) = self.fetch_backup_playlist(&preferred).await
        {
            if !has_ads(&content) {
                self.state.lock().unwrap().preferred_at = Some(Instant::now());
                tracing::debug!("[AdBlock] Using sticky preferred source: {preferred}");
                return Some((preferred, content));
            }
            tracing::debug!("[AdBlock] Preferred source {preferred} now has ads, trying others...");
        }

        for player_type in BACKUP_PLAYER_TYPES {
            if self
                .state
                .lock()
                .unwrap()
                .rate_limiter
                .is_failed(player_type)
            {
                continue;
            }
            let Some(content) = self.fetch_backup_playlist(player_type).await else {
                continue;
            };
            if has_ads(&content) {
                tracing::debug!("[AdBlock] Backup {player_type} also has ads, trying next...");
                continue;
            }
            tracing::debug!("[AdBlock] Found ad-free backup from {player_type}");
            {
                let mut state = self.state.lock().unwrap();
                state.preferred_type = Some(player_type.to_string());
                state.preferred_at = Some(Instant::now());
            }
            return Some((player_type.to_string(), content));
        }

        tracing::debug!("[AdBlock] No ad-free backup found, falling back to filtering");
        None
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
            tracing::debug!(
                "[AdBlock] Backup request failed with status {}",
                resp.status()
            );
            return None;
        }
        resp.text().await.ok()
    }
}

pub struct BackupStreamManager {
    fetcher: Arc<BackupFetcher>,
    /// In-flight background flow, harvested when finished.
    pending: Option<tokio::task::JoinHandle<Option<(String, String)>>>,

    is_showing_ad: bool,
    num_stripped: usize,
}

impl BackupStreamManager {
    pub fn new(
        client: reqwest::Client,
        api: TwitchApi,
        usher: UsherService,
        channel: &str,
        resolution: Option<(u32, u32)>,
    ) -> Self {
        tracing::debug!(
            "[AdBlock] BackupStreamManager initialized for channel={channel}, resolution={resolution:?}"
        );
        Self {
            fetcher: Arc::new(BackupFetcher {
                client,
                api,
                usher,
                channel: channel.to_string(),
                resolution,
                state: Mutex::new(SharedState {
                    rate_limiter: RateLimiter::new(),
                    encodings_cache: HashMap::new(),
                    preferred_type: None,
                    preferred_at: None,
                }),
            }),
            pending: None,
            is_showing_ad: false,
            num_stripped: 0,
        }
    }

    pub fn on_ad_start(&mut self) {
        if !self.is_showing_ad {
            self.is_showing_ad = true;
            self.num_stripped = 0;
            tracing::info!("[AdBlock] Ad detected, attempting backup stream...");
        }
    }

    pub fn on_ad_end(&mut self) {
        if self.is_showing_ad {
            self.is_showing_ad = false;
            tracing::info!(
                "[AdBlock] Ad break finished (stripped {} segments)",
                self.num_stripped
            );
            self.num_stripped = 0;
        }
        if let Some(handle) = self.pending.take() {
            handle.abort();
        }
    }

    pub fn count_stripped(&mut self) {
        self.num_stripped += 1;
    }

    /// Non-blocking: harvest a finished background flow, or start one.
    /// Returns Some only when a fetched ad-free playlist is ready.
    pub async fn poll_ad_free_playlist(&mut self) -> Option<(String, String)> {
        if let Some(handle) = &self.pending {
            if !handle.is_finished() {
                return None;
            }
            let handle = self.pending.take().expect("checked above");
            return match handle.await {
                Ok(result) => result,
                Err(err) => {
                    tracing::debug!("[AdBlock] Backup fetch task failed: {err}");
                    None
                }
            };
        }

        let fetcher = self.fetcher.clone();
        self.pending = Some(tokio::spawn(
            async move { fetcher.get_ad_free_playlist().await },
        ));
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_mapping() {
        assert_eq!(platform_for("embed"), "web");
        assert_eq!(platform_for("popout"), "web");
        assert_eq!(platform_for("autoplay"), "android");
    }

    #[test]
    fn ad_detection_is_case_insensitive_substring() {
        assert!(has_ads("#EXT-X-DATERANGE:CLASS=\"twitch-STITCHED-ad\""));
        assert!(has_ads("#EXT-X-DATERANGE:ID=\"stitched-ad-123\""));
        assert!(!has_ads("#EXTINF:2.000,live\nseg.ts"));
    }

    #[test]
    fn rate_limiter_blacklist_is_permanent() {
        let mut limiter = RateLimiter::new();
        assert!(limiter.can_request("embed"));
        limiter.mark_failed("embed");
        assert!(!limiter.can_request("embed"));
        assert!(limiter.can_request("popout"));
    }

    #[test]
    fn rate_limiter_min_interval() {
        let mut limiter = RateLimiter::new();
        limiter.mark_requested("popout");
        assert!(!limiter.can_request("popout"), "2s interval not elapsed");
    }
}
