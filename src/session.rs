//! Shared local-HLS session engine used by the CLI server mode and JNI.

use std::collections::{BTreeMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::adblock::TwitchAdBlock;
use crate::hls::hook::{AdFilterHook, SegmentAction, SegmentHook};
use crate::hls::m3u8::{
    MediaPlaylist, MediaSegment, VariantStream, parse_media_playlist, parse_multivariant_playlist,
};
use crate::quality::select_variant;
use crate::timeline::{DEFAULT_BUFFER_CAP_BYTES, SegmentSource, StagedSegment, Timeline};
use crate::twitch::api::{TokenResult, TwitchApi, VodInfo};
use crate::twitch::usher::{PLAYER_REFERER, UsherService};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionConfig {
    pub channel: String,
    #[serde(default = "default_quality")]
    pub quality: String,
    pub buffer_dir: String,
    #[serde(default = "default_buffer_cap")]
    pub buffer_cap_bytes: u64,
    #[serde(default)]
    pub adblock: AdblockConfig,
    #[serde(default = "default_recovery_timeout")]
    pub recovery_timeout_sec: u64,
    #[serde(default)]
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdblockConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub backup_player_types: bool,
    #[serde(default = "default_true")]
    pub delayed_playback: bool,
}

impl Default for AdblockConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backup_player_types: true,
            delayed_playback: true,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_quality() -> String {
    "best".to_string()
}

fn default_buffer_cap() -> u64 {
    DEFAULT_BUFFER_CAP_BYTES
}

fn default_recovery_timeout() -> u64 {
    600
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum SessionEvent {
    #[serde(rename = "liveEdge")]
    LiveEdge {
        #[serde(rename = "unixMs")]
        unix_ms: i64,
        #[serde(rename = "streamOffsetSec")]
        stream_offset_sec: f64,
    },
    #[serde(rename = "outage")]
    Outage {
        state: &'static str,
        #[serde(rename = "gapStartUnixMs")]
        gap_start_unix_ms: i64,
        #[serde(rename = "gapEndUnixMs")]
        gap_end_unix_ms: i64,
    },
    #[serde(rename = "gap")]
    Gap {
        reason: &'static str,
        #[serde(rename = "missedSec")]
        missed_sec: f64,
    },
    #[serde(rename = "adState")]
    AdState {
        active: bool,
        strategy: &'static str,
    },
    #[serde(rename = "quality")]
    Quality { available: Vec<String> },
    #[serde(rename = "error")]
    Error { message: String, fatal: bool },
    #[serde(rename = "ended")]
    Ended,
}

#[derive(Default)]
pub struct EventQueue {
    queue: StdMutex<VecDeque<String>>,
    ready: Condvar,
}

impl EventQueue {
    pub fn push(&self, event: SessionEvent) {
        if let Ok(json) = serde_json::to_string(&event) {
            self.queue.lock().unwrap().push_back(json);
            self.ready.notify_one();
        }
    }

    pub fn poll(&self, timeout: Duration) -> Option<String> {
        let mut queue = self.queue.lock().unwrap();
        if let Some(event) = queue.pop_front() {
            return Some(event);
        }
        let (mut queue, _) = self.ready.wait_timeout(queue, timeout).unwrap();
        queue.pop_front()
    }
}

pub struct Session {
    local_url: String,
    timeline: Timeline,
    events: Arc<EventQueue>,
    cancel: CancellationToken,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
}

impl Session {
    pub async fn start(mut config: SessionConfig) -> Result<Arc<Self>> {
        config.channel = normalize_channel(&config.channel)?;
        if config.quality.trim().is_empty() {
            config.quality = default_quality();
        }
        let session_dir = PathBuf::from(&config.buffer_dir).join(format!(
            "{}-{}-{}",
            config.channel,
            std::process::id(),
            Utc::now().timestamp_millis()
        ));
        let timeline = Timeline::new(session_dir, config.buffer_cap_bytes).await?;
        let listener = tokio::net::TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            config.port,
        ))
        .await
        .context("failed to bind local HLS server")?;
        let address = listener.local_addr()?;
        let local_url = format!("http://127.0.0.1:{}/stream.m3u8", address.port());
        let events = Arc::new(EventQueue::default());
        let cancel = CancellationToken::new();
        let session = Arc::new(Self {
            local_url,
            timeline: timeline.clone(),
            events: events.clone(),
            cancel: cancel.clone(),
            tasks: StdMutex::new(Vec::new()),
        });

        let app = Router::new()
            .route("/stream.m3u8", get(serve_playlist))
            .route("/seg/{name}", get(serve_segment))
            .route("/control/position", post(report_position))
            .with_state(timeline.clone());
        let server_cancel = cancel.clone();
        let server = tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app)
                .with_graceful_shutdown(server_cancel.cancelled_owned())
                .await
            {
                tracing::error!("local HLS server failed: {err}");
            }
        });

        let engine_cancel = cancel.clone();
        let engine_timeline = timeline;
        let engine_events = events;
        let engine = tokio::spawn(async move {
            if let Err(err) = run_engine(
                config,
                engine_timeline.clone(),
                engine_events.clone(),
                engine_cancel,
            )
            .await
            {
                engine_events.push(SessionEvent::Error {
                    message: format!("{err:#}"),
                    fatal: true,
                });
            }
            engine_timeline.end().await;
            engine_events.push(SessionEvent::Ended);
        });
        session.tasks.lock().unwrap().extend([server, engine]);
        Ok(session)
    }

    pub fn local_url(&self) -> &str {
        &self.local_url
    }

    pub fn poll_event(&self, timeout: Duration) -> Option<String> {
        self.events.poll(timeout)
    }

    pub async fn report_position(&self, stream_offset_sec: f64) {
        self.timeline.report_position(stream_offset_sec).await;
    }

    pub async fn stop(&self) {
        self.cancel.cancel();
        self.timeline.end().await;
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionReport {
    stream_offset_sec: f64,
}

async fn serve_playlist(State(timeline): State<Timeline>) -> Response {
    let playlist = timeline.playlist().await;
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.apple.mpegurl"),
        )
        .header(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))
        .body(Body::from(playlist))
        .unwrap()
}

async fn serve_segment(State(timeline): State<Timeline>, Path(name): Path<String>) -> Response {
    let Some(id) = name
        .strip_suffix(".ts")
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(path) = timeline.segment_path(id).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            timeline.note_segment_request(id).await;
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, HeaderValue::from_static("video/mp2t"))
                .body(Body::from(bytes))
                .unwrap()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn report_position(
    State(timeline): State<Timeline>,
    Json(report): Json<PositionReport>,
) -> StatusCode {
    timeline.report_position(report.stream_offset_sec).await;
    StatusCode::NO_CONTENT
}

trait IntoResponse {
    fn into_response(self) -> Response;
}

impl IntoResponse for StatusCode {
    fn into_response(self) -> Response {
        Response::builder()
            .status(self)
            .body(Body::empty())
            .unwrap()
    }
}

struct EventHook {
    inner: Box<dyn SegmentHook>,
    events: Arc<EventQueue>,
    active: bool,
    had_content: bool,
    backup_enabled: bool,
    delayed_enabled: bool,
}

#[async_trait::async_trait]
impl SegmentHook for EventHook {
    async fn on_playlist(&mut self, playlist: &MediaPlaylist) {
        let active = playlist.segments.iter().any(|segment| segment.ad);
        if active != self.active {
            let strategy = if !self.had_content && self.backup_enabled {
                "backupPlayerTypes"
            } else if self.delayed_enabled {
                "delayedPlayback"
            } else {
                "filter"
            };
            self.events.push(SessionEvent::AdState { active, strategy });
            self.active = active;
        }
        self.had_content |= playlist.segments.iter().any(|segment| !segment.ad);
        self.inner.on_playlist(playlist).await;
    }

    async fn segment_action(&mut self, segment: &MediaSegment) -> SegmentAction {
        self.inner.segment_action(segment).await
    }
}

async fn run_engine(
    config: SessionConfig,
    timeline: Timeline,
    events: Arc<EventQueue>,
    cancel: CancellationToken,
) -> Result<()> {
    let client = build_http_client()?;
    let api = TwitchApi::new(client.clone(), &BTreeMap::new())?;
    let usher = UsherService::new(&["h264".to_string()]);
    let metadata = api.metadata_channel(&config.channel).await.ok();
    let stream_started_ms = metadata
        .as_ref()
        .and_then(|metadata| metadata.stream_created_at_unix_ms);
    let mut resolved = resolve_live(
        &client,
        &api,
        &usher,
        &config.channel,
        &config.quality,
        false,
    )
    .await?;
    events.push(SessionEvent::Quality {
        available: resolved
            .variants
            .iter()
            .map(|variant| variant.name.clone())
            .collect(),
    });

    let hook: Box<dyn SegmentHook> = if config.adblock.enabled {
        Box::new(TwitchAdBlock::new_with_options(
            client.clone(),
            api.clone(),
            usher.clone(),
            &config.channel,
            resolved.variant.resolution,
            config.adblock.delayed_playback,
            config.adblock.backup_player_types,
        ))
    } else {
        Box::new(AdFilterHook)
    };
    let mut hook = EventHook {
        inner: hook,
        events: events.clone(),
        active: false,
        had_content: false,
        backup_enabled: config.adblock.backup_player_types,
        delayed_enabled: config.adblock.delayed_playback,
    };
    let mut sequence = None;
    let mut last_productive = Instant::now();
    let mut backfills: Vec<JoinHandle<()>> = Vec::new();

    loop {
        if cancel.is_cancelled() {
            break;
        }
        let playlist = match fetch_media_playlist(&client, &resolved.variant.uri).await {
            Ok(playlist) => playlist,
            Err(err) => {
                tracing::warn!("live playlist failed: {err:#}");
                let outage_start = Utc::now().timestamp_millis();
                events.push(SessionEvent::Outage {
                    state: "lost",
                    gap_start_unix_ms: outage_start,
                    gap_end_unix_ms: outage_start,
                });
                events.push(SessionEvent::Outage {
                    state: "recovering",
                    gap_start_unix_ms: outage_start,
                    gap_end_unix_ms: outage_start,
                });
                timeline.begin_gap().await;
                let (next, outage_end) = recover_live(
                    &client,
                    &api,
                    &usher,
                    &config,
                    &events,
                    outage_start,
                    &cancel,
                )
                .await?;
                resolved = next;
                sequence = None;
                last_productive = Instant::now();
                events.push(SessionEvent::Outage {
                    state: "backfilling",
                    gap_start_unix_ms: outage_start,
                    gap_end_unix_ms: outage_end,
                });
                let task = tokio::spawn(backfill_gap(
                    client.clone(),
                    api.clone(),
                    usher.clone(),
                    config.clone(),
                    timeline.clone(),
                    events.clone(),
                    cancel.clone(),
                    stream_started_ms,
                    outage_start,
                    outage_end,
                    resolved.variant.normalized_name(),
                ));
                backfills.push(task);
                continue;
            }
        };
        hook.on_playlist(&playlist).await;
        let segments: Vec<_> = if let Some(next) = sequence {
            playlist
                .segments
                .iter()
                .filter(|segment| segment.num >= next)
                .cloned()
                .collect()
        } else {
            let edge = 2.min(playlist.segments.len());
            playlist.segments[playlist.segments.len().saturating_sub(edge)..].to_vec()
        };

        let mut downloaded = false;
        for segment in segments {
            sequence = Some(segment.num + 1);
            let action = hook.segment_action(&segment).await;
            let url = match action {
                SegmentAction::Fetch => segment.uri.clone(),
                SegmentAction::Substitute(url) => url,
                SegmentAction::Skip => continue,
            };
            match download_segment(&client, &url).await {
                Ok(bytes) => {
                    let unix_ms = segment.date.map(|date| date.timestamp_millis());
                    let offset = timeline
                        .append_live(&bytes, segment.duration, segment.discontinuity, unix_ms)
                        .await?;
                    events.push(SessionEvent::LiveEdge {
                        unix_ms: unix_ms.unwrap_or_else(|| Utc::now().timestamp_millis()),
                        stream_offset_sec: offset + segment.duration,
                    });
                    last_productive = Instant::now();
                    downloaded = true;
                }
                Err(err) => {
                    tracing::warn!("live segment {} failed: {err:#}", segment.num);
                    sequence = Some(segment.num);
                    break;
                }
            }
        }

        let reload = playlist
            .segments
            .last()
            .map(|segment| segment.duration)
            .or(playlist.targetduration)
            .unwrap_or(2.0)
            .clamp(1.0, 10.0);
        if !downloaded && last_productive.elapsed() > Duration::from_secs_f64(reload * 4.0) {
            resolved.variant.uri = "invalid:stalled-playlist".to_string();
            continue;
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs_f64(reload)) => {}
        }
        backfills.retain(|task| !task.is_finished());
    }
    for task in backfills {
        task.abort();
    }
    Ok(())
}

struct ResolvedLive {
    variant: VariantStream,
    variants: Vec<VariantStream>,
}

async fn resolve_live(
    client: &reqwest::Client,
    api: &TwitchApi,
    usher: &UsherService,
    channel: &str,
    quality: &str,
    allow_offline: bool,
) -> Result<ResolvedLive> {
    let token = match api.access_token(channel, "popout", "site").await? {
        TokenResult::Token(token) => token,
        TokenResult::Offline if allow_offline => bail!("channel offline"),
        TokenResult::Offline => bail!("no playable streams found on channel {channel}"),
        TokenResult::Error(message) => bail!("Twitch access token failed: {message}"),
    };
    let primary_url = usher.channel_url(channel, &token, false);
    let legacy_url = usher.legacy_channel_url(channel, &token, false);
    let (master_url, text) = fetch_first_playlist(client, &[primary_url, legacy_url]).await?;
    let playlist = parse_multivariant_playlist(&text, Some(master_url.as_str()));
    let priority = vec![quality.to_string(), "best".to_string()];
    let variant = select_variant(&playlist.variants, &priority)
        .cloned()
        .ok_or_else(|| anyhow!("no stream matched quality {quality}"))?;
    Ok(ResolvedLive {
        variant,
        variants: playlist.variants,
    })
}

async fn fetch_first_playlist(
    client: &reqwest::Client,
    urls: &[url::Url],
) -> Result<(url::Url, String)> {
    let mut last_error = None;
    for url in urls {
        match client
            .get(url.as_str())
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                return Ok((url.clone(), response.text().await?));
            }
            Ok(response) => {
                last_error = Some(anyhow!("{} returned {}", url, response.status()));
            }
            Err(error) => last_error = Some(error.into()),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("no playlist URLs supplied")))
}

async fn recover_live(
    client: &reqwest::Client,
    api: &TwitchApi,
    usher: &UsherService,
    config: &SessionConfig,
    events: &EventQueue,
    outage_start: i64,
    cancel: &CancellationToken,
) -> Result<(ResolvedLive, i64)> {
    let started = Instant::now();
    let mut delay = Duration::from_secs(1);
    loop {
        if config.recovery_timeout_sec > 0
            && started.elapsed() >= Duration::from_secs(config.recovery_timeout_sec)
        {
            bail!(
                "stream recovery timed out after {} seconds",
                config.recovery_timeout_sec
            );
        }
        if cancel.is_cancelled() {
            bail!("session stopped");
        }
        match resolve_live(client, api, usher, &config.channel, &config.quality, true).await {
            Ok(resolved) => {
                if fetch_media_playlist(client, &resolved.variant.uri)
                    .await
                    .is_ok_and(|playlist| !playlist.segments.is_empty())
                {
                    return Ok((resolved, Utc::now().timestamp_millis()));
                }
            }
            Err(err) => tracing::debug!("recovery resolve failed: {err:#}"),
        }
        events.push(SessionEvent::Outage {
            state: "recovering",
            gap_start_unix_ms: outage_start,
            gap_end_unix_ms: Utc::now().timestamp_millis(),
        });
        tokio::select! {
            _ = cancel.cancelled() => bail!("session stopped"),
            _ = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(Duration::from_secs(10));
    }
}

#[allow(clippy::too_many_arguments)]
async fn backfill_gap(
    client: reqwest::Client,
    api: TwitchApi,
    usher: UsherService,
    config: SessionConfig,
    timeline: Timeline,
    events: Arc<EventQueue>,
    cancel: CancellationToken,
    stream_started_ms: Option<i64>,
    gap_start_ms: i64,
    gap_end_ms: i64,
    quality: String,
) {
    let max_wait = if config.recovery_timeout_sec == 0 {
        Duration::from_secs(600)
    } else {
        Duration::from_secs(config.recovery_timeout_sec)
    };
    let started = Instant::now();
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match try_backfill(
            &client,
            &api,
            &usher,
            &config.channel,
            &timeline,
            stream_started_ms,
            gap_start_ms,
            gap_end_ms,
            &quality,
        )
        .await
        {
            Ok(Some(staged)) => {
                timeline.finish_gap(staged).await;
                events.push(SessionEvent::Outage {
                    state: "recovered",
                    gap_start_unix_ms: gap_start_ms,
                    gap_end_unix_ms: gap_end_ms,
                });
                return;
            }
            Ok(None) => {}
            Err(err) => tracing::debug!("VOD backfill not ready: {err:#}"),
        }
        if started.elapsed() >= max_wait {
            timeline.finish_gap(Vec::new()).await;
            events.push(SessionEvent::Gap {
                reason: "noVod",
                missed_sec: (gap_end_ms - gap_start_ms).max(0) as f64 / 1000.0,
            });
            events.push(SessionEvent::Outage {
                state: "recovered",
                gap_start_unix_ms: gap_start_ms,
                gap_end_unix_ms: gap_end_ms,
            });
            return;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(15)) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_backfill(
    client: &reqwest::Client,
    api: &TwitchApi,
    usher: &UsherService,
    channel: &str,
    timeline: &Timeline,
    stream_started_ms: Option<i64>,
    gap_start_ms: i64,
    gap_end_ms: i64,
    quality: &str,
) -> Result<Option<Vec<StagedSegment>>> {
    let vods = api.recent_vods(channel).await?;
    let Some(vod) = choose_vod(&vods, stream_started_ms, gap_start_ms) else {
        return Ok(None);
    };
    let (gap_start_sec, gap_end_sec) =
        map_gap_to_vod_offsets(gap_start_ms, gap_end_ms, vod.recorded_at_unix_ms);
    if gap_start_sec < 0.0 {
        return Ok(None);
    }
    let token = match api.video_access_token(&vod.id).await? {
        TokenResult::Token(token) => token,
        _ => return Ok(None),
    };
    let master_url = usher.vod_url(&vod.id, &token);
    let master = client
        .get(master_url.as_str())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let variants = parse_multivariant_playlist(&master, Some(master_url.as_str()));
    let priority = vec![quality.to_string(), "best".to_string()];
    let Some(variant) = select_variant(&variants.variants, &priority) else {
        return Ok(None);
    };
    let playlist = fetch_media_playlist(client, &variant.uri).await?;
    let total_duration: f64 = playlist
        .segments
        .iter()
        .map(|segment| segment.duration)
        .sum();
    if total_duration + 0.5 < gap_end_sec {
        return Ok(None);
    }

    let mut offset = 0.0;
    let mut selected = Vec::new();
    for segment in playlist.segments {
        let segment_end = offset + segment.duration;
        if segment_end > gap_start_sec && offset < gap_end_sec {
            selected.push(segment);
        }
        offset = segment_end;
        if offset >= gap_end_sec {
            break;
        }
    }
    if selected.is_empty() {
        return Ok(None);
    }
    let mut staged = Vec::with_capacity(selected.len());
    for segment in selected {
        let bytes = download_segment(client, &segment.uri).await?;
        staged.push(
            timeline
                .stage(
                    &bytes,
                    segment.duration,
                    segment.discontinuity,
                    SegmentSource::Vod,
                    segment.date.map(|date| date.timestamp_millis()),
                )
                .await?,
        );
    }
    Ok(Some(staged))
}

fn choose_vod<'a>(
    vods: &'a [VodInfo],
    stream_started_ms: Option<i64>,
    gap_start_ms: i64,
) -> Option<&'a VodInfo> {
    let expected = stream_started_ms.unwrap_or(gap_start_ms);
    vods.iter()
        .filter(|vod| vod.recorded_at_unix_ms <= gap_start_ms + 30_000)
        .min_by_key(|vod| (vod.recorded_at_unix_ms - expected).abs())
}

pub fn map_gap_to_vod_offsets(
    gap_start_unix_ms: i64,
    gap_end_unix_ms: i64,
    vod_recorded_at_unix_ms: i64,
) -> (f64, f64) {
    (
        (gap_start_unix_ms - vod_recorded_at_unix_ms) as f64 / 1000.0,
        (gap_end_unix_ms - vod_recorded_at_unix_ms) as f64 / 1000.0,
    )
}

async fn fetch_media_playlist(client: &reqwest::Client, url: &str) -> Result<MediaPlaylist> {
    let response = client
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("playlist request failed")?
        .error_for_status()
        .context("playlist returned an error status")?;
    let text = response.text().await?;
    let playlist = parse_media_playlist(&text, Some(url));
    if playlist.segments.is_empty() {
        bail!("playlist had no segments");
    }
    Ok(playlist)
}

async fn download_segment(client: &reqwest::Client, url: &str) -> Result<bytes::Bytes> {
    client
        .get(url)
        .send()
        .await
        .context("segment request failed")?
        .error_for_status()
        .context("segment returned an error status")?
        .bytes()
        .await
        .context("segment body failed")
}

fn build_http_client() -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Referer", PLAYER_REFERER.parse().unwrap());
    headers.insert("Origin", PLAYER_REFERER.parse().unwrap());
    reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        )
        .default_headers(headers)
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .http1_only()
        .build()
        .context("failed to build HTTP client")
}

fn normalize_channel(input: &str) -> Result<String> {
    let input = input.trim();
    let channel = if let Some(rest) = input
        .strip_prefix("https://")
        .or_else(|| input.strip_prefix("http://"))
    {
        let rest = rest
            .strip_prefix("www.")
            .or_else(|| rest.strip_prefix("m."))
            .unwrap_or(rest);
        rest.strip_prefix("twitch.tv/")
            .context("not a twitch.tv URL")?
            .split(['/', '?'])
            .next()
            .unwrap_or_default()
    } else {
        input
    }
    .trim()
    .to_lowercase();
    if channel.is_empty()
        || !channel
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        bail!("invalid channel name: {input}");
    }
    Ok(channel)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_only_outage_window_to_vod() {
        let vod_start = 1_000_000;
        let (start, end) = map_gap_to_vod_offsets(1_120_000, 1_165_500, vod_start);
        assert_eq!(start, 120.0);
        assert_eq!(end, 165.5);
        assert_eq!(end - start, 45.5);
    }

    #[test]
    fn chooses_vod_matching_stream_start() {
        let vods = vec![
            VodInfo {
                id: "old".into(),
                recorded_at_unix_ms: 100,
                duration_sec: 100.0,
            },
            VodInfo {
                id: "current".into(),
                recorded_at_unix_ms: 10_000,
                duration_sec: 100.0,
            },
        ];
        assert_eq!(
            choose_vod(&vods, Some(10_500), 20_000).unwrap().id,
            "current"
        );
    }
}
