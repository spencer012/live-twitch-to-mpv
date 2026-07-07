mod adblock;
mod buffer;
mod config;
mod hls;
mod player;
mod quality;
mod twitch;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use tokio::sync::{Mutex, mpsc};

use crate::adblock::TwitchAdBlock;
use crate::buffer::byte_channel;
use crate::config::Config;
use crate::hls::fetch::{FetcherConfig, SegmentFetcher};
use crate::hls::hook::{AdFilterHook, SegmentHook};
use crate::hls::m3u8::{VariantStream, parse_multivariant_playlist};
use crate::hls::worker::{PlaylistWorker, RecoveryRequest, WorkerConfig};
use crate::player::Player;
use crate::quality::select_variant;
use crate::twitch::api::{TokenResult, TwitchApi};
use crate::twitch::usher::{PLAYER_REFERER, UsherService};

const PREBUFFER_SIZE: usize = 8192;

#[derive(Parser, Debug)]
#[command(
    name = "streamlink-rust",
    about = "Standalone Twitch live stream player pipeline (mpv output)",
    version
)]
struct Cli {
    /// Channel name or twitch.tv URL
    channel: String,

    /// Stream quality override (e.g. 1080p60, 720p, best, worst)
    quality: Option<String>,

    /// Path to the TOML config file
    #[arg(long)]
    config: Option<PathBuf>,

    /// Verify token/usher/playlist fetching and parsing, then exit
    /// without launching the player
    #[arg(long)]
    check: bool,

    /// Override the log level from the config (trace|debug|info|warn|error)
    #[arg(long)]
    log_level: Option<String>,
}

fn parse_channel(input: &str) -> Result<String> {
    let input = input.trim();
    let channel = if let Some(rest) = input
        .strip_prefix("https://")
        .or_else(|| input.strip_prefix("http://"))
    {
        let rest = rest
            .strip_prefix("www.")
            .or_else(|| rest.strip_prefix("m."))
            .unwrap_or(rest);
        let rest = rest
            .strip_prefix("twitch.tv/")
            .with_context(|| format!("not a twitch.tv URL: {input}"))?;
        rest.split(['/', '?']).next().unwrap_or_default()
    } else {
        input
    };
    let channel = channel.trim().to_lowercase();
    if channel.is_empty()
        || !channel
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        bail!("invalid channel name: {input}");
    }
    Ok(channel)
}

fn init_logging(level: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn build_http_client() -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Referer", PLAYER_REFERER.parse().unwrap());
    headers.insert("Origin", PLAYER_REFERER.parse().unwrap());
    reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
             Chrome/136.0.0.0 Safari/537.36",
        )
        .default_headers(headers)
        .build()
        .context("failed to build HTTP client")
}

struct ResolvedStream {
    variant: VariantStream,
    variants: Vec<VariantStream>,
}

/// Token -> usher -> multivariant -> variant selection.
async fn resolve_stream(
    client: &reqwest::Client,
    api: &TwitchApi,
    usher: &UsherService,
    config: &Config,
    channel: &str,
    quality_override: Option<&str>,
) -> Result<Option<ResolvedStream>> {
    let token = match api.access_token(channel, "popout", "site").await? {
        TokenResult::Token(token) => token,
        TokenResult::Offline => {
            tracing::debug!("Access token empty: channel is offline or does not exist");
            return Ok(None);
        }
        TokenResult::Error(message) => {
            // No client-integrity browser flow in this port: report clearly.
            bail!(
                "Twitch API error while fetching the access token: {message}\n\
                 (client-integrity token acquisition is not supported by streamlink-rust)"
            );
        }
    };

    let url = usher.channel_url(channel, &token, config.twitch.ad_block);
    tracing::debug!("Fetching multivariant playlist");
    let resp = client
        .get(url.as_str())
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("usher request failed")?;
    if !resp.status().is_success() {
        tracing::debug!("Usher returned status {}: stream offline", resp.status());
        return Ok(None);
    }
    let content = resp.text().await.context("failed to read usher response")?;
    let multivariant = parse_multivariant_playlist(&content, Some(url.as_str()));
    if multivariant.variants.is_empty() {
        return Ok(None);
    }

    let names: Vec<&str> = multivariant
        .variants
        .iter()
        .map(|v| v.name.as_str())
        .collect();
    tracing::info!("Available streams: {}", names.join(", "));

    // CLI quality wins unless the config prefers its own priority list.
    let priority: Vec<String> = match quality_override {
        Some(q) if !config.quality.prefer_config => {
            q.split(',').map(|s| s.trim().to_string()).collect()
        }
        _ => config.quality.priority.clone(),
    };

    let Some(variant) = select_variant(&multivariant.variants, &priority) else {
        bail!(
            "no stream matched the quality priority list {:?}; available: {}",
            priority,
            names.join(", ")
        );
    };
    tracing::info!("Selected stream quality: {}", variant.name);

    Ok(Some(ResolvedStream {
        variant: variant.clone(),
        variants: multivariant.variants,
    }))
}

/// Poll for stream availability using the retry config.
async fn wait_for_stream(
    client: &reqwest::Client,
    api: &TwitchApi,
    usher: &UsherService,
    config: &Config,
    channel: &str,
    quality_override: Option<&str>,
) -> Result<ResolvedStream> {
    let mut polls: u64 = 0;
    loop {
        polls += 1;
        match resolve_stream(client, api, usher, config, channel, quality_override).await? {
            Some(resolved) => return Ok(resolved),
            None => {
                if config.retry.streams <= 0.0 {
                    bail!("no playable streams found on channel {channel}");
                }
                if config.retry.max > 0 && polls >= config.retry.max {
                    bail!(
                        "no playable streams found on channel {channel} after {polls} attempts"
                    );
                }
                if polls == 1 {
                    tracing::info!(
                        "Waiting for stream to become available (polling every {}s)...",
                        config.retry.streams
                    );
                }
                tokio::time::sleep(Duration::from_secs_f64(config.retry.streams)).await;
            }
        }
    }
}

enum StreamEnd {
    /// Playlist/worker ended (stream went offline for good).
    Ended,
    /// The player process was closed by the user.
    PlayerClosed,
}

/// Run the full pipeline for one stream: worker -> fetcher -> buffer -> player.
async fn run_stream(
    client: &reqwest::Client,
    api: &TwitchApi,
    usher: &UsherService,
    config: &Config,
    channel: &str,
    resolved: &ResolvedStream,
    title: Option<&str>,
) -> Result<StreamEnd> {
    let hook: Arc<Mutex<dyn SegmentHook>> = if config.twitch.ad_block {
        Arc::new(Mutex::new(TwitchAdBlock::new(
            client.clone(),
            api.clone(),
            usher.clone(),
            channel,
            resolved.variant.resolution,
        )))
    } else {
        tracing::info!("Will skip ad segments");
        Arc::new(Mutex::new(AdFilterHook))
    };

    if config.twitch.low_latency {
        let edge = config
            .stream
            .live_edge
            .clamp(1, hls::worker::LOW_LATENCY_MAX_LIVE_EDGE);
        tracing::info!("Low latency streaming (HLS live edge: {edge})");
    }

    let recovery = RecoveryRequest::default();
    let (segment_tx, segment_rx) = mpsc::channel(20);
    let (byte_tx, mut byte_rx) = byte_channel(config.stream.ring_buffer_mb as usize * 1024 * 1024);

    let worker = PlaylistWorker::new(
        client.clone(),
        resolved.variant.uri.clone(),
        WorkerConfig {
            low_latency: config.twitch.low_latency,
            live_edge: config.stream.live_edge,
            live_restart: config.stream.live_restart,
            persist_stream: config.stream.persist_stream,
            recovery_timeout: config.stream.recovery_timeout,
            reload_attempts: config.stream.segment_attempts,
        },
        hook.clone(),
        segment_tx,
        recovery.clone(),
    );

    let fetcher = SegmentFetcher::new(
        client.clone(),
        FetcherConfig {
            threads: config.stream.segment_threads,
            attempts: config.stream.segment_attempts,
            timeout: config.stream.segment_timeout,
            persist_stream: config.stream.persist_stream,
        },
        hook,
        recovery,
    );

    let worker_task = tokio::spawn(worker.run());
    let fetcher_task = tokio::spawn(fetcher.run(segment_rx, byte_tx));

    // A read timeout aborts the stream like streamlink's --stream-timeout.
    let stream_timeout = Duration::from_secs_f64(config.stream.stream_timeout.max(1.0));
    async fn read(
        rx: &mut crate::buffer::ByteReceiver,
        timeout: Duration,
    ) -> Result<Option<bytes::Bytes>, tokio::time::error::Elapsed> {
        tokio::time::timeout(timeout, rx.recv()).await
    }

    // Prebuffer before launching the player.
    tracing::debug!("Pre-buffering {PREBUFFER_SIZE} bytes...");
    let mut prebuffer: Vec<bytes::Bytes> = Vec::new();
    let mut prebuffered = 0usize;
    while prebuffered < PREBUFFER_SIZE {
        match read(&mut byte_rx, stream_timeout).await {
            Ok(Some(chunk)) => {
                prebuffered += chunk.len();
                prebuffer.push(chunk);
            }
            Ok(None) => {
                worker_task.abort();
                fetcher_task.abort();
                bail!("stream ended before any data was received");
            }
            Err(_) => {
                worker_task.abort();
                fetcher_task.abort();
                bail!(
                    "no stream data within {:.0}s (stream timeout)",
                    stream_timeout.as_secs_f64()
                );
            }
        }
    }

    let mut player = Player::spawn(&config.player, channel, title)?;
    tracing::info!("Player started, streaming...");

    let mut result = StreamEnd::Ended;
    'outer: {
        for chunk in prebuffer {
            if player.write(&chunk).await.is_err() {
                result = StreamEnd::PlayerClosed;
                break 'outer;
            }
        }
        loop {
            match read(&mut byte_rx, stream_timeout).await {
                Ok(Some(chunk)) => {
                    if player.write(&chunk).await.is_err() {
                        result = StreamEnd::PlayerClosed;
                        break;
                    }
                }
                Ok(None) => {
                    tracing::info!("Stream ended");
                    break;
                }
                Err(_) => {
                    tracing::error!(
                        "No stream data within {:.0}s, stopping stream",
                        stream_timeout.as_secs_f64()
                    );
                    break;
                }
            }
        }
    }

    worker_task.abort();
    fetcher_task.abort();

    match result {
        StreamEnd::PlayerClosed => {
            tracing::info!("Player closed");
            let _ = player.child.kill().await;
        }
        StreamEnd::Ended => {
            player.finish().await?;
        }
    }

    Ok(result)
}

/// `--check` mode: verify the whole request/parse chain without a player.
async fn run_check(
    client: &reqwest::Client,
    config: &Config,
    channel: &str,
    resolved: &ResolvedStream,
) -> Result<()> {
    println!("channel:  {channel}");
    println!("variants: {}", resolved.variants.len());
    for v in &resolved.variants {
        let res = v
            .resolution
            .map(|(w, h)| format!("{w}x{h}"))
            .unwrap_or_else(|| "audio".into());
        println!("  - {:<20} {:>9}  {} bps", v.name, res, v.bandwidth);
    }
    println!("selected: {} ({})", resolved.variant.name, resolved.variant.uri);

    let resp = client
        .get(&resolved.variant.uri)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("media playlist request failed")?;
    anyhow::ensure!(
        resp.status().is_success(),
        "media playlist request failed with status {}",
        resp.status()
    );
    let content = resp.text().await?;
    let playlist = crate::hls::m3u8::parse_media_playlist(&content, Some(&resolved.variant.uri));
    println!(
        "media playlist: {} segments (media_sequence={}, targetduration={:?}, prefetch={}, ads={})",
        playlist.segments.len(),
        playlist.media_sequence,
        playlist.targetduration,
        playlist.segments.iter().filter(|s| s.prefetch).count(),
        playlist.segments.iter().filter(|s| s.ad).count(),
    );
    anyhow::ensure!(
        !playlist.segments.is_empty(),
        "media playlist contains no segments"
    );

    // Fetch the first segment to prove segment downloads work.
    let first = &playlist.segments[0];
    let data = client
        .get(&first.uri)
        .timeout(Duration::from_secs_f64(config.stream.segment_timeout))
        .send()
        .await
        .context("segment request failed")?
        .bytes()
        .await?;
    println!("first segment: {} bytes (num={})", data.len(), first.num);
    println!("check OK");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(cli.config.as_deref())?;
    let level = cli.log_level.as_deref().unwrap_or(&config.log.level);
    init_logging(level);

    let channel = parse_channel(&cli.channel)?;
    let client = build_http_client()?;
    let api = TwitchApi::new(client.clone(), &config.twitch.api_headers)?;
    let usher = UsherService::new(&config.twitch.supported_codecs);

    // Metadata (best effort): used for the player title and live check logs.
    let metadata = match api.metadata_channel(&channel).await {
        Ok(meta) => {
            if let Some(title) = &meta.title {
                tracing::info!("Stream title: {title}");
            }
            if let Some(game) = &meta.game {
                tracing::info!("Category: {game}");
            }
            if !meta.is_live() {
                tracing::info!("Channel appears to be offline");
            }
            Some(meta)
        }
        Err(err) => {
            tracing::debug!("Failed to fetch channel metadata: {err:#}");
            None
        }
    };
    let title = metadata.as_ref().and_then(|m| {
        let author = m.display_name.as_deref().unwrap_or(&channel);
        m.title.as_ref().map(|t| format!("{author} - {t}"))
    });

    let resolved = wait_for_stream(
        &client,
        &api,
        &usher,
        &config,
        &channel,
        cli.quality.as_deref(),
    )
    .await?;

    if cli.check {
        return run_check(&client, &config, &channel, &resolved).await;
    }

    // retry.open: attempts to open (start) the selected stream.
    let attempts = config.retry.open.max(1);
    let mut last_err = None;
    for attempt in 1..=attempts {
        if attempt > 1 {
            tracing::warn!("Retrying stream open (attempt {attempt}/{attempts})...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        match run_stream(&client, &api, &usher, &config, &channel, &resolved, title.as_deref())
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                tracing::error!("Stream failed to open: {err:#}");
                last_err = Some(err);
            }
        }
    }
    Err(last_err.expect("at least one attempt was made"))
}
