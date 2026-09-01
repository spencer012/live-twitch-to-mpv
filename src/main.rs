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
use crate::session::{AdblockConfig, Session, SessionConfig};
use crate::timeline::DEFAULT_BUFFER_CAP_BYTES;
use crate::twitch::api::{TokenResult, TwitchApi};
use crate::twitch::usher::{PLAYER_REFERER, UsherService};

const PREBUFFER_SIZE: usize = 8192;

#[derive(Parser, Debug)]
#[command(
    name = "twitchpipe",
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

    /// List available stream qualities and exit (one per line)
    #[arg(long)]
    streams: bool,

    /// Override the log level from the config (trace|debug|info|warn|error)
    #[arg(long)]
    log_level: Option<String>,

    /// Serve a seekable local HLS stream instead of launching mpv.
    /// An omitted port or port 0 selects a free localhost port.
    #[arg(long, num_args = 0..=1, default_missing_value = "0")]
    serve: Option<u16>,

    /// Directory used for local-HLS segment storage
    #[arg(long)]
    buffer_dir: Option<PathBuf>,

    /// Maximum local-HLS disk buffer size in bytes
    #[arg(long, default_value_t = DEFAULT_BUFFER_CAP_BYTES)]
    buffer_cap_bytes: u64,
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
        .with_writer(std::io::stderr)
        .init();
}

fn build_http_client(config: &Config) -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Referer", PLAYER_REFERER.parse().unwrap());
    headers.insert("Origin", PLAYER_REFERER.parse().unwrap());
    reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) \
             Chrome/136.0.0.0 Safari/537.36",
        )
        .default_headers(headers)
        // Timeouts are connect + read (max time between received bytes), like
        // streamlink/requests — NOT a total deadline. A total deadline would
        // cut off slow-but-alive segment downloads and expire while responses
        // wait their turn in the ordered write-out queue.
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs_f64(
            config.stream.segment_timeout.max(1.0),
        ))
        // HTTP/1.1 only, like streamlink (requests/urllib3). Over HTTP/2 all
        // requests share one connection, and segment responses that are held
        // with unread bodies (for ordered write-out) consume the connection's
        // flow-control window, stalling the actively-drained segment stream
        // and playlist reloads. HTTP/1.1 gives each in-flight segment its own
        // connection instead.
        .http1_only()
        .build()
        .context("failed to build HTTP client")
}

/// Re-resolves the media playlist URL for the persist/recovery path:
/// fresh token -> usher -> multivariant -> the originally selected quality.
struct VariantResolver {
    client: reqwest::Client,
    api: TwitchApi,
    usher: UsherService,
    channel: String,
    ad_block: bool,
    /// Quality names tried in order (original variant name, then "best").
    priority: Vec<String>,
}

#[async_trait::async_trait]
impl crate::hls::worker::StreamUrlResolver for VariantResolver {
    async fn resolve(&self) -> Option<String> {
        let token = match self.api.access_token(&self.channel, "popout", "site").await {
            Ok(TokenResult::Token(token)) => token,
            Ok(_) => return None,
            Err(err) => {
                tracing::debug!("Re-resolve token request failed: {err:#}");
                return None;
            }
        };
        let urls = [
            self.usher.channel_url(&self.channel, &token, self.ad_block),
            self.usher
                .legacy_channel_url(&self.channel, &token, self.ad_block),
        ];
        for url in urls {
            let Ok(resp) = self
                .client
                .get(url.as_str())
                .timeout(Duration::from_secs(10))
                .send()
                .await
            else {
                continue;
            };
            if !resp.status().is_success() {
                continue;
            }
            let content = resp.text().await.ok()?;
            let multivariant = parse_multivariant_playlist(&content, Some(url.as_str()));
            if let Some(variant) = select_variant(&multivariant.variants, &self.priority) {
                return Some(variant.uri.clone());
            }
        }
        None
    }
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
    list_only: bool,
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
                 (client-integrity token acquisition is not supported by TwitchPipe)"
            );
        }
    };

    tracing::debug!("Fetching multivariant playlist");
    let urls = [
        usher.channel_url(channel, &token, config.twitch.ad_block),
        usher.legacy_channel_url(channel, &token, config.twitch.ad_block),
    ];
    let mut selected_response = None;
    for url in urls {
        let resp = client
            .get(url.as_str())
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .context("usher request failed")?;
        if resp.status().is_success() {
            selected_response = Some((url, resp));
            break;
        }
        tracing::debug!("Usher returned status {}", resp.status());
    }
    let Some((url, resp)) = selected_response else {
        return Ok(None);
    };
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

    // Listing does not depend on quality configuration. Keep the first variant
    // only as an unused placeholder in the existing resolved-stream shape.
    if list_only {
        return Ok(Some(ResolvedStream {
            variant: multivariant.variants[0].clone(),
            variants: multivariant.variants,
        }));
    }

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
        match resolve_stream(client, api, usher, config, channel, quality_override, false).await? {
            Some(resolved) => return Ok(resolved),
            None => {
                if config.retry.streams <= 0.0 {
                    bail!("no playable streams found on channel {channel}");
                }
                if config.retry.max > 0 && polls >= config.retry.max {
                    bail!("no playable streams found on channel {channel} after {polls} attempts");
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
            .clamp(1, crate::hls::worker::LOW_LATENCY_MAX_LIVE_EDGE);
        tracing::info!("Low latency streaming (HLS live edge: {edge})");
    }

    let recovery = RecoveryRequest::default();
    let (segment_tx, segment_rx) = mpsc::channel(20);
    let (byte_tx, mut byte_rx) = byte_channel(config.stream.ring_buffer_mb as usize * 1024 * 1024);

    let resolver = Arc::new(VariantResolver {
        client: client.clone(),
        api: api.clone(),
        usher: usher.clone(),
        channel: channel.to_string(),
        ad_block: config.twitch.ad_block,
        priority: vec![resolved.variant.normalized_name(), "best".to_string()],
    });

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
        resolver,
    );

    let fetcher = SegmentFetcher::new(
        client.clone(),
        FetcherConfig {
            threads: config.stream.segment_threads,
            attempts: config.stream.segment_attempts,
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
    println!(
        "selected: {} ({})",
        resolved.variant.name, resolved.variant.uri
    );

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

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(cli.config.as_deref())?;
    let level = cli.log_level.as_deref().unwrap_or(&config.log.level);
    init_logging(level);

    let channel = parse_channel(&cli.channel)?;

    if let Some(port) = cli.serve {
        let buffer_dir = cli
            .buffer_dir
            .unwrap_or_else(|| std::env::temp_dir().join("twitchpipe"));
        let session = Session::start(SessionConfig {
            channel,
            quality: cli.quality.clone().unwrap_or_else(|| {
                config
                    .quality
                    .priority
                    .first()
                    .cloned()
                    .unwrap_or("best".into())
            }),
            buffer_dir: buffer_dir.to_string_lossy().into_owned(),
            buffer_cap_bytes: cli.buffer_cap_bytes,
            adblock: AdblockConfig {
                enabled: config.twitch.ad_block,
                backup_player_types: true,
                delayed_playback: true,
            },
            recovery_timeout_sec: config.stream.recovery_timeout.max(0.0) as u64,
            port,
        })
        .await?;
        println!("{}", session.local_url());
        tracing::info!("Local HLS ready at {}", session.local_url());
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = tokio::time::sleep(Duration::from_millis(250)) => {
                    while let Some(event) = session.poll_event(Duration::ZERO) {
                        tracing::info!("{event}");
                        if event == r#"{"type":"ended"}"# {
                            session.stop().await;
                            return Ok(());
                        }
                    }
                }
            }
        }
        session.stop().await;
        return Ok(());
    }

    let client = build_http_client(&config)?;
    let api = TwitchApi::new(client.clone(), &config.twitch.api_headers)?;
    let usher = UsherService::new(&config.twitch.supported_codecs);

    if cli.streams {
        let resolved = resolve_stream(&client, &api, &usher, &config, &channel, None, true).await?;
        let Some(resolved) = resolved else {
            let err = anyhow::anyhow!("no streams found on channel {channel}");
            tracing::error!("{err}");
            return Err(err);
        };
        for variant in resolved.variants {
            println!("{}", variant.name);
        }
        return Ok(());
    }

    // Metadata (best effort, used for the player title and live check logs)
    // is fetched concurrently with the stream resolution to save a round trip
    // on startup.
    let (metadata, resolved) = tokio::join!(
        api.metadata_channel(&channel),
        wait_for_stream(
            &client,
            &api,
            &usher,
            &config,
            &channel,
            cli.quality.as_deref(),
        ),
    );
    let resolved = resolved?;

    let metadata = match metadata {
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

    if cli.check {
        return run_check(&client, &config, &channel, &resolved).await;
    }

    // retry.open: attempts to open (start) the selected stream.
    let attempts = config.retry.open.max(1);
    let mut resolved = resolved;
    let mut last_err = None;
    for attempt in 1..=attempts {
        if attempt > 1 {
            tracing::warn!("Retrying stream open (attempt {attempt}/{attempts})...");
            tokio::time::sleep(Duration::from_secs(1)).await;
            // The playlist URL may have expired while the stream was failing:
            // re-resolve a fresh one, keeping the old on failure.
            match resolve_stream(
                &client,
                &api,
                &usher,
                &config,
                &channel,
                cli.quality.as_deref(),
                false,
            )
            .await
            {
                Ok(Some(fresh)) => resolved = fresh,
                Ok(None) => tracing::debug!("Channel appears offline while retrying"),
                Err(err) => tracing::debug!("Re-resolve failed: {err:#}"),
            }
        }
        match run_stream(
            &client,
            &api,
            &usher,
            &config,
            &channel,
            &resolved,
            title.as_deref(),
        )
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
