//! Headless recorder mode (`--record`): write the stream's MPEG-TS to a file
//! or stdout and report exactly what was written as JSON Lines events.
//!
//! The recorder reuses the player pipeline unchanged (worker -> fetcher ->
//! byte buffer) with instrumentation switched on, and replaces the player
//! with an output writer. No player is spawned and stdin is never read.
//! Logs go to stderr; the `--events` file is the machine-readable contract.

pub mod events;
pub mod tracker;

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Args;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::buffer::Item;
use crate::config::Config;
use crate::hls::events::{EventRx, PipelineEvent, SlotMarker, redact_urls};
use crate::hls::worker::WorkerExit;
use crate::twitch::api::{ChannelMetadata, TwitchApi};
use crate::twitch::usher::UsherService;
use crate::{Cli, Pipeline, ResolvedStream, StartError};

use events::{End, Event, EventLog, Start};
use tracker::Tracker;

/// Exit codes (documented in the README).
pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 1;
pub const EXIT_USAGE: i32 = 2;
pub const EXIT_OFFLINE: i32 = 3;
pub const EXIT_AUTH: i32 = 4;
pub const EXIT_NO_QUALITY: i32 = 5;
pub const EXIT_OUTPUT: i32 = 6;
pub const EXIT_STREAM: i32 = 7;
/// Signals exit with 128 + signal number (130 = SIGINT, 143 = SIGTERM).
pub const EXIT_SIGNAL_BASE: i32 = 128;

const SIGINT: i32 = 2;
const SIGTERM: i32 = 15;
/// Max time spent writing already-buffered data after a signal.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(2);
/// Hard exit if a graceful shutdown hangs (e.g. a blocked stdout pipe).
const SHUTDOWN_WATCHDOG: Duration = Duration::from_secs(6);
const OUTPUT_BUFFER: usize = 256 * 1024;

#[derive(Args, Debug, Default, Clone, PartialEq)]
#[command(next_help_heading = "Recorder mode")]
pub struct RecordArgs {
    /// Headless recorder mode: write MPEG-TS to --output instead of starting
    /// the player
    #[arg(long)]
    pub record: bool,

    /// Recorder output file ("-" = stdout). Must be new or empty unless
    /// --append is given
    #[arg(long, value_name = "PATH|-", requires = "record")]
    pub output: Option<PathBuf>,

    /// Write JSON Lines recorder events to this file ("-" = stdout)
    #[arg(long, value_name = "PATH|-", requires = "record")]
    pub events: Option<PathBuf>,

    /// Append to existing non-empty --output/--events files instead of
    /// refusing them
    #[arg(long, requires = "record")]
    pub append: bool,
}

/// How the recorder finished.
#[derive(Debug, Clone, PartialEq)]
pub struct Finish {
    pub reason: &'static str,
    pub code: i32,
    pub signal: Option<&'static str>,
    pub error: Option<String>,
}

impl Finish {
    fn new(reason: &'static str, code: i32, error: Option<String>) -> Self {
        Self {
            reason,
            code,
            signal: None,
            error,
        }
    }

    fn signal(sig: i32) -> Self {
        Self {
            reason: "signal",
            code: EXIT_SIGNAL_BASE + sig,
            signal: Some(signal_name(sig)),
            error: None,
        }
    }

    fn output(err: impl std::fmt::Display) -> Self {
        Self::new("output_error", EXIT_OUTPUT, Some(err.to_string()))
    }
}

fn signal_name(sig: i32) -> &'static str {
    match sig {
        SIGINT => "SIGINT",
        SIGTERM => "SIGTERM",
        _ => "signal",
    }
}

/// Map a start-up error (stream resolution) to a finish reason/exit code.
fn classify_start_error(err: &anyhow::Error) -> Finish {
    let message = redact_urls(&format!("{err:#}"));
    match err.downcast_ref::<StartError>() {
        Some(StartError::Offline(_)) => Finish::new("offline", EXIT_OFFLINE, Some(message)),
        Some(StartError::Auth(_)) => Finish::new("auth_failed", EXIT_AUTH, Some(message)),
        Some(StartError::NoQuality(_)) => {
            Finish::new("no_matching_quality", EXIT_NO_QUALITY, Some(message))
        }
        None => Finish::new("stream_error", EXIT_STREAM, Some(message)),
    }
}

/// SIGTERM/SIGINT (Ctrl+C elsewhere) as a cloneable future.
pub struct Shutdown {
    rx: watch::Receiver<Option<i32>>,
}

impl Shutdown {
    /// Register the signal handlers now (so a signal during start-up is
    /// handled gracefully) and start listening in the background.
    pub fn install() -> std::io::Result<Self> {
        let (tx, rx) = watch::channel(None);
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = signal(SignalKind::terminate())?;
            let mut int = signal(SignalKind::interrupt())?;
            tokio::spawn(async move {
                let sig = tokio::select! {
                    _ = term.recv() => SIGTERM,
                    _ = int.recv() => SIGINT,
                };
                on_signal(&tx, sig);
                // Keep the handlers registered; repeat signals are ignored.
                loop {
                    tokio::select! {
                        _ = term.recv() => {}
                        _ = int.recv() => {}
                    }
                }
            });
        }
        #[cfg(not(unix))]
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                on_signal(&tx, SIGINT);
                std::future::pending::<()>().await;
            }
        });
        Ok(Self { rx })
    }

    #[cfg(test)]
    pub fn manual() -> (watch::Sender<Option<i32>>, Self) {
        let (tx, rx) = watch::channel(None);
        (tx, Self { rx })
    }

    /// Resolves with the signal number once a signal arrived (immediately if
    /// one already did).
    pub async fn wait(&mut self) -> i32 {
        loop {
            if let Some(sig) = *self.rx.borrow_and_update() {
                return sig;
            }
            if self.rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

fn on_signal(tx: &watch::Sender<Option<i32>>, sig: i32) {
    tracing::info!("Received {}, stopping recording", signal_name(sig));
    let _ = tx.send(Some(sig));
    std::thread::spawn(move || {
        std::thread::sleep(SHUTDOWN_WATCHDOG);
        eprintln!("twitchpipe: graceful shutdown took too long, exiting");
        std::process::exit(EXIT_SIGNAL_BASE + sig);
    });
}

/// Buffered MPEG-TS output (file or stdout).
pub struct Output {
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
}

impl Output {
    /// Open the output; returns it with the offset of the first byte written.
    fn open(path: &Path, append: bool) -> std::io::Result<(Self, u64)> {
        if path == Path::new("-") {
            return Ok((Self::from_writer(Box::new(tokio::io::stdout())), 0));
        }
        let (file, offset) = events::open_new_or_append(path, append)?;
        Ok((
            Self::from_writer(Box::new(tokio::fs::File::from_std(file))),
            offset,
        ))
    }

    pub fn from_writer(writer: Box<dyn AsyncWrite + Unpin + Send>) -> Self {
        Self {
            writer: BufWriter::with_capacity(OUTPUT_BUFFER, writer),
        }
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        self.writer
            .write_all(data)
            .await
            .map_err(|e| format!("failed to write output: {e}"))
    }

    async fn flush(&mut self) -> Result<(), String> {
        self.writer
            .flush()
            .await
            .map_err(|e| format!("failed to write output: {e}"))
    }
}

/// How one pipeline run (one stream open attempt) ended.
#[derive(Debug, Clone, PartialEq)]
pub enum AttemptEnd {
    /// The pipeline drained: the worker stopped for this reason.
    Stream(WorkerExit),
    /// No progress within the stream timeout.
    Timeout,
    Signal(i32),
    Output(String),
}

/// Events + tracker state for one recorder run.
pub struct Session {
    log: EventLog,
    pub tracker: Tracker,
    events_error: Option<String>,
}

impl Session {
    pub fn new(log: EventLog, start_offset: u64) -> Self {
        Self {
            log,
            tracker: Tracker::new(start_offset),
            events_error: None,
        }
    }

    fn emit(&mut self, event: Event) {
        log_event(&event);
        if self.events_error.is_some() {
            return;
        }
        if let Err(err) = self.log.write(&event) {
            let message = format!("failed to write events: {err}");
            tracing::error!("{message}");
            self.events_error = Some(message);
        }
    }

    fn emit_all(&mut self, events: Vec<Event>) {
        for event in events {
            self.emit(event);
        }
    }

    fn handle_marker(&mut self, marker: SlotMarker) {
        let events = self.tracker.on_marker(marker);
        self.emit_all(events);
    }

    fn handle_pipeline(&mut self, event: PipelineEvent) {
        let events = self.tracker.on_pipeline(event);
        self.emit_all(events);
    }

    /// Write one byte-buffer item to the output and report it.
    async fn handle_item(&mut self, item: Item, out: &mut Output) -> Result<(), String> {
        match item {
            Item::Data(bytes) => {
                out.write(&bytes).await?;
                self.tracker.on_data(bytes.len());
            }
            Item::Marker(marker) => {
                // Segment events are only written once their bytes are.
                if matches!(marker, SlotMarker::End { .. }) {
                    out.flush().await?;
                }
                self.handle_marker(marker);
            }
        }
        Ok(())
    }

    /// Record from one running pipeline until it drains, stalls, fails to
    /// write, or a signal arrives.
    pub async fn run_attempt(
        &mut self,
        pipeline: Pipeline,
        mut pipe_rx: EventRx,
        out: &mut Output,
        stream_timeout: Duration,
        shutdown: &mut Shutdown,
    ) -> AttemptEnd {
        let Pipeline {
            worker,
            fetcher,
            mut bytes,
        } = pipeline;

        // Progress = any data or marker. Suspended while persist/recovery
        // waits for the stream to come back (recovery_timeout governs that).
        let mut deadline = Instant::now() + stream_timeout;
        let mut recovering = false;

        let end = loop {
            if let Some(err) = &self.events_error {
                break AttemptEnd::Output(err.clone());
            }
            tokio::select! {
                biased;
                sig = shutdown.wait() => break AttemptEnd::Signal(sig),
                Some(event) = pipe_rx.recv() => {
                    match &event {
                        PipelineEvent::StreamDown { .. } => recovering = true,
                        PipelineEvent::StreamRecovered { .. } => {
                            recovering = false;
                            deadline = Instant::now() + stream_timeout;
                        }
                        _ => {}
                    }
                    self.handle_pipeline(event);
                }
                item = bytes.recv_item() => {
                    let Some(item) = item else {
                        break AttemptEnd::Stream(WorkerExit::OutputClosed);
                    };
                    deadline = Instant::now() + stream_timeout;
                    if let Err(err) = self.handle_item(item, out).await {
                        break AttemptEnd::Output(err);
                    }
                }
                _ = tokio::time::sleep_until(deadline), if !recovering => break AttemptEnd::Timeout,
            }
        };

        let end = match end {
            AttemptEnd::Stream(_) => {
                // The buffer closed, so the fetcher and worker are done.
                let _ = fetcher.await;
                let exit = match tokio::time::timeout(Duration::from_secs(5), worker).await {
                    Ok(Ok(exit)) => exit,
                    _ => WorkerExit::OutputClosed,
                };
                AttemptEnd::Stream(exit)
            }
            other => {
                // Stop fetching. Aborting the fetcher drops the buffer's
                // sender, so draining below terminates.
                worker.abort();
                fetcher.abort();
                let _ = fetcher.await;
                let _ = worker.await;
                if !matches!(other, AttemptEnd::Output(_)) {
                    let drain_until = Instant::now() + SHUTDOWN_DRAIN;
                    while let Ok(Some(item)) =
                        tokio::time::timeout_at(drain_until, bytes.recv_item()).await
                    {
                        if self.handle_item(item, out).await.is_err() {
                            break;
                        }
                    }
                }
                other
            }
        };

        while let Ok(event) = pipe_rx.try_recv() {
            self.handle_pipeline(event);
        }
        let reason = match &end {
            AttemptEnd::Signal(_) => "interrupted by signal",
            AttemptEnd::Timeout => "stream timeout",
            AttemptEnd::Output(_) => "output error",
            AttemptEnd::Stream(_) => "stream ended mid-segment",
        };
        let events = self.tracker.finish(reason);
        self.emit_all(events);
        if let Err(err) = out.flush().await
            && !matches!(end, AttemptEnd::Output(_))
        {
            return AttemptEnd::Output(err);
        }
        end
    }

    /// Write the final `error` (if any) and `end` events; returns the exit code.
    fn finish(&mut self, finish: Finish) -> i32 {
        if let Some(message) = &finish.error {
            tracing::error!("{message}");
            self.emit(Event::error(finish.reason, message.clone()));
        }
        let totals = self.tracker.totals.clone();
        tracing::info!(
            "Recording finished: {} (exit code {}), {} segments, {} bytes, {} gaps",
            finish.reason,
            finish.code,
            totals.segments,
            totals.bytes,
            totals.gaps
        );
        self.emit(Event::End(End {
            reason: finish.reason,
            exit_code: finish.code,
            signal: finish.signal,
            error: finish.error,
            totals,
        }));
        finish.code
    }
}

fn log_event(event: &Event) {
    match event {
        Event::Segment(s) => tracing::debug!(
            "segment seq={:?} slot={} kind={} offset={} length={}",
            s.sequence,
            s.slot_sequence,
            s.kind,
            s.offset,
            s.length
        ),
        Event::Gap(g) => tracing::warn!(
            "gap: slots {}..={} ({} segments, {})",
            g.from_sequence,
            g.to_sequence,
            g.count,
            g.reason
        ),
        Event::Discontinuity(d) => tracing::info!(
            "discontinuity at offset {} ({}: {:?} -> {:?})",
            d.offset,
            d.reason,
            d.previous_sequence,
            d.sequence
        ),
        Event::AdStart { strategy, .. } => tracing::info!("ad break started ({strategy})"),
        Event::AdEnd { strategy, .. } => tracing::info!("ad break ended ({strategy})"),
        Event::StreamDown { reason, .. } => tracing::warn!("stream down ({reason})"),
        Event::StreamRecovered { downtime_s, .. } => {
            tracing::info!("stream recovered after {downtime_s:.1}s")
        }
        Event::Warning { message, .. } => tracing::warn!("{message}"),
        _ => {}
    }
}

fn init_logging(level: &str) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init();
}

fn effective_live_edge(config: &Config) -> usize {
    if config.twitch.low_latency {
        config
            .stream
            .live_edge
            .clamp(1, crate::hls::worker::LOW_LATENCY_MAX_LIVE_EDGE)
    } else {
        config.stream.live_edge.max(1)
    }
}

fn start_event(
    config: &Config,
    channel: &str,
    resolved: &ResolvedStream,
    metadata: Option<&ChannelMetadata>,
    output: &Path,
    output_offset: u64,
) -> Event {
    let v = &resolved.variant;
    Event::Start(Box::new(Start {
        schema: events::SCHEMA_VERSION,
        version: env!("CARGO_PKG_VERSION"),
        pid: std::process::id(),
        channel: channel.to_string(),
        quality: v.name.clone(),
        resolution: v.resolution.map(|(w, h)| format!("{w}x{h}")),
        width: v.resolution.map(|(w, _)| w),
        height: v.resolution.map(|(_, h)| h),
        framerate: v.framerate,
        codecs: v.codecs.clone(),
        bandwidth: v.bandwidth,
        low_latency: config.twitch.low_latency,
        live_edge: effective_live_edge(config),
        ad_block: config.twitch.ad_block,
        persist_stream: config.stream.persist_stream,
        recovery_timeout: config.stream.recovery_timeout,
        segment_threads: config.stream.segment_threads,
        segment_attempts: config.stream.segment_attempts,
        segment_timeout: config.stream.segment_timeout,
        stream_timeout: config.stream.stream_timeout,
        supported_codecs: config.twitch.supported_codecs.clone(),
        output: output.display().to_string(),
        output_offset,
        stream_id: metadata.and_then(|m| m.stream_id.clone()),
        title: metadata.and_then(|m| m.title.clone()),
        category: metadata.and_then(|m| m.game.clone()),
    }))
}

/// Entry point for `--record`. Returns the process exit code.
pub async fn run(cli: Cli) -> i32 {
    let mut config = match Config::load(cli.config.as_deref()) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("twitchpipe: {err:#}");
            return EXIT_USAGE;
        }
    };
    if let Err(err) = cli.overrides.apply(&mut config) {
        eprintln!("twitchpipe: {err:#}");
        return EXIT_USAGE;
    }
    init_logging(cli.log_level.as_deref().unwrap_or(&config.log.level));

    let args = &cli.record;
    let stdout = Path::new("-");
    if !cli.check && args.output.is_none() {
        tracing::error!("--record requires --output <PATH|->");
        return EXIT_USAGE;
    }
    if args.output.as_deref() == Some(stdout) && args.events.as_deref() == Some(stdout) {
        tracing::error!("--output and --events cannot both be stdout");
        return EXIT_USAGE;
    }

    let log = match &args.events {
        Some(path) if !cli.check => match EventLog::open(path, args.append) {
            Ok(log) => log,
            Err(err) => {
                tracing::error!("cannot open events file {}: {err}", path.display());
                return EXIT_OUTPUT;
            }
        },
        _ => EventLog::disabled(),
    };
    let mut session = Session::new(log, 0);

    let mut shutdown = match Shutdown::install() {
        Ok(shutdown) => shutdown,
        Err(err) => {
            return session.finish(Finish::new(
                "error",
                EXIT_ERROR,
                Some(format!("failed to install signal handlers: {err}")),
            ));
        }
    };

    if !cli.check
        && let Some(path) = &args.output
        && let Err(err) = events::check_writable_target(path, args.append)
    {
        return session.finish(Finish::output(format!(
            "cannot write output {}: {err}",
            path.display()
        )));
    }

    let channel = match crate::parse_channel(&cli.channel) {
        Ok(channel) => channel,
        Err(err) => return session.finish(Finish::new("usage", EXIT_USAGE, Some(err.to_string()))),
    };
    let client = match crate::build_http_client() {
        Ok(client) => client,
        Err(err) => {
            return session.finish(Finish::new("error", EXIT_ERROR, Some(format!("{err:#}"))));
        }
    };
    let api = match TwitchApi::new(client.clone(), &config.twitch.api_headers) {
        Ok(api) => api,
        Err(err) => {
            return session.finish(Finish::new("usage", EXIT_USAGE, Some(format!("{err:#}"))));
        }
    };
    let usher = UsherService::new(&config.twitch.supported_codecs);

    let metadata = tokio::select! {
        meta = api.metadata_channel(&channel) => match meta {
            Ok(meta) => {
                if let Some(title) = &meta.title {
                    tracing::info!("Stream title: {title}");
                }
                if !meta.is_live() {
                    tracing::info!("Channel appears to be offline");
                }
                Some(meta)
            }
            Err(err) => {
                tracing::debug!("Failed to fetch channel metadata: {}", redact_urls(&format!("{err:#}")));
                None
            }
        },
        sig = shutdown.wait() => return session.finish(Finish::signal(sig)),
    };

    let resolved = tokio::select! {
        resolved = crate::wait_for_stream(&client, &api, &usher, &config, &channel, cli.quality.as_deref()) => resolved,
        sig = shutdown.wait() => return session.finish(Finish::signal(sig)),
    };
    let resolved = match resolved {
        Ok(resolved) => resolved,
        Err(err) => return session.finish(classify_start_error(&err)),
    };

    if cli.check {
        return match crate::run_check(&client, &config, &channel, &resolved).await {
            Ok(()) => EXIT_OK,
            Err(err) => {
                tracing::error!("{}", redact_urls(&format!("{err:#}")));
                EXIT_STREAM
            }
        };
    }

    let output_path = args.output.clone().expect("checked above");
    let (mut out, offset) = match Output::open(&output_path, args.append) {
        Ok(opened) => opened,
        Err(err) => {
            return session.finish(Finish::output(format!(
                "cannot open output {}: {err}",
                output_path.display()
            )));
        }
    };
    session.tracker = Tracker::new(offset);
    session.emit(start_event(
        &config,
        &channel,
        &resolved,
        metadata.as_ref(),
        &output_path,
        offset,
    ));

    let finish = record(
        &mut session,
        &client,
        &api,
        &usher,
        &config,
        &channel,
        &resolved,
        &mut out,
        &mut shutdown,
    )
    .await;
    session.finish(finish)
}

/// The stream-open retry loop (`retry.open`), recording into `out`.
#[allow(clippy::too_many_arguments)]
async fn record(
    session: &mut Session,
    client: &reqwest::Client,
    api: &TwitchApi,
    usher: &UsherService,
    config: &Config,
    channel: &str,
    resolved: &ResolvedStream,
    out: &mut Output,
    shutdown: &mut Shutdown,
) -> Finish {
    let attempts = config.retry.open.max(1);
    let stream_timeout = Duration::from_secs_f64(config.stream.stream_timeout.max(1.0));
    let mut end = AttemptEnd::Stream(WorkerExit::OutputClosed);

    for attempt in 1..=attempts {
        if attempt > 1 {
            session.emit(Event::warning(
                "retry_open",
                format!("no data received, retrying stream open (attempt {attempt}/{attempts})"),
            ));
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                sig = shutdown.wait() => {
                    end = AttemptEnd::Signal(sig);
                    break;
                }
            }
        }
        session.tracker.begin_attempt(attempt > 1);
        let hook = crate::build_hook(client, api, usher, config, channel, &resolved.variant);
        let (tx, rx) = mpsc::unbounded_channel();
        let pipeline = crate::spawn_pipeline(client, &resolved.variant.uri, config, hook, Some(tx));
        end = session
            .run_attempt(pipeline, rx, out, stream_timeout, shutdown)
            .await;

        let retryable = matches!(end, AttemptEnd::Stream(_) | AttemptEnd::Timeout);
        if !retryable || session.tracker.attempt_segments() > 0 {
            break;
        }
    }

    finish_for(&end, session.tracker.totals.segments, stream_timeout)
}

/// Map how the last attempt ended to the recorder's finish reason.
pub fn finish_for(end: &AttemptEnd, segments: u64, stream_timeout: Duration) -> Finish {
    let no_data = || {
        Finish::new(
            "no_data",
            EXIT_STREAM,
            Some("stream ended before any data was received".into()),
        )
    };
    match end {
        AttemptEnd::Signal(sig) => Finish::signal(*sig),
        AttemptEnd::Output(err) => Finish::output(err),
        AttemptEnd::Timeout => Finish::new(
            "stream_timeout",
            EXIT_STREAM,
            Some(format!(
                "no stream data within {:.0}s (stream timeout)",
                stream_timeout.as_secs_f64()
            )),
        ),
        AttemptEnd::Stream(WorkerExit::LoadFailed(err)) if segments == 0 => Finish::new(
            "stream_error",
            EXIT_STREAM,
            Some(format!("failed to load the media playlist: {err}")),
        ),
        AttemptEnd::Stream(_) if segments == 0 => no_data(),
        AttemptEnd::Stream(WorkerExit::RecoveryTimeout) => {
            Finish::new("recovery_timeout", EXIT_OK, None)
        }
        AttemptEnd::Stream(_) => Finish::new("stream_ended", EXIT_OK, None),
    }
}

#[cfg(test)]
mod tests;
