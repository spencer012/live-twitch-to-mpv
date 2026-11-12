//! Headless recorder mode (`--record`): write the stream's MPEG-TS to a file
//! or stdout and report exactly what was written as JSON Lines events.
//!
//! The recorder reuses the player pipeline unchanged (worker -> fetcher ->
//! byte buffer) with instrumentation switched on, and replaces the player
//! with an output writer. No player is spawned and stdin is never read.
//! Logs go to stderr; the `--events` file is the machine-readable contract.

pub mod control;
pub mod events;
pub mod ring;
pub mod tracker;

use std::collections::{HashMap, VecDeque};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
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

use control::{ControlMsg, ControlRx, ControlTx, Opened, SwitchRequest};
use events::{
    End, Event, EventLog, OutputSwitchFailed, OutputSwitched, Start, SwitchFrom, SwitchTo,
};
use ring::Ring;
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
pub const DEFAULT_RING_KEEP_S: f64 = 300.0;
pub const DEFAULT_RING_CHUNK_S: f64 = 60.0;
pub const DEFAULT_CONTROL_OPEN_TIMEOUT_S: f64 = 120.0;

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

    /// Ring mode: instead of --output, write segment-aligned chunk files
    /// into DIR and delete the oldest beyond --ring-keep-s (needs --events)
    #[arg(long, value_name = "DIR", requires = "record", conflicts_with_all = ["output", "append"])]
    pub ring_dir: Option<PathBuf>,

    /// Ring mode: keep at least this many seconds of media [default: 300]
    #[arg(long, value_name = "SECS", requires = "ring_dir")]
    pub ring_keep_s: Option<f64>,

    /// Ring mode: close a chunk at the first segment end after it holds this
    /// many seconds [default: 60]
    #[arg(long, value_name = "SECS", requires = "ring_dir")]
    pub ring_chunk_s: Option<f64>,

    /// Watch this file for output switch requests (JSON, see the README)
    #[arg(long, value_name = "PATH", requires = "record")]
    pub control: Option<PathBuf>,

    /// Give up on a switch whose new files are not open after this many
    /// seconds [default: 120]
    #[arg(long, value_name = "SECS", requires = "control")]
    pub control_open_timeout: Option<f64>,

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

    pub async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        self.writer
            .write_all(data)
            .await
            .map_err(|e| format!("failed to write output: {e}"))
    }

    pub async fn flush(&mut self) -> Result<(), String> {
        self.writer
            .flush()
            .await
            .map_err(|e| format!("failed to write output: {e}"))
    }
}

/// Where the recorder writes: one file (or stdout), or a ring of chunks.
pub enum Sink {
    Plain { out: Output, path: PathBuf },
    Ring(Box<Ring>),
}

impl Sink {
    pub fn plain(out: Output, path: impl Into<PathBuf>) -> Self {
        Sink::Plain {
            out,
            path: path.into(),
        }
    }

    /// The output file, or the ring directory.
    pub fn path(&self) -> &Path {
        match self {
            Sink::Plain { path, .. } => path,
            Sink::Ring(ring) => ring.dir(),
        }
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        match self {
            Sink::Plain { out, .. } => out.write(data).await,
            Sink::Ring(ring) => ring.write(data).await,
        }
    }

    async fn flush(&mut self) -> Result<(), String> {
        match self {
            Sink::Plain { out, .. } => out.flush().await,
            Sink::Ring(ring) => ring.flush().await,
        }
    }

    fn set_head_init(&mut self, len: u64) {
        if let Sink::Ring(ring) = self {
            ring.set_head_init(len);
        }
    }
}

/// What the session knows about fMP4 init sections.
#[derive(Default)]
struct InitState {
    /// The last init section the fetcher wrote.
    current: Option<Bytes>,
    /// The next slot brings its own init (this long).
    next_slot: Option<u64>,
    /// The output is new: its first slot is its head.
    fresh: bool,
    /// ...and the current init must be written at that head.
    inject: bool,
}

/// Output switch requests in flight.
struct Control {
    tx: ControlTx,
    rx: ControlRx,
    open_timeout: Duration,
    /// Opening the request's files.
    pending: Option<Pending>,
    /// Files open, waiting for a segment boundary.
    ready: Option<(SwitchRequest, Opened)>,
    /// Requests that arrived while another one was in flight.
    queue: VecDeque<SwitchRequest>,
    /// Each finished request id's acknowledgement (repeated on request).
    outcomes: HashMap<String, Event>,
}

struct Pending {
    req: SwitchRequest,
    deadline: Instant,
    abandoned: Arc<AtomicBool>,
}

impl Control {
    fn busy(&self) -> bool {
        self.pending.is_some() || self.ready.is_some()
    }

    fn in_flight(&self, id: &str) -> bool {
        self.pending.as_ref().is_some_and(|p| p.req.id == id)
            || self.ready.as_ref().is_some_and(|(r, _)| r.id == id)
            || self.queue.iter().any(|r| r.id == id)
    }
}

async fn recv_control(ctl: &mut Option<Control>) -> ControlMsg {
    match ctl {
        // the control holds a sender itself, so this never ends
        Some(ctl) => match ctl.rx.recv().await {
            Some(msg) => msg,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}

/// Close files without blocking the recorder (a hung mount blocks only this
/// thread).
fn close_in_background<T: Send + 'static>(what: T) {
    let _ = std::thread::Builder::new()
        .name("twitchpipe-close".into())
        .spawn(move || drop(what));
}

fn mark_repeat(event: &Event) -> Event {
    let mut event = event.clone();
    match &mut event {
        Event::OutputSwitched(e) => e.repeat = true,
        Event::OutputSwitchFailed(e) => e.repeat = true,
        _ => {}
    }
    event
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
    /// The run's `start` event (the head of every switched events file).
    start: Option<Start>,
    /// The current events file ("-" = stdout), for reports.
    events_path: Option<PathBuf>,
    init: InitState,
    ctl: Option<Control>,
}

impl Session {
    pub fn new(log: EventLog, start_offset: u64) -> Self {
        Self {
            log,
            tracker: Tracker::new(start_offset),
            events_error: None,
            start: None,
            events_path: None,
            init: InitState {
                fresh: true,
                ..Default::default()
            },
            ctl: None,
        }
    }

    pub fn set_events_path(&mut self, path: Option<PathBuf>) {
        self.events_path = path;
    }

    /// Accept output switch requests; returns the sender they arrive on
    /// (the control file watcher, or a test).
    pub fn enable_control(&mut self, open_timeout: Duration) -> ControlTx {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.ctl = Some(Control {
            tx: tx.clone(),
            rx,
            open_timeout,
            pending: None,
            ready: None,
            queue: VecDeque::new(),
            outcomes: HashMap::new(),
        });
        tx
    }

    fn switch_busy(&self) -> bool {
        self.ctl.as_ref().is_some_and(Control::busy)
    }

    fn emit(&mut self, event: Event) {
        if let Event::Start(start) = &event {
            self.start = Some((**start).clone());
        }
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

    /// Emit tracker events; a ring also counts the segments per chunk.
    fn emit_tracked(&mut self, events: Vec<Event>, out: &mut Sink) {
        if let Sink::Ring(ring) = out {
            for event in &events {
                if let Event::Segment(seg) = event {
                    ring.on_segment(seg);
                }
            }
        }
        self.emit_all(events);
    }

    fn handle_marker(&mut self, marker: SlotMarker, out: &mut Sink) {
        let events = self.tracker.on_marker(marker);
        self.emit_tracked(events, out);
    }

    fn handle_pipeline(&mut self, event: PipelineEvent) {
        let events = self.tracker.on_pipeline(event);
        self.emit_all(events);
    }

    /// Write one byte-buffer item to the output and report it.
    async fn handle_item(&mut self, item: Item, out: &mut Sink) -> Result<(), String> {
        match item {
            Item::Data(bytes) => {
                out.write(&bytes).await?;
                self.tracker.on_data(bytes.len());
            }
            Item::Marker(SlotMarker::Init(init)) => {
                self.init.next_slot = Some(init.len() as u64);
                self.init.current = Some(init);
                if let Sink::Ring(ring) = out {
                    ring.set_fmp4();
                }
            }
            Item::Marker(marker @ SlotMarker::Begin(_)) => {
                self.handle_marker(marker, out);
                let own = self.init.next_slot.take();
                if std::mem::take(&mut self.init.fresh) {
                    // the first slot of a new output (or ring chunk): it
                    // starts with an init section, its own or the current one
                    let inject = std::mem::take(&mut self.init.inject);
                    match (own, &self.init.current) {
                        (Some(len), _) => out.set_head_init(len),
                        (None, Some(init)) if inject => {
                            let init = init.clone();
                            out.set_head_init(init.len() as u64);
                            out.write(&init).await?;
                            self.tracker.on_data(init.len());
                        }
                        _ => {}
                    }
                }
            }
            Item::Marker(marker) => {
                let end = matches!(marker, SlotMarker::End { .. });
                // Segment events are only written once their bytes are.
                if end {
                    out.flush().await?;
                }
                self.handle_marker(marker, out);
                if end
                    && let Sink::Ring(ring) = out
                    && ring.should_rotate()
                {
                    self.rotate(ring).await?;
                }
                self.try_switch(out).await;
            }
        }
        Ok(())
    }

    /// Close the ring's chunk at this segment boundary and prune (not while
    /// a switch is in flight: those chunks are about to be wanted).
    async fn rotate(&mut self, ring: &mut Ring) -> Result<(), String> {
        let (closed, flushed) = ring.close_open("rotate").await;
        let index = closed.as_ref().map(|c| c.index);
        if let Some(chunk) = closed {
            self.emit(Event::Chunk(chunk));
        }
        flushed?;
        self.emit(Event::Ring(ring.event("rotate", index, Vec::new())));
        self.init.fresh = true;
        self.init.inject = true;
        if !self.switch_busy() {
            let removed = ring.prune();
            if !removed.is_empty() {
                self.emit(Event::Ring(ring.event("prune", None, removed)));
            }
        }
        Ok(())
    }

    fn switch_failed(
        &mut self,
        id: Option<String>,
        reason: &'static str,
        message: String,
        req: Option<&SwitchRequest>,
    ) {
        tracing::warn!("output switch {id:?} failed ({reason}): {message}");
        let event = Event::OutputSwitchFailed(OutputSwitchFailed {
            request_id: id.clone(),
            repeat: false,
            reason,
            message,
            output: req.map(|r| r.output.display().to_string()),
            events: req.map(|r| r.events.display().to_string()),
        });
        self.emit(event.clone());
        if let (Some(id), Some(ctl)) = (id, &mut self.ctl) {
            ctl.outcomes.insert(id, event);
        }
    }

    /// Start opening a request's files. False if it was refused.
    fn start_request(&mut self, req: SwitchRequest, out: &Sink) -> bool {
        let mut current = vec![out.path()];
        if let Some(events) = &self.events_path {
            current.push(events);
        }
        if let Err(message) = control::validate(&req, &current) {
            self.switch_failed(Some(req.id.clone()), "invalid", message, Some(&req));
            return false;
        }
        tracing::info!(
            "Output switch {} requested: {}",
            req.id,
            req.output.display()
        );
        self.emit(Event::OutputSwitchRequested {
            request_id: req.id.clone(),
            output: req.output.display().to_string(),
            events: req.events.display().to_string(),
        });
        let ctl = self.ctl.as_mut().expect("control is enabled");
        let abandoned = Arc::new(AtomicBool::new(false));
        control::spawn_open(&req, ctl.tx.clone(), abandoned.clone());
        ctl.pending = Some(Pending {
            req,
            deadline: Instant::now() + ctl.open_timeout,
            abandoned,
        });
        true
    }

    /// The request in flight finished: start the next queued one.
    fn next_request(&mut self, out: &Sink) {
        loop {
            let Some(ctl) = &mut self.ctl else { return };
            if ctl.busy() {
                return;
            }
            let Some(req) = ctl.queue.pop_front() else {
                return;
            };
            if self.start_request(req, out) {
                return;
            }
        }
    }

    async fn on_control(&mut self, msg: ControlMsg, out: &mut Sink) {
        let Some(ctl) = &mut self.ctl else { return };
        match msg {
            ControlMsg::Request(req) => {
                if let Some(done) = ctl.outcomes.get(&req.id) {
                    let event = mark_repeat(done);
                    self.emit(event);
                } else if ctl.in_flight(&req.id) {
                    // already being worked on
                } else if ctl.busy() {
                    ctl.queue.push_back(req);
                } else {
                    self.start_request(req, out);
                }
            }
            ControlMsg::Invalid { id, message } => match id {
                Some(id) if ctl.outcomes.contains_key(&id) => {
                    let event = mark_repeat(&ctl.outcomes[&id]);
                    self.emit(event);
                }
                Some(id) if ctl.in_flight(&id) => {}
                Some(id) => self.switch_failed(Some(id), "invalid", message, None),
                None => self.emit(Event::warning("control_invalid", message)),
            },
            ControlMsg::Opened { id, result } => {
                match ctl.pending.take_if(|p| p.req.id == id) {
                    Some(pending) => match result {
                        Ok(opened) => {
                            ctl.ready = Some((pending.req, opened));
                            self.try_switch(out).await;
                        }
                        Err(message) => {
                            self.switch_failed(
                                Some(id),
                                "open_failed",
                                message,
                                Some(&pending.req),
                            );
                            self.next_request(out);
                        }
                    },
                    // it timed out meanwhile
                    None => {
                        if let Ok(opened) = result {
                            close_in_background(opened);
                        }
                    }
                }
            }
        }
    }

    fn on_open_timeout(&mut self, out: &Sink) {
        let Some(ctl) = &mut self.ctl else { return };
        let Some(pending) = ctl.pending.take() else {
            return;
        };
        pending.abandoned.store(true, Ordering::SeqCst);
        let message = format!(
            "opening {} and {} did not finish within {:.0}s",
            pending.req.output.display(),
            pending.req.events.display(),
            ctl.open_timeout.as_secs_f64()
        );
        self.switch_failed(
            Some(pending.req.id.clone()),
            "open_timeout",
            message,
            Some(&pending.req),
        );
        self.next_request(out);
    }

    /// Switch to the ready output if this is a segment boundary.
    async fn try_switch(&mut self, out: &mut Sink) {
        if self.tracker.is_open() {
            return;
        }
        let Some(ctl) = &mut self.ctl else { return };
        let Some((req, opened)) = ctl.ready.take() else {
            return;
        };
        self.switch(out, req, opened).await;
        self.next_request(out);
    }

    /// At a segment boundary: close the current output (or the ring's open
    /// chunk), continue in the new files, and acknowledge in both events
    /// files.
    async fn switch(&mut self, out: &mut Sink, req: SwitchRequest, opened: Opened) {
        let gap = self.tracker.flush_pending_gap();
        self.emit_tracked(gap, out);
        let end_offset = self.tracker.output_offset();
        let (chunks, error) = match out {
            Sink::Plain { out, .. } => (None, out.flush().await.err()),
            Sink::Ring(ring) => {
                let (closed, flushed) = ring.close_open("switch").await;
                if let Some(chunk) = closed {
                    self.emit(Event::Chunk(chunk));
                }
                (Some(ring.held()), flushed.err())
            }
        };
        let old_totals = self.tracker.switch_output();
        let last = self.tracker.last_segment().cloned();
        let switched = Event::OutputSwitched(Box::new(OutputSwitched {
            request_id: req.id.clone(),
            repeat: false,
            last_slot_sequence: self.tracker.last_slot(),
            last_sequence: last.as_ref().and_then(|l| l.sequence),
            next_slot_sequence: self.tracker.last_slot().map(|s| s + 1),
            program_date_time: last.and_then(|l| l.end_program_date_time),
            init: self.init.current.is_some(),
            from: SwitchFrom {
                output: out.path().display().to_string(),
                events: self.events_path.as_ref().map(|p| p.display().to_string()),
                end_offset,
                segments: old_totals.segments,
                bytes: old_totals.bytes,
                chunks,
                error,
            },
            to: SwitchTo {
                output: req.output.display().to_string(),
                events: req.events.display().to_string(),
                offset: 0,
            },
        }));
        self.emit(switched.clone());

        let new_out = Output::from_writer(Box::new(tokio::fs::File::from_std(opened.output)));
        let old_out = std::mem::replace(out, Sink::plain(new_out, req.output.clone()));
        let old_log = std::mem::replace(&mut self.log, EventLog::from_file(opened.events));
        // a ring's held chunks stay on disk: dropping it deletes nothing
        close_in_background((old_out, old_log));
        self.events_error = None;
        self.events_path = Some(req.events.clone());
        tracing::info!(
            "Output switched ({}): now writing {}",
            req.id,
            req.output.display()
        );

        if let Some(start) = &self.start {
            let mut start = start.clone();
            start.output = req.output.display().to_string();
            start.output_offset = 0;
            start.ring = None;
            start.request_id = Some(req.id.clone());
            self.emit(Event::Start(Box::new(start)));
        }
        self.emit(switched.clone());
        self.init.fresh = true;
        self.init.inject = true;
        if let Some(ctl) = &mut self.ctl {
            ctl.outcomes.insert(req.id, switched);
        }
    }

    /// The recording is over: report switches that never happened and close
    /// the ring's last chunk.
    pub async fn close(&mut self, out: &mut Sink) {
        if let Some(ctl) = &mut self.ctl {
            let mut left: Vec<SwitchRequest> = Vec::new();
            if let Some(pending) = ctl.pending.take() {
                pending.abandoned.store(true, Ordering::SeqCst);
                left.push(pending.req);
            }
            if let Some((req, opened)) = ctl.ready.take() {
                close_in_background(opened);
                left.push(req);
            }
            left.extend(ctl.queue.drain(..));
            for req in left {
                self.switch_failed(
                    Some(req.id.clone()),
                    "recording_ended",
                    "the recording ended before the switch".into(),
                    Some(&req),
                );
            }
        }
        if let Sink::Ring(ring) = out {
            let (closed, flushed) = ring.close_open("end").await;
            if let Some(chunk) = closed {
                self.emit(Event::Chunk(chunk));
            }
            if let Err(err) = flushed {
                tracing::error!("{err}");
            }
        }
    }

    /// Record from one running pipeline until it drains, stalls, fails to
    /// write, or a signal arrives.
    pub async fn run_attempt(
        &mut self,
        pipeline: Pipeline,
        mut pipe_rx: EventRx,
        out: &mut Sink,
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
        // a switch that became ready after the last attempt ended
        self.try_switch(out).await;

        let end = loop {
            if let Some(err) = &self.events_error {
                break AttemptEnd::Output(err.clone());
            }
            let open_deadline = self
                .ctl
                .as_ref()
                .and_then(|c| c.pending.as_ref())
                .map(|p| p.deadline);
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
                msg = recv_control(&mut self.ctl) => self.on_control(msg, out).await,
                _ = tokio::time::sleep_until(open_deadline.unwrap_or_else(Instant::now)), if open_deadline.is_some() => {
                    self.on_open_timeout(out);
                }
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
        self.emit_tracked(events, out);
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
) -> Start {
    let v = &resolved.variant;
    Start {
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
        ring: None,
        control: None,
        request_id: None,
    }
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
    if !cli.check && args.output.is_none() && args.ring_dir.is_none() {
        tracing::error!("--record requires --output <PATH|-> or --ring-dir <DIR>");
        return EXIT_USAGE;
    }
    if args.ring_dir.is_none() && (args.ring_keep_s.is_some() || args.ring_chunk_s.is_some()) {
        tracing::error!("--ring-keep-s and --ring-chunk-s need --ring-dir");
        return EXIT_USAGE;
    }
    if !cli.check && args.ring_dir.is_some() && args.events.is_none() {
        tracing::error!("--ring-dir requires --events (chunks are reported there)");
        return EXIT_USAGE;
    }
    for (flag, value) in [
        ("--ring-keep-s", args.ring_keep_s),
        ("--ring-chunk-s", args.ring_chunk_s),
        ("--control-open-timeout", args.control_open_timeout),
    ] {
        if value.is_some_and(|v| !v.is_finite() || v < 0.0) {
            tracing::error!("{flag} must be a number of seconds >= 0");
            return EXIT_USAGE;
        }
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
    session.set_events_path(args.events.clone());

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
        && args.ring_dir.is_none()
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

    let (mut out, offset) = if let Some(dir) = &args.ring_dir {
        let keep_s = args.ring_keep_s.unwrap_or(DEFAULT_RING_KEEP_S);
        let chunk_s = args.ring_chunk_s.unwrap_or(DEFAULT_RING_CHUNK_S);
        match Ring::create(dir, keep_s, chunk_s) {
            Ok(ring) => (Sink::Ring(Box::new(ring)), 0),
            Err(err) => {
                return session.finish(Finish::output(format!(
                    "cannot use ring directory {}: {err}",
                    dir.display()
                )));
            }
        }
    } else {
        let output_path = args.output.clone().expect("checked above");
        match Output::open(&output_path, args.append) {
            Ok((out, offset)) => (Sink::plain(out, output_path), offset),
            Err(err) => {
                return session.finish(Finish::output(format!(
                    "cannot open output {}: {err}",
                    output_path.display()
                )));
            }
        }
    };
    session.tracker = Tracker::new(offset);
    let mut start = start_event(
        &config,
        &channel,
        &resolved,
        metadata.as_ref(),
        out.path(),
        offset,
    );
    if let Sink::Ring(ring) = &out {
        start.ring = Some(ring.config());
    }
    start.control = args.control.as_ref().map(|p| p.display().to_string());
    session.emit(Event::Start(Box::new(start)));

    if let Some(path) = &args.control {
        let timeout = args
            .control_open_timeout
            .unwrap_or(DEFAULT_CONTROL_OPEN_TIMEOUT_S);
        let tx = session.enable_control(Duration::from_secs_f64(timeout));
        if let Err(err) = control::spawn_watcher(path.clone(), tx) {
            return session.finish(Finish::new(
                "error",
                EXIT_ERROR,
                Some(format!("cannot watch the control file: {err}")),
            ));
        }
    }

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
    session.close(&mut out).await;
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
    out: &mut Sink,
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

    finish_for(&end, session.tracker.lifetime_segments(), stream_timeout)
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
