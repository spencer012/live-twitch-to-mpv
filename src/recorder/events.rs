//! Recorder events: the JSON Lines contract written to `--events`.
//!
//! Every line is one JSON object with `ts` (UTC RFC 3339, milliseconds) and
//! `type`, followed by the event's fields. Lines are flushed one by one. See
//! the README ("Recorder mode") for the schema.

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;

use chrono::{DateTime, FixedOffset, SecondsFormat, TimeZone, Utc};
use serde::Serialize;

/// Bumped on incompatible changes to the events schema.
pub const SCHEMA_VERSION: u32 = 1;

pub fn fmt_time<Tz: TimeZone>(dt: &DateTime<Tz>) -> String {
    dt.with_timezone(&Utc)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn fmt_opt_time(dt: Option<DateTime<FixedOffset>>) -> Option<String> {
    dt.as_ref().map(fmt_time)
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Start {
    pub schema: u32,
    pub version: &'static str,
    pub pid: u32,
    pub channel: String,
    /// Variant (quality) name as advertised by Twitch, e.g. "1080p60 (source)".
    pub quality: String,
    pub resolution: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub framerate: Option<f64>,
    pub codecs: Option<String>,
    pub bandwidth: u64,
    pub low_latency: bool,
    /// Effective live edge (after the low-latency clamp).
    pub live_edge: usize,
    pub ad_block: bool,
    pub persist_stream: bool,
    pub recovery_timeout: f64,
    pub segment_threads: usize,
    pub segment_attempts: u32,
    pub segment_timeout: f64,
    pub stream_timeout: f64,
    pub supported_codecs: Vec<String>,
    /// "-" for stdout.
    pub output: String,
    /// Byte offset of the first byte this run writes (non-zero with --append).
    pub output_offset: u64,
    pub stream_id: Option<String>,
    pub title: Option<String>,
    pub category: Option<String>,
    /// Ring mode (`--ring-dir`): `output` is the ring directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ring: Option<RingConfig>,
    /// The control file watched for output switch requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control: Option<String>,
    /// Set in the `start` of an events file opened by an output switch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RingConfig {
    pub dir: String,
    pub keep_s: f64,
    pub chunk_s: f64,
}

/// One ring chunk file. Its bytes are `offset..offset+length` of the ring's
/// virtual output (all chunks back to back); segment events use the same
/// offsets.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Chunk {
    pub index: u64,
    pub path: String,
    pub offset: u64,
    pub length: u64,
    /// The chunk starts with an fMP4 init section of this many bytes (part
    /// of its first segment); 0 for MPEG-TS.
    pub init_length: u64,
    pub segments: u64,
    /// Sum of the segments' durations (seconds).
    pub duration: f64,
    pub first_slot_sequence: Option<i64>,
    pub last_slot_sequence: Option<i64>,
    pub first_program_date_time: Option<String>,
    pub last_program_date_time: Option<String>,
    /// rotate | switch | end (`chunk` events only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_by: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Ring {
    /// rotate | prune
    pub action: &'static str,
    pub dir: String,
    /// rotate: the chunk just closed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed: Option<u64>,
    /// prune: the chunk files deleted.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<String>,
    /// Indexes of the chunks now on disk (closed ones), oldest first.
    pub chunks: Vec<u64>,
    pub held_bytes: u64,
    /// Media held, including the chunk being written.
    pub held_seconds: f64,
    /// Ring offset of the oldest byte held.
    pub base_offset: u64,
    pub first_slot_sequence: Option<i64>,
}

/// One side of an output switch.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SwitchFrom {
    /// The previous output file, or the ring directory.
    pub output: String,
    pub events: Option<String>,
    /// Offset just past the last byte written there (ring: ring offset).
    pub end_offset: u64,
    pub segments: u64,
    pub bytes: u64,
    /// Ring: the chunk files left on disk, oldest first. Never touched again.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunks: Option<Vec<Chunk>>,
    /// Flushing/closing the previous output failed (its tail may be short).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SwitchTo {
    pub output: String,
    pub events: String,
    /// Offset of the first byte written there (always 0).
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OutputSwitched {
    pub request_id: String,
    /// A repeated request id: this re-acknowledges an earlier switch.
    pub repeat: bool,
    /// The last live slot seen before the boundary (written or skipped) and
    /// the last segment written to the previous output.
    pub last_slot_sequence: Option<i64>,
    pub last_sequence: Option<i64>,
    /// The new output starts with this slot (unless a gap follows).
    pub next_slot_sequence: Option<i64>,
    /// End date of the last segment written to the previous output.
    pub program_date_time: Option<String>,
    /// The new output starts with the current fMP4 init section.
    pub init: bool,
    pub from: SwitchFrom,
    pub to: SwitchTo,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OutputSwitchFailed {
    pub request_id: Option<String>,
    pub repeat: bool,
    /// invalid | open_failed | open_timeout | recording_ended
    pub reason: &'static str,
    pub message: String,
    pub output: Option<String>,
    pub events: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SourceInfo {
    pub strategy: &'static str,
    pub player_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Segment {
    /// Media sequence number of the written content (for `ad_substitute`,
    /// the number in the substitute playlist; may be null if unknown).
    pub sequence: Option<i64>,
    /// Live playlist slot this content was written for.
    pub slot_sequence: i64,
    /// live | prefetch | ad_substitute | ad
    pub kind: &'static str,
    pub program_date_time: Option<String>,
    pub duration: Option<f64>,
    /// True for prefetch segments: date/duration are estimates (see
    /// `segment_timing`).
    pub timing_estimated: bool,
    /// Byte offset in the output where this segment starts.
    pub offset: u64,
    pub length: u64,
    /// False if the download failed midway or the recorder stopped while
    /// writing it.
    pub complete: bool,
    /// A `discontinuity` event precedes this segment.
    pub discontinuity: bool,
    pub source: Option<SourceInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SegmentTiming {
    pub sequence: i64,
    pub offset: u64,
    pub program_date_time: Option<String>,
    pub duration: f64,
    pub estimated_program_date_time: Option<String>,
    pub estimated_duration: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Gap {
    /// Live playlist slots `from_sequence..=to_sequence` were not written.
    pub from_sequence: i64,
    pub to_sequence: i64,
    pub count: i64,
    /// ad_dropped | ad_block_skipped | download_failed | playlist_jump |
    /// stream_recovery | stream_reopen
    pub reason: &'static str,
    /// Output byte offset at which the missing data would have been.
    pub offset: u64,
    /// Date of the first missing slot, if known.
    pub program_date_time: Option<String>,
    /// Sum of the missing slots' durations, if known.
    pub duration: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Discontinuity {
    /// Output byte offset where the new content starts.
    pub offset: u64,
    /// source_change | sequence_jump | hls_discontinuity
    pub reason: &'static str,
    pub previous_sequence: Option<i64>,
    pub sequence: Option<i64>,
    pub previous_kind: &'static str,
    pub kind: &'static str,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Totals {
    pub segments: u64,
    pub bytes: u64,
    pub gaps: u64,
    pub gap_segments: i64,
    pub discontinuities: u64,
    pub ad_substitute_segments: u64,
    pub incomplete_segments: u64,
    /// Sum of the written segments' durations (seconds).
    pub media_duration_s: f64,
    pub first_program_date_time: Option<String>,
    pub last_program_date_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct End {
    pub reason: &'static str,
    pub exit_code: i32,
    pub signal: Option<&'static str>,
    pub error: Option<String>,
    #[serde(flatten)]
    pub totals: Totals,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Start(Box<Start>),
    Segment(Segment),
    SegmentTiming(SegmentTiming),
    Gap(Gap),
    Discontinuity(Discontinuity),
    AdStart {
        strategy: &'static str,
        preroll: bool,
        sequence: i64,
        break_duration: Option<f64>,
    },
    AdEnd {
        strategy: &'static str,
        sequence: i64,
        catching_up: bool,
    },
    AdCatchupComplete {
        strategy: &'static str,
        sequence: i64,
        substituted: usize,
    },
    StreamDown {
        reason: String,
        last_sequence: i64,
    },
    StreamRecovered {
        downtime_s: f64,
        sequence: i64,
    },
    Warning {
        code: &'static str,
        message: String,
    },
    Chunk(Chunk),
    Ring(Ring),
    OutputSwitchRequested {
        request_id: String,
        output: String,
        events: String,
    },
    OutputSwitched(Box<OutputSwitched>),
    OutputSwitchFailed(OutputSwitchFailed),
    Error {
        code: &'static str,
        message: String,
    },
    End(End),
}

impl Event {
    pub fn warning(code: &'static str, message: impl Into<String>) -> Self {
        Event::Warning {
            code,
            message: message.into(),
        }
    }

    pub fn error(code: &'static str, message: impl Into<String>) -> Self {
        Event::Error {
            code,
            message: message.into(),
        }
    }
}

#[derive(Serialize)]
struct Line<'a> {
    ts: String,
    #[serde(flatten)]
    event: &'a Event,
}

/// Serialize one event as a JSON line (with trailing newline).
pub fn to_line(event: &Event, ts: &DateTime<Utc>) -> String {
    let mut line = serde_json::to_string(&Line {
        ts: fmt_time(ts),
        event,
    })
    .expect("events always serialize");
    line.push('\n');
    line
}

/// The `--events` sink. Disabled when no path was given.
pub struct EventLog {
    out: Option<Box<dyn Write + Send>>,
}

impl EventLog {
    pub fn disabled() -> Self {
        Self { out: None }
    }

    /// Open the events destination: "-" = stdout, otherwise a file that must
    /// be new or empty unless `append` is set.
    pub fn open(path: &Path, append: bool) -> io::Result<Self> {
        let out: Box<dyn Write + Send> = if path == Path::new("-") {
            Box::new(io::stdout())
        } else {
            Box::new(open_new_or_append(path, append)?.0)
        };
        Ok(Self { out: Some(out) })
    }

    #[cfg(test)]
    pub fn to_writer(out: Box<dyn Write + Send>) -> Self {
        Self { out: Some(out) }
    }

    /// Events into an already opened file (an output switch's new file).
    pub fn from_file(file: std::fs::File) -> Self {
        Self {
            out: Some(Box::new(file)),
        }
    }

    /// Write and flush one event line.
    pub fn write(&mut self, event: &Event) -> io::Result<()> {
        let Some(out) = self.out.as_mut() else {
            return Ok(());
        };
        out.write_all(to_line(event, &Utc::now()).as_bytes())?;
        out.flush()
    }
}

/// Open `path` for writing: create it if missing, reuse it if empty, and
/// refuse a non-empty file unless `append`. Returns the file and its current
/// length (the offset of the first byte written).
pub fn open_new_or_append(path: &Path, append: bool) -> io::Result<(std::fs::File, u64)> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => return Ok((file, 0)),
        Err(err) if err.kind() != io::ErrorKind::AlreadyExists => return Err(err),
        Err(_) => {}
    }
    let len = std::fs::metadata(path)?.len();
    if len > 0 && !append {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "{} already exists and is not empty (use --append to add to it)",
                path.display()
            ),
        ));
    }
    let file = OpenOptions::new().append(true).open(path)?;
    let len = file.metadata()?.len();
    Ok((file, len))
}

/// Fail early (before connecting) if `path` would be refused later.
pub fn check_writable_target(path: &Path, append: bool) -> io::Result<()> {
    if path == Path::new("-") {
        return Ok(());
    }
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_dir() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is a directory", path.display()),
        )),
        Ok(meta) if meta.len() > 0 && !append => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "{} already exists and is not empty (use --append to add to it)",
                path.display()
            ),
        )),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            if parent.is_dir() {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("directory {} does not exist", parent.display()),
                ))
            }
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 4, 17, 0, 0).unwrap() + chrono::Duration::milliseconds(5)
    }

    #[test]
    fn line_has_ts_and_type_first() {
        let line = to_line(&Event::warning("test", "hello"), &ts());
        assert_eq!(
            line,
            "{\"ts\":\"2026-11-04T17:00:00.005Z\",\"type\":\"warning\",\"code\":\"test\",\"message\":\"hello\"}\n"
        );
    }

    #[test]
    fn end_flattens_totals() {
        let end = Event::End(End {
            reason: "stream_ended",
            exit_code: 0,
            signal: None,
            error: None,
            totals: Totals {
                segments: 2,
                bytes: 10,
                ..Default::default()
            },
        });
        let v: serde_json::Value = serde_json::from_str(&to_line(&end, &ts())).unwrap();
        assert_eq!(v["type"], "end");
        assert_eq!(v["reason"], "stream_ended");
        assert_eq!(v["segments"], 2);
        assert_eq!(v["bytes"], 10);
        assert!(v["signal"].is_null());
    }

    #[test]
    fn pdt_formatting_is_utc_millis() {
        let dt = DateTime::parse_from_rfc3339("2026-01-01T01:00:00.123+01:00").unwrap();
        assert_eq!(fmt_time(&dt), "2026-01-01T00:00:00.123Z");
    }

    #[test]
    fn output_open_rules() {
        let dir = std::env::temp_dir().join(format!("twitchpipe-open-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("out.ts");

        // new file
        assert!(check_writable_target(&path, false).is_ok());
        let (mut f, off) = open_new_or_append(&path, false).unwrap();
        assert_eq!(off, 0);
        // existing empty file is fine
        assert_eq!(open_new_or_append(&path, false).unwrap().1, 0);
        f.write_all(b"abc").unwrap();
        drop(f);
        // non-empty: refused without append
        assert!(check_writable_target(&path, false).is_err());
        assert_eq!(
            open_new_or_append(&path, false).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        // append continues at the end
        assert!(check_writable_target(&path, true).is_ok());
        let (mut f, off) = open_new_or_append(&path, true).unwrap();
        assert_eq!(off, 3);
        f.write_all(b"de").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"abcde");
        // missing directory / directory target
        assert!(check_writable_target(&dir.join("nope/out.ts"), false).is_err());
        assert!(check_writable_target(&dir, true).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
