//! Optional pipeline instrumentation used by recorder mode.
//!
//! When the worker/fetcher are given an [`EventTx`], they report what they do:
//! the fetcher puts [`SlotMarker`]s into the byte buffer *in-band* (so they
//! are ordered exactly with the bytes they describe) and both send
//! out-of-band [`PipelineEvent`]s. Without an `EventTx` (player mode) none of
//! this is produced and the byte stream is identical.

use bytes::Bytes;
use chrono::{DateTime, FixedOffset};
use tokio::sync::mpsc;

use crate::hls::hook::{HookEvent, Substitute};
use crate::hls::m3u8::MediaSegment;

pub type EventTx = mpsc::UnboundedSender<PipelineEvent>;
pub type EventRx = mpsc::UnboundedReceiver<PipelineEvent>;

/// Out-of-band events from the worker, the fetcher and the hook.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineEvent {
    Hook(HookEvent),
    /// The stream stopped producing segments; persist/recovery started.
    StreamDown {
        reason: String,
        last_sequence: i64,
    },
    /// Recovery found new segments.
    StreamRecovered {
        downtime_s: f64,
        sequence: i64,
    },
    /// Recovery gave up after `recovery_timeout`.
    RecoveryTimedOut {
        waited_s: f64,
    },
    /// The worker never queued slots `from..=to` (the playlist moved past
    /// them before they were seen).
    SequenceJump {
        from: i64,
        to: i64,
        after_recovery: bool,
    },
    /// Playlist values for a segment that was queued as a prefetch segment
    /// (whose duration/date were estimates at queue time).
    SegmentTiming {
        sequence: i64,
        date: Option<DateTime<FixedOffset>>,
        duration: f64,
    },
    Warning(String),
}

/// Reporting-safe description of a substitute segment (no URL).
#[derive(Debug, Clone, PartialEq)]
pub struct SubstituteInfo {
    pub strategy: &'static str,
    pub player_type: Option<String>,
    pub sequence: Option<i64>,
    pub date: Option<DateTime<FixedOffset>>,
    pub duration: Option<f64>,
}

impl From<&Substitute> for SubstituteInfo {
    fn from(s: &Substitute) -> Self {
        Self {
            strategy: s.strategy,
            player_type: s.player_type.clone(),
            sequence: s.sequence,
            date: s.date,
            duration: s.duration,
        }
    }
}

/// One live playlist slot as seen by the fetcher.
#[derive(Debug, Clone, PartialEq)]
pub struct SlotInfo {
    /// Media sequence number of the slot in the live playlist.
    pub sequence: i64,
    pub date: Option<DateTime<FixedOffset>>,
    pub duration: f64,
    pub prefetch: bool,
    pub ad: bool,
    /// EXT-X-DISCONTINUITY (after the parser's false-discontinuity fixes).
    pub discontinuity: bool,
    /// Set when the hook substituted the slot.
    pub substitute: Option<SubstituteInfo>,
}

impl SlotInfo {
    pub fn new(segment: &MediaSegment, substitute: Option<&Substitute>) -> Self {
        Self {
            sequence: segment.num,
            date: segment.date,
            duration: segment.duration,
            prefetch: segment.prefetch,
            ad: segment.ad,
            discontinuity: segment.discontinuity,
            substitute: substitute.map(SubstituteInfo::from),
        }
    }
}

/// In-band markers, interleaved with segment bytes in the output buffer.
#[derive(Debug, Clone, PartialEq)]
pub enum SlotMarker {
    /// The bytes that follow (until `End`) belong to this slot.
    Begin(SlotInfo),
    /// The current slot is complete (`error` = body download failed midway).
    End { error: Option<String> },
    /// The hook dropped the slot; nothing was written.
    Skipped(SlotInfo),
    /// The download failed after all attempts; nothing was written.
    Failed(SlotInfo, String),
    /// The next slot (its `Begin` follows) starts with this fMP4 init
    /// section (`EXT-X-MAP`): its bytes are the first data of that slot.
    /// Sent only when the fetcher writes an init, so the recorder always
    /// knows the current one (to put it at the head of a new output).
    Init(Bytes),
}

/// Send an event if instrumentation is enabled.
pub fn emit(tx: &Option<EventTx>, event: PipelineEvent) {
    if let Some(tx) = tx {
        let _ = tx.send(event);
    }
}

/// Strip query strings and fragments from any URLs in a message, so signed
/// URLs/tokens never reach logs or event files.
pub fn redact_urls(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(pos) = rest.find("http") {
        let (head, tail) = rest.split_at(pos);
        out.push_str(head);
        if !(tail.starts_with("http://") || tail.starts_with("https://")) {
            out.push_str("http");
            rest = &tail[4..];
            continue;
        }
        let end = tail
            .find(|c: char| c.is_whitespace() || matches!(c, ')' | '"' | '\'' | '>' | ']'))
            .unwrap_or(tail.len());
        let url = &tail[..end];
        match url.find(['?', '#']) {
            Some(q) => {
                out.push_str(&url[..q]);
                out.push_str("?<redacted>");
            }
            None => out.push_str(url),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_query_strings() {
        assert_eq!(
            redact_urls(
                "error sending request for url (https://usher.ttvnw.net/api/x.m3u8?sig=abc&token=xyz): timed out"
            ),
            "error sending request for url (https://usher.ttvnw.net/api/x.m3u8?<redacted>): timed out"
        );
        assert_eq!(
            redact_urls("see https://a.b/c and http://d.e/f#frag."),
            "see https://a.b/c and http://d.e/f?<redacted>"
        );
        assert_eq!(redact_urls("httpx no url"), "httpx no url");
    }
}
