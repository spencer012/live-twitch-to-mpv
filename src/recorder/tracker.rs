//! Turns the ordered stream of output bytes and slot markers (plus
//! out-of-band pipeline events) into recorder events.
//!
//! The tracker sees exactly what is written to the output, in order, so
//! `segment` offsets/lengths are exact: they are counted from the bytes the
//! recorder actually wrote, not from what was downloaded.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, FixedOffset};

use crate::hls::events::{PipelineEvent, SlotInfo, SlotMarker};
use crate::hls::hook::HookEvent;
use crate::recorder::events::{
    Discontinuity, Event, Gap, Segment, SegmentTiming, SourceInfo, Totals, fmt_opt_time,
};

const PREFETCH_MEMORY: usize = 64;

struct Open {
    info: SlotInfo,
    start: u64,
    len: u64,
}

struct Written {
    /// Sequence space: "main" for live/prefetch/delayed (Twitch numbers the
    /// delayed playlist like the live one), "backup:<player type>" otherwise.
    space: String,
    sequence: Option<i64>,
    kind: &'static str,
}

struct PendingGap {
    gap: Gap,
}

struct Hint {
    from: i64,
    to: i64,
    after_recovery: bool,
}

struct EmittedPrefetch {
    sequence: i64,
    offset: u64,
    date: Option<DateTime<FixedOffset>>,
    duration: f64,
}

pub struct Tracker {
    offset: u64,
    open: Option<Open>,
    last_slot: Option<i64>,
    last_written: Option<Written>,
    pending_gap: Option<PendingGap>,
    hints: Vec<Hint>,
    reopen_pending: bool,
    /// Final timing that arrived before the prefetch segment was emitted.
    pending_timing: HashMap<i64, (Option<DateTime<FixedOffset>>, f64)>,
    emitted_prefetch: VecDeque<EmittedPrefetch>,
    pub totals: Totals,
    attempt_segments: u64,
    /// Segments written over all outputs (totals restart at a switch).
    lifetime_segments: u64,
    /// Sequence numbers and end date of the last segment written.
    last_segment: Option<LastSegment>,
}

/// The last segment written: where the next output picks up.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LastSegment {
    pub slot_sequence: i64,
    pub sequence: Option<i64>,
    /// Program date time + duration: the date at the segment's end.
    pub end_program_date_time: Option<String>,
}

impl Tracker {
    /// `start_offset`: output length before the first byte is written.
    pub fn new(start_offset: u64) -> Self {
        Self {
            offset: start_offset,
            open: None,
            last_slot: None,
            last_written: None,
            pending_gap: None,
            hints: Vec::new(),
            reopen_pending: false,
            pending_timing: HashMap::new(),
            emitted_prefetch: VecDeque::new(),
            totals: Totals::default(),
            attempt_segments: 0,
            lifetime_segments: 0,
            last_segment: None,
        }
    }

    /// A segment is being written (bytes may still arrive for it).
    pub fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// The last live playlist slot seen (written, skipped or failed).
    pub fn last_slot(&self) -> Option<i64> {
        self.last_slot
    }

    pub fn last_segment(&self) -> Option<&LastSegment> {
        self.last_segment.as_ref()
    }

    /// Segments written over all outputs of this run.
    pub fn lifetime_segments(&self) -> u64 {
        self.lifetime_segments
    }

    /// Bytes written to the current output (plus its start offset).
    pub fn output_offset(&self) -> u64 {
        self.offset
    }

    /// Report a gap that is waiting for the next segment now (before the
    /// output changes, so it lands in the output where it happened).
    pub fn flush_pending_gap(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        self.flush_gap(&mut events);
        events
    }

    /// The output was switched at a segment boundary: offsets restart at 0
    /// and totals count the new output only. Sequence continuity (gaps,
    /// discontinuities) carries over. Returns the old output's totals.
    /// Late `segment_timing` corrections for prefetch segments written to the
    /// old output are dropped (their offsets belong to the old file).
    pub fn switch_output(&mut self) -> Totals {
        debug_assert!(self.open.is_none() && self.pending_gap.is_none());
        self.offset = 0;
        self.emitted_prefetch.clear();
        std::mem::take(&mut self.totals)
    }

    /// Current output offset (bytes written so far, including any start offset).
    #[cfg(test)]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Segments written since the last `begin_attempt`.
    pub fn attempt_segments(&self) -> u64 {
        self.attempt_segments
    }

    /// A new pipeline (stream re-open) starts.
    pub fn begin_attempt(&mut self, reopen: bool) {
        self.attempt_segments = 0;
        self.hints.clear();
        self.pending_timing.clear();
        self.reopen_pending = reopen;
    }

    /// Bytes were written to the output.
    pub fn on_data(&mut self, len: usize) {
        self.offset += len as u64;
        if let Some(open) = &mut self.open {
            open.len += len as u64;
        }
    }

    pub fn on_marker(&mut self, marker: SlotMarker) -> Vec<Event> {
        let mut events = Vec::new();
        match marker {
            SlotMarker::Begin(info) => {
                if self.open.is_some() {
                    self.close(
                        Some("segment ended without an end marker".into()),
                        &mut events,
                    );
                }
                self.slot_seen(info.sequence, &mut events);
                self.flush_gap(&mut events);
                self.open = Some(Open {
                    info,
                    start: self.offset,
                    len: 0,
                });
            }
            SlotMarker::End { error } => self.close(error, &mut events),
            SlotMarker::Skipped(info) => {
                self.slot_seen(info.sequence, &mut events);
                let reason = if info.ad {
                    "ad_dropped"
                } else {
                    "ad_block_skipped"
                };
                self.add_gap(&info, reason, None, &mut events);
            }
            SlotMarker::Failed(info, error) => {
                self.slot_seen(info.sequence, &mut events);
                self.add_gap(&info, "download_failed", Some(error), &mut events);
            }
            // the init's bytes arrive as data inside the next slot
            SlotMarker::Init(_) => {}
        }
        events
    }

    pub fn on_pipeline(&mut self, event: PipelineEvent) -> Vec<Event> {
        match event {
            PipelineEvent::Hook(HookEvent::AdStart {
                strategy,
                preroll,
                sequence,
                break_duration,
            }) => vec![Event::AdStart {
                strategy,
                preroll,
                sequence,
                break_duration,
            }],
            PipelineEvent::Hook(HookEvent::AdEnd {
                strategy,
                sequence,
                catching_up,
            }) => vec![Event::AdEnd {
                strategy,
                sequence,
                catching_up,
            }],
            PipelineEvent::Hook(HookEvent::AdCatchupComplete {
                strategy,
                sequence,
                substituted,
            }) => vec![Event::AdCatchupComplete {
                strategy,
                sequence,
                substituted,
            }],
            PipelineEvent::StreamDown {
                reason,
                last_sequence,
            } => vec![Event::StreamDown {
                reason,
                last_sequence,
            }],
            PipelineEvent::StreamRecovered {
                downtime_s,
                sequence,
            } => vec![Event::StreamRecovered {
                downtime_s,
                sequence,
            }],
            PipelineEvent::RecoveryTimedOut { waited_s } => vec![Event::warning(
                "recovery_timeout",
                format!("stream did not recover within {waited_s:.0}s"),
            )],
            PipelineEvent::SequenceJump {
                from,
                to,
                after_recovery,
            } => {
                self.hints.push(Hint {
                    from,
                    to,
                    after_recovery,
                });
                Vec::new()
            }
            PipelineEvent::SegmentTiming {
                sequence,
                date,
                duration,
            } => {
                if let Some(pos) = self
                    .emitted_prefetch
                    .iter()
                    .position(|p| p.sequence == sequence)
                {
                    let p = self.emitted_prefetch.remove(pos).expect("index is valid");
                    return vec![timing_event(&p, date, duration)];
                }
                let not_yet_written = self.open.as_ref().is_some_and(|o| {
                    o.info.sequence == sequence && o.info.prefetch && o.info.substitute.is_none()
                }) || self.last_slot.is_none_or(|last| sequence > last);
                if not_yet_written {
                    self.pending_timing.insert(sequence, (date, duration));
                }
                Vec::new()
            }
            PipelineEvent::Warning(message) => vec![Event::warning("pipeline", message)],
        }
    }

    /// The pipeline stopped: close a partially written segment and flush any
    /// pending gap.
    pub fn finish(&mut self, interrupted_reason: &str) -> Vec<Event> {
        let mut events = Vec::new();
        if self.open.is_some() {
            self.close(Some(interrupted_reason.to_string()), &mut events);
        }
        self.flush_gap(&mut events);
        events
    }

    fn slot_seen(&mut self, sequence: i64, events: &mut Vec<Event>) {
        if let Some(last) = self.last_slot
            && sequence > last + 1
        {
            self.flush_gap(events);
            let from = last + 1;
            let to = sequence - 1;
            let reason = if self.reopen_pending {
                "stream_reopen"
            } else {
                match self.hints.iter().find(|h| h.from <= from && h.to >= from) {
                    Some(h) if h.after_recovery => "stream_recovery",
                    _ => "playlist_jump",
                }
            };
            let count = to - from + 1;
            self.totals.gaps += 1;
            self.totals.gap_segments += count;
            events.push(Event::Gap(Gap {
                from_sequence: from,
                to_sequence: to,
                count,
                reason,
                offset: self.offset,
                program_date_time: None,
                duration: None,
                error: None,
            }));
        }
        self.reopen_pending = false;
        self.last_slot = Some(sequence);
        self.hints.retain(|h| h.to > sequence);
        self.pending_timing.retain(|seq, _| *seq >= sequence);
    }

    fn add_gap(
        &mut self,
        info: &SlotInfo,
        reason: &'static str,
        error: Option<String>,
        events: &mut Vec<Event>,
    ) {
        if let Some(pending) = &mut self.pending_gap {
            let gap = &mut pending.gap;
            if gap.reason == reason && gap.to_sequence + 1 == info.sequence {
                gap.to_sequence = info.sequence;
                gap.count += 1;
                gap.duration = gap.duration.map(|d| d + info.duration);
                if error.is_some() {
                    gap.error = error;
                }
                return;
            }
        }
        self.flush_gap(events);
        self.pending_gap = Some(PendingGap {
            gap: Gap {
                from_sequence: info.sequence,
                to_sequence: info.sequence,
                count: 1,
                reason,
                offset: self.offset,
                program_date_time: fmt_opt_time(info.date),
                duration: Some(info.duration),
                error,
            },
        });
    }

    fn flush_gap(&mut self, events: &mut Vec<Event>) {
        if let Some(pending) = self.pending_gap.take() {
            self.totals.gaps += 1;
            self.totals.gap_segments += pending.gap.count;
            events.push(Event::Gap(pending.gap));
        }
    }

    fn close(&mut self, error: Option<String>, events: &mut Vec<Event>) {
        let Some(open) = self.open.take() else {
            return;
        };
        let info = open.info;
        if open.len == 0 {
            // Nothing reached the output: report the slot as lost.
            self.add_gap(&info, "download_failed", error, events);
            return;
        }

        let (kind, space, sequence, date, duration, source) = match &info.substitute {
            Some(sub) => (
                "ad_substitute",
                if sub.strategy == crate::adblock::STRATEGY_DELAYED {
                    "main".to_string()
                } else {
                    format!(
                        "{}:{}",
                        sub.strategy,
                        sub.player_type.as_deref().unwrap_or("")
                    )
                },
                sub.sequence,
                sub.date,
                sub.duration,
                Some(SourceInfo {
                    strategy: sub.strategy,
                    player_type: sub.player_type.clone(),
                }),
            ),
            None => (
                if info.ad {
                    "ad"
                } else if info.prefetch {
                    "prefetch"
                } else {
                    "live"
                },
                "main".to_string(),
                Some(info.sequence),
                info.date,
                Some(info.duration),
                None,
            ),
        };

        let discontinuity = self.last_written.as_ref().and_then(|prev| {
            let reason = if prev.space != space {
                "source_change"
            } else if let (Some(p), Some(c)) = (prev.sequence, sequence)
                && c != p + 1
            {
                "sequence_jump"
            } else if info.discontinuity && info.substitute.is_none() {
                "hls_discontinuity"
            } else {
                return None;
            };
            Some(Discontinuity {
                offset: open.start,
                reason,
                previous_sequence: prev.sequence,
                sequence,
                previous_kind: prev.kind,
                kind,
            })
        });
        let has_discontinuity = discontinuity.is_some();
        if let Some(d) = discontinuity {
            self.totals.discontinuities += 1;
            events.push(Event::Discontinuity(d));
        }

        let complete = error.is_none();
        let program_date_time = fmt_opt_time(date);
        self.totals.segments += 1;
        self.attempt_segments += 1;
        self.lifetime_segments += 1;
        self.last_segment =
            Some(LastSegment {
                slot_sequence: info.sequence,
                sequence,
                end_program_date_time: fmt_opt_time(date.map(|d| {
                    d + chrono::Duration::milliseconds(
                        (duration.unwrap_or(0.0) * 1000.0).round() as i64
                    )
                })),
            });
        self.totals.bytes += open.len;
        self.totals.media_duration_s += duration.unwrap_or(0.0);
        if kind == "ad_substitute" {
            self.totals.ad_substitute_segments += 1;
        }
        if !complete {
            self.totals.incomplete_segments += 1;
        }
        if self.totals.first_program_date_time.is_none() {
            self.totals.first_program_date_time = program_date_time.clone();
        }
        if program_date_time.is_some() {
            self.totals.last_program_date_time = program_date_time.clone();
        }

        events.push(Event::Segment(Segment {
            sequence,
            slot_sequence: info.sequence,
            kind,
            program_date_time,
            duration,
            timing_estimated: kind == "prefetch",
            offset: open.start,
            length: open.len,
            complete,
            discontinuity: has_discontinuity,
            source,
            error,
        }));

        if kind == "prefetch" {
            let emitted = EmittedPrefetch {
                sequence: info.sequence,
                offset: open.start,
                date: info.date,
                duration: info.duration,
            };
            if let Some((date, duration)) = self.pending_timing.remove(&info.sequence) {
                events.push(timing_event(&emitted, date, duration));
            } else {
                self.emitted_prefetch.push_back(emitted);
                while self.emitted_prefetch.len() > PREFETCH_MEMORY {
                    self.emitted_prefetch.pop_front();
                }
            }
        }

        self.last_written = Some(Written {
            space,
            sequence,
            kind,
        });
    }
}

fn timing_event(p: &EmittedPrefetch, date: Option<DateTime<FixedOffset>>, duration: f64) -> Event {
    Event::SegmentTiming(SegmentTiming {
        sequence: p.sequence,
        offset: p.offset,
        program_date_time: fmt_opt_time(date),
        duration,
        estimated_program_date_time: fmt_opt_time(p.date),
        estimated_duration: Some(p.duration),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hls::events::SubstituteInfo;

    fn date(secs: i64) -> Option<DateTime<FixedOffset>> {
        Some(
            DateTime::parse_from_rfc3339("2026-01-01T00:00:00.000Z").unwrap()
                + chrono::Duration::seconds(secs),
        )
    }

    fn slot(seq: i64) -> SlotInfo {
        SlotInfo {
            sequence: seq,
            date: date(seq * 2),
            duration: 2.0,
            prefetch: false,
            ad: false,
            discontinuity: false,
            substitute: None,
        }
    }

    /// Feed one complete segment of `len` bytes.
    fn write(t: &mut Tracker, info: SlotInfo, len: usize) -> Vec<Event> {
        let mut ev = t.on_marker(SlotMarker::Begin(info));
        t.on_data(len / 2);
        t.on_data(len - len / 2);
        ev.extend(t.on_marker(SlotMarker::End { error: None }));
        ev
    }

    fn segments(events: &[Event]) -> Vec<&Segment> {
        events
            .iter()
            .filter_map(|e| match e {
                Event::Segment(s) => Some(s),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn offsets_and_lengths_are_cumulative() {
        let mut t = Tracker::new(0);
        let mut ev = Vec::new();
        ev.extend(write(&mut t, slot(10), 100));
        ev.extend(write(&mut t, slot(11), 250));
        ev.extend(write(&mut t, slot(12), 7));
        let segs = segments(&ev);
        assert_eq!(ev.len(), 3, "no gaps/discontinuities: {ev:?}");
        assert_eq!(
            segs.iter()
                .map(|s| (s.sequence.unwrap(), s.offset, s.length))
                .collect::<Vec<_>>(),
            vec![(10, 0, 100), (11, 100, 250), (12, 350, 7)]
        );
        assert!(segs.iter().all(|s| s.kind == "live" && s.complete));
        assert_eq!(
            segs[0].program_date_time.as_deref(),
            Some("2026-01-01T00:00:20.000Z")
        );
        assert_eq!(t.totals.bytes, 357);
        assert_eq!(t.offset(), 357);
        assert_eq!(t.totals.segments, 3);
        assert_eq!(t.totals.media_duration_s, 6.0);
    }

    #[test]
    fn switch_output_restarts_offsets_and_keeps_continuity() {
        let mut t = Tracker::new(0);
        write(&mut t, slot(1), 10);
        let mut ad = slot(2);
        ad.ad = true;
        t.on_marker(SlotMarker::Skipped(ad));
        assert!(!t.is_open());
        // the gap at the boundary goes to the old output, at its end
        let gap = t.flush_pending_gap();
        assert!(matches!(&gap[..], [Event::Gap(g)] if g.offset == 10 && g.from_sequence == 2));
        let old = t.switch_output();
        assert_eq!((old.segments, old.bytes, old.gaps), (1, 10, 1));
        assert_eq!(t.last_slot(), Some(2));
        let last = t.last_segment().unwrap();
        assert_eq!(last.slot_sequence, 1);
        assert_eq!(
            last.end_program_date_time.as_deref(),
            Some("2026-01-01T00:00:04.000Z")
        );
        let ev = write(&mut t, slot(3), 7);
        // continuity is tracked across the switch: 1 -> 3 is a jump
        assert!(matches!(&ev[0], Event::Discontinuity(d) if d.offset == 0));
        assert_eq!(segments(&ev)[0].offset, 0);
        assert_eq!(t.totals.segments, 1);
        assert_eq!(t.lifetime_segments(), 2);
    }

    #[test]
    fn append_offsets_start_at_existing_length() {
        let mut t = Tracker::new(1000);
        let ev = write(&mut t, slot(1), 10);
        assert_eq!(segments(&ev)[0].offset, 1000);
        assert_eq!(t.totals.bytes, 10);
    }

    #[test]
    fn consecutive_ad_drops_merge_into_one_gap() {
        let mut t = Tracker::new(0);
        let mut ev = write(&mut t, slot(1), 10);
        for seq in 2..=4 {
            let mut s = slot(seq);
            s.ad = true;
            ev.extend(t.on_marker(SlotMarker::Skipped(s)));
        }
        ev.extend(write(&mut t, slot(5), 10));
        let types: Vec<&str> = ev
            .iter()
            .map(|e| match e {
                Event::Segment(_) => "segment",
                Event::Gap(_) => "gap",
                Event::Discontinuity(_) => "discontinuity",
                _ => "other",
            })
            .collect();
        // gap is emitted before the next segment, in output order
        assert_eq!(types, vec!["segment", "gap", "discontinuity", "segment"]);
        let Event::Gap(gap) = &ev[1] else {
            unreachable!()
        };
        assert_eq!((gap.from_sequence, gap.to_sequence, gap.count), (2, 4, 3));
        assert_eq!(gap.reason, "ad_dropped");
        assert_eq!(gap.offset, 10);
        assert_eq!(gap.duration, Some(6.0));
        assert_eq!(
            gap.program_date_time.as_deref(),
            Some("2026-01-01T00:00:04.000Z")
        );
        let Event::Discontinuity(d) = &ev[2] else {
            unreachable!()
        };
        assert_eq!((d.reason, d.offset), ("sequence_jump", 10));
        assert_eq!((d.previous_sequence, d.sequence), (Some(1), Some(5)));
        assert_eq!(t.totals.gaps, 1);
        assert_eq!(t.totals.gap_segments, 3);
    }

    #[test]
    fn different_gap_reasons_are_separate() {
        let mut t = Tracker::new(0);
        let mut ev = write(&mut t, slot(1), 10);
        let mut ad = slot(2);
        ad.ad = true;
        ev.extend(t.on_marker(SlotMarker::Skipped(ad)));
        ev.extend(t.on_marker(SlotMarker::Failed(slot(3), "status 404".into())));
        ev.extend(t.finish("stopped"));
        let gaps: Vec<(&str, i64)> = ev
            .iter()
            .filter_map(|e| match e {
                Event::Gap(g) => Some((g.reason, g.from_sequence)),
                _ => None,
            })
            .collect();
        assert_eq!(gaps, vec![("ad_dropped", 2), ("download_failed", 3)]);
    }

    #[test]
    fn playlist_jump_and_recovery_gaps() {
        let mut t = Tracker::new(0);
        let mut ev = write(&mut t, slot(1), 10);
        // worker reports it never queued 2..=3
        assert!(
            t.on_pipeline(PipelineEvent::SequenceJump {
                from: 2,
                to: 3,
                after_recovery: false
            })
            .is_empty()
        );
        ev.extend(write(&mut t, slot(4), 10));
        t.on_pipeline(PipelineEvent::SequenceJump {
            from: 5,
            to: 9,
            after_recovery: true,
        });
        let mut after = slot(10);
        after.discontinuity = true;
        ev.extend(write(&mut t, after, 10));
        // a jump without a hint is still reported
        ev.extend(write(&mut t, slot(12), 10));
        let gaps: Vec<(i64, i64, &str, u64)> = ev
            .iter()
            .filter_map(|e| match e {
                Event::Gap(g) => Some((g.from_sequence, g.to_sequence, g.reason, g.offset)),
                _ => None,
            })
            .collect();
        assert_eq!(
            gaps,
            vec![
                (2, 3, "playlist_jump", 10),
                (5, 9, "stream_recovery", 20),
                (11, 11, "playlist_jump", 30)
            ]
        );
    }

    #[test]
    fn reopen_gap_reason() {
        let mut t = Tracker::new(0);
        write(&mut t, slot(1), 10);
        t.begin_attempt(true);
        let ev = write(&mut t, slot(5), 10);
        assert!(matches!(&ev[0], Event::Gap(g) if g.reason == "stream_reopen"));
        assert_eq!(t.attempt_segments(), 1);
    }

    #[test]
    fn hls_discontinuity_flag_is_reported() {
        let mut t = Tracker::new(0);
        write(&mut t, slot(1), 10);
        let mut s = slot(2);
        s.discontinuity = true;
        let ev = write(&mut t, s, 10);
        assert!(
            matches!(&ev[0], Event::Discontinuity(d) if d.reason == "hls_discontinuity" && d.offset == 10)
        );
        assert!(segments(&ev)[0].discontinuity);
    }

    #[test]
    fn delayed_substitutes_report_source_and_continuity() {
        let mut t = Tracker::new(0);
        let mut ev = write(&mut t, slot(100), 10);
        // live slots 101..=102 are filled with delayed content 101..=102
        for (slot_seq, src_seq) in [(101, 101), (102, 102)] {
            let mut s = slot(slot_seq);
            s.ad = true;
            s.substitute = Some(SubstituteInfo {
                strategy: crate::adblock::STRATEGY_DELAYED,
                player_type: Some("popout".into()),
                sequence: Some(src_seq),
                date: date(src_seq * 2),
                duration: Some(2.002),
            });
            ev.extend(write(&mut t, s, 20));
        }
        let segs = segments(&ev);
        assert_eq!(ev.len(), 3, "contiguous delayed content: no discontinuity");
        assert_eq!(segs[1].kind, "ad_substitute");
        assert_eq!(segs[1].sequence, Some(101));
        assert_eq!(segs[1].slot_sequence, 101);
        assert_eq!(segs[1].duration, Some(2.002));
        assert_eq!(
            segs[1].source,
            Some(SourceInfo {
                strategy: "delayed_playback",
                player_type: Some("popout".into())
            })
        );
        assert_eq!(t.totals.ad_substitute_segments, 2);
    }

    #[test]
    fn backup_substitutes_are_a_source_change() {
        let mut t = Tracker::new(0);
        write(&mut t, slot(1), 10);
        let mut s = slot(2);
        s.ad = true;
        s.substitute = Some(SubstituteInfo {
            strategy: crate::adblock::STRATEGY_BACKUP,
            player_type: Some("embed".into()),
            sequence: Some(2),
            date: None,
            duration: None,
        });
        let ev = write(&mut t, s, 10);
        assert!(matches!(&ev[0], Event::Discontinuity(d) if d.reason == "source_change"));
        let ev = write(&mut t, slot(3), 10);
        assert!(matches!(&ev[0], Event::Discontinuity(d) if d.reason == "source_change"));
    }

    #[test]
    fn incomplete_download_and_empty_failure() {
        let mut t = Tracker::new(0);
        let mut ev = t.on_marker(SlotMarker::Begin(slot(1)));
        t.on_data(42);
        ev.extend(t.on_marker(SlotMarker::End {
            error: Some("connection reset".into()),
        }));
        // a body that failed before any byte is a gap, not a segment
        ev.extend(t.on_marker(SlotMarker::Begin(slot(2))));
        ev.extend(t.on_marker(SlotMarker::End {
            error: Some("connection reset".into()),
        }));
        ev.extend(write(&mut t, slot(3), 8));
        let segs = segments(&ev);
        assert_eq!(segs.len(), 2);
        assert!(!segs[0].complete);
        assert_eq!(segs[0].error.as_deref(), Some("connection reset"));
        assert_eq!((segs[1].offset, segs[1].length), (42, 8));
        assert!(
            ev.iter()
                .any(|e| matches!(e, Event::Gap(g) if g.reason == "download_failed" && g.from_sequence == 2 && g.offset == 42))
        );
        assert_eq!(t.totals.incomplete_segments, 1);
    }

    #[test]
    fn finish_closes_partial_segment() {
        let mut t = Tracker::new(0);
        t.on_marker(SlotMarker::Begin(slot(1)));
        t.on_data(5);
        let ev = t.finish("interrupted");
        let segs = segments(&ev);
        assert_eq!(segs.len(), 1);
        assert!(!segs[0].complete);
        assert_eq!(segs[0].length, 5);
        assert!(t.finish("again").is_empty());
    }

    #[test]
    fn prefetch_timing_follows_segment_event() {
        let mut t = Tracker::new(0);
        let mut p = slot(7);
        p.prefetch = true;
        p.duration = 1.9;
        // timing arrives after the segment was written
        let ev = write(&mut t, p.clone(), 10);
        let seg = segments(&ev)[0];
        assert_eq!(seg.kind, "prefetch");
        assert!(seg.timing_estimated);
        let ev = t.on_pipeline(PipelineEvent::SegmentTiming {
            sequence: 7,
            date: date(15),
            duration: 2.0,
        });
        let Event::SegmentTiming(timing) = &ev[0] else {
            panic!("{ev:?}")
        };
        assert_eq!(timing.sequence, 7);
        assert_eq!(timing.offset, 0);
        assert_eq!(timing.duration, 2.0);
        assert_eq!(timing.estimated_duration, Some(1.9));
        assert_eq!(
            timing.program_date_time.as_deref(),
            Some("2026-01-01T00:00:15.000Z")
        );

        // timing arrives while the segment is still being written: it is
        // emitted right after the segment event
        let mut p = slot(8);
        p.prefetch = true;
        let mut ev = t.on_marker(SlotMarker::Begin(p));
        t.on_data(3);
        assert!(
            t.on_pipeline(PipelineEvent::SegmentTiming {
                sequence: 8,
                date: date(17),
                duration: 2.0,
            })
            .is_empty()
        );
        ev.extend(t.on_marker(SlotMarker::End { error: None }));
        assert!(matches!(ev[0], Event::Segment(_)));
        assert!(matches!(&ev[1], Event::SegmentTiming(s) if s.sequence == 8 && s.offset == 10));
    }
}
