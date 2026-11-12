//! Ring mode (`--ring-dir`): the output is a series of chunk files that
//! together hold at least the last `keep_s` seconds of the stream.
//!
//! Chunks are cut at segment boundaries only, once the open chunk holds
//! `chunk_s` seconds of media. Offsets are counted over the ring's virtual
//! output (every chunk ever written, back to back), so `segment` events keep
//! their usual meaning and a chunk is the byte range `offset..offset+length`
//! of that output. For fMP4 every chunk starts with the current init section
//! (the session writes it into the chunk's first slot), so any chunk plays on
//! its own and pruning never loses the init.
//!
//! The ring deletes only its own pruned chunks. After an output switch it is
//! dropped and the held chunks stay on disk untouched.

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};

use super::Output;
use super::events::{self, Chunk, Ring as RingEvent, Segment};

pub const CHUNK_PREFIX: &str = "chunk-";

pub struct Ring {
    dir: PathBuf,
    keep_s: f64,
    chunk_s: f64,
    /// Closed chunks on disk, oldest first.
    held: VecDeque<Chunk>,
    open: Option<(Chunk, Output)>,
    next_index: u64,
    /// Ring offset of the next byte.
    total: u64,
    /// fMP4 stream (an init section was seen): chunks are named `.mp4`.
    fmp4: bool,
    /// Init length at the head of the next chunk opened.
    head_init: u64,
}

impl Ring {
    /// Create the ring directory (if missing). It must not hold chunk files
    /// already: a ring never mixes with an earlier run's chunks.
    pub fn create(dir: &Path, keep_s: f64, chunk_s: f64) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        for entry in std::fs::read_dir(dir)? {
            let name = entry?.file_name();
            if name.to_string_lossy().starts_with(CHUNK_PREFIX) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "{} already holds chunk files ({})",
                        dir.display(),
                        name.to_string_lossy()
                    ),
                ));
            }
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            keep_s: keep_s.max(0.0),
            chunk_s: chunk_s.max(0.0),
            held: VecDeque::new(),
            open: None,
            next_index: 1,
            total: 0,
            fmp4: false,
            head_init: 0,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn config(&self) -> events::RingConfig {
        events::RingConfig {
            dir: self.dir.display().to_string(),
            keep_s: self.keep_s,
            chunk_s: self.chunk_s,
        }
    }

    pub fn set_fmp4(&mut self) {
        self.fmp4 = true;
    }

    /// The next chunk starts with an init section of `len` bytes.
    pub fn set_head_init(&mut self, len: u64) {
        match &mut self.open {
            Some((chunk, _)) if chunk.length == 0 => chunk.init_length = len,
            Some(_) => {}
            None => self.head_init = len,
        }
    }

    fn chunk_path(&self, index: u64) -> PathBuf {
        let ext = if self.fmp4 { "mp4" } else { "ts" };
        self.dir.join(format!("{CHUNK_PREFIX}{index:06}.{ext}"))
    }

    fn ensure_open(&mut self) -> Result<&mut (Chunk, Output), String> {
        if self.open.is_none() {
            let index = self.next_index;
            let path = self.chunk_path(index);
            let (file, _) = events::open_new_or_append(&path, false)
                .map_err(|e| format!("cannot open ring chunk {}: {e}", path.display()))?;
            self.next_index += 1;
            let chunk = Chunk {
                index,
                path: path.display().to_string(),
                offset: self.total,
                init_length: std::mem::take(&mut self.head_init),
                ..Default::default()
            };
            let out = Output::from_writer(Box::new(tokio::fs::File::from_std(file)));
            self.open = Some((chunk, out));
        }
        Ok(self.open.as_mut().expect("opened above"))
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        let (chunk, out) = self.ensure_open()?;
        out.write(data).await?;
        chunk.length += data.len() as u64;
        self.total += data.len() as u64;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), String> {
        match &mut self.open {
            Some((_, out)) => out.flush().await,
            None => Ok(()),
        }
    }

    /// A segment was written into the open chunk.
    pub fn on_segment(&mut self, seg: &Segment) {
        let Some((chunk, _)) = &mut self.open else {
            return;
        };
        chunk.segments += 1;
        chunk.duration += seg.duration.unwrap_or(0.0);
        chunk.first_slot_sequence.get_or_insert(seg.slot_sequence);
        chunk.last_slot_sequence = Some(seg.slot_sequence);
        if chunk.first_program_date_time.is_none() {
            chunk.first_program_date_time = seg.program_date_time.clone();
        }
        if seg.program_date_time.is_some() {
            chunk.last_program_date_time = seg.program_date_time.clone();
        }
    }

    /// The open chunk holds enough media to be closed at this boundary.
    pub fn should_rotate(&self) -> bool {
        self.open
            .as_ref()
            .is_some_and(|(c, _)| c.segments > 0 && c.duration >= self.chunk_s)
    }

    /// Flush and close the open chunk (if any); it joins the held chunks.
    /// Returns the chunk (for its `chunk` event) and how the flush went.
    pub async fn close_open(
        &mut self,
        closed_by: &'static str,
    ) -> (Option<Chunk>, Result<(), String>) {
        let Some((mut chunk, mut out)) = self.open.take() else {
            return (None, Ok(()));
        };
        let flushed = out.flush().await;
        drop(out);
        chunk.closed_by = Some(closed_by);
        self.held.push_back(Chunk {
            closed_by: None,
            ..chunk.clone()
        });
        (Some(chunk), flushed)
    }

    /// Delete the oldest chunks while the rest still hold `keep_s` seconds.
    /// Returns the deleted paths.
    pub fn prune(&mut self) -> Vec<String> {
        let open = self.open.as_ref().map_or(0.0, |(c, _)| c.duration);
        let mut held: f64 = open + self.held.iter().map(|c| c.duration).sum::<f64>();
        let mut removed = Vec::new();
        while let Some(oldest) = self.held.front() {
            if held - oldest.duration < self.keep_s {
                break;
            }
            held -= oldest.duration;
            let oldest = self.held.pop_front().expect("front exists");
            if let Err(err) = std::fs::remove_file(&oldest.path)
                && err.kind() != io::ErrorKind::NotFound
            {
                tracing::warn!("cannot delete ring chunk {}: {err}", oldest.path);
            }
            removed.push(oldest.path);
        }
        removed
    }

    /// The closed chunks on disk, oldest first.
    pub fn held(&self) -> Vec<Chunk> {
        self.held.iter().cloned().collect()
    }

    pub fn event(
        &self,
        action: &'static str,
        closed: Option<u64>,
        removed: Vec<String>,
    ) -> RingEvent {
        let open = self.open.as_ref().map(|(c, _)| c);
        let all = || self.held.iter().chain(open);
        RingEvent {
            action,
            dir: self.dir.display().to_string(),
            closed,
            removed,
            chunks: self.held.iter().map(|c| c.index).collect(),
            held_bytes: all().map(|c| c.length).sum(),
            held_seconds: (all().map(|c| c.duration).sum::<f64>() * 1000.0).round() / 1000.0,
            base_offset: self.held.front().or(open).map_or(self.total, |c| c.offset),
            first_slot_sequence: all().find_map(|c| c.first_slot_sequence),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(slot: i64, duration: f64) -> Segment {
        Segment {
            sequence: Some(slot),
            slot_sequence: slot,
            kind: "live",
            program_date_time: None,
            duration: Some(duration),
            timing_estimated: false,
            offset: 0,
            length: 0,
            complete: true,
            discontinuity: false,
            source: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn rotates_by_duration_and_keeps_at_least_keep_s() {
        let dir = std::env::temp_dir().join(format!("twitchpipe-ring-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut ring = Ring::create(&dir, 5.0, 4.0).unwrap();
        let mut slot = 0;
        let mut removed_all = Vec::new();
        for _ in 0..10 {
            ring.write(&[slot as u8; 10]).await.unwrap();
            ring.on_segment(&seg(slot, 2.0));
            slot += 1;
            if ring.should_rotate() {
                let (closed, flushed) = ring.close_open("rotate").await;
                flushed.unwrap();
                let closed = closed.unwrap();
                assert_eq!(closed.duration, 4.0);
                assert_eq!(closed.length, 20);
                removed_all.extend(ring.prune());
                let ev = ring.event("prune", None, Vec::new());
                let recorded = slot as f64 * 2.0;
                assert!(ev.held_seconds >= recorded.min(5.0), "{ev:?}");
            }
        }
        // 10 segments of 2 s, chunks of 4 s: 5 chunks; keep 5 s -> 2 held
        let held = ring.held();
        assert_eq!(held.iter().map(|c| c.index).collect::<Vec<_>>(), vec![4, 5]);
        assert_eq!(held[0].offset, 60);
        assert_eq!(removed_all.len(), 3);
        for path in &removed_all {
            assert!(!Path::new(path).exists());
        }
        for c in &held {
            assert_eq!(std::fs::read(&c.path).unwrap().len() as u64, c.length);
        }
        // a second ring refuses a directory with chunks in it
        assert!(Ring::create(&dir, 5.0, 4.0).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
