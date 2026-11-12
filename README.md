# TwitchPipe

TwitchPipe is a standalone Rust CLI that pipes live Twitch streams into mpv,
or records them headlessly (see [Recorder mode](#recorder-mode)).
It is inspired by the Twitch and HLS parts of Streamlink, but it is a focused
player rather than a Streamlink-compatible clone: low-latency HLS,
persist/recovery for dropped streams, and a segmented ad-block system
(delayed playback for mid-rolls, backup player types for pre-rolls).

Live streams only — no VODs, no clips, and no client-integrity browser flow
(access-token failures produce a clear error instead).

## Build

Requires a recent stable Rust toolchain.

```powershell
cd live-twitch-to-mpv
cargo build --release
```

The binary ends up at `target\release\twitchpipe.exe`.

For development checks:

```powershell
cargo test
cargo clippy --all-targets
```

## Configuration

All settings live in a TOML file (no environment variables). The config is
loaded from, in order:

1. the path given via `--config <PATH>`
2. `config.toml` next to the executable
3. `config.toml` in the current directory

Copy [`config.example.toml`](config.example.toml) to `config.toml` and adjust.
`config.toml` is ignored by git because it may contain a Twitch OAuth token or
other local-only settings. Keep real tokens out of commits; the example config
contains placeholders only.

Key sections:

| Section | Highlights |
| --- | --- |
| `[twitch]` | `low_latency`, `supported_codecs`, `ad_block`, `api_headers` (OAuth token) |
| `[player]` | `command`, `args`, `no_close`, `include_channel_name` |
| `[stream]` | `ring_buffer_mb`, `live_edge`, `persist_stream`, `recovery_timeout`, segment attempts/threads/timeouts |
| `[retry]` | `streams` (poll interval), `max` (poll count), `open` (open attempts) |
| `[quality]` | `priority` list, e.g. `["1080p60", "1080p", "720p60", "best"]` |

## Usage

```powershell
# channel name or twitch.tv URL; quality is optional (falls back to [quality].priority)
twitchpipe some_channel
twitchpipe https://twitch.tv/some_channel 720p60

# custom config path
twitchpipe --config .\config.toml some_channel

# verify token/usher/playlist fetching + parsing without launching a player
twitchpipe --check some_channel

# override the log level
twitchpipe --log-level debug some_channel

# config overrides (any mode; omitted flags leave the config untouched)
twitchpipe --live-edge 2 --no-ad-block some_channel
```

## Recorder mode

`--record` turns TwitchPipe into a headless recorder: the same HLS pipeline
(low latency, live edge, ad handling, persist/recovery, segment
threads/retries) runs, but the MPEG-TS goes to `--output` instead of a
player. No player is spawned, stdin is never read, logs go to **stderr**, and
it runs fine non-interactively in a container. Without `--record` nothing
changes: `twitchpipe some_channel [quality]` launches the configured player
exactly as before, and none of the recorder flags are accepted.

```sh
twitchpipe --record --output /rec/001.ts --events /rec/001.jsonl \
    --config /etc/twitchpipe.toml --oauth-token-file /run/secrets/twitch \
    https://twitch.tv/some_channel best

# MPEG-TS on stdout, events in a file
twitchpipe --record --output - --events rec.jsonl some_channel 720p60 | ffmpeg -i - ...

# check the whole chain with the recorder's exit codes, record nothing
twitchpipe --record --check some_channel
```

| Flag | Meaning |
| --- | --- |
| `--record` | Enable recorder mode. |
| `--output <PATH\|->` | MPEG-TS destination (required unless `--check`). A new or empty file is used; an existing non-empty file is refused unless `--append`. `-` = stdout. |
| `--events <PATH\|->` | JSON Lines events (see below). Same file rules as `--output`. `-` = stdout (not together with `--output -`). |
| `--append` | Append to existing non-empty output/events files. Byte offsets in events are absolute file offsets (`start.output_offset` is the old length). |
| `--ring-dir <DIR>` | Ring mode instead of `--output`: rotating chunk files in DIR (see [Ring mode](#ring-mode)). Needs `--events`; not with `--append`. |
| `--ring-keep-s <SECS>` | Ring mode: keep at least this many seconds of media (default 300). |
| `--ring-chunk-s <SECS>` | Ring mode: close a chunk at the first segment end once it holds this many seconds (default 60). |
| `--control <PATH>` | Watch this file for output switch requests (see [Output switch](#output-switch---control)). Works with `--output` and `--ring-dir`. |
| `--control-open-timeout <SECS>` | Give up on a switch whose new files are not open after this long (default 120). |

Config overrides, usable in every mode (config file values otherwise apply):
`--oauth-token-file <PATH>` (file contains the token, with or without an
`OAuth ` prefix; replaces any configured `Authorization` header; never
logged), `--live-edge N`, `--low-latency`/`--no-low-latency`,
`--ad-block`/`--no-ad-block`, `--persist`/`--no-persist`,
`--recovery-timeout SECS`, `--segment-threads N`, `--segment-attempts N`,
`--segment-timeout SECS`, `--stream-timeout SECS`, `--retry-streams SECS`,
`--retry-max N`, `--retry-open N`, `--supported-codecs av1,h265,h264`. The
positional quality works as in player mode (`[quality].prefer_config` still
wins over it). Note that low latency clamps the live edge to 1-2; use
`--no-low-latency` for a larger live edge.

Recorder-specific behaviour:

- `stream_timeout` counts *progress* (bytes, or segments being dropped as
  ads), so long filtered ad breaks don't end a recording; it is suspended
  while persist/recovery waits for the stream (`recovery_timeout` governs
  that).
- `retry.open`: if an attempt ends without writing any segment, the stream
  is re-opened (up to `retry.open` attempts) into the same output.
- SIGTERM/SIGINT: fetching stops at once, already-buffered data is written
  (at most ~2 s), the output and events file are flushed, the `end` event is
  written and the process exits (a watchdog forces exit after 6 s). A
  segment cut off by the signal is reported with `complete: false`.

### Ring mode

`--ring-dir DIR` keeps the last minutes of a stream on disk without growing
forever: a loop recorder's pre-roll.

```sh
twitchpipe --record --ring-dir /spool/ring/chan/1731400000 --ring-keep-s 300 \
    --events /spool/ring/chan/1731400000/events.jsonl \
    --control /spool/ring/chan/1731400000/control.json some_channel best
```

- The output goes to `DIR/chunk-000001.ts`, `chunk-000002.ts`, ... (`.mp4`
  for fMP4/CMAF streams). DIR is created if missing and must not hold
  `chunk-*` files already.
- A chunk is closed at the first segment end after it holds
  `--ring-chunk-s` seconds of media, so every chunk holds whole segments.
- After each rotation the oldest chunks are deleted while the remaining ones
  (including the one being written) still hold `--ring-keep-s` seconds.
  Nothing is pruned while an output switch is in flight.
- fMP4: every chunk starts with the current `EXT-X-MAP` init section, so each
  chunk plays on its own and pruning never removes the init. The init bytes
  are part of the chunk's first segment (as at the start of any recording);
  `chunk.init_length` says how many there are, for a consumer that
  concatenates chunks and wants to drop the repeated ones. MPEG-TS chunks
  need nothing.
- Offsets in events are *ring offsets*: positions in the virtual output made
  of every chunk ever written, back to back. Chunk `N` is the byte range
  `offset..offset+length`; a segment at ring offset `o` is at
  `o - chunk.offset` in that chunk's file. Segment events of pruned chunks
  stay in the events file; use `ring.base_offset` to skip them.
- The ring never deletes chunks except by pruning: after a switch, on exit
  or on a signal, the held chunks stay in DIR (the last one is reported
  with `closed_by: "switch"` or `"end"`).

### Output switch (`--control`)

A supervisor can move a running recording to new files without signals and
without a gap: at the next segment boundary the recorder closes the current
output (the ring's open chunk, or a plain `--output`), continues in the new
output and switches its events to a new events file. Switching works any
number of times.

Write the request as one JSON object to the control file, best by writing a
temporary file next to it and renaming it over the path:

```json
{"id": "promote-42", "output": "/rec/part-0002.ts", "events": "/rec/part-0002.events.jsonl"}
```

- `id` (string, required): the request id. `output`, `events` (required):
  new or empty files (as for `--output`; not `-`, not the current files).
  Their directory must exist. `action` is optional and may only be
  `"switch_output"`. Other fields are ignored.
- The file is polled every 200 ms. Every new version of it (a new inode,
  size or modification time) is read as a request. Content already there at
  start-up counts too, so use a fresh control path per process (or remove the
  file first).
- On a new id the recorder writes `output_switch_requested` and opens both
  files **on a thread of its own**: a slow or hung mount never stalls segment
  fetching. Until they are open, recording goes on into the current output
  (a ring keeps rotating but does not prune). If the open fails, or takes
  longer than `--control-open-timeout`, an `output_switch_failed` event is
  written and recording simply continues where it was; nothing is lost.
- Once both files are open, at the next segment boundary:
  1. a gap waiting to be reported at the boundary is written to the old
     events file; a ring writes the `chunk` event of its open chunk
     (`closed_by: "switch"`);
  2. `output_switched` is written to the **old** events file (its last line;
     no `end` follows there) and the old files are closed;
  3. the new events file starts like a fresh recording: `start` (the run's
     `start` with `output` = the new output, `output_offset: 0` and
     `request_id`), then the same `output_switched` event;
  4. the new output starts at offset 0; for fMP4 it starts with the current
     init section (inside its first segment, as for any recording).
  The segment after the boundary is the first one in the new output:
  sequence numbers continue without a gap or a duplicate (a stream gap
  that happens to fall at the boundary is still reported as a `gap`).
- Idempotent: a request whose id was already handled is not executed again;
  its acknowledgement (`output_switched` or `output_switch_failed`) is
  written again to the current events file with `repeat: true`. A request
  for an id that is still in flight is ignored. Requests that arrive while
  another one is in flight are handled after it, in order. To retry a failed
  switch, use a new id.
- If the recording ends before a pending switch happens, the switch is
  reported as `output_switch_failed` with `reason: "recording_ended"` (the
  new files may already exist, empty).
- After a switch the `end` totals and `segment` offsets describe the new
  output only. A `segment_timing` correction for a prefetch segment written
  just before the boundary is dropped (it would point into the old file).

### Exit codes

| Code | `end.reason` | Meaning |
| --- | --- | --- |
| 0 | `stream_ended`, `recovery_timeout` | The stream ended (endlist / no new segments / persist recovery gave up) after data was recorded. |
| 1 | `error` | Internal error. |
| 2 | `usage` | Bad flags, config file or token file (some of these happen before the events file is opened, so no events). |
| 3 | `offline` | Channel offline or nonexistent at start (after `retry.streams`/`retry.max` polling). |
| 4 | `auth_failed` | Twitch refused the access token request (bad OAuth token, integrity check, ...). |
| 5 | `no_matching_quality` | No variant matched the quality / priority list. |
| 6 | `output_error` | Output or events file exists/cannot be created/written. |
| 7 | `stream_error`, `stream_timeout`, `no_data` | Playlist could not be loaded, no progress within `stream_timeout`, or the stream ended before any data. |
| 130 / 143 | `signal` | Stopped by SIGINT / SIGTERM (128 + signal number). |

### Events (`--events`, schema 1)

One JSON object per line, flushed per line. Every object has `ts` (UTC RFC
3339 with milliseconds, e.g. `2026-11-04T17:28:01.123Z`) and `type`. All
dates are UTC RFC 3339 with milliseconds. URLs and tokens are never
included. `start` is the first line when recording starts (absent if it
fails before that); `end` is always the last line (unless the process is
SIGKILLed).

Sequence numbers are Twitch media sequence numbers. A *slot* is a segment
of the live media playlist; normally the slot is written as is, but during
ads it may be dropped or replaced by a substitute segment.

- `start`: `schema`, `version`, `pid`, `channel`, `quality` (variant name),
  `resolution`, `width`, `height`, `framerate`, `codecs`, `bandwidth`,
  `low_latency`, `live_edge` (effective), `ad_block`, `persist_stream`,
  `recovery_timeout`, `segment_threads`, `segment_attempts`,
  `segment_timeout`, `stream_timeout`, `supported_codecs`, `output`,
  `output_offset`, `stream_id`, `title`, `category`. Only when set: `ring`
  (`dir`, `keep_s`, `chunk_s`; `output` is then the ring directory),
  `control` (the control file path), `request_id` (in a `start` written by
  an output switch).
- `segment`: one per segment written, **in output order**, emitted after its
  bytes were written and flushed. `offset` + `length` locate it in the
  output; consecutive segments are contiguous, so offsets plus lengths sum to
  the bytes written. `sequence` is the sequence number of the content
  written, `slot_sequence` the live slot it was written for (equal unless
  substituted). `kind`: `live`, `prefetch` (low-latency
  `EXT-X-TWITCH-PREFETCH` segment), `ad_substitute` (with `source.strategy`
  = `delayed_playback` or `backup_player`, and `source.player_type`), or `ad`
  (ad content written; not produced by the built-in hooks).
  `program_date_time` / `duration` describe the written content
  (EXT-X-PROGRAM-DATE-TIME / EXTINF). For `prefetch` they are estimates
  (`timing_estimated: true`): the parser extrapolates the date and averages
  the duration. `complete: false` (+ `error`) means the body was cut off
  (download failure or signal). `discontinuity: true` means a
  `discontinuity` event precedes it.
- `segment_timing`: final playlist `program_date_time`/`duration` of a
  segment written as `prefetch`, once the segment appears in the playlist as
  a full segment (usually one reload later). Has `sequence`, `offset` (of
  that segment) and the earlier `estimated_*` values. Always after the
  matching `segment` event.
- `gap`: slots `from_sequence..=to_sequence` (`count`) produced no output at
  `offset`. `reason`: `ad_dropped` (ad slot without substitute),
  `ad_block_skipped` (live slot skipped while delayed playback has no
  segment ready; its content usually follows later as delayed content),
  `download_failed` (after all attempts; `error` says why), `playlist_jump`
  (the playlist moved past slots before they were seen),
  `stream_recovery` (slots lost while persist/recovery waited),
  `stream_reopen` (between `retry.open` attempts). `program_date_time` and
  `duration` describe the missing slots when known (null for jumps).
  Consecutive slots with the same reason are merged into one event. Gaps are
  emitted in output order, before the next `segment`.
- `discontinuity`: the next segment does not continue the previous one.
  `reason`: `sequence_jump` (content sequence is not previous + 1, e.g.
  after a gap or a delayed-playback replay), `source_change` (switch between
  the live/delayed sequence space and a backup player type), or
  `hls_discontinuity` (EXT-X-DISCONTINUITY on a contiguous live segment).
  Has `offset`, `previous_sequence`, `sequence`, `previous_kind`, `kind`.
- `ad_start` / `ad_end`: ads appeared/disappeared at the live edge.
  `strategy`: `filter` (ad block off: ad slots are dropped),
  `delayed_playback` (mid-roll) or `backup_player` (pre-roll); `preroll`,
  `sequence`, `break_duration` (from Twitch's daterange, if known);
  `ad_end.catching_up` is true while delayed playback is still behind live.
- `ad_catchup_complete`: delayed playback reached the live edge again
  (`sequence`, `substituted` segment count).
- `stream_down` (`reason`: `no_new_segments` or `segment_fetch_failed`,
  `last_sequence`) / `stream_recovered` (`downtime_s`, `sequence`): only
  with `persist_stream`.
- `warning` / `error`: `code` + `message` (`error.code` equals the `end`
  reason). Warning codes: `retry_open`, `recovery_timeout`, `pipeline`
  (playlist reload failed), `control_invalid`.
- `chunk` (ring mode): a chunk file was closed. `index`, `path`, `offset`
  and `length` (ring offsets), `init_length` (fMP4 init bytes at its head,
  else 0), `segments`, `duration` (sum of segment durations),
  `first_slot_sequence`, `last_slot_sequence`, `first_program_date_time`,
  `last_program_date_time`, `closed_by` (`rotate`, `switch` or `end`).
  Written after the chunk's last `segment` event.
- `ring` (ring mode): `action` `rotate` (after the `chunk` event; `closed` =
  its index) or `prune` (`removed` = the deleted paths). Both describe what
  is held now: `chunks` (indexes of the closed chunks on disk, oldest first),
  `held_bytes` and `held_seconds` (including the chunk being written),
  `base_offset` (ring offset of the oldest byte held),
  `first_slot_sequence`.
- `output_switch_requested`: a switch request was accepted and its files are
  being opened: `request_id`, `output`, `events`.
- `output_switched`: see [Output switch](#output-switch---control).
  `request_id`, `repeat`, `last_slot_sequence` (last live slot seen before
  the boundary, written or not), `last_sequence` (content sequence of the
  last segment written to the old output), `next_slot_sequence` (=
  `last_slot_sequence + 1`: where the new output starts unless a gap
  follows), `program_date_time` (end date of the last segment written to the
  old output: its PDT + duration), `init` (the new output starts with an
  fMP4 init section), `from` {`output` (file or ring directory), `events`,
  `end_offset` (offset just past its last byte; ring offset for a ring),
  `segments`, `bytes`, `chunks` (ring only: the `chunk` objects left on disk,
  oldest first), `error` (only if flushing the old output failed)}, `to`
  {`output`, `events`, `offset` (always 0)}.
- `output_switch_failed`: `request_id` (null if the control file had none),
  `repeat`, `reason` (`invalid`, `open_failed`, `open_timeout`,
  `recording_ended`), `message`, `output`, `events`. A control file that
  cannot be parsed at all gives a `warning` with code `control_invalid`.
- `end`: `reason`, `exit_code`, `signal` (`SIGTERM`/`SIGINT` or null),
  `error`, and totals: `segments`, `bytes`, `gaps`, `gap_segments`,
  `discontinuities`, `ad_substitute_segments`, `incomplete_segments`,
  `media_duration_s` (sum of segment durations), `first_program_date_time`,
  `last_program_date_time`.

`quality_changed` is never emitted: the variant is fixed for a recording
(recovery and re-opens reuse it).

Examples (abridged from a real recording):

```jsonl
{"ts":"2026-11-04T17:28:42.621Z","type":"start","schema":1,"version":"0.1.0","pid":7,"channel":"northernlion","quality":"1080p60","resolution":"1920x1080","width":1920,"height":1080,"framerate":60.0,"codecs":"avc1.64002A,mp4a.40.2","bandwidth":6837838,"low_latency":true,"live_edge":2,"ad_block":false,"persist_stream":false,"recovery_timeout":600.0,"segment_threads":1,"segment_attempts":3,"segment_timeout":10.0,"stream_timeout":60.0,"supported_codecs":["h264"],"output":"/rec/A.ts","output_offset":0,"stream_id":"318534100440","title":"...","category":"Tape to Tape"}
{"ts":"2026-11-04T17:28:44.825Z","type":"segment","sequence":2577,"slot_sequence":2577,"kind":"prefetch","program_date_time":"2026-11-04T17:28:42.531Z","duration":2.0,"timing_estimated":true,"offset":0,"length":1611348,"complete":true,"discontinuity":false,"source":null}
{"ts":"2026-11-04T17:28:45.662Z","type":"segment_timing","sequence":2577,"offset":0,"program_date_time":"2026-11-04T17:28:42.531Z","duration":2.0,"estimated_program_date_time":"2026-11-04T17:28:42.531Z","estimated_duration":2.0}
{"ts":"2026-11-04T17:28:42.991Z","type":"segment","sequence":2571,"slot_sequence":2571,"kind":"live","program_date_time":"2026-11-04T17:28:30.531Z","duration":2.0,"timing_estimated":false,"offset":0,"length":1579576,"complete":true,"discontinuity":false,"source":null}
{"ts":"2026-11-04T17:31:59.804Z","type":"segment","sequence":102,"slot_sequence":102,"kind":"ad_substitute","program_date_time":"2026-01-01T00:00:04.000Z","duration":2.0,"timing_estimated":false,"offset":2200,"length":640,"complete":true,"discontinuity":false,"source":{"strategy":"delayed_playback","player_type":"popout"}}
{"ts":"2026-11-04T17:30:02.060Z","type":"segment","sequence":2616,"slot_sequence":2616,"kind":"prefetch","program_date_time":"2026-11-04T17:30:00.531Z","duration":2.0,"timing_estimated":true,"offset":62380656,"length":1132324,"complete":false,"discontinuity":false,"source":null,"error":"interrupted by signal"}
{"ts":"2026-11-04T17:31:57.662Z","type":"gap","from_sequence":102,"to_sequence":103,"count":2,"reason":"ad_dropped","offset":2200,"program_date_time":"2026-01-01T00:00:04.000Z","duration":4.0}
{"ts":"2026-11-04T17:31:57.662Z","type":"gap","from_sequence":105,"to_sequence":105,"count":1,"reason":"download_failed","offset":2533,"program_date_time":"2026-01-01T00:00:10.000Z","duration":2.0,"error":"segment request failed with status 404 Not Found"}
{"ts":"2026-11-04T17:31:57.662Z","type":"discontinuity","offset":2200,"reason":"sequence_jump","previous_sequence":101,"sequence":104,"previous_kind":"live","kind":"live"}
{"ts":"2026-11-04T17:31:57.357Z","type":"ad_start","strategy":"filter","preroll":false,"sequence":107,"break_duration":4.0}
{"ts":"2026-11-04T17:32:31.020Z","type":"ad_end","strategy":"delayed_playback","sequence":131,"catching_up":true}
{"ts":"2026-11-04T17:33:05.410Z","type":"ad_catchup_complete","strategy":"delayed_playback","sequence":148,"substituted":17}
{"ts":"2026-11-04T18:02:10.003Z","type":"stream_down","reason":"no_new_segments","last_sequence":3480}
{"ts":"2026-11-04T18:03:41.512Z","type":"stream_recovered","downtime_s":91.5,"sequence":3526}
{"ts":"2026-11-04T18:03:41.513Z","type":"warning","code":"pipeline","message":"playlist reload failed: ..."}
{"ts":"2026-11-04T18:05:00.001Z","type":"error","code":"stream_timeout","message":"no stream data within 60s (stream timeout)"}
{"ts":"2026-11-04T17:30:02.061Z","type":"end","reason":"signal","exit_code":143,"signal":"SIGTERM","error":null,"segments":40,"bytes":63512980,"gaps":0,"gap_segments":0,"discontinuities":0,"ad_substitute_segments":0,"incomplete_segments":1,"media_duration_s":80.0,"first_program_date_time":"2026-11-04T17:28:42.531Z","last_program_date_time":"2026-11-04T17:30:00.531Z"}
```

(`start`, `segment`, `segment_timing` and `end` are from a live recording;
`ad_substitute`, `gap`, `discontinuity` and `ad_start` from the fixture tests;
the rest are illustrative.)

`segment.program_date_time` gives wall-clock alignment per segment, so no
separate PDT probe is needed. To cut a recording by time, map the PDT to
`offset` and seek bytes: every complete segment is a whole TS file, so it
starts and ends on TS packet boundaries. A `complete: false` segment may
end mid-packet.

## How it works

```
TwitchApi (GQL) -> Usher URL -> PlaylistWorker -> SegmentFetcher -> byte buffer -> mpv stdin
                                      ^                  ^                     '--> (--record) output file + events
                                      '---- SegmentHook --'   (adblock module)
```

- **PlaylistWorker** reloads the media playlist on a segment-duration cadence
  in low-latency mode (halving the interval when the playlist is unchanged),
  starts at the configured live edge, and — with `persist_stream` — polls for
  recovery for up to `recovery_timeout` seconds when the stream drops.
- **SegmentFetcher** downloads segments in parallel (`segment_threads`) but
  writes them out strictly in order, with per-segment retries and timeouts.
- **Adblock** is fully isolated behind a small hook
  (`on_playlist` / `segment_action`, plus `drain_events` for reporting). With `ad_block = false` the pipeline
  simply skips ad segments. With `ad_block = true`:
  - mid-roll ads trigger *delayed playback*: a fresh access token usually
    serves an ad-free playlist slightly behind live; those segments are
    substituted until ads end, then playback catches back up to the live edge;
  - pre-roll ads fall back to *backup player types* (embed/popout/autoplay),
    which often serve ad-free streams;
  - if neither can supply a segment, ad slots are dropped silently.

## Differences From Streamlink

This is a focused Twitch-live player, not a replacement for mainline
[Streamlink](https://streamlink.github.io/).

- Twitch live streams only. VODs, clips, and other sites/plugins are not
  implemented.
- No client-integrity browser automation. If Twitch requires a
  Client-Integrity token, this tool reports a clear error instead of launching
  a browser/CDP flow.
- Configuration is TOML and intentionally not compatible with Streamlink's
  config syntax.
- Output is mpv-focused and pipes MPEG-TS to the player over stdin.
- HLS handling is tailored for low-latency Twitch playback, local
  persist/recovery, and the custom ad-block behavior from the reference fork.
- The ad-block code is isolated behind a small hook interface so normal HLS
  playback does not depend on the delayed/backup ad logic.
- The optional ffmpeg timestamp-remux path from the Python fork is not included.

## Tests

```powershell
cargo test
cargo clippy --all-targets
```

Unit tests cover the M3U8 parser (prefetch, dateranges, ad flagging,
discontinuity fixes), quality selection, player argument building, the GQL
response parsing and the byte buffer backpressure. Recorder tests pin CLI
parsing (no new flags = player mode), config parsing, event
ordering/offsets/gaps/kinds, and run the real worker + fetcher + recorder
against a local HTTP server serving fixture playlists (including SIGTERM
handling and a check that player mode's byte stream carries no markers).
Ring and switch tests check that chunks tile the output and are pruned to
`--ring-keep-s`, that sequence numbers run on without a gap or duplicate
across several switches (out of a ring and between plain files), that every
fMP4 chunk and switched output starts with exactly one init section, that a
hung open (a FIFO) never stalls the recording, and that repeated request ids
are only re-acknowledged.

On Linux without a local toolchain, everything runs in the `rust:1` image:

```sh
docker run --rm -v "$PWD":/src -w /src rust:1 sh -c \
  'cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test'
```

## License

Released under the [Unlicense](LICENSE).
