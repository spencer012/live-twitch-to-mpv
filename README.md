# streamlink-rust

A standalone Rust CLI that plays Twitch live streams through mpv. It is a
focused port of a custom streamlink fork: low-latency HLS pipeline,
persist/recovery for dropped streams, and a segmented ad-block system
(delayed playback for mid-rolls, backup player types for pre-rolls).

Live streams only — no VODs, no clips, and no client-integrity browser flow
(access-token failures produce a clear error instead).

## Build

Requires a recent stable Rust toolchain.

```powershell
cd streamlink-rust
cargo build --release
```

The binary ends up at `target\release\streamlink-rust.exe`.

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
streamlink-rust some_channel
streamlink-rust https://twitch.tv/some_channel 720p60

# custom config path
streamlink-rust --config .\config.toml some_channel

# verify token/usher/playlist fetching + parsing without launching a player
streamlink-rust --check some_channel

# override the log level
streamlink-rust --log-level debug some_channel
```

## How it works

```
TwitchApi (GQL) -> Usher URL -> PlaylistWorker -> SegmentFetcher -> byte buffer -> mpv stdin
                                      ^                  ^
                                      '---- SegmentHook --'   (adblock module)
```

- **PlaylistWorker** reloads the media playlist on a segment-duration cadence
  in low-latency mode (halving the interval when the playlist is unchanged),
  starts at the configured live edge, and — with `persist_stream` — polls for
  recovery for up to `recovery_timeout` seconds when the stream drops.
- **SegmentFetcher** downloads segments in parallel (`segment_threads`) but
  writes them out strictly in order, with per-segment retries and timeouts.
- **Adblock** is fully isolated behind a two-method hook
  (`on_playlist` / `segment_action`). With `ad_block = false` the pipeline
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
response parsing and the byte buffer backpressure.

## License

Released under the [Unlicense](LICENSE).
