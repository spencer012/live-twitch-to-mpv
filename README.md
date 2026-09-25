# TwitchPipe

TwitchPipe is a standalone Rust CLI that pipes live Twitch streams into mpv.
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

On Windows, a normal Rust install already puts `~\.cargo\bin` on PATH. To copy only `twitchpipe.exe` there, without adding the whole `target\release` directory:

```powershell
sfw cargo install --path . --locked --target-dir .\target
```

That copies only the executable. Run it again after you rebuild or update the repo.

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
| `[replay]` | `enabled`, `chatterino_url`, `report_interval_ms` |
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

# list available stream qualities, one per line, without launching a player
twitchpipe --streams some_channel

# override the log level
twitchpipe --log-level debug some_channel
```

Before starting mpv, TwitchPipe measures the median duration of recent
completed, non-ad HLS segments. For a cadence of 3 seconds or longer, it tells
the `catchup_live_v2.lua` script to skip mpv's runtime `low-latency` profile
near the live edge. This avoids a decoder restart and keeps mpv's normal
cache-pause behavior on streams with long gaps between keyframes. Catch-up
speed control, the large mpv cache, and Twitch's progressive prefetch remain
enabled. `--check` prints the measured cadence and the profile decision.
If the playlist cannot provide three completed content segments, TwitchPipe
keeps the configured mpv behavior.

## How it works

### Replay chat in Chatterino

Set `[replay] enabled = true` in `config.toml`, then start a Twitch stream in
TwitchPipe. In Chatterino, press Ctrl+Alt+R on that channel's split or choose
"Replay chat from player" in the split's More menu. The split follows mpv's
playback time while it is behind live, and returns to normal live scrolling
near the stream edge. The border is green during replay, blue near live, and
orange while waiting for a player. Hover or the configured pause modifier
temporarily stops tracking so you can read older chat.
While replay follows playback, Chatterino hides "More messages below". If you
pause chat and scroll manually, the banner returns until chat resumes following.

TwitchPipe creates a private mpv IPC endpoint for each process and connects to
Chatterino at `ws://127.0.0.1:38776/replay` by default. If Chatterino is not
running, TwitchPipe keeps retrying and shows one mpv warning. Change
`chatterino_url` if you changed Chatterino's listener port. Replay is available
only for messages still held in the split's scrollback.
TwitchPipe also records each connection change in mpv's backtick console. It
keeps notices until that console is opened, so an early disconnect is visible
later.
With `[player] no_close = true`, TwitchPipe keeps the replay connection alive
after the stream ends. It exits when the remaining mpv window closes, so
seeking through mpv's buffered video can still move replay chat.

Playback positions are reported every 100 ms by default. Chatterino keeps
discrete message jumps but breaks large routine advances into steps of at most
five messages every 50 ms. Seeking in mpv jumps chat directly to the sought
time.

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
