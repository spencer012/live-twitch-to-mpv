//! TOML config file loading (no environment variables involved).
//!
//! The config is looked up at the path given via `--config`, or `config.toml`
//! next to the executable, or `config.toml` in the current directory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub twitch: TwitchConfig,
    #[serde(default)]
    pub player: PlayerConfig,
    #[serde(default)]
    pub stream: StreamConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub quality: QualityConfig,
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TwitchConfig {
    /// Enable low-latency streaming (prefetch segments, clamped live edge).
    #[serde(default = "d_true")]
    pub low_latency: bool,
    /// Codec preference signalled to Twitch, e.g. ["av1", "h265", "h264"].
    #[serde(default = "d_codecs")]
    pub supported_codecs: Vec<String>,
    /// Enable the delayed-playback ad-block system.
    #[serde(default)]
    pub ad_block: bool,
    /// Extra headers added to every Twitch GQL API request.
    /// Typically: Authorization = "OAuth <your token>"
    #[serde(default)]
    pub api_headers: BTreeMap<String, String>,
}

impl Default for TwitchConfig {
    fn default() -> Self {
        Self {
            low_latency: true,
            supported_codecs: d_codecs(),
            ad_block: false,
            api_headers: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlayerConfig {
    /// Player executable (searched in PATH if not an absolute path).
    #[serde(default = "d_player")]
    pub command: String,
    /// Raw argument string passed to the player (split respecting quotes).
    #[serde(default)]
    pub args: String,
    /// Don't kill the player when the stream ends (lets mpv --keep-open work).
    #[serde(default)]
    pub no_close: bool,
    /// Pass the channel login to mpv via --script-opts-append=mpv_twitch_report-channel=...
    #[serde(default)]
    pub include_channel_name: bool,
    /// Show player stdout/stderr instead of discarding it.
    #[serde(default)]
    pub verbose: bool,
}

impl Default for PlayerConfig {
    fn default() -> Self {
        Self {
            command: d_player(),
            args: String::new(),
            no_close: false,
            include_channel_name: false,
            verbose: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamConfig {
    /// Output ring buffer size in MiB.
    #[serde(default = "d_ringbuffer")]
    pub ring_buffer_mb: u64,
    /// Number of segments from the live edge to start playback at.
    /// Low-latency mode clamps this to at most 2 (minimum 1).
    #[serde(default = "d_live_edge")]
    pub live_edge: usize,
    /// Start playback at the beginning of the playlist instead of the live edge.
    #[serde(default)]
    pub live_restart: bool,
    /// Keep the stream open when it drops and poll for recovery.
    #[serde(default)]
    pub persist_stream: bool,
    /// Max seconds to wait for stream recovery (0 = wait forever).
    #[serde(default = "d_recovery_timeout")]
    pub recovery_timeout: f64,
    /// Download attempts per segment.
    #[serde(default = "d_segment_attempts")]
    pub segment_attempts: u32,
    /// Parallel segment download threads.
    #[serde(default = "d_segment_threads")]
    pub segment_threads: usize,
    /// Per-segment request timeout in seconds.
    #[serde(default = "d_segment_timeout")]
    pub segment_timeout: f64,
    /// Overall stream read timeout in seconds.
    #[serde(default = "d_stream_timeout")]
    pub stream_timeout: f64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            ring_buffer_mb: d_ringbuffer(),
            live_edge: d_live_edge(),
            live_restart: false,
            persist_stream: false,
            recovery_timeout: d_recovery_timeout(),
            segment_attempts: d_segment_attempts(),
            segment_threads: d_segment_threads(),
            segment_timeout: d_segment_timeout(),
            stream_timeout: d_stream_timeout(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Seconds to wait between stream availability polls (0 = don't poll).
    #[serde(default = "d_retry_streams")]
    pub streams: f64,
    /// Max number of availability polls (0 = infinite).
    #[serde(default)]
    pub max: u64,
    /// Attempts to open the selected stream before giving up.
    #[serde(default = "d_retry_open")]
    pub open: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            streams: d_retry_streams(),
            max: 0,
            open: d_retry_open(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualityConfig {
    /// Quality names tried in order, e.g. ["1080p60", "1080p", "720p60", "best"].
    #[serde(default = "d_priority")]
    pub priority: Vec<String>,
    /// Prefer this priority list over the quality given on the command line.
    #[serde(default)]
    pub prefer_config: bool,
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self {
            priority: d_priority(),
            prefer_config: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    /// trace | debug | info | warn | error
    #[serde(default = "d_loglevel")]
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self { level: d_loglevel() }
    }
}

fn d_true() -> bool {
    true
}
fn d_codecs() -> Vec<String> {
    vec!["h264".into()]
}
fn d_player() -> String {
    "mpv".into()
}
fn d_ringbuffer() -> u64 {
    16
}
fn d_live_edge() -> usize {
    3
}
fn d_recovery_timeout() -> f64 {
    600.0
}
fn d_segment_attempts() -> u32 {
    3
}
fn d_segment_threads() -> usize {
    1
}
fn d_segment_timeout() -> f64 {
    10.0
}
fn d_stream_timeout() -> f64 {
    60.0
}
fn d_retry_streams() -> f64 {
    0.0
}
fn d_retry_open() -> u32 {
    1
}
fn d_priority() -> Vec<String> {
    vec!["best".into()]
}
fn d_loglevel() -> String {
    "info".into()
}

impl Config {
    pub fn load(explicit: Option<&Path>) -> Result<Self> {
        let path = match explicit {
            Some(p) => Some(p.to_path_buf()),
            None => Self::default_path(),
        };

        match path {
            Some(p) if p.exists() => {
                let text = std::fs::read_to_string(&p)
                    .with_context(|| format!("failed to read config file {}", p.display()))?;
                let config: Config = toml::from_str(&text)
                    .with_context(|| format!("failed to parse config file {}", p.display()))?;
                tracing::debug!("Loaded config from {}", p.display());
                Ok(config)
            }
            Some(p) if explicit.is_some() => {
                anyhow::bail!("config file not found: {}", p.display())
            }
            _ => {
                tracing::debug!("No config file found, using defaults");
                Ok(Config::default())
            }
        }
    }

    fn default_path() -> Option<PathBuf> {
        if let Ok(exe) = std::env::current_exe()
            && let Some(dir) = exe.parent()
        {
            let p = dir.join("config.toml");
            if p.exists() {
                return Some(p);
            }
        }
        let p = PathBuf::from("config.toml");
        if p.exists() { Some(p) } else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config() {
        let text = r#"
[twitch]
low_latency = true
supported_codecs = ["av1", "h265", "h264"]
ad_block = true
[twitch.api_headers]
Authorization = "OAuth <test-token>"

[player]
command = "mpv"
args = "--profile='low-latency-stream' --keep-open=always"
no_close = true
include_channel_name = true

[stream]
ring_buffer_mb = 128
live_edge = 1
live_restart = true
persist_stream = true
recovery_timeout = 600
segment_attempts = 10
segment_threads = 2
segment_timeout = 30
stream_timeout = 600

[retry]
streams = 1
max = 600
open = 60

[quality]
priority = ["1080p60", "1080p", "720p60", "720p", "480p", "360p"]
prefer_config = true
"#;
        let config: Config = toml::from_str(text).unwrap();
        assert!(config.twitch.ad_block);
        assert_eq!(config.stream.ring_buffer_mb, 128);
        assert_eq!(config.stream.live_edge, 1);
        assert_eq!(config.quality.priority.len(), 6);
        assert_eq!(
            config.twitch.api_headers.get("Authorization").unwrap(),
            "OAuth <test-token>"
        );
    }

    #[test]
    fn defaults_match_streamlink() {
        let config: Config = toml::from_str("").unwrap();
        assert_eq!(config.stream.ring_buffer_mb, 16);
        assert_eq!(config.stream.live_edge, 3);
        assert_eq!(config.stream.segment_attempts, 3);
        assert_eq!(config.stream.segment_threads, 1);
        assert!((config.stream.segment_timeout - 10.0).abs() < f64::EPSILON);
        assert!((config.stream.stream_timeout - 60.0).abs() < f64::EPSILON);
        assert!(!config.twitch.ad_block);
    }
}
