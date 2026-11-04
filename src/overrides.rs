//! Command-line overrides for config-file settings (mainly for automation).
//!
//! Every flag is optional; with none given the config is left untouched, so
//! existing invocations behave exactly as before.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::config::Config;

#[derive(Args, Debug, Default, Clone, PartialEq)]
#[command(next_help_heading = "Config overrides")]
pub struct Overrides {
    /// Read the Twitch OAuth token from this file (contents = the token,
    /// optionally prefixed with "OAuth "). Overrides any Authorization
    /// header from the config. The token is never logged.
    #[arg(long, value_name = "PATH")]
    pub oauth_token_file: Option<PathBuf>,

    /// Segments back from the live edge to start at (low latency clamps to 1-2)
    #[arg(long, value_name = "N")]
    pub live_edge: Option<usize>,

    /// Enable low-latency streaming
    #[arg(long, overrides_with = "no_low_latency")]
    pub low_latency: bool,
    /// Disable low-latency streaming
    #[arg(long, overrides_with = "low_latency")]
    pub no_low_latency: bool,

    /// Enable the delayed-playback/backup ad-block system
    #[arg(long, overrides_with = "no_ad_block")]
    pub ad_block: bool,
    /// Disable ad-block (ad segments are dropped)
    #[arg(long, overrides_with = "ad_block")]
    pub no_ad_block: bool,

    /// Keep the stream open when it drops and poll for recovery
    #[arg(long, overrides_with = "no_persist")]
    pub persist: bool,
    /// Don't poll for recovery when the stream drops
    #[arg(long, overrides_with = "persist")]
    pub no_persist: bool,

    /// Max seconds to wait for stream recovery (0 = forever)
    #[arg(long, value_name = "SECS")]
    pub recovery_timeout: Option<f64>,

    /// Parallel segment downloads
    #[arg(long, value_name = "N")]
    pub segment_threads: Option<usize>,

    /// Download attempts per segment
    #[arg(long, value_name = "N")]
    pub segment_attempts: Option<u32>,

    /// Per-segment request timeout in seconds
    #[arg(long, value_name = "SECS")]
    pub segment_timeout: Option<f64>,

    /// Stream read timeout in seconds
    #[arg(long, value_name = "SECS")]
    pub stream_timeout: Option<f64>,

    /// Seconds between stream availability polls (0 = don't poll)
    #[arg(long, value_name = "SECS")]
    pub retry_streams: Option<f64>,

    /// Max availability polls (0 = infinite)
    #[arg(long, value_name = "N")]
    pub retry_max: Option<u64>,

    /// Attempts to open the selected stream
    #[arg(long, value_name = "N")]
    pub retry_open: Option<u32>,

    /// Codec preference signalled to Twitch, comma separated (e.g. av1,h265,h264)
    #[arg(long, value_name = "LIST", value_delimiter = ',')]
    pub supported_codecs: Option<Vec<String>>,
}

fn pair(yes: bool, no: bool) -> Option<bool> {
    match (yes, no) {
        (true, _) => Some(true),
        (_, true) => Some(false),
        _ => None,
    }
}

impl Overrides {
    /// Apply the given overrides on top of the loaded config.
    pub fn apply(&self, config: &mut Config) -> Result<()> {
        if let Some(path) = &self.oauth_token_file {
            let token = read_oauth_token(path)?;
            config
                .twitch
                .api_headers
                .retain(|k, _| !k.eq_ignore_ascii_case("authorization"));
            config
                .twitch
                .api_headers
                .insert("Authorization".into(), format!("OAuth {token}"));
            tracing::debug!("Using the OAuth token from --oauth-token-file");
        }
        if let Some(v) = self.live_edge {
            config.stream.live_edge = v;
        }
        if let Some(v) = pair(self.low_latency, self.no_low_latency) {
            config.twitch.low_latency = v;
        }
        if let Some(v) = pair(self.ad_block, self.no_ad_block) {
            config.twitch.ad_block = v;
        }
        if let Some(v) = pair(self.persist, self.no_persist) {
            config.stream.persist_stream = v;
        }
        if let Some(v) = self.recovery_timeout {
            config.stream.recovery_timeout = non_negative("--recovery-timeout", v)?;
        }
        if let Some(v) = self.segment_threads {
            config.stream.segment_threads = v;
        }
        if let Some(v) = self.segment_attempts {
            config.stream.segment_attempts = v;
        }
        if let Some(v) = self.segment_timeout {
            config.stream.segment_timeout = positive("--segment-timeout", v)?;
        }
        if let Some(v) = self.stream_timeout {
            config.stream.stream_timeout = positive("--stream-timeout", v)?;
        }
        if let Some(v) = self.retry_streams {
            config.retry.streams = non_negative("--retry-streams", v)?;
        }
        if let Some(v) = self.retry_max {
            config.retry.max = v;
        }
        if let Some(v) = self.retry_open {
            config.retry.open = v;
        }
        if let Some(v) = &self.supported_codecs {
            config.twitch.supported_codecs = v
                .iter()
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
        Ok(())
    }
}

fn positive(flag: &str, v: f64) -> Result<f64> {
    if v.is_finite() && v > 0.0 {
        Ok(v)
    } else {
        bail!("{flag} must be a positive number of seconds")
    }
}

fn non_negative(flag: &str, v: f64) -> Result<f64> {
    if v.is_finite() && v >= 0.0 {
        Ok(v)
    } else {
        bail!("{flag} must be zero or a positive number of seconds")
    }
}

/// Read and validate an OAuth token file. Error messages never include the
/// file contents.
pub fn read_oauth_token(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read OAuth token file {}", path.display()))?;
    parse_oauth_token(&text).with_context(|| format!("invalid OAuth token file {}", path.display()))
}

fn parse_oauth_token(text: &str) -> Result<String> {
    let mut token = text.trim();
    if token.len() >= 6 && token[..6].eq_ignore_ascii_case("oauth ") {
        token = token[6..].trim_start();
    }
    if token.is_empty() {
        bail!("the file is empty");
    }
    if !token.chars().all(|c| c.is_ascii_graphic()) {
        bail!("the token must be a single line of printable ASCII characters");
    }
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_overrides_leave_config_untouched() {
        let text =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.toml"))
                .unwrap();
        for original in [Config::default(), toml::from_str::<Config>(&text).unwrap()] {
            let mut config = original.clone();
            Overrides::default().apply(&mut config).unwrap();
            assert_eq!(format!("{config:?}"), format!("{original:?}"));
        }
    }

    #[test]
    fn overrides_apply() {
        let mut config = Config::default();
        let o = Overrides {
            live_edge: Some(6),
            no_low_latency: true,
            ad_block: true,
            persist: true,
            recovery_timeout: Some(120.0),
            segment_threads: Some(3),
            segment_attempts: Some(7),
            segment_timeout: Some(15.0),
            stream_timeout: Some(90.0),
            retry_streams: Some(2.0),
            retry_max: Some(5),
            retry_open: Some(3),
            supported_codecs: Some(vec!["av1".into(), " h264 ".into()]),
            ..Default::default()
        };
        o.apply(&mut config).unwrap();
        assert_eq!(config.stream.live_edge, 6);
        assert!(!config.twitch.low_latency);
        assert!(config.twitch.ad_block);
        assert!(config.stream.persist_stream);
        assert_eq!(config.stream.recovery_timeout, 120.0);
        assert_eq!(config.stream.segment_threads, 3);
        assert_eq!(config.stream.segment_attempts, 7);
        assert_eq!(config.stream.segment_timeout, 15.0);
        assert_eq!(config.stream.stream_timeout, 90.0);
        assert_eq!(config.retry.streams, 2.0);
        assert_eq!(config.retry.max, 5);
        assert_eq!(config.retry.open, 3);
        assert_eq!(config.twitch.supported_codecs, vec!["av1", "h264"]);

        // Disabling flags win over config values.
        let mut config = Config::default();
        config.twitch.ad_block = true;
        config.stream.persist_stream = true;
        Overrides {
            no_ad_block: true,
            no_persist: true,
            low_latency: true,
            ..Default::default()
        }
        .apply(&mut config)
        .unwrap();
        assert!(!config.twitch.ad_block);
        assert!(!config.stream.persist_stream);
        assert!(config.twitch.low_latency);
    }

    #[test]
    fn rejects_bad_timeouts() {
        let mut config = Config::default();
        let o = Overrides {
            stream_timeout: Some(0.0),
            ..Default::default()
        };
        assert!(o.apply(&mut config).is_err());
    }

    #[test]
    fn oauth_token_parsing() {
        assert_eq!(parse_oauth_token("abc123\n").unwrap(), "abc123");
        assert_eq!(parse_oauth_token("OAuth abc123").unwrap(), "abc123");
        assert_eq!(parse_oauth_token("oauth  abc123 ").unwrap(), "abc123");
        assert!(parse_oauth_token("  \n").is_err());
        assert!(parse_oauth_token("abc 123").is_err());
        assert!(parse_oauth_token("abc\n123").is_err());
    }

    #[test]
    fn oauth_token_file_replaces_config_header_without_leaking() {
        let dir = std::env::temp_dir().join(format!("twitchpipe-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        std::fs::write(&path, "secret-token-value\n").unwrap();

        let mut config = Config::default();
        config
            .twitch
            .api_headers
            .insert("authorization".into(), "OAuth old".into());
        Overrides {
            oauth_token_file: Some(path.clone()),
            ..Default::default()
        }
        .apply(&mut config)
        .unwrap();
        assert_eq!(config.twitch.api_headers.len(), 1);
        assert_eq!(
            config.twitch.api_headers.get("Authorization").unwrap(),
            "OAuth secret-token-value"
        );

        std::fs::write(&path, "bad token\n").unwrap();
        let err = Overrides {
            oauth_token_file: Some(path.clone()),
            ..Default::default()
        }
        .apply(&mut Config::default())
        .unwrap_err();
        assert!(!format!("{err:#}").contains("bad token"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
