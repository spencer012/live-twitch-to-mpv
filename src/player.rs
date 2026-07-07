//! mpv player output: spawn with the stream piped to stdin.

use std::process::Stdio;

use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};

use crate::config::PlayerConfig;

/// Split a raw argument string respecting single and double quotes,
/// e.g. `--profile='a,b' --keep-open=always` -> ["--profile=a,b", "--keep-open=always"].
pub fn split_args(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_token = false;
    let mut quote: Option<char> = None;

    for c in input.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                } else {
                    current.push(c);
                }
            }
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    in_token = true;
                }
                c if c.is_whitespace() => {
                    if in_token {
                        args.push(std::mem::take(&mut current));
                        in_token = false;
                    }
                }
                c => {
                    current.push(c);
                    in_token = true;
                }
            },
        }
    }
    if in_token {
        args.push(current);
    }
    args
}

/// Build the full player argument list.
///
/// - stream is delivered on stdin (`-`)
/// - `--force-media-title=<title>` (mpv window title)
/// - `--script-opts-append=mpv_twitch_report-channel=<channel>` when
///   `include_channel_name` is enabled (the fork's mechanism)
pub fn build_player_args(
    config: &PlayerConfig,
    channel: &str,
    title: Option<&str>,
) -> Vec<String> {
    let mut args = Vec::new();

    let media_title = title.filter(|t| !t.is_empty()).unwrap_or(channel);
    args.push(format!("--force-media-title={media_title}"));

    args.extend(split_args(&config.args));

    if config.include_channel_name {
        args.push(format!(
            "--script-opts-append=mpv_twitch_report-channel={}",
            channel.to_lowercase()
        ));
    }

    args.push("-".to_string());
    args
}

pub struct Player {
    pub child: Child,
    pub stdin: ChildStdin,
    no_close: bool,
}

impl Player {
    pub fn spawn(config: &PlayerConfig, channel: &str, title: Option<&str>) -> Result<Player> {
        let args = build_player_args(config, channel, title);
        tracing::info!("Starting player: {} {}", config.command, args.join(" "));

        let (stdout, stderr) = if config.verbose {
            (Stdio::inherit(), Stdio::inherit())
        } else {
            (Stdio::null(), Stdio::null())
        };

        let mut child = Command::new(&config.command)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .with_context(|| format!("failed to start player: {}", config.command))?;

        let stdin = child.stdin.take().context("player stdin unavailable")?;

        Ok(Player {
            child,
            stdin,
            no_close: config.no_close,
        })
    }

    /// Write a chunk to the player. An error means the player was closed.
    pub async fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.stdin.write_all(data).await
    }

    /// The stream has ended. Close stdin to signal EOF; with `no_close`, wait
    /// for the player to exit on its own (mpv --keep-open), otherwise kill it.
    pub async fn finish(mut self) -> Result<()> {
        drop(self.stdin);
        if self.no_close {
            tracing::info!("Stream ended; leaving player open (player.no_close)");
            let _ = self.child.wait().await;
        } else {
            let _ = self.child.kill().await;
            let _ = self.child.wait().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_plain_args() {
        assert_eq!(
            split_args("--keep-open=always --loop=no"),
            vec!["--keep-open=always", "--loop=no"]
        );
    }

    #[test]
    fn splits_quoted_args() {
        assert_eq!(
            split_args("--profile='low-latency-stream,stream-start-enabled-catchup' --keep-open=always"),
            vec![
                "--profile=low-latency-stream,stream-start-enabled-catchup",
                "--keep-open=always"
            ]
        );
    }

    #[test]
    fn splits_double_quoted_args() {
        assert_eq!(
            split_args(r#"--title="my stream title" -v"#),
            vec!["--title=my stream title", "-v"]
        );
    }

    #[test]
    fn empty_args() {
        assert!(split_args("").is_empty());
        assert!(split_args("   ").is_empty());
    }

    #[test]
    fn builds_full_arg_list() {
        let config = PlayerConfig {
            command: "mpv".into(),
            args: "--keep-open=always".into(),
            no_close: true,
            include_channel_name: true,
            verbose: false,
        };
        let args = build_player_args(&config, "SomeChannel", Some("Stream Title"));
        assert_eq!(
            args,
            vec![
                "--force-media-title=Stream Title",
                "--keep-open=always",
                "--script-opts-append=mpv_twitch_report-channel=somechannel",
                "-",
            ]
        );
    }

    #[test]
    fn falls_back_to_channel_title() {
        let config = PlayerConfig::default();
        let args = build_player_args(&config, "SomeChannel", None);
        assert_eq!(args[0], "--force-media-title=SomeChannel");
        assert_eq!(args.last().unwrap(), "-");
    }
}
