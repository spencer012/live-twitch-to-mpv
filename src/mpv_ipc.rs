//! mpv's newline-delimited JSON IPC connection.

use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum Event {
    Property(String, Value),
    Connected,
}

pub struct Notice {
    pub text: String,
    pub show_osd: bool,
}

#[cfg(windows)]
async fn connect(
    path: &str,
) -> std::io::Result<impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> {
    tokio::net::windows::named_pipe::ClientOptions::new().open(path)
}

#[cfg(unix)]
async fn connect(
    path: &str,
) -> std::io::Result<impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> {
    tokio::net::UnixStream::connect(path).await
}

pub async fn run(
    path: String,
    events: mpsc::UnboundedSender<Event>,
    mut notices: mpsc::UnboundedReceiver<Notice>,
) {
    loop {
        match connect(&path).await {
            Ok(stream) => {
                tracing::debug!("Connected to mpv IPC at {path}");
                let _ = events.send(Event::Connected);
                let (reader, mut writer) = tokio::io::split(stream);
                let mut lines = BufReader::new(reader).lines();
                for (id, name) in [
                    "time-pos",
                    "speed",
                    "pause",
                    "demuxer-cache-time",
                    "eof-reached",
                    "seeking",
                ]
                .iter()
                .enumerate()
                {
                    let msg = json!({"command": ["observe_property", id + 1, name]});
                    if writer
                        .write_all(format!("{msg}\n").as_bytes())
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                loop {
                    tokio::select! {
                        line = lines.next_line() => match line {
                            Ok(Some(line)) => {
                                if let Ok(value) = serde_json::from_str::<Value>(&line)
                                    && value["event"] == "property-change"
                                    && let Some(name) = value["name"].as_str() {
                                        let _ = events.send(Event::Property(name.to_owned(), value["data"].clone()));
                                }
                            }
                            _ => break,
                        },
                        Some(notice) = notices.recv() => {
                            let command = json!({"command": ["script-message-to",
                                crate::player::replay_script_name(), "status", notice.text]});
                            if writer.write_all(format!("{command}\n").as_bytes()).await.is_err() {
                                break;
                            }
                            if notice.show_osd {
                                let command = json!({"command": ["show-text", notice.text, 3000]});
                                if writer.write_all(format!("{command}\n").as_bytes()).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
                tracing::debug!("mpv IPC disconnected; reconnecting");
            }
            Err(error) => tracing::debug!("Waiting for mpv IPC: {error}"),
        }
        if events.is_closed() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
