//! Map mpv playback positions to HLS program dates and report them to Chatterino.

use std::collections::VecDeque;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use crate::config::ReplayConfig;
use crate::mpv_ipc::Event;

fn report_disconnect(notices: &mpsc::UnboundedSender<crate::mpv_ipc::Notice>, failed: &mut bool) {
    if !*failed {
        let _ = notices.send(crate::mpv_ipc::Notice {
            text: "Chatterino replay: disconnected; retrying".into(),
            show_osd: true,
        });
        *failed = true;
    }
}

#[derive(Default)]
struct Position {
    time_pos: Option<f64>,
    cache_end: Option<f64>,
    speed: f64,
    paused: bool,
    seeking: bool,
    seeked: bool,
    live_ms: Option<i64>,
    anchors: VecDeque<i64>,
    dirty: bool,
}

impl Position {
    fn update(&mut self, event: Event) {
        match event {
            Event::Connected => {
                self.time_pos = None;
                self.cache_end = None;
                self.anchors.clear();
                self.dirty = true;
            }
            Event::Property(name, value) => match name.as_str() {
                "time-pos" => self.time_pos = value.as_f64(),
                "demuxer-cache-time" => {
                    self.cache_end = value.as_f64();
                    if let (Some(live), Some(end)) = (self.live_ms, self.cache_end) {
                        self.anchors.push_back(live - (end * 1000.0).round() as i64);
                        if self.anchors.len() > 7 {
                            self.anchors.pop_front();
                        }
                    }
                }
                "speed" => {
                    self.speed = value.as_f64().unwrap_or(1.0);
                    self.dirty = true;
                }
                "pause" => {
                    self.paused = value.as_bool().unwrap_or(false);
                    self.dirty = true;
                }
                "seeking" => {
                    let seeking = value.as_bool().unwrap_or(false);
                    if seeking && !self.seeking {
                        self.seeked = true;
                    }
                    self.seeking = seeking;
                    self.dirty = true;
                }
                "eof-reached" => self.dirty = true,
                _ => {}
            },
        }
    }

    fn wall_ms(&self) -> Option<i64> {
        let pos = self.time_pos?;
        let mut anchors: Vec<_> = self.anchors.iter().copied().collect();
        anchors.sort_unstable();
        Some(anchors.get(anchors.len() / 2)? + (pos * 1000.0).round() as i64)
    }
}

pub async fn run(
    config: ReplayConfig,
    channel: String,
    ipc_path: String,
    mut segment_ends: mpsc::UnboundedReceiver<i64>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let (event_tx, mut events) = mpsc::unbounded_channel();
    let (notice_tx, notice_rx) = mpsc::unbounded_channel();
    let ipc_task = tokio::spawn(crate::mpv_ipc::run(ipc_path, event_tx, notice_rx));
    let instance = format!("{}-{}", std::process::id(), channel);
    let mut state = Position {
        speed: 1.0,
        ..Default::default()
    };
    let mut interval =
        tokio::time::interval(Duration::from_millis(config.report_interval_ms.max(50)));
    let mut retry = Duration::from_secs(1);
    let mut failed = false;
    let mut socket = None;
    let (closed_tx, mut closed_rx) = mpsc::unbounded_channel();
    let mut reader_task = None;
    let mut connection_id = 0u64;

    'reports: loop {
        if socket.is_none() {
            match tokio_tungstenite::connect_async(&config.chatterino_url).await {
                Ok((mut connected, _)) => {
                    let hello = json!({"type":"hello", "instance":instance, "channel":channel,
                                       "pid":std::process::id()});
                    if connected
                        .send(Message::Text(hello.to_string().into()))
                        .await
                        .is_ok()
                    {
                        let _ = notice_tx.send(crate::mpv_ipc::Notice {
                            text: "Chatterino replay: connected".into(),
                            show_osd: failed,
                        });
                        failed = false;
                        retry = Duration::from_secs(1);
                        state.dirty = true;
                        connection_id += 1;
                        let id = connection_id;
                        let (write, mut read) = connected.split();
                        let closed_tx = closed_tx.clone();
                        reader_task = Some(tokio::spawn(async move {
                            while let Some(Ok(_)) = read.next().await {}
                            let _ = closed_tx.send(id);
                        }));
                        socket = Some(write);
                    } else {
                        report_disconnect(&notice_tx, &mut failed);
                    }
                }
                Err(error) => {
                    if !failed {
                        tracing::warn!("Chatterino replay: {error}; retrying");
                        report_disconnect(&notice_tx, &mut failed);
                    } else {
                        tracing::debug!("Chatterino replay reconnect: {error}");
                    }
                }
            }
            if socket.is_none() {
                let sleep = tokio::time::sleep(retry);
                tokio::pin!(sleep);
                loop {
                    tokio::select! {
                        _ = &mut sleep => break,
                        Some(end) = segment_ends.recv() => state.live_ms = Some(end),
                        Some(event) = events.recv() => state.update(event),
                        _ = &mut shutdown => break 'reports,
                    }
                }
                retry = (retry * 2).min(Duration::from_secs(30));
                continue;
            }
        }

        let mut report = false;
        tokio::select! {
            _ = &mut shutdown => break,
            Some(id) = closed_rx.recv() => {
                if id == connection_id {
                    socket = None;
                    if let Some(task) = reader_task.take() { task.abort(); }
                    report_disconnect(&notice_tx, &mut failed);
                }
            }
            Some(end) = segment_ends.recv() => state.live_ms = Some(end),
            Some(event) = events.recv() => {
                state.update(event);
                report = state.dirty;
            },
            _ = interval.tick() => report = true,
        }
        if report && !state.seeking && (!state.paused || state.dirty) {
            if let (Some(wall_ms), Some(live_ms), Some(ws)) =
                (state.wall_ms(), state.live_ms, socket.as_mut())
            {
                let position = json!({"type":"position", "instance":instance,
                    "channel":channel, "wall_ms":wall_ms, "speed":state.speed,
                    "paused":state.paused, "live_ms":live_ms, "seeked":state.seeked});
                tracing::debug!("Replay position: {position}");
                if ws
                    .send(Message::Text(position.to_string().into()))
                    .await
                    .is_err()
                {
                    socket = None;
                    if let Some(task) = reader_task.take() {
                        task.abort();
                    }
                    report_disconnect(&notice_tx, &mut failed);
                } else {
                    state.dirty = false;
                    state.seeked = false;
                }
            }
        }
    }
    if let Some(mut ws) = socket {
        let bye = json!({"type":"bye", "instance":instance});
        let _ = ws.send(Message::Text(bye.to_string().into())).await;
        let _ = ws.close().await;
    }
    ipc_task.abort();
    if let Some(task) = reader_task {
        task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_anchor_maps_seeked_position() {
        let mut position = Position {
            time_pos: Some(15.0),
            ..Default::default()
        };
        position.anchors.extend([1_000_000, 1_001_000, 999_000]);
        assert_eq!(position.wall_ms(), Some(1_015_000));
        position.time_pos = Some(2.0);
        assert_eq!(position.wall_ms(), Some(1_002_000));
    }

    #[test]
    fn seek_is_reported_once_when_it_starts() {
        let mut position = Position::default();
        position.update(Event::Property("seeking".into(), json!(true)));
        assert!(position.seeked);
        position.seeked = false;
        position.update(Event::Property("seeking".into(), json!(false)));
        assert!(!position.seeked);
    }
}
