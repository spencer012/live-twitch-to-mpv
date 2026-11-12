//! Output switch requests (`--control PATH`).
//!
//! A supervisor writes one JSON object to the control file (best: write a
//! temporary file and rename it over the path):
//!
//! ```json
//! {"id": "promote-42", "output": "/rec/part-0002.mp4", "events": "/rec/part-0002.events.jsonl"}
//! ```
//!
//! A watcher thread polls the file and reports every new version (a changed
//! inode, size or modification time), so writing the same request again
//! re-acknowledges it. Opening the new files happens on a thread of its own:
//! a hung network mount never stalls segment fetching or the current output,
//! and the switch itself happens at the first segment boundary after both
//! files are open.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::mpsc;

use super::events;

/// How often the control file is checked.
pub const POLL: Duration = Duration::from_millis(200);
pub const ACTION_SWITCH: &str = "switch_output";

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SwitchRequest {
    pub id: String,
    /// Only "switch_output" (the default).
    #[serde(default)]
    pub action: Option<String>,
    pub output: PathBuf,
    pub events: PathBuf,
}

/// Files opened for a switch, waiting for the next segment boundary.
#[derive(Debug)]
pub struct Opened {
    pub output: std::fs::File,
    pub events: std::fs::File,
}

#[derive(Debug)]
pub enum ControlMsg {
    Request(SwitchRequest),
    /// Unusable control file content (`id` if one could be read).
    Invalid {
        id: Option<String>,
        message: String,
    },
    Opened {
        id: String,
        result: Result<Opened, String>,
    },
}

pub type ControlTx = mpsc::UnboundedSender<ControlMsg>;
pub type ControlRx = mpsc::UnboundedReceiver<ControlMsg>;

/// Parse one version of the control file.
pub fn parse(data: &[u8]) -> ControlMsg {
    let value: serde_json::Value = match serde_json::from_slice(data) {
        Ok(v) => v,
        Err(err) => {
            return ControlMsg::Invalid {
                id: None,
                message: format!("control file is not JSON: {err}"),
            };
        }
    };
    let id = value.get("id").and_then(|v| v.as_str()).map(str::to_string);
    let req: SwitchRequest = match serde_json::from_value(value) {
        Ok(req) => req,
        Err(err) => {
            return ControlMsg::Invalid {
                id,
                message: format!("invalid control request: {err}"),
            };
        }
    };
    if let Some(action) = &req.action
        && action != ACTION_SWITCH
    {
        return ControlMsg::Invalid {
            id,
            message: format!("unknown control action {action:?}"),
        };
    }
    if req.id.is_empty() {
        return ControlMsg::Invalid {
            id: None,
            message: "control request has an empty id".into(),
        };
    }
    ControlMsg::Request(req)
}

/// Check a request against the current output/events paths.
pub fn validate(req: &SwitchRequest, current: &[&Path]) -> Result<(), String> {
    let stdout = Path::new("-");
    for path in [&req.output, &req.events] {
        if path.as_os_str().is_empty() || path == stdout {
            return Err(format!(
                "{:?} is not a file path",
                path.display().to_string()
            ));
        }
        if current.contains(&path.as_path()) {
            return Err(format!(
                "{} is the current output or events file",
                path.display()
            ));
        }
    }
    if req.output == req.events {
        return Err("output and events must be different files".into());
    }
    Ok(())
}

/// Identity of one version of the control file.
#[derive(Debug, Clone, PartialEq)]
struct Version {
    ino: u64,
    len: u64,
    modified: Option<std::time::SystemTime>,
}

fn version(meta: &std::fs::Metadata) -> Version {
    #[cfg(unix)]
    let ino = {
        use std::os::unix::fs::MetadataExt;
        meta.ino()
    };
    #[cfg(not(unix))]
    let ino = 0;
    Version {
        ino,
        len: meta.len(),
        modified: meta.modified().ok(),
    }
}

/// Poll `path` on a thread of its own (a hung file system blocks only that
/// thread) and send every new version. Content present at start-up counts
/// as a request. Ends when the receiver is gone.
pub fn spawn_watcher(path: PathBuf, tx: ControlTx) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("twitchpipe-control".into())
        .spawn(move || {
            let mut last: Option<Version> = None;
            while !tx.is_closed() {
                match std::fs::metadata(&path) {
                    Ok(meta) => {
                        let v = version(&meta);
                        if last.as_ref() != Some(&v) {
                            last = Some(v);
                            match std::fs::read(&path) {
                                Ok(data) if data.trim_ascii().is_empty() => {}
                                Ok(data) => {
                                    let _ = tx.send(parse(&data));
                                }
                                Err(err) => {
                                    let _ = tx.send(ControlMsg::Invalid {
                                        id: None,
                                        message: format!("cannot read control file: {err}"),
                                    });
                                }
                            }
                        }
                    }
                    // removed: a later file with identical metadata is new
                    Err(_) => last = None,
                }
                std::thread::sleep(POLL);
            }
        })
        .map(|_| ())
}

/// Open the request's files on a thread of its own and report the result.
/// Once `abandoned` is set (the open timed out), files that open late are
/// closed again on that thread.
pub fn spawn_open(req: &SwitchRequest, tx: ControlTx, abandoned: Arc<AtomicBool>) {
    let id = req.id.clone();
    let output = req.output.clone();
    let events_path = req.events.clone();
    let spawned = std::thread::Builder::new()
        .name("twitchpipe-open".into())
        .spawn({
            let tx = tx.clone();
            let id = id.clone();
            move || {
                let open = |path: &Path| {
                    events::open_new_or_append(path, false)
                        .map(|(file, _)| file)
                        .map_err(|e| format!("cannot open {}: {e}", path.display()))
                };
                let result = open(&output).and_then(|output| {
                    let events = open(&events_path)?;
                    Ok(Opened { output, events })
                });
                if abandoned.load(Ordering::SeqCst) {
                    return;
                }
                let _ = tx.send(ControlMsg::Opened { id, result });
            }
        });
    if let Err(err) = spawned {
        let _ = tx.send(ControlMsg::Opened {
            id,
            result: Err(format!("cannot start the open thread: {err}")),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_requests() {
        let msg = parse(br#"{"id":"a","output":"/x/o.ts","events":"/x/o.jsonl","extra":1}"#);
        let ControlMsg::Request(req) = msg else {
            panic!("{msg:?}")
        };
        assert_eq!(req.id, "a");
        assert_eq!(req.output, PathBuf::from("/x/o.ts"));
        assert!(matches!(
            parse(br#"{"id":"b","output":"/x/o.ts"}"#),
            ControlMsg::Invalid { id: Some(id), .. } if id == "b"
        ));
        assert!(matches!(
            parse(br#"{"id":"c","action":"stop","output":"/o","events":"/e"}"#),
            ControlMsg::Invalid { id: Some(_), .. }
        ));
        assert!(matches!(
            parse(b"{not json"),
            ControlMsg::Invalid { id: None, .. }
        ));
        assert!(matches!(
            parse(br#"{"id":"","output":"/o","events":"/e"}"#),
            ControlMsg::Invalid { id: None, .. }
        ));
    }

    #[test]
    fn validates_paths() {
        let req = |o: &str, e: &str| SwitchRequest {
            id: "x".into(),
            action: None,
            output: o.into(),
            events: e.into(),
        };
        let cur = [Path::new("/rec/a.ts"), Path::new("/rec/a.jsonl")];
        assert!(validate(&req("/rec/b.ts", "/rec/b.jsonl"), &cur).is_ok());
        assert!(validate(&req("-", "/rec/b.jsonl"), &cur).is_err());
        assert!(validate(&req("/rec/a.ts", "/rec/b.jsonl"), &cur).is_err());
        assert!(validate(&req("/rec/b.ts", "/rec/b.ts"), &cur).is_err());
    }

    #[tokio::test]
    async fn watcher_reports_each_new_version() {
        let dir = std::env::temp_dir().join(format!("twitchpipe-ctl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("control.json");
        let write = |body: &str| {
            let tmp = dir.join("control.tmp");
            std::fs::write(&tmp, body).unwrap();
            std::fs::rename(&tmp, &path).unwrap();
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        spawn_watcher(path.clone(), tx).unwrap();
        let next = async |rx: &mut ControlRx| {
            tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("watcher reported")
                .unwrap()
        };
        let req = r#"{"id":"r1","output":"/o1","events":"/e1"}"#;
        write(req);
        assert!(matches!(next(&mut rx).await, ControlMsg::Request(r) if r.id == "r1"));
        // the same request written again (new inode): reported again
        write(req);
        assert!(matches!(next(&mut rx).await, ControlMsg::Request(r) if r.id == "r1"));
        write("garbage");
        assert!(matches!(next(&mut rx).await, ControlMsg::Invalid { .. }));
        // nothing new: nothing reported
        tokio::time::sleep(POLL * 3).await;
        assert!(rx.try_recv().is_err());
        drop(rx);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
