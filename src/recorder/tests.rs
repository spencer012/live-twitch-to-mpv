//! End-to-end recorder tests: the real worker + fetcher + recorder against a
//! tiny local HTTP server serving fixture playlists and segments.

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};

use super::*;
use crate::hls::hook::{AdFilterHook, SegmentAction, SegmentHook, Substitute};
use crate::hls::m3u8::{MediaPlaylist, MediaSegment};

#[derive(Clone)]
struct Route {
    body: Vec<u8>,
    delay: Duration,
    /// Send only this many body bytes, then stall forever.
    stall_after: Option<usize>,
}

fn route(body: Vec<u8>) -> Route {
    Route {
        body,
        delay: Duration::ZERO,
        stall_after: None,
    }
}

/// Serve `routes` (path -> response) over HTTP/1.1; unknown paths are 404.
async fn serve(routes: HashMap<String, Route>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let routes = Arc::new(routes);
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let routes = routes.clone();
            tokio::spawn(async move {
                let mut req = Vec::new();
                let mut buf = [0u8; 1024];
                while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => req.extend_from_slice(&buf[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&req);
                let path = text.split_whitespace().nth(1).unwrap_or("/").to_string();
                let Some(r) = routes.get(&path).cloned() else {
                    let _ = sock
                        .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await;
                    return;
                };
                tokio::time::sleep(r.delay).await;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    r.body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                match r.stall_after {
                    Some(n) => {
                        let _ = sock.write_all(&r.body[..n]).await;
                        let _ = sock.flush().await;
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                    }
                    None => {
                        let _ = sock.write_all(&r.body).await;
                    }
                }
            });
        }
    });
    format!("http://{addr}")
}

/// A Write sink shared with the test.
#[derive(Clone, Default)]
struct SharedBuf(Arc<StdMutex<Vec<u8>>>);

impl Write for SharedBuf {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SharedBuf {
    fn lines(&self) -> Vec<Value> {
        let data = self.0.lock().unwrap().clone();
        String::from_utf8(data)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("twitchpipe-rec-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("out.ts")
}

fn body(len: usize, byte: u8) -> Vec<u8> {
    vec![byte; len]
}

const FIXTURE: &str = "#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:2
#EXT-X-MEDIA-SEQUENCE:100
#EXT-X-DATERANGE:ID=\"stitched-ad-1\",CLASS=\"twitch-stitched-ad\",START-DATE=\"2026-01-01T00:00:04.000Z\",DURATION=4.000
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z
#EXTINF:2.000,live
/seg100.ts
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:02.000Z
#EXTINF:2.000,live
/seg101.ts
#EXT-X-DISCONTINUITY
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:04.000Z
#EXTINF:2.000,Amazon
/ad102.ts
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:06.000Z
#EXTINF:2.000,Amazon
/ad103.ts
#EXT-X-DISCONTINUITY
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:08.000Z
#EXTINF:2.000,live
/seg104.ts
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:10.000Z
#EXTINF:2.000,live
/missing105.ts
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:12.000Z
#EXTINF:2.000,live
/seg106.ts
#EXT-X-TWITCH-PREFETCH:/seg107.ts
#EXT-X-ENDLIST
";

async fn fixture_server() -> String {
    let mut routes = HashMap::new();
    routes.insert("/playlist.m3u8".into(), route(FIXTURE.as_bytes().to_vec()));
    routes.insert("/seg100.ts".into(), route(body(1500, 1)));
    // Slow segment: later downloads finish first with 3 threads, but the
    // output must stay in playlist order.
    routes.insert(
        "/seg101.ts".into(),
        Route {
            delay: Duration::from_millis(300),
            ..route(body(700, 2))
        },
    );
    routes.insert("/ad102.ts".into(), route(body(999, 0xAD)));
    routes.insert("/ad103.ts".into(), route(body(999, 0xAD)));
    routes.insert("/sub102.ts".into(), route(body(640, 9)));
    routes.insert("/seg104.ts".into(), route(body(333, 4)));
    routes.insert("/seg106.ts".into(), route(body(2048, 6)));
    routes.insert("/seg107.ts".into(), route(body(10, 7)));
    serve(routes).await
}

fn test_config() -> Config {
    let mut config = Config::default();
    config.stream.segment_threads = 3;
    config.stream.segment_attempts = 1;
    config.stream.segment_timeout = 5.0;
    config
}

/// Substitutes slot 102 like delayed playback would; drops other ads.
struct SubstituteHook {
    base: String,
}

#[async_trait]
impl SegmentHook for SubstituteHook {
    async fn on_playlist(&mut self, _playlist: &MediaPlaylist) {}

    async fn segment_action(&mut self, segment: &MediaSegment) -> SegmentAction {
        match segment.num {
            102 => SegmentAction::Substitute(Substitute {
                sequence: Some(102),
                date: segment.date,
                duration: Some(2.0),
                player_type: Some("popout".into()),
                ..Substitute::url_only(
                    format!("{}/sub102.ts", self.base),
                    crate::adblock::STRATEGY_DELAYED,
                )
            }),
            _ if segment.ad => SegmentAction::Skip,
            _ => SegmentAction::Fetch,
        }
    }
}

async fn record_fixture(
    hook: Arc<Mutex<dyn SegmentHook>>,
    base: &str,
    name: &str,
) -> (AttemptEnd, Vec<Value>, Vec<u8>, Session) {
    let path = temp_path(name);
    let events = SharedBuf::default();
    let mut session = Session::new(EventLog::to_writer(Box::new(events.clone())), 0);
    let (mut out, offset) = Output::open(&path, false).unwrap();
    assert_eq!(offset, 0);
    let (_tx, mut shutdown) = Shutdown::manual();

    let client = reqwest::Client::new();
    let config = test_config();
    let (etx, erx) = mpsc::unbounded_channel();
    let pipeline = crate::spawn_pipeline(
        &client,
        &format!("{base}/playlist.m3u8"),
        &config,
        hook,
        Some(etx),
    );
    session.tracker.begin_attempt(false);
    let end = tokio::time::timeout(
        Duration::from_secs(20),
        session.run_attempt(
            pipeline,
            erx,
            &mut out,
            Duration::from_secs(10),
            &mut shutdown,
        ),
    )
    .await
    .expect("recording finished");
    drop(out);
    let data = std::fs::read(&path).unwrap();
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    (end, events.lines(), data, session)
}

fn of_type<'a>(lines: &'a [Value], kind: &str) -> Vec<&'a Value> {
    lines.iter().filter(|l| l["type"] == kind).collect()
}

/// Segment offsets are contiguous, start at 0 and sum to the file size.
fn assert_offsets_cover(lines: &[Value], data: &[u8]) {
    let mut expected = 0u64;
    for seg in of_type(lines, "segment") {
        assert_eq!(seg["offset"].as_u64().unwrap(), expected, "{seg}");
        expected += seg["length"].as_u64().unwrap();
    }
    assert_eq!(expected, data.len() as u64);
}

#[tokio::test]
async fn records_fixture_with_exact_offsets_kinds_and_gaps() {
    let base = fixture_server().await;
    let hook: Arc<Mutex<dyn SegmentHook>> = Arc::new(Mutex::new(AdFilterHook::default()));
    let (end, lines, data, session) = record_fixture(hook, &base, "filter").await;

    assert_eq!(end, AttemptEnd::Stream(WorkerExit::Endlist));
    for line in &lines {
        let ts = line["ts"].as_str().unwrap();
        assert!(ts.ends_with('Z') && ts.len() == 24, "{ts}");
    }

    // Output bytes are exactly the written segments, in playlist order.
    let mut expected = Vec::new();
    for (len, b) in [(1500, 1), (700, 2), (333, 4), (2048, 6), (10, 7)] {
        expected.extend(body(len, b));
    }
    assert_eq!(data, expected);
    assert_offsets_cover(&lines, &data);

    // Ordered view of the output-related events.
    let ordered: Vec<String> = lines
        .iter()
        .filter(|l| ["segment", "gap", "discontinuity"].contains(&l["type"].as_str().unwrap()))
        .map(|l| match l["type"].as_str().unwrap() {
            "segment" => format!(
                "segment:{}:{}",
                l["slot_sequence"],
                l["kind"].as_str().unwrap()
            ),
            "gap" => format!(
                "gap:{}-{}:{}",
                l["from_sequence"],
                l["to_sequence"],
                l["reason"].as_str().unwrap()
            ),
            _ => format!("discontinuity:{}", l["reason"].as_str().unwrap()),
        })
        .collect();
    assert_eq!(
        ordered,
        vec![
            "segment:100:live",
            "segment:101:live",
            "gap:102-103:ad_dropped",
            "discontinuity:sequence_jump",
            "segment:104:live",
            "gap:105-105:download_failed",
            "discontinuity:sequence_jump",
            "segment:106:live",
            "segment:107:prefetch",
        ]
    );

    let segs = of_type(&lines, "segment");
    assert_eq!(segs[0]["program_date_time"], "2026-01-01T00:00:00.000Z");
    assert_eq!(segs[0]["duration"], 2.0);
    assert_eq!(segs[0]["sequence"], 100);
    assert_eq!(segs[2]["discontinuity"], true);
    // prefetch: date extrapolated from seg106, flagged as estimated
    assert_eq!(segs[4]["program_date_time"], "2026-01-01T00:00:14.000Z");
    assert_eq!(segs[4]["timing_estimated"], true);

    let gaps = of_type(&lines, "gap");
    assert_eq!(gaps[0]["offset"], 2200);
    assert_eq!(gaps[0]["duration"], 4.0);
    assert_eq!(gaps[0]["program_date_time"], "2026-01-01T00:00:04.000Z");
    assert_eq!(gaps[1]["offset"], 2533);
    assert!(gaps[1]["error"].as_str().unwrap().contains("404"));

    let ad_start = of_type(&lines, "ad_start");
    assert_eq!(ad_start.len(), 1);
    assert_eq!(ad_start[0]["strategy"], "filter");
    assert_eq!(ad_start[0]["break_duration"], 4.0);

    let totals = &session.tracker.totals;
    assert_eq!(totals.segments, 5);
    assert_eq!(totals.bytes, data.len() as u64);
    assert_eq!(totals.gaps, 2);
    assert_eq!(totals.gap_segments, 3);
    assert_eq!(
        finish_for(&end, totals.segments, Duration::from_secs(10)).reason,
        "stream_ended"
    );
}

#[tokio::test]
async fn substitutes_report_their_source() {
    let base = fixture_server().await;
    let hook: Arc<Mutex<dyn SegmentHook>> =
        Arc::new(Mutex::new(SubstituteHook { base: base.clone() }));
    let (_, lines, data, _) = record_fixture(hook, &base, "substitute").await;
    assert_offsets_cover(&lines, &data);

    let segs = of_type(&lines, "segment");
    let sub = segs.iter().find(|s| s["slot_sequence"] == 102).unwrap();
    assert_eq!(sub["kind"], "ad_substitute");
    assert_eq!(sub["sequence"], 102);
    assert_eq!(sub["offset"], 2200);
    assert_eq!(sub["length"], 640);
    assert_eq!(sub["source"]["strategy"], "delayed_playback");
    assert_eq!(sub["source"]["player_type"], "popout");
    // contiguous delayed content: no discontinuity before it
    assert_eq!(sub["discontinuity"], false);
    let gaps = of_type(&lines, "gap");
    assert_eq!(gaps[0]["from_sequence"], 103);
    assert_eq!(gaps[0]["reason"], "ad_dropped");
    // no URLs (signed or otherwise) are ever reported
    assert!(!lines.iter().any(|l| l.to_string().contains("sub102.ts")));
}

#[tokio::test]
async fn signal_stops_promptly_and_reports_partial_segment() {
    let playlist = "#EXTM3U
#EXT-X-TARGETDURATION:2
#EXT-X-MEDIA-SEQUENCE:200
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z
#EXTINF:2.000,live
/seg200.ts
#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:02.000Z
#EXTINF:2.000,live
/seg201.ts
";
    let mut routes = HashMap::new();
    routes.insert(
        "/playlist.m3u8".to_string(),
        route(playlist.as_bytes().to_vec()),
    );
    routes.insert("/seg200.ts".to_string(), route(body(5000, 1)));
    routes.insert(
        "/seg201.ts".to_string(),
        Route {
            stall_after: Some(100),
            ..route(body(5000, 2))
        },
    );
    let base = serve(routes).await;

    let path = temp_path("signal");
    let events = SharedBuf::default();
    let mut session = Session::new(EventLog::to_writer(Box::new(events.clone())), 0);
    let (mut out, _) = Output::open(&path, false).unwrap();
    let (signal_tx, mut shutdown) = Shutdown::manual();
    let client = reqwest::Client::new();
    let mut config = test_config();
    config.stream.segment_threads = 1;
    let (etx, erx) = mpsc::unbounded_channel();
    let pipeline = crate::spawn_pipeline(
        &client,
        &format!("{base}/playlist.m3u8"),
        &config,
        Arc::new(Mutex::new(AdFilterHook::default())),
        Some(etx),
    );
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let _ = signal_tx.send(Some(SIGTERM));
        // keep the sender alive until the test ends
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let started = std::time::Instant::now();
    let end = session
        .run_attempt(
            pipeline,
            erx,
            &mut out,
            Duration::from_secs(60),
            &mut shutdown,
        )
        .await;
    assert_eq!(end, AttemptEnd::Signal(SIGTERM));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    let code = session.finish(finish_for(
        &end,
        session.tracker.totals.segments,
        Duration::from_secs(60),
    ));
    assert_eq!(code, 143);
    drop(out);

    let data = std::fs::read(&path).unwrap();
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    assert_eq!(data.len(), 5100);
    let lines = events.lines();
    assert_offsets_cover(&lines, &data);
    let segs = of_type(&lines, "segment");
    assert_eq!(segs.len(), 2);
    assert_eq!(segs[0]["complete"], true);
    assert_eq!(segs[1]["complete"], false);
    assert_eq!(segs[1]["length"], 100);
    let end_line = lines.last().unwrap();
    assert_eq!(end_line["type"], "end");
    assert_eq!(end_line["reason"], "signal");
    assert_eq!(end_line["signal"], "SIGTERM");
    assert_eq!(end_line["exit_code"], 143);
    assert_eq!(end_line["bytes"], 5100);
    assert_eq!(end_line["incomplete_segments"], 1);
}

#[test]
fn finish_reasons_and_exit_codes() {
    let t = Duration::from_secs(60);
    let f = |end: AttemptEnd, segments| finish_for(&end, segments, t);
    assert_eq!(f(AttemptEnd::Stream(WorkerExit::Endlist), 3).code, EXIT_OK);
    assert_eq!(
        f(AttemptEnd::Stream(WorkerExit::NoNewSegments), 3).reason,
        "stream_ended"
    );
    let r = f(AttemptEnd::Stream(WorkerExit::RecoveryTimeout), 3);
    assert_eq!((r.reason, r.code), ("recovery_timeout", EXIT_OK));
    let r = f(AttemptEnd::Stream(WorkerExit::NoNewSegments), 0);
    assert_eq!((r.reason, r.code), ("no_data", EXIT_STREAM));
    let r = f(AttemptEnd::Stream(WorkerExit::LoadFailed("x".into())), 0);
    assert_eq!((r.reason, r.code), ("stream_error", EXIT_STREAM));
    let r = f(AttemptEnd::Timeout, 3);
    assert_eq!((r.reason, r.code), ("stream_timeout", EXIT_STREAM));
    let r = f(AttemptEnd::Signal(SIGINT), 3);
    assert_eq!(
        (r.reason, r.code, r.signal),
        ("signal", 130, Some("SIGINT"))
    );
    let r = f(AttemptEnd::Output("disk full".into()), 3);
    assert_eq!((r.reason, r.code), ("output_error", EXIT_OUTPUT));
}

#[test]
fn start_errors_map_to_exit_codes() {
    let code = |e: StartError| classify_start_error(&anyhow::Error::from(e)).code;
    assert_eq!(code(StartError::Offline("x".into())), EXIT_OFFLINE);
    assert_eq!(code(StartError::Auth("x".into())), EXIT_AUTH);
    assert_eq!(code(StartError::NoQuality("x".into())), EXIT_NO_QUALITY);
    assert_eq!(
        classify_start_error(&anyhow::anyhow!("network down")).code,
        EXIT_STREAM
    );
}

#[tokio::test]
async fn player_pipeline_carries_bytes_only() {
    // Player mode passes no EventTx: the buffer must contain exactly the
    // segment bytes, with no markers, in playlist order.
    let base = fixture_server().await;
    let client = reqwest::Client::new();
    let mut pipeline = crate::spawn_pipeline(
        &client,
        &format!("{base}/playlist.m3u8"),
        &test_config(),
        Arc::new(Mutex::new(AdFilterHook::default())),
        None,
    );
    let mut data = Vec::new();
    while let Some(item) = pipeline.bytes.recv_item().await {
        match item {
            crate::buffer::Item::Data(b) => data.extend_from_slice(&b),
            crate::buffer::Item::Marker(m) => panic!("unexpected marker in player mode: {m:?}"),
        }
    }
    let mut expected = Vec::new();
    for (len, b) in [(1500, 1), (700, 2), (333, 4), (2048, 6), (10, 7)] {
        expected.extend(body(len, b));
    }
    assert_eq!(data, expected);
    assert_eq!(pipeline.worker.await.unwrap(), WorkerExit::Endlist);
}
