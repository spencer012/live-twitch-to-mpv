use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jdouble, jint, jlong, jstring};

use crate::session::{Session, SessionConfig};

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static SESSIONS: OnceLock<Mutex<HashMap<i64, Arc<Session>>>> = OnceLock::new();
static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("twitchpipe")
            .build()
            .expect("failed to build TwitchPipe runtime")
    })
}

fn sessions() -> &'static Mutex<HashMap<i64, Arc<Session>>> {
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn throw(env: &mut JNIEnv<'_>, message: impl AsRef<str>) {
    let _ = env.throw_new("java/lang/IllegalStateException", message.as_ref());
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_twitchpipe_TwitchPipe_startSession(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
) -> jlong {
    let result = (|| {
        let json: String = env
            .get_string(&config_json)
            .map_err(|error| error.to_string())?
            .into();
        let config: SessionConfig =
            serde_json::from_str(&json).map_err(|error| error.to_string())?;
        let session = runtime()
            .block_on(Session::start(config))
            .map_err(|error| format!("{error:#}"))?;
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        sessions().lock().unwrap().insert(handle, session);
        Ok::<i64, String>(handle)
    })();
    match result {
        Ok(handle) => handle,
        Err(message) => {
            throw(&mut env, message);
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_twitchpipe_TwitchPipe_getLocalUrl(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jstring {
    let session = sessions().lock().unwrap().get(&handle).cloned();
    let Some(session) = session else {
        throw(
            &mut env,
            format!("unknown TwitchPipe session handle {handle}"),
        );
        return std::ptr::null_mut();
    };
    match env.new_string(session.local_url()) {
        Ok(value) => value.into_raw(),
        Err(error) => {
            throw(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_twitchpipe_TwitchPipe_pollEvent(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    timeout_ms: jint,
) -> jstring {
    let session = sessions().lock().unwrap().get(&handle).cloned();
    let Some(session) = session else {
        throw(
            &mut env,
            format!("unknown TwitchPipe session handle {handle}"),
        );
        return std::ptr::null_mut();
    };
    let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
    let Some(event) = session.poll_event(timeout) else {
        return std::ptr::null_mut();
    };
    match env.new_string(event) {
        Ok(value) => value.into_raw(),
        Err(error) => {
            throw(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_twitchpipe_TwitchPipe_reportPosition(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    stream_offset_sec: jdouble,
) {
    let session = sessions().lock().unwrap().get(&handle).cloned();
    let Some(session) = session else {
        throw(
            &mut env,
            format!("unknown TwitchPipe session handle {handle}"),
        );
        return;
    };
    runtime().block_on(session.report_position(stream_offset_sec));
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_twitchpipe_TwitchPipe_stopSession(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    let session = sessions().lock().unwrap().remove(&handle);
    let Some(session) = session else {
        throw(
            &mut env,
            format!("unknown TwitchPipe session handle {handle}"),
        );
        return;
    };
    runtime().block_on(session.stop());
}
