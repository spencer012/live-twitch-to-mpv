# Local HLS and Android builds

## Desktop test

Build TwitchPipe:

```powershell
cargo build
```

Start a local HLS session. `--serve` with no value picks a free port.

```powershell
.\target\debug\twitchpipe.exe monstercat --serve
```

The command prints a URL such as
`http://127.0.0.1:49152/stream.m3u8`. Keep TwitchPipe running, then open
another PowerShell window:

```powershell
mpv.exe http://127.0.0.1:49152/stream.m3u8
```

You can inspect the growing EVENT playlist without mpv:

```powershell
Invoke-WebRequest http://127.0.0.1:49152/stream.m3u8 |
    Select-Object -ExpandProperty Content
```

Optional server arguments:

```powershell
# Fixed port, custom storage, and a 512 MiB cap
.\target\debug\twitchpipe.exe monstercat --serve 8787 `
    --buffer-dir D:\TwitchPipeBuffer `
    --buffer-cap-bytes 536870912
```

The server accepts playback position reports for eviction:

```powershell
Invoke-RestMethod -Method Post `
    -ContentType application/json `
    -Body '{"streamOffsetSec":120.5}' `
    http://127.0.0.1:8787/control/position
```

## Android library

The `android-jni` feature exports the JNI methods expected by
`dev.twitchpipe.TwitchPipe`. Install an Android NDK, the Rust Android target,
and cargo-ndk, then build arm64:

```powershell
rustup target add aarch64-linux-android
cargo install cargo-ndk
$env:ANDROID_NDK_HOME = "$env:LOCALAPPDATA\Android\Sdk\ndk\<version>"
cargo ndk -t arm64-v8a -o .\android-jniLibs build --release `
    --features android-jni --lib
```

The output is
`android-jniLibs\arm64-v8a\libtwitchpipe_core.so`. Load it with
`System.loadLibrary("twitchpipe_core")`.

Host-side JNI type checking does not require an NDK:

```powershell
cargo check --features android-jni
```
