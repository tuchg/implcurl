# impcurl

[![impcurl-sys](https://img.shields.io/crates/v/impcurl-sys?label=impcurl-sys)](https://crates.io/crates/impcurl-sys)
[![impcurl](https://img.shields.io/crates/v/impcurl?label=impcurl)](https://crates.io/crates/impcurl)
[![impcurl-ws](https://img.shields.io/crates/v/impcurl-ws?label=impcurl-ws)](https://crates.io/crates/impcurl-ws)
[![MIT](https://img.shields.io/crates/l/impcurl)](LICENSE)

Rust WebSocket client with TLS fingerprint impersonation, powered by [libcurl-impersonate](https://github.com/lexiforest/curl-impersonate).

Bypass TLS fingerprinting by impersonating real browser signatures (Chrome, Safari, Firefox, Edge, Tor).

## Crates

| Crate | Description |
|-------|-------------|
| `impcurl-sys` | Dynamic FFI bindings for `libcurl-impersonate` with auto-fetch |
| `impcurl` | Safe blocking wrapper — WebSocket handshake, send, recv |
| `impcurl-ws` | Async tokio `Stream + Sink` WebSocket connection |

## Quick Start

```toml
[dependencies]
impcurl-ws = "0.2"
futures-util = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use futures_util::{SinkExt, StreamExt};
use impcurl_ws::{Message, WsConnection};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut ws = WsConnection::connect("wss://echo.websocket.org").await?;

    ws.send(Message::Text("hello".to_owned())).await?;

    if let Some(message) = ws.next().await.transpose()? {
        match message {
            Message::Text(text) => println!("{text}"),
            Message::Binary(bytes) => println!("{} bytes", bytes.len()),
            Message::Ping(_) | Message::Pong(_) | Message::Close(_) => {}
        }
    }

    Ok(())
}
```

## Builder API

```rust
use impcurl::ImpersonateTarget;
use impcurl_ws::{ControlFrameMode, WsConnection};
use std::time::Duration;

let ws = WsConnection::builder("wss://example.com/ws")
    .header("Origin", "https://example.com")
    .header("User-Agent", "Mozilla/5.0 ...")
    .proxy("socks5h://127.0.0.1:1080")
    .impersonate(ImpersonateTarget::Chrome136)
    .connect_timeout(Duration::from_secs(10))
    .control_frame_mode(ControlFrameMode::Manual)
    .read_buffer_messages(32)
    .write_buffer_messages(32)
    .verbose(true)
    .connect()
    .await?;
```

## Runtime Library

The `libcurl-impersonate` shared library is resolved at runtime in this order:

1. `CURL_IMPERSONATE_LIB` env var
2. Near executable (`../lib/` and side-by-side)
3. `IMPCURL_LIB_DIR` env var
4. `~/.impcurl/lib`, `~/.cuimp/binaries`
5. Auto-fetch from [curl_cffi](https://github.com/lexiforest/curl_cffi) wheel (enabled by default)

`impcurl-ws` does not expose a `lib_path(...)` builder escape hatch anymore. Runtime library resolution is treated as deployment/runtime configuration rather than a connection-level concern.

### Auto-fetch Controls

| Env Var | Description |
|---------|-------------|
| `IMPCURL_AUTO_FETCH=0` | Disable auto-download |
| `IMPCURL_CURL_CFFI_VERSION` | curl_cffi release tag (default `0.11.3`) |
| `IMPCURL_AUTO_FETCH_CACHE_DIR` | Override fetch cache directory |

## Architecture

```
impcurl-ws (async tokio client)
  └── impcurl (safe blocking wrapper)
       └── impcurl-sys (dynamic FFI + auto-fetch)
            └── libcurl-impersonate (runtime .so/.dylib/.dll)
```

On Unix, the async event loop uses `CURLMOPT_SOCKETFUNCTION` / `CURLMOPT_TIMERFUNCTION` with `tokio::io::unix::AsyncFd` for efficient socket-level readiness notification. Non-Unix falls back to `curl_multi_poll`.

## License

MIT
