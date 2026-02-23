# impcurl workspace

Rust workspace for `libcurl-impersonate` WebSocket wrapping and GMGN smoke testing.

## Crates

- `impcurl-sys`: dynamic loading + raw FFI bindings for `libcurl-impersonate`
- `impcurl`: safe blocking wrapper for websocket handshake/send/recv
- `impcurl-ws`: tokio bridge (`worker thread + channels`) and GMGN smoke example

The WebSocket handshake path uses `curl_multi_*` (`multi_poll` + `multi_socket_action` + `multi_perform`) instead of direct `curl_easy_perform`.
At runtime, Unix builds use `CURLMOPT_SOCKETFUNCTION`/`CURLMOPT_TIMERFUNCTION` to maintain socket/timer interests and dispatch readiness via `tokio::io::unix::AsyncFd` + `socket_action(fd, mask)`, with `multi_perform` only as fallback.

## Build

```bash
cargo build -p impcurl-ws --example 
```

## Run smoke test

```bash
CURL_IMPERSONATE_LIB=/abs/path/libcurl-impersonate.4.dylib cargo run -p impcurl-ws --example 
```

Tokio-friendly minimal API example:

```bash
cargo run -p impcurl-ws --example 
```

Runtime library lookup order:

1. `WsClientConfig::with_lib_path(...)`
2. `CURL_IMPERSONATE_LIB`
3. Packaged runtime path near executable (`../lib` and side-by-side)
4. `IMPCURL_LIB_DIR`
5. Default runtime directories (`~/.impcurl/lib`, `~/.cuimp/binaries`)
6. Auto-fetch from `curl_cffi` release wheel (enabled by default)

Auto-fetch controls:

- `IMPCURL_AUTO_FETCH=0` disables runtime download attempts.
- `IMPCURL_CURL_CFFI_VERSION=<version>` selects `curl_cffi` release tag (default `0.11.3`).
- `IMPCURL_AUTO_FETCH_CACHE_DIR=/abs/path` overrides fetch cache/output dir.

## Package

Package binary + runtime library into `dist/<target>/`:

```bash
./scripts/package
```

## Fetch Runtime Asset

Download runtime assets from `curl_cffi` release wheels (default) into `vendor/<target>/lib`:

```bash
./scripts/fetch-libcurl-impersonate --target aarch64-apple-darwin --curl-cffi-version 0.11.3 --lib-version 1.4.2
```

If you want the raw upstream `curl-impersonate` release instead:

```bash
./scripts/fetch-libcurl-impersonate --source curl-impersonate --target aarch64-apple-darwin --lib-version 1.4.2
```

Notes:

- `curl_cffi` publishes wheels/sdist on GitHub releases; the script resolves wheel assets via GitHub API.
- Some target wheels may not expose a standalone `libcurl-impersonate` shared library (script prints a warning in that case).
- Upstream `curl-impersonate` tarballs can be static-only on some targets (`.a` only), which is not usable for runtime `dlopen`.

You can override package/bin/target/lib:

```bash
./scripts/package --package impcurl-ws --example gmgn_smoke --target x86_64-unknown-linux-gnu --lib /abs/path/libcurl-impersonate.so.4
```

Output layout:

```text
dist/<target>/
  bin/<artifact>[.exe]
  lib/<runtime library>
  manifest.json
```

Archive output:

- `dist/impcurl-ws-<target>.tar.gz` (non-Windows)
- `dist/impcurl-ws-<target>.zip` (Windows)
