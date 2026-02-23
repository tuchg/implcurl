use anyhow::{Context, Result, bail};
use impcurl::{
    MultiSession, WebSocketConnectConfig, WsFrameAssembler,
    complete_connect_only_websocket_handshake_with_multi, detach_easy_from_multi,
    ensure_curl_global_init, prepare_connect_only_websocket_session, ws_try_recv_frame,
};
#[cfg(unix)]
use impcurl::ws_send_text_async;
#[cfg(not(unix))]
use impcurl::ws_send_text;
use impcurl_sys::{
    CURL_CSELECT_ERR, CURL_CSELECT_IN, CURL_CSELECT_OUT, CURL_POLL_IN, CURL_POLL_INOUT,
    CURL_POLL_OUT, CURL_POLL_REMOVE, Curl, CurlApi, CurlSocket,
};
#[cfg(unix)]
use std::collections::HashMap;
#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(unix)]
use std::os::raw::{c_int, c_long, c_void};
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, info, warn};
#[cfg(unix)]
use tokio::io::Interest;
#[cfg(unix)]
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
#[cfg(unix)]
use tokio::time::sleep;
#[cfg(unix)]
use tokio::time::timeout as timeout_async;
use tokio::time::timeout;

#[derive(Debug)]
pub enum WorkerCommand {
    SendText(String),
    Shutdown,
}

#[derive(Debug)]
pub enum WorkerEvent {
    Connected,
    Frame { flags: i32, payload: Vec<u8> },
    Error(String),
    Shutdown,
}


#[derive(Clone, Debug)]
pub struct WsClientConfig {
    pub lib_path: Option<PathBuf>,
    pub url: String,
    pub origin: Option<String>,
    pub user_agent: Option<String>,
    pub impersonate_target: String,
    pub verbose: bool,
    pub connect_timeout: Duration,
    pub multi_poll_timeout: Duration,
    pub send_again_sleep: Duration,
    pub loop_sleep: Duration,
    pub event_channel_capacity: usize,
}

impl WsClientConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            lib_path: None,
            url: url.into(),
            origin: None,
            user_agent: None,
            impersonate_target: "chrome136".to_owned(),
            verbose: false,
            connect_timeout: Duration::from_secs(20),
            multi_poll_timeout: Duration::from_millis(500),
            send_again_sleep: Duration::from_millis(20),
            loop_sleep: Duration::from_millis(40),
            event_channel_capacity: 1024,
        }
    }

    pub fn with_lib_path(mut self, lib_path: impl Into<PathBuf>) -> Self {
        self.lib_path = Some(lib_path.into());
        self
    }
}

pub struct WsClient {
    cmd_tx: UnboundedSender<WorkerCommand>,
    event_rx: mpsc::Receiver<WorkerEvent>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

impl WsClient {
    pub async fn connect(mut cfg: WsClientConfig) -> Result<Self> {
        let resolved_lib = resolve_lib_path_for_connect(cfg.lib_path.clone())?;
        cfg.lib_path = Some(resolved_lib);

        let connect_timeout = cfg.connect_timeout;
        let event_capacity = cfg.event_channel_capacity;
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<WorkerCommand>();
        let (event_tx, mut event_rx) = mpsc::channel::<WorkerEvent>(event_capacity);

        let url = cfg.url.clone();
        let worker = tokio::spawn(async move {
            if let Err(err) = run_worker(cfg, cmd_rx, event_tx.clone()).await {
                let _ = event_tx.try_send(WorkerEvent::Error(format!("{err:#}")));
            }
        });

        info!(url = %url, "connecting websocket");
        let connected = timeout(connect_timeout, event_rx.recv())
            .await
            .context("timed out waiting for websocket connect event")?;

        match connected {
            Some(WorkerEvent::Connected) => {
                info!("websocket connected");
            }
            Some(WorkerEvent::Error(err)) => {
                warn!(err = %err, "worker failed before connect");
                bail!("worker failed before websocket connected: {err}");
            }
            Some(WorkerEvent::Shutdown) | None => {
                bail!("worker stopped before websocket connected");
            }
            Some(WorkerEvent::Frame { .. }) => {
                bail!("worker produced frame before websocket connected");
            }
        }

        Ok(Self {
            cmd_tx,
            event_rx,
            worker: Some(worker),
        })
    }

    pub fn send_text(&self, text: String) -> Result<()> {
        self.cmd_tx
            .send(WorkerCommand::SendText(text))
            .map_err(|_| anyhow::anyhow!("worker channel closed while sending"))
    }

    pub async fn recv_event(&mut self, max_wait: Duration) -> Option<WorkerEvent> {
        timeout(max_wait, self.event_rx.recv()).await.ok().flatten()
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        debug!("shutting down websocket worker");
        let _ = self.cmd_tx.send(WorkerCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            match worker.await {
                Ok(()) => {}
                Err(join_err) if join_err.is_cancelled() => {}
                Err(join_err) => {
                    bail!("worker task failed: {join_err}");
                }
            }
        }
        Ok(())
    }

    pub async fn connect_url(url: impl Into<String>) -> Result<Self> {
        Self::connect(WsClientConfig::new(url)).await
    }

    pub async fn connect_with_browser(
        url: impl Into<String>,
        origin: impl Into<String>,
        user_agent: impl Into<String>,
    ) -> Result<Self> {
        let mut cfg = WsClientConfig::new(url);
        cfg.origin = Some(origin.into());
        cfg.user_agent = Some(user_agent.into());
        Self::connect(cfg).await
    }

    pub fn send_json(&self, value: &serde_json::Value) -> Result<()> {
        self.send_text(value.to_string())
    }

    pub async fn next_event(&mut self, max_wait: Duration) -> Result<Option<WorkerEvent>> {
        match self.recv_event(max_wait).await {
            Some(WorkerEvent::Error(err)) => bail!("websocket worker error: {err}"),
            other => Ok(other),
        }
    }

    pub async fn next_text(&mut self, max_wait: Duration) -> Result<Option<String>> {
        loop {
            match self.next_event(max_wait).await? {
                Some(WorkerEvent::Frame { payload, .. }) => {
                    let text = String::from_utf8(payload)
                        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
                    return Ok(Some(text));
                }
                Some(WorkerEvent::Connected) => continue,
                Some(WorkerEvent::Shutdown) | None => return Ok(None),
                Some(WorkerEvent::Error(_)) => unreachable!(),
            }
        }
    }
}

impl Drop for WsClient {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(WorkerCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

#[cfg(unix)]
#[derive(Default)]
struct MultiCallbackState {
    socket_events: HashMap<CurlSocket, c_int>,
    timeout_ms: Option<i64>,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy)]
struct RawSocketFd(RawFd);

#[cfg(unix)]
impl AsRawFd for RawSocketFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

#[cfg(unix)]
struct AsyncFdCache {
    fds: HashMap<CurlSocket, (AsyncFd<RawSocketFd>, Interest)>,
}

#[cfg(unix)]
impl AsyncFdCache {
    fn new() -> Self {
        Self { fds: HashMap::new() }
    }

    fn sync_with(&mut self, socket_events: &[(CurlSocket, c_int)]) {
        self.fds.retain(|sock, _| socket_events.iter().any(|(s, _)| s == sock));
        for &(socket, ev) in socket_events {
            let Some(interest) = tokio_interest_from_curl_poll(ev) else {
                self.fds.remove(&socket);
                continue;
            };
            let needs_rebuild = match self.fds.get(&socket) {
                Some((_, old_interest)) => *old_interest != interest,
                None => true,
            };
            if needs_rebuild {
                let fd = socket as RawFd;
                if fd >= 0 {
                    if let Ok(async_fd) = AsyncFd::with_interest(RawSocketFd(fd), interest) {
                        self.fds.insert(socket, (async_fd, interest));
                    }
                }
            }
        }
    }

    async fn wait_any_ready(&self, wait: Duration) -> Option<(CurlSocket, c_int)> {
        if self.fds.len() == 1 {
            let (&socket, (async_fd, interest)) = self.fds.iter().next().unwrap();
            return match timeout_async(wait, async_fd.ready(*interest)).await {
                Ok(Ok(mut guard)) => {
                    let mask = ready_to_mask(&guard);
                    guard.clear_ready();
                    Some((socket, mask))
                }
                _ => None,
            };
        }

        // Multi-socket: poll each cached AsyncFd with short per-fd timeout
        // Typical case: 1-2 sockets per WS connection, so this is O(1)-O(2)
        let per_fd = Duration::from_millis(
            (wait.as_millis() / self.fds.len().max(1) as u128).max(1) as u64,
        );
        for (&socket, (async_fd, interest)) in &self.fds {
            if let Ok(Ok(mut guard)) = timeout_async(per_fd, async_fd.ready(*interest)).await {
                let mask = ready_to_mask(&guard);
                guard.clear_ready();
                return Some((socket, mask));
            }
        }
        None
    }
}

#[cfg(unix)]
fn ready_to_mask(guard: &tokio::io::unix::AsyncFdReadyGuard<'_, RawSocketFd>) -> c_int {
    let ready = guard.ready();
    let mut mask = 0;
    if ready.is_readable() { mask |= CURL_CSELECT_IN; }
    if ready.is_writable() { mask |= CURL_CSELECT_OUT; }
    if ready.is_error() || mask == 0 { mask |= CURL_CSELECT_ERR; }
    mask
}

#[cfg(unix)]
unsafe fn callback_state_from_userp<'a>(
    userp: *mut c_void,
) -> Option<&'a Mutex<MultiCallbackState>> {
    if userp.is_null() {
        None
    } else {
        Some(unsafe { &*(userp as *const Mutex<MultiCallbackState>) })
    }
}

#[cfg(unix)]
unsafe extern "C" fn multi_socket_callback(
    _easy: *mut Curl,
    socket: CurlSocket,
    what: c_int,
    userp: *mut c_void,
    _socketp: *mut c_void,
) -> c_int {
    let Some(state) = (unsafe { callback_state_from_userp(userp) }) else {
        return 0;
    };
    if let Ok(mut guard) = state.lock() {
        if what == CURL_POLL_REMOVE {
            guard.socket_events.remove(&socket);
        } else {
            guard.socket_events.insert(socket, what);
        }
    }
    0
}

#[cfg(unix)]
unsafe extern "C" fn multi_timer_callback(
    _multi: *mut impcurl_sys::CurlMulti,
    timeout_ms: c_long,
    userp: *mut c_void,
) -> c_int {
    let Some(state) = (unsafe { callback_state_from_userp(userp) }) else {
        return 0;
    };
    if let Ok(mut guard) = state.lock() {
        guard.timeout_ms = if timeout_ms < 0 {
            None
        } else {
            Some(timeout_ms as i64)
        };
    }
    0
}

#[cfg(unix)]
fn register_multi_callbacks(
    multi: &MultiSession<'_>,
    state: &Mutex<MultiCallbackState>,
) -> Result<()> {
    let userp = (state as *const Mutex<MultiCallbackState>)
        .cast_mut()
        .cast::<c_void>();
    multi.set_socket_callback(Some(multi_socket_callback), userp)?;
    multi.set_timer_callback(Some(multi_timer_callback), userp)?;
    Ok(())
}

#[cfg(unix)]
fn clear_multi_callbacks(multi: &MultiSession<'_>) -> Result<()> {
    multi.set_socket_callback(None, std::ptr::null_mut())?;
    multi.set_timer_callback(None, std::ptr::null_mut())?;
    Ok(())
}

#[cfg(unix)]
fn tokio_interest_from_curl_poll(what: c_int) -> Option<Interest> {
    match what {
        CURL_POLL_IN => Some(Interest::READABLE),
        CURL_POLL_OUT => Some(Interest::WRITABLE),
        CURL_POLL_INOUT => Some(Interest::READABLE.add(Interest::WRITABLE)),
        _ => None,
    }
}

#[cfg(unix)]
async fn drive_multi_once_with_cache(
    multi: &mut MultiSession<'_>,
    callback_state: &Mutex<MultiCallbackState>,
    fd_cache: &mut AsyncFdCache,
    default_wait: Duration,
) -> Result<()> {
    let (socket_events, timer_timeout_ms) = {
        let guard = callback_state
            .lock()
            .map_err(|_| anyhow::anyhow!("curl multi callback state lock poisoned"))?;
        (
            guard.socket_events.iter().map(|(s, e)| (*s, *e)).collect::<Vec<_>>(),
            guard.timeout_ms,
        )
    };

    fd_cache.sync_with(&socket_events);

    let default_wait_ms = default_wait.as_millis().min(i64::MAX as u128) as i64;
    let wait_ms = timer_timeout_ms
        .map(|ms| ms.max(0).min(default_wait_ms))
        .unwrap_or(default_wait_ms);
    let wait_duration = Duration::from_millis(wait_ms.max(0) as u64);

    let mut running_handles = 0;

    if fd_cache.fds.is_empty() {
        if wait_ms > 0 {
            sleep(wait_duration).await;
        }
        multi.socket_action_timeout(&mut running_handles)?;
        if running_handles > 0 {
            multi.perform(&mut running_handles)?;
        }
        return Ok(());
    }

    // Poll persistent AsyncFds — no epoll_ctl churn
    let action = fd_cache.wait_any_ready(wait_duration).await;

    match action {
        Some((socket, mask)) => {
            multi.socket_action(socket, mask, &mut running_handles)?;
        }
        None => {
            multi.socket_action_timeout(&mut running_handles)?;
        }
    }

    if running_handles > 0 {
        multi.perform(&mut running_handles)?;
    }
    Ok(())
}

#[cfg(unix)]
async fn run_worker_loop_async(
    api: &CurlApi,
    multi: &mut MultiSession<'_>,
    easy_handle: usize,
    send_again_sleep: Duration,
    loop_poll_timeout: Duration,
    callback_state: &Mutex<MultiCallbackState>,
    mut cmd_rx: UnboundedReceiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> Result<()> {
    let mut assembler = WsFrameAssembler::default();
    let mut fd_cache = AsyncFdCache::new();
    loop {
        while let Some(frame) = ws_try_recv_frame(api, easy_handle as *mut Curl, &mut assembler)? {
            let _ = event_tx
                .try_send(WorkerEvent::Frame {
                    flags: frame.flags,
                    payload: frame.payload,
                });
        }

        match cmd_rx.try_recv() {
            Ok(WorkerCommand::SendText(text)) => {
                ws_send_text_async(api, easy_handle, &text, send_again_sleep)
                    .await
                    .with_context(|| format!("failed to send websocket text: {text}"))?;
            }
            Ok(WorkerCommand::Shutdown) => {
                debug!("worker received shutdown command");
                let _ = event_tx.try_send(WorkerEvent::Shutdown);
                return Ok(());
            }
            Err(TryRecvError::Disconnected) => {
                debug!("worker command channel disconnected");
                let _ = event_tx.try_send(WorkerEvent::Shutdown);
                return Ok(());
            }
            Err(TryRecvError::Empty) => {}
        }

        drive_multi_once_with_cache(multi, callback_state, &mut fd_cache, loop_poll_timeout).await?;
    }
}

#[cfg(not(unix))]
fn run_worker_loop(
    api: &CurlApi,
    multi: &mut MultiSession<'_>,
    easy_handle: usize,
    send_again_sleep: Duration,
    loop_poll_timeout: Duration,
    mut cmd_rx: UnboundedReceiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> Result<()> {
    let mut assembler = WsFrameAssembler::default();
    loop {
        while let Some(frame) = ws_try_recv_frame(api, easy_handle as *mut Curl, &mut assembler)? {
            let _ = event_tx
                .try_send(WorkerEvent::Frame {
                    flags: frame.flags,
                    payload: frame.payload,
                });
        }

        match cmd_rx.try_recv() {
            Ok(WorkerCommand::SendText(text)) => {
                ws_send_text(api, easy_handle as *mut Curl, &text, send_again_sleep)
                    .with_context(|| format!("failed to send websocket text: {text}"))?;
            }
            Ok(WorkerCommand::Shutdown) => {
                debug!("worker received shutdown command");
                let _ = event_tx.try_send(WorkerEvent::Shutdown);
                return Ok(());
            }
            Err(TryRecvError::Disconnected) => {
                debug!("worker command channel disconnected");
                let _ = event_tx.try_send(WorkerEvent::Shutdown);
                return Ok(());
            }
            Err(TryRecvError::Empty) => {}
        }

        drive_multi_once_poll_fallback(multi, loop_poll_timeout)?;
    }
}

#[cfg(not(unix))]
fn drive_multi_once_poll_fallback(
    multi: &mut MultiSession<'_>,
    default_wait: Duration,
) -> Result<()> {
    let _ = multi.poll(default_wait)?;
    let mut running_handles = 0;
    multi.socket_action_timeout(&mut running_handles)?;
    if running_handles > 0 {
        multi.perform(&mut running_handles)?;
    }
    Ok(())
}

async fn run_worker(
    cfg: WsClientConfig,
    cmd_rx: UnboundedReceiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> Result<()> {
    let lib_path = cfg
        .lib_path
        .as_ref()
        .context("internal error: missing resolved lib_path in worker config")?;
    let api = impcurl_sys::shared_curl_api(lib_path)?;
    ensure_curl_global_init(&api)?;

    let connect_cfg = WebSocketConnectConfig {
        url: &cfg.url,
        origin: cfg.origin.as_deref(),
        user_agent: cfg.user_agent.as_deref(),
        impersonate_target: &cfg.impersonate_target,
        verbose: cfg.verbose,
    };
    let session = prepare_connect_only_websocket_session(&api, &connect_cfg)?;
    let mut multi = MultiSession::new(&api)?;
    complete_connect_only_websocket_handshake_with_multi(
        &api,
        &multi,
        session.easy_handle(),
        cfg.multi_poll_timeout,
    )?;
    #[cfg(unix)]
    let callback_state = Box::new(Mutex::new(MultiCallbackState::default()));
    #[cfg(unix)]
    register_multi_callbacks(&multi, callback_state.as_ref())?;
    #[cfg(unix)]
    {
        let mut running_handles = 0;
        multi.socket_action_timeout(&mut running_handles)?;
        if running_handles > 0 {
            multi.perform(&mut running_handles)?;
        }
    }

    if event_tx.try_send(WorkerEvent::Connected).is_err() {
        return Ok(());
    }

    #[cfg(unix)]
    let run_result = run_worker_loop_async(
        &api,
        &mut multi,
        session.easy_handle() as usize,
        cfg.send_again_sleep,
        cfg.loop_sleep,
        callback_state.as_ref(),
        cmd_rx,
        event_tx,
    )
    .await;
    #[cfg(not(unix))]
    let run_result = run_worker_loop(
        &api,
        &mut multi,
        session.easy_handle() as usize,
        cfg.send_again_sleep,
        cfg.loop_sleep,
        cmd_rx,
        event_tx,
    );
    #[cfg(unix)]
    let _ = clear_multi_callbacks(&multi);
    let _ = detach_easy_from_multi(&multi, session.easy_handle());
    run_result
}

fn resolve_lib_path_for_connect(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        if path.exists() {
            return Ok(path);
        }
        bail!("configured lib_path does not exist: {}", path.display());
    }

    impcurl_sys::resolve_impersonate_lib_path(&[]).with_context(|| {
        "failed to resolve libcurl-impersonate (auto-fetch may also have failed). set CURL_IMPERSONATE_LIB, set IMPCURL_LIB_DIR, or set IMPCURL_AUTO_FETCH=0 to disable runtime download attempts"
    })
}
