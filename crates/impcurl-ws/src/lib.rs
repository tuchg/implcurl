use anyhow::{Context, Result, bail};
use impcurl::{
    MultiSession, WebSocketConnectConfig, WsFrameAssembler,
    complete_connect_only_websocket_handshake_with_multi, detach_easy_from_multi,
    ensure_curl_global_init, prepare_connect_only_websocket_session, ws_send,
    ws_try_recv_frame,
};
use impcurl_sys::{
    CURL_CSELECT_ERR, CURL_CSELECT_IN, CURL_CSELECT_OUT, CURL_POLL_IN, CURL_POLL_INOUT,
    CURL_POLL_OUT, CURL_POLL_REMOVE, CURLE_AGAIN, CURLWS_TEXT, Curl, CurlApi, CurlSocket,
};
#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(unix)]
use std::os::raw::{c_int, c_long, c_void};
use std::path::PathBuf;
#[cfg(unix)]
use smallvec::SmallVec;
#[cfg(unix)]
use std::sync::Mutex;
use std::time::Duration;
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
use tracing::{debug, info, warn};

#[derive(Debug)]
enum WorkerCommand {
    Send(Vec<u8>),
    Shutdown,
}

#[derive(Debug)]
enum WorkerEvent {
    Connected,
    Frame { payload: Vec<u8> },
    Error(String),
    Shutdown,
}

#[derive(Clone, Debug)]
struct WsClientConfig {
    lib_path: Option<PathBuf>,
    url: String,
    headers: Vec<String>,
    proxy: Option<String>,
    impersonate_target: impcurl::ImpersonateTarget,
    verbose: bool,
    connect_timeout: Duration,
    multi_poll_timeout: Duration,
    send_again_sleep: Duration,
    loop_sleep: Duration,
    event_channel_capacity: usize,
}

impl WsClientConfig {
    fn new(url: impl Into<String>) -> Self {
        Self {
            lib_path: None,
            url: url.into(),
            headers: Vec::new(),
            proxy: None,
            impersonate_target: impcurl::ImpersonateTarget::Chrome136,
            verbose: false,
            connect_timeout: Duration::from_secs(20),
            multi_poll_timeout: Duration::from_millis(500),
            send_again_sleep: Duration::from_millis(20),
            loop_sleep: Duration::from_millis(40),
            event_channel_capacity: 1024,
        }
    }
}

/// Builder for configuring a WebSocket connection.
pub struct WsClientBuilder {
    cfg: WsClientConfig,
}

impl WsClientBuilder {
    pub fn header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.cfg.headers.push(format!("{}: {}", name.as_ref(), value.as_ref()));
        self
    }

    pub fn proxy(mut self, proxy: impl Into<String>) -> Self {
        self.cfg.proxy = Some(proxy.into());
        self
    }

    pub fn impersonate(mut self, target: impcurl::ImpersonateTarget) -> Self {
        self.cfg.impersonate_target = target;
        self
    }

    pub fn connect_timeout(mut self, t: Duration) -> Self {
        self.cfg.connect_timeout = t;
        self
    }

    pub fn lib_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.cfg.lib_path = Some(path.into());
        self
    }

    pub fn verbose(mut self, on: bool) -> Self {
        self.cfg.verbose = on;
        self
    }

    pub async fn connect(self) -> Result<WsClient> {
        WsClient::connect_inner(self.cfg).await
    }
}

pub struct WsClient {
    cmd_tx: UnboundedSender<WorkerCommand>,
    event_rx: mpsc::Receiver<WorkerEvent>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

impl WsClient {
    /// Quick connect with just a URL.
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        Self::connect_inner(WsClientConfig::new(url)).await
    }

    /// Returns a builder for advanced configuration.
    pub fn builder(url: impl Into<String>) -> WsClientBuilder {
        WsClientBuilder {
            cfg: WsClientConfig::new(url),
        }
    }

    /// Send a text frame.
    pub fn send_text(&self, text: &str) -> Result<()> {
        self.send(text.as_bytes())
    }

    /// Send raw bytes as a text frame.
    pub fn send(&self, data: &[u8]) -> Result<()> {
        self.send_owned(data.to_vec())
    }

    /// Send raw bytes as a text frame, taking ownership of the buffer (zero-copy).
    pub fn send_owned(&self, data: Vec<u8>) -> Result<()> {
        self.cmd_tx
            .send(WorkerCommand::Send(data))
            .map_err(|_| anyhow::anyhow!("websocket closed"))
    }

    /// Receive the next frame as bytes. Returns `None` on shutdown.
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        self.recv_inner(None).await
    }

    /// Receive with a timeout. Returns `None` on shutdown or timeout.
    pub async fn recv_timeout(&mut self, max_wait: Duration) -> Result<Option<Vec<u8>>> {
        self.recv_inner(Some(max_wait)).await
    }

    /// Receive the next frame as a UTF-8 string.
    pub async fn recv_text(&mut self) -> Result<Option<String>> {
        Ok(self.recv().await?.map(|b| match String::from_utf8(b) {
            Ok(s) => s,
            Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
        }))
    }

    /// Receive as UTF-8 string with a timeout.
    pub async fn recv_text_timeout(&mut self, max_wait: Duration) -> Result<Option<String>> {
        Ok(self.recv_timeout(max_wait).await?.map(|b| match String::from_utf8(b) {
            Ok(s) => s,
            Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
        }))
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        debug!("shutting down websocket worker");
        let _ = self.cmd_tx.send(WorkerCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            match worker.await {
                Ok(()) => {}
                Err(join_err) if join_err.is_cancelled() => {}
                Err(join_err) => bail!("worker task failed: {join_err}"),
            }
        }
        Ok(())
    }

    async fn connect_inner(mut cfg: WsClientConfig) -> Result<Self> {
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
            Some(WorkerEvent::Connected) => info!("websocket connected"),
            Some(WorkerEvent::Error(err)) => {
                warn!(err = %err, "worker failed before connect");
                bail!("worker failed before websocket connected: {err}");
            }
            Some(WorkerEvent::Shutdown) | None => {
                bail!("worker stopped before websocket connected")
            }
            Some(WorkerEvent::Frame { .. }) => {
                bail!("worker produced frame before websocket connected")
            }
        }

        Ok(Self {
            cmd_tx,
            event_rx,
            worker: Some(worker),
        })
    }

    async fn recv_inner(&mut self, max_wait: Option<Duration>) -> Result<Option<Vec<u8>>> {
        loop {
            let event = match max_wait {
                Some(d) => timeout(d, self.event_rx.recv()).await.ok().flatten(),
                None => self.event_rx.recv().await,
            };
            match event {
                Some(WorkerEvent::Frame { payload }) => return Ok(Some(payload)),
                Some(WorkerEvent::Error(err)) => bail!("websocket worker error: {err}"),
                Some(WorkerEvent::Connected) => continue,
                Some(WorkerEvent::Shutdown) | None => return Ok(None),
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
    socket_events: SmallVec<[(CurlSocket, c_int); 4]>,
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
    fds: SmallVec<[(CurlSocket, AsyncFd<RawSocketFd>, Interest); 4]>,
}

#[cfg(unix)]
impl AsyncFdCache {
    fn new() -> Self {
        Self { fds: SmallVec::new() }
    }

    fn sync_with(&mut self, socket_events: &[(CurlSocket, c_int)]) {
        self.fds
            .retain(|e| socket_events.iter().any(|&(s, _)| s == e.0));
        for &(socket, ev) in socket_events {
            let Some(interest) = tokio_interest_from_curl_poll(ev) else {
                self.fds.retain(|e| e.0 != socket);
                continue;
            };
            let needs_rebuild = match self.fds.iter().find(|e| e.0 == socket) {
                Some(e) => e.2 != interest,
                None => true,
            };
            if needs_rebuild {
                self.fds.retain(|e| e.0 != socket);
                let fd = socket as RawFd;
                if fd >= 0 {
                    if let Ok(async_fd) = AsyncFd::with_interest(RawSocketFd(fd), interest) {
                        self.fds.push((socket, async_fd, interest));
                    }
                }
            }
        }
    }

    async fn wait_any_ready(&self, wait: Duration) -> Option<(CurlSocket, c_int)> {
        if self.fds.len() == 1 {
            let (socket, async_fd, interest) = &self.fds[0];
            return match timeout_async(wait, async_fd.ready(*interest)).await {
                Ok(Ok(mut guard)) => {
                    let mask = ready_to_mask(&guard);
                    guard.clear_ready();
                    Some((*socket, mask))
                }
                _ => None,
            };
        }

        let per_fd =
            Duration::from_millis((wait.as_millis() / self.fds.len().max(1) as u128).max(1) as u64);
        for (socket, async_fd, interest) in &self.fds {
            if let Ok(Ok(mut guard)) = timeout_async(per_fd, async_fd.ready(*interest)).await {
                let mask = ready_to_mask(&guard);
                guard.clear_ready();
                return Some((*socket, mask));
            }
        }
        None
    }
}

#[cfg(unix)]
fn ready_to_mask(guard: &tokio::io::unix::AsyncFdReadyGuard<'_, RawSocketFd>) -> c_int {
    let ready = guard.ready();
    let mut mask = 0;
    if ready.is_readable() {
        mask |= CURL_CSELECT_IN;
    }
    if ready.is_writable() {
        mask |= CURL_CSELECT_OUT;
    }
    if ready.is_error() || mask == 0 {
        mask |= CURL_CSELECT_ERR;
    }
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
            guard.socket_events.retain(|e| e.0 != socket);
        } else if let Some(entry) = guard.socket_events.iter_mut().find(|e| e.0 == socket) {
            entry.1 = what;
        } else {
            guard.socket_events.push((socket, what));
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
    sock_buf: &mut SmallVec<[(CurlSocket, c_int); 4]>,
) -> Result<()> {
    let timer_timeout_ms = {
        let guard = callback_state
            .lock()
            .map_err(|_| anyhow::anyhow!("curl multi callback state lock poisoned"))?;
        sock_buf.clear();
        sock_buf.extend_from_slice(&guard.socket_events);
        guard.timeout_ms
    };

    fd_cache.sync_with(sock_buf);

    let default_wait_ms = default_wait.as_millis().min(i64::MAX as u128) as i64;
    let wait_ms = timer_timeout_ms
        .map(|ms| ms.max(0).min(default_wait_ms))
        .unwrap_or(default_wait_ms);
    let wait_duration = Duration::from_millis(wait_ms.max(0) as u64);

    let mut running_handles = 0;

    let action = if fd_cache.fds.is_empty() {
        if wait_ms > 0 {
            sleep(wait_duration).await;
        }
        None
    } else {
        fd_cache.wait_any_ready(wait_duration).await
    };

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

/// Returns true if shutdown requested.
fn handle_cmd(
    api: &CurlApi,
    easy_handle: usize,
    cmd: WorkerCommand,
    pending_send: &mut Option<(Vec<u8>, usize)>,
    event_tx: &mpsc::Sender<WorkerEvent>,
) -> Result<bool> {
    match cmd {
        WorkerCommand::Send(data) => {
            match ws_send(api, easy_handle as *mut Curl, &data, CURLWS_TEXT) {
                Ok(sent) if sent >= data.len() => {}
                Ok(sent) => {
                    *pending_send = Some((data, sent));
                }
                Err(e) if matches!(&e, impcurl::ImpcurlError::Curl { code, .. } if *code == CURLE_AGAIN) =>
                {
                    *pending_send = Some((data, 0));
                }
                Err(e) => return Err(e.into()),
            }
            Ok(false)
        }
        WorkerCommand::Shutdown => {
            debug!("worker received shutdown command");
            let _ = event_tx.try_send(WorkerEvent::Shutdown);
            Ok(true)
        }
    }
}

#[cfg(unix)]
async fn run_worker_loop_async(
    api: &CurlApi,
    multi: &mut MultiSession<'_>,
    easy_handle: usize,
    _send_again_sleep: Duration,
    loop_poll_timeout: Duration,
    callback_state: &Mutex<MultiCallbackState>,
    mut cmd_rx: UnboundedReceiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> Result<()> {
    let mut assembler = WsFrameAssembler::default();
    let mut fd_cache = AsyncFdCache::new();
    let mut pending_send: Option<(Vec<u8>, usize)> = None;
    let mut pending_cmd: Option<WorkerCommand> = None;
    let mut sock_buf: SmallVec<[(CurlSocket, c_int); 4]> = SmallVec::new();
    loop {
        // recv all available frames
        while let Some(frame) = ws_try_recv_frame(api, easy_handle as *mut Curl, &mut assembler)? {
            if event_tx
                .try_send(WorkerEvent::Frame {
                    payload: frame.payload,
                })
                .is_err()
            {
                warn!("event channel full, frame dropped");
            }
        }

        // resume pending send
        if let Some((ref data, ref mut offset)) = pending_send {
            match ws_send(api, easy_handle as *mut Curl, &data[*offset..], CURLWS_TEXT) {
                Ok(sent) => {
                    *offset += sent;
                    if *offset >= data.len() {
                        pending_send = None;
                    }
                }
                Err(e) if matches!(&e, impcurl::ImpcurlError::Curl { code, .. } if *code == CURLE_AGAIN) =>
                    {}
                Err(e) => return Err(e.into()),
            }
        }

        // process buffered command from select!, then drain channel
        if let Some(cmd) = pending_cmd.take() {
            if handle_cmd(api, easy_handle, cmd, &mut pending_send, &event_tx)? {
                return Ok(());
            }
        }
        loop {
            match cmd_rx.try_recv() {
                Ok(cmd) => {
                    if handle_cmd(api, easy_handle, cmd, &mut pending_send, &event_tx)? {
                        return Ok(());
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    debug!("worker command channel disconnected");
                    let _ = event_tx.try_send(WorkerEvent::Shutdown);
                    return Ok(());
                }
                Err(TryRecvError::Empty) => break,
            }
        }

        // wait for socket ready OR command arrival
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(c) => pending_cmd = Some(c),
                    None => {
                        let _ = event_tx.try_send(WorkerEvent::Shutdown);
                        return Ok(());
                    }
                }
            }
            result = drive_multi_once_with_cache(multi, callback_state, &mut fd_cache, loop_poll_timeout, &mut sock_buf) => {
                result?;
            }
        }
    }
}

#[cfg(not(unix))]
fn run_worker_loop(
    api: &CurlApi,
    multi: &mut MultiSession<'_>,
    easy_handle: usize,
    _send_again_sleep: Duration,
    loop_poll_timeout: Duration,
    mut cmd_rx: UnboundedReceiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> Result<()> {
    let mut assembler = WsFrameAssembler::default();
    let mut pending_send: Option<(Vec<u8>, usize)> = None;
    loop {
        while let Some(frame) = ws_try_recv_frame(api, easy_handle as *mut Curl, &mut assembler)? {
            if event_tx
                .try_send(WorkerEvent::Frame {
                    payload: frame.payload,
                })
                .is_err()
            {
                warn!("event channel full, frame dropped");
            }
        }

        if let Some((ref data, ref mut offset)) = pending_send {
            match ws_send(api, easy_handle as *mut Curl, &data[*offset..], CURLWS_TEXT) {
                Ok(sent) => {
                    *offset += sent;
                    if *offset >= data.len() {
                        pending_send = None;
                    }
                }
                Err(impcurl::ImpcurlError::Curl { code, .. }) if code == CURLE_AGAIN => {}
                Err(e) => return Err(e.into()),
            }
        }

        loop {
            match cmd_rx.try_recv() {
                Ok(cmd) => {
                    if handle_cmd(api, easy_handle, cmd, &mut pending_send, &event_tx)? {
                        return Ok(());
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    debug!("worker command channel disconnected");
                    let _ = event_tx.try_send(WorkerEvent::Shutdown);
                    return Ok(());
                }
                Err(TryRecvError::Empty) => break,
            }
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
        headers: &cfg.headers,
        proxy: cfg.proxy.as_deref(),
        impersonate_target: cfg.impersonate_target,
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
