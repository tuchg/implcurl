use anyhow::{Context as AnyhowContext, Result as AnyResult, bail};
use bytes::Bytes;
use futures_core::Stream;
use futures_sink::Sink;
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
use std::collections::VecDeque;
#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(unix)]
use std::os::raw::{c_int, c_long, c_void};
use std::path::PathBuf;
use std::pin::Pin;
#[cfg(unix)]
use smallvec::SmallVec;
#[cfg(unix)]
use std::sync::Mutex;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
#[cfg(unix)]
use tokio::io::Interest;
#[cfg(unix)]
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self, Receiver};
#[cfg(unix)]
use tokio::time::sleep;
#[cfg(unix)]
use tokio::time::timeout as timeout_async;
use tokio::time::timeout;
use tokio_util::sync::{PollSendError, PollSender};
use tracing::{debug, info, warn};
use thiserror::Error as ThisError;

const CURLWS_BINARY: u32 = 1 << 1;
const CURLWS_CLOSE: u32 = 1 << 3;
const CURLWS_PING: u32 = 1 << 4;
const CURLWS_PONG: u32 = 1 << 6;

/// Result type for `impcurl-ws` public APIs.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by `WsConnection` and its builder.
#[derive(Clone, Debug, ThisError, PartialEq, Eq)]
pub enum Error {
    /// The runtime `libcurl-impersonate` library could not be resolved or loaded.
    #[error("failed to resolve runtime library: {message}")]
    RuntimeLibrary { message: String },
    /// The WebSocket handshake failed before the connection became usable.
    #[error("failed to connect websocket: {message}")]
    Connect { message: String },
    /// The background transport task failed after the connection had been established.
    #[error("websocket transport error: {message}")]
    Transport { message: String },
    /// The received or outbound WebSocket frame was not protocol-valid for this API surface.
    #[error("websocket protocol error: {message}")]
    Protocol { message: String },
    /// The caller attempted to send a message that is invalid before it reaches libcurl.
    #[error("invalid websocket message: {message}")]
    InvalidMessage { message: String },
    /// The connection is no longer open for sending or receiving.
    #[error("websocket connection is closed")]
    Closed,
}

/// Standard WebSocket close code wrapper.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CloseCode(u16);

impl CloseCode {
    /// 1000: normal closure.
    pub const NORMAL: Self = Self(1000);
    /// 1001: endpoint is going away.
    pub const GOING_AWAY: Self = Self(1001);
    /// 1002: protocol error.
    pub const PROTOCOL_ERROR: Self = Self(1002);
    /// 1003: unsupported data.
    pub const UNSUPPORTED_DATA: Self = Self(1003);
    /// 1007: invalid frame payload data.
    pub const INVALID_FRAME_PAYLOAD_DATA: Self = Self(1007);
    /// 1008: policy violation.
    pub const POLICY_VIOLATION: Self = Self(1008);
    /// 1009: message too big.
    pub const MESSAGE_TOO_BIG: Self = Self(1009);
    /// 1010: extension required.
    pub const MANDATORY_EXTENSION: Self = Self(1010);
    /// 1011: internal server error.
    pub const INTERNAL_SERVER_ERROR: Self = Self(1011);

    /// Creates a close code from its raw numeric representation.
    pub const fn from_u16(code: u16) -> Self {
        Self(code)
    }

    /// Returns the raw RFC 6455 close code.
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

/// Structured close payload surfaced by `Message::Close`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseFrame {
    /// Close status code.
    pub code: CloseCode,
    /// UTF-8 reason string carried by the close frame.
    pub reason: String,
}

/// Full WebSocket message surface exposed by `WsConnection`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// UTF-8 text data.
    Text(String),
    /// Binary payload bytes.
    Binary(Bytes),
    /// Ping control frame.
    Ping(Bytes),
    /// Pong control frame.
    Pong(Bytes),
    /// Close control frame, including optional close payload.
    Close(Option<CloseFrame>),
}

/// Controls how incoming control frames are surfaced and optionally auto-replied to.
///
/// `Manual` keeps the connection fully explicit: callers receive `Message::Ping`,
/// `Message::Pong`, and `Message::Close` items from the stream and must decide how to react.
/// `AutoReply` still surfaces those control frames, but can also enqueue protocol-compliant
/// replies for common cases such as responding to `Ping` with `Pong`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ControlFrameMode {
    /// Surface all control frames and leave protocol actions to the caller.
    #[default]
    Manual,
    /// Keep control frames visible while automatically replying to common control frames.
    AutoReply {
        /// Automatically enqueue a pong when a ping is received.
        pong: bool,
        /// Automatically reply to a peer close frame if the caller has not sent one.
        close_reply: bool,
    },
}

#[derive(Debug)]
enum WorkerCommand {
    Send { sequence: u64, message: Message },
}

#[derive(Debug)]
enum WorkerEvent {
    Connected,
    Message(Message),
    Flushed(u64),
    Error(Error),
    Shutdown,
}

#[derive(Clone, Debug)]
struct WsConnectionConfig {
    resolved_lib_path: Option<PathBuf>,
    url: String,
    headers: Vec<String>,
    proxy: Option<String>,
    impersonate_target: impcurl::ImpersonateTarget,
    verbose: bool,
    connect_timeout: Duration,
    multi_poll_timeout: Duration,
    send_again_sleep: Duration,
    loop_sleep: Duration,
    read_buffer_messages: usize,
    write_buffer_messages: usize,
    control_frame_mode: ControlFrameMode,
}

impl WsConnectionConfig {
    fn new(url: impl Into<String>) -> Self {
        Self {
            resolved_lib_path: None,
            url: url.into(),
            headers: Vec::new(),
            proxy: None,
            impersonate_target: impcurl::ImpersonateTarget::Chrome136,
            verbose: false,
            connect_timeout: Duration::from_secs(20),
            multi_poll_timeout: Duration::from_millis(500),
            send_again_sleep: Duration::from_millis(20),
            loop_sleep: Duration::from_millis(40),
            read_buffer_messages: 1024,
            write_buffer_messages: 64,
            control_frame_mode: ControlFrameMode::Manual,
        }
    }
}

/// Builder for establishing a `WsConnection`.
///
/// Handshake-related options such as headers, proxy, impersonation target, and connect timeout
/// live here. Runtime transport semantics such as read/write buffering and control-frame reply
/// behavior are configured here as well, before `connect()` consumes the builder.
pub struct WsConnectionBuilder {
    cfg: WsConnectionConfig,
}

impl WsConnectionBuilder {
    /// Appends an HTTP header used during the WebSocket handshake.
    pub fn header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.cfg.headers.push(format!("{}: {}", name.as_ref(), value.as_ref()));
        self
    }

    /// Configures an explicit proxy URI for the handshake connection.
    pub fn proxy(mut self, proxy: impl Into<String>) -> Self {
        self.cfg.proxy = Some(proxy.into());
        self
    }

    /// Selects the browser fingerprint impersonation profile used by `libcurl-impersonate`.
    pub fn impersonate(mut self, target: impcurl::ImpersonateTarget) -> Self {
        self.cfg.impersonate_target = target;
        self
    }

    /// Sets how long the initial WebSocket handshake may take.
    pub fn connect_timeout(mut self, t: Duration) -> Self {
        self.cfg.connect_timeout = t;
        self
    }

    /// Enables or disables verbose libcurl logging for this connection.
    pub fn verbose(mut self, on: bool) -> Self {
        self.cfg.verbose = on;
        self
    }

    /// Chooses whether control frames are fully manual or should auto-reply.
    ///
    /// This setting does not hide control frames from the stream. It only controls whether the
    /// worker automatically writes protocol replies such as `Pong` or close acknowledgements.
    pub fn control_frame_mode(mut self, mode: ControlFrameMode) -> Self {
        self.cfg.control_frame_mode = mode;
        self
    }

    /// Sets the bounded number of logical messages that may wait for the caller to read them.
    ///
    /// When this queue is full, the worker stops forwarding additional decoded messages until the
    /// caller polls the stream again.
    pub fn read_buffer_messages(mut self, capacity: usize) -> Self {
        self.cfg.read_buffer_messages = capacity.max(1);
        self
    }

    /// Sets the bounded number of logical messages that may wait to be written by the worker.
    ///
    /// This value drives backpressure for the `Sink<Message>` implementation: once the queue is
    /// full, `poll_ready` returns `Pending` until capacity becomes available.
    pub fn write_buffer_messages(mut self, capacity: usize) -> Self {
        self.cfg.write_buffer_messages = capacity.max(1);
        self
    }

    /// Opens the WebSocket connection and returns a `Stream + Sink` connection object.
    ///
    /// The returned connection starts producing `Message` items only after the WebSocket handshake
    /// has completed successfully. Handshake failures are returned as `Error::Connect`.
    pub async fn connect(self) -> Result<WsConnection> {
        WsConnection::connect_inner(self.cfg).await
    }
}

/// A connected WebSocket transport that implements `Stream` and `Sink<Message>`.
///
/// `Stream` yields typed WebSocket messages, including control frames. `Sink<Message>` applies
/// bounded backpressure via the configured write queue. `poll_flush` completes only after all
/// previously accepted logical messages have been handed off to libcurl's websocket send path.
pub struct WsConnection {
    cmd_tx: PollSender<WorkerCommand>,
    event_rx: mpsc::Receiver<WorkerEvent>,
    worker: Option<tokio::task::JoinHandle<()>>,
    pending_messages: VecDeque<Message>,
    next_sequence: u64,
    last_enqueued_sequence: Option<u64>,
    last_flushed_sequence: Option<u64>,
    terminal_error: Option<Error>,
    terminal_error_streamed: bool,
    terminated: bool,
}

impl WsConnection {
    /// Connects to a WebSocket URL using default builder settings.
    ///
    /// This is a convenience wrapper around `WsConnection::builder(url).connect().await`.
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        Self::connect_inner(WsConnectionConfig::new(url)).await
    }

    /// Returns a builder for advanced handshake and runtime configuration.
    pub fn builder(url: impl Into<String>) -> WsConnectionBuilder {
        WsConnectionBuilder {
            cfg: WsConnectionConfig::new(url),
        }
    }

    async fn connect_inner(mut cfg: WsConnectionConfig) -> Result<Self> {
        let resolved_lib = resolve_lib_path_for_connect()?;
        cfg.resolved_lib_path = Some(resolved_lib);

        let connect_timeout = cfg.connect_timeout;
        let event_capacity = cfg.read_buffer_messages;
        let command_capacity = cfg.write_buffer_messages;
        let (raw_cmd_tx, cmd_rx) = mpsc::channel::<WorkerCommand>(command_capacity);
        let (event_tx, mut event_rx) = mpsc::channel::<WorkerEvent>(event_capacity);

        let url = cfg.url.clone();
        let worker = tokio::spawn(async move {
            if let Err(err) = run_worker(cfg, cmd_rx, event_tx.clone()).await {
                let _ = event_tx
                    .send(WorkerEvent::Error(Error::Transport {
                        message: format!("{err:#}"),
                    }))
                    .await;
            }
        });

        info!(url = %url, "connecting websocket");
        let connected = timeout(connect_timeout, event_rx.recv())
            .await
            .map_err(|_| Error::Connect {
                message: "timed out waiting for websocket connect event".to_owned(),
            })?;

        match connected {
            Some(WorkerEvent::Connected) => info!("websocket connected"),
            Some(WorkerEvent::Error(err)) => {
                warn!(err = %err, "worker failed before connect");
                return Err(match err {
                    Error::Transport { message } => Error::Connect { message },
                    other => other,
                });
            }
            Some(WorkerEvent::Shutdown) | None => {
                return Err(Error::Connect {
                    message: "worker stopped before websocket connected".to_owned(),
                });
            }
            Some(WorkerEvent::Message(_)) | Some(WorkerEvent::Flushed(_)) => {
                return Err(Error::Connect {
                    message: "worker produced application events before websocket connected"
                        .to_owned(),
                });
            }
        }

        Ok(Self {
            cmd_tx: PollSender::new(raw_cmd_tx),
            event_rx,
            worker: Some(worker),
            pending_messages: VecDeque::new(),
            next_sequence: 1,
            last_enqueued_sequence: None,
            last_flushed_sequence: None,
            terminal_error: None,
            terminal_error_streamed: false,
            terminated: false,
        })
    }

    fn note_event(&mut self, event: WorkerEvent) -> Option<Result<Message>> {
        match event {
            WorkerEvent::Connected => None,
            WorkerEvent::Message(message) => Some(Ok(message)),
            WorkerEvent::Flushed(sequence) => {
                self.last_flushed_sequence = Some(sequence);
                None
            }
            WorkerEvent::Error(err) => {
                self.terminal_error = Some(err.clone());
                self.terminated = true;
                Some(Err(err))
            }
            WorkerEvent::Shutdown => {
                self.terminated = true;
                None
            }
        }
    }

    fn flush_satisfied(&self) -> bool {
        match self.last_enqueued_sequence {
            Some(last_enqueued) => self.last_flushed_sequence.unwrap_or(0) >= last_enqueued,
            None => true,
        }
    }

    fn validate_message(message: &Message) -> Result<()> {
        match message {
            Message::Ping(payload) | Message::Pong(payload) if payload.len() > 125 => {
                Err(Error::InvalidMessage {
                    message: "ping and pong payloads must be 125 bytes or smaller".to_owned(),
                })
            }
            Message::Close(Some(frame)) if !is_valid_close_code(frame.code) => {
                Err(Error::InvalidMessage {
                    message: format!("invalid websocket close code: {}", frame.code.as_u16()),
                })
            }
            Message::Close(Some(frame)) if frame.reason.len() > 123 => Err(Error::InvalidMessage {
                message: "close reason must fit within the WebSocket control frame payload budget"
                    .to_owned(),
            }),
            _ => Ok(()),
        }
    }

    fn map_poll_send_error(_err: PollSendError<WorkerCommand>) -> Error {
        Error::Closed
    }
}

impl Stream for WsConnection {
    type Item = Result<Message>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if let Some(message) = self.pending_messages.pop_front() {
            return Poll::Ready(Some(Ok(message)));
        }

        if let Some(err) = self.terminal_error.clone() {
            if !self.terminal_error_streamed {
                self.terminal_error_streamed = true;
                return Poll::Ready(Some(Err(err)));
            }
        }

        if self.terminated {
            return Poll::Ready(None);
        }

        loop {
            match Pin::new(&mut self.event_rx).poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(event)) => {
                    if let Some(item) = self.note_event(event) {
                        return Poll::Ready(Some(item));
                    }
                    if self.terminated {
                        if let Some(err) = self.terminal_error.clone() {
                            if !self.terminal_error_streamed {
                                self.terminal_error_streamed = true;
                                return Poll::Ready(Some(Err(err)));
                            }
                        }
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(None) => {
                    self.terminated = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Sink<Message> for WsConnection {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<()>> {
        if let Some(err) = self.terminal_error.clone() {
            return Poll::Ready(Err(err));
        }
        Pin::new(&mut self.cmd_tx)
            .poll_ready(cx)
            .map_err(Self::map_poll_send_error)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<()> {
        Self::validate_message(&item)?;
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.last_enqueued_sequence = Some(sequence);

        Pin::new(&mut self.cmd_tx)
            .start_send(WorkerCommand::Send { sequence, message: item })
            .map_err(Self::map_poll_send_error)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<()>> {
        if let Some(err) = self.terminal_error.clone() {
            return Poll::Ready(Err(err));
        }

        match Pin::new(&mut self.cmd_tx)
            .poll_flush(cx)
            .map_err(Self::map_poll_send_error)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {}
        }

        while !self.flush_satisfied() {
            match Pin::new(&mut self.event_rx).poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(event)) => {
                    if let Some(item) = self.note_event(event) {
                        if let Ok(message) = item {
                            self.pending_messages.push_back(message);
                        } else if let Some(err) = self.terminal_error.clone() {
                            return Poll::Ready(Err(err));
                        }
                    }
                }
                Poll::Ready(None) => {
                    self.terminated = true;
                    return if self.flush_satisfied() {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Ready(Err(Error::Closed))
                    };
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<()>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => Pin::new(&mut self.cmd_tx)
                .poll_close(cx)
                .map_err(Self::map_poll_send_error),
        }
    }
}

impl Drop for WsConnection {
    fn drop(&mut self) {
        self.cmd_tx.close();
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
) -> AnyResult<()> {
    let userp = (state as *const Mutex<MultiCallbackState>)
        .cast_mut()
        .cast::<c_void>();
    multi.set_socket_callback(Some(multi_socket_callback), userp)?;
    multi.set_timer_callback(Some(multi_timer_callback), userp)?;
    Ok(())
}

#[cfg(unix)]
fn clear_multi_callbacks(multi: &MultiSession<'_>) -> AnyResult<()> {
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
) -> AnyResult<()> {
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

#[derive(Debug)]
struct PendingSend {
    sequence: u64,
    payload: Vec<u8>,
    offset: usize,
    flags: u32,
}

fn encode_close_payload(frame: Option<CloseFrame>) -> AnyResult<Vec<u8>> {
    match frame {
        None => Ok(Vec::new()),
        Some(frame) => {
            if !is_valid_close_code(frame.code) {
                bail!("invalid websocket close code: {}", frame.code.as_u16());
            }
            let reason = frame.reason.into_bytes();
            if reason.len() > 123 {
                bail!("close reason is too large for a control frame");
            }

            let mut payload = Vec::with_capacity(2 + reason.len());
            payload.extend_from_slice(&frame.code.as_u16().to_be_bytes());
            payload.extend_from_slice(&reason);
            Ok(payload)
        }
    }
}

fn decode_close_payload(payload: Vec<u8>) -> AnyResult<Option<CloseFrame>> {
    if payload.is_empty() {
        return Ok(None);
    }
    if payload.len() == 1 {
        bail!("close frame payload must be empty or at least 2 bytes");
    }

    let code = CloseCode::from_u16(u16::from_be_bytes([payload[0], payload[1]]));
    if !is_valid_close_code(code) {
        bail!("invalid websocket close code: {}", code.as_u16());
    }
    let reason = String::from_utf8(payload[2..].to_vec())?;
    Ok(Some(CloseFrame {
        code,
        reason,
    }))
}

fn is_valid_close_code(code: CloseCode) -> bool {
    matches!(code.as_u16(), 1000..=1003 | 1007..=1014 | 3000..=4999)
}

fn encode_message(message: Message) -> AnyResult<(Vec<u8>, u32)> {
    match message {
        Message::Text(text) => Ok((text.into_bytes(), CURLWS_TEXT)),
        Message::Binary(payload) => Ok((payload.to_vec(), CURLWS_BINARY)),
        Message::Ping(payload) => Ok((payload.to_vec(), CURLWS_PING)),
        Message::Pong(payload) => Ok((payload.to_vec(), CURLWS_PONG)),
        Message::Close(frame) => Ok((encode_close_payload(frame)?, CURLWS_CLOSE)),
    }
}

fn decode_message(flags: u32, payload: Vec<u8>) -> AnyResult<Message> {
    if flags & CURLWS_TEXT != 0 {
        return Ok(Message::Text(String::from_utf8(payload)?));
    }
    if flags & CURLWS_CLOSE != 0 {
        return Ok(Message::Close(decode_close_payload(payload)?));
    }
    if flags & CURLWS_PING != 0 {
        return Ok(Message::Ping(Bytes::from(payload)));
    }
    if flags & CURLWS_PONG != 0 {
        return Ok(Message::Pong(Bytes::from(payload)));
    }
    Ok(Message::Binary(Bytes::from(payload)))
}

fn pending_send_from_message(sequence: u64, message: Message) -> AnyResult<PendingSend> {
    let (payload, flags) = encode_message(message)?;
    Ok(PendingSend {
        sequence,
        payload,
        offset: 0,
        flags,
    })
}

fn auto_reply_for_message(
    control_frame_mode: &ControlFrameMode,
    message: &Message,
) -> AnyResult<Option<PendingSend>> {
    match (control_frame_mode, message) {
        (ControlFrameMode::AutoReply { pong: true, .. }, Message::Ping(payload)) => {
            Ok(Some(pending_send_from_message(0, Message::Pong(payload.clone()))?))
        }
        (
            ControlFrameMode::AutoReply {
                close_reply: true,
                ..
            },
            Message::Close(frame),
        ) => Ok(Some(pending_send_from_message(0, Message::Close(frame.clone()))?)),
        _ => Ok(None),
    }
}

fn try_send_pending(api: &CurlApi, easy_handle: usize, pending: &mut PendingSend) -> AnyResult<bool> {
    match ws_send(
        api,
        easy_handle as *mut Curl,
        &pending.payload[pending.offset..],
        pending.flags,
    ) {
        Ok(sent) => {
            pending.offset += sent;
            Ok(pending.offset >= pending.payload.len())
        }
        Err(impcurl::ImpcurlError::Curl { code, .. }) if code == CURLE_AGAIN => Ok(false),
        Err(err) => Err(err.into()),
    }
}

async fn handle_cmd_async(
    api: &CurlApi,
    easy_handle: usize,
    cmd: WorkerCommand,
    pending_send: &mut Option<PendingSend>,
    event_tx: &mpsc::Sender<WorkerEvent>,
) -> AnyResult<bool> {
    match cmd {
        WorkerCommand::Send { sequence, message } => {
            let (payload, flags) = encode_message(message)?;
            let mut send = PendingSend {
                sequence,
                payload,
                offset: 0,
                flags,
            };
            if try_send_pending(api, easy_handle, &mut send)? {
                if event_tx.send(WorkerEvent::Flushed(sequence)).await.is_err() {
                    return Ok(true);
                }
            } else {
                *pending_send = Some(send);
            }
            Ok(false)
        }
    }
}

#[cfg(not(unix))]
fn handle_cmd_blocking(
    api: &CurlApi,
    easy_handle: usize,
    cmd: WorkerCommand,
    pending_send: &mut Option<PendingSend>,
    event_tx: &mpsc::Sender<WorkerEvent>,
) -> AnyResult<bool> {
    match cmd {
        WorkerCommand::Send { sequence, message } => {
            let (payload, flags) = encode_message(message)?;
            let mut send = PendingSend {
                sequence,
                payload,
                offset: 0,
                flags,
            };
            if try_send_pending(api, easy_handle, &mut send)? {
                if event_tx.blocking_send(WorkerEvent::Flushed(sequence)).is_err() {
                    return Ok(true);
                }
            } else {
                *pending_send = Some(send);
            }
            Ok(false)
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
    control_frame_mode: &ControlFrameMode,
    callback_state: &Mutex<MultiCallbackState>,
    mut cmd_rx: Receiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> AnyResult<()> {
    let mut assembler = WsFrameAssembler::default();
    let mut fd_cache = AsyncFdCache::new();
    let mut pending_send: Option<PendingSend> = None;
    let mut pending_cmd: Option<WorkerCommand> = None;
    let mut protocol_queue: VecDeque<PendingSend> = VecDeque::new();
    let mut sock_buf: SmallVec<[(CurlSocket, c_int); 4]> = SmallVec::new();
    loop {
        while let Some(frame) = ws_try_recv_frame(api, easy_handle as *mut Curl, &mut assembler)? {
            let message = decode_message(frame.flags as u32, frame.payload)?;
            if let Some(reply) = auto_reply_for_message(control_frame_mode, &message)? {
                protocol_queue.push_back(reply);
            }
            if event_tx.send(WorkerEvent::Message(message)).await.is_err() {
                return Ok(());
            }
        }

        if let Some(ref mut pending) = pending_send {
            if try_send_pending(api, easy_handle, pending)? {
                let sequence = pending.sequence;
                pending_send = None;
                if event_tx.send(WorkerEvent::Flushed(sequence)).await.is_err() {
                    return Ok(());
                }
            }
        }

        if pending_send.is_none() {
            if let Some(mut pending) = protocol_queue.pop_front() {
                if try_send_pending(api, easy_handle, &mut pending)? {
                    if pending.sequence != 0 && event_tx.send(WorkerEvent::Flushed(pending.sequence)).await.is_err() {
                        return Ok(());
                    }
                } else {
                    pending_send = Some(pending);
                }
            }
        }

        if let Some(cmd) = pending_cmd.take() {
            if handle_cmd_async(api, easy_handle, cmd, &mut pending_send, &event_tx).await? {
                return Ok(());
            }
        }
        loop {
            match cmd_rx.try_recv() {
                Ok(cmd) => {
                    if handle_cmd_async(api, easy_handle, cmd, &mut pending_send, &event_tx)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    debug!("worker command channel disconnected");
                    let _ = event_tx.send(WorkerEvent::Shutdown).await;
                    return Ok(());
                }
                Err(TryRecvError::Empty) => break,
            }
        }

        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(c) => pending_cmd = Some(c),
                    None => {
                        let _ = event_tx.send(WorkerEvent::Shutdown).await;
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
    control_frame_mode: &ControlFrameMode,
    mut cmd_rx: Receiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> AnyResult<()> {
    let mut assembler = WsFrameAssembler::default();
    let mut pending_send: Option<PendingSend> = None;
    let mut protocol_queue: VecDeque<PendingSend> = VecDeque::new();
    loop {
        while let Some(frame) = ws_try_recv_frame(api, easy_handle as *mut Curl, &mut assembler)? {
            let message = decode_message(frame.flags as u32, frame.payload)?;
            if let Some(reply) = auto_reply_for_message(control_frame_mode, &message)? {
                protocol_queue.push_back(reply);
            }
            if event_tx.blocking_send(WorkerEvent::Message(message)).is_err() {
                return Ok(());
            }
        }

        if let Some(ref mut pending) = pending_send {
            if try_send_pending(api, easy_handle, pending)? {
                let sequence = pending.sequence;
                pending_send = None;
                if event_tx.blocking_send(WorkerEvent::Flushed(sequence)).is_err() {
                    return Ok(());
                }
            }
        }

        if pending_send.is_none() {
            if let Some(mut pending) = protocol_queue.pop_front() {
                if try_send_pending(api, easy_handle, &mut pending)? {
                    if pending.sequence != 0 && event_tx.blocking_send(WorkerEvent::Flushed(pending.sequence)).is_err() {
                        return Ok(());
                    }
                } else {
                    pending_send = Some(pending);
                }
            }
        }

        loop {
            match cmd_rx.try_recv() {
                Ok(cmd) => {
                    if handle_cmd_blocking(api, easy_handle, cmd, &mut pending_send, &event_tx)? {
                        return Ok(());
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    debug!("worker command channel disconnected");
                    let _ = event_tx.blocking_send(WorkerEvent::Shutdown);
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
) -> AnyResult<()> {
    let _ = multi.poll(default_wait)?;
    let mut running_handles = 0;
    multi.socket_action_timeout(&mut running_handles)?;
    if running_handles > 0 {
        multi.perform(&mut running_handles)?;
    }
    Ok(())
}

async fn run_worker(
    cfg: WsConnectionConfig,
    cmd_rx: Receiver<WorkerCommand>,
    event_tx: mpsc::Sender<WorkerEvent>,
) -> AnyResult<()> {
    let lib_path = cfg
        .resolved_lib_path
        .as_ref()
        .context("internal error: missing resolved lib_path in worker config")?;
    let api = impcurl_sys::shared_curl_api(lib_path)?;
    ensure_curl_global_init(&api)?;
    let control_frame_mode = cfg.control_frame_mode.clone();

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

    if event_tx.send(WorkerEvent::Connected).await.is_err() {
        return Ok(());
    }

    #[cfg(unix)]
    let run_result = run_worker_loop_async(
        &api,
        &mut multi,
        session.easy_handle() as usize,
        cfg.send_again_sleep,
        cfg.loop_sleep,
        &control_frame_mode,
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
        &control_frame_mode,
        cmd_rx,
        event_tx,
    );
    #[cfg(unix)]
    let _ = clear_multi_callbacks(&multi);
    let _ = detach_easy_from_multi(&multi, session.easy_handle());
    run_result
}

fn resolve_lib_path_for_connect() -> Result<PathBuf> {
    impcurl_sys::resolve_impersonate_lib_path(&[])
        .map_err(|err| Error::RuntimeLibrary {
            message: format!(
                "{err:#}. set CURL_IMPERSONATE_LIB, set IMPCURL_LIB_DIR, or set IMPCURL_AUTO_FETCH=0 to disable runtime download attempts"
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_close_codes() {
        for code in [999_u16, 1004, 1005, 1006, 1015, 2000] {
            let err = WsConnection::validate_message(&Message::Close(Some(CloseFrame {
                code: CloseCode::from_u16(code),
                reason: String::new(),
            })))
            .expect_err("invalid close code should be rejected");

            assert!(matches!(err, Error::InvalidMessage { .. }));
        }
    }

    #[test]
    fn accepts_valid_close_codes() {
        for code in [1000_u16, 1001, 1002, 1012, 1013, 1014, 3000, 4999] {
            WsConnection::validate_message(&Message::Close(Some(CloseFrame {
                code: CloseCode::from_u16(code),
                reason: String::new(),
            })))
            .expect("valid close code should pass validation");
        }
    }

    #[test]
    fn auto_reply_mode_can_auto_reply_to_control_frames() {
        let mode = ControlFrameMode::AutoReply {
            pong: true,
            close_reply: true,
        };

        let ping_reply = auto_reply_for_message(&mode, &Message::Ping(Bytes::from_static(b"hi")))
            .expect("auto-reply ping reply should encode")
            .expect("auto-reply mode should enqueue pong");
        assert_eq!(ping_reply.sequence, 0);
        assert_eq!(ping_reply.flags, CURLWS_PONG);
        assert_eq!(ping_reply.payload, b"hi");

        let close_reply = auto_reply_for_message(
            &mode,
            &Message::Close(Some(CloseFrame {
                code: CloseCode::NORMAL,
                reason: "bye".to_owned(),
            })),
        )
        .expect("auto-reply close reply should encode")
        .expect("auto-reply mode should enqueue close reply");
        assert_eq!(close_reply.sequence, 0);
        assert_eq!(close_reply.flags, CURLWS_CLOSE);
        assert_eq!(decode_message(close_reply.flags, close_reply.payload).unwrap(), Message::Close(Some(CloseFrame {
            code: CloseCode::NORMAL,
            reason: "bye".to_owned(),
        })));
    }
}
