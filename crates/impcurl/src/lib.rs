use impcurl_sys::{
    CURL_GLOBAL_DEFAULT, CURL_HTTP_VERSION_1_1, CURL_SOCKET_TIMEOUT, CURLE_AGAIN, CURLE_OK,
    CURLM_OK, CURLMOPT_SOCKETDATA, CURLMOPT_SOCKETFUNCTION, CURLMOPT_TIMERDATA,
    CURLMOPT_TIMERFUNCTION, CURLMSG_DONE, CURLOPT_CONNECT_ONLY, CURLOPT_HTTP_VERSION,
    CURLOPT_HTTPHEADER, CURLOPT_URL, CURLOPT_VERBOSE, CURLWS_TEXT, Curl, CurlApi, CurlCode,
    CurlMCode, CurlMulti, CurlMultiSocketCallback, CurlMultiTimerCallback, CurlOption, CurlSlist,
    CurlWsFrame,
};
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_long, c_void};
use std::ptr;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;
use tracing::{debug, trace};

#[derive(Debug, thiserror::Error)]
pub enum ImpcurlError {
    #[error("{step} failed: {message} ({code})")]
    Curl {
        step: String,
        message: String,
        code: CurlCode,
    },
    #[error("{step} failed: {message} ({code})")]
    CurlMulti {
        step: String,
        message: String,
        code: CurlMCode,
    },
    #[error("curl_easy_init returned null")]
    NullEasyHandle,
    #[error("curl_multi_init returned null")]
    NullMultiHandle,
    #[error("header contains NUL byte: {0}")]
    InvalidCString(#[from] std::ffi::NulError),
    #[error("failed to append header: {0}")]
    HeaderAppend(String),
    #[error(transparent)]
    Sys(#[from] impcurl_sys::SysError),
    #[error("curl_ws_send returned CURLE_OK but sent 0 bytes")]
    SendZeroBytes,
    #[error("multi websocket handshake ended without CURLMSG_DONE for easy handle")]
    MissingHandshakeDoneMessage,
}

pub type Result<T> = std::result::Result<T, ImpcurlError>;

static GLOBAL_INIT: OnceLock<std::result::Result<(), String>> = OnceLock::new();

/// Ensure curl_global_init is called exactly once for the process lifetime.
/// Never calls curl_global_cleanup — the library stays loaded until process exit.
/// If the first call fails, subsequent calls return the same error.
pub fn ensure_curl_global_init(api: &CurlApi) -> Result<()> {
    let result = GLOBAL_INIT.get_or_init(|| {
        debug!("initializing curl global state");
        let code = unsafe { (api.global_init)(CURL_GLOBAL_DEFAULT) };
        match check_code(api, code, "curl_global_init") {
            Ok(()) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    });
    match result {
        Ok(()) => Ok(()),
        Err(msg) => Err(ImpcurlError::Curl {
            step: "curl_global_init".to_owned(),
            message: msg.clone(),
            code: -1,
        }),
    }
}

pub struct EasySession<'a> {
    api: &'a CurlApi,
    easy: *mut Curl,
    headers: *mut CurlSlist,
}

// The easy handle is owned by the session and is never accessed concurrently.
unsafe impl Send for EasySession<'_> {}

impl Drop for EasySession<'_> {
    fn drop(&mut self) {
        unsafe {
            if !self.headers.is_null() {
                (self.api.slist_free_all)(self.headers);
            }
            if !self.easy.is_null() {
                (self.api.easy_cleanup)(self.easy);
            }
        }
    }
}

impl<'a> EasySession<'a> {
    pub fn new(api: &'a CurlApi) -> Result<Self> {
        let easy = unsafe { (api.easy_init)() };
        if easy.is_null() {
            return Err(ImpcurlError::NullEasyHandle);
        }
        Ok(Self {
            api,
            easy,
            headers: ptr::null_mut(),
        })
    }

    pub fn append_header(&mut self, header: &str) -> Result<()> {
        let c_header = CString::new(header)?;
        let new_list = unsafe { (self.api.slist_append)(self.headers, c_header.as_ptr()) };
        if new_list.is_null() {
            return Err(ImpcurlError::HeaderAppend(header.to_owned()));
        }
        self.headers = new_list;
        Ok(())
    }

    pub fn easy_handle(&self) -> *mut Curl {
        self.easy
    }

    pub fn api(&self) -> &CurlApi {
        self.api
    }
}

pub struct MultiSession<'a> {
    api: &'a CurlApi,
    multi: *mut CurlMulti,
}

// The multi handle is owned by the session and is never accessed concurrently.
unsafe impl Send for MultiSession<'_> {}

impl Drop for MultiSession<'_> {
    fn drop(&mut self) {
        if self.multi.is_null() {
            return;
        }
        unsafe {
            let _ = (self.api.multi_cleanup)(self.multi);
        }
    }
}

impl<'a> MultiSession<'a> {
    pub fn new(api: &'a CurlApi) -> Result<Self> {
        let multi = unsafe { (api.multi_init)() };
        if multi.is_null() {
            return Err(ImpcurlError::NullMultiHandle);
        }
        Ok(Self { api, multi })
    }

    pub fn add_easy(&self, easy: *mut Curl) -> Result<()> {
        let code = unsafe { (self.api.multi_add_handle)(self.multi, easy) };
        check_multi_code(self.api, code, "curl_multi_add_handle")
    }

    pub fn remove_easy(&self, easy: *mut Curl) -> Result<()> {
        let code = unsafe { (self.api.multi_remove_handle)(self.multi, easy) };
        check_multi_code(self.api, code, "curl_multi_remove_handle")
    }

    pub fn set_socket_callback(
        &self,
        callback: Option<CurlMultiSocketCallback>,
        userp: *mut c_void,
    ) -> Result<()> {
        let cb_code =
            unsafe { (self.api.multi_setopt)(self.multi, CURLMOPT_SOCKETFUNCTION, callback) };
        check_multi_code(
            self.api,
            cb_code,
            "curl_multi_setopt(CURLMOPT_SOCKETFUNCTION)",
        )?;

        let data_code = unsafe { (self.api.multi_setopt)(self.multi, CURLMOPT_SOCKETDATA, userp) };
        check_multi_code(
            self.api,
            data_code,
            "curl_multi_setopt(CURLMOPT_SOCKETDATA)",
        )
    }

    pub fn set_timer_callback(
        &self,
        callback: Option<CurlMultiTimerCallback>,
        userp: *mut c_void,
    ) -> Result<()> {
        let cb_code =
            unsafe { (self.api.multi_setopt)(self.multi, CURLMOPT_TIMERFUNCTION, callback) };
        check_multi_code(
            self.api,
            cb_code,
            "curl_multi_setopt(CURLMOPT_TIMERFUNCTION)",
        )?;

        let data_code = unsafe { (self.api.multi_setopt)(self.multi, CURLMOPT_TIMERDATA, userp) };
        check_multi_code(self.api, data_code, "curl_multi_setopt(CURLMOPT_TIMERDATA)")
    }

    pub fn fdset(
        &self,
        readfds: *mut c_void,
        writefds: *mut c_void,
        errfds: *mut c_void,
        max_fd: &mut c_int,
    ) -> Result<()> {
        let code = unsafe { (self.api.multi_fdset)(self.multi, readfds, writefds, errfds, max_fd) };
        check_multi_code(self.api, code, "curl_multi_fdset")
    }

    pub fn timeout_ms(&self) -> Result<c_long> {
        let mut timeout_ms: c_long = -1;
        let code = unsafe { (self.api.multi_timeout)(self.multi, &mut timeout_ms) };
        check_multi_code(self.api, code, "curl_multi_timeout")?;
        Ok(timeout_ms)
    }

    pub fn perform(&self, running_handles: &mut i32) -> Result<()> {
        let code = unsafe { (self.api.multi_perform)(self.multi, running_handles) };
        check_multi_code(self.api, code, "curl_multi_perform")
    }

    pub fn socket_action(
        &self,
        socket: impcurl_sys::CurlSocket,
        ev_bitmask: c_int,
        running_handles: &mut i32,
    ) -> Result<()> {
        let code = unsafe {
            (self.api.multi_socket_action)(self.multi, socket, ev_bitmask, running_handles)
        };
        check_multi_code(self.api, code, "curl_multi_socket_action")
    }

    pub fn socket_action_timeout(&self, running_handles: &mut i32) -> Result<()> {
        self.socket_action(CURL_SOCKET_TIMEOUT, 0, running_handles)
    }

    pub fn poll(&self, timeout: Duration) -> Result<i32> {
        let timeout_ms = timeout.as_millis().clamp(0, c_int::MAX as u128) as c_int;
        let mut numfds = 0;
        let code = unsafe {
            (self.api.multi_poll)(self.multi, ptr::null_mut(), 0, timeout_ms, &mut numfds)
        };
        check_multi_code(self.api, code, "curl_multi_poll")?;
        Ok(numfds)
    }

    pub fn read_done_message_for_easy(&self, easy: *mut Curl) -> Option<CurlCode> {
        let mut msgs_in_queue = 0;
        loop {
            let msg = unsafe { (self.api.multi_info_read)(self.multi, &mut msgs_in_queue) };
            if msg.is_null() {
                return None;
            }

            let msg_ref = unsafe { &*msg };
            if msg_ref.msg != CURLMSG_DONE || msg_ref.easy_handle != easy {
                continue;
            }

            return Some(unsafe { msg_ref.done_result() });
        }
    }
}

pub struct WebSocketConnectConfig<'a> {
    pub url: &'a str,
    pub origin: Option<&'a str>,
    pub user_agent: Option<&'a str>,
    pub impersonate_target: &'a str,
    pub verbose: bool,
}

fn configure_connect_only_websocket_session(
    session: &mut EasySession<'_>,
    cfg: &WebSocketConnectConfig<'_>,
) -> Result<()> {
    {
        let api = session.api();
        let c_target = CString::new(cfg.impersonate_target)?;
        let impersonate_code =
            unsafe { (api.easy_impersonate)(session.easy_handle(), c_target.as_ptr(), 1) };
        check_code(api, impersonate_code, "curl_easy_impersonate")?;

        let c_url = CString::new(cfg.url)?;
        unsafe {
            setopt_cstr(
                api,
                session.easy_handle(),
                CURLOPT_URL,
                c_url.as_ptr(),
                "CURLOPT_URL",
            )?;
            setopt_long(
                api,
                session.easy_handle(),
                CURLOPT_HTTP_VERSION,
                CURL_HTTP_VERSION_1_1,
                "CURLOPT_HTTP_VERSION",
            )?;
            setopt_long(
                api,
                session.easy_handle(),
                CURLOPT_CONNECT_ONLY,
                2,
                "CURLOPT_CONNECT_ONLY=2",
            )?;
            if cfg.verbose {
                setopt_long(
                    api,
                    session.easy_handle(),
                    CURLOPT_VERBOSE,
                    1,
                    "CURLOPT_VERBOSE",
                )?;
            }
        }
    }

    if let Some(origin) = cfg.origin {
        session.append_header(&format!("Origin: {origin}"))?;
    }
    if let Some(user_agent) = cfg.user_agent {
        session.append_header(&format!("User-Agent: {user_agent}"))?;
    }

    {
        let api = session.api();
        unsafe {
            setopt_slist(
                api,
                session.easy_handle(),
                CURLOPT_HTTPHEADER,
                session.headers,
                "CURLOPT_HTTPHEADER",
            )?;
        }
    }

    Ok(())
}

pub fn prepare_connect_only_websocket_session<'a>(
    api: &'a CurlApi,
    cfg: &WebSocketConnectConfig<'_>,
) -> Result<EasySession<'a>> {
    debug!(url = cfg.url, impersonate = cfg.impersonate_target, "preparing websocket session");
    let mut session = EasySession::new(api)?;
    configure_connect_only_websocket_session(&mut session, cfg)?;
    Ok(session)
}

pub fn complete_connect_only_websocket_handshake_with_multi(
    api: &CurlApi,
    multi: &MultiSession<'_>,
    easy: *mut Curl,
    poll_timeout: Duration,
) -> Result<()> {
    multi.add_easy(easy)?;
    let mut running_handles = 0;
    multi.socket_action_timeout(&mut running_handles)?;
    multi.perform(&mut running_handles)?;

    loop {
        if let Some(done_code) = multi.read_done_message_for_easy(easy) {
            check_code(
                api,
                done_code,
                "curl_multi_socket_action websocket handshake",
            )?;
            debug!("websocket handshake complete");
            return Ok(());
        }

        if running_handles <= 0 {
            return Err(ImpcurlError::MissingHandshakeDoneMessage);
        }

        let _ = multi.poll(poll_timeout)?;
        multi.socket_action_timeout(&mut running_handles)?;
        multi.perform(&mut running_handles)?;
    }
}

pub fn detach_easy_from_multi(multi: &MultiSession<'_>, easy: *mut Curl) -> Result<()> {
    multi.remove_easy(easy)
}

pub fn open_connect_only_websocket_session<'a>(
    api: &'a CurlApi,
    cfg: &WebSocketConnectConfig<'_>,
) -> Result<EasySession<'a>> {
    let session = prepare_connect_only_websocket_session(api, cfg)?;
    let multi = MultiSession::new(api)?;
    complete_connect_only_websocket_handshake_with_multi(
        api,
        &multi,
        session.easy_handle(),
        Duration::from_millis(500),
    )?;
    detach_easy_from_multi(&multi, session.easy_handle())?;

    Ok(session)
}

pub fn check_code(api: &CurlApi, code: CurlCode, step: &str) -> Result<()> {
    if code == CURLE_OK {
        Ok(())
    } else {
        Err(ImpcurlError::Curl {
            step: step.to_owned(),
            message: api.error_text(code),
            code,
        })
    }
}

pub fn check_multi_code(api: &CurlApi, code: CurlMCode, step: &str) -> Result<()> {
    if code == CURLM_OK {
        Ok(())
    } else {
        Err(ImpcurlError::CurlMulti {
            step: step.to_owned(),
            message: api.multi_error_text(code),
            code,
        })
    }
}

unsafe fn setopt_long(
    api: &CurlApi,
    easy: *mut Curl,
    option: CurlOption,
    value: c_long,
    step: &str,
) -> Result<()> {
    let code = unsafe { (api.easy_setopt)(easy, option, value) };
    check_code(api, code, step)
}

unsafe fn setopt_cstr(
    api: &CurlApi,
    easy: *mut Curl,
    option: CurlOption,
    value: *const c_char,
    step: &str,
) -> Result<()> {
    let code = unsafe { (api.easy_setopt)(easy, option, value) };
    check_code(api, code, step)
}

unsafe fn setopt_slist(
    api: &CurlApi,
    easy: *mut Curl,
    option: CurlOption,
    value: *mut CurlSlist,
    step: &str,
) -> Result<()> {
    let code = unsafe { (api.easy_setopt)(easy, option, value) };
    check_code(api, code, step)
}

#[derive(Default)]
pub struct WsFrameAssembler {
    frame_flags: i32,
    complete: Vec<u8>,
    started: bool,
}

pub struct WsFrame {
    pub flags: i32,
    pub payload: Vec<u8>,
}

pub fn ws_try_recv_frame(
    api: &CurlApi,
    easy: *mut Curl,
    assembler: &mut WsFrameAssembler,
) -> Result<Option<WsFrame>> {
    loop {
        let mut recv_buf = [0u8; 16 * 1024];
        let mut received = 0usize;
        let mut meta_ptr: *const CurlWsFrame = ptr::null();

        let code = unsafe {
            (api.ws_recv)(
                easy,
                recv_buf.as_mut_ptr().cast(),
                recv_buf.len(),
                &mut received,
                &mut meta_ptr,
            )
        };

        if code == CURLE_AGAIN {
            return Ok(None);
        }

        if code != CURLE_OK {
            return Err(ImpcurlError::Curl {
                step: "curl_ws_recv".to_owned(),
                message: api.error_text(code),
                code,
            });
        }

        if !meta_ptr.is_null() {
            let meta = unsafe { &*meta_ptr };
            if !assembler.started {
                assembler.frame_flags = meta.flags;
                assembler.started = true;
                let total = received as i64 + meta.bytesleft;
                if total > 0 {
                    assembler.complete.reserve(total as usize);
                }
            }

            if received > 0 {
                assembler.complete.extend_from_slice(&recv_buf[..received]);
            }

            if meta.bytesleft == 0 {
                assembler.started = false;
                let flags = assembler.frame_flags;
                let payload = std::mem::take(&mut assembler.complete);
                trace!(len = payload.len(), flags, "ws frame assembled");
                return Ok(Some(WsFrame { flags, payload }));
            }
            continue;
        }

        if received > 0 {
            if assembler.started {
                assembler.complete.extend_from_slice(&recv_buf[..received]);
                assembler.started = false;
                let flags = assembler.frame_flags;
                let payload = std::mem::take(&mut assembler.complete);
                return Ok(Some(WsFrame { flags, payload }));
            }
            return Ok(Some(WsFrame {
                flags: 0,
                payload: recv_buf[..received].to_vec(),
            }));
        }
    }
}

pub fn ws_send_text(
    api: &CurlApi,
    easy: *mut Curl,
    text: &str,
    again_sleep: Duration,
) -> Result<()> {
    let payload = text.as_bytes();
    let mut offset = 0usize;

    while offset < payload.len() {
        let mut sent = 0usize;
        let code = unsafe {
            (api.ws_send)(
                easy,
                payload[offset..].as_ptr().cast(),
                payload.len() - offset,
                &mut sent,
                0,
                CURLWS_TEXT,
            )
        };

        if code == CURLE_OK {
            if sent == 0 {
                return Err(ImpcurlError::SendZeroBytes);
            }
            offset += sent;
            continue;
        }

        if code == CURLE_AGAIN {
            thread::sleep(again_sleep);
            continue;
        }

        return Err(ImpcurlError::Curl {
            step: "curl_ws_send".to_owned(),
            message: api.error_text(code),
            code,
        });
    }

    Ok(())
}

pub async fn ws_send_text_async(
    api: &CurlApi,
    easy_handle: usize,
    text: &str,
    again_sleep: Duration,
) -> Result<()> {
    let payload = text.as_bytes();
    let mut offset = 0usize;

    while offset < payload.len() {
        let result = ws_send_once(api, easy_handle as *mut Curl, &payload[offset..]);
        match result {
            Ok(sent) => {
                offset += sent;
            }
            Err(ImpcurlError::Curl { code, .. }) if code == CURLE_AGAIN => {
                tokio::time::sleep(again_sleep).await;
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

fn ws_send_once(api: &CurlApi, easy: *mut Curl, data: &[u8]) -> Result<usize> {
    let mut sent = 0usize;
    let code = unsafe {
        (api.ws_send)(
            easy,
            data.as_ptr().cast(),
            data.len(),
            &mut sent,
            0,
            CURLWS_TEXT,
        )
    };
    if code == CURLE_OK {
        if sent == 0 {
            return Err(ImpcurlError::SendZeroBytes);
        }
        Ok(sent)
    } else {
        Err(ImpcurlError::Curl {
            step: "curl_ws_send".to_owned(),
            message: api.error_text(code),
            code,
        })
    }
}
