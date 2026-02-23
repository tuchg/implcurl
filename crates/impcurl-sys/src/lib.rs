use libloading::Library;
use tracing::{debug, info};
use serde_json::Value;
use std::ffi::CStr;
use std::fs::{self, File};
use std::io;
use std::mem::ManuallyDrop;
use std::os::raw::{c_char, c_int, c_long, c_short, c_uint, c_ulong, c_void};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};
use zip::ZipArchive;

pub type CurlCode = c_int;
pub type CurlOption = c_uint;
pub type CurlMCode = c_int;
pub type CurlMOption = c_int;

#[cfg(windows)]
pub type CurlSocket = usize;
#[cfg(not(windows))]
pub type CurlSocket = c_int;

pub type CurlMultiSocketCallback =
    unsafe extern "C" fn(*mut Curl, CurlSocket, c_int, *mut c_void, *mut c_void) -> c_int;
pub type CurlMultiTimerCallback =
    unsafe extern "C" fn(*mut CurlMulti, c_long, *mut c_void) -> c_int;

#[repr(C)]
pub struct Curl {
    _private: [u8; 0],
}

#[repr(C)]
pub struct CurlMulti {
    _private: [u8; 0],
}

#[repr(C)]
pub struct CurlSlist {
    pub data: *mut c_char,
    pub next: *mut CurlSlist,
}

#[repr(C)]
pub struct CurlWsFrame {
    pub age: c_int,
    pub flags: c_int,
    pub offset: i64,
    pub bytesleft: i64,
    pub len: usize,
}

#[repr(C)]
pub union CurlMessageData {
    pub whatever: *mut c_void,
    pub result: CurlCode,
}

#[repr(C)]
pub struct CurlMessage {
    pub msg: c_int,
    pub easy_handle: *mut Curl,
    pub data: CurlMessageData,
}

#[repr(C)]
pub struct CurlWaitFd {
    pub fd: CurlSocket,
    pub events: c_short,
    pub revents: c_short,
}

pub const CURLE_OK: CurlCode = 0;
pub const CURLE_AGAIN: CurlCode = 81;
pub const CURL_GLOBAL_DEFAULT: c_ulong = 3;
pub const CURLM_OK: CurlMCode = 0;
pub const CURLMSG_DONE: c_int = 1;

#[cfg(windows)]
pub const CURL_SOCKET_TIMEOUT: CurlSocket = usize::MAX;
#[cfg(not(windows))]
pub const CURL_SOCKET_TIMEOUT: CurlSocket = -1;

pub const CURL_CSELECT_IN: c_int = 0x01;
pub const CURL_CSELECT_OUT: c_int = 0x02;
pub const CURL_CSELECT_ERR: c_int = 0x04;

pub const CURL_POLL_NONE: c_int = 0;
pub const CURL_POLL_IN: c_int = 1;
pub const CURL_POLL_OUT: c_int = 2;
pub const CURL_POLL_INOUT: c_int = 3;
pub const CURL_POLL_REMOVE: c_int = 4;

pub const CURLMOPT_SOCKETFUNCTION: CurlMOption = 20001;
pub const CURLMOPT_SOCKETDATA: CurlMOption = 10002;
pub const CURLMOPT_TIMERFUNCTION: CurlMOption = 20004;
pub const CURLMOPT_TIMERDATA: CurlMOption = 10005;

pub const CURLOPT_URL: CurlOption = 10002;
pub const CURLOPT_HTTPHEADER: CurlOption = 10023;
pub const CURLOPT_HTTP_VERSION: CurlOption = 84;
pub const CURLOPT_CONNECT_ONLY: CurlOption = 141;
pub const CURLOPT_VERBOSE: CurlOption = 41;

pub const CURL_HTTP_VERSION_1_1: c_long = 2;
pub const CURLWS_TEXT: c_uint = 1;

#[derive(Debug, thiserror::Error)]
pub enum SysError {
    #[error("failed to load dynamic library {path}: {source}")]
    LoadLibrary {
        path: PathBuf,
        #[source]
        source: libloading::Error,
    },
    #[error("missing symbol {name}: {source}")]
    MissingSymbol {
        name: String,
        #[source]
        source: libloading::Error,
    },
    #[error("CURL_IMPERSONATE_LIB points to a missing file: {0}")]
    MissingEnvPath(PathBuf),
    #[error("failed to locate libcurl-impersonate. searched: {0:?}")]
    LibraryNotFound(Vec<PathBuf>),
    #[error(
        "failed to locate libcurl-impersonate after auto-fetch attempt. searched: {searched:?}; auto-fetch error: {auto_fetch_error}"
    )]
    LibraryNotFoundAfterAutoFetch {
        searched: Vec<PathBuf>,
        auto_fetch_error: String,
    },
    #[error("auto-fetch is not supported on target: {0}")]
    AutoFetchUnsupportedTarget(String),
    #[error("auto-fetch needs cache directory but HOME and IMPCURL_LIB_DIR are not set")]
    AutoFetchCacheDirUnavailable,
    #[error("failed to run downloader command {command}: {source}")]
    AutoFetchCommandSpawn { command: String, source: io::Error },
    #[error("downloader command {command} failed with status {status:?}: {stderr}")]
    AutoFetchCommandFailed {
        command: String,
        status: Option<i32>,
        stderr: String,
    },
    #[error("failed to parse GitHub release JSON: {0}")]
    AutoFetchJson(#[from] serde_json::Error),
    #[error("I/O error during auto-fetch: {0}")]
    AutoFetchIo(#[from] io::Error),
    #[error("failed to extract wheel archive: {0}")]
    AutoFetchWheel(#[from] zip::result::ZipError),
    #[error("no matching curl_cffi wheel asset for version={version}, platform_tag={platform_tag}")]
    AutoFetchWheelAssetNotFound {
        version: String,
        platform_tag: String,
    },
    #[error(
        "wheel extracted shared objects into {cache_dir}, but no standalone libcurl-impersonate runtime was found"
    )]
    AutoFetchNoStandaloneRuntime { cache_dir: PathBuf },
}

pub struct CurlApi {
    // Keep the dynamic library loaded for process lifetime. Unloading can crash
    // with libcurl-impersonate on process teardown in some environments.
    _lib: ManuallyDrop<Library>,
    pub global_init: unsafe extern "C" fn(c_ulong) -> CurlCode,
    pub global_cleanup: unsafe extern "C" fn(),
    pub easy_init: unsafe extern "C" fn() -> *mut Curl,
    pub easy_cleanup: unsafe extern "C" fn(*mut Curl),
    pub easy_perform: unsafe extern "C" fn(*mut Curl) -> CurlCode,
    pub easy_setopt: unsafe extern "C" fn(*mut Curl, CurlOption, ...) -> CurlCode,
    pub easy_strerror: unsafe extern "C" fn(CurlCode) -> *const c_char,
    pub easy_impersonate: unsafe extern "C" fn(*mut Curl, *const c_char, c_int) -> CurlCode,
    pub slist_append: unsafe extern "C" fn(*mut CurlSlist, *const c_char) -> *mut CurlSlist,
    pub slist_free_all: unsafe extern "C" fn(*mut CurlSlist),
    pub ws_send:
        unsafe extern "C" fn(*mut Curl, *const c_void, usize, *mut usize, i64, c_uint) -> CurlCode,
    pub ws_recv: unsafe extern "C" fn(
        *mut Curl,
        *mut c_void,
        usize,
        *mut usize,
        *mut *const CurlWsFrame,
    ) -> CurlCode,
    pub multi_init: unsafe extern "C" fn() -> *mut CurlMulti,
    pub multi_cleanup: unsafe extern "C" fn(*mut CurlMulti) -> CurlMCode,
    pub multi_setopt: unsafe extern "C" fn(*mut CurlMulti, CurlMOption, ...) -> CurlMCode,
    pub multi_add_handle: unsafe extern "C" fn(*mut CurlMulti, *mut Curl) -> CurlMCode,
    pub multi_remove_handle: unsafe extern "C" fn(*mut CurlMulti, *mut Curl) -> CurlMCode,
    pub multi_fdset: unsafe extern "C" fn(
        *mut CurlMulti,
        *mut c_void,
        *mut c_void,
        *mut c_void,
        *mut c_int,
    ) -> CurlMCode,
    pub multi_timeout: unsafe extern "C" fn(*mut CurlMulti, *mut c_long) -> CurlMCode,
    pub multi_perform: unsafe extern "C" fn(*mut CurlMulti, *mut c_int) -> CurlMCode,
    pub multi_poll: unsafe extern "C" fn(
        *mut CurlMulti,
        *mut CurlWaitFd,
        c_uint,
        c_int,
        *mut c_int,
    ) -> CurlMCode,
    pub multi_socket_action:
        unsafe extern "C" fn(*mut CurlMulti, CurlSocket, c_int, *mut c_int) -> CurlMCode,
    pub multi_info_read: unsafe extern "C" fn(*mut CurlMulti, *mut c_int) -> *mut CurlMessage,
    pub multi_strerror: unsafe extern "C" fn(CurlMCode) -> *const c_char,
}

impl CurlApi {
    /// # Safety
    /// Caller must ensure the loaded library is ABI-compatible with the symbols used.
    pub unsafe fn load(path: &Path) -> Result<Self, SysError> {
        debug!(path = %path.display(), "loading curl-impersonate library");
        let lib = unsafe { Library::new(path) }.map_err(|source| SysError::LoadLibrary {
            path: path.to_path_buf(),
            source,
        })?;

        let global_init = unsafe {
            load_symbol::<unsafe extern "C" fn(c_ulong) -> CurlCode>(&lib, b"curl_global_init\0")?
        };
        let global_cleanup =
            unsafe { load_symbol::<unsafe extern "C" fn()>(&lib, b"curl_global_cleanup\0")? };
        let easy_init = unsafe {
            load_symbol::<unsafe extern "C" fn() -> *mut Curl>(&lib, b"curl_easy_init\0")?
        };
        let easy_cleanup = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut Curl)>(&lib, b"curl_easy_cleanup\0")?
        };
        let easy_perform = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut Curl) -> CurlCode>(
                &lib,
                b"curl_easy_perform\0",
            )?
        };
        let easy_setopt = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut Curl, CurlOption, ...) -> CurlCode>(
                &lib,
                b"curl_easy_setopt\0",
            )?
        };
        let easy_strerror = unsafe {
            load_symbol::<unsafe extern "C" fn(CurlCode) -> *const c_char>(
                &lib,
                b"curl_easy_strerror\0",
            )?
        };
        let easy_impersonate = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut Curl, *const c_char, c_int) -> CurlCode>(
                &lib,
                b"curl_easy_impersonate\0",
            )?
        };
        let slist_append = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlSlist, *const c_char) -> *mut CurlSlist>(
                &lib,
                b"curl_slist_append\0",
            )?
        };
        let slist_free_all = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlSlist)>(&lib, b"curl_slist_free_all\0")?
        };
        let ws_send = unsafe {
            load_symbol::<
                unsafe extern "C" fn(
                    *mut Curl,
                    *const c_void,
                    usize,
                    *mut usize,
                    i64,
                    c_uint,
                ) -> CurlCode,
            >(&lib, b"curl_ws_send\0")?
        };
        let ws_recv = unsafe {
            load_symbol::<
                unsafe extern "C" fn(
                    *mut Curl,
                    *mut c_void,
                    usize,
                    *mut usize,
                    *mut *const CurlWsFrame,
                ) -> CurlCode,
            >(&lib, b"curl_ws_recv\0")?
        };
        let multi_init = unsafe {
            load_symbol::<unsafe extern "C" fn() -> *mut CurlMulti>(&lib, b"curl_multi_init\0")?
        };
        let multi_cleanup = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlMulti) -> CurlMCode>(
                &lib,
                b"curl_multi_cleanup\0",
            )?
        };
        let multi_setopt = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlMulti, CurlMOption, ...) -> CurlMCode>(
                &lib,
                b"curl_multi_setopt\0",
            )?
        };
        let multi_add_handle = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlMulti, *mut Curl) -> CurlMCode>(
                &lib,
                b"curl_multi_add_handle\0",
            )?
        };
        let multi_remove_handle = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlMulti, *mut Curl) -> CurlMCode>(
                &lib,
                b"curl_multi_remove_handle\0",
            )?
        };
        let multi_fdset = unsafe {
            load_symbol::<
                unsafe extern "C" fn(
                    *mut CurlMulti,
                    *mut c_void,
                    *mut c_void,
                    *mut c_void,
                    *mut c_int,
                ) -> CurlMCode,
            >(&lib, b"curl_multi_fdset\0")?
        };
        let multi_timeout = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlMulti, *mut c_long) -> CurlMCode>(
                &lib,
                b"curl_multi_timeout\0",
            )?
        };
        let multi_perform = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlMulti, *mut c_int) -> CurlMCode>(
                &lib,
                b"curl_multi_perform\0",
            )?
        };
        let multi_poll = unsafe {
            load_symbol::<
                unsafe extern "C" fn(
                    *mut CurlMulti,
                    *mut CurlWaitFd,
                    c_uint,
                    c_int,
                    *mut c_int,
                ) -> CurlMCode,
            >(&lib, b"curl_multi_poll\0")?
        };
        let multi_socket_action = unsafe {
            load_symbol::<
                unsafe extern "C" fn(*mut CurlMulti, CurlSocket, c_int, *mut c_int) -> CurlMCode,
            >(&lib, b"curl_multi_socket_action\0")?
        };
        let multi_info_read = unsafe {
            load_symbol::<unsafe extern "C" fn(*mut CurlMulti, *mut c_int) -> *mut CurlMessage>(
                &lib,
                b"curl_multi_info_read\0",
            )?
        };
        let multi_strerror = unsafe {
            load_symbol::<unsafe extern "C" fn(CurlMCode) -> *const c_char>(
                &lib,
                b"curl_multi_strerror\0",
            )?
        };

        info!(path = %path.display(), "curl-impersonate library loaded");
        Ok(Self {
            _lib: ManuallyDrop::new(lib),
            global_init,
            global_cleanup,
            easy_init,
            easy_cleanup,
            easy_perform,
            easy_setopt,
            easy_strerror,
            easy_impersonate,
            slist_append,
            slist_free_all,
            ws_send,
            ws_recv,
            multi_init,
            multi_cleanup,
            multi_setopt,
            multi_add_handle,
            multi_remove_handle,
            multi_fdset,
            multi_timeout,
            multi_perform,
            multi_poll,
            multi_socket_action,
            multi_info_read,
            multi_strerror,
        })
    }

    pub fn error_text(&self, code: CurlCode) -> String {
        unsafe {
            let ptr = (self.easy_strerror)(code);
            if ptr.is_null() {
                return format!("CURLcode {}", code);
            }
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }

    pub fn multi_error_text(&self, code: CurlMCode) -> String {
        unsafe {
            let ptr = (self.multi_strerror)(code);
            if ptr.is_null() {
                return format!("CURLMcode {}", code);
            }
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

// CurlApi only contains function pointers and a ManuallyDrop<Library> (never unloaded).
// All function pointers are process-global symbols — safe to share across threads.
unsafe impl Send for CurlApi {}
unsafe impl Sync for CurlApi {}

static SHARED_API: OnceLock<Arc<CurlApi>> = OnceLock::new();

/// Get or initialize the process-wide shared CurlApi instance.
/// First call loads the library; subsequent calls return the cached Arc.
pub fn shared_curl_api(lib_path: &Path) -> Result<Arc<CurlApi>, SysError> {
    if let Some(api) = SHARED_API.get() {
        return Ok(Arc::clone(api));
    }
    let api = unsafe { CurlApi::load(lib_path) }?;
    let arc = Arc::new(api);
    // Race is fine — loser's CurlApi uses ManuallyDrop so no resource leak.
    let _ = SHARED_API.set(Arc::clone(&arc));
    Ok(SHARED_API.get().map(Arc::clone).unwrap_or(arc))
}

impl CurlMessage {
    /// # Safety
    /// Caller must only use this for messages where `msg == CURLMSG_DONE`.
    pub unsafe fn done_result(&self) -> CurlCode {
        unsafe { self.data.result }
    }
}

unsafe fn load_symbol<T: Copy>(lib: &Library, name: &[u8]) -> Result<T, SysError> {
    let symbol = unsafe { lib.get::<T>(name) }.map_err(|source| SysError::MissingSymbol {
        name: String::from_utf8_lossy(name)
            .trim_end_matches('\0')
            .to_owned(),
        source,
    })?;
    Ok(*symbol)
}

pub fn platform_library_names() -> &'static [&'static str] {
    if cfg!(target_os = "macos") {
        &["libcurl-impersonate.4.dylib", "libcurl-impersonate.dylib"]
    } else if cfg!(target_os = "linux") {
        &["libcurl-impersonate.so.4", "libcurl-impersonate.so"]
    } else if cfg!(target_os = "windows") {
        &[
            "curl-impersonate.dll",
            "libcurl-impersonate.dll",
            "libcurl.dll",
        ]
    } else {
        &[
            "libcurl-impersonate.4.dylib",
            "libcurl-impersonate.so.4",
            "curl-impersonate.dll",
        ]
    }
}

pub fn find_near_executable() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let exe_dir = exe.parent()?;
    for name in platform_library_names() {
        let in_lib = exe_dir.join("..").join("lib").join(name);
        if in_lib.exists() {
            return Some(in_lib);
        }
        let side_by_side = exe_dir.join(name);
        if side_by_side.exists() {
            return Some(side_by_side);
        }
    }
    None
}

fn probe_library_dir(dir: &Path, searched: &mut Vec<PathBuf>) -> Option<PathBuf> {
    for name in platform_library_names() {
        let candidate = dir.join(name);
        searched.push(candidate.clone());
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn default_runtime_search_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();

    if let Ok(dir) = std::env::var("IMPCURL_LIB_DIR") {
        roots.push(PathBuf::from(dir));
    }

    if let Ok(home) = std::env::var("HOME") {
        roots.push(PathBuf::from(&home).join(".impcurl/lib"));
        roots.push(PathBuf::from(home).join(".cuimp/binaries"));
    }

    roots
}

fn auto_fetch_enabled() -> bool {
    match std::env::var("IMPCURL_AUTO_FETCH") {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            !matches!(normalized.as_str(), "0" | "false" | "no" | "off")
        }
        Err(_) => true,
    }
}

fn auto_fetch_cache_dir() -> Result<PathBuf, SysError> {
    if let Ok(dir) = std::env::var("IMPCURL_AUTO_FETCH_CACHE_DIR") {
        return Ok(PathBuf::from(dir));
    }
    if let Ok(dir) = std::env::var("IMPCURL_LIB_DIR") {
        return Ok(PathBuf::from(dir));
    }
    if let Ok(home) = std::env::var("HOME") {
        return Ok(PathBuf::from(home).join(".impcurl/lib"));
    }
    Err(SysError::AutoFetchCacheDirUnavailable)
}

fn current_target_triple() -> &'static str {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        "aarch64-apple-darwin"
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        "x86_64-apple-darwin"
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu"
    )) {
        "x86_64-unknown-linux-gnu"
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "x86",
        target_env = "gnu"
    )) {
        "i686-unknown-linux-gnu"
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "aarch64",
        target_env = "gnu"
    )) {
        "aarch64-unknown-linux-gnu"
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "musl"
    )) {
        "x86_64-unknown-linux-musl"
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "aarch64",
        target_env = "musl"
    )) {
        "aarch64-unknown-linux-musl"
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        "x86_64-pc-windows-msvc"
    } else if cfg!(all(target_os = "windows", target_arch = "x86")) {
        "i686-pc-windows-msvc"
    } else if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
        "aarch64-pc-windows-msvc"
    } else {
        "unknown"
    }
}

fn wheel_platform_tag_for_target(target: &str) -> Option<&'static str> {
    match target {
        "x86_64-apple-darwin" => Some("macosx_10_9_x86_64"),
        "aarch64-apple-darwin" => Some("macosx_11_0_arm64"),
        "x86_64-unknown-linux-gnu" => Some("manylinux_2_17_x86_64.manylinux2014_x86_64"),
        "aarch64-unknown-linux-gnu" => Some("manylinux_2_17_aarch64.manylinux2014_aarch64"),
        "i686-unknown-linux-gnu" => Some("manylinux_2_17_i686.manylinux2014_i686"),
        "x86_64-unknown-linux-musl" => Some("musllinux_1_1_x86_64"),
        "aarch64-unknown-linux-musl" => Some("musllinux_1_1_aarch64"),
        "x86_64-pc-windows-msvc" | "x86_64-pc-windows-gnu" => Some("win_amd64"),
        "i686-pc-windows-msvc" | "i686-pc-windows-gnu" => Some("win32"),
        "aarch64-pc-windows-msvc" => Some("win_arm64"),
        _ => None,
    }
}

fn is_shared_library_name(file_name: &str) -> bool {
    if cfg!(target_os = "macos") {
        file_name.ends_with(".dylib")
    } else if cfg!(target_os = "linux") {
        file_name.contains(".so")
    } else if cfg!(target_os = "windows") {
        file_name.to_ascii_lowercase().ends_with(".dll")
    } else {
        file_name.ends_with(".dylib")
            || file_name.contains(".so")
            || file_name.to_ascii_lowercase().ends_with(".dll")
    }
}

fn run_download_command(command: &mut Command, command_label: &str) -> Result<Vec<u8>, SysError> {
    let output = command
        .output()
        .map_err(|source| SysError::AutoFetchCommandSpawn {
            command: command_label.to_owned(),
            source,
        })?;

    if output.status.success() {
        return Ok(output.stdout);
    }

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    Err(SysError::AutoFetchCommandFailed {
        command: command_label.to_owned(),
        status: output.status.code(),
        stderr,
    })
}

fn fetch_url_to_string(url: &str) -> Result<String, SysError> {
    let mut curl_cmd = Command::new("curl");
    curl_cmd
        .arg("-fsSL")
        .arg("-H")
        .arg("User-Agent: impcurl-sys")
        .arg(url);
    match run_download_command(&mut curl_cmd, "curl") {
        Ok(stdout) => {
            let body = String::from_utf8_lossy(&stdout).to_string();
            return Ok(body);
        }
        Err(SysError::AutoFetchCommandSpawn { .. }) => {}
        Err(err) => return Err(err),
    }

    let mut wget_cmd = Command::new("wget");
    wget_cmd.arg("-qO-").arg(url);
    let stdout = run_download_command(&mut wget_cmd, "wget")?;
    Ok(String::from_utf8_lossy(&stdout).to_string())
}

fn fetch_url_to_file(url: &str, output_path: &Path) -> Result<(), SysError> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let output_str = output_path.to_string_lossy().to_string();

    let mut curl_cmd = Command::new("curl");
    curl_cmd
        .arg("-fL")
        .arg("-o")
        .arg(&output_str)
        .arg("-H")
        .arg("User-Agent: impcurl-sys")
        .arg(url);
    match run_download_command(&mut curl_cmd, "curl") {
        Ok(_) => return Ok(()),
        Err(SysError::AutoFetchCommandSpawn { .. }) => {}
        Err(err) => return Err(err),
    }

    let mut wget_cmd = Command::new("wget");
    wget_cmd.arg("-O").arg(&output_str).arg(url);
    run_download_command(&mut wget_cmd, "wget")?;
    Ok(())
}

fn select_curl_cffi_wheel_download_url(
    release_json: &str,
    version: &str,
    platform_tag: &str,
) -> Option<String> {
    let parsed: Value = serde_json::from_str(release_json).ok()?;
    let assets = parsed.get("assets")?.as_array()?;
    for asset in assets {
        let name = asset.get("name")?.as_str()?;
        if !name.starts_with(&format!("curl_cffi-{version}-")) {
            continue;
        }
        if !name.contains("-abi3-") {
            continue;
        }
        if !name.ends_with(&format!("-{platform_tag}.whl")) {
            continue;
        }
        let url = asset.get("browser_download_url")?.as_str()?;
        return Some(url.to_owned());
    }
    None
}

fn extract_shared_libraries_from_wheel(
    wheel_path: &Path,
    output_dir: &Path,
) -> Result<Vec<PathBuf>, SysError> {
    let wheel_file = File::open(wheel_path)?;
    let mut archive = ZipArchive::new(wheel_file)?;
    let mut copied = Vec::new();

    for index in 0..archive.len() {
        let mut file = archive.by_index(index)?;
        if !file.is_file() {
            continue;
        }
        let file_name = match Path::new(file.name()).file_name().and_then(|s| s.to_str()) {
            Some(name) => name,
            None => continue,
        };
        if !is_shared_library_name(file_name) {
            continue;
        }
        let dst = output_dir.join(file_name);
        let mut out = File::create(&dst)?;
        io::copy(&mut file, &mut out)?;
        copied.push(dst);
    }

    Ok(copied)
}

fn auto_fetch_from_curl_cffi(cache_dir: &Path) -> Result<(), SysError> {
    info!(cache_dir = %cache_dir.display(), "auto-fetching curl-impersonate from curl_cffi wheel");
    fs::create_dir_all(cache_dir)?;

    let cffi_version =
        std::env::var("IMPCURL_CURL_CFFI_VERSION").unwrap_or_else(|_| "0.11.3".to_owned());
    let target = current_target_triple();
    let platform_tag = wheel_platform_tag_for_target(target)
        .ok_or_else(|| SysError::AutoFetchUnsupportedTarget(target.to_owned()))?;

    let release_api_url =
        format!("https://api.github.com/repos/lexiforest/curl_cffi/releases/tags/v{cffi_version}");
    let release_json = fetch_url_to_string(&release_api_url)?;
    let wheel_url = select_curl_cffi_wheel_download_url(&release_json, &cffi_version, platform_tag)
        .ok_or_else(|| SysError::AutoFetchWheelAssetNotFound {
            version: cffi_version.clone(),
            platform_tag: platform_tag.to_owned(),
        })?;

    let wheel_path = cache_dir.join(format!(
        ".curl_cffi-{cffi_version}-{platform_tag}-{}.whl",
        std::process::id()
    ));

    fetch_url_to_file(&wheel_url, &wheel_path)?;
    let _ = extract_shared_libraries_from_wheel(&wheel_path, cache_dir)?;
    let _ = fs::remove_file(&wheel_path);

    let mut searched = Vec::new();
    if probe_library_dir(cache_dir, &mut searched).is_none() {
        return Err(SysError::AutoFetchNoStandaloneRuntime {
            cache_dir: cache_dir.to_path_buf(),
        });
    }

    Ok(())
}

pub fn resolve_impersonate_lib_path(extra_search_roots: &[PathBuf]) -> Result<PathBuf, SysError> {
    if let Ok(path) = std::env::var("CURL_IMPERSONATE_LIB") {
        let resolved = PathBuf::from(path);
        if resolved.exists() {
            debug!(path = %resolved.display(), "found via CURL_IMPERSONATE_LIB");
            return Ok(resolved);
        }
        return Err(SysError::MissingEnvPath(resolved));
    }

    if let Some(packaged) = find_near_executable() {
        debug!(path = %packaged.display(), "found near executable");
        return Ok(packaged);
    }

    let mut searched = Vec::new();
    for root in extra_search_roots {
        if let Some(found) = probe_library_dir(root, &mut searched) {
            return Ok(found);
        }
    }

    for root in default_runtime_search_roots() {
        if let Some(found) = probe_library_dir(&root, &mut searched) {
            return Ok(found);
        }
    }

    if auto_fetch_enabled() {
        let auto_fetch_result = (|| -> Result<PathBuf, SysError> {
            let cache_dir = auto_fetch_cache_dir()?;
            if let Some(found) = probe_library_dir(&cache_dir, &mut searched) {
                return Ok(found);
            }
            auto_fetch_from_curl_cffi(&cache_dir)?;
            probe_library_dir(&cache_dir, &mut searched).ok_or_else(|| {
                SysError::AutoFetchNoStandaloneRuntime {
                    cache_dir: cache_dir.to_path_buf(),
                }
            })
        })();

        return match auto_fetch_result {
            Ok(found) => Ok(found),
            Err(err) => Err(SysError::LibraryNotFoundAfterAutoFetch {
                searched,
                auto_fetch_error: err.to_string(),
            }),
        };
    }

    Err(SysError::LibraryNotFound(searched))
}

#[cfg(test)]
mod tests {
    use super::select_curl_cffi_wheel_download_url;

    #[test]
    fn picks_matching_wheel_asset_url() {
        let release_json = r#"
        {
          "assets": [
            {
              "name": "curl_cffi-0.11.3-cp39-abi3-macosx_10_9_x86_64.whl",
              "browser_download_url": "https://example.test/x86_64.whl"
            },
            {
              "name": "curl_cffi-0.11.3-cp39-abi3-macosx_11_0_arm64.whl",
              "browser_download_url": "https://example.test/arm64.whl"
            }
          ]
        }
        "#;

        let url = select_curl_cffi_wheel_download_url(release_json, "0.11.3", "macosx_11_0_arm64")
            .expect("expected matching wheel URL");

        assert_eq!(url, "https://example.test/arm64.whl");
    }
}
