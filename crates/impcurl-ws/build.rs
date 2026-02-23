use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=CURL_IMPERSONATE_LIB");
    println!("cargo:rerun-if-env-changed=CURL_IMPERSONATE_LIB_DIR");

    let target = env::var("TARGET").unwrap_or_default();
    println!("cargo:rustc-env=IMPCURL_WS_TARGET={target}");

    if target.contains("apple-darwin") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path/../lib");
    } else if target.contains("linux") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/../lib");
    }

    if let Ok(dir) = env::var("CURL_IMPERSONATE_LIB_DIR") {
        println!("cargo:rustc-link-search=native={dir}");
    } else {
        let candidate = PathBuf::from("vendor").join(&target).join("lib");
        if candidate.is_dir() {
            println!("cargo:rustc-link-search=native={}", candidate.display());
        }
    }
}
