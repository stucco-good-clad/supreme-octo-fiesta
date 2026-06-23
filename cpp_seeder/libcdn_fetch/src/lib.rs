use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::OnceLock;
use std::time::Duration;
use reqwest::Client;
use tokio::runtime::Runtime;

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("tokio runtime"))
}

fn client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .user_agent("cdn_seed/1.0")
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .pool_max_idle_per_host(8)
            .pool_idle_timeout(Duration::from_secs(30))
            .http2_keep_alive_interval(Some(Duration::from_secs(10)))
            .http2_keep_alive_timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client")
    })
}

pub async fn do_fetch(url: &str, offset: i32, len: i32) -> Result<Vec<u8>, String> {
    let range = format!("bytes={}-{}", offset, offset + len - 1);

    let response = client()
        .get(url)
        .header("Range", &range)
        .send()
        .await
        .map_err(|e| format!("request: {e}"))?;

    let status = response.status();
    if status != reqwest::StatusCode::OK
        && status != reqwest::StatusCode::PARTIAL_CONTENT
    {
        return Err(format!("HTTP {status}"));
    }

    let bytes = response.bytes()
        .await
        .map_err(|e| format!("body: {e}"))?;

    Ok(bytes.to_vec())
}

#[unsafe(no_mangle)]
pub extern "C" fn cdn_fetch(
    url: *const c_char,
    offset: i32,
    len: i32,
    output: *mut u8,
) -> i32 {
    if url.is_null() || output.is_null() || len <= 0 {
        return -1;
    }

    let url_str = match unsafe { CStr::from_ptr(url) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };

    let result = runtime()
        .block_on(do_fetch(url_str, offset, len));

    match result {
        Ok(body) => {
            let copy_len = std::cmp::min(body.len() as i32, len);
            unsafe {
                std::ptr::copy_nonoverlapping(body.as_ptr(), output, copy_len as usize);
            }
            copy_len
        }
        Err(e) => {
            eprintln!("[cdn_fetch] {e}");
            -1
        }
    }
}
