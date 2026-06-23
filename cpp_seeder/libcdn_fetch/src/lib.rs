use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};
use reqwest::Client;
use tokio::runtime::Runtime;

// === Cache configuration (set once via cdn_cache_init) ===

struct CacheConfig {
    piece_size: usize,
    soft_limit: usize,
    hard_limit: usize,
}

static CONFIG: OnceLock<CacheConfig> = OnceLock::new();

#[unsafe(no_mangle)]
pub extern "C" fn cdn_cache_init(piece_size: i32, soft_limit_mb: i32, hard_limit_mb: i32) {
    let _ = CONFIG.set(CacheConfig {
        piece_size: piece_size as usize,
        soft_limit: (soft_limit_mb as usize) * 1024 * 1024,
        hard_limit: (hard_limit_mb as usize) * 1024 * 1024,
    });
}

// === Async runtime & HTTP client ===

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

// === Low-level CDN fetch (async, Range: bytes) ===

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

    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("body: {e}"))?;

    Ok(bytes.to_vec())
}

// === Piece cache ===

struct CacheEntry {
    data: Arc<Vec<u8>>,
    last_access: Instant,
}

struct PendingFetch {
    result: Mutex<Option<Result<Arc<Vec<u8>>, String>>>,
    cvar: Condvar,
}

struct CacheInner {
    entries: HashMap<i32, CacheEntry>,
    pending: HashMap<i32, Arc<PendingFetch>>,
    total_bytes: usize,
}

impl CacheInner {
    fn evict_one(&mut self) {
        if let Some(&oldest_piece) = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.last_access)
            .map(|(k, _)| k)
        {
            if let Some(entry) = self.entries.remove(&oldest_piece) {
                self.total_bytes -= entry.data.len();
            }
        }
    }

    fn evict_to(&mut self, target: usize) {
        while self.total_bytes > target && !self.entries.is_empty() {
            self.evict_one();
        }
    }
}

struct PieceCache {
    inner: Mutex<CacheInner>,
}

static CACHE: OnceLock<PieceCache> = OnceLock::new();

fn cache() -> &'static PieceCache {
    CACHE.get_or_init(|| PieceCache {
        inner: Mutex::new(CacheInner {
            entries: HashMap::new(),
            pending: HashMap::new(),
            total_bytes: 0,
        }),
    })
}

fn parse_piece_index(url: &str) -> Option<i32> {
    let start = url.rfind("piece_")?;
    let num_part = &url[start + 6..];
    let end = num_part.find('.')?;
    num_part[..end].parse().ok()
}

/// Copy up to `len` bytes at `offset` from `data` into `output`.
/// Returns the number of bytes actually copied.
fn copy_range(data: &[u8], offset: i32, len: i32, output: &mut [u8]) -> Option<i32> {
    let available = data.len() as i32;
    if offset >= available {
        return None;
    }
    let copy_len = std::cmp::min(len, available - offset) as usize;
    output[..copy_len].copy_from_slice(&data[offset as usize..][..copy_len]);
    Some(copy_len as i32)
}

/// Try to serve from cache or register as in-flight waiter.
/// If we are the first to request this piece, fetch the full piece from CDN,
/// store in cache, and notify all waiters.
fn cache_fetch(url: &str, offset: i32, len: i32, output: &mut [u8]) -> i32 {
    let config = match CONFIG.get() {
        Some(c) => c,
        None => return do_direct_fetch(url, offset, len, output),
    };

    let piece_index = match parse_piece_index(url) {
        Some(idx) => idx,
        None => return do_direct_fetch(url, offset, len, output),
    };

    // Fast path: piece already cached
    {
        let mut inner = cache().inner.lock().unwrap();
        if let Some(entry) = inner.entries.get_mut(&piece_index) {
            entry.last_access = Instant::now();
            if let Some(n) = copy_range(&entry.data, offset, len, output) {
                return n;
            }
            return -1;
        }
    }

    // Check in-flight (or register as the fetcher)
    let waiter = {
        let mut inner = cache().inner.lock().unwrap();

        // Double-check after acquiring lock (another thread may have cached it)
        if let Some(entry) = inner.entries.get_mut(&piece_index) {
            entry.last_access = Instant::now();
            if let Some(n) = copy_range(&entry.data, offset, len, output) {
                return n;
            }
            return -1;
        }

        if let Some(pending) = inner.pending.get(&piece_index) {
            // Another thread is already fetching this piece; wait for it
            Some(pending.clone())
        } else {
            // We are the first; register ourselves as the fetcher
            let pf = Arc::new(PendingFetch {
                result: Mutex::new(None),
                cvar: Condvar::new(),
            });
            inner.pending.insert(piece_index, pf.clone());
            drop(inner);
            return fetch_and_cache(piece_index, url, offset, len, output, config, pf);
        }
    };

    // Wait for the fetcher thread to complete
    let pf = match waiter {
        Some(pf) => pf,
        None => unreachable!(),
    };

    let mut guard = pf.result.lock().unwrap();
    while guard.is_none() {
        guard = pf.cvar.wait(guard).unwrap();
    }

    match guard.as_ref().unwrap() {
        Ok(data) => {
            if let Some(n) = copy_range(data, offset, len, output) {
                return n;
            }
            -1
        }
        Err(_) => -1,
    }
}

/// Fetch the full piece from CDN, store in cache, and notify waiters.
/// Called at most once per piece (by the first thread that accessed it).
fn fetch_and_cache(
    piece_index: i32,
    url: &str,
    offset: i32,
    len: i32,
    output: &mut [u8],
    config: &CacheConfig,
    pf: Arc<PendingFetch>,
) -> i32 {
    let piece_size = config.piece_size as i32;
    let fetch_result = runtime().block_on(do_fetch(url, 0, piece_size));

    match fetch_result {
        Ok(data) => {
            let data_len = data.len();
            let data_arc = Arc::new(data);

            // Store in cache (with eviction if needed)
            {
                let mut inner = cache().inner.lock().unwrap();

                // Evict down to soft limit if adding would exceed it
                let needed = inner.total_bytes + data_len;
                if needed > config.soft_limit {
                    let target = config.soft_limit.saturating_sub(data_len);
                    inner.evict_to(target);
                }

                // Only insert if we remain under hard limit
                let new_total = inner.total_bytes + data_len;
                if new_total <= config.hard_limit
                    // Re-check: another fetcher may have cached it while we were fetching
                    && !inner.entries.contains_key(&piece_index)
                {
                    inner.entries.insert(
                        piece_index,
                        CacheEntry {
                            data: data_arc.clone(),
                            last_access: Instant::now(),
                        },
                    );
                    inner.total_bytes = new_total;
                }

                // Remove from pending map
                inner.pending.remove(&piece_index);
            }

            {
                let mut guard = pf.result.lock().unwrap();
                *guard = Some(Ok(data_arc.clone()));
                pf.cvar.notify_all();
            }

            // Serve the requested range
            if let Some(n) = copy_range(&data_arc, offset, len, output) {
                return n;
            }
            -1
        }
        Err(e) => {
            eprintln!("[cdn_fetch] piece={piece_index} error: {e}");

            {
                let mut inner = cache().inner.lock().unwrap();
                inner.pending.remove(&piece_index);
            }

            {
                let mut guard = pf.result.lock().unwrap();
                *guard = Some(Err(e));
                pf.cvar.notify_all();
            }

            -1
        }
    }
}

/// Fallback: fetch only the requested range (no caching).
/// Used when caching is not configured or the URL doesn't contain a piece index.
fn do_direct_fetch(url: &str, offset: i32, len: i32, output: &mut [u8]) -> i32 {
    let result = runtime().block_on(do_fetch(url, offset, len));
    match result {
        Ok(body) => {
            let copy_len = std::cmp::min(body.len() as i32, len) as usize;
            output[..copy_len].copy_from_slice(&body[..copy_len]);
            copy_len as i32
        }
        Err(e) => {
            eprintln!("[cdn_fetch] {e}");
            -1
        }
    }
}

// === Public C API ===

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

    if offset < 0 || len < 0 {
        return -1;
    }

    let out_slice =
        unsafe { std::slice::from_raw_parts_mut(output, len as usize) };

    cache_fetch(url_str, offset, len, out_slice)
}
