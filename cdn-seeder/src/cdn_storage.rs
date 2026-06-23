use async_trait::async_trait;
use bytes::Bytes;
use fx_torrent::{PieceIndex, Sha1Hash, Sha256Hash};
use fx_torrent::storage::{Extension, Metrics as StorageMetrics};
use reqwest::Client;
use sha1::{Sha1, Digest};

use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::{info, trace, warn};

const MAX_CONCURRENT_REQUESTS: usize = 8;

pub struct CdnStorage {
    client: Client,
    base_url: String,
    piece_length: usize,
    total_length: usize,
    num_pieces: usize,
    metrics: StorageMetrics,
    semaphore: Arc<Semaphore>,
}

impl std::fmt::Debug for CdnStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdnStorage")
            .field("base_url", &self.base_url)
            .field("piece_length", &self.piece_length)
            .field("total_length", &self.total_length)
            .field("num_pieces", &self.num_pieces)
            .finish()
    }
}

impl CdnStorage {
    pub fn new(base_url: impl Into<String>, piece_length: usize, total_length: usize) -> Self {
        let base_url_string = base_url.into();
        let num_pieces = (total_length + piece_length - 1) / piece_length;
        info!(
            "CdnStorage::new(base_url={}, piece_length={piece_length}, total_length={total_length}, num_pieces={num_pieces}, max_concurrent={MAX_CONCURRENT_REQUESTS})",
            base_url_string
        );
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .pool_max_idle_per_host(MAX_CONCURRENT_REQUESTS)
                .build()
                .expect("Failed to create HTTP client"),
            base_url: base_url_string,
            piece_length,
            total_length,
            num_pieces,
            metrics: StorageMetrics::default(),
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
        }
    }

    fn piece_url(&self, piece: &PieceIndex) -> String {
        format!("{}/piece_{piece}.bin", self.base_url)
    }

    fn piece_size(&self, piece: &PieceIndex) -> usize {
        if *piece < self.num_pieces - 1 {
            self.piece_length
        } else {
            self.total_length - piece * self.piece_length
        }
    }

    async fn fetch_range_inner(&self, piece: &PieceIndex, offset: usize, len: usize) -> Result<Bytes, Error> {
        let url = self.piece_url(piece);
        let range = format!("bytes={}-{}", offset, offset + len - 1);
        let start = Instant::now();

        let _permit = self.semaphore.acquire().await.map_err(|e| {
            Error::new(ErrorKind::Other, format!("semaphore closed: {e}"))
        })?;

        trace!("CDN REQUEST: piece={piece} range={range} url={url}");

        let response = self
            .client
            .get(&url)
            .header("Range", &range)
            .send()
            .await
            .map_err(|e| {
                warn!("CDN ERROR: piece={piece} range={range} error={e}");
                Error::new(ErrorKind::Other, format!("HTTP error for piece {piece}: {e}"))
            })?;

        let status = response.status();
        let elapsed = start.elapsed();

        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            let msg = format!("HTTP {status} fetching piece {piece} range {range}");
            warn!("CDN FAIL: {msg} (took {elapsed:?})");
            return Err(Error::new(ErrorKind::Other, msg));
        }

        let content_length = response.content_length().unwrap_or(0);

        let bytes = response
            .bytes()
            .await
            .map_err(|e| {
                warn!("CDN BODY ERROR: piece={piece} error={e} (took {elapsed:?})");
                Error::new(ErrorKind::Other, format!("response body error for piece {piece}: {e}"))
            })?;

        let actual_len = bytes.len();
        trace!(
            "CDN RESPONSE: piece={piece} range={range} status={status} content_length={content_length} actual_bytes={actual_len} took={elapsed:?}"
        );

        Ok(bytes)
    }

    async fn fetch_range(&self, piece: &PieceIndex, offset: usize, len: usize) -> Result<Bytes, Error> {
        const MAX_RETRIES: u32 = 3;
        let mut last_err = None;

        for attempt in 1..=MAX_RETRIES {
            match self.fetch_range_inner(piece, offset, len).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    if attempt < MAX_RETRIES {
                        warn!("CDN RETRY: piece={piece} attempt {attempt}/{MAX_RETRIES} failed: {e}, retrying in {attempt}s...");
                        tokio::time::sleep(std::time::Duration::from_secs(attempt as u64)).await;
                    }
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap())
    }
}

#[async_trait]
impl Extension for CdnStorage {
    async fn read(
        &self,
        buffer: &mut [u8],
        piece: &PieceIndex,
        offset: usize,
    ) -> Result<usize, Error> {
        let piece_size = self.piece_size(piece);

        if buffer.is_empty() {
            return Ok(0);
        }

        if offset >= piece_size {
            warn!("READ: piece={piece} offset {offset} >= piece_size {piece_size}");
            return Ok(0);
        }

        let available = piece_size - offset;
        let to_fetch = buffer.len().min(available);

        let start = Instant::now();
        let bytes = self.fetch_range(piece, offset, to_fetch).await?;
        let to_copy = bytes.len().min(buffer.len());
        buffer[..to_copy].copy_from_slice(&bytes[..to_copy]);
        self.metrics.bytes_read.inc_by(to_copy as u64);

        trace!(
            "READ OK: piece={piece} offset={offset} fetched={to_copy} bytes total_read={} took={:?}",
            self.metrics.bytes_read.total(),
            start.elapsed()
        );

        Ok(to_copy)
    }

    async fn write(
        &self,
        _data: &[u8],
        _piece: &PieceIndex,
        _offset: usize,
    ) -> Result<usize, Error> {
        Ok(0)
    }

    async fn hash_v1(&self, piece: &PieceIndex) -> Result<Sha1Hash, Error> {
        let size = self.piece_size(piece);
        let bytes = self.fetch_range(piece, 0, size).await?;
        let hash = Sha1::digest(&bytes);
        Ok(hash.into())
    }

    async fn hash_v2(&self, _piece: &PieceIndex) -> Result<Sha256Hash, Error> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "v2 hashing not supported",
        ))
    }

    async fn move_storage(&self, _new_path: &Path) -> Result<(), Error> {
        Ok(())
    }

    fn metrics(&self) -> &StorageMetrics {
        &self.metrics
    }
}
