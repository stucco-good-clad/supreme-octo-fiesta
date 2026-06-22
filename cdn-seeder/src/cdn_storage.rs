use async_trait::async_trait;
use bytes::Bytes;
use fx_torrent::{PieceIndex, Sha1Hash, Sha256Hash};
use fx_torrent::storage::{Extension, Metrics as StorageMetrics};
use reqwest::Client;
use sha1::Digest;
use std::io::{Error, ErrorKind};
use std::path::Path;
use tracing::{debug, warn};

#[derive(Debug)]
pub struct CdnStorage {
    client: Client,
    base_url: String,
    piece_length: usize,
    total_length: usize,
    num_pieces: usize,
    metrics: StorageMetrics,
}

impl CdnStorage {
    pub fn new(base_url: impl Into<String>, piece_length: usize, total_length: usize) -> Self {
        let num_pieces = (total_length + piece_length - 1) / piece_length;
        Self {
            client: Client::new(),
            base_url: base_url.into(),
            piece_length,
            total_length,
            num_pieces,
            metrics: StorageMetrics::default(),
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

    async fn fetch_range(&self, piece: &PieceIndex, offset: usize, len: usize) -> Result<Bytes, Error> {
        let url = self.piece_url(piece);
        let range = format!("bytes={}-{}", offset, offset + len - 1);
        debug!("fetching piece {piece} range {range} from {url}");

        let response = self
            .client
            .get(&url)
            .header("Range", &range)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("HTTP error for piece {piece}: {e}")))?;

        let status = response.status();
        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            let msg = format!("HTTP {status} fetching piece {piece} range {range} from {url}");
            warn!("{msg}");
            return Err(Error::new(ErrorKind::Other, msg));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("response body error for piece {piece}: {e}")))?;

        debug!("fetched piece {piece} range {range}: {} bytes", bytes.len());
        Ok(bytes)
    }

    async fn fetch_piece_full(&self, piece: &PieceIndex) -> Result<Bytes, Error> {
        let len = self.piece_size(piece);
        self.fetch_range(piece, 0, len).await
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
        if buffer.is_empty() {
            return Ok(0);
        }

        let piece_size = self.piece_size(piece);
        if offset >= piece_size {
            warn!("read: offset {offset} >= piece size {piece_size} for piece {piece}");
            return Ok(0);
        }

        let available = piece_size - offset;
        let to_fetch = buffer.len().min(available);
        let bytes = self.fetch_range(piece, offset, to_fetch).await?;

        let to_copy = bytes.len().min(buffer.len());
        buffer[..to_copy].copy_from_slice(&bytes[..to_copy]);
        self.metrics.bytes_read.inc_by(to_copy as u64);

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
        let bytes = self.fetch_piece_full(piece).await?;
        let mut hasher = sha1::Sha1::new();
        hasher.update(&bytes);
        let result = hasher.finalize();
        let mut hash = [0u8; 20];
        hash.copy_from_slice(&result);
        debug!("hash_v1 piece {piece}: {:02x?}", hash);
        Ok(hash)
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
