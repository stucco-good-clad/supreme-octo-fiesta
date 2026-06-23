use async_trait::async_trait;
use fx_torrent::operation::{Extension, TorrentOperationResult};
use fx_torrent::{PieceIndex, TorrentContext};
use fx_torrent::peer::PeerDiscovery;
use tracing::info;

#[derive(Debug)]
pub struct SeedAllPiecesOperation {
    marked: bool,
}

impl SeedAllPiecesOperation {
    pub fn new() -> Self {
        Self { marked: false }
    }
}

#[async_trait]
impl Extension for SeedAllPiecesOperation {
    fn name(&self) -> &str {
        "SeedAllPieces"
    }

    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        _peer_discoveries: &[PeerDiscovery],
    ) -> TorrentOperationResult {
        if self.marked {
            return TorrentOperationResult::Continue;
        }

        let total_pieces = context.data_pool().num_of_pieces().await;
        if total_pieces == 0 {
            return TorrentOperationResult::Continue;
        }

        info!("SeedAllPieces: marking all {total_pieces} pieces as complete");
        for piece in 0..total_pieces {
            context.piece_completed(piece as PieceIndex).await;
        }
        self.marked = true;
        info!("SeedAllPieces: done - all pieces marked complete, bytes_completed now correct");

        TorrentOperationResult::Continue
    }
}
