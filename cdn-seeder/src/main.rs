use fx_callback::Callback;
use fx_torrent::operation::{
    ConnectPeersOperation, CreatePiecesAndFilesOperation, DhtNodesOperation, DhtPeersOperation,
    LsdPeersOperation, MetadataOperation, StatsOperation, TrackerPeersOperation, TrackersOperation,
    TorrentOperationFactory,
};
use fx_torrent::{FxTorrentSession, Session, SessionConfig, TorrentEvent, TorrentFlags, TorrentMetadata};

mod cdn_storage;

fn seed_operations() -> Vec<TorrentOperationFactory> {
    vec![
        TorrentOperationFactory::new(|| StatsOperation::new().into()),
        TorrentOperationFactory::new(|| TrackersOperation::new().into()),
        TorrentOperationFactory::new(|| DhtNodesOperation::new().into()),
        TorrentOperationFactory::new(|| DhtPeersOperation::new().into()),
        TorrentOperationFactory::new(|| LsdPeersOperation::new().into()),
        TorrentOperationFactory::new(|| TrackerPeersOperation::new().into()),
        TorrentOperationFactory::new(|| ConnectPeersOperation::new(true).into()),
        TorrentOperationFactory::new(|| MetadataOperation::new(None).into()),
        TorrentOperationFactory::new(|| CreatePiecesAndFilesOperation::new().into()),
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: cdn-seeder <torrent-file> <cdn-url>");
        std::process::exit(1);
    }
    let torrent_path = &args[1];
    let cdn_url = &args[2];

    let torrent_bytes = tokio::fs::read(torrent_path).await?;
    let metadata = TorrentMetadata::try_from(&torrent_bytes[..])?;
    let info = metadata.info.as_ref().ok_or("torrent has no info dict")?;
    let piece_length = info.piece_length as usize;
    let total_length = info.len();
    let num_pieces = (total_length + piece_length - 1) / piece_length;

    println!(
        "Torrent: {} ({} pieces, {} MiB, piece size {} KiB)",
        metadata.name().unwrap_or("unknown"),
        num_pieces,
        total_length / (1024 * 1024),
        piece_length / 1024,
    );

    let url = cdn_url.to_string();
    let session = FxTorrentSession::builder()
        .config(
            SessionConfig::builder()
                .client_name("cdn-seeder")
                .path("/tmp/cdn-seeder")
                .build(),
        )
        .default_extensions()
        .operations(seed_operations())
        .storage(move |_params| {
            cdn_storage::CdnStorage::new(url.clone(), piece_length, total_length).into()
        })
        .build()?;

    let torrent = session
        .add_torrent_from_info(metadata, TorrentFlags::SeedMode | TorrentFlags::UploadMode)
        .await?;

    let port = torrent.peer_port().await.unwrap_or_default();
    println!("Seeding on port {port}");
    println!("Press Ctrl+C to stop.");

    let mut events = torrent.subscribe();
    loop {
        tokio::select! {
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        match &*event {
                            TorrentEvent::StateChanged(state) => {
                                println!("State: {state:?}");
                                if matches!(state, fx_torrent::TorrentState::Seeding) {
                                    println!("Now seeding!");
                                }
                            }
                            TorrentEvent::PeerConnected(info) => {
                                println!("Peer connected: {info:?}");
                            }
                            TorrentEvent::PeerDisconnected(info) => {
                                println!("Peer disconnected: {info:?}");
                            }
                            TorrentEvent::PieceCompleted(idx) => {
                                println!("Piece {idx} completed");
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        eprintln!("Event error: {e}");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nShutting down...");
                break;
            }
        }
    }

    Ok(())
}
