use fx_callback::Callback;
use fx_torrent::dht::DhtTracker;
use fx_torrent::operation::{
    ConnectPeersOperation, CreatePiecesAndFilesOperation, DhtNodesOperation, DhtPeersOperation,
    LsdPeersOperation, MetadataOperation, StatsOperation, TrackerPeersOperation, TrackersOperation,
    TorrentOperationFactory,
};
use fx_torrent::{FxTorrentSession, LocalServiceDiscovery, Session, SessionConfig, TorrentEvent, TorrentFlags, TorrentMetadata};
use seed_operation::SeedAllPiecesOperation;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;
use tracing::{info, warn};

mod cdn_storage;
mod seed_operation;

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
        TorrentOperationFactory::new(|| SeedAllPiecesOperation::new().into()),
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cdn_seeder=info,fx_torrent=warn,fx_torrent::dht::tracker=off".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: cdn-seeder <torrent-file> <cdn-url>");
        std::process::exit(1);
    }
    let torrent_path = &args[1];
    let cdn_url = &args[2];

    info!("=== CDNSeeder starting ===");
    info!("Torrent file: {torrent_path}");
    info!("CDN URL: {cdn_url}");

    let torrent_bytes = tokio::fs::read(torrent_path).await?;
    let metadata = TorrentMetadata::try_from(&torrent_bytes[..])?;
    let info = metadata.info.as_ref().ok_or("torrent has no info dict")?;
    let piece_length = info.piece_length as usize;
    let total_length = info.len();
    let num_pieces = (total_length + piece_length - 1) / piece_length;

    info!("Torrent name: {}", metadata.name().unwrap_or("unknown"));
    info!("Info hash: {:?}", metadata.info_hash);
    info!("Piece length: {piece_length} bytes ({} KiB)", piece_length / 1024);
    info!("Total length: {total_length} bytes ({} MiB)", total_length / (1024 * 1024));
    info!("Number of pieces: {num_pieces}");
    info!("Trackers: {:?}", metadata.announce_list);

    let url = cdn_url.to_string();
    info!("Building session with CDN storage...");

    let dht = DhtTracker::builder()
        .default_routing_nodes()
        .build()
        .await?;
    info!("DHT tracker created");

    let lsd = LocalServiceDiscovery::new(Ipv4Addr::UNSPECIFIED.into())
        .await?;
    info!("LSD created");

    let session = FxTorrentSession::builder()
        .config(
            SessionConfig::builder()
                .client_name("cdn-seeder")
                .path("/tmp/cdn-seeder")
                .enable_tcp_peer(true)
                .enable_utp_peer(true)
                .build(),
        )
        .dht(dht)
        .local_service_discovery(lsd)
        .default_extensions()
        .operations(seed_operations())
        .storage(move |params| {
            info!("Storage factory called: info_hash={:?} path={}", params.info_hash, params.path.display());
            cdn_storage::CdnStorage::new(url.clone(), piece_length, total_length).into()
        })
        .build()?;

    info!("Session built. Adding torrent...");

    let torrent = session
        .add_torrent_from_info(metadata, TorrentFlags::SeedMode | TorrentFlags::UploadMode)
        .await?;

    let port = torrent.peer_port().await.unwrap_or_default();
    info!("=== Seeding on port {port} ===");

    if let Some(ext_port) = upnp_forward(port).await {
        info!("=== UPnP mapped: external port {ext_port} -> internal {port} ===");
    } else {
        info!("=== No UPnP: ensure port {port} is forwarded on your router ===");
    }

    let mut events = torrent.subscribe();
    info!("Event loop started. Waiting for activity...");

    loop {
        tokio::select! {
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        match &*event {
                            TorrentEvent::StateChanged(state) => {
                                info!("State -> {state:?}");
                            }
                            TorrentEvent::PeerConnected(info) => {
                                info!("PEER CONNECTED: {info:?}");
                            }
                            TorrentEvent::PeerDisconnected(info) => {
                                info!("PEER DISCONNECTED: {info:?}");
                            }
                            TorrentEvent::PieceCompleted(_) => {}
                            TorrentEvent::TrackersChanged => {}
                            TorrentEvent::PiecesChanged(count) => {
                                info!("Pieces: {count} available");
                            }
                            TorrentEvent::Stats(metrics) => {
                                let (up, down, peers) = (metrics.upload.total(), metrics.download.total(), metrics.peers.get());
                                if peers > 0 || up > 0 || down > 0 {
                                    info!("Stats -> upload={up} download={down} peers={peers}");
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        tracing::error!("Event error: {e}");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Shutting down...");
                break;
            }
        }
    }

    Ok(())
}

async fn upnp_forward(port: u16) -> Option<u16> {
    let local_addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

    let opts = igd::SearchOptions {
        timeout: Some(Duration::from_secs(3)),
        ..Default::default()
    };

    let gateway = match igd::aio::search_gateway(opts).await {
        Ok(gw) => gw,
        Err(e) => {
            warn!("UPnP: no gateway found: {e}");
            return None;
        }
    };

    match gateway.get_external_ip().await {
        Ok(ip) => info!("UPnP: gateway found, external IP: {ip}"),
        Err(e) => warn!("UPnP: could not get external IP: {e}"),
    }

    match gateway
        .add_port(
            igd::PortMappingProtocol::TCP,
            port,
            local_addr,
            3600,
            "cdn-seeder",
        )
        .await
    {
        Ok(()) => {
            info!("UPnP: TCP port {port} forwarded successfully");
            Some(port)
        }
        Err(e) => {
            warn!("UPnP: failed to forward TCP port {port}: {e}");
            None
        }
    }
}
