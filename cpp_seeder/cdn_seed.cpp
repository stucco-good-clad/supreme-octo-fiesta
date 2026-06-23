#include "cdn_disk_interface.hpp"
#include <libtorrent/session.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/peer_info.hpp>
#include <libtorrent/time.hpp>
#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <vector>
#include <cstdio>

extern "C" void cdn_cache_init(int piece_size, int soft_limit_mb, int hard_limit_mb);

int main() {
    try {
        // Initialize Rust piece cache: 1MB pieces, 8GB soft, 12GB hard limit
        cdn_cache_init(1048576, 8192, 12288);

        lt::session_params params;
        params.disk_io_constructor = &cdn_disk_interface::create;

        lt::settings_pack& pack = params.settings;
        pack.set_int(lt::settings_pack::alert_mask,
                     lt::alert_category::status |
                     lt::alert_category::error |
                     lt::alert_category::storage |
                     lt::alert_category::peer |
                     lt::alert_category::tracker);
        pack.set_str(lt::settings_pack::listen_interfaces, "0.0.0.0:6881,[::]:6881");
        pack.set_bool(lt::settings_pack::disable_hash_checks, true);
        pack.set_bool(lt::settings_pack::enable_dht, true);
        pack.set_bool(lt::settings_pack::enable_lsd, true);
        pack.set_bool(lt::settings_pack::enable_upnp, true);
        pack.set_bool(lt::settings_pack::enable_natpmp, true);
        pack.set_str(lt::settings_pack::dht_bootstrap_nodes,
            "dht.libtorrent.org:25401,router.bittorrent.com:6881,"
            "dht.transmissionbt.com:6881,router.bt.ouinet.work:6881");

        pack.set_int(lt::settings_pack::unchoke_slots_limit, -1);
        pack.set_int(lt::settings_pack::num_optimistic_unchoke_slots, -1);
        pack.set_int(lt::settings_pack::seed_choking_algorithm,
                     lt::settings_pack::seed_choking_algorithm_t::fastest_upload);

        // Buffer tuning for CDN-backed seeding (fast reads, variable TCP)
        pack.set_int(lt::settings_pack::send_buffer_watermark, 100 * 1024 * 1024);          // 100 MB per peer
        pack.set_int(lt::settings_pack::send_buffer_low_watermark, 512 * 1024);             // 512 KB initial window
        pack.set_int(lt::settings_pack::max_queued_disk_bytes, 50 * 1024 * 1024);           // 50 MB disk queue
        pack.set_int(lt::settings_pack::max_peer_recv_buffer_size, 5 * 1024 * 1024);        // 5 MB recv buffer

        lt::session ses(params);

        lt::add_torrent_params atp;
        std::string torrent_path = "../cdn-seeder/test.torrent";
        atp.ti = std::make_shared<lt::torrent_info>(torrent_path);
        atp.save_path = "/tmp/cdn_seed_dummy";

        int num_pieces = atp.ti->num_pieces();
        atp.have_pieces.resize(num_pieces, true);
        atp.verified_pieces.resize(num_pieces, true);
        atp.flags |= lt::torrent_flags::seed_mode;

        lt::torrent_handle h = ses.add_torrent(atp);

        std::cout << "CDN Seed started. Pieces: " << num_pieces
                  << " | Monitoring (Ctrl+C to stop)...\n"
                  << "  Legend: remote_interested=peer wants our data | choked=we choked them\n"
                  << "  up_queue=requests from peer pending | last_active=seconds since last transfer\n"
                  << std::endl;

        for (int i = 0; ; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            lt::torrent_status st = h.status();
            std::cout << "[" << i+1 << "] "
                      << "State: " << st.state << " | "
                      << "Peers: " << st.num_peers << " | "
                      << "DHT: " << ses.is_dht_running() << " | "
                      << "Up: " << (st.upload_rate / 1024) << " KB/s | "
                      << "TotUp: " << (st.total_payload_upload / 1024) << " KB | "
                      << "seed_mode: " << bool(st.flags & lt::torrent_flags::seed_mode)
                      << " | auto_managed: " << bool(st.flags & lt::torrent_flags::auto_managed)
                      << std::endl;

            std::vector<lt::peer_info> peers;
            h.get_peer_info(peers);
            if (!peers.empty()) {
                std::cout << "  -- Connected peers (" << peers.size() << ") --\n";
            }
            for (auto const& p : peers) {
                char f[256];
                int n = 0;
                n += std::snprintf(f + n, sizeof(f) - n, "remote_interested=%d ",
                    int(bool(p.flags & lt::peer_info::remote_interested)));
                n += std::snprintf(f + n, sizeof(f) - n, "choked(we_choke_them)=%d ",
                    int(bool(p.flags & lt::peer_info::choked)));
                n += std::snprintf(f + n, sizeof(f) - n, "remote_choked(they_choke_us)=%d ",
                    int(bool(p.flags & lt::peer_info::remote_choked)));
                n += std::snprintf(f + n, sizeof(f) - n, "interesting=%d ",
                    int(bool(p.flags & lt::peer_info::interesting)));
                n += std::snprintf(f + n, sizeof(f) - n, "seed=%d ",
                    int(bool(p.flags & lt::peer_info::seed)));
                n += std::snprintf(f + n, sizeof(f) - n, "snubbed=%d ",
                    int(bool(p.flags & lt::peer_info::snubbed)));
                n += std::snprintf(f + n, sizeof(f) - n, "upload_only=%d ",
                    int(bool(p.flags & lt::peer_info::upload_only)));

                std::cout << "  [PEER] " << p.ip
                          << " | client=" << (p.client.empty() ? "?" : p.client)
                          << " | " << f
                          << " | up_queue=" << p.upload_queue_length
                          << " | down_queue=" << p.download_queue_length
                          << " | last_active=" << lt::total_seconds(p.last_active) << "s"
                          << " | rtt=" << p.rtt << "ms"
                          << " | total_up=" << (p.total_upload / 1024) << "KB"
                          << " | up_speed=" << (p.payload_up_speed / 1024) << "KB/s"
                          << " | progress=" << (p.progress_ppm / 10000) << "." << ((p.progress_ppm % 10000) / 100) << "%"
                          << std::endl;
            }

            std::vector<lt::alert*> alerts;
            ses.pop_alerts(&alerts);
            for (auto* a : alerts) {
                std::cerr << "\nALERT: " << a->message() << std::endl;
            }

            if (i >= 300) break;
        }
        std::cout << "\nDone." << std::endl;

    } catch (std::exception const& e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
        return 1;
    }
}
