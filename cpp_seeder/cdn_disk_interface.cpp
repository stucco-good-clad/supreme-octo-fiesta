#include "cdn_disk_interface.hpp"
#include <libtorrent/error_code.hpp>
#include <libtorrent/hasher.hpp>
#include <boost/asio/post.hpp>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <cstdio>
#include <chrono>

extern "C" int cdn_fetch(const char* url, int offset, int len, unsigned char* output);

cdn_disk_interface::cdn_disk_interface(
    lt::io_context& ioc,
    lt::settings_interface const& settings,
    lt::counters& cnt)
: m_ioc(ioc)
, m_counters(cnt)
, m_buffer_pool(ioc)
, m_disable_hash_checks(settings.get_bool(lt::settings_pack::disable_hash_checks))
{
    m_buffer_pool.set_settings(settings);
}

std::unique_ptr<lt::disk_interface> cdn_disk_interface::create(
    lt::io_context& ioc,
    lt::settings_interface const& settings,
    lt::counters& cnt)
{
    return std::make_unique<cdn_disk_interface>(ioc, settings, cnt);
}

lt::storage_holder cdn_disk_interface::new_torrent(
    lt::storage_params const& /*p*/,
    std::shared_ptr<void> const& /*torrent*/)
{
    return lt::storage_holder{lt::storage_index_t{0}, *this};
}

void cdn_disk_interface::remove_torrent(lt::storage_index_t) {}

int cdn_disk_interface::fetch_from_cdn(char* buf, int piece_index, int offset, int len) {
    auto t0 = std::chrono::steady_clock::now();

    std::string url = m_base_url + "/piece_" + std::to_string(piece_index) + ".bin";

    int ret = cdn_fetch(url.c_str(), offset, len, reinterpret_cast<unsigned char*>(buf));

    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    if (ret < 0) {
        fprintf(stderr, "\n[CDN_TIMING] piece=%d off=%d FAIL %ldms (ret=%d)\n",
            piece_index, offset, ms, ret);
        return -1;
    }

    int copy_len = std::min(ret, len);
    if (copy_len < len) {
        std::memset(buf + copy_len, 0, static_cast<std::size_t>(len - copy_len));
    }

    fprintf(stderr, "\n[CDN_TIMING] piece=%d off=%d len=%d OK %dbytes %ldms\n",
        piece_index, offset, len, copy_len, ms);
    return copy_len;
}

void cdn_disk_interface::async_read(
    lt::storage_index_t /*st*/, lt::peer_request const& r,
    std::function<void(lt::disk_buffer_holder, lt::storage_error const&)> handler,
    lt::disk_job_flags_t /*flags*/)
{
    fprintf(stderr, "\n[CDN_DEBUG] async_read called: piece=%d start=%d length=%d\n",
        static_cast<int>(r.piece), r.start, r.length);

    // allocate buffer synchronously (fast)
    char* buf = m_buffer_pool.public_allocate_buffer("cdn_read");
    if (!buf) {
        lt::storage_error se;
        se.ec = lt::error_code(boost::system::errc::not_enough_memory,
            boost::system::generic_category());
        se.operation = lt::operation_t::alloc_cache_piece;
        boost::asio::post(m_ioc, [h = std::move(handler), se] {
            h(lt::disk_buffer_holder{}, se);
        });
        return;
    }

    // dispatch CDN fetch to background thread pool
    boost::asio::post(m_pool, [this, buf, r, handler = std::move(handler)] {
        if (m_aborted) { m_buffer_pool.public_free_buffer(buf); return; }

        int n = fetch_from_cdn(buf, static_cast<int>(r.piece), r.start, r.length);

        if (n < 0) {
            boost::asio::post(m_ioc, [this, buf, handler = std::move(handler)] {
                if (m_aborted) { m_buffer_pool.public_free_buffer(buf); return; }
                m_buffer_pool.public_free_buffer(buf);
                lt::storage_error se;
                se.ec = lt::error_code(boost::system::errc::io_error,
                    boost::system::generic_category());
                se.operation = lt::operation_t::file_read;
                handler(lt::disk_buffer_holder{}, se);
            });
            return;
        }

        fprintf(stderr, "\n[CDN_DEBUG] async_read success: fetched %d bytes\n", n);
        boost::asio::post(m_ioc, [this, buf, r, handler = std::move(handler)] {
            if (m_aborted) { m_buffer_pool.public_free_buffer(buf); return; }
            lt::disk_buffer_holder holder(m_buffer_pool, buf, r.length);
            handler(std::move(holder), lt::storage_error{});
        });
    });
}
            
        bool cdn_disk_interface::async_write(
            lt::storage_index_t, lt::peer_request const&, char const*,
            std::shared_ptr<lt::disk_observer>,
            std::function<void(lt::storage_error const&)> handler,
            lt::disk_job_flags_t)
        {
            handler(lt::storage_error{});
            return false;
        }
        
void cdn_disk_interface::async_hash(
    lt::storage_index_t st, lt::piece_index_t piece,
    lt::span<lt::sha256_hash> /*v2*/,
    lt::disk_job_flags_t flags,
    std::function<void(lt::piece_index_t, lt::sha1_hash const&,
        lt::storage_error const&)> handler)
{
    fprintf(stderr, "\n[CDN_DEBUG] async_hash called: piece=%d disable_hash_checks=%d\n",
        static_cast<int>(piece), int(m_disable_hash_checks));

    if (m_disable_hash_checks) {
        boost::asio::post(m_ioc, [this, piece, handler = std::move(handler)] {
            if (m_aborted) return;
            handler(piece, lt::sha1_hash{}, lt::storage_error{});
        });
        return;
    }

    boost::asio::post(m_pool, [this, piece, handler = std::move(handler)] {
        if (m_aborted) return;

        std::vector<char> buf(static_cast<std::size_t>(m_piece_size));
        int n = fetch_from_cdn(buf.data(), static_cast<int>(piece), 0, m_piece_size);
        lt::hasher h;
        if (n > 0) {
            h.update({buf.data(), n});
        }

        boost::asio::post(m_ioc, [this, piece, h, handler = std::move(handler)]() mutable {
            if (m_aborted) return;
            handler(piece, h.final(), lt::storage_error{});
        });
    });
}

void cdn_disk_interface::async_hash2(
    lt::storage_index_t, lt::piece_index_t piece, int,
    lt::disk_job_flags_t,
    std::function<void(lt::piece_index_t, lt::sha256_hash const&,
        lt::storage_error const&)> handler)
{
    boost::asio::post(m_ioc, [piece, handler = std::move(handler)] {
        handler(piece, lt::sha256_hash{}, lt::storage_error{});
    });
}

void cdn_disk_interface::async_move_storage(lt::storage_index_t, std::string p, lt::move_flags_t,
    std::function<void(lt::status_t, std::string const&, lt::storage_error const&)> h) { h(lt::status_t::no_error, p, {}); }
void cdn_disk_interface::async_release_files(lt::storage_index_t, std::function<void()> h) { if(h) h(); }
void cdn_disk_interface::async_delete_files(lt::storage_index_t, lt::remove_flags_t, std::function<void(lt::storage_error const&)> h) { h({}); }
void cdn_disk_interface::async_check_files(lt::storage_index_t, lt::add_torrent_params const*, lt::aux::vector<std::string, lt::file_index_t>, std::function<void(lt::status_t, lt::storage_error const&)> h) { h(lt::status_t::no_error, {}); }
void cdn_disk_interface::async_rename_file(lt::storage_index_t, lt::file_index_t, std::string n, std::function<void(std::string const&, lt::file_index_t, lt::storage_error const&)> h) { h(n, {}, {}); }
void cdn_disk_interface::async_stop_torrent(lt::storage_index_t, std::function<void()> h) { if(h) h(); }
void cdn_disk_interface::async_set_file_priority(lt::storage_index_t, lt::aux::vector<lt::download_priority_t, lt::file_index_t> p, std::function<void(lt::storage_error const&, lt::aux::vector<lt::download_priority_t, lt::file_index_t>)> h) { h({}, std::move(p)); }
void cdn_disk_interface::async_clear_piece(lt::storage_index_t, lt::piece_index_t i, std::function<void(lt::piece_index_t)> h) { h(i); }
void cdn_disk_interface::update_stats_counters(lt::counters&) const {}
std::vector<lt::open_file_state> cdn_disk_interface::get_status(lt::storage_index_t) const { return {}; }
void cdn_disk_interface::abort(bool)
{
    m_aborted = true;
    m_pool.stop();
    m_pool.join();
}
void cdn_disk_interface::submit_jobs() {}
void cdn_disk_interface::settings_updated() {}
