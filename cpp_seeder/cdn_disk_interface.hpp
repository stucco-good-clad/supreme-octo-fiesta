#pragma once

#include <libtorrent/disk_interface.hpp>
#include <libtorrent/io_context.hpp>
#include <libtorrent/storage_defs.hpp>
#include <libtorrent/peer_request.hpp>
#include <libtorrent/sha1_hash.hpp>
#include <libtorrent/sha256.hpp>
#include <libtorrent/span.hpp>
#include <libtorrent/units.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/performance_counters.hpp>
#include <libtorrent/disk_buffer_holder.hpp>
#include <libtorrent/aux_/disk_buffer_pool.hpp>
#include <boost/asio/thread_pool.hpp>
#include <atomic>
#include <string>
#include <memory>

class cdn_disk_interface final : public lt::disk_interface {
public:
    cdn_disk_interface(lt::io_context& ioc,
                       lt::settings_interface const& settings,
                       lt::counters& cnt);

    static std::unique_ptr<lt::disk_interface> create(
        lt::io_context& ioc,
        lt::settings_interface const& settings,
        lt::counters& cnt);

    lt::storage_holder new_torrent(lt::storage_params const& p,
                                   std::shared_ptr<void> const& torrent) override;
    void remove_torrent(lt::storage_index_t st) override;

    void async_read(lt::storage_index_t st, lt::peer_request const& r,
                    std::function<void(lt::disk_buffer_holder, lt::storage_error const&)> handler,
                    lt::disk_job_flags_t flags) override;

    bool async_write(lt::storage_index_t st, lt::peer_request const& r,
                     char const* buf, std::shared_ptr<lt::disk_observer> o,
                     std::function<void(lt::storage_error const&)> handler,
                     lt::disk_job_flags_t flags) override;

    void async_hash(lt::storage_index_t st, lt::piece_index_t piece,
                    lt::span<lt::sha256_hash> v2,
                    lt::disk_job_flags_t flags,
                    std::function<void(lt::piece_index_t, lt::sha1_hash const&,
                                       lt::storage_error const&)> handler) override;

    void async_hash2(lt::storage_index_t st, lt::piece_index_t piece,
                     int offset, lt::disk_job_flags_t flags,
                     std::function<void(lt::piece_index_t, lt::sha256_hash const&,
                                        lt::storage_error const&)> handler) override;

    void async_move_storage(lt::storage_index_t st, std::string p, lt::move_flags_t f,
                            std::function<void(lt::status_t, std::string const&,
                                               lt::storage_error const&)> handler) override;
    void async_release_files(lt::storage_index_t st, std::function<void()> handler) override;
    void async_delete_files(lt::storage_index_t st, lt::remove_flags_t options,
                            std::function<void(lt::storage_error const&)> handler) override;
    void async_check_files(lt::storage_index_t st,
                           lt::add_torrent_params const* resume_data,
                           lt::aux::vector<std::string, lt::file_index_t> links,
                           std::function<void(lt::status_t, lt::storage_error const&)> handler) override;
    void async_rename_file(lt::storage_index_t st, lt::file_index_t idx, std::string name,
                           std::function<void(std::string const&, lt::file_index_t,
                                              lt::storage_error const&)> handler) override;
    void async_stop_torrent(lt::storage_index_t st, std::function<void()> handler) override;
    void async_set_file_priority(lt::storage_index_t st,
                                 lt::aux::vector<lt::download_priority_t, lt::file_index_t> prio,
                                 std::function<void(lt::storage_error const&,
                                                    lt::aux::vector<lt::download_priority_t,
                                                                    lt::file_index_t>)> handler) override;
    void async_clear_piece(lt::storage_index_t st, lt::piece_index_t index,
                           std::function<void(lt::piece_index_t)> handler) override;

    void update_stats_counters(lt::counters& c) const override;
    std::vector<lt::open_file_state> get_status(lt::storage_index_t st) const override;
    void abort(bool wait) override;
    void submit_jobs() override;
    void settings_updated() override;

private:
    int fetch_from_cdn(char* buf, int piece_index, int offset, int len);

    lt::io_context& m_ioc;
    lt::counters& m_counters;
    lt::aux::disk_buffer_pool m_buffer_pool;
    boost::asio::thread_pool m_pool{6};
    std::atomic<bool> m_aborted{false};
    std::string m_base_url = "https://raw.githubusercontent.com/stucco-good-clad/supreme-octo-fiesta/refs/heads/main";
    int m_piece_size = 1048576;
};
