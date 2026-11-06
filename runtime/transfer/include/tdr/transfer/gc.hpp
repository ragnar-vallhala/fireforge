// StoreGc (docs/design/phase5.md §6): keeps the local artifact store under a
// byte cap. LRU by store-file mtime (bumped by touch()); evicts only copies the
// control plane agrees to release (another node holds one, not pinned).
//
// Single-threaded: everything on the daemon's io_context thread.
#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "fireforge/control/v1/control.pb.h"
#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/artifacts/store.hpp"

namespace tdr::transfer {

struct GcOptions {
    std::uint64_t max_bytes = 0;                    // 0 = no cap: GC never evicts
    std::uint32_t high_pct = 90;                    // start above this % of max_bytes
    std::uint32_t low_pct = 80;                     // evict down to this %
    std::chrono::milliseconds interval{60000};
    std::chrono::milliseconds min_age{600000};      // younger artifacts (mtime) are kept
};

struct GcHooks {
    // Ids that must not be evicted now (inputs of non-terminal jobs / fetching
    // leases, being served or fetched).
    std::function<bool(const std::string& artifact_id)> is_protected;
    // Ask the control plane (POST .../release, force=false). Unset or error ->
    // keep the artifact.
    std::function<void(const std::string& artifact_id,
                       std::function<void(std::expected<::fireforge::control::v1::ReleaseReply, std::string>)>)> release;
};

class StoreGc {
public:
    StoreGc(boost::asio::io_context& io, artifacts::IArtifactStore& store, GcOptions options, GcHooks hooks);
    ~StoreGc();
    StoreGc(const StoreGc&) = delete;
    StoreGc& operator=(const StoreGc&) = delete;

    void start();   // periodic runs
    void stop();    // cancels timers; a running pass finishes its current release, then stops

    // One pass now (coalesced with a running one: cb gets that pass's report).
    // Evicts toward low_pct only when above high_pct, unless `force_to_low`.
    void run(std::function<void(const ::fireforge::tdr::v1::GcReport&)> cb, bool force_to_low = false);

    // Bump the LRU clock (store file mtime = now). No-op if absent.
    void touch(const std::string& artifact_id);
    // Bytes in <store>/sha256 (rescanned each pass; cached between passes,
    // updated by note_added()).
    std::uint64_t store_bytes() const;
    void note_added(std::uint64_t bytes);
    // Would `bytes` more fit under max_bytes (true if no cap)?
    bool has_room(std::uint64_t bytes) const;
    const GcOptions& options() const;

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace tdr::transfer
