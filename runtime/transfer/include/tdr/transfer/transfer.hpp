// ArtifactService (docs/design/phase5.md §2-§3): serves local artifacts to
// peers and fetches missing ones from peers over data links. Chunked (1 MiB),
// windowed, resumable from <store>/partial/<hex>.part, sha256-verified,
// multi-source, deduplicated, optionally rate-limited.
//
// Single-threaded: everything on the daemon's io_context thread. File reads
// and writes are synchronous per chunk (1 MiB).
#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "tdr/artifacts/store.hpp"
#include "tdr/journal/journal.hpp"
#include "tdr/peer/manager.hpp"
#include "tdr/transfer/fetcher.hpp"

namespace tdr::transfer {

struct TransferOptions {
    std::size_t chunk_bytes = 1u << 20;
    std::uint32_t window = 8;                       // chunks in flight per transfer
    std::uint32_t max_fetching = 4;                 // concurrent outgoing fetches (others queue)
    std::uint32_t max_serving = 8;                  // concurrent serves (beyond: ERROR UNAVAILABLE "busy")
    std::uint64_t max_serve_bytes_per_sec = 0;      // total serving rate cap, 0 = unlimited
    std::chrono::milliseconds idle_timeout{30000};  // no chunk/ack for this long -> link dropped
};

struct TransferHooks {
    // Node ids holding the artifact, from the control plane (GET
    // /artifacts/sha256/{hex}). Self is skipped by the service. Error -> fetch fails.
    std::function<void(const std::string& artifact_id,
                       std::function<void(std::expected<std::vector<std::string>, std::string>)>)> holders;
    // A fetched artifact entered the store (journal row already recorded):
    // announce it, maybe run GC.
    std::function<void(const artifacts::ArtifactMeta&)> on_stored;
    // Before a fetch starts: may `bytes` more be stored? (GC may free space.)
    // Unset = always. false -> fetch fails "store full".
    std::function<bool(std::uint64_t bytes)> has_room;
    // An artifact was used (served): bump its LRU clock.
    std::function<void(const std::string& artifact_id)> touch;
};

struct TransferStats {
    std::uint32_t fetching = 0;
    std::uint32_t queued = 0;
    std::uint32_t serving = 0;
    std::uint64_t fetched = 0;          // completed fetches
    std::uint64_t fetch_failures = 0;
    std::uint64_t bytes_received = 0;
    std::uint64_t bytes_served = 0;
    std::uint64_t resumed = 0;          // transfers that started at offset > 0
};

class ArtifactService : public IArtifactFetcher {
public:
    // Installs itself as `peers`' data acceptor. peers/store/journal must outlive it.
    ArtifactService(boost::asio::io_context& io, peer::PeerManager& peers, artifacts::IArtifactStore& store,
                    journal::IJournal& journal, TransferOptions options, TransferHooks hooks);
    ~ArtifactService() override;
    ArtifactService(const ArtifactService&) = delete;
    ArtifactService& operator=(const ArtifactService&) = delete;

    void fetch(const std::string& artifact_id,
               std::function<void(std::expected<artifacts::ArtifactMeta, std::string>)> cb) override;

    // Fails pending/active fetches ("transfer stopped"), closes all data links,
    // refuses new work. Keeps .part files for resumption.
    void stop();

    // Being served or fetched right now (GC must not delete it).
    bool busy(const std::string& artifact_id) const;
    TransferStats stats() const;

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace tdr::transfer
