// Artifact fetching interface (docs/design/phase5.md §3, §5). Kept separate
// from ArtifactService so the RemoteExecutor can be tested with a fake.
#pragma once

#include <expected>
#include <functional>
#include <string>

#include "tdr/artifacts/store.hpp"

namespace tdr::transfer {

class IArtifactFetcher {
public:
    virtual ~IArtifactFetcher() = default;
    // Make `artifact_id` present in the local store, fetching it from a peer
    // if needed. cb exactly once, later, on the io thread (also when already
    // present). Error strings are human-readable ("no holder", "store full",
    // "<node>: <reason>" of the last holder tried, "transfer stopped").
    virtual void fetch(const std::string& artifact_id,
                       std::function<void(std::expected<artifacts::ArtifactMeta, std::string>)> cb) = 0;
};

}  // namespace tdr::transfer
