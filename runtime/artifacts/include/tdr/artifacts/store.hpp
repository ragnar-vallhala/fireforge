// Local content-addressed artifact store.
//
// Layout:  <root>/sha256/<first 2 hex>/<64 hex>      immutable, mode 0444
//          <root>/tmp/                                staging for ingest
// Id:      "artifact://sha256/<64 lowercase hex>"
#pragma once

#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace tdr::artifacts {

namespace fs = std::filesystem;

struct ArtifactMeta {
    std::string id;           // artifact://sha256/<hex>
    std::string sha256_hex;
    std::uint64_t size = 0;
    std::string media_type;   // "application/octet-stream" if unknown
    std::string producer_job; // job id, may be empty
    std::int64_t created_unix_ms = 0;
    fs::path local_path;      // path inside the store
};

std::string make_artifact_id(std::string_view sha256_hex);
// Returns the 64-hex digest, or nullopt if `id` is not a valid artifact id.
std::optional<std::string> parse_artifact_id(std::string_view id);

// Streaming SHA-256 of a file (OpenSSL EVP), lowercase hex.
std::expected<std::string, std::string> sha256_file(const fs::path& path);

class IArtifactStore {
public:
    virtual ~IArtifactStore() = default;

    // Hash `src` and publish it into the store. Atomic: readers never see a
    // partial file (write/copy to tmp/, fsync, rename). If `move_source` is
    // true and src is on the same filesystem it is renamed instead of copied.
    // Idempotent: ingesting identical content returns the existing artifact.
    virtual std::expected<ArtifactMeta, std::string> ingest(const fs::path& src,
                                                            std::string media_type,
                                                            std::string producer_job,
                                                            bool move_source) = 0;

    virtual bool contains(std::string_view id) const = 0;
    virtual std::optional<fs::path> path_of(std::string_view id) const = 0;

    // Place a read-only copy at `dest` (hard link when possible, else copy).
    // Creates parent dirs. Fails if the artifact is missing.
    virtual std::expected<void, std::string> materialize(std::string_view id, const fs::path& dest) const = 0;

    // Re-hash the stored file; false if missing or corrupted.
    virtual bool verify(std::string_view id) const = 0;

    virtual const fs::path& root() const = 0;
};

// Creates <root>/sha256 and <root>/tmp. Clears stale files in tmp/ on open.
std::unique_ptr<IArtifactStore> open_local_store(const fs::path& root);

}  // namespace tdr::artifacts
