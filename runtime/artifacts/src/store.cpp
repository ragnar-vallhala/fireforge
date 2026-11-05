// Local content-addressed artifact store. See store.hpp.
#include "tdr/artifacts/store.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <openssl/evp.h>

#include <cerrno>
#include <cstring>
#include <format>
#include <system_error>
#include <vector>

#include "tdr/common/common.hpp"

namespace tdr::artifacts {

namespace {

constexpr std::string_view kIdPrefix = "artifact://sha256/";
constexpr std::size_t kChunk = 1 << 20;  // 1 MiB

std::string errno_msg(std::string_view what, const fs::path& p, int err = errno) {
    return std::format("{} {}: {}", what, p.string(), std::strerror(err));
}

bool is_lower_hex64(std::string_view s) {
    if (s.size() != 64) return false;
    for (char ch : s) {
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) return false;
    }
    return true;
}

// RAII file descriptor.
class Fd {
public:
    explicit Fd(int fd = -1) : fd_(fd) {}
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    ~Fd() { if (fd_ >= 0) ::close(fd_); }
    int get() const { return fd_; }
    bool ok() const { return fd_ >= 0; }
    int release() { int f = fd_; fd_ = -1; return f; }
private:
    int fd_;
};

std::expected<void, std::string> fsync_path(const fs::path& p, int flags) {
    Fd fd(::open(p.c_str(), flags | O_CLOEXEC));
    if (!fd.ok()) return std::unexpected(errno_msg("open", p));
    if (::fsync(fd.get()) != 0) return std::unexpected(errno_msg("fsync", p));
    return {};
}

std::expected<void, std::string> fsync_dir(const fs::path& p) { return fsync_path(p, O_RDONLY | O_DIRECTORY); }

// Copies src to dst (dst must not exist), fsyncs dst. Mode 0644 before chmod.
std::expected<void, std::string> copy_file_synced(const fs::path& src, const fs::path& dst) {
    Fd in(::open(src.c_str(), O_RDONLY | O_CLOEXEC));
    if (!in.ok()) return std::unexpected(errno_msg("open", src));
    Fd out(::open(dst.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644));
    if (!out.ok()) return std::unexpected(errno_msg("create", dst));
    std::vector<char> buf(kChunk);
    for (;;) {
        ssize_t n = ::read(in.get(), buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            return std::unexpected(errno_msg("read", src));
        }
        if (n == 0) break;
        const char* p = buf.data();
        while (n > 0) {
            ssize_t w = ::write(out.get(), p, static_cast<std::size_t>(n));
            if (w < 0) {
                if (errno == EINTR) continue;
                return std::unexpected(errno_msg("write", dst));
            }
            p += w;
            n -= w;
        }
    }
    if (::fsync(out.get()) != 0) return std::unexpected(errno_msg("fsync", dst));
    if (::close(out.release()) != 0) return std::unexpected(errno_msg("close", dst));
    return {};
}

class LocalStore final : public IArtifactStore {
public:
    explicit LocalStore(fs::path root) : root_(std::move(root)) {}

    std::expected<ArtifactMeta, std::string> ingest(const fs::path& src, std::string media_type,
                                                    std::string producer_job, bool move_source) override {
        struct stat st{};
        if (::stat(src.c_str(), &st) != 0) return std::unexpected(errno_msg("stat", src));
        if (!S_ISREG(st.st_mode)) return std::unexpected(std::format("not a regular file: {}", src.string()));

        const fs::path staged = root_ / "tmp" / new_id("ingest");
        bool remove_source_after = false;
        if (move_source) {
            if (::rename(src.c_str(), staged.c_str()) == 0) {
                if (auto r = fsync_path(staged, O_RDONLY); !r) return fail(staged, r.error());
            } else if (errno == EXDEV) {
                if (auto r = copy_file_synced(src, staged); !r) return fail(staged, r.error());
                remove_source_after = true;
            } else {
                return std::unexpected(errno_msg("rename", src));
            }
        } else {
            if (auto r = copy_file_synced(src, staged); !r) return fail(staged, r.error());
        }

        auto hex = sha256_file(staged);
        if (!hex) return fail(staged, hex.error());

        std::error_code ec;
        const fs::path final_path = object_path(*hex);
        fs::create_directories(final_path.parent_path(), ec);
        if (ec) return fail(staged, std::format("mkdir {}: {}", final_path.parent_path().string(), ec.message()));

        if (fs::exists(final_path, ec)) {
            fs::remove(staged, ec);  // identical content already published
        } else {
            if (::chmod(staged.c_str(), 0444) != 0) return fail(staged, errno_msg("chmod", staged));
            if (::rename(staged.c_str(), final_path.c_str()) != 0) return fail(staged, errno_msg("rename", staged));
            if (auto r = fsync_dir(final_path.parent_path()); !r) return std::unexpected(r.error());
        }
        if (remove_source_after) {
            fs::remove(src, ec);
            if (ec) log_warn("artifact ingest: could not remove moved source {}: {}", src.string(), ec.message());
        }

        struct stat fst{};
        if (::stat(final_path.c_str(), &fst) != 0) return std::unexpected(errno_msg("stat", final_path));
        ArtifactMeta m;
        m.id = make_artifact_id(*hex);
        m.sha256_hex = *hex;
        m.size = static_cast<std::uint64_t>(fst.st_size);
        m.media_type = media_type.empty() ? "application/octet-stream" : std::move(media_type);
        m.producer_job = std::move(producer_job);
        m.created_unix_ms = unix_ms();
        m.local_path = final_path;
        return m;
    }

    bool contains(std::string_view id) const override { return path_of(id).has_value(); }

    std::optional<fs::path> path_of(std::string_view id) const override {
        auto hex = parse_artifact_id(id);
        if (!hex) return std::nullopt;
        fs::path p = object_path(*hex);
        std::error_code ec;
        if (!fs::is_regular_file(p, ec)) return std::nullopt;
        return p;
    }

    std::expected<void, std::string> materialize(std::string_view id, const fs::path& dest) const override {
        auto p = path_of(id);
        if (!p) return std::unexpected(std::format("artifact not found: {}", id));
        std::error_code ec;
        if (dest.has_parent_path()) {
            fs::create_directories(dest.parent_path(), ec);
            if (ec) return std::unexpected(std::format("mkdir {}: {}", dest.parent_path().string(), ec.message()));
        }
        if (::link(p->c_str(), dest.c_str()) == 0) return {};  // store file is already 0444
        if (errno == EEXIST) return std::unexpected(errno_msg("link", dest));
        if (auto r = copy_file_synced(*p, dest); !r) {
            fs::remove(dest, ec);
            return r;
        }
        if (::chmod(dest.c_str(), 0444) != 0) return std::unexpected(errno_msg("chmod", dest));
        return {};
    }

    bool verify(std::string_view id) const override {
        auto hex = parse_artifact_id(id);
        if (!hex) return false;
        auto p = path_of(id);
        if (!p) return false;
        auto actual = sha256_file(*p);
        return actual && *actual == *hex;
    }

    const fs::path& root() const override { return root_; }

private:
    fs::path object_path(const std::string& hex) const { return root_ / "sha256" / hex.substr(0, 2) / hex; }

    static std::unexpected<std::string> fail(const fs::path& staged, std::string msg) {
        std::error_code ec;
        fs::remove(staged, ec);
        return std::unexpected(std::move(msg));
    }

    fs::path root_;
};

}  // namespace

std::string make_artifact_id(std::string_view sha256_hex) { return std::format("{}{}", kIdPrefix, sha256_hex); }

std::optional<std::string> parse_artifact_id(std::string_view id) {
    if (!id.starts_with(kIdPrefix)) return std::nullopt;
    auto hex = id.substr(kIdPrefix.size());
    if (!is_lower_hex64(hex)) return std::nullopt;
    return std::string(hex);
}

std::expected<std::string, std::string> sha256_file(const fs::path& path) {
    Fd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
    if (!fd.ok()) return std::unexpected(errno_msg("open", path));

    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(), &EVP_MD_CTX_free);
    if (!ctx || EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr) != 1) {
        return std::unexpected(std::string("EVP sha256 init failed"));
    }
    std::vector<unsigned char> buf(kChunk);
    for (;;) {
        ssize_t n = ::read(fd.get(), buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            return std::unexpected(errno_msg("read", path));
        }
        if (n == 0) break;
        if (EVP_DigestUpdate(ctx.get(), buf.data(), static_cast<std::size_t>(n)) != 1) {
            return std::unexpected(std::string("EVP sha256 update failed"));
        }
    }
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    if (EVP_DigestFinal_ex(ctx.get(), md, &len) != 1) return std::unexpected(std::string("EVP sha256 final failed"));
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (unsigned int i = 0; i < len; ++i) {
        out.push_back(kHex[md[i] >> 4]);
        out.push_back(kHex[md[i] & 0xf]);
    }
    return out;
}

std::unique_ptr<IArtifactStore> open_local_store(const fs::path& root) {
    fs::create_directories(root / "sha256");
    fs::create_directories(root / "tmp");
    for (const auto& e : fs::directory_iterator(root / "tmp")) {
        std::error_code ec;
        fs::remove_all(e.path(), ec);
        if (ec) log_warn("artifact store: cannot clear {}: {}", e.path().string(), ec.message());
    }
    return std::make_unique<LocalStore>(root);
}

}  // namespace tdr::artifacts
