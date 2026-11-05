#include <boost/test/unit_test.hpp>

#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <sstream>

#include "tdr/artifacts/store.hpp"
#include "tdr/common/common.hpp"

namespace fs = std::filesystem;
using namespace tdr::artifacts;

namespace {

struct TempDir {
    fs::path path = fs::path("/tmp/claude-1000") / tdr::new_id("artifacts-test");
    TempDir() { fs::create_directories(path); }
    ~TempDir() {
        std::error_code ec;
        // Store files are 0444; their directories are writable, so remove_all works.
        fs::remove_all(path, ec);
    }
};

void write_file(const fs::path& p, const std::string& data) {
    fs::create_directories(p.parent_path());
    std::ofstream(p, std::ios::binary) << data;
}

std::string read_file(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

constexpr const char* kAbcHex = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
constexpr const char* kEmptyHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

mode_t mode_of(const fs::path& p) {
    struct stat st{};
    ::stat(p.c_str(), &st);
    return st.st_mode & 0777;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(artifacts)

BOOST_AUTO_TEST_CASE(sha256_known_vector) {
    TempDir t;
    write_file(t.path / "abc", "abc");
    auto h = sha256_file(t.path / "abc");
    BOOST_TEST_REQUIRE(h.has_value());
    BOOST_TEST(*h == kAbcHex);
    BOOST_TEST(!sha256_file(t.path / "missing").has_value());
}

BOOST_AUTO_TEST_CASE(sha256_multi_chunk) {
    // 1 MiB + 3 bytes crosses the chunk boundary; compare against hashing the same
    // content written in a different split (content identity is what matters).
    TempDir t;
    std::string big((1 << 20) + 3, 'x');
    write_file(t.path / "a", big);
    write_file(t.path / "b", big);
    auto a = sha256_file(t.path / "a");
    auto b = sha256_file(t.path / "b");
    BOOST_TEST_REQUIRE(a.has_value());
    BOOST_TEST(*a == *b);
    BOOST_TEST(*a != kEmptyHex);
}

BOOST_AUTO_TEST_CASE(id_parse) {
    const std::string id = make_artifact_id(kAbcHex);
    BOOST_TEST(id == std::string("artifact://sha256/") + kAbcHex);
    auto p = parse_artifact_id(id);
    BOOST_TEST_REQUIRE(p.has_value());
    BOOST_TEST(*p == kAbcHex);

    BOOST_TEST(!parse_artifact_id(std::string("artifact://sha512/") + kAbcHex).has_value());
    BOOST_TEST(!parse_artifact_id(std::string("sha256/") + kAbcHex).has_value());
    BOOST_TEST(!parse_artifact_id(id.substr(0, id.size() - 1)).has_value());
    BOOST_TEST(!parse_artifact_id(id + "0").has_value());
    std::string upper = id;
    upper[upper.size() - 2] = 'A';
    BOOST_TEST(!parse_artifact_id(upper).has_value());
    std::string nonhex = id;
    nonhex.back() = 'g';
    BOOST_TEST(!parse_artifact_id(nonhex).has_value());
    BOOST_TEST(!parse_artifact_id("").has_value());
}

BOOST_AUTO_TEST_CASE(ingest_copy) {
    TempDir t;
    auto store = open_local_store(t.path / "store");
    write_file(t.path / "src.txt", "abc");
    auto m = store->ingest(t.path / "src.txt", "text/plain", "job-1", false);
    BOOST_TEST_REQUIRE(m.has_value(), (m ? "" : m.error()));
    BOOST_TEST(m->sha256_hex == kAbcHex);
    BOOST_TEST(m->id == make_artifact_id(kAbcHex));
    BOOST_TEST(m->size == 3u);
    BOOST_TEST(m->media_type == "text/plain");
    BOOST_TEST(m->producer_job == "job-1");
    BOOST_TEST(m->created_unix_ms > 0);
    BOOST_TEST(m->local_path == t.path / "store" / "sha256" / "ba" / kAbcHex);
    BOOST_TEST(fs::exists(t.path / "src.txt"));  // copy keeps source
    BOOST_TEST(read_file(m->local_path) == "abc");
    BOOST_TEST(mode_of(m->local_path) == 0444u);
    BOOST_TEST(store->contains(m->id));
    BOOST_TEST(*store->path_of(m->id) == m->local_path);
    BOOST_TEST(fs::is_empty(t.path / "store" / "tmp"));
}

BOOST_AUTO_TEST_CASE(ingest_move) {
    TempDir t;
    auto store = open_local_store(t.path / "store");
    write_file(t.path / "out.bin", "moved content");
    auto m = store->ingest(t.path / "out.bin", "", "", true);
    BOOST_TEST_REQUIRE(m.has_value(), (m ? "" : m.error()));
    BOOST_TEST(!fs::exists(t.path / "out.bin"));
    BOOST_TEST(m->media_type == "application/octet-stream");
    BOOST_TEST(read_file(m->local_path) == "moved content");
    BOOST_TEST(mode_of(m->local_path) == 0444u);
    BOOST_TEST(store->verify(m->id));
}

BOOST_AUTO_TEST_CASE(ingest_idempotent) {
    TempDir t;
    auto store = open_local_store(t.path / "store");
    write_file(t.path / "a", "same");
    write_file(t.path / "b", "same");
    auto m1 = store->ingest(t.path / "a", "", "", false);
    auto m2 = store->ingest(t.path / "a", "", "", false);
    auto m3 = store->ingest(t.path / "b", "", "", true);
    BOOST_TEST_REQUIRE(m1.has_value());
    BOOST_TEST_REQUIRE(m2.has_value());
    BOOST_TEST_REQUIRE(m3.has_value(), (m3 ? "" : m3.error()));
    BOOST_TEST(m1->id == m2->id);
    BOOST_TEST(m1->id == m3->id);
    BOOST_TEST(m1->local_path == m3->local_path);
    BOOST_TEST(!fs::exists(t.path / "b"));
    BOOST_TEST(fs::is_empty(t.path / "store" / "tmp"));
    std::size_t n = 0;
    for (auto& e : fs::recursive_directory_iterator(t.path / "store" / "sha256")) n += e.is_regular_file();
    BOOST_TEST(n == 1u);
}

BOOST_AUTO_TEST_CASE(ingest_errors) {
    TempDir t;
    auto store = open_local_store(t.path / "store");
    BOOST_TEST(!store->ingest(t.path / "nope", "", "", false).has_value());
    BOOST_TEST(!store->ingest(t.path / "nope", "", "", true).has_value());
    BOOST_TEST(!store->ingest(t.path, "", "", false).has_value());  // directory
}

BOOST_AUTO_TEST_CASE(empty_file) {
    TempDir t;
    auto store = open_local_store(t.path / "store");
    write_file(t.path / "empty", "");
    auto m = store->ingest(t.path / "empty", "", "", false);
    BOOST_TEST_REQUIRE(m.has_value());
    BOOST_TEST(m->sha256_hex == kEmptyHex);
    BOOST_TEST(m->size == 0u);
    BOOST_TEST(store->verify(m->id));
    BOOST_TEST(store->materialize(m->id, t.path / "w" / "e").has_value());
    BOOST_TEST(fs::file_size(t.path / "w" / "e") == 0u);
}

BOOST_AUTO_TEST_CASE(materialize_link_and_copy) {
    TempDir t;
    auto store = open_local_store(t.path / "store");
    write_file(t.path / "src", "payload");
    auto m = store->ingest(t.path / "src", "", "", false);
    BOOST_TEST_REQUIRE(m.has_value());

    // Same filesystem: hard link.
    const fs::path dest = t.path / "work" / "job" / "inputs" / m->sha256_hex;
    auto r = store->materialize(m->id, dest);
    BOOST_TEST_REQUIRE(r.has_value(), (r ? "" : r.error()));
    BOOST_TEST(read_file(dest) == "payload");
    BOOST_TEST(mode_of(dest) == 0444u);
    BOOST_TEST(fs::equivalent(dest, m->local_path));

    // Existing destination fails.
    BOOST_TEST(!store->materialize(m->id, dest).has_value());

    // Cross-device: /dev/shm is tmpfs (copy fallback), when it is a different fs.
    const fs::path shm = fs::path("/dev/shm") / tdr::new_id("artifacts-test");
    std::error_code ec;
    if (fs::is_directory("/dev/shm", ec)) {
        auto r2 = store->materialize(m->id, shm / "copy");
        BOOST_TEST_REQUIRE(r2.has_value(), (r2 ? "" : r2.error()));
        BOOST_TEST(read_file(shm / "copy") == "payload");
        BOOST_TEST(mode_of(shm / "copy") == 0444u);
        fs::remove_all(shm, ec);
    }

    // Missing artifact / bad id.
    BOOST_TEST(!store->materialize(make_artifact_id(kEmptyHex), t.path / "x").has_value());
    BOOST_TEST(!store->materialize("garbage", t.path / "y").has_value());
    BOOST_TEST(!store->contains("garbage"));
    BOOST_TEST(!store->path_of(make_artifact_id(kEmptyHex)).has_value());
}

BOOST_AUTO_TEST_CASE(ingest_move_cross_device) {
    std::error_code ec;
    const fs::path shm = fs::path("/dev/shm") / tdr::new_id("artifacts-test");
    if (!fs::create_directories(shm, ec) || ec) return;  // no /dev/shm here
    TempDir t;
    auto store = open_local_store(t.path / "store");
    write_file(shm / "out", "from tmpfs");
    auto m = store->ingest(shm / "out", "", "", true);
    BOOST_TEST_REQUIRE(m.has_value(), (m ? "" : m.error()));
    BOOST_TEST(!fs::exists(shm / "out"));
    BOOST_TEST(read_file(m->local_path) == "from tmpfs");
    BOOST_TEST(fs::is_empty(t.path / "store" / "tmp"));
    fs::remove_all(shm, ec);
}

BOOST_AUTO_TEST_CASE(verify_detects_corruption) {
    TempDir t;
    auto store = open_local_store(t.path / "store");
    write_file(t.path / "src", "original");
    auto m = store->ingest(t.path / "src", "", "", false);
    BOOST_TEST_REQUIRE(m.has_value());
    BOOST_TEST(store->verify(m->id));
    BOOST_TEST_REQUIRE(::chmod(m->local_path.c_str(), 0644) == 0);
    write_file(m->local_path, "tampered");
    BOOST_TEST(!store->verify(m->id));
    fs::remove(m->local_path);
    BOOST_TEST(!store->verify(m->id));
    BOOST_TEST(!store->contains(m->id));
    BOOST_TEST(!store->verify("garbage"));
}

BOOST_AUTO_TEST_CASE(open_clears_tmp) {
    TempDir t;
    {
        auto store = open_local_store(t.path / "store");
        BOOST_TEST(store->root() == t.path / "store");
        BOOST_TEST(fs::is_directory(t.path / "store" / "sha256"));
        BOOST_TEST(fs::is_directory(t.path / "store" / "tmp"));
        write_file(t.path / "src", "keep me");
        BOOST_TEST(store->ingest(t.path / "src", "", "", false).has_value());
    }
    write_file(t.path / "store" / "tmp" / "stale-partial", "half");
    write_file(t.path / "store" / "tmp" / "subdir" / "x", "y");
    auto store = open_local_store(t.path / "store");
    BOOST_TEST(fs::is_empty(t.path / "store" / "tmp"));
    BOOST_TEST(store->verify(make_artifact_id(*sha256_file(t.path / "src"))));
}

BOOST_AUTO_TEST_SUITE_END()
