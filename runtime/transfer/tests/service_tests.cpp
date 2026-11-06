// ArtifactService over real TLS + PeerManager on 127.0.0.1 (docs/design/phase5.md §9).
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include "tdr/artifacts/store.hpp"
#include "tdr/common/common.hpp"
#include "tdr/journal/journal.hpp"
#include "tdr/peer/manager.hpp"
#include "tdr/peer/transport.hpp"
#include "tdr/tls/tls.hpp"
#include "tdr/transfer/transfer.hpp"

namespace {

namespace fs = std::filesystem;
namespace net = boost::asio;
namespace cv1 = ::fireforge::control::v1;
using namespace std::chrono_literals;
using namespace tdr;
using Result = std::expected<artifacts::ArtifactMeta, std::string>;

constexpr std::size_t kMiB = 1u << 20;

struct Pki {
    fs::path dir = fs::path("/tmp/claude-1000") / tdr::new_id("xfer-test");
    fs::path ca = dir / "ca";
    Pki() {
        fs::create_directories(dir);
        BOOST_REQUIRE(tls::create_ca(ca, "ca-x"));
        for (const char* n : {"n1", "n2", "n3", "n4"}) {
            tls::IssueOptions o;
            o.role = tls::Role::kNode;
            o.name = n;
            auto r = tls::issue(ca, o, ca / (std::string(n) + ".key"), ca / (std::string(n) + ".crt"));
            BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
        }
    }
    ~Pki() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    std::shared_ptr<net::ssl::context> ctx(const std::string& name) const {
        return tls::make_peer_context({ca / "ca.crt", ca / (name + ".crt"), ca / (name + ".key")});
    }
};

const Pki& pki() {
    static Pki p;
    return p;
}

template <class Pred>
bool run_until(net::io_context& io, Pred pred, std::chrono::milliseconds timeout = 10s) {
    auto end = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > end) return false;
        io.restart();
        io.run_for(5ms);
    }
    return true;
}

void run_for(net::io_context& io, std::chrono::milliseconds d) {
    run_until(io, [] { return false; }, d);
}

std::string random_bytes(std::size_t n, unsigned seed) {
    std::mt19937_64 rng(seed);
    std::string s(n, '\0');
    for (std::size_t i = 0; i < n; i += 8) {
        auto v = rng();
        for (std::size_t k = 0; k < 8 && i + k < n; ++k) s[i + k] = static_cast<char>((v >> (8 * k)) & 0xff);
    }
    return s;
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void write_file(const fs::path& p, const std::string& data) {
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
}

using HolderMap = std::map<std::string, std::vector<std::string>>;

// One node: TLS transport, PeerManager, store, journal, ArtifactService.
struct Node {
    Node(net::io_context& io_, const fs::path& root, std::string name_, HolderMap& holders_,
         transfer::TransferOptions o = {})
        : io(io_), name(name_), id("ff-node-" + name_), dir(root / name_), holders(holders_), opt(o) {
        fs::create_directories(dir);
        peer::TransportOptions to;
        to.listen_address = "127.0.0.1";
        to.listen_port = 0;
        transport = std::make_unique<peer::TlsTransport>(io, pki().ctx(name), to);
        peer::PeerOptions po;
        po.self_node_id = id;
        po.self_name = name;
        po.daemon_version = "test";
        peers = std::make_unique<peer::PeerManager>(io, *transport, po);
        peers->start();
        transport->start();
        store = artifacts::open_local_store(dir / "store");
        journal = journal::open_sqlite_journal(dir / "journal.db");
        make_service();
    }
    ~Node() {
        svc.reset();
        peers->stop();
        transport->stop();
    }

    void make_service() {
        transfer::TransferHooks h;
        h.holders = [this](const std::string& aid,
                           std::function<void(std::expected<std::vector<std::string>, std::string>)> cb) {
            ++holder_queries;
            if (holders_error) {
                net::post(io, [cb, e = *holders_error] { cb(std::unexpected(e)); });
                return;
            }
            auto list = holders[aid];
            net::post(io, [cb, list] { cb(list); });
        };
        h.on_stored = [this](const artifacts::ArtifactMeta& m) { stored.push_back(m); };
        h.has_room = [this](std::uint64_t bytes) {
            room_asks.push_back(bytes);
            return room;
        };
        h.touch = [this](const std::string& aid) { touched.push_back(aid); };
        svc.reset();  // the old service's destructor clears the data acceptor
        svc = std::make_unique<transfer::ArtifactService>(io, *peers, *store, *journal, opt, h);
    }

    cv1::NodeInfo info() const {
        cv1::NodeInfo n;
        n.set_node_id(id);
        n.set_name(name);
        n.set_health(cv1::OFFLINE);  // no peer links: holder order stays as given
        auto* a = n.add_addresses();
        a->set_type("lan");
        a->set_address("127.0.0.1");
        a->set_port(transport->bound_port());
        return n;
    }

    // Ingest `data` into this node's store (+ journal row); returns the id.
    std::string put(const std::string& data, const std::string& media = "application/x-test") {
        auto src = dir / tdr::new_id("src");
        write_file(src, data);
        auto m = store->ingest(src, media, "job-1", true);
        BOOST_REQUIRE_MESSAGE(m, (m ? "" : m.error()));
        journal::ArtifactRow row;
        row.id = m->id;
        row.sha256_hex = m->sha256_hex;
        row.size = m->size;
        row.media_type = m->media_type;
        row.producer_job = m->producer_job;
        row.local_path = m->local_path.string();
        row.created_unix_ms = m->created_unix_ms;
        journal->record_artifact(row);
        return m->id;
    }

    Result fetch(const std::string& aid, std::chrono::milliseconds timeout = 20s) {
        std::optional<Result> r;
        svc->fetch(aid, [&](Result x) { r = std::move(x); });
        BOOST_TEST(!r.has_value());  // never inline
        BOOST_REQUIRE(run_until(io, [&] { return r.has_value(); }, timeout));
        return *r;
    }

    // Error string of a fetch ("<ok>" if it succeeded).
    std::string fetch_error(const std::string& aid) {
        auto r = fetch(aid);
        return r ? std::string("<ok>") : r.error();
    }

    fs::path part_of(const std::string& aid) const {
        return store->root() / "partial" / (*artifacts::parse_artifact_id(aid) + ".part");
    }

    net::io_context& io;
    std::string name, id;
    fs::path dir;
    HolderMap& holders;
    transfer::TransferOptions opt;
    std::unique_ptr<peer::TlsTransport> transport;
    std::unique_ptr<peer::PeerManager> peers;
    std::unique_ptr<artifacts::IArtifactStore> store;
    std::unique_ptr<journal::IJournal> journal;
    std::unique_ptr<transfer::ArtifactService> svc;

    std::optional<std::string> holders_error;
    bool room = true;
    int holder_queries = 0;
    std::vector<std::uint64_t> room_asks;
    std::vector<artifacts::ArtifactMeta> stored;
    std::vector<std::string> touched;
};

transfer::TransferOptions small_chunks(std::uint64_t rate = 0) {
    transfer::TransferOptions o;
    o.chunk_bytes = 256 * 1024;
    o.window = 4;
    o.max_serve_bytes_per_sec = rate;
    return o;
}

struct Fixture {
    net::io_context io;
    fs::path root = fs::path("/tmp/claude-1000") / tdr::new_id("xfer");
    HolderMap holders;
    std::vector<std::unique_ptr<Node>> nodes;

    Fixture() { fs::create_directories(root); }
    ~Fixture() {
        nodes.clear();
        run_for(io, 20ms);
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    // n1..nN with per-node options; directories cross-installed.
    void make(std::vector<transfer::TransferOptions> opts) {
        for (std::size_t i = 0; i < opts.size(); ++i)
            nodes.push_back(std::make_unique<Node>(io, root, "n" + std::to_string(i + 1), holders, opts[i]));
        cv1::NodeList list;
        for (auto& n : nodes) *list.add_nodes() = n->info();
        for (auto& n : nodes) n->peers->update_directory(list);
    }
    Node& n(int i) { return *nodes.at(static_cast<std::size_t>(i - 1)); }
};

}  // namespace

BOOST_AUTO_TEST_SUITE(artifact_service)

BOOST_FIXTURE_TEST_CASE(small_and_empty_artifacts, Fixture) {
    make({{}, {}});
    auto data = random_bytes(1000, 1);
    auto aid = n(2).put(data, "text/plain");
    auto empty = n(2).put("");
    holders[aid] = {n(1).id, n(2).id};  // self is skipped
    holders[empty] = {n(2).id};

    auto r = n(1).fetch(aid);
    BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
    BOOST_TEST(r->id == aid);
    BOOST_TEST(r->size == 1000u);
    BOOST_TEST(r->media_type == "text/plain");
    BOOST_TEST(read_file(r->local_path) == data);
    BOOST_TEST(n(1).store->contains(aid));
    auto row = n(1).journal->get_artifact(aid);
    BOOST_REQUIRE(row.has_value());
    BOOST_TEST(row->media_type == "text/plain");
    BOOST_TEST(row->producer_job == "");
    BOOST_REQUIRE(n(1).stored.size() == 1u);
    BOOST_TEST(n(1).stored[0].id == aid);
    BOOST_TEST(!fs::exists(n(1).part_of(aid)));
    BOOST_TEST(n(2).touched == std::vector<std::string>{aid}, boost::test_tools::per_element());
    BOOST_TEST(n(1).room_asks == std::vector<std::uint64_t>{1000}, boost::test_tools::per_element());

    auto e = n(1).fetch(empty);
    BOOST_REQUIRE_MESSAGE(e, (e ? "" : e.error()));
    BOOST_TEST(e->size == 0u);
    BOOST_TEST(e->media_type == "application/x-test");
    BOOST_TEST(fs::file_size(e->local_path) == 0u);

    auto s = n(1).svc->stats();
    BOOST_TEST(s.fetched == 2u);
    BOOST_TEST(s.bytes_received == 1000u);
    BOOST_TEST(s.resumed == 0u);
    BOOST_TEST(s.fetch_failures == 0u);
    BOOST_TEST(n(2).svc->stats().bytes_served == 1000u);

    // Already present: answered from the store, no transfer, no holder lookup.
    int queries = n(1).holder_queries;
    auto again = n(1).fetch(aid);
    BOOST_REQUIRE(again.has_value());
    BOOST_TEST(again->media_type == "text/plain");
    BOOST_TEST(again->local_path == r->local_path);
    BOOST_TEST(n(1).holder_queries == queries);
    BOOST_TEST(n(1).svc->stats().fetched == 2u);

    // Serves end once the fetcher closes.
    BOOST_TEST(run_until(io, [&] { return n(2).svc->stats().serving == 0; }));
}

BOOST_FIXTURE_TEST_CASE(multi_chunk_artifacts, Fixture) {
    make({small_chunks(), small_chunks(), {}});
    auto five = random_bytes(5 * kMiB + 123, 2);
    auto aid = n(2).put(five);
    holders[aid] = {n(2).id};
    auto r = n(1).fetch(aid);
    BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
    BOOST_TEST(r->size == five.size());
    BOOST_TEST(r->sha256_hex == *artifacts::sha256_file(r->local_path));
    BOOST_TEST(read_file(r->local_path) == five);

    // 32 MiB with the default 1 MiB chunks, window 8.
    auto big = random_bytes(32 * kMiB, 3);
    auto bid = n(3).put(big);
    holders[bid] = {n(3).id};
    auto b = n(1).fetch(bid, 60s);
    BOOST_REQUIRE_MESSAGE(b, (b ? "" : b.error()));
    BOOST_TEST(b->size == big.size());
    BOOST_TEST(*artifacts::sha256_file(b->local_path) == b->sha256_hex);
    BOOST_TEST(n(3).svc->stats().bytes_served == big.size());
    BOOST_TEST(!fs::exists(n(1).part_of(bid)));
}

BOOST_FIXTURE_TEST_CASE(resume_from_second_holder_after_link_cut, Fixture) {
    // n2 serves slowly; it goes away mid-transfer and n3 finishes from the .part.
    make({small_chunks(), small_chunks(4 * kMiB), small_chunks()});
    auto data = random_bytes(3 * kMiB, 4);
    auto aid = n(2).put(data);
    BOOST_REQUIRE(n(3).put(data) == aid);
    holders[aid] = {n(2).id, n(3).id};
    std::optional<Result> r;
    n(1).svc->fetch(aid, [&](Result x) { r = std::move(x); });
    BOOST_REQUIRE(run_until(io, [&] { return n(1).svc->stats().bytes_received >= kMiB; }));
    BOOST_TEST(n(1).svc->busy(aid));
    BOOST_TEST(n(2).svc->busy(aid));
    n(2).svc.reset();  // closes its serving links
    BOOST_REQUIRE(run_until(io, [&] { return r.has_value(); }));
    BOOST_REQUIRE_MESSAGE(*r, (*r ? "" : r->error()));
    BOOST_TEST(read_file((*r)->local_path) == data);
    auto s = n(1).svc->stats();
    BOOST_TEST(s.resumed == 1u);
    BOOST_TEST(s.fetched == 1u);
    auto served3 = n(3).svc->stats().bytes_served;
    BOOST_TEST(served3 > 0u);
    BOOST_TEST(served3 < data.size());
    BOOST_TEST(s.bytes_received >= data.size());
    BOOST_TEST(!n(1).svc->busy(aid));
    BOOST_TEST(!fs::exists(n(1).part_of(aid)));
}

BOOST_FIXTURE_TEST_CASE(resume_from_existing_part_and_oversized_part, Fixture) {
    make({small_chunks(), small_chunks()});
    auto data = random_bytes(kMiB + 77, 5);
    auto aid = n(2).put(data);
    holders[aid] = {n(2).id};
    fs::create_directories(n(1).part_of(aid).parent_path());
    write_file(n(1).part_of(aid), data.substr(0, 300000));
    auto r = n(1).fetch(aid);
    BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
    BOOST_TEST(read_file(r->local_path) == data);
    BOOST_TEST(n(1).svc->stats().resumed == 1u);
    BOOST_TEST(n(2).svc->stats().bytes_served == data.size() - 300000);

    // A .part larger than the artifact is discarded; same holder, from 0.
    auto other = random_bytes(5000, 6);
    auto oid = n(2).put(other);
    holders[oid] = {n(2).id};
    write_file(n(1).part_of(oid), random_bytes(9000, 7));
    auto o = n(1).fetch(oid);
    BOOST_REQUIRE_MESSAGE(o, (o ? "" : o.error()));
    BOOST_TEST(read_file(o->local_path) == other);
}

BOOST_FIXTURE_TEST_CASE(corrupted_source_fails_verification, Fixture) {
    make({small_chunks(), small_chunks(), small_chunks()});
    auto data = random_bytes(600000, 8);
    auto aid = n(2).put(data);
    BOOST_REQUIRE(n(3).put(data) == aid);
    // Corrupt n2's copy in place (same size).
    auto p = *n(2).store->path_of(aid);
    fs::permissions(p, fs::perms::owner_write, fs::perm_options::add);
    auto bad = data;
    bad[1234] = static_cast<char>(bad[1234] ^ 0x5a);
    write_file(p, bad);

    holders[aid] = {n(2).id};
    auto r = n(1).fetch(aid);
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST(r.error() == "n2: checksum mismatch");
    BOOST_TEST(!fs::exists(n(1).part_of(aid)));
    BOOST_TEST(!n(1).store->contains(aid));
    BOOST_TEST(n(1).svc->stats().fetch_failures == 1u);

    holders[aid] = {n(2).id, n(3).id};
    auto ok = n(1).fetch(aid);
    BOOST_REQUIRE_MESSAGE(ok, (ok ? "" : ok.error()));
    BOOST_TEST(read_file(ok->local_path) == data);
    BOOST_TEST(n(3).svc->stats().bytes_served == data.size());  // from 0, not resumed
    BOOST_TEST(n(1).svc->stats().resumed == 0u);
}

BOOST_FIXTURE_TEST_CASE(not_found_and_unreachable_move_to_next_holder, Fixture) {
    make({{}, {}, {}});
    auto data = random_bytes(4000, 9);
    auto aid = n(3).put(data);
    holders[aid] = {n(2).id};
    auto r = n(1).fetch(aid);
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST(r.error() == "n2: not found");

    holders[aid] = {"ff-node-ghost", n(2).id, n(3).id};
    auto ok = n(1).fetch(aid);
    BOOST_REQUIRE_MESSAGE(ok, (ok ? "" : ok.error()));
    BOOST_TEST(read_file(ok->local_path) == data);
}

BOOST_FIXTURE_TEST_CASE(busy_server_moves_to_next_holder, Fixture) {
    transfer::TransferOptions none;
    none.max_serving = 0;
    make({{}, none, {}});
    auto data = random_bytes(4000, 10);
    auto aid = n(2).put(data);
    BOOST_REQUIRE(n(3).put(data) == aid);
    holders[aid] = {n(2).id};
    auto r = n(1).fetch(aid);
    BOOST_REQUIRE(!r.has_value());
    BOOST_TEST(r.error() == "n2: busy");
    holders[aid] = {n(2).id, n(3).id};
    auto ok = n(1).fetch(aid);
    BOOST_REQUIRE_MESSAGE(ok, (ok ? "" : ok.error()));
    BOOST_TEST(n(2).svc->stats().bytes_served == 0u);
    BOOST_TEST(n(3).svc->stats().bytes_served == data.size());
}

BOOST_FIXTURE_TEST_CASE(busy_with_one_held_transfer, Fixture) {
    // n2 serves one slow transfer (max_serving 1); a second fetcher goes to n3.
    auto slow = small_chunks(2 * kMiB);
    slow.max_serving = 1;
    make({small_chunks(), slow, small_chunks(), small_chunks()});
    auto big = random_bytes(2 * kMiB, 11);
    auto bid = n(2).put(big);
    holders[bid] = {n(2).id};
    auto small = random_bytes(5000, 12);
    auto sid = n(2).put(small);
    BOOST_REQUIRE(n(3).put(small) == sid);
    holders[sid] = {n(2).id, n(3).id};

    std::optional<Result> held;
    n(1).svc->fetch(bid, [&](Result x) { held = std::move(x); });
    BOOST_REQUIRE(run_until(io, [&] { return n(2).svc->stats().serving == 1; }));
    auto r = n(4).fetch(sid);
    BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
    BOOST_TEST(n(3).svc->stats().bytes_served == small.size());
    BOOST_TEST(!held.has_value());
    BOOST_REQUIRE(run_until(io, [&] { return held.has_value(); }));
    BOOST_TEST(held->has_value());
}

BOOST_FIXTURE_TEST_CASE(concurrent_fetches_share_one_transfer, Fixture) {
    make({small_chunks(), small_chunks()});
    auto data = random_bytes(kMiB, 13);
    auto aid = n(2).put(data);
    holders[aid] = {n(2).id};
    std::vector<Result> got;
    n(1).svc->fetch(aid, [&](Result x) { got.push_back(std::move(x)); });
    n(1).svc->fetch(aid, [&](Result x) { got.push_back(std::move(x)); });
    BOOST_TEST(n(1).svc->busy(aid));
    BOOST_REQUIRE(run_until(io, [&] { return got.size() == 2; }));
    for (auto& g : got) BOOST_TEST(g.has_value());
    BOOST_TEST(n(1).holder_queries == 1);
    BOOST_TEST(n(1).svc->stats().fetched == 1u);
    BOOST_TEST(n(2).svc->stats().bytes_served == data.size());
    BOOST_TEST(n(1).stored.size() == 1u);
}

BOOST_FIXTURE_TEST_CASE(fetches_beyond_max_fetching_queue, Fixture) {
    auto one = small_chunks();
    one.max_fetching = 1;
    make({one, small_chunks()});
    std::vector<std::string> ids;
    for (unsigned i = 0; i < 3; ++i) {
        ids.push_back(n(2).put(random_bytes(300000, 20 + i)));
        holders[ids.back()] = {n(2).id};
    }
    int done = 0;
    for (auto& id : ids)
        n(1).svc->fetch(id, [&](Result x) {
            BOOST_TEST(x.has_value());
            ++done;
        });
    auto s = n(1).svc->stats();
    BOOST_TEST(s.fetching == 0u);
    BOOST_TEST(s.queued == 3u);
    BOOST_REQUIRE(run_until(io, [&] { return n(1).svc->stats().fetching == 1; }));
    BOOST_TEST(n(1).svc->stats().queued == 2u);
    BOOST_REQUIRE(run_until(io, [&] { return done == 3; }));
    BOOST_TEST(n(1).svc->stats().fetched == 3u);
}

BOOST_FIXTURE_TEST_CASE(rate_limit_is_roughly_honoured, Fixture) {
    make({small_chunks(), small_chunks(4 * kMiB)});
    auto data = random_bytes(2 * kMiB, 14);
    auto aid = n(2).put(data);
    holders[aid] = {n(2).id};
    auto t0 = std::chrono::steady_clock::now();
    auto r = n(1).fetch(aid);
    auto dt = std::chrono::steady_clock::now() - t0;
    BOOST_REQUIRE_MESSAGE(r, (r ? "" : r.error()));
    BOOST_TEST(dt >= 400ms);
    BOOST_TEST(dt < 3s);
}

BOOST_FIXTURE_TEST_CASE(errors_before_any_transfer, Fixture) {
    make({{}, {}});
    auto aid = n(2).put(random_bytes(10000, 15));

    holders[aid] = {n(2).id};
    n(1).room = false;
    auto full = n(1).fetch(aid);
    BOOST_REQUIRE(!full.has_value());
    BOOST_TEST(full.error() == "store full");
    BOOST_TEST(!n(1).store->contains(aid));
    n(1).room = true;

    holders[aid] = {};
    BOOST_TEST(n(1).fetch_error(aid) == "no holder");
    holders[aid] = {n(1).id};
    BOOST_TEST(n(1).fetch_error(aid) == "no holder");

    n(1).holders_error = "control plane down";
    BOOST_TEST(n(1).fetch_error(aid) == "holders: control plane down");
    n(1).holders_error.reset();

    BOOST_TEST(n(1).fetch_error("artifact://sha256/nothex").find("invalid artifact id") == 0u);
    BOOST_TEST(n(1).svc->stats().fetched == 0u);
}

BOOST_FIXTURE_TEST_CASE(stop_and_destroy_with_transfers_in_flight, Fixture) {
    make({small_chunks(), small_chunks(2 * kMiB), small_chunks()});
    auto a = n(2).put(random_bytes(2 * kMiB, 16));
    auto b = n(2).put(random_bytes(2 * kMiB, 17));
    holders[a] = {n(2).id};
    holders[b] = {n(2).id};

    // stop(): pending fetches fail "transfer stopped", .part kept.
    std::vector<Result> got;
    n(1).svc->fetch(a, [&](Result x) { got.push_back(std::move(x)); });
    n(1).svc->fetch(a, [&](Result x) { got.push_back(std::move(x)); });
    BOOST_REQUIRE(run_until(io, [&] { return n(1).svc->stats().bytes_received > 0; }));
    n(1).svc->stop();
    BOOST_TEST(got.empty());  // posted
    BOOST_REQUIRE(run_until(io, [&] { return got.size() == 2; }));
    for (auto& g : got) {
        BOOST_REQUIRE(!g.has_value());
        BOOST_TEST(g.error() == "transfer stopped");
    }
    BOOST_TEST(fs::exists(n(1).part_of(a)));
    BOOST_TEST(n(1).fetch_error(a) == "transfer stopped");
    BOOST_TEST(run_until(io, [&] { return n(2).svc->stats().serving == 0; }));

    // Destroy: no callback afterwards; the serving side copes.
    bool called = false;
    n(3).svc->fetch(b, [&](Result) { called = true; });
    BOOST_REQUIRE(run_until(io, [&] { return n(3).svc->stats().bytes_received > 0; }));
    n(3).svc.reset();
    run_for(io, 100ms);
    BOOST_TEST(!called);
    BOOST_TEST(run_until(io, [&] { return n(2).svc->stats().serving == 0; }));

    // And the server destroyed while serving (fetcher n1 restarted).
    n(1).make_service();
    std::optional<Result> r;
    n(1).svc->fetch(b, [&](Result x) { r = std::move(x); });
    BOOST_REQUIRE(run_until(io, [&] { return n(1).svc->stats().bytes_received > 0; }));
    n(2).svc.reset();
    BOOST_REQUIRE(run_until(io, [&] { return r.has_value(); }));
    BOOST_REQUIRE(!r->has_value());
    BOOST_TEST(r->error().find("n2: link closed") == 0u);
    BOOST_TEST(fs::exists(n(1).part_of(b)));
}

BOOST_AUTO_TEST_SUITE_END()
