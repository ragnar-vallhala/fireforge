// StoreGc against a real local store in a temp dir and a fake release hook
// (replies via asio::post, like the control client).
#include <boost/asio/post.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <deque>
#include <fstream>
#include <map>
#include <optional>
#include <set>

#include "tdr/common/common.hpp"
#include "tdr/transfer/gc.hpp"

namespace asio = boost::asio;
namespace fs = std::filesystem;
namespace tv1 = ::fireforge::tdr::v1;
namespace cv1 = ::fireforge::control::v1;
using namespace std::chrono_literals;
using tdr::transfer::GcHooks;
using tdr::transfer::GcOptions;
using tdr::transfer::StoreGc;

namespace {

using ReleaseResult = std::expected<cv1::ReleaseReply, std::string>;
using ReleaseCb = std::function<void(ReleaseResult)>;

ReleaseResult released() {
    cv1::ReleaseReply r;
    r.set_released(true);
    return r;
}
ReleaseResult refused(const std::string& reason) {
    cv1::ReleaseReply r;
    r.set_released(false);
    r.set_reason(reason);
    return r;
}

struct Rig {
    fs::path dir = fs::temp_directory_path() / tdr::new_id("gc-test");
    asio::io_context io;
    std::unique_ptr<tdr::artifacts::IArtifactStore> store;
    std::set<std::string> protected_ids;
    std::vector<std::string> release_calls;
    std::deque<std::pair<std::string, ReleaseCb>> pending;
    // Answer per id (default: released). nullopt-returning -> left pending.
    std::function<std::optional<ReleaseResult>(const std::string&)> auto_reply =
        [](const std::string&) -> std::optional<ReleaseResult> { return released(); };
    std::unique_ptr<StoreGc> gc;
    int counter = 0;

    Rig() {
        fs::create_directories(dir);
        store = tdr::artifacts::open_local_store(dir / "store");
    }
    ~Rig() {
        gc.reset();
        io.restart();
        io.poll();
        std::error_code ec;
        // Store files are 0444 in writable dirs: remove_all works.
        fs::remove_all(dir, ec);
    }

    GcHooks hooks() {
        GcHooks h;
        h.is_protected = [this](const std::string& id) { return protected_ids.contains(id); };
        h.release = [this](const std::string& id, ReleaseCb cb) {
            release_calls.push_back(id);
            auto r = auto_reply ? auto_reply(id) : std::nullopt;
            if (!r) {
                pending.emplace_back(id, std::move(cb));
                return;
            }
            asio::post(io, [cb = std::move(cb), r = std::move(*r)]() mutable { cb(std::move(r)); });
        };
        return h;
    }

    void make(std::uint64_t max_bytes, std::chrono::milliseconds min_age = 0ms,
              std::chrono::milliseconds interval = 60000ms) {
        GcOptions o;
        o.max_bytes = max_bytes;
        o.min_age = min_age;
        o.interval = interval;
        gc = std::make_unique<StoreGc>(io, *store, o, hooks());
    }

    // Ingests `size` unique bytes whose mtime is `age` in the past.
    std::string add(std::size_t size, std::chrono::seconds age) {
        const fs::path f = dir / tdr::new_id("src");
        std::string data = std::to_string(counter++) + ":";
        data.resize(size, 'x');
        std::ofstream(f, std::ios::binary) << data;
        auto m = store->ingest(f, "application/octet-stream", "", true);
        BOOST_REQUIRE(m.has_value());
        fs::last_write_time(m->local_path, fs::file_time_type::clock::now() - age);
        return m->id;
    }

    tv1::GcReport pass(bool force = false) {
        std::optional<tv1::GcReport> rep;
        gc->run([&](const tv1::GcReport& r) { rep = r; }, force);
        BOOST_REQUIRE(run_until([&] { return rep.has_value(); }));
        return *rep;
    }

    template <class Pred>
    bool run_until(Pred pred, std::chrono::milliseconds timeout = 5000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!pred()) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            if (io.stopped()) io.restart();
            io.run_one_for(10ms);
        }
        return true;
    }
    void run_for(std::chrono::milliseconds d) {
        const auto deadline = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < deadline) {
            if (io.stopped()) io.restart();
            io.run_one_for(10ms);
        }
    }
    void answer(ReleaseResult r) {
        BOOST_REQUIRE(!pending.empty());
        auto cb = std::move(pending.front().second);
        pending.pop_front();
        asio::post(io, [cb = std::move(cb), r = std::move(r)]() mutable { cb(std::move(r)); });
    }

    // Ten 100-byte artifacts, ids[0] the oldest (age 100 s ... 10 s).
    std::vector<std::string> ten() {
        std::vector<std::string> ids;
        for (int i = 0; i < 10; ++i) ids.push_back(add(100, std::chrono::seconds(100 - 10 * i)));
        return ids;
    }
};

std::vector<std::string> evicted_of(const tv1::GcReport& r) { return {r.evicted().begin(), r.evicted().end()}; }

}  // namespace

BOOST_AUTO_TEST_SUITE(store_gc)

BOOST_AUTO_TEST_CASE(no_cap_never_evicts) {
    Rig rig;
    const auto ids = rig.ten();
    rig.make(0);
    BOOST_TEST(rig.gc->store_bytes() == 1000u);
    BOOST_TEST(rig.gc->has_room(1ull << 50));
    const auto r = rig.pass(true);
    BOOST_TEST(r.evicted_size() == 0);
    BOOST_TEST(r.store_bytes() == 1000u);
    BOOST_TEST(r.max_bytes() == 0u);
    BOOST_TEST(rig.release_calls.empty());
}

BOOST_AUTO_TEST_CASE(below_high_does_nothing) {
    Rig rig;
    for (int i = 0; i < 9; ++i) rig.add(100, 60s);
    rig.make(1000);  // 900 = high: not above
    const auto r = rig.pass();
    BOOST_TEST(r.evicted_size() == 0);
    BOOST_TEST(r.store_bytes() == 900u);
    BOOST_TEST(r.max_bytes() == 1000u);
    BOOST_TEST(rig.release_calls.empty());
}

BOOST_AUTO_TEST_CASE(above_high_evicts_oldest_first_down_to_low) {
    Rig rig;
    const auto ids = rig.ten();
    rig.make(1000);
    const auto r = rig.pass();
    BOOST_TEST(evicted_of(r) == (std::vector<std::string>{ids[0], ids[1]}), boost::test_tools::per_element());
    BOOST_TEST(rig.release_calls == (std::vector<std::string>{ids[0], ids[1]}), boost::test_tools::per_element());
    BOOST_TEST(r.freed_bytes() == 200u);
    BOOST_TEST(r.store_bytes() == 800u);
    BOOST_TEST(rig.gc->store_bytes() == 800u);
    BOOST_TEST(!rig.store->contains(ids[0]));
    BOOST_TEST(!rig.store->contains(ids[1]));
    BOOST_TEST(rig.store->contains(ids[2]));
    // Below high now: the next pass does nothing.
    rig.release_calls.clear();
    BOOST_TEST(rig.pass().evicted_size() == 0);
    BOOST_TEST(rig.release_calls.empty());
}

BOOST_AUTO_TEST_CASE(protected_and_young_are_skipped) {
    Rig rig;
    const auto a = rig.add(100, 1000s);  // protected
    const auto b = rig.add(100, 900s);   // protected
    const auto c = rig.add(100, 700s);   // the only evictable one
    std::vector<std::string> young;
    for (int i = 0; i < 7; ++i) young.push_back(rig.add(100, std::chrono::seconds(5 + i)));
    rig.protected_ids = {a, b};
    rig.make(1000, 600000ms);
    const auto r = rig.pass();
    BOOST_TEST(evicted_of(r) == std::vector<std::string>{c}, boost::test_tools::per_element());
    BOOST_TEST(rig.release_calls == std::vector<std::string>{c}, boost::test_tools::per_element());
    BOOST_TEST(r.kept_protected() == 9u);
    BOOST_TEST(r.store_bytes() == 900u);
    BOOST_TEST(rig.store->contains(a));
    BOOST_TEST(rig.store->contains(b));
}

BOOST_AUTO_TEST_CASE(refused_or_failed_release_keeps_the_file) {
    Rig rig;
    const auto ids = rig.ten();
    rig.auto_reply = [&](const std::string& id) -> std::optional<ReleaseResult> {
        if (id == ids[0]) return refused("last copy");
        if (id == ids[1]) return refused("pinned");
        if (id == ids[2]) return ReleaseResult(std::unexpected(std::string("HTTP 503: down")));
        return released();
    };
    rig.make(1000);
    const auto r = rig.pass();
    BOOST_TEST(evicted_of(r) == (std::vector<std::string>{ids[3], ids[4]}), boost::test_tools::per_element());
    BOOST_TEST(r.kept_last_copy() == 2u);  // last copy + error
    BOOST_TEST(r.kept_pinned() == 1u);
    BOOST_TEST(r.store_bytes() == 800u);
    for (int i = 0; i < 3; ++i) BOOST_TEST(rig.store->contains(ids[i]));
    BOOST_TEST(rig.store->verify(ids[0]));
}

BOOST_AUTO_TEST_CASE(nothing_releasable_ends_the_pass) {
    Rig rig;
    const auto ids = rig.ten();
    rig.auto_reply = [](const std::string&) -> std::optional<ReleaseResult> { return refused("last copy"); };
    rig.make(1000);
    const auto r = rig.pass();
    BOOST_TEST(r.evicted_size() == 0);
    BOOST_TEST(r.kept_last_copy() == 10u);
    BOOST_TEST(r.store_bytes() == 1000u);
    BOOST_TEST(!rig.gc->has_room(1));
}

BOOST_AUTO_TEST_CASE(force_to_low_evicts_below_high) {
    Rig rig;
    const auto old = rig.add(100, 100s);
    for (int i = 0; i < 7; ++i) rig.add(100, 50s);
    rig.add(50, 40s);  // 850: above low, below high
    rig.make(1000);
    BOOST_TEST(rig.pass().evicted_size() == 0);
    const auto r = rig.pass(true);
    BOOST_TEST(evicted_of(r) == std::vector<std::string>{old}, boost::test_tools::per_element());
    BOOST_TEST(r.store_bytes() == 750u);
}

BOOST_AUTO_TEST_CASE(touch_changes_lru_order) {
    Rig rig;
    const auto ids = rig.ten();
    rig.make(1000);
    rig.gc->touch(ids[0]);
    rig.gc->touch("artifact://sha256/" + std::string(64, '0'));  // absent: no-op
    const auto r = rig.pass();
    BOOST_TEST(evicted_of(r) == (std::vector<std::string>{ids[1], ids[2]}), boost::test_tools::per_element());
    BOOST_TEST(rig.store->contains(ids[0]));
}

BOOST_AUTO_TEST_CASE(concurrent_runs_are_coalesced) {
    Rig rig;
    const auto ids = rig.ten();
    rig.auto_reply = nullptr;  // answered by hand
    rig.make(1000);
    std::vector<tv1::GcReport> reps;
    rig.gc->run([&](const tv1::GcReport& r) { reps.push_back(r); });
    rig.gc->run([&](const tv1::GcReport& r) { reps.push_back(r); }, true);
    BOOST_REQUIRE(rig.pending.size() == 1u);
    rig.answer(released());
    BOOST_REQUIRE(rig.run_until([&] { return rig.pending.size() == 1u; }));
    rig.gc->run([&](const tv1::GcReport& r) { reps.push_back(r); });
    rig.answer(released());
    BOOST_REQUIRE(rig.run_until([&] { return reps.size() == 3u; }));
    BOOST_TEST(rig.release_calls.size() == 2u);
    for (const auto& r : reps) {
        BOOST_TEST(r.evicted_size() == 2);
        BOOST_TEST(r.store_bytes() == 800u);
    }
}

BOOST_AUTO_TEST_CASE(stop_finishes_the_release_in_flight) {
    Rig rig;
    const auto ids = rig.ten();
    rig.auto_reply = nullptr;
    rig.make(1000);
    std::optional<tv1::GcReport> rep;
    rig.gc->run([&](const tv1::GcReport& r) { rep = r; });
    BOOST_REQUIRE(rig.pending.size() == 1u);
    rig.gc->stop();
    rig.answer(released());  // the control plane dropped our copy: delete it
    BOOST_REQUIRE(rig.run_until([&] { return rep.has_value(); }));
    BOOST_TEST(evicted_of(*rep) == std::vector<std::string>{ids[0]}, boost::test_tools::per_element());
    BOOST_TEST(!rig.store->contains(ids[0]));
    BOOST_TEST(rig.release_calls.size() == 1u);
    BOOST_TEST(rig.pending.empty());
}

BOOST_AUTO_TEST_CASE(released_but_now_protected_is_kept) {
    Rig rig;
    const auto ids = rig.ten();
    rig.auto_reply = nullptr;
    rig.make(1000);
    std::optional<tv1::GcReport> rep;
    rig.gc->run([&](const tv1::GcReport& r) { rep = r; });
    BOOST_REQUIRE(rig.pending.size() == 1u);
    rig.protected_ids.insert(ids[0]);  // a job started using it meanwhile
    rig.answer(released());
    BOOST_REQUIRE(rig.run_until([&] { return rig.pending.size() == 1u; }));
    rig.answer(released());
    BOOST_REQUIRE(rig.run_until([&] { return rig.pending.size() == 1u; }));
    rig.answer(released());
    BOOST_REQUIRE(rig.run_until([&] { return rep.has_value(); }));
    BOOST_TEST(rig.store->contains(ids[0]));
    BOOST_TEST(evicted_of(*rep) == (std::vector<std::string>{ids[1], ids[2]}), boost::test_tools::per_element());
    BOOST_TEST(rep->kept_protected() == 1u);
}

BOOST_AUTO_TEST_CASE(destroy_mid_pass_is_safe) {
    Rig rig;
    const auto ids = rig.ten();
    rig.auto_reply = nullptr;
    rig.make(1000);
    bool called = false;
    rig.gc->run([&](const tv1::GcReport&) { called = true; });
    BOOST_REQUIRE(rig.pending.size() == 1u);
    rig.gc.reset();
    rig.answer(released());
    rig.run_for(50ms);
    BOOST_TEST(!called);
    BOOST_TEST(rig.store->contains(ids[0]));
}

BOOST_AUTO_TEST_CASE(periodic_runs) {
    Rig rig;
    const auto ids = rig.ten();
    rig.make(1000, 0ms, 30ms);
    rig.gc->start();
    BOOST_REQUIRE(rig.run_until([&] { return !rig.store->contains(ids[1]); }));
    BOOST_TEST(!rig.store->contains(ids[0]));
    BOOST_TEST(rig.store->contains(ids[2]));
    rig.gc->stop();
}

BOOST_AUTO_TEST_CASE(has_room_and_note_added) {
    Rig rig;
    for (int i = 0; i < 5; ++i) rig.add(100, 60s);
    rig.make(1000);
    BOOST_TEST(rig.gc->store_bytes() == 500u);
    BOOST_TEST(rig.gc->has_room(500));
    BOOST_TEST(!rig.gc->has_room(501));
    rig.gc->note_added(100);
    BOOST_TEST(rig.gc->store_bytes() == 600u);
    BOOST_TEST(!rig.gc->has_room(401));
    BOOST_TEST(rig.gc->options().max_bytes == 1000u);
}

BOOST_AUTO_TEST_SUITE_END()
