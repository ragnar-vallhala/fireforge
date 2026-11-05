#include <boost/test/unit_test.hpp>

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <map>
#include <filesystem>
#include <future>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

#include "tdr/ipc/ipc.hpp"

namespace fs = std::filesystem;
namespace asio = boost::asio;
using namespace std::chrono_literals;
using tdr::ipc::Connection;
using tdr::ipc::Handlers;
using tdr::ipc::UnixClient;
using tdr::ipc::UnixServer;
using tdr::protocol::Frame;

namespace {

fs::path make_temp_dir() {
    fs::path base = "/tmp/claude-1000";
    fs::create_directories(base);
    std::string tmpl = (base / "ipcXXXXXX").string();
    char* d = ::mkdtemp(tmpl.data());
    BOOST_REQUIRE(d != nullptr);
    return fs::path(d);
}

Frame text_frame(std::uint64_t id, std::string payload, std::uint64_t corr = 0, std::uint16_t type = 0x1003) {
    Frame f;
    f.header.message_type = type;
    f.header.message_id = id;
    f.header.correlation_id = corr;
    f.payload = std::move(payload);
    f.header.payload_length = static_cast<std::uint32_t>(f.payload.size());
    return f;
}

// Server on its own io thread. Handlers run on that thread; the test thread
// observes them through a mutex/condition variable.
struct Fixture {
    fs::path dir = make_temp_dir();
    fs::path sock = dir / "sub" / "t.sock";
    asio::io_context io;
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>> guard{asio::make_work_guard(io)};
    std::unique_ptr<UnixServer> server;
    std::thread thread;

    std::mutex mu;
    std::condition_variable cv;
    std::vector<std::shared_ptr<Connection>> opened;  // first frame seen
    std::vector<std::uint64_t> closed_ids;
    std::map<std::uint64_t, int> close_counts;
    std::vector<std::pair<std::optional<pid_t>, std::optional<uid_t>>> creds;

    // Default handler: echo payload with correlation, reply type ERROR.
    std::function<void(const std::shared_ptr<Connection>&, Frame)> on_frame = [](const auto& c, Frame f) {
        c->send(text_frame(1000 + f.header.message_id, "echo:" + f.payload, f.header.message_id));
    };

    void start(unsigned mode = 0660) {
        Handlers h;
        h.on_frame = [this](const std::shared_ptr<Connection>& c, Frame f) {
            {
                std::lock_guard lk(mu);
                bool known = false;
                for (auto& o : opened) known |= o.get() == c.get();
                if (!known) {
                    opened.push_back(c);
                    creds.emplace_back(c->peer_pid(), c->peer_uid());
                }
            }
            on_frame(c, std::move(f));
            cv.notify_all();
        };
        h.on_close = [this](const std::shared_ptr<Connection>& c) {
            BOOST_CHECK(!c->is_open());
            {
                std::lock_guard lk(mu);
                closed_ids.push_back(c->id());
                close_counts[c->id()]++;
            }
            cv.notify_all();
        };
        server = std::make_unique<UnixServer>(io, sock, std::move(h), mode);
        server->start();
        thread = std::thread([this] { io.run(); });
    }

    template <class Pred>
    bool wait(Pred p, std::chrono::milliseconds t = 5s) {
        std::unique_lock lk(mu);
        return cv.wait_for(lk, t, p);
    }

    // Run fn on the io thread and wait for its result.
    template <class Fn>
    auto on_io(Fn fn) {
        std::packaged_task<decltype(fn())()> task(std::move(fn));
        auto fut = task.get_future();
        asio::post(io, [&task] { task(); });
        return fut.get();
    }

    void shutdown() {
        if (thread.joinable()) {
            on_io([this] {
                server->stop();
                return 0;
            });
            guard.reset();
            thread.join();
        }
        server.reset();
    }

    ~Fixture() {
        shutdown();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
};

}  // namespace

BOOST_FIXTURE_TEST_SUITE(ipc_server_client, Fixture)

BOOST_AUTO_TEST_CASE(echo_request_reply) {
    start();
    BOOST_CHECK(fs::exists(sock));
    UnixClient c(sock);
    Frame reply = c.request(text_frame(42, "hi"), 5s);
    BOOST_CHECK_EQUAL(reply.header.correlation_id, 42u);
    BOOST_CHECK_EQUAL(reply.payload, "echo:hi");
    Frame reply2 = c.request(text_frame(43, "again"), 5s);
    BOOST_CHECK_EQUAL(reply2.payload, "echo:again");
}

BOOST_AUTO_TEST_CASE(request_discards_uncorrelated_frames) {
    on_frame = [](const auto& c, Frame f) {
        c->send(text_frame(1, "noise", 0));
        c->send(text_frame(2, "other", f.header.message_id + 1));
        c->send(text_frame(3, "mine", f.header.message_id));
    };
    start();
    UnixClient c(sock);
    Frame r = c.request(text_frame(7, "q"), 5s);
    BOOST_CHECK_EQUAL(r.payload, "mine");
}

BOOST_AUTO_TEST_CASE(socket_mode_applied) {
    start(0600);
    struct stat st{};
    BOOST_REQUIRE_EQUAL(::stat(sock.c_str(), &st), 0);
    BOOST_CHECK(S_ISSOCK(st.st_mode));
    BOOST_CHECK_EQUAL(st.st_mode & 0777, 0600u);
}

BOOST_AUTO_TEST_CASE(several_clients) {
    start();
    std::vector<std::unique_ptr<UnixClient>> clients;
    for (int i = 0; i < 5; ++i) clients.push_back(std::make_unique<UnixClient>(sock));
    // Interleave requests across clients.
    for (int round = 0; round < 3; ++round) {
        for (std::size_t i = 0; i < clients.size(); ++i) {
            std::uint64_t id = 100 * (round + 1) + i;
            std::string p = "c" + std::to_string(i) + "r" + std::to_string(round);
            Frame r = clients[i]->request(text_frame(id, p), 5s);
            BOOST_CHECK_EQUAL(r.header.correlation_id, id);
            BOOST_CHECK_EQUAL(r.payload, "echo:" + p);
        }
    }
    BOOST_CHECK_EQUAL(on_io([this] { return server->connection_count(); }), 5u);
    std::set<std::uint64_t> ids;
    {
        std::lock_guard lk(mu);
        for (auto& o : opened) ids.insert(o->id());
    }
    BOOST_CHECK_EQUAL(ids.size(), 5u);
    BOOST_CHECK(ids.count(1) == 1);  // ids start at 1
}

BOOST_AUTO_TEST_CASE(server_initiated_send_in_order) {
    on_frame = [](const auto&, Frame) {};
    start();
    UnixClient c(sock);
    c.send(text_frame(1, "hello"));
    BOOST_REQUIRE(wait([this] { return opened.size() == 1; }));
    std::shared_ptr<Connection> conn;
    {
        std::lock_guard lk(mu);
        conn = opened[0];
    }
    on_io([conn] {
        for (std::uint64_t i = 1; i <= 50; ++i) conn->send(text_frame(i, std::string(i * 1000, 'p')));
        return 0;
    });
    for (std::uint64_t i = 1; i <= 50; ++i) {
        auto f = c.receive(5s);
        BOOST_REQUIRE(f.has_value());
        BOOST_CHECK_EQUAL(f->header.message_id, i);
        BOOST_CHECK_EQUAL(f->payload.size(), i * 1000);
    }
}

BOOST_AUTO_TEST_CASE(peer_credentials) {
    start();
    UnixClient c(sock);
    c.request(text_frame(1, "x"), 5s);
    std::lock_guard lk(mu);
    BOOST_REQUIRE_EQUAL(creds.size(), 1u);
    BOOST_REQUIRE(creds[0].first.has_value());
    BOOST_CHECK_EQUAL(*creds[0].first, ::getpid());
    BOOST_REQUIRE(creds[0].second.has_value());
    BOOST_CHECK_EQUAL(*creds[0].second, ::getuid());
}

BOOST_AUTO_TEST_CASE(on_close_fires_once_on_disconnect) {
    start();
    {
        UnixClient c(sock);
        c.request(text_frame(1, "x"), 5s);
    }
    BOOST_REQUIRE(wait([this] { return closed_ids.size() == 1; }));
    BOOST_CHECK_EQUAL(on_io([this] { return server->connection_count(); }), 0u);
    std::shared_ptr<Connection> conn;
    {
        std::lock_guard lk(mu);
        conn = opened.at(0);
    }
    // Closing again (and sending) is a no-op and does not re-fire on_close.
    on_io([conn] {
        conn->close();
        conn->send(text_frame(9, "late"));
        return 0;
    });
    std::lock_guard lk(mu);
    BOOST_CHECK_EQUAL(close_counts[conn->id()], 1);
    BOOST_CHECK(!conn->is_open());
}

BOOST_AUTO_TEST_CASE(server_close_from_handler) {
    on_frame = [](const auto& c, Frame f) {
        if (f.payload == "bye") c->close();
        else c->send(text_frame(5, "ok", f.header.message_id));
    };
    start();
    UnixClient c(sock);
    BOOST_CHECK_EQUAL(c.request(text_frame(1, "x"), 5s).payload, "ok");
    c.send(text_frame(2, "bye"));
    BOOST_CHECK(!c.receive(5s).has_value());  // EOF
    BOOST_REQUIRE(wait([this] { return closed_ids.size() == 1; }));
}

BOOST_AUTO_TEST_CASE(garbage_client_disconnected_others_unaffected) {
    start();
    UnixClient good(sock);
    BOOST_CHECK_EQUAL(good.request(text_frame(1, "a"), 5s).payload, "echo:a");

    UnixClient bad(sock);
    std::string junk(100, 'Z');
    BOOST_REQUIRE_EQUAL(::write(bad.fd(), junk.data(), junk.size()), static_cast<ssize_t>(junk.size()));
    // Server closes the bad connection: client sees EOF (or a reset).
    try {
        BOOST_CHECK(!bad.receive(5s).has_value());
    } catch (const std::system_error&) {
    }
    BOOST_REQUIRE(wait([this] { return closed_ids.size() == 1; }));

    BOOST_CHECK_EQUAL(good.request(text_frame(2, "b"), 5s).payload, "echo:b");
    BOOST_CHECK_EQUAL(on_io([this] { return server->connection_count(); }), 1u);
}

BOOST_AUTO_TEST_CASE(frame_split_across_writes) {
    start();
    UnixClient c(sock);
    std::string bytes = tdr::protocol::encode_frame(text_frame(77, "split"));
    for (char ch : bytes) BOOST_REQUIRE_EQUAL(::write(c.fd(), &ch, 1), 1);
    auto r = c.receive(5s);
    BOOST_REQUIRE(r.has_value());
    BOOST_CHECK_EQUAL(r->header.correlation_id, 77u);
    BOOST_CHECK_EQUAL(r->payload, "echo:split");
}

BOOST_AUTO_TEST_CASE(receive_timeout) {
    on_frame = [](const auto&, Frame) {};
    start();
    UnixClient c(sock);
    auto t0 = std::chrono::steady_clock::now();
    BOOST_CHECK(!c.receive(50ms).has_value());
    BOOST_CHECK(std::chrono::steady_clock::now() - t0 >= 50ms);
    BOOST_CHECK_THROW(c.request(text_frame(1, "x"), 50ms), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(stale_socket_replaced) {
    fs::create_directories(sock.parent_path());
    {
        // Leave a bound-but-dead socket file behind.
        int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::strcpy(addr.sun_path, sock.c_str());
        BOOST_REQUIRE_EQUAL(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        ::close(fd);
    }
    BOOST_REQUIRE(fs::exists(sock));
    BOOST_CHECK_THROW(UnixClient{sock}, std::system_error);  // dead
    start();
    UnixClient c(sock);
    BOOST_CHECK_EQUAL(c.request(text_frame(1, "x"), 5s).payload, "echo:x");
}

BOOST_AUTO_TEST_CASE(live_socket_not_stolen) {
    start();
    asio::io_context io2;
    UnixServer second(io2, sock, Handlers{});
    BOOST_CHECK_THROW(second.start(), std::system_error);
    UnixClient c(sock);
    BOOST_CHECK_EQUAL(c.request(text_frame(1, "x"), 5s).payload, "echo:x");
}

BOOST_AUTO_TEST_CASE(stop_closes_everything) {
    start();
    UnixClient a(sock), b(sock);
    a.request(text_frame(1, "x"), 5s);
    b.request(text_frame(2, "y"), 5s);
    on_io([this] {
        server->stop();
        return 0;
    });
    BOOST_REQUIRE(wait([this] { return closed_ids.size() == 2; }));
    BOOST_CHECK(!a.receive(5s).has_value());
    BOOST_CHECK(!b.receive(5s).has_value());
    BOOST_CHECK_EQUAL(on_io([this] { return server->connection_count(); }), 0u);
    BOOST_CHECK_THROW(UnixClient{sock}, std::system_error);  // listener gone
    std::lock_guard lk(mu);
    for (auto& [id, n] : close_counts) BOOST_CHECK_EQUAL(n, 1);
}

BOOST_AUTO_TEST_CASE(destructor_unlinks_socket) {
    start();
    BOOST_CHECK(fs::exists(sock));
    shutdown();
    BOOST_CHECK(!fs::exists(sock));
}

BOOST_AUTO_TEST_CASE(connect_missing_socket_throws) {
    BOOST_CHECK_THROW(UnixClient{dir / "nope.sock"}, std::system_error);
}

BOOST_AUTO_TEST_SUITE_END()
