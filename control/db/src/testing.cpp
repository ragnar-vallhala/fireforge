// Throwaway PostgreSQL clusters for tests.
#include "tdr/cpdb/testing.hpp"

#include <fcntl.h>
#include <pwd.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <vector>

#include "pg.hpp"

extern char** environ;

namespace tdr::cpdb::testing {

namespace {

struct RunResult {
    int status = -1;  // exit code, or -1 if killed / spawn failed
    std::string output;
};

// Runs argv[0] (absolute path, or looked up in PATH when `search`), capturing
// stdout+stderr. No shell involved.
RunResult run(const std::vector<std::string>& argv, bool search = false) {
    int fds[2];
    if (pipe2(fds, O_CLOEXEC) != 0) throw std::runtime_error(std::string("pipe2: ") + std::strerror(errno));
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    posix_spawn_file_actions_adddup2(&fa, fds[1], 1);
    posix_spawn_file_actions_adddup2(&fa, fds[1], 2);

    std::vector<char*> args;
    for (const auto& a : argv) args.push_back(const_cast<char*>(a.c_str()));
    args.push_back(nullptr);

    pid_t pid = -1;
    int rc = search ? posix_spawnp(&pid, args[0], &fa, nullptr, args.data(), environ)
                    : posix_spawn(&pid, args[0], &fa, nullptr, args.data(), environ);
    posix_spawn_file_actions_destroy(&fa);
    close(fds[1]);
    RunResult res;
    if (rc != 0) {
        close(fds[0]);
        res.output = "spawn " + argv[0] + ": " + std::strerror(rc);
        return res;
    }
    char buf[4096];
    for (;;) {
        ssize_t n = read(fds[0], buf, sizeof buf);
        if (n > 0) res.output.append(buf, static_cast<std::size_t>(n));
        else if (n == 0 || errno != EINTR) break;
    }
    close(fds[0]);
    int st = 0;
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR) {
    }
    res.status = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
    return res;
}

void must(const std::vector<std::string>& argv) {
    auto r = run(argv);
    if (r.status != 0) {
        std::string cmd;
        for (const auto& a : argv) cmd += (cmd.empty() ? "" : " ") + a;
        throw std::runtime_error("TempCluster: `" + cmd + "` failed (" + std::to_string(r.status) + "):\n" + r.output);
    }
}

std::filesystem::path find_bin() {
    if (const char* e = std::getenv("PG_BIN"); e && *e) return e;
    std::filesystem::path deb = "/usr/lib/postgresql/16/bin";
    if (std::filesystem::exists(deb / "initdb")) return deb;
    auto r = run({"pg_config", "--bindir"}, true);
    while (!r.output.empty() && (r.output.back() == '\n' || r.output.back() == ' ')) r.output.pop_back();
    if (r.status != 0 || r.output.empty()) throw std::runtime_error("TempCluster: cannot find PostgreSQL binaries");
    return r.output;
}

std::string current_user() {
    if (const passwd* pw = getpwuid(geteuid())) return pw->pw_name;
    throw std::runtime_error("TempCluster: cannot determine current user");
}

}  // namespace

TempCluster::TempCluster() {
    bin_ = find_bin();
    std::string tmpl = "/tmp/ffpg-XXXXXX";
    if (!mkdtemp(tmpl.data())) throw std::runtime_error(std::string("mkdtemp: ") + std::strerror(errno));
    dir_ = tmpl;
    const auto data = dir_ / "data";
    try {
        must({(bin_ / "initdb").string(), "-D", data.string(), "-U", current_user(), "--auth=trust", "-E", "UTF8",
              "--locale=C", "--no-sync", "--no-instructions"});
        {
            std::ofstream conf(data / "postgresql.conf", std::ios::app);
            conf << "\nlisten_addresses = ''\n"
                 << "unix_socket_directories = '" << dir_.string() << "'\n"
                 << "port = 5432\n"
                 << "shared_buffers = 16MB\n"
                 << "fsync = off\n"
                 << "synchronous_commit = off\n"
                 << "full_page_writes = off\n"
                 << "max_connections = 20\n";
            if (!conf) throw std::runtime_error("TempCluster: cannot write postgresql.conf");
        }
        must({(bin_ / "pg_ctl").string(), "-D", data.string(), "-l", (dir_ / "server.log").string(), "-w", "-t",
              "60", "start"});
        conninfo_ = create_database("fireforge");
    } catch (...) {
        if (std::filesystem::exists(data / "postmaster.pid"))
            run({(bin_ / "pg_ctl").string(), "-D", data.string(), "-m", "immediate", "-w", "stop"});
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
        throw;
    }
}

TempCluster::~TempCluster() {
    run({(bin_ / "pg_ctl").string(), "-D", (dir_ / "data").string(), "-m", "immediate", "-w", "stop"});
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
}

std::string TempCluster::create_database(const std::string& name) {
    pg::Conn c("host=" + dir_.string() + " port=5432 dbname=postgres");
    char* q = PQescapeIdentifier(c.raw(), name.c_str(), name.size());
    if (!q) throw std::runtime_error("TempCluster: bad database name '" + name + "'");
    std::string ident(q);
    PQfreemem(q);
    c.exec_script("CREATE DATABASE " + ident);
    bool plain = !name.empty();
    for (char ch : name) plain = plain && (std::isalnum(static_cast<unsigned char>(ch)) || ch == '_');
    if (plain) return "host=" + dir_.string() + " port=5432 dbname=" + name;
    std::string quoted;
    for (char ch : name) {
        if (ch == '\'' || ch == '\\') quoted += '\\';
        quoted += ch;
    }
    return "host=" + dir_.string() + " port=5432 dbname='" + quoted + "'";
}

}  // namespace tdr::cpdb::testing
