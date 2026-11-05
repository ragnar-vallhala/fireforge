#include <boost/test/unit_test.hpp>

#include <filesystem>
#include <fstream>

#include "tdr/common/common.hpp"
#include "tdr/common/config.hpp"

namespace fs = std::filesystem;

namespace {

fs::path example_conf() {
    // runtime/common/tests/config_tests.cpp -> repo root
    return fs::path(__FILE__).parent_path().parent_path().parent_path().parent_path() / "deploy" / "etc" /
           "tdr.conf.example";
}

std::expected<tdr::Config, std::string> cfg(const std::string& text) {
    auto ini = tdr::parse_ini(text);
    if (!ini) return std::unexpected(ini.error());
    return tdr::config_from_ini(*ini);
}

void check_defaults(const tdr::Config& c) {
    tdr::Config d;
    BOOST_TEST(c.control.address == d.control.address);
    BOOST_TEST(c.control.port == 8080);
    BOOST_TEST(c.control.scheme == "https");
    BOOST_TEST(c.node.name == "");
    BOOST_TEST(c.node.advertise_address == "auto");
    BOOST_TEST(c.network.listen_address == "0.0.0.0");
    BOOST_TEST(c.network.listen_port == 7400);
    BOOST_TEST(c.network.tailscale == "auto");
    BOOST_TEST(c.resources.max_cpu_cores == 0u);
    BOOST_TEST(c.resources.max_memory_mb == 0u);
    BOOST_TEST(c.resources.max_jobs == 0u);
    BOOST_TEST(c.resources.max_disk_mb == 0u);
    BOOST_TEST(c.resources.default_job_memory_mb == 1024u);
    BOOST_TEST(c.resources.cgroups == "auto");
    BOOST_TEST(c.paths.state_dir == fs::path("/var/lib/fireforge"));
    BOOST_TEST(c.paths.log_file.empty());
    BOOST_TEST(c.log_level == "info");
}

}  // namespace

BOOST_AUTO_TEST_SUITE(config)

BOOST_AUTO_TEST_CASE(example_file_parses_to_defaults) {
    auto c = tdr::load_config(example_conf());
    BOOST_TEST_REQUIRE(c.has_value(), (c ? "" : c.error()));
    check_defaults(*c);
}

BOOST_AUTO_TEST_CASE(empty_text_gives_defaults) {
    auto c = cfg("");
    BOOST_TEST_REQUIRE(c.has_value());
    check_defaults(*c);
}

BOOST_AUTO_TEST_CASE(overrides) {
    auto c = cfg(R"(
[control]
address = ctl.example.org
port = 9443
[node]
name = box1
[network]
listen_port = 7500
tailscale = off
[resources]
max_cpu_cores = 12
max_memory_mb = 65536
max_jobs = 4
max_disk_mb = 100000
default_job_memory_mb = 2048
cgroups = required
[paths]
state_dir = /tmp/ff state
log_file = /var/log/tdr.log
[log]
level = debug
)");
    BOOST_TEST_REQUIRE(c.has_value(), (c ? "" : c.error()));
    BOOST_TEST(c->control.address == "ctl.example.org");
    BOOST_TEST(c->control.port == 9443);
    BOOST_TEST(c->node.name == "box1");
    BOOST_TEST(c->network.listen_port == 7500);
    BOOST_TEST(c->network.tailscale == "off");
    BOOST_TEST(c->resources.max_cpu_cores == 12u);
    BOOST_TEST(c->resources.max_memory_mb == 65536u);
    BOOST_TEST(c->resources.max_jobs == 4u);
    BOOST_TEST(c->resources.max_disk_mb == 100000u);
    BOOST_TEST(c->resources.default_job_memory_mb == 2048u);
    BOOST_TEST(c->resources.cgroups == "required");
    BOOST_TEST(c->paths.state_dir == fs::path("/tmp/ff state"));
    BOOST_TEST(c->journal_path() == fs::path("/tmp/ff state/state/journal.db"));
    BOOST_TEST(c->paths.log_file == fs::path("/var/log/tdr.log"));
    BOOST_TEST(c->log_level == "debug");
}

BOOST_AUTO_TEST_CASE(empty_value_keeps_default_and_last_duplicate_wins) {
    auto c = cfg("[control]\nport =\nscheme = http\nscheme = grpc\n");
    BOOST_TEST_REQUIRE(c.has_value());
    BOOST_TEST(c->control.port == 8080);
    BOOST_TEST(c->control.scheme == "grpc");
}

BOOST_AUTO_TEST_CASE(unknown_key_and_section) {
    auto k = cfg("[control]\nprot = 1\n");
    BOOST_TEST_REQUIRE(!k.has_value());
    BOOST_TEST(k.error().find("prot") != std::string::npos);

    auto s = cfg("[contrl]\nport = 1\n");
    BOOST_TEST_REQUIRE(!s.has_value());
    BOOST_TEST(s.error().find("contrl") != std::string::npos);

    auto top = cfg("port = 1\n");
    BOOST_TEST_REQUIRE(!top.has_value());
    BOOST_TEST(top.error().find("port") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(bad_numbers) {
    BOOST_TEST(!cfg("[control]\nport = abc\n").has_value());
    BOOST_TEST(!cfg("[control]\nport = 70000\n").has_value());  // > uint16
    BOOST_TEST(!cfg("[resources]\nmax_jobs = -1\n").has_value());
    BOOST_TEST(!cfg("[resources]\nmax_jobs = 4x\n").has_value());
    auto e = cfg("[resources]\nmax_memory_mb = 1.5\n");
    BOOST_TEST_REQUIRE(!e.has_value());
    BOOST_TEST(e.error().find("max_memory_mb") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(comments) {
    auto ini = tdr::parse_ini(
        "# full line\n"
        "; also full line\n"
        "   # indented\n"
        "[network]   # header comment\n"
        "listen_address = 10.0.0.1   # trailing\n"
        "tailscale = on ; trailing semicolon\n"
        "[node]\n"
        "name = a#b\n");  // '#' not preceded by space is part of the value
    BOOST_TEST_REQUIRE(ini.has_value(), (ini ? "" : ini.error()));
    BOOST_TEST(*ini->find("network", "listen_address") == "10.0.0.1");
    BOOST_TEST(*ini->find("network", "tailscale") == "on");
    BOOST_TEST(*ini->find("node", "name") == "a#b");
    BOOST_TEST(ini->find("node", "missing") == nullptr);
    BOOST_TEST(ini->find("nosuch", "name") == nullptr);
}

BOOST_AUTO_TEST_CASE(whitespace_and_crlf) {
    auto ini = tdr::parse_ini("\t[ log ]\r\n   level   =   warn  \r\n");
    BOOST_TEST_REQUIRE(ini.has_value());
    BOOST_TEST(*ini->find("log", "level") == "warn");
}

BOOST_AUTO_TEST_CASE(errors_have_line_numbers) {
    auto a = tdr::parse_ini("[control]\nport = 1\n\njust some words\n");
    BOOST_TEST_REQUIRE(!a.has_value());
    BOOST_TEST(a.error().find("line 4") != std::string::npos, a.error());

    auto b = tdr::parse_ini("# c\n[control\n");
    BOOST_TEST_REQUIRE(!b.has_value());
    BOOST_TEST(b.error().find("line 2") != std::string::npos, b.error());

    auto c = tdr::parse_ini("[x]\n = v\n");
    BOOST_TEST_REQUIRE(!c.has_value());
    BOOST_TEST(c.error().find("line 2") != std::string::npos, c.error());
}

BOOST_AUTO_TEST_CASE(load_config_missing_file) {
    auto c = tdr::load_config("/nonexistent/dir/tdr.conf");
    BOOST_TEST(!c.has_value());
}

BOOST_AUTO_TEST_CASE(load_config_from_file) {
    const fs::path dir = fs::path("/tmp/claude-1000") / tdr::new_id("cfgtest");
    fs::create_directories(dir);
    {
        std::ofstream(dir / "tdr.conf") << "[paths]\nstate_dir = " << dir.string() << "\n[log]\nlevel = warn\n";
    }
    auto c = tdr::load_config(dir / "tdr.conf");
    fs::remove_all(dir);
    BOOST_TEST_REQUIRE(c.has_value());
    BOOST_TEST(c->paths.state_dir == dir);
    BOOST_TEST(c->log_level == "warn");
    BOOST_TEST(c->ctl_socket() == dir / "run" / "ctl.sock");
}

BOOST_AUTO_TEST_SUITE_END()
