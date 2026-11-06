// tdr-launch helpers: gang file JSON, hostfile and argv builders (phase7.md §5).
#include "tdr/launch/launch.hpp"

#include <boost/json.hpp>

#include <fstream>
#include <limits>
#include <sstream>

namespace tdr::launch {

namespace json = boost::json;

namespace {

std::string trim(std::string_view s) {
    const char* ws = " \t\r\n";
    auto b = s.find_first_not_of(ws);
    if (b == std::string_view::npos) return {};
    auto e = s.find_last_not_of(ws);
    return std::string(s.substr(b, e - b + 1));
}

// "k=v,k=v" -> pairs. Empty entries (",," or a trailing comma) are skipped.
std::expected<std::vector<std::pair<std::string, std::string>>, std::string> split_mca(const std::string& mca) {
    std::vector<std::pair<std::string, std::string>> out;
    std::size_t start = 0;
    while (start <= mca.size()) {
        auto comma = mca.find(',', start);
        if (comma == std::string::npos) comma = mca.size();
        std::string entry = trim(std::string_view(mca).substr(start, comma - start));
        start = comma + 1;
        if (entry.empty()) continue;
        auto eq = entry.find('=');
        if (eq == std::string::npos) return std::unexpected("mca entry without '=': " + entry);
        std::string k = trim(std::string_view(entry).substr(0, eq));
        std::string v = trim(std::string_view(entry).substr(eq + 1));
        if (k.empty()) return std::unexpected("mca entry with empty key: " + entry);
        out.emplace_back(std::move(k), std::move(v));
    }
    return out;
}

std::expected<std::string, std::string> get_string(const json::object& o, std::string_view key, bool required) {
    auto* v = o.if_contains(key);
    if (!v || v->is_null()) {
        if (required) return std::unexpected("missing \"" + std::string(key) + "\"");
        return std::string{};
    }
    if (!v->is_string()) return std::unexpected("\"" + std::string(key) + "\" must be a string");
    return std::string(v->get_string());
}

std::expected<std::uint32_t, std::string> get_u32(const json::object& o, std::string_view key, bool required,
                                                  std::uint32_t def) {
    auto* v = o.if_contains(key);
    if (!v || v->is_null()) {
        if (required) return std::unexpected("missing \"" + std::string(key) + "\"");
        return def;
    }
    std::uint64_t n = 0;
    if (v->is_uint64()) {
        n = v->get_uint64();
    } else if (v->is_int64() && v->get_int64() >= 0) {
        n = static_cast<std::uint64_t>(v->get_int64());
    } else {
        return std::unexpected("\"" + std::string(key) + "\" must be a non-negative integer");
    }
    if (n > std::numeric_limits<std::uint32_t>::max()) {
        return std::unexpected("\"" + std::string(key) + "\" out of range");
    }
    return static_cast<std::uint32_t>(n);
}

}  // namespace

std::string to_json(const GangFile& g) {
    json::object o;
    o["gang_id"] = g.gang_id;
    o["self_node_id"] = g.self_node_id;
    json::array members;
    for (const auto& m : g.members) {
        members.push_back(json::object{{"node_id", m.node_id}, {"name", m.name}, {"ranks", m.ranks}});
    }
    o["members"] = std::move(members);
    o["ranks"] = g.ranks;
    o["mpirun"] = g.mpirun;
    o["if_include"] = g.if_include;
    o["mca"] = g.mca;
    o["launch_timeout"] = g.launch_timeout;
    json::array args;
    for (const auto& a : g.mpirun_args) args.push_back(json::string(a));
    o["mpirun_args"] = std::move(args);
    return json::serialize(o);
}

std::expected<GangFile, std::string> parse_gang_file(const std::string& text) {
    boost::system::error_code ec;
    json::value v = json::parse(text, ec);
    if (ec) return std::unexpected("gang file: bad JSON: " + ec.message());
    if (!v.is_object()) return std::unexpected("gang file: not a JSON object");
    const auto& o = v.get_object();
    auto fail = [](const std::string& e) { return std::unexpected("gang file: " + e); };

    GangFile g;
    auto s = get_string(o, "gang_id", false);
    if (!s) return fail(s.error());
    g.gang_id = *s;
    if (!(s = get_string(o, "self_node_id", false))) return fail(s.error());
    g.self_node_id = *s;

    auto* mv = o.if_contains("members");
    if (!mv || !mv->is_array()) return fail("missing \"members\" array");
    if (mv->get_array().empty()) return fail("no members");
    std::uint64_t sum = 0;
    for (const auto& e : mv->get_array()) {
        if (!e.is_object()) return fail("member must be an object");
        const auto& mo = e.get_object();
        GangFile::Member m;
        auto id = get_string(mo, "node_id", true);
        if (!id) return fail("member: " + id.error());
        if (id->empty()) return fail("member with empty node_id");
        m.node_id = *id;
        auto name = get_string(mo, "name", false);
        if (!name) return fail("member: " + name.error());
        m.name = *name;
        auto r = get_u32(mo, "ranks", true, 0);
        if (!r) return fail("member: " + r.error());
        if (*r == 0) return fail("member " + m.node_id + " has 0 ranks");
        m.ranks = *r;
        sum += m.ranks;
        g.members.push_back(std::move(m));
    }

    auto r = get_u32(o, "ranks", true, 0);
    if (!r) return fail(r.error());
    g.ranks = *r;
    if (sum != g.ranks) {
        return fail("ranks " + std::to_string(g.ranks) + " != sum of member ranks " + std::to_string(sum));
    }

    if (!(s = get_string(o, "mpirun", false))) return fail(s.error());
    if (!s->empty()) g.mpirun = *s;
    if (!(s = get_string(o, "if_include", false))) return fail(s.error());
    g.if_include = *s;
    if (!(s = get_string(o, "mca", false))) return fail(s.error());
    g.mca = *s;
    if (auto m = split_mca(g.mca); !m) return fail(m.error());
    auto t = get_u32(o, "launch_timeout", false, 120);
    if (!t) return fail(t.error());
    g.launch_timeout = *t;

    if (auto* av = o.if_contains("mpirun_args"); av && !av->is_null()) {
        if (!av->is_array()) return fail("\"mpirun_args\" must be an array");
        for (const auto& a : av->get_array()) {
            if (!a.is_string()) return fail("\"mpirun_args\" entries must be strings");
            g.mpirun_args.emplace_back(a.get_string());
        }
    }
    return g;
}

std::expected<GangFile, std::string> read_gang_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return std::unexpected("gang file: cannot open " + path.string());
    std::ostringstream ss;
    ss << in.rdbuf();
    if (in.bad()) return std::unexpected("gang file: cannot read " + path.string());
    return parse_gang_file(ss.str());
}

std::string hostfile(const GangFile& g) {
    std::string out;
    for (const auto& m : g.members) {
        out += (m.node_id == g.self_node_id) ? std::string("localhost") : m.node_id;
        out += " slots=" + std::to_string(m.ranks) + "\n";
    }
    return out;
}

std::vector<std::string> mpirun_argv(const GangFile& g, const fs::path& hostfile_path, const fs::path& self,
                                     std::span<const std::string> program) {
    std::vector<std::string> a{g.mpirun,
                               "--hostfile",
                               hostfile_path.string(),
                               "-np",
                               std::to_string(g.ranks),
                               "--set-cwd-to-session-dir",
                               "--mca",
                               "plm_rsh_agent",
                               self.string() + " agent",
                               "--mca",
                               "plm_rsh_no_tree_spawn",
                               "1"};
    if (!g.if_include.empty()) {
        a.insert(a.end(), {"--mca", "oob_tcp_if_include", g.if_include, "--mca", "btl_tcp_if_include", g.if_include});
    }
    if (auto mca = split_mca(g.mca)) {  // validated by parse_gang_file; bad entries are dropped here
        for (auto& [k, v] : *mca) a.insert(a.end(), {"--mca", k, v});
    }
    a.insert(a.end(), g.mpirun_args.begin(), g.mpirun_args.end());
    a.push_back(self.string());
    a.push_back("rank");
    a.push_back("--");
    a.insert(a.end(), program.begin(), program.end());
    return a;
}

std::string agent_command(std::span<const std::string> words) {
    std::string out;
    for (const auto& w : words) {
        if (w == "--daemonize") continue;
        if (!out.empty()) out += ' ';
        out += w;
    }
    return out;
}

}  // namespace tdr::launch
