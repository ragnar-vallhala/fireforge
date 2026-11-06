// tdr-launch helpers (docs/design/phase7.md §5): the gang file shared by
// RemoteExecutor (writes it) and `tdr-launch head` (reads it), and the pure
// argv/command builders. The executable itself is runtime/launch/app/main.cpp.
#pragma once
#include <cstdint>
#include <expected>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

namespace tdr::launch {

namespace fs = std::filesystem;

struct GangFile {
    struct Member {
        std::string node_id;
        std::string name;
        std::uint32_t ranks = 0;
    };
    std::string gang_id;                // control-plane attempt id
    std::string self_node_id;           // the head's node id
    std::vector<Member> members;        // member_index order; [0] is the head
    std::uint32_t ranks = 0;            // total
    std::string mpirun = "mpirun";
    std::string if_include;             // "" = Open MPI default
    std::string mca;                    // "k=v,k=v"
    std::uint32_t launch_timeout = 120;
    std::vector<std::string> mpirun_args;
};

// JSON round trip (Boost.JSON). read: error on missing file, bad JSON, no
// members, ranks != sum of member ranks, or an mca entry without '='.
std::string to_json(const GangFile& g);
std::expected<GangFile, std::string> parse_gang_file(const std::string& json);
std::expected<GangFile, std::string> read_gang_file(const fs::path& path);

// One line per member: "<node_id> slots=<ranks>", the head (self_node_id) as
// "localhost slots=<ranks>".
std::string hostfile(const GangFile& g);

// mpirun --hostfile H -np R --set-cwd-to-session-dir
//   --mca plm_rsh_agent "<self> agent" --mca plm_rsh_no_tree_spawn 1
//   [--mca oob_tcp_if_include X --mca btl_tcp_if_include X] [--mca k v]...
//   <mpirun_args...> <self> rank -- <program...>
std::vector<std::string> mpirun_argv(const GangFile& g, const fs::path& hostfile_path, const fs::path& self,
                                     std::span<const std::string> program);

// The command an agent forwards: words joined by single spaces with every
// "--daemonize" word removed.
std::string agent_command(std::span<const std::string> words);

}  // namespace tdr::launch
