// tdrctl subcommands that don't talk to the local daemon.
#pragma once

#include <stdexcept>
#include <string>
#include <vector>

namespace tdrctl {

struct UsageError : std::runtime_error { using std::runtime_error::runtime_error; };

int run_pki(const std::vector<std::string>& args);  // tdrctl pki ...
int run_cp(const std::vector<std::string>& args);   // tdrctl cp ...

}  // namespace tdrctl
