#include "tdr/common/common.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <stdexcept>
#include <system_error>

#include <sys/random.h>

namespace tdr {
namespace {

void fill_random(void* buf, std::size_t len) {
    auto* p = static_cast<unsigned char*>(buf);
    while (len > 0) {
        ssize_t n = getrandom(p, len, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "getrandom");
        }
        p += n;
        len -= static_cast<std::size_t>(n);
    }
}

std::string to_hex(const unsigned char* data, std::size_t len) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out(len * 2, '\0');
    for (std::size_t i = 0; i < len; ++i) {
        out[2 * i] = kDigits[data[i] >> 4];
        out[2 * i + 1] = kDigits[data[i] & 0x0f];
    }
    return out;
}

std::atomic<int> g_level{static_cast<int>(LogLevel::kInfo)};
std::mutex g_log_mutex;

}  // namespace

std::string new_id(std::string_view prefix) {
    unsigned char bytes[16];
    fill_random(bytes, sizeof bytes);
    std::string out(prefix);
    out += '-';
    out += to_hex(bytes, sizeof bytes);
    return out;
}

std::string new_secret() {
    unsigned char bytes[32];
    fill_random(bytes, sizeof bytes);
    return to_hex(bytes, sizeof bytes);
}

NodeId node_id_from_string(std::string_view s) {
    // FNV-1a 64-bit, two independent seeds -> 16 bytes. Stable, not cryptographic.
    auto fnv = [&](std::uint64_t h) {
        for (unsigned char c : s) { h ^= c; h *= 0x100000001b3ull; }
        return h;
    };
    std::uint64_t a = fnv(0xcbf29ce484222325ull), b = fnv(0x84222325cbf29ce4ull);
    NodeId id{};
    for (int i = 0; i < 8; ++i) {
        id[i] = static_cast<std::uint8_t>(a >> (56 - 8 * i));
        id[8 + i] = static_cast<std::uint8_t>(b >> (56 - 8 * i));
    }
    return id;
}

std::int64_t unix_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void set_log_level(LogLevel level) { g_level.store(static_cast<int>(level)); }

LogLevel parse_log_level(std::string_view s) {
    if (s == "debug") return LogLevel::kDebug;
    if (s == "info") return LogLevel::kInfo;
    if (s == "warn" || s == "warning") return LogLevel::kWarn;
    if (s == "error") return LogLevel::kError;
    throw std::invalid_argument("unknown log level: " + std::string(s));
}

void log_line(LogLevel level, std::string_view message) {
    if (static_cast<int>(level) < g_level.load()) return;
    static constexpr const char* kNames[] = {"DEBUG", "INFO ", "WARN ", "ERROR"};
    auto now = std::chrono::floor<std::chrono::milliseconds>(std::chrono::system_clock::now());
    std::string line = std::format("{:%FT%T}Z {} {}\n", now, kNames[static_cast<int>(level)], message);
    std::lock_guard lock(g_log_mutex);
    std::fwrite(line.data(), 1, line.size(), stderr);
}

}  // namespace tdr
