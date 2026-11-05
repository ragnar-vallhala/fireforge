// Small shared utilities: ids, time, logging.
#pragma once

#include <array>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>

namespace tdr {

// ---- ids ------------------------------------------------------------------

// Random 128-bit id rendered as "<prefix>-<32 lowercase hex>", e.g. "job-3f9a...".
// Uses a CSPRNG (getrandom). Thread-safe.
std::string new_id(std::string_view prefix);

// 32 random bytes as 64 lowercase hex chars. Used for attempt tokens.
std::string new_secret();

using NodeId = std::array<std::uint8_t, 16>;
NodeId node_id_from_string(std::string_view s);  // hashes s to 16 bytes (stable)

// ---- time -----------------------------------------------------------------

std::int64_t unix_ms();  // wall clock, milliseconds since epoch

// ---- logging --------------------------------------------------------------
// One line per call to stderr: "2026-11-05T20:10:00.123Z INFO  message".
// systemd captures stderr into the journal; tdr.log is optional (see Config).

enum class LogLevel { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3 };

void set_log_level(LogLevel level);
LogLevel parse_log_level(std::string_view s);  // "debug"|"info"|"warn"|"error"
void log_line(LogLevel level, std::string_view message);

template <class... Args>
void log_debug(std::format_string<Args...> f, Args&&... a) { log_line(LogLevel::kDebug, std::format(f, std::forward<Args>(a)...)); }
template <class... Args>
void log_info(std::format_string<Args...> f, Args&&... a) { log_line(LogLevel::kInfo, std::format(f, std::forward<Args>(a)...)); }
template <class... Args>
void log_warn(std::format_string<Args...> f, Args&&... a) { log_line(LogLevel::kWarn, std::format(f, std::forward<Args>(a)...)); }
template <class... Args>
void log_error(std::format_string<Args...> f, Args&&... a) { log_line(LogLevel::kError, std::format(f, std::forward<Args>(a)...)); }

}  // namespace tdr
