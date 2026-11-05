// Internals of the agent module shared with its unit tests (not a public contract).
#pragma once

#include <chrono>
#include <random>

namespace tdr::agent::detail {

// `base` scaled by a uniform factor in [0.8, 1.2] (±20% jitter).
std::chrono::milliseconds jitter(std::chrono::milliseconds base, std::mt19937& rng);

// Next exponential backoff step: 0 -> min, then doubling, capped at max (before jitter).
std::chrono::milliseconds next_backoff_base(std::chrono::milliseconds current, std::chrono::milliseconds min,
                                            std::chrono::milliseconds max);

// Duration of one "heartbeat_interval_seconds" unit. 1 s in production; unit
// tests shrink it so heartbeat cycles run in milliseconds. Single-threaded use only.
extern std::chrono::milliseconds g_interval_unit;

}  // namespace tdr::agent::detail
