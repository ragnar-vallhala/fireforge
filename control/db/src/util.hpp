// Internal helpers shared by the repository sources (not a public contract).
#pragma once

#include <google/protobuf/message.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "fireforge/control/v1/control.pb.h"
#include "tdr/cpdb/db.hpp"

namespace tdr::cpdb::detail {

// proto3 JSON with original field names. Throws DbError.
std::string to_json(const google::protobuf::Message& m);
// Ignores unknown fields. Throws DbError.
void from_json(const std::string& s, google::protobuf::Message* m);

// A repeated string field as a plain JSON array ("[...]").
std::string string_array_json(const google::protobuf::RepeatedPtrField<std::string>& v);

// prefix + 16 random hex characters.
std::string short_id(std::string_view prefix);

// bytea hex text ("\x0a1b...") <-> raw bytes.
std::string to_bytea_hex(const std::string& raw);
std::string from_hex(const std::string& hex);  // plain hex (encode(col, 'hex')); throws DbError

inline std::int64_t clamp_limit(std::size_t n) {
    constexpr auto max = static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max());
    return static_cast<std::int64_t>(n > max ? max : n);
}

}  // namespace tdr::cpdb::detail

// SQL fragments. $n is a unix-ms bigint parameter.
#define TDR_TS(n) "to_timestamp($" #n "::bigint / 1000.0)"
#define TDR_MS(col) "(extract(epoch FROM " col ") * 1000)::bigint"
