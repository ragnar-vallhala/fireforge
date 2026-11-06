#include "util.hpp"

#include <google/protobuf/util/json_util.h>

#include "tdr/common/common.hpp"

namespace tdr::cpdb::detail {

std::string to_json(const google::protobuf::Message& m) {
    google::protobuf::util::JsonPrintOptions o;
    o.preserve_proto_field_names = true;
    std::string out;
    auto st = google::protobuf::util::MessageToJsonString(m, &out, o);
    if (!st.ok()) throw DbError("proto to json: " + std::string(st.message()));
    return out;
}

void from_json(const std::string& s, google::protobuf::Message* m) {
    google::protobuf::util::JsonParseOptions o;
    o.ignore_unknown_fields = true;
    auto st = google::protobuf::util::JsonStringToMessage(s, m, o);
    if (!st.ok()) throw DbError("stored json does not parse: " + std::string(st.message()));
}

// Serialize through a carrier message ({"capabilities": [...]}) and keep only the array.
std::string string_array_json(const google::protobuf::RepeatedPtrField<std::string>& v) {
    cv1::RegisterRequest carrier;
    *carrier.mutable_capabilities() = v;
    std::string s = to_json(carrier);  // {"capabilities":[...]} or {}
    auto b = s.find('['), e = s.rfind(']');
    return b == std::string::npos ? "[]" : s.substr(b, e - b + 1);
}

std::string short_id(std::string_view prefix) { return std::string(prefix) + new_secret().substr(0, 16); }

std::string to_bytea_hex(const std::string& raw) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out = "\\x";
    out.reserve(2 + raw.size() * 2);
    for (unsigned char c : raw) {
        out += kHex[c >> 4];
        out += kHex[c & 0xf];
    }
    return out;
}

std::string from_hex(const std::string& hex) {
    auto nib = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    if (hex.size() % 2) throw DbError("odd-length hex from database");
    std::string out;
    out.reserve(hex.size() / 2);
    for (std::size_t i = 0; i < hex.size(); i += 2) {
        int h = nib(hex[i]), l = nib(hex[i + 1]);
        if (h < 0 || l < 0) throw DbError("bad hex from database");
        out += static_cast<char>((h << 4) | l);
    }
    return out;
}

}  // namespace tdr::cpdb::detail
