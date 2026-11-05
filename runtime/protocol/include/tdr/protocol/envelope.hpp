// TDR binary envelope: 48-byte header + protobuf payload.
// Wire format is specified in docs/design/phase1.md §2. All integers big-endian.
// Never memcpy this struct onto the wire; always use encode/decode.
#pragma once

#include <array>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

#include "fireforge/tdr/v1/envelope.pb.h"

namespace tdr::protocol {

namespace v1 = ::fireforge::tdr::v1;

inline constexpr std::uint32_t kMagic = 0x46465452;  // "FFTR"
inline constexpr std::uint16_t kVersion = 1;
inline constexpr std::size_t kHeaderSize = 48;
inline constexpr std::uint32_t kMaxPayload = 64u * 1024u * 1024u;
inline constexpr std::uint32_t kKnownFlags = 0;  // v1 defines no flags; others must be 0

struct Header {
    std::uint16_t version = kVersion;
    std::uint16_t message_type = 0;   // v1::MessageType value
    std::uint32_t flags = 0;
    std::uint32_t payload_length = 0;
    std::uint64_t message_id = 0;
    std::uint64_t correlation_id = 0; // message_id of the request this replies to, else 0
    std::array<std::uint8_t, 16> sender_id{};

    friend bool operator==(const Header&, const Header&) = default;
};

enum class DecodeError {
    kBadMagic,
    kUnsupportedVersion,
    kPayloadTooLarge,
    kUnknownFlags,
};
std::string_view to_string(DecodeError e);

void encode_header(const Header& h, std::span<std::uint8_t, kHeaderSize> out);
std::expected<Header, DecodeError> decode_header(std::span<const std::uint8_t, kHeaderSize> in);

struct Frame {
    Header header;
    std::string payload;  // serialized protobuf

    v1::MessageType type() const { return static_cast<v1::MessageType>(header.message_type); }

    // Parse payload into a protobuf message. False if it does not parse.
    template <class Msg>
    bool parse(Msg& out) const { return out.ParseFromString(payload); }
};

// Build a frame from a protobuf message. payload_length is set automatically.
template <class Msg>
Frame make_frame(v1::MessageType type, const Msg& msg, std::uint64_t message_id,
                 std::uint64_t correlation_id = 0) {
    Frame f;
    f.header.message_type = static_cast<std::uint16_t>(type);
    f.header.message_id = message_id;
    f.header.correlation_id = correlation_id;
    msg.SerializeToString(&f.payload);
    f.header.payload_length = static_cast<std::uint32_t>(f.payload.size());
    return f;
}

// Header + payload as one contiguous byte string ready to write.
// Throws ProtocolError if payload exceeds kMaxPayload.
std::string encode_frame(const Frame& f);

struct ProtocolError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Incremental decoder for stream sockets. Feed bytes as they arrive, then
// drain complete frames with next(). After a decode error the decoder is
// poisoned: next() keeps throwing and the connection must be closed.
class FrameDecoder {
public:
    void feed(std::span<const std::uint8_t> bytes);
    void feed(std::string_view bytes);
    std::optional<Frame> next();     // throws ProtocolError on malformed input
    std::size_t buffered() const { return buf_.size() - pos_; }

private:
    std::string buf_;
    std::size_t pos_ = 0;
    std::optional<std::string> error_;
};

// Blocking helpers for simple synchronous clients (tdrctl, tests).
// write_frame loops until all bytes are written (handles EINTR / short writes).
// read_frame returns nullopt on clean EOF before any header byte; throws
// ProtocolError on malformed data or EOF mid-frame; std::system_error on I/O errors.
void write_frame(int fd, const Frame& f);
std::optional<Frame> read_frame(int fd);

// Process-wide monotonically increasing message ids, starting at 1. Thread-safe.
std::uint64_t next_message_id();

}  // namespace tdr::protocol
