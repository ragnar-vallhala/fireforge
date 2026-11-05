#include "tdr/protocol/envelope.hpp"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <system_error>

#include <sys/socket.h>
#include <unistd.h>

namespace tdr::protocol {

namespace {

template <class T>
void put_be(std::uint8_t* p, T v) {
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        p[sizeof(T) - 1 - i] = static_cast<std::uint8_t>(v & 0xFF);
        v = static_cast<T>(v >> 8);
    }
}

template <class T>
T get_be(const std::uint8_t* p) {
    T v = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        v = static_cast<T>((v << 8) | static_cast<T>(p[i]));
    }
    return v;
}

std::string error_message(DecodeError e) {
    return "protocol decode error: " + std::string(to_string(e));
}

}  // namespace

std::string_view to_string(DecodeError e) {
    switch (e) {
        case DecodeError::kBadMagic: return "bad magic";
        case DecodeError::kUnsupportedVersion: return "unsupported version";
        case DecodeError::kPayloadTooLarge: return "payload too large";
        case DecodeError::kUnknownFlags: return "unknown flags";
    }
    return "unknown decode error";
}

void encode_header(const Header& h, std::span<std::uint8_t, kHeaderSize> out) {
    std::uint8_t* p = out.data();
    put_be<std::uint32_t>(p + 0, kMagic);
    put_be<std::uint16_t>(p + 4, h.version);
    put_be<std::uint16_t>(p + 6, h.message_type);
    put_be<std::uint32_t>(p + 8, h.flags);
    put_be<std::uint32_t>(p + 12, h.payload_length);
    put_be<std::uint64_t>(p + 16, h.message_id);
    put_be<std::uint64_t>(p + 24, h.correlation_id);
    std::memcpy(p + 32, h.sender_id.data(), h.sender_id.size());
}

std::expected<Header, DecodeError> decode_header(std::span<const std::uint8_t, kHeaderSize> in) {
    const std::uint8_t* p = in.data();
    if (get_be<std::uint32_t>(p + 0) != kMagic) return std::unexpected(DecodeError::kBadMagic);
    Header h;
    h.version = get_be<std::uint16_t>(p + 4);
    if (h.version != kVersion) return std::unexpected(DecodeError::kUnsupportedVersion);
    h.message_type = get_be<std::uint16_t>(p + 6);
    h.flags = get_be<std::uint32_t>(p + 8);
    if ((h.flags & ~kKnownFlags) != 0) return std::unexpected(DecodeError::kUnknownFlags);
    h.payload_length = get_be<std::uint32_t>(p + 12);
    if (h.payload_length > kMaxPayload) return std::unexpected(DecodeError::kPayloadTooLarge);
    h.message_id = get_be<std::uint64_t>(p + 16);
    h.correlation_id = get_be<std::uint64_t>(p + 24);
    std::memcpy(h.sender_id.data(), p + 32, h.sender_id.size());
    return h;
}

std::string encode_frame(const Frame& f) {
    if (f.payload.size() > kMaxPayload) {
        throw ProtocolError("payload too large: " + std::to_string(f.payload.size()) + " bytes");
    }
    Header h = f.header;
    h.payload_length = static_cast<std::uint32_t>(f.payload.size());
    std::string out(kHeaderSize + f.payload.size(), '\0');
    auto* p = reinterpret_cast<std::uint8_t*>(out.data());
    encode_header(h, std::span<std::uint8_t, kHeaderSize>(p, kHeaderSize));
    std::memcpy(out.data() + kHeaderSize, f.payload.data(), f.payload.size());
    return out;
}

// ---- FrameDecoder ---------------------------------------------------------

void FrameDecoder::feed(std::span<const std::uint8_t> bytes) {
    feed(std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
}

void FrameDecoder::feed(std::string_view bytes) {
    if (error_) return;  // poisoned: nothing more will be decoded
    // Compact consumed bytes so the buffer does not grow without bound.
    if (pos_ == buf_.size()) {
        buf_.clear();
        pos_ = 0;
    } else if (pos_ > 0 && (pos_ >= 64 * 1024 || pos_ * 2 >= buf_.size())) {
        buf_.erase(0, pos_);
        pos_ = 0;
    }
    buf_.append(bytes);
}

std::optional<Frame> FrameDecoder::next() {
    if (error_) throw ProtocolError(*error_);
    if (buffered() < kHeaderSize) return std::nullopt;
    const auto* p = reinterpret_cast<const std::uint8_t*>(buf_.data() + pos_);
    auto h = decode_header(std::span<const std::uint8_t, kHeaderSize>(p, kHeaderSize));
    if (!h) {
        error_ = error_message(h.error());
        buf_.clear();
        buf_.shrink_to_fit();
        pos_ = 0;
        throw ProtocolError(*error_);
    }
    if (buffered() < kHeaderSize + h->payload_length) return std::nullopt;
    Frame f;
    f.header = *h;
    f.payload.assign(buf_.data() + pos_ + kHeaderSize, h->payload_length);
    pos_ += kHeaderSize + h->payload_length;
    if (pos_ == buf_.size()) {
        buf_.clear();
        pos_ = 0;
    }
    return f;
}

// ---- Blocking I/O -----------------------------------------------------------

namespace {

void write_all(int fd, const char* data, std::size_t len) {
    bool use_send = true;
    while (len > 0) {
        ssize_t n;
        if (use_send) {
            n = ::send(fd, data, len, MSG_NOSIGNAL);
            if (n < 0 && errno == ENOTSOCK) {
                use_send = false;
                continue;
            }
        } else {
            n = ::write(fd, data, len);
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "write_frame");
        }
        data += n;
        len -= static_cast<std::size_t>(n);
    }
}

// Reads up to len bytes; returns number read (< len only on EOF).
std::size_t read_full(int fd, char* data, std::size_t len) {
    std::size_t got = 0;
    while (got < len) {
        ssize_t n = ::read(fd, data + got, len - got);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "read_frame");
        }
        if (n == 0) break;
        got += static_cast<std::size_t>(n);
    }
    return got;
}

}  // namespace

void write_frame(int fd, const Frame& f) {
    std::string bytes = encode_frame(f);
    write_all(fd, bytes.data(), bytes.size());
}

std::optional<Frame> read_frame(int fd) {
    std::array<std::uint8_t, kHeaderSize> hdr{};
    std::size_t got = read_full(fd, reinterpret_cast<char*>(hdr.data()), hdr.size());
    if (got == 0) return std::nullopt;
    if (got < kHeaderSize) throw ProtocolError("unexpected EOF in frame header");
    auto h = decode_header(hdr);
    if (!h) throw ProtocolError(error_message(h.error()));
    Frame f;
    f.header = *h;
    f.payload.resize(h->payload_length);
    if (read_full(fd, f.payload.data(), f.payload.size()) < f.payload.size()) {
        throw ProtocolError("unexpected EOF in frame payload");
    }
    return f;
}

std::uint64_t next_message_id() {
    static std::atomic<std::uint64_t> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

}  // namespace tdr::protocol
