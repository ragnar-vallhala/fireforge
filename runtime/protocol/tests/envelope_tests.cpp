#include <boost/test/unit_test.hpp>

#include <array>
#include <cstring>
#include <set>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include "fireforge/tdr/v1/local.pb.h"
#include "tdr/protocol/envelope.hpp"

using namespace tdr::protocol;

namespace {

Header sample_header() {
    Header h;
    h.version = 1;
    h.message_type = 0x1001;
    h.flags = 0;
    h.payload_length = 0x01020304;
    h.message_id = 0x0102030405060708ull;
    h.correlation_id = 0x1112131415161718ull;
    for (std::size_t i = 0; i < 16; ++i) h.sender_id[i] = static_cast<std::uint8_t>(0xA0 + i);
    return h;
}

std::array<std::uint8_t, kHeaderSize> encode(const Header& h) {
    std::array<std::uint8_t, kHeaderSize> b{};
    encode_header(h, b);
    return b;
}

Frame text_frame(std::uint64_t id, std::string payload, std::uint16_t type = 0x1003) {
    Frame f;
    f.header.message_type = type;
    f.header.message_id = id;
    f.payload = std::move(payload);
    f.header.payload_length = static_cast<std::uint32_t>(f.payload.size());
    return f;
}

struct SocketPair {
    int fds[2]{-1, -1};
    SocketPair() { BOOST_REQUIRE_EQUAL(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0); }
    ~SocketPair() {
        for (int fd : fds)
            if (fd >= 0) ::close(fd);
    }
    void close(int i) {
        ::close(fds[i]);
        fds[i] = -1;
    }
};

void raw_write(int fd, std::string_view s) {
    BOOST_REQUIRE_EQUAL(::write(fd, s.data(), s.size()), static_cast<ssize_t>(s.size()));
}

}  // namespace

BOOST_AUTO_TEST_SUITE(protocol_header)

BOOST_AUTO_TEST_CASE(golden_bytes) {
    const std::array<std::uint8_t, kHeaderSize> expected = {
        0x46, 0x46, 0x54, 0x52,                          // magic "FFTR"
        0x00, 0x01,                                      // version
        0x10, 0x01,                                      // message_type
        0x00, 0x00, 0x00, 0x00,                          // flags
        0x01, 0x02, 0x03, 0x04,                          // payload_length
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,  // message_id
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,  // correlation_id
        0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7,  // sender_id
        0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF,
    };
    auto got = encode(sample_header());
    BOOST_CHECK_EQUAL_COLLECTIONS(got.begin(), got.end(), expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(round_trip) {
    Header h = sample_header();
    h.payload_length = 1234;
    auto b = encode(h);
    auto d = decode_header(b);
    BOOST_REQUIRE(d.has_value());
    BOOST_CHECK(*d == h);
}

BOOST_AUTO_TEST_CASE(bad_magic) {
    auto b = encode(Header{});
    b[0] = 0x00;
    auto d = decode_header(b);
    BOOST_REQUIRE(!d.has_value());
    BOOST_CHECK(d.error() == DecodeError::kBadMagic);
}

BOOST_AUTO_TEST_CASE(unsupported_version) {
    Header h;
    h.version = 2;
    auto d = decode_header(encode(h));
    BOOST_REQUIRE(!d.has_value());
    BOOST_CHECK(d.error() == DecodeError::kUnsupportedVersion);
}

BOOST_AUTO_TEST_CASE(unknown_flags) {
    Header h;
    h.flags = 0x80000000u;
    auto d = decode_header(encode(h));
    BOOST_REQUIRE(!d.has_value());
    BOOST_CHECK(d.error() == DecodeError::kUnknownFlags);
}

BOOST_AUTO_TEST_CASE(payload_too_large) {
    Header h;
    h.payload_length = kMaxPayload;
    BOOST_CHECK(decode_header(encode(h)).has_value());
    h.payload_length = kMaxPayload + 1;
    auto d = decode_header(encode(h));
    BOOST_REQUIRE(!d.has_value());
    BOOST_CHECK(d.error() == DecodeError::kPayloadTooLarge);
}

BOOST_AUTO_TEST_CASE(validation_order) {
    // Every field bad: magic is reported first, then version, flags, size.
    Header h;
    h.version = 9;
    h.flags = 1;
    h.payload_length = kMaxPayload + 1;
    auto b = encode(h);
    b[0] = 0;
    BOOST_CHECK(decode_header(b).error() == DecodeError::kBadMagic);
    b = encode(h);
    BOOST_CHECK(decode_header(b).error() == DecodeError::kUnsupportedVersion);
    h.version = 1;
    BOOST_CHECK(decode_header(encode(h)).error() == DecodeError::kUnknownFlags);
    h.flags = 0;
    BOOST_CHECK(decode_header(encode(h)).error() == DecodeError::kPayloadTooLarge);
}

BOOST_AUTO_TEST_CASE(error_strings) {
    std::set<std::string_view> seen;
    for (auto e : {DecodeError::kBadMagic, DecodeError::kUnsupportedVersion,
                   DecodeError::kPayloadTooLarge, DecodeError::kUnknownFlags}) {
        auto s = to_string(e);
        BOOST_CHECK(!s.empty());
        seen.insert(s);
    }
    BOOST_CHECK_EQUAL(seen.size(), 4u);
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE(protocol_frame)

BOOST_AUTO_TEST_CASE(encode_frame_layout) {
    Frame f = text_frame(7, "hello");
    f.header.payload_length = 999;  // ignored: encode_frame uses payload.size()
    std::string bytes = encode_frame(f);
    BOOST_REQUIRE_EQUAL(bytes.size(), kHeaderSize + 5);
    BOOST_CHECK_EQUAL(bytes.substr(kHeaderSize), "hello");
    auto d = decode_header(std::span<const std::uint8_t, kHeaderSize>(
        reinterpret_cast<const std::uint8_t*>(bytes.data()), kHeaderSize));
    BOOST_REQUIRE(d.has_value());
    BOOST_CHECK_EQUAL(d->payload_length, 5u);
    BOOST_CHECK_EQUAL(d->message_id, 7u);
}

BOOST_AUTO_TEST_CASE(encode_frame_oversize_throws) {
    Frame f;
    f.payload.assign(kMaxPayload + 1, 'x');
    BOOST_CHECK_THROW(encode_frame(f), ProtocolError);
}

BOOST_AUTO_TEST_CASE(make_frame_and_parse_protobuf) {
    tdr::protocol::v1::WorkerHello hello;
    hello.set_attempt_id("att_123");
    hello.set_token("secret");
    hello.set_pid(4242);
    hello.set_sdk_version("0.1.0");
    Frame f = make_frame(v1::WORKER_HELLO, hello, 11, 3);
    BOOST_CHECK(f.type() == v1::WORKER_HELLO);
    BOOST_CHECK_EQUAL(f.header.message_id, 11u);
    BOOST_CHECK_EQUAL(f.header.correlation_id, 3u);
    BOOST_CHECK_EQUAL(f.header.payload_length, f.payload.size());

    FrameDecoder dec;
    dec.feed(encode_frame(f));
    auto g = dec.next();
    BOOST_REQUIRE(g.has_value());
    BOOST_CHECK(g->header == f.header);
    tdr::protocol::v1::WorkerHello out;
    BOOST_REQUIRE(g->parse(out));
    BOOST_CHECK_EQUAL(out.attempt_id(), "att_123");
    BOOST_CHECK_EQUAL(out.token(), "secret");
    BOOST_CHECK_EQUAL(out.pid(), 4242u);
    BOOST_CHECK_EQUAL(out.sdk_version(), "0.1.0");
}

BOOST_AUTO_TEST_CASE(parse_garbage_fails) {
    Frame f = text_frame(1, std::string("\xff\xff\xff\xff\xff", 5));
    tdr::protocol::v1::WorkerHello out;
    BOOST_CHECK(!f.parse(out));
}

BOOST_AUTO_TEST_CASE(next_message_id_monotonic) {
    std::uint64_t a = next_message_id();
    std::uint64_t b = next_message_id();
    BOOST_CHECK_GE(a, 1u);
    BOOST_CHECK_GT(b, a);

    std::vector<std::uint64_t> ids[4];
    std::vector<std::thread> ts;
    for (auto& v : ids)
        ts.emplace_back([&v] {
            for (int i = 0; i < 1000; ++i) v.push_back(next_message_id());
        });
    for (auto& t : ts) t.join();
    std::set<std::uint64_t> all;
    for (auto& v : ids) all.insert(v.begin(), v.end());
    BOOST_CHECK_EQUAL(all.size(), 4000u);
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE(protocol_decoder)

BOOST_AUTO_TEST_CASE(byte_at_a_time) {
    std::string bytes = encode_frame(text_frame(1, "abc")) + encode_frame(text_frame(2, ""));
    FrameDecoder dec;
    std::vector<Frame> got;
    for (char c : bytes) {
        dec.feed(std::string_view(&c, 1));
        while (auto f = dec.next()) got.push_back(std::move(*f));
    }
    BOOST_REQUIRE_EQUAL(got.size(), 2u);
    BOOST_CHECK_EQUAL(got[0].header.message_id, 1u);
    BOOST_CHECK_EQUAL(got[0].payload, "abc");
    BOOST_CHECK_EQUAL(got[1].header.message_id, 2u);
    BOOST_CHECK_EQUAL(got[1].payload, "");
    BOOST_CHECK_EQUAL(dec.buffered(), 0u);
}

BOOST_AUTO_TEST_CASE(concatenated_frames_one_feed) {
    std::string bytes;
    for (std::uint64_t i = 1; i <= 5; ++i) bytes += encode_frame(text_frame(i, std::string(i * 10, 'z')));
    FrameDecoder dec;
    dec.feed(std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size()));
    for (std::uint64_t i = 1; i <= 5; ++i) {
        auto f = dec.next();
        BOOST_REQUIRE(f.has_value());
        BOOST_CHECK_EQUAL(f->header.message_id, i);
        BOOST_CHECK_EQUAL(f->payload.size(), i * 10);
    }
    BOOST_CHECK(!dec.next().has_value());
}

BOOST_AUTO_TEST_CASE(split_across_feeds) {
    std::string a = encode_frame(text_frame(1, "first-payload"));
    std::string b = encode_frame(text_frame(2, "second"));
    std::string all = a + b;
    // Split in the middle of the first payload and in the middle of the second header.
    std::size_t cut1 = kHeaderSize + 4, cut2 = a.size() + 10;
    FrameDecoder dec;
    dec.feed(all.substr(0, cut1));
    BOOST_CHECK(!dec.next().has_value());
    BOOST_CHECK_EQUAL(dec.buffered(), cut1);
    dec.feed(all.substr(cut1, cut2 - cut1));
    auto f1 = dec.next();
    BOOST_REQUIRE(f1.has_value());
    BOOST_CHECK_EQUAL(f1->payload, "first-payload");
    BOOST_CHECK(!dec.next().has_value());
    dec.feed(all.substr(cut2));
    auto f2 = dec.next();
    BOOST_REQUIRE(f2.has_value());
    BOOST_CHECK_EQUAL(f2->payload, "second");
    BOOST_CHECK(!dec.next().has_value());
}

BOOST_AUTO_TEST_CASE(poisoned_after_error) {
    FrameDecoder dec;
    std::string good = encode_frame(text_frame(1, "ok"));
    std::string bad = good;
    bad[0] = 'X';
    dec.feed(good + bad + good);
    BOOST_REQUIRE(dec.next().has_value());
    BOOST_CHECK_THROW(dec.next(), ProtocolError);
    BOOST_CHECK_THROW(dec.next(), ProtocolError);
    dec.feed(good);
    BOOST_CHECK_THROW(dec.next(), ProtocolError);
}

BOOST_AUTO_TEST_CASE(oversize_header_rejected_before_payload) {
    Header h;
    h.payload_length = kMaxPayload + 1;
    auto b = encode(h);
    FrameDecoder dec;
    dec.feed(std::span<const std::uint8_t>(b));
    BOOST_CHECK_THROW(dec.next(), ProtocolError);
}

BOOST_AUTO_TEST_CASE(buffer_compacts) {
    FrameDecoder dec;
    std::string one = encode_frame(text_frame(1, std::string(1000, 'q')));
    // Keep a partial frame pending at all times so the buffer is never fully drained.
    dec.feed(one.substr(0, 10));
    for (int i = 0; i < 2000; ++i) {
        dec.feed(one.substr(10) + one.substr(0, 10));
        auto f = dec.next();
        BOOST_REQUIRE(f.has_value());
        BOOST_REQUIRE(!dec.next().has_value());
        BOOST_REQUIRE_EQUAL(dec.buffered(), 10u);
    }
    dec.feed(one.substr(10));
    BOOST_CHECK(dec.next().has_value());
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_AUTO_TEST_SUITE(protocol_blocking_io)

BOOST_AUTO_TEST_CASE(write_read_socketpair) {
    SocketPair sp;
    tdr::protocol::v1::WorkerHello hello;
    hello.set_attempt_id("att_x");
    write_frame(sp.fds[0], make_frame(v1::WORKER_HELLO, hello, 5));
    write_frame(sp.fds[0], text_frame(6, "tail"));
    auto f = read_frame(sp.fds[1]);
    BOOST_REQUIRE(f.has_value());
    BOOST_CHECK(f->type() == v1::WORKER_HELLO);
    tdr::protocol::v1::WorkerHello out;
    BOOST_REQUIRE(f->parse(out));
    BOOST_CHECK_EQUAL(out.attempt_id(), "att_x");
    auto g = read_frame(sp.fds[1]);
    BOOST_REQUIRE(g.has_value());
    BOOST_CHECK_EQUAL(g->payload, "tail");
}

BOOST_AUTO_TEST_CASE(large_frame_over_socketpair) {
    // Larger than the socket buffer: exercises short writes/reads.
    SocketPair sp;
    std::string payload(4 * 1024 * 1024, '\0');
    for (std::size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<char>(i * 31);
    std::thread writer([&] { write_frame(sp.fds[0], text_frame(9, payload)); });
    auto f = read_frame(sp.fds[1]);
    writer.join();
    BOOST_REQUIRE(f.has_value());
    BOOST_CHECK(f->payload == payload);
}

BOOST_AUTO_TEST_CASE(clean_eof_is_nullopt) {
    SocketPair sp;
    sp.close(0);
    BOOST_CHECK(!read_frame(sp.fds[1]).has_value());
}

BOOST_AUTO_TEST_CASE(eof_mid_header_throws) {
    SocketPair sp;
    std::string bytes = encode_frame(text_frame(1, "abc"));
    raw_write(sp.fds[0], bytes.substr(0, 20));
    sp.close(0);
    BOOST_CHECK_THROW(read_frame(sp.fds[1]), ProtocolError);
}

BOOST_AUTO_TEST_CASE(eof_mid_payload_throws) {
    SocketPair sp;
    std::string bytes = encode_frame(text_frame(1, "abcdef"));
    raw_write(sp.fds[0], bytes.substr(0, kHeaderSize + 2));
    sp.close(0);
    BOOST_CHECK_THROW(read_frame(sp.fds[1]), ProtocolError);
}

BOOST_AUTO_TEST_CASE(bad_header_throws) {
    SocketPair sp;
    std::string bytes = encode_frame(text_frame(1, "abc"));
    bytes[5] = 7;  // version
    raw_write(sp.fds[0], bytes);
    BOOST_CHECK_THROW(read_frame(sp.fds[1]), ProtocolError);
}

BOOST_AUTO_TEST_CASE(oversize_payload_rejected) {
    SocketPair sp;
    Header h;
    h.payload_length = kMaxPayload + 1;
    auto b = encode(h);
    raw_write(sp.fds[0], std::string_view(reinterpret_cast<const char*>(b.data()), b.size()));
    BOOST_CHECK_THROW(read_frame(sp.fds[1]), ProtocolError);
    Frame big;
    big.payload.assign(kMaxPayload + 1, 'x');
    BOOST_CHECK_THROW(write_frame(sp.fds[0], big), ProtocolError);
}

BOOST_AUTO_TEST_CASE(write_to_closed_peer_is_error_not_sigpipe) {
    SocketPair sp;
    sp.close(1);
    BOOST_CHECK_THROW(write_frame(sp.fds[0], text_frame(1, "x")), std::system_error);
}

BOOST_AUTO_TEST_CASE(write_to_non_socket) {
    int p[2];
    BOOST_REQUIRE_EQUAL(::pipe(p), 0);
    write_frame(p[1], text_frame(3, "pipe"));
    ::close(p[1]);
    auto f = read_frame(p[0]);
    BOOST_REQUIRE(f.has_value());
    BOOST_CHECK_EQUAL(f->payload, "pipe");
    BOOST_CHECK(!read_frame(p[0]).has_value());
    ::close(p[0]);
}

BOOST_AUTO_TEST_SUITE_END()
