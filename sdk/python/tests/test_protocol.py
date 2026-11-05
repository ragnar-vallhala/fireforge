import socket
import struct
import threading

import pytest

from fireforge import protocol
from fireforge.protocol import (HEADER_SIZE, MAGIC, MAX_PAYLOAD, VERSION, FrameReader, Header,
                                MessageIdCounter, ProtocolError, encode_frame)
from fireforge.tdr.v1 import envelope_pb2, local_pb2


def test_constants():
    assert MAGIC == 0x46465452
    assert VERSION == 1
    assert HEADER_SIZE == 48
    assert MAX_PAYLOAD == 64 * 1024 * 1024


def test_golden_header_bytes():
    sender = bytes(range(0xA0, 0xB0))
    h = Header(message_type=0x1004, payload_length=0x01020304, message_id=0x1122334455667788,
               correlation_id=0x99AABBCCDDEEFF00, sender_id=sender)
    raw = h.pack()
    assert len(raw) == 48
    assert raw[0:4] == b"FFTR" == bytes.fromhex("46465452")       # magic @0
    assert raw[4:6] == bytes.fromhex("0001")                       # version @4
    assert raw[6:8] == bytes.fromhex("1004")                       # message_type @6
    assert raw[8:12] == bytes.fromhex("00000000")                  # flags @8
    assert raw[12:16] == bytes.fromhex("01020304")                 # payload_length @12
    assert raw[16:24] == bytes.fromhex("1122334455667788")         # message_id @16
    assert raw[24:32] == bytes.fromhex("99AABBCCDDEEFF00")         # correlation_id @24
    assert raw[32:48] == sender                                    # sender_id @32
    expected = bytes.fromhex(
        "46465452" "0001" "1004" "00000000" "01020304"
        "1122334455667788" "99AABBCCDDEEFF00"
        "A0A1A2A3A4A5A6A7A8A9AAABACADAEAF")
    assert raw == expected
    assert Header.unpack(raw) == h


def test_encode_frame_hello():
    msg = local_pb2.WorkerHello(attempt_id="att-1", token="t", pid=42, sdk_version="0.1.0")
    frame = encode_frame(envelope_pb2.WORKER_HELLO, msg, message_id=1)
    payload = msg.SerializeToString()
    assert frame[:48] == (bytes.fromhex("46465452" "0001" "1001" "00000000")
                          + struct.pack(">I", len(payload))
                          + (1).to_bytes(8, "big") + bytes(8) + bytes(16))
    assert frame[48:] == payload


def _pair():
    a, b = socket.socketpair()
    a.settimeout(5)
    b.settimeout(5)
    return a, b


def test_round_trip_multiple_frames():
    a, b = _pair()
    msgs = [local_pb2.WorkerProgress(attempt_id="x", fraction=i / 10, message=f"m{i}") for i in range(5)]
    data = b"".join(encode_frame(envelope_pb2.WORKER_PROGRESS, m, i + 1, correlation_id=7)
                    for i, m in enumerate(msgs))
    data += encode_frame(envelope_pb2.WORKER_ACK, b"", 99)
    # Send in awkward chunks from another thread to exercise read_exact.
    def send():
        for i in range(0, len(data), 7):
            a.sendall(data[i:i + 7])
        a.close()
    t = threading.Thread(target=send)
    t.start()
    frames = list(FrameReader(b))
    t.join()
    assert len(frames) == 6
    for i, (h, p) in enumerate(frames[:5]):
        assert h.message_type == envelope_pb2.WORKER_PROGRESS
        assert h.message_id == i + 1 and h.correlation_id == 7
        assert local_pb2.WorkerProgress.FromString(p) == msgs[i]
    assert frames[5][0].message_type == envelope_pb2.WORKER_ACK and frames[5][1] == b""


def test_clean_eof_returns_none():
    a, b = _pair()
    a.close()
    assert FrameReader(b).read_frame() is None


def _bad_header(**overrides):
    fields = dict(magic=MAGIC, version=VERSION, mtype=1, flags=0, length=0, mid=1, cid=0, sender=bytes(16))
    fields.update(overrides)
    return struct.pack(">IHHIIQQ16s", *fields.values())


@pytest.mark.parametrize("overrides,match", [
    (dict(magic=0x46465453), "magic"),
    (dict(version=2), "version"),
    (dict(flags=1), "flags"),
    (dict(length=MAX_PAYLOAD + 1), "exceeds"),
])
def test_header_rejections(overrides, match):
    raw = _bad_header(**overrides)
    with pytest.raises(ProtocolError, match=match):
        Header.unpack(raw)
    a, b = _pair()
    a.sendall(raw)
    with pytest.raises(ProtocolError, match=match):
        FrameReader(b).read_frame()


def test_max_payload_header_accepted():
    assert Header.unpack(_bad_header(length=MAX_PAYLOAD)).payload_length == MAX_PAYLOAD


def test_eof_mid_header():
    a, b = _pair()
    a.sendall(encode_frame(1, b"abc", 1)[:20])
    a.close()
    with pytest.raises(ProtocolError, match="mid-frame"):
        FrameReader(b).read_frame()


def test_eof_mid_payload():
    a, b = _pair()
    a.sendall(encode_frame(1, b"abcdef", 1)[:-2])
    a.close()
    with pytest.raises(ProtocolError, match="mid-frame"):
        FrameReader(b).read_frame()


def test_encode_oversize_rejected(monkeypatch):
    monkeypatch.setattr(protocol, "MAX_PAYLOAD", 4)
    with pytest.raises(ProtocolError):
        encode_frame(1, b"12345", 1)


def test_message_id_counter_threadsafe():
    c = MessageIdCounter()
    assert c.next() == 1
    seen = []
    lock = threading.Lock()

    def work():
        ids = [c.next() for _ in range(1000)]
        with lock:
            seen.extend(ids)
    ts = [threading.Thread(target=work) for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert sorted(seen) == list(range(2, 4002))
