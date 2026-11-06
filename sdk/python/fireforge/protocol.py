"""TDR binary envelope: 48-byte big-endian header + protobuf payload.

Layout (docs/design/phase1.md section 2)::

    offset size field
         0    4 magic           0x46465452 ("FFTR")
         4    2 version         1
         6    2 message_type    fireforge.tdr.v1.MessageType
         8    4 flags           v1: must be 0
        12    4 payload_length  <= 64 MiB
        16    8 message_id      sender-unique, starts at 1
        24    8 correlation_id  request's message_id on replies, else 0
        32   16 sender_id       node id; zero on local IPC
        48    n payload

This module depends only on the standard library. ``encode_frame`` accepts
either raw bytes or any object with ``SerializeToString()`` (a protobuf message).
"""

from __future__ import annotations

import itertools
import socket
import struct
import threading
from dataclasses import dataclass

MAGIC = 0x46465452
VERSION = 1
HEADER_SIZE = 48
MAX_PAYLOAD = 64 * 1024 * 1024
SENDER_ID_SIZE = 16
ZERO_SENDER = b"\x00" * SENDER_ID_SIZE

_HEADER = struct.Struct(">IHHIIQQ16s")
assert _HEADER.size == HEADER_SIZE


class ProtocolError(Exception):
    """Malformed frame or broken stream. The connection must be closed."""


@dataclass(frozen=True)
class Header:
    message_type: int
    payload_length: int
    message_id: int
    correlation_id: int = 0
    sender_id: bytes = ZERO_SENDER
    flags: int = 0
    version: int = VERSION
    magic: int = MAGIC

    def pack(self) -> bytes:
        if len(self.sender_id) != SENDER_ID_SIZE:
            raise ValueError(f"sender_id must be {SENDER_ID_SIZE} bytes")
        return _HEADER.pack(
            self.magic,
            self.version,
            self.message_type,
            self.flags,
            self.payload_length,
            self.message_id,
            self.correlation_id,
            self.sender_id,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "Header":
        """Decode and validate a header. Raises ProtocolError."""
        if len(data) != HEADER_SIZE:
            raise ProtocolError(f"header must be {HEADER_SIZE} bytes, got {len(data)}")
        magic, version, mtype, flags, length, mid, cid, sender = _HEADER.unpack(data)
        if magic != MAGIC:
            raise ProtocolError(f"bad magic 0x{magic:08X}")
        if version != VERSION:
            raise ProtocolError(f"unsupported protocol version {version}")
        if flags != 0:
            raise ProtocolError(f"non-zero flags 0x{flags:08X}")
        if length > MAX_PAYLOAD:
            raise ProtocolError(f"payload length {length} exceeds {MAX_PAYLOAD}")
        return cls(
            message_type=mtype,
            payload_length=length,
            message_id=mid,
            correlation_id=cid,
            sender_id=sender,
            flags=flags,
            version=version,
            magic=magic,
        )


def _payload_bytes(message) -> bytes:
    if message is None:
        return b""
    if isinstance(message, (bytes, bytearray, memoryview)):
        return bytes(message)
    return message.SerializeToString()


def encode_frame(msg_type: int, message, message_id: int, correlation_id: int = 0,
                 sender_id: bytes = ZERO_SENDER) -> bytes:
    """Serialize ``message`` (protobuf or bytes) into one complete frame."""
    payload = _payload_bytes(message)
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError(f"payload length {len(payload)} exceeds {MAX_PAYLOAD}")
    header = Header(
        message_type=int(msg_type),
        payload_length=len(payload),
        message_id=message_id,
        correlation_id=correlation_id,
        sender_id=sender_id,
    )
    return header.pack() + payload


class MessageIdCounter:
    """Thread-safe sender-unique message ids, starting at 1."""

    def __init__(self, start: int = 1) -> None:
        self._it = itertools.count(start)
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            return next(self._it)

    __next__ = next


class FrameReader:
    """Reads complete frames from a stream socket."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock

    def _read_exact(self, n: int, *, at_frame_start: bool) -> bytes | None:
        buf = bytearray(n)
        view = memoryview(buf)
        got = 0
        while got < n:
            r = self._sock.recv_into(view[got:], n - got)
            if r == 0:
                if got == 0 and at_frame_start:
                    return None
                raise ProtocolError(f"connection closed mid-frame ({got}/{n} bytes)")
            got += r
        return bytes(buf)

    def read_frame(self) -> tuple[Header, bytes] | None:
        """Return (header, payload), or None on a clean EOF between frames."""
        raw = self._read_exact(HEADER_SIZE, at_frame_start=True)
        if raw is None:
            return None
        header = Header.unpack(raw)
        payload = b""
        if header.payload_length:
            payload = self._read_exact(header.payload_length, at_frame_start=False)
        return header, payload

    def __iter__(self):
        while True:
            frame = self.read_frame()
            if frame is None:
                return
            yield frame


class FrameWriter:
    """Thread-safe frame sender with its own message-id counter."""

    def __init__(self, sock: socket.socket, ids: MessageIdCounter | None = None) -> None:
        self._sock = sock
        self._lock = threading.Lock()
        self.ids = ids or MessageIdCounter()

    def send(self, msg_type: int, message, correlation_id: int = 0, on_id=None) -> int:
        """Send one frame and return its message_id.

        ``on_id(message_id)`` (optional) runs after the id is assigned and
        before the frame is written, so a caller can register for the reply
        before it can possibly arrive.
        """
        with self._lock:
            mid = self.ids.next()
            frame = encode_frame(msg_type, message, mid, correlation_id)
            if on_id is not None:
                on_id(mid)
            self._sock.sendall(frame)
            return mid
