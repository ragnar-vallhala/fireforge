"""Import the generated protobuf modules with a helpful error if they are missing."""

try:
    from fireforge.tdr.v1 import envelope_pb2, local_pb2  # noqa: F401
except ImportError as exc:  # pragma: no cover - depends on the checkout state
    raise ImportError(
        "fireforge: generated protobuf modules (fireforge/tdr/v1/*_pb2.py) are "
        "missing or broken; run tools/gen_proto.sh from the repository root "
        f"({exc})"
    ) from exc

MessageType = envelope_pb2.MessageType

WORKER_HELLO = envelope_pb2.WORKER_HELLO
WORKER_TASK = envelope_pb2.WORKER_TASK
WORKER_PROGRESS = envelope_pb2.WORKER_PROGRESS
WORKER_RESULT = envelope_pb2.WORKER_RESULT
WORKER_CANCEL = envelope_pb2.WORKER_CANCEL
WORKER_ACK = envelope_pb2.WORKER_ACK
ERROR = envelope_pb2.ERROR


def type_name(msg_type: int) -> str:
    try:
        return MessageType.Name(msg_type)
    except ValueError:
        return f"0x{msg_type:04X}"
