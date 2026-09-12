"""Frame and envelope encoding for the cTrader Open API.

Pure and synchronous: no sockets, no event loop. Every wire-format decision lives here, so
this is the one module that can be compared byte-for-byte against Spotware's implementation.
"""

from __future__ import annotations

import struct

from google.protobuf.message import DecodeError, Message

from nautilus_ctrader.common.errors import CTraderProtocolError
from nautilus_ctrader.constants import LENGTH_PREFIX_BYTES, LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES
from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common


def encode_envelope(payload: Message, client_msg_id: str | None = None) -> bytes:
    """Wrap a payload message in a `ProtoMessage` envelope and serialise it."""
    envelope = common.ProtoMessage(
        payloadType=payload.payloadType,
        payload=payload.SerializeToString(),
    )
    if client_msg_id is not None:
        envelope.clientMsgId = client_msg_id
    return envelope.SerializeToString()


def encode_frame(payload: Message, client_msg_id: str | None = None) -> bytes:
    """Serialise a payload message into a complete length-prefixed frame."""
    body = encode_envelope(payload, client_msg_id)
    return struct.pack(LENGTH_PREFIX_FORMAT, len(body)) + body


def decode_length(prefix: bytes) -> int:
    """Read a frame's declared body length from its length prefix."""
    if len(prefix) != LENGTH_PREFIX_BYTES:
        raise CTraderProtocolError(
            f"length prefix must be {LENGTH_PREFIX_BYTES} bytes, got {len(prefix)}",
        )
    (length,) = struct.unpack(LENGTH_PREFIX_FORMAT, prefix)
    if length > MAX_FRAME_BYTES:
        raise CTraderProtocolError(f"frame length {length} exceeds maximum {MAX_FRAME_BYTES}")
    return length


def decode_envelope(body: bytes) -> common.ProtoMessage:
    """Parse a frame body into its `ProtoMessage` envelope."""
    envelope = common.ProtoMessage()
    try:
        envelope.ParseFromString(body)
    except (DecodeError, ValueError) as e:
        raise CTraderProtocolError(f"undecodable envelope of {len(body)} bytes") from e
    if not envelope.IsInitialized():
        raise CTraderProtocolError("envelope is missing its required payloadType")
    return envelope
