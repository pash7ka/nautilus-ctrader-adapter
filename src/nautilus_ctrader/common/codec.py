"""Frame and envelope encoding for the cTrader Open API.

Pure and synchronous: no sockets, no event loop. Every wire-format decision lives here, so
this is the one module that can be compared byte-for-byte against Spotware's implementation.
"""

from __future__ import annotations

import struct

from google.protobuf.message import DecodeError, EncodeError, Message

from nautilus_ctrader.common.errors import CTraderProtocolError, CTraderRequestError
from nautilus_ctrader.constants import LENGTH_PREFIX_BYTES, LENGTH_PREFIX_FORMAT, MAX_FRAME_BYTES
from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa


def encode_envelope(payload: Message, client_msg_id: str | None = None) -> bytes:
    """Wrap a payload message in a `ProtoMessage` envelope and serialise it.

    Raises `CTraderProtocolError` if a required field is missing.
    """
    try:
        body = payload.SerializeToString()
    except EncodeError as e:
        # The message names the missing fields, never their values.
        raise CTraderProtocolError(f"cannot encode payloadType={payload.payloadType}: {e}") from e
    envelope = common.ProtoMessage(payloadType=payload.payloadType, payload=body)
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


_MILLISECONDS_PER_SECOND = 1000

_registry: dict[int, type[Message]] | None = None


def _build_registry() -> dict[int, type[Message]]:
    registry: dict[int, type[Message]] = {}
    for module in (common, oa):
        for name in dir(module):
            if not name.startswith("Proto"):
                continue
            candidate = getattr(module, name)
            if not isinstance(candidate, type) or not issubclass(candidate, Message):
                continue
            field = candidate.DESCRIPTOR.fields_by_name.get("payloadType")
            # ProtoMessage's payloadType is `required` with no default and would otherwise
            # register itself under 0, shadowing real lookups.
            if field is None or not field.has_default_value:
                continue
            registry[candidate().payloadType] = candidate
    return registry


def payload_class(payload_type: int) -> type[Message]:
    """Return the message class for a payload type."""
    global _registry
    if _registry is None:
        _registry = _build_registry()
    try:
        return _registry[payload_type]
    except KeyError as e:
        raise CTraderProtocolError(f"unknown payload type {payload_type}") from e


def parse_payload(envelope: common.ProtoMessage) -> Message:
    """Parse an envelope's payload into its typed message."""
    message = payload_class(envelope.payloadType)()
    try:
        message.ParseFromString(envelope.payload)
    except (DecodeError, ValueError) as e:
        raise CTraderProtocolError(
            f"undecodable payload for type {envelope.payloadType}",
        ) from e
    return message


def as_request_error(payload: Message) -> CTraderRequestError | None:
    """Map an error payload to an exception, or return None if it is not one.

    The two error messages document `maintenanceEndTimestamp` in different units -
    milliseconds for `ProtoErrorRes`, seconds for `ProtoOAErrorRes` - and nothing on the wire
    distinguishes them. Each is read per its own documented unit and normalised to seconds.
    TODO(verify): the first observed maintenance window on a live connection settles this.
    """
    if isinstance(payload, oa.ProtoOAErrorRes):
        return CTraderRequestError(
            payload.errorCode,
            payload.description or None,
            maintenance_end_secs=(
                payload.maintenanceEndTimestamp
                if payload.HasField("maintenanceEndTimestamp")
                else None
            ),
            retry_after_secs=payload.retryAfter if payload.HasField("retryAfter") else None,
        )
    if isinstance(payload, common.ProtoErrorRes):
        return CTraderRequestError(
            payload.errorCode,
            payload.description or None,
            maintenance_end_secs=(
                payload.maintenanceEndTimestamp // _MILLISECONDS_PER_SECOND
                if payload.HasField("maintenanceEndTimestamp")
                else None
            ),
        )
    return None
