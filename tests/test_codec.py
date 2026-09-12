"""Framing and envelope encoding. Pure and synchronous - no sockets, no event loop."""

import struct

import pytest

from nautilus_ctrader.common import codec
from nautilus_ctrader.common.errors import CTraderProtocolError
from nautilus_ctrader.constants import MAX_FRAME_BYTES
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model


def test_encode_frame_is_big_endian_length_then_body() -> None:
    payload = oa.ProtoOAApplicationAuthReq(clientId="id", clientSecret="secret")
    frame = codec.encode_frame(payload, "msg-1")

    declared = struct.unpack("!I", frame[:4])[0]
    assert declared == len(frame) - 4

    envelope = codec.decode_envelope(frame[4:])
    assert envelope.payloadType == oa_model.PROTO_OA_APPLICATION_AUTH_REQ
    assert envelope.clientMsgId == "msg-1"


def test_client_msg_id_is_absent_when_not_given() -> None:
    frame = codec.encode_frame(oa.ProtoOAApplicationAuthReq(clientId="i", clientSecret="s"))
    envelope = codec.decode_envelope(frame[4:])
    assert not envelope.HasField("clientMsgId")


def test_decode_length_reads_the_prefix() -> None:
    assert codec.decode_length(struct.pack("!I", 1234)) == 1234


def test_decode_length_rejects_a_short_prefix() -> None:
    with pytest.raises(CTraderProtocolError, match="4 bytes"):
        codec.decode_length(b"\x00\x00")


def test_decode_length_rejects_an_oversized_frame() -> None:
    with pytest.raises(CTraderProtocolError, match="exceeds maximum"):
        codec.decode_length(struct.pack("!I", MAX_FRAME_BYTES + 1))


def test_decode_envelope_rejects_undecodable_bytes() -> None:
    with pytest.raises(CTraderProtocolError):
        codec.decode_envelope(b"\xff\xff\xff\xff\xff\xff")


def test_decode_envelope_rejects_a_missing_payload_type() -> None:
    # payloadType is `required` in the proto2 schema, so an envelope without it is invalid.
    with pytest.raises(CTraderProtocolError, match="payloadType"):
        codec.decode_envelope(b"")
