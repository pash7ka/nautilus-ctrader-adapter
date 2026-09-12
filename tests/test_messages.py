"""The vendored schema generates importable bindings with the payload types we rely on."""

from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiCommonModelMessages_pb2 as common_model
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model


def test_envelope_round_trips() -> None:
    envelope = common.ProtoMessage(payloadType=42, payload=b"body", clientMsgId="abc")
    parsed = common.ProtoMessage.FromString(envelope.SerializeToString())
    assert parsed.payloadType == 42
    assert parsed.payload == b"body"
    assert parsed.clientMsgId == "abc"


def test_common_payload_types() -> None:
    assert common_model.HEARTBEAT_EVENT == 51
    assert common_model.ERROR_RES == 50
    assert common_model.BLOCKED_PAYLOAD_TYPE == 11


def test_open_api_payload_types() -> None:
    assert oa_model.PROTO_OA_APPLICATION_AUTH_REQ == 2100
    assert oa_model.PROTO_OA_ACCOUNT_AUTH_REQ == 2102
    assert oa_model.PROTO_OA_ERROR_RES == 2142
    assert oa_model.PROTO_OA_REFRESH_TOKEN_REQ == 2173


def test_messages_carry_their_own_payload_type_default() -> None:
    assert oa.ProtoOAApplicationAuthReq().payloadType == oa_model.PROTO_OA_APPLICATION_AUTH_REQ
    assert common.ProtoHeartbeatEvent().payloadType == common_model.HEARTBEAT_EVENT


def test_error_response_carries_retry_after() -> None:
    # retryAfter is field 6 and is absent from the released ctrader-open-api package,
    # which is one reason bindings are generated here rather than imported.
    error = oa.ProtoOAErrorRes(errorCode="BLOCKED_PAYLOAD_TYPE", retryAfter=7)
    assert oa.ProtoOAErrorRes.FromString(error.SerializeToString()).retryAfter == 7
