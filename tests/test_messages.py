"""The vendored schema generates importable bindings with the payload types we rely on."""

import importlib.util
import pathlib
import sys

from nautilus_ctrader.messages import OpenApiCommonMessages_pb2 as common
from nautilus_ctrader.messages import OpenApiCommonModelMessages_pb2 as common_model
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model

_SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "gen_protobuf.py"
_SPEC = importlib.util.spec_from_file_location("gen_protobuf", _SCRIPT_PATH)
gen_protobuf = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gen_protobuf
_SPEC.loader.exec_module(gen_protobuf)


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


def test_protobuf_floor_covers_every_generated_module() -> None:
    floor, ceiling = gen_protobuf.declared_protobuf()
    generated = sorted(gen_protobuf.MESSAGES_DIR.glob("*_pb2.py"))
    assert generated
    for path in generated:
        version = gen_protobuf.generated_version(path)
        assert version is not None, path.name
        assert version <= floor, path.name
    # Policy, not a protobuf rule: a new major may drop APIs, as 7 dropped FieldDescriptor.label.
    assert ceiling == floor[0] + 1


def test_generator_refuses_a_floor_older_than_the_generated_code(monkeypatch) -> None:
    monkeypatch.setattr(gen_protobuf, "declared_protobuf", lambda: ((7, 0, 0), 8))
    assert "raise the floor" in gen_protobuf._check_versions()
