"""Our framing must agree byte-for-byte with Spotware's own implementation.

The fixture was captured from `ctrader-open-api` in an isolated interpreter, because that
package pins protobuf==3.20.1 and cannot coexist with the protobuf this project requires.
Regenerate with scripts/gen_differential_fixtures.py.
"""

import base64
import json
from pathlib import Path

import pytest

from nautilus_ctrader.common import codec

FIXTURE = Path(__file__).parent / "fixtures" / "differential_frames.json"


def _frames() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["frames"]


@pytest.mark.parametrize("case", _frames(), ids=lambda c: c["name"])
def test_our_encoding_matches_spotware(case: dict) -> None:
    payload = codec.payload_class(case["payload_type"])()
    payload.ParseFromString(base64.b64decode(case["payload_bytes"]))

    ours = codec.encode_frame(payload, case["client_msg_id"])
    assert ours == base64.b64decode(case["frame"])


@pytest.mark.parametrize("case", _frames(), ids=lambda c: c["name"])
def test_we_decode_what_spotware_encoded(case: dict) -> None:
    frame = base64.b64decode(case["frame"])

    length = codec.decode_length(frame[:4])
    assert length == len(frame) - 4

    envelope = codec.decode_envelope(frame[4:])
    assert envelope.payloadType == case["payload_type"]
    assert (envelope.clientMsgId or None) == case["client_msg_id"]

    parsed = codec.parse_payload(envelope)
    assert type(parsed).__name__ == case["payload_class"]
