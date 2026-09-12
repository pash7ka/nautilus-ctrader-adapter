"""Capture Spotware's own framing bytes as a test fixture.

Run in a throwaway interpreter that never touches the project environment:

    uv run --isolated --no-project --python 3.12 --with ctrader-open-api \
        python scripts/gen_differential_fixtures.py

`ctrader-open-api` pins protobuf==3.20.1, which cannot coexist with the protobuf this project
requires, so it can never be a project dependency. Capturing its output once preserves the
oracle; the cost is freshness, and regeneration is this one command.

No Twisted reactor is needed. TcpProtocol inherits framing from Int32StringReceiver, so with
a stubbed transport its exact output can be read straight off the wire it thinks it has.
"""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

OUTPUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "differential_frames.json"


class _CaptureTransport:
    """Stands in for a Twisted transport and records what would have been written."""

    def __init__(self) -> None:
        self.written = bytearray()

    def write(self, data: bytes) -> None:
        self.written.extend(data)


class _StubFactory:
    numberOfMessagesToSendPerSecond = 5

    def connected(self, protocol: object) -> None: ...
    def disconnected(self, reason: object) -> None: ...
    def received(self, message: object) -> None: ...


def _cases() -> list[tuple[str, object, str | None]]:
    from ctrader_open_api.messages import OpenApiMessages_pb2 as oa
    from ctrader_open_api.messages import OpenApiModelMessages_pb2 as model

    return [
        (
            "application_auth_req",
            oa.ProtoOAApplicationAuthReq(clientId="client-id", clientSecret="client-secret"),
            "msg-1",
        ),
        (
            "account_auth_req",
            oa.ProtoOAAccountAuthReq(ctidTraderAccountId=1234567, accessToken="access-token"),
            "msg-2",
        ),
        (
            "subscribe_spots_req",
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=1234567, symbolId=[1, 22, 333]),
            "msg-3",
        ),
        (
            "get_trendbars_req",
            oa.ProtoOAGetTrendbarsReq(
                ctidTraderAccountId=1234567,
                fromTimestamp=1_700_000_000_000,
                toTimestamp=1_700_003_600_000,
                period=model.ProtoOATrendbarPeriod.Value("M15"),
                symbolId=1,
                count=500,
            ),
            "msg-4",
        ),
        (
            "refresh_token_req",
            oa.ProtoOARefreshTokenReq(refreshToken="refresh-token"),
            "msg-5",
        ),
        ("heartbeat_no_client_msg_id", None, None),
    ]


def main() -> int:
    from ctrader_open_api.messages import OpenApiCommonMessages_pb2 as common
    from ctrader_open_api.tcpProtocol import TcpProtocol
    from google.protobuf import __version__ as protobuf_version

    entries = []
    for name, message, client_msg_id in _cases():
        payload = common.ProtoHeartbeatEvent() if message is None else message

        protocol = TcpProtocol()
        protocol.factory = _StubFactory()
        transport = _CaptureTransport()
        protocol.transport = transport
        protocol.send(payload, instant=True, clientMsgId=client_msg_id)

        entries.append(
            {
                "name": name,
                "payload_type": int(payload.payloadType),
                "payload_class": type(payload).__name__,
                "client_msg_id": client_msg_id,
                "payload_bytes": base64.b64encode(payload.SerializeToString()).decode(),
                "frame": base64.b64encode(bytes(transport.written)).decode(),
            },
        )

    document = {
        "generated_by": "scripts/gen_differential_fixtures.py",
        "source_package": "ctrader-open-api",
        "python": sys.version.split()[0],
        "protobuf": protobuf_version,
        "note": (
            "Frames as produced by Spotware's TcpProtocol. Regenerate with: uv run --isolated "
            "--no-project --python 3.12 --with ctrader-open-api python "
            "scripts/gen_differential_fixtures.py"
        ),
        "frames": entries,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(entries)} frames to {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
