"""Loaders for the recorded fixtures.

- `m2_recorded.json`: market data, made by `scripts/record_fixtures.py`;
- `m3_execution_recorded.json`: a manual trading session, made by `scripts/record_execution.py`;
- `m3_stage2_recorded.json`: a session of the node's own orders on EURUSD, one of its positions
  partly and then fully closed by hand, made by the same script.

All are produced offline against a real broker connection and scrubbed; they are not generated
as part of the test suite.
"""

from __future__ import annotations

import base64
import json
import pathlib

from nautilus_ctrader.common import codec

FAKE_ACCOUNT_ID = 1_000_001
FAKE_TRADER_LOGIN = 2_000_002

_M2 = pathlib.Path(__file__).with_name("m2_recorded.json")
_M3_EXECUTION = pathlib.Path(__file__).with_name("m3_execution_recorded.json")
_M3_STAGE2 = pathlib.Path(__file__).with_name("m3_stage2_recorded.json")


def load_recorded() -> dict[str, list]:
    """Load and parse `m2_recorded.json` into typed protobuf messages, keyed as it was recorded."""
    try:
        raw = json.loads(_M2.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"{_M2} is missing; generate it with "
            "'uv run python scripts/record_fixtures.py --account-id <id>'",
        ) from e

    out: dict[str, list] = {}
    for key, items in raw.items():
        messages = []
        for item in items:
            cls = codec.payload_class(item["type"])
            message = cls()
            message.ParseFromString(base64.b64decode(item["payload"]))
            messages.append(message)
        out[key] = messages
    return out


def load_execution_recording(path: pathlib.Path = _M3_EXECUTION) -> dict:
    """A recorded execution session, with every payload parsed into its protobuf message.

    `timeline` keeps the order and spacing in which the venue sent its events; `closing` holds
    the deal, order and cash-flow lists asked for at the end.
    """

    def decode(item: dict):
        message = codec.payload_class(item["type"])()
        message.ParseFromString(base64.b64decode(item["payload"]))
        return message

    raw = json.loads(path.read_text(encoding="utf-8"))
    return {
        "format": raw["format"],
        "timeline": [
            {
                "t": item["t"],
                "kind": item["kind"],
                "note": item["note"],
                "message": decode(item) if "payload" in item else None,
            }
            for item in raw["timeline"]
        ],
        "closing": {key: [decode(item) for item in items] for key, items in raw["closing"].items()},
    }


def load_stage2_recording() -> dict:
    """`m3_stage2_recorded.json`, shaped as `load_execution_recording()` returns it."""
    return load_execution_recording(_M3_STAGE2)
