"""Loader for the recorded M2 fixtures (`m2_recorded.json`).

The file itself is produced offline by `scripts/record_fixtures.py`, run once against a real,
read-only broker connection; it is not generated as part of the test suite.
"""

from __future__ import annotations

import base64
import json
import pathlib

from nautilus_ctrader.common import codec

FAKE_ACCOUNT_ID = 1_000_001
FAKE_TRADER_LOGIN = 2_000_002

_M2 = pathlib.Path(__file__).with_name("m2_recorded.json")


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
