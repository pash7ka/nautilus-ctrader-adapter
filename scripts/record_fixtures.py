"""Read-only fixture recorder for M2 (instruments and market data).

Records a scrubbed snapshot of what a real broker connection returns for trader info, assets,
symbols, symbol specs, an EUR->USD conversion chain, trendbars and a short window of live spot
ticks, plus the account list. Every recorded message is scrubbed of real account ids, trader
logins, tokens and broker names before it ever reaches disk, and the final JSON is checked
against every real identifying value observed during the run before it is written.

This is read-only against the broker: it authenticates and subscribes to public market data,
places no orders and changes no account state.

Run once per fixture refresh, from the repository root:

    uv run python scripts/record_fixtures.py --account-id <id>

The account id is passed on the command line and never written anywhere. Overwrites
`tests/fixtures/m2_recorded.json`.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import importlib.util
import json
import pathlib
import sys
import time
from collections.abc import Iterable

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message

from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.constants import BUCKET_HISTORICAL, DEMO_HOST, LIVE_HOST, PROTOBUF_PORT
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

# scripts/ is not a package; get_tokens.py is loaded by file path, exactly as
# tests/test_get_tokens.py does, to reuse its load_env() without duplicating it.
_GET_TOKENS_SPEC = importlib.util.spec_from_file_location(
    "get_tokens",
    pathlib.Path(__file__).resolve().with_name("get_tokens.py"),
)
get_tokens = importlib.util.module_from_spec(_GET_TOKENS_SPEC)
sys.modules[_GET_TOKENS_SPEC.name] = get_tokens
_GET_TOKENS_SPEC.loader.exec_module(get_tokens)

FAKE_ACCOUNT_ID = 1_000_001
FAKE_TRADER_LOGIN = 2_000_002
FAKE_TOKEN = "scrubbed-token"

SYMBOLS = ("EURUSD", "XAUUSD", "GER40.cash", "US100.cash")
SPOT_RECORD_SECS = 150.0  # covers at least one M1 bar-to-bar transition

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_OUTPUT_PATH = _REPO_ROOT / "tests" / "fixtures" / "m2_recorded.json"

# Cleared outright wherever they occur: personal or non-protocol account state that the
# scrubbed fixtures have no need to carry.
_CLEARED_FIELDS = frozenset(
    {
        "brokerName",
        "brokerTitleShort",
        "balance",
        "balanceVersion",
        "managerBonus",
        "ibBonus",
        "nonWithdrawableBonus",
        "registrationTimestamp",
    }
)

_IDENTIFYING_INT_TYPES = (FieldDescriptor.TYPE_INT64, FieldDescriptor.TYPE_UINT64)


class ScrubError(RuntimeError):
    """`assert_clean` found a forbidden identifier left in the data about to be written."""


def scrub(message: Message, real_account_id: int, real_login: int | None) -> Message:
    """Return a scrubbed copy of `message`.

    Recursively walks every field of a `CopyFrom` copy: any int64/uint64 field named
    `ctidTraderAccountId` becomes `FAKE_ACCOUNT_ID`, `traderLogin` becomes `FAKE_TRADER_LOGIN`,
    `accessToken`/`refreshToken` become `FAKE_TOKEN`, and `_CLEARED_FIELDS` are cleared -
    wherever any of these occur, at any nesting depth. As a second pass, independent of field
    name, any other int64/uint64 scalar still carrying exactly `real_account_id` or
    `real_login` is replaced too, so a field this function does not yet know the name of can
    never carry a real identifier through unnoticed.
    """
    result = type(message)()
    result.CopyFrom(message)
    _scrub_in_place(result, real_account_id, real_login)
    return result


def _scrub_in_place(message: Message, real_account_id: int, real_login: int | None) -> None:
    for field, value in list(message.ListFields()):
        name = field.name
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            items = value if field.label == FieldDescriptor.LABEL_REPEATED else (value,)
            for item in items:
                _scrub_in_place(item, real_account_id, real_login)
            continue

        if name == "ctidTraderAccountId":
            setattr(message, name, FAKE_ACCOUNT_ID)
        elif name == "traderLogin":
            setattr(message, name, FAKE_TRADER_LOGIN)
        elif name in ("accessToken", "refreshToken"):
            setattr(message, name, FAKE_TOKEN)
        elif name in _CLEARED_FIELDS:
            message.ClearField(name)
        elif field.type in _IDENTIFYING_INT_TYPES and field.label != FieldDescriptor.LABEL_REPEATED:
            if real_account_id is not None and value == real_account_id:
                setattr(message, name, FAKE_ACCOUNT_ID)
            elif real_login is not None and value == real_login:
                setattr(message, name, FAKE_TRADER_LOGIN)


def assert_clean(blob: bytes, forbidden: Iterable[bytes]) -> None:
    """Raise `ScrubError` if any forbidden byte string occurs in `blob`."""
    for needle in forbidden:
        if needle and needle in blob:
            raise ScrubError(
                f"forbidden identifier (length {len(needle)}) found in recorded fixture data",
            )


class _QuietLogger:
    """Enough of the Nautilus `Logger` interface for `CTraderConnection`.

    Debug and info are dropped - a one-shot recording script has no use for per-message
    envelope noise - so only warnings and errors reach stderr.
    """

    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        print(f"warning: {message}", file=sys.stderr)

    def error(self, message: str) -> None:
        print(f"error: {message}", file=sys.stderr)

    def exception(self, message: str, ex: BaseException) -> None:
        print(f"error: {message}: {type(ex).__name__}", file=sys.stderr)


def _light_symbol_id(symbol: om.ProtoOALightSymbol) -> int:
    return symbol.symbolId


class RecordedSecrets:
    """Every real identifying value observed during a `record()` run.

    Kept separate from the recorded messages themselves so `assert_clean` has a plain list to
    check against, without having to re-derive these from already-scrubbed data.

    A plain class, not `@dataclass`: this module is loaded by file path without being
    registered in `sys.modules` (see `tests/test_record_fixtures.py`), which `@dataclass`
    cannot handle - it resolves each field's annotation via `sys.modules[cls.__module__]`.
    """

    def __init__(
        self,
        *,
        account_id: int,
        real_login: int | None,
        access_token: str,
        refresh_token: str,
        client_id: str,
        broker_name: str | None,
        broker_title_short: str | None,
    ) -> None:
        self.account_id = account_id
        self.real_login = real_login
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.broker_name = broker_name
        self.broker_title_short = broker_title_short

    def forbidden_bytes(self) -> list[bytes]:
        values = [
            str(self.account_id),
            str(self.real_login) if self.real_login is not None else None,
            self.access_token,
            self.refresh_token or None,
            self.client_id,
            self.broker_name,
            self.broker_title_short,
        ]
        return [v.encode() for v in values if v]


class RecordResult:
    """Same non-`@dataclass` constraint as `RecordedSecrets` applies here."""

    def __init__(self, messages: dict[str, list[Message]], secrets: RecordedSecrets) -> None:
        self.messages = messages
        self.secrets = secrets


async def _fetch_account_list(
    access_token: str,
    client_id: str,
    client_secret: str,
) -> oa.ProtoOAGetAccountListByAccessTokenRes:
    """Pre-connect to the demo host just to list the granted accounts.

    An account's environment (demo or live) is only known from its own `isLive` flag, not from
    how its token was issued - the account list itself is served on either host.
    """
    connection = CTraderConnection(DEMO_HOST, PROTOBUF_PORT, logger=_QuietLogger())
    await connection.connect()
    try:
        await connection.request(
            oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
        )
        return await connection.request(
            oa.ProtoOAGetAccountListByAccessTokenReq(accessToken=access_token),
        )
    finally:
        await connection.close()


async def record(account_id: int, env: dict[str, str]) -> RecordResult:
    """Run the full read-only recording flow and return the collected, unscrubbed messages.

    `env` is the already-loaded `.env` file (see `main`) - loading it is a blocking filesystem
    read, kept out of this `async def` rather than run from inside it.

    The caller is responsible for scrubbing and writing the result; keeping that split lets the
    scrubbing/assert_clean pass be reasoned about independently of the network flow that fills
    the result.
    """
    client_id = env["CTRADER_CLIENT_ID"]
    client_secret = env["CTRADER_CLIENT_SECRET"]
    access_token = env["CTRADER_ACCESS_TOKEN"]
    refresh_token = env.get("CTRADER_REFRESH_TOKEN", "")

    account_list_res = await _fetch_account_list(access_token, client_id, client_secret)
    target = next(
        (a for a in account_list_res.ctidTraderAccount if a.ctidTraderAccountId == account_id),
        None,
    )
    if target is None:
        raise RuntimeError("the access token does not grant the requested account id")
    real_login = target.traderLogin if target.HasField("traderLogin") else None
    host = LIVE_HOST if target.isLive else DEMO_HOST

    recorded: dict[str, list[Message]] = {
        "account_list": [account_list_res],
        "trader": [],
        "assets": [],
        "symbols": [],
        "symbol_specs": [],
        "conversion_eur_usd": [],
        "trendbars_m15": [],
        "trendbars_h1": [],
        "spot_events": [],
    }

    connection = CTraderConnection(host, PROTOBUF_PORT, logger=_QuietLogger(), tls=True)
    await connection.connect()
    try:
        await connection.request(
            oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
        )
        await connection.request(
            oa.ProtoOAAccountAuthReq(ctidTraderAccountId=account_id, accessToken=access_token),
        )

        trader_res = await connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=account_id))
        recorded["trader"].append(trader_res)

        assets_res = await connection.request(
            oa.ProtoOAAssetListReq(ctidTraderAccountId=account_id),
        )
        recorded["assets"].append(assets_res)
        asset_id_by_name = {asset.name: asset.assetId for asset in assets_res.asset}
        eur_asset_id = asset_id_by_name["EUR"]
        usd_asset_id = asset_id_by_name["USD"]

        conversion_res = await connection.request(
            oa.ProtoOASymbolsForConversionReq(
                ctidTraderAccountId=account_id,
                firstAssetId=eur_asset_id,
                lastAssetId=usd_asset_id,
            ),
        )
        recorded["conversion_eur_usd"].append(conversion_res)
        chain_symbol_ids = {_light_symbol_id(s) for s in conversion_res.symbol}

        all_symbols_res = await connection.request(
            oa.ProtoOASymbolsListReq(ctidTraderAccountId=account_id),
        )
        wanted_symbols = [
            s
            for s in all_symbols_res.symbol
            if s.symbolName in SYMBOLS or _light_symbol_id(s) in chain_symbol_ids
        ]
        filtered_symbols_res = oa.ProtoOASymbolsListRes(
            ctidTraderAccountId=account_id,
            symbol=wanted_symbols,
        )
        recorded["symbols"].append(filtered_symbols_res)

        target_symbol_id_by_name = {
            s.symbolName: _light_symbol_id(s) for s in wanted_symbols if s.symbolName in SYMBOLS
        }
        all_wanted_ids = [_light_symbol_id(s) for s in wanted_symbols]
        symbol_specs_res = await connection.request(
            oa.ProtoOASymbolByIdReq(ctidTraderAccountId=account_id, symbolId=all_wanted_ids),
        )
        recorded["symbol_specs"].append(symbol_specs_res)

        now_ms = int(time.time() * 1000)
        day_ms = 24 * 60 * 60 * 1000
        for symbol_name in SYMBOLS:
            symbol_id = target_symbol_id_by_name[symbol_name]
            m15_res = await connection.request(
                oa.ProtoOAGetTrendbarsReq(
                    ctidTraderAccountId=account_id,
                    symbolId=symbol_id,
                    period=om.M15,
                    count=50,
                    fromTimestamp=now_ms - 7 * day_ms,
                    toTimestamp=now_ms,
                ),
                bucket=BUCKET_HISTORICAL,
            )
            recorded["trendbars_m15"].append(m15_res)

            h1_res = await connection.request(
                oa.ProtoOAGetTrendbarsReq(
                    ctidTraderAccountId=account_id,
                    symbolId=symbol_id,
                    period=om.H1,
                    count=50,
                    fromTimestamp=now_ms - 21 * day_ms,
                    toTimestamp=now_ms,
                ),
                bucket=BUCKET_HISTORICAL,
            )
            recorded["trendbars_h1"].append(h1_res)

        spot_symbol_ids = [
            target_symbol_id_by_name["EURUSD"],
            target_symbol_id_by_name["GER40.cash"],
        ]
        spot_events: list[Message] = []

        def _on_event(message: Message) -> None:
            if isinstance(message, oa.ProtoOASpotEvent):
                spot_events.append(message)

        connection.set_event_handler(_on_event)

        await connection.request(
            oa.ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=account_id,
                symbolId=spot_symbol_ids,
                subscribeToSpotTimestamp=True,
            ),
        )
        for symbol_id in spot_symbol_ids:
            await connection.request(
                oa.ProtoOASubscribeLiveTrendbarReq(
                    ctidTraderAccountId=account_id,
                    symbolId=symbol_id,
                    period=om.M1,
                ),
            )

        await asyncio.sleep(SPOT_RECORD_SECS)
        recorded["spot_events"] = list(spot_events)

        for symbol_id in spot_symbol_ids:
            await connection.request(
                oa.ProtoOAUnsubscribeLiveTrendbarReq(
                    ctidTraderAccountId=account_id,
                    symbolId=symbol_id,
                    period=om.M1,
                ),
            )
        await connection.request(
            oa.ProtoOAUnsubscribeSpotsReq(ctidTraderAccountId=account_id, symbolId=spot_symbol_ids),
        )
    finally:
        await connection.close()

    secrets = RecordedSecrets(
        account_id=account_id,
        real_login=real_login,
        access_token=access_token,
        refresh_token=refresh_token,
        client_id=client_id,
        broker_name=(
            trader_res.trader.brokerName if trader_res.trader.HasField("brokerName") else None
        ),
        broker_title_short=(
            target.brokerTitleShort if target.HasField("brokerTitleShort") else None
        ),
    )
    return RecordResult(messages=recorded, secrets=secrets)


def _encode(message: Message) -> dict:
    return {
        "type": message.payloadType,
        "payload": base64.b64encode(message.SerializeToString()).decode("ascii"),
    }


async def _run(account_id: int, env: dict[str, str]) -> None:
    result = await record(account_id, env)
    real_login = result.secrets.real_login

    scrubbed = {
        key: [scrub(message, account_id, real_login) for message in messages]
        for key, messages in result.messages.items()
    }
    output = {key: [_encode(message) for message in messages] for key, messages in scrubbed.items()}
    output_bytes = json.dumps(output, indent=2).encode("utf-8")

    assert_clean(output_bytes, result.secrets.forbidden_bytes())

    _OUTPUT_PATH.write_bytes(output_bytes)

    for key, messages in scrubbed.items():
        print(f"{key}: {len(messages)}")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record a scrubbed snapshot of broker responses into tests/fixtures/m2_recorded.json."
        ),
    )
    parser.add_argument("--account-id", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    env = get_tokens.load_env(_REPO_ROOT / ".env")
    asyncio.run(_run(args.account_id, env))
    return 0


if __name__ == "__main__":
    sys.exit(main())
