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
from nautilus_ctrader.common.errors import CTraderRequestError
from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.constants import (
    BUCKET_DEFAULT,
    BUCKET_HISTORICAL,
    DEMO_HOST,
    LIVE_HOST,
    PROTOBUF_PORT,
)
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

# Conservative outbound rates for the recorder's connection: the historical bucket in
# particular is much tighter than the default one, and a burst of trendbar requests (one per
# symbol, M15 and H1) is exactly what triggered a live BLOCKED_PAYLOAD_TYPE rejection.
_RATE_LIMITS = {BUCKET_DEFAULT: 5.0, BUCKET_HISTORICAL: 1.0}
_MAX_RATE_LIMIT_RETRIES = 3
_RATE_LIMIT_FALLBACK_WAIT_SECS = 2.0

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
FAKE_BALANCE = 1_000_000

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


def _varint(n: int) -> bytes:
    """Encode `n` as a protobuf varint - the wire encoding of an int64/uint64 scalar field.

    Used to search for a real account id or trader login inside a message's raw serialized
    bytes: the base64-encoded JSON built from those bytes can never contain a literal decimal
    match for a value that is actually encoded as binary.
    """
    if n < 0:
        raise ValueError("_varint does not support negative numbers")
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


# proto2 `required` fields cannot just be cleared: an unset required field makes the whole
# message fail SerializeToString(). Cleared fields that turn out to be required get one of
# these fixed fake values instead (0 for any not listed here) so the message stays valid.
_FAKE_REQUIRED_VALUES: dict[str, object] = {"balance": FAKE_BALANCE}


class ScrubError(RuntimeError):
    """`assert_clean` found a forbidden identifier left in the data about to be written."""


def scrub(message: Message, real_account_id: int, real_login: int | None) -> Message:
    """Return a scrubbed copy of `message`.

    Recursively walks every field of a `CopyFrom` copy: any int64/uint64 field named
    `ctidTraderAccountId` becomes `FAKE_ACCOUNT_ID`, `traderLogin` becomes `FAKE_TRADER_LOGIN`,
    `accessToken`/`refreshToken` become `FAKE_TOKEN`, and `_CLEARED_FIELDS` are cleared -
    wherever any of these occur, at any nesting depth. A `_CLEARED_FIELDS` entry that is
    `required` in the schema is set to a fake value instead of cleared, since an unset
    required field would make the message fail to serialize. As a second pass, independent of
    field name, any other int64/uint64 scalar still carrying exactly `real_account_id` or
    `real_login` is replaced too - including each element of a repeated int64/uint64 field -
    so a field this function does not yet know the name of can never carry a real identifier
    through unnoticed.
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
            if field.label == FieldDescriptor.LABEL_REQUIRED:
                # Clearing would leave it unset and break serialization; give it a fake value
                # that still satisfies the required-field constraint instead.
                setattr(message, name, _FAKE_REQUIRED_VALUES.get(name, 0))
            else:
                message.ClearField(name)
        elif field.type in _IDENTIFYING_INT_TYPES:
            if field.label == FieldDescriptor.LABEL_REPEATED:
                for i, item in enumerate(value):
                    if real_account_id is not None and item == real_account_id:
                        value[i] = FAKE_ACCOUNT_ID
                    elif real_login is not None and item == real_login:
                        value[i] = FAKE_TRADER_LOGIN
            elif real_account_id is not None and value == real_account_id:
                setattr(message, name, FAKE_ACCOUNT_ID)
            elif real_login is not None and value == real_login:
                setattr(message, name, FAKE_TRADER_LOGIN)


def _verify_scrubs_cleanly(message: Message, account_id: int, real_login: int | None) -> None:
    """Scrub `message` and serialize the result, right away.

    Called as each message is recorded, not just once at the very end, so a message that
    `scrub` cannot turn back into something serializable (a required field cleared instead of
    faked) fails before any further network time is spent on later requests.
    """
    scrub(message, account_id, real_login).SerializeToString()


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

    def forbidden_bytes(self, *, include_varints: bool = False) -> list[bytes]:
        """The text form of every real identifying value, as bytes.

        With `include_varints=True`, also includes the protobuf varint encoding of the
        account id and trader login - the wire form an int64/uint64 field actually leaks as,
        which a decimal-text search can never match. Meant for checking a message's own raw
        `SerializeToString()` bytes; the varints are pointless noise against the base64 JSON,
        so that check stays text-only.
        """
        values = [
            str(self.account_id),
            str(self.real_login) if self.real_login is not None else None,
            self.access_token,
            self.refresh_token or None,
            self.client_id,
            self.broker_name,
            self.broker_title_short,
        ]
        forbidden = [v.encode() for v in values if v]
        if include_varints:
            forbidden.append(_varint(self.account_id))
            if self.real_login is not None:
                forbidden.append(_varint(self.real_login))
        return forbidden


class RecordResult:
    """Same non-`@dataclass` constraint as `RecordedSecrets` applies here."""

    def __init__(
        self,
        messages: dict[str, list[Message]],
        secrets: RecordedSecrets,
        rate_limit_retries: list[float | None],
    ) -> None:
        self.messages = messages
        self.secrets = secrets
        self.rate_limit_retries = rate_limit_retries


async def _request_with_retry(
    connection: CTraderConnection,
    payload: Message,
    *,
    bucket: str,
    rate_limit_retries: list[float | None],
) -> Message:
    """Send `payload`, retrying up to `_MAX_RATE_LIMIT_RETRIES` times on `BLOCKED_PAYLOAD_TYPE`.

    Waits the venue's own `retryAfter` before each retry, or `_RATE_LIMIT_FALLBACK_WAIT_SECS`
    if the venue didn't send one. Every observed `retryAfter` (`None` included) is appended to
    `rate_limit_retries` - a real protocol fact worth keeping, regardless of whether the retry
    that follows it succeeds.
    """
    for attempt in range(_MAX_RATE_LIMIT_RETRIES + 1):
        try:
            return await connection.request(payload, bucket=bucket)
        except CTraderRequestError as e:
            if e.error_code != "BLOCKED_PAYLOAD_TYPE":
                raise
            rate_limit_retries.append(e.retry_after_secs)
            if attempt == _MAX_RATE_LIMIT_RETRIES:
                raise
            wait_secs = (
                e.retry_after_secs
                if e.retry_after_secs is not None
                else _RATE_LIMIT_FALLBACK_WAIT_SECS
            )
            await asyncio.sleep(wait_secs)
    raise AssertionError("unreachable: the loop above always returns or raises")


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
    _verify_scrubs_cleanly(account_list_res, account_id, real_login)

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

    rate_limit_retries: list[float | None] = []
    connection = CTraderConnection(
        host,
        PROTOBUF_PORT,
        logger=_QuietLogger(),
        tls=True,
        rate_limiter=RateLimiter(_RATE_LIMITS),
    )
    await connection.connect()
    try:
        await connection.request(
            oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
        )
        await connection.request(
            oa.ProtoOAAccountAuthReq(ctidTraderAccountId=account_id, accessToken=access_token),
        )

        trader_res = await connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=account_id))
        _verify_scrubs_cleanly(trader_res, account_id, real_login)
        recorded["trader"].append(trader_res)

        assets_res = await connection.request(
            oa.ProtoOAAssetListReq(ctidTraderAccountId=account_id),
        )
        _verify_scrubs_cleanly(assets_res, account_id, real_login)
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
        _verify_scrubs_cleanly(conversion_res, account_id, real_login)
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
        _verify_scrubs_cleanly(filtered_symbols_res, account_id, real_login)
        recorded["symbols"].append(filtered_symbols_res)

        target_symbol_id_by_name = {
            s.symbolName: _light_symbol_id(s) for s in wanted_symbols if s.symbolName in SYMBOLS
        }
        all_wanted_ids = [_light_symbol_id(s) for s in wanted_symbols]
        symbol_specs_res = await connection.request(
            oa.ProtoOASymbolByIdReq(ctidTraderAccountId=account_id, symbolId=all_wanted_ids),
        )
        _verify_scrubs_cleanly(symbol_specs_res, account_id, real_login)
        recorded["symbol_specs"].append(symbol_specs_res)

        now_ms = int(time.time() * 1000)
        day_ms = 24 * 60 * 60 * 1000
        for symbol_name in SYMBOLS:
            symbol_id = target_symbol_id_by_name[symbol_name]
            m15_res = await _request_with_retry(
                connection,
                oa.ProtoOAGetTrendbarsReq(
                    ctidTraderAccountId=account_id,
                    symbolId=symbol_id,
                    period=om.M15,
                    count=50,
                    fromTimestamp=now_ms - 7 * day_ms,
                    toTimestamp=now_ms,
                ),
                bucket=BUCKET_HISTORICAL,
                rate_limit_retries=rate_limit_retries,
            )
            _verify_scrubs_cleanly(m15_res, account_id, real_login)
            recorded["trendbars_m15"].append(m15_res)

            h1_res = await _request_with_retry(
                connection,
                oa.ProtoOAGetTrendbarsReq(
                    ctidTraderAccountId=account_id,
                    symbolId=symbol_id,
                    period=om.H1,
                    count=50,
                    fromTimestamp=now_ms - 21 * day_ms,
                    toTimestamp=now_ms,
                ),
                bucket=BUCKET_HISTORICAL,
                rate_limit_retries=rate_limit_retries,
            )
            _verify_scrubs_cleanly(h1_res, account_id, real_login)
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
        for spot_event in spot_events:
            _verify_scrubs_cleanly(spot_event, account_id, real_login)
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
    return RecordResult(
        messages=recorded,
        secrets=secrets,
        rate_limit_retries=rate_limit_retries,
    )


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

    # Checked per message, on its own raw serialized bytes: this is where a real identifier
    # actually leaks (an int64/uint64 field as a varint, a string field as literal text). The
    # base64-encoded JSON built below is checked separately, but a decimal identifier can
    # never appear literally inside base64, so that check alone would miss exactly this.
    raw_forbidden = result.secrets.forbidden_bytes(include_varints=True)
    for messages in scrubbed.values():
        for message in messages:
            assert_clean(message.SerializeToString(), raw_forbidden)

    output = {key: [_encode(message) for message in messages] for key, messages in scrubbed.items()}
    output_bytes = json.dumps(output, indent=2).encode("utf-8")

    assert_clean(output_bytes, result.secrets.forbidden_bytes())

    _OUTPUT_PATH.write_bytes(output_bytes)

    for key, messages in scrubbed.items():
        print(f"{key}: {len(messages)}")
    print(
        f"rate-limit blocks: {len(result.rate_limit_retries)}, "
        f"retryAfter values: {result.rate_limit_retries}",
    )


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
    try:
        asyncio.run(_run(args.account_id, env))
    except Exception as e:
        # A raised CTraderRequestError carries the venue's own description; an uncaught
        # traceback would print it. Only the exception's type name reaches the terminal.
        print(f"error: {type(e).__name__}: recording failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
