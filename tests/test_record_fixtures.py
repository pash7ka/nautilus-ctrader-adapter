"""Tests for the fixture recorder's scrubbing, and a sanity check on the recorded fixtures.

`scripts/record_fixtures.py` is developer tooling, not part of the installed package, so it is
imported by file path rather than as `nautilus_ctrader.*` - the same pattern
`tests/test_get_tokens.py` uses. The `scrub`/`assert_clean` tests run purely offline; the
recording flow itself needs a live broker connection and is not run in CI - only its already
recorded, already-scrubbed output (`tests/fixtures/m2_recorded.json`) is checked here.
"""

import base64
import importlib.util
import json
import pathlib

import pytest

from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from tests.fixtures import FAKE_ACCOUNT_ID, load_recorded

_SPEC = importlib.util.spec_from_file_location(
    "record_fixtures",
    pathlib.Path(__file__).resolve().parents[1] / "scripts" / "record_fixtures.py",
)
record_fixtures = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(record_fixtures)

REAL_ACCOUNT = 987654321
REAL_LOGIN = 123456789


def _trader() -> oa.ProtoOATraderRes:
    return oa.ProtoOATraderRes(
        ctidTraderAccountId=REAL_ACCOUNT,
        trader=om.ProtoOATrader(
            ctidTraderAccountId=REAL_ACCOUNT,
            balance=1234567,
            balanceVersion=9,
            managerBonus=1,
            ibBonus=2,
            nonWithdrawableBonus=3,
            depositAssetId=15,
            traderLogin=REAL_LOGIN,
            brokerName="Some Broker Ltd",
            moneyDigits=2,
            registrationTimestamp=1_700_000_000_000,
            accountType=om.HEDGED,
        ),
    )


def _account_list() -> oa.ProtoOAGetAccountListByAccessTokenRes:
    return oa.ProtoOAGetAccountListByAccessTokenRes(
        accessToken="tok-secret-value",
        permissionScope=om.SCOPE_TRADE,
        ctidTraderAccount=[
            om.ProtoOACtidTraderAccount(
                ctidTraderAccountId=REAL_ACCOUNT,
                isLive=True,
                traderLogin=REAL_LOGIN,
                lastClosingDealTimestamp=1_700_000_000_000,
                lastBalanceUpdateTimestamp=1_700_000_000_000,
                brokerTitleShort="Some Broker",
            )
        ],
    )


def _symbol() -> oa.ProtoOASymbolByIdRes:
    return oa.ProtoOASymbolByIdRes(
        ctidTraderAccountId=REAL_ACCOUNT,
        symbol=[
            om.ProtoOASymbol(
                symbolId=1,
                digits=5,
                pipPosition=4,
                scheduleTimeZone="Europe/Somewhere",
                holiday=[
                    om.ProtoOAHoliday(
                        holidayId=7,
                        name="Some Holiday",
                        description="A broker-authored note",
                        scheduleTimeZone="Europe/Somewhere",
                        holidayDate=20_000,
                        isRecurring=True,
                    ),
                ],
            ),
        ],
    )


def test_scrub_replaces_ids_and_removes_personal_fields() -> None:
    scrubbed = record_fixtures.scrub(_trader(), REAL_ACCOUNT, REAL_LOGIN)
    t = scrubbed.trader
    assert scrubbed.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert t.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    for field in (
        "balanceVersion",
        "managerBonus",
        "ibBonus",
        "nonWithdrawableBonus",
        "brokerName",
        "registrationTimestamp",
        "traderLogin",
    ):
        assert not t.HasField(field), field
    assert t.moneyDigits == 2 and t.accountType == om.HEDGED


def test_scrub_zeroes_the_required_balance_field_instead_of_clearing_it() -> None:
    """`balance` is `required` in the schema; clearing it (like the other personal fields)
    would make the message fail to serialize, so it stays present and is set to zero."""
    scrubbed = record_fixtures.scrub(_trader(), REAL_ACCOUNT, REAL_LOGIN)
    assert scrubbed.trader.HasField("balance")
    assert scrubbed.trader.balance == 0


def test_scrub_account_list_drops_token_and_broker_title() -> None:
    s = record_fixtures.scrub(_account_list(), REAL_ACCOUNT, REAL_LOGIN)
    assert s.accessToken == record_fixtures.FAKE_TOKEN
    a = s.ctidTraderAccount[0]
    assert a.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert not a.HasField("brokerTitleShort")
    for field in ("lastClosingDealTimestamp", "lastBalanceUpdateTimestamp", "traderLogin"):
        assert not a.HasField(field), field
    assert a.isLive is True


def test_scrub_clears_the_brokers_calendar() -> None:
    """A cleared field can be a whole repeated submessage, not only a scalar."""
    scrubbed = record_fixtures.scrub(_symbol(), REAL_ACCOUNT, REAL_LOGIN)
    symbol = scrubbed.symbol[0]
    assert not symbol.holiday
    assert not symbol.HasField("scheduleTimeZone")
    assert (symbol.digits, symbol.pipPosition) == (5, 4)


def test_scrub_produces_serializable_messages() -> None:
    """A message `scrub` cannot turn back into something valid (a required field cleared
    instead of faked) must be caught here, not discovered mid-recording against the broker."""
    for message in (_trader(), _account_list(), _symbol()):
        scrubbed = record_fixtures.scrub(message, REAL_ACCOUNT, REAL_LOGIN)
        scrubbed.SerializeToString()


def test_assert_clean_rejects_any_leftover_identifier() -> None:
    blob = b"...987654321..."
    try:
        record_fixtures.assert_clean(blob, [str(REAL_ACCOUNT).encode()])
    except record_fixtures.ScrubError:
        return
    raise AssertionError("leftover identifier not detected")


def test_varint_matches_known_encodings() -> None:
    assert record_fixtures._varint(0) == b"\x00"
    assert record_fixtures._varint(127) == b"\x7f"
    assert record_fixtures._varint(128) == b"\x80\x01"
    assert record_fixtures._varint(300) == b"\xac\x02"


def test_scrub_miss_in_raw_bytes_is_caught_by_the_byte_level_check() -> None:
    """`scrub` only knows to fix specific field names/types; a real identifier smuggled into a
    field it never touches - here, a light symbol's `symbolName`, a plain string field - must
    still be caught downstream by checking the scrubbed message's own raw serialized bytes.

    The base64-encoded JSON built from those bytes can never contain this leak as a literal
    decimal match, which is exactly why the raw-bytes check exists.
    """
    leaky = oa.ProtoOASymbolsListRes(
        ctidTraderAccountId=REAL_ACCOUNT,
        symbol=[om.ProtoOALightSymbol(symbolId=1, symbolName=str(REAL_ACCOUNT))],
    )
    scrubbed = record_fixtures.scrub(leaky, REAL_ACCOUNT, REAL_LOGIN)
    assert scrubbed.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert scrubbed.symbol[0].symbolName == str(REAL_ACCOUNT)  # the miss

    try:
        record_fixtures.assert_clean(scrubbed.SerializeToString(), [str(REAL_ACCOUNT).encode()])
    except record_fixtures.ScrubError:
        return
    raise AssertionError("scrub miss inside the raw serialized bytes was not detected")


def _recorded_under_the_old_rules() -> bytes:
    """A recorded file from before `_CLEARED_FIELDS` grew: ids and tokens already faked, the
    fields added to the set later still in place."""
    account_list = record_fixtures.scrub(_account_list(), REAL_ACCOUNT, REAL_LOGIN)
    account = account_list.ctidTraderAccount[0]
    account.lastClosingDealTimestamp = 1_700_000_000_000
    account.lastBalanceUpdateTimestamp = 1_700_000_000_000
    account.traderLogin = record_fixtures.FAKE_TRADER_LOGIN
    symbol_specs = record_fixtures.scrub(_symbol(), REAL_ACCOUNT, REAL_LOGIN)
    symbol_specs.symbol[0].holiday.extend(_symbol().symbol[0].holiday)
    symbol_specs.symbol[0].scheduleTimeZone = _symbol().symbol[0].scheduleTimeZone
    return json.dumps(
        {
            "account_list": [record_fixtures._encode(account_list)],
            "symbol_specs": [record_fixtures._encode(symbol_specs)],
        },
        indent=2,
    ).encode("utf-8")


def _parse(cls, item: dict):
    message = cls()
    message.ParseFromString(base64.b64decode(item["payload"]))
    return message


def test_rescrub_removes_what_the_current_rules_clear() -> None:
    rescrubbed = json.loads(record_fixtures.rescrub_bytes(_recorded_under_the_old_rules()))

    account_list = _parse(
        oa.ProtoOAGetAccountListByAccessTokenRes,
        rescrubbed["account_list"][0],
    )
    account = account_list.ctidTraderAccount[0]
    for field in ("lastClosingDealTimestamp", "lastBalanceUpdateTimestamp", "traderLogin"):
        assert not account.HasField(field), field
    symbol = _parse(oa.ProtoOASymbolByIdRes, rescrubbed["symbol_specs"][0]).symbol[0]
    assert not symbol.holiday
    assert not symbol.HasField("scheduleTimeZone")

    # The values the original scrub installed survive the replay untouched.
    assert account_list.accessToken == record_fixtures.FAKE_TOKEN
    assert account.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert account.isLive is True


def test_rescrub_preserves_the_recorded_files_shape() -> None:
    """A re-scrub is meant to show up as nothing but the removed data, so keys, order, payload
    types and the JSON formatting all have to come out as a fresh recording writes them."""
    before = _recorded_under_the_old_rules()
    after = record_fixtures.rescrub_bytes(before)

    assert list(json.loads(after)) == list(json.loads(before))
    assert [i["type"] for i in json.loads(after)["account_list"]] == [
        i["type"] for i in json.loads(before)["account_list"]
    ]
    assert after == json.dumps(json.loads(after), indent=2).encode("utf-8")
    assert record_fixtures.rescrub_bytes(after) == after


def test_rescrub_mode_takes_no_account_id() -> None:
    """Nothing to connect with: the offline mode excludes the account id the recording needs."""
    args = record_fixtures._build_arg_parser().parse_args(["--rescrub"])
    assert args.rescrub and args.account_id is None
    with pytest.raises(SystemExit):
        record_fixtures._build_arg_parser().parse_args(["--rescrub", "--account-id", "1"])


def test_recorded_fixtures_are_scrubbed_and_complete() -> None:
    rec = load_recorded()
    for key in (
        "trader",
        "assets",
        "symbols",
        "symbol_specs",
        "conversion_eur_usd",
        "trendbars_m15",
        "trendbars_h1",
        "spot_events",
        "account_list",
    ):
        assert rec[key], key
    trader = rec["trader"][0].trader
    assert trader.ctidTraderAccountId == FAKE_ACCOUNT_ID
    assert not trader.HasField("brokerName")
    assert not trader.HasField("traderLogin")
    # `required`, so present rather than cleared - but carrying no invented number.
    assert trader.HasField("balance") and trader.balance == 0
    for symbol in (s for m in rec["symbol_specs"] for s in m.symbol):
        assert not symbol.holiday
        assert not symbol.HasField("scheduleTimeZone")
    for account in rec["account_list"][0].ctidTraderAccount:
        for field in ("lastClosingDealTimestamp", "lastBalanceUpdateTimestamp", "traderLogin"):
            assert not account.HasField(field), field
    m1 = [
        tb.utcTimestampInMinutes
        for ev in rec["spot_events"]
        for tb in ev.trendbar
        if tb.period == 1
    ]
    assert len(set(m1)) >= 2, "capture must contain an M1 bar transition"
