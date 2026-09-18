"""Tests for the fixture recorder's scrubbing, and a sanity check on the recorded fixtures.

`scripts/record_fixtures.py` is developer tooling, not part of the installed package, so it is
imported by file path rather than as `nautilus_ctrader.*` - the same pattern
`tests/test_get_tokens.py` uses. The `scrub`/`assert_clean` tests run purely offline; the
recording flow itself needs a live broker connection and is not run in CI - only its already
recorded, already-scrubbed output (`tests/fixtures/m2_recorded.json`) is checked here.
"""

import importlib.util
import pathlib

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
                brokerTitleShort="Some Broker",
            )
        ],
    )


def test_scrub_replaces_ids_and_removes_personal_fields() -> None:
    scrubbed = record_fixtures.scrub(_trader(), REAL_ACCOUNT, REAL_LOGIN)
    t = scrubbed.trader
    assert scrubbed.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert t.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert t.traderLogin == record_fixtures.FAKE_TRADER_LOGIN
    for field in (
        "balanceVersion",
        "managerBonus",
        "ibBonus",
        "nonWithdrawableBonus",
        "brokerName",
        "registrationTimestamp",
    ):
        assert not t.HasField(field), field
    assert t.moneyDigits == 2 and t.accountType == om.HEDGED


def test_scrub_fakes_the_required_balance_field_instead_of_clearing_it() -> None:
    """`balance` is `required` in the schema; clearing it (like the other personal fields)
    would make the message fail to serialize, so it must be set to a fixed fake value."""
    scrubbed = record_fixtures.scrub(_trader(), REAL_ACCOUNT, REAL_LOGIN)
    assert scrubbed.trader.HasField("balance")
    assert scrubbed.trader.balance == record_fixtures.FAKE_BALANCE


def test_scrub_account_list_drops_token_and_broker_title() -> None:
    s = record_fixtures.scrub(_account_list(), REAL_ACCOUNT, REAL_LOGIN)
    assert s.accessToken == record_fixtures.FAKE_TOKEN
    a = s.ctidTraderAccount[0]
    assert a.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert a.traderLogin == record_fixtures.FAKE_TRADER_LOGIN
    assert not a.HasField("brokerTitleShort")
    assert a.isLive is True


def test_scrub_produces_serializable_messages() -> None:
    """A message `scrub` cannot turn back into something valid (a required field cleared
    instead of faked) must be caught here, not discovered mid-recording against the broker."""
    for message in (_trader(), _account_list()):
        scrubbed = record_fixtures.scrub(message, REAL_ACCOUNT, REAL_LOGIN)
        scrubbed.SerializeToString()


def test_assert_clean_rejects_any_leftover_identifier() -> None:
    blob = b"...987654321..."
    try:
        record_fixtures.assert_clean(blob, [str(REAL_ACCOUNT).encode()])
    except record_fixtures.ScrubError:
        return
    raise AssertionError("leftover identifier not detected")


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
    assert rec["trader"][0].trader.ctidTraderAccountId == FAKE_ACCOUNT_ID
    assert not rec["trader"][0].trader.HasField("brokerName")
    m1 = [
        tb.utcTimestampInMinutes
        for ev in rec["spot_events"]
        for tb in ev.trendbar
        if tb.period == 1
    ]
    assert len(set(m1)) >= 2, "capture must contain an M1 bar transition"
