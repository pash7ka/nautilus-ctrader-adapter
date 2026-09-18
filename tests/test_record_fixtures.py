"""Tests for the fixture recorder's scrubbing.

`scripts/record_fixtures.py` is developer tooling, not part of the installed package, so it is
imported by file path rather than as `nautilus_ctrader.*` - the same pattern
`tests/test_get_tokens.py` uses. Only the pure `scrub`/`assert_clean` functions are exercised
here, offline; the recording flow itself needs a live broker connection and is not run in CI.
"""

import importlib.util
import pathlib

from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

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


def test_scrub_replaces_ids_and_removes_personal_fields() -> None:
    scrubbed = record_fixtures.scrub(_trader(), REAL_ACCOUNT, REAL_LOGIN)
    t = scrubbed.trader
    assert scrubbed.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert t.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert t.traderLogin == record_fixtures.FAKE_TRADER_LOGIN
    for field in (
        "balance",
        "balanceVersion",
        "managerBonus",
        "ibBonus",
        "nonWithdrawableBonus",
        "brokerName",
        "registrationTimestamp",
    ):
        assert not t.HasField(field), field
    assert t.moneyDigits == 2 and t.accountType == om.HEDGED


def test_scrub_account_list_drops_token_and_broker_title() -> None:
    res = oa.ProtoOAGetAccountListByAccessTokenRes(
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
    s = record_fixtures.scrub(res, REAL_ACCOUNT, REAL_LOGIN)
    assert s.accessToken == record_fixtures.FAKE_TOKEN
    a = s.ctidTraderAccount[0]
    assert a.ctidTraderAccountId == record_fixtures.FAKE_ACCOUNT_ID
    assert a.traderLogin == record_fixtures.FAKE_TRADER_LOGIN
    assert not a.HasField("brokerTitleShort")
    assert a.isLive is True


def test_assert_clean_rejects_any_leftover_identifier() -> None:
    blob = b"...987654321..."
    try:
        record_fixtures.assert_clean(blob, [str(REAL_ACCOUNT).encode()])
    except record_fixtures.ScrubError:
        return
    raise AssertionError("leftover identifier not detected")
