"""Tests for the owner-run token verification script.

`scripts/verify_tokens.py` is developer tooling, imported by file path like the other scripts'
tests do. The decisions are checked against canned outcomes; whole runs go against a fake venue
that issues, refreshes and revokes tokens by a policy each test sets, over the real framing.

Every token the fake venue issues, and the client id and secret, are distinctive strings, so a
run's printed and logged output can be searched for them. The venue also puts the token into
its error descriptions, which the script must never print.
"""

from __future__ import annotations

import asyncio
import importlib.util
import itertools
import pathlib
import socket
import sys
import time

import pytest

from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests import account_venue
from tests.fake_server import FakeCTraderServer
from tests.recording_logger import RecordingLogger

_SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "verify_tokens.py"
_SPEC = importlib.util.spec_from_file_location("verify_tokens", _SCRIPT_PATH)
verify_tokens = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = verify_tokens
_SPEC.loader.exec_module(verify_tokens)

v = verify_tokens
OK, DIFFERS, UNKNOWN = v.OK, v.DIFFERS, v.UNKNOWN
Outcome = v.Outcome

CLIENT_ID = "cid-SECRET-7f3a"
CLIENT_SECRET = "csecret-SECRET-9b2c"

accepted = Outcome.accepted()


def refused(code: str = "CH_ACCESS_TOKEN_INVALID") -> v.Outcome:
    return Outcome.refused(code)


def failed(reason: str = "CTraderTimeoutError") -> v.Outcome:
    return Outcome.failed(reason)


def joined(decision: v.Decision) -> str:
    return "\n".join(decision[1])


# -- Outcomes -------------------------------------------------------------------------------


def test_an_outcome_reads_as_its_status_and_reason() -> None:
    assert accepted.text == "accepted"
    assert refused("OA_AUTH_TOKEN_EXPIRED").text == "refused with OA_AUTH_TOKEN_EXPIRED"
    assert failed("ConnectionError").text == "failed: ConnectionError"


# -- 1a: the first pair's access token after a second authorisation --------------------------


def test_1a_without_a_second_authorisation_is_unknown() -> None:
    status, _ = v.decide_access_after_second_authorisation(
        accepted,
        second_problem="no second authorisation was made",
        after_authorisation=None,
        after_refresh=None,
    )
    assert status == UNKNOWN


def test_1a_is_unknown_when_the_first_access_token_did_not_work_to_begin_with() -> None:
    decision = v.decide_access_after_second_authorisation(
        refused("OA_AUTH_TOKEN_EXPIRED"),
        second_problem=None,
        after_authorisation=refused("OA_AUTH_TOKEN_EXPIRED"),
        after_refresh=None,
    )
    assert decision[0] == UNKNOWN
    assert "OA_AUTH_TOKEN_EXPIRED" in joined(decision)


def test_1a_differs_when_the_second_authorisation_revokes_the_first_access_token() -> None:
    decision = v.decide_access_after_second_authorisation(
        accepted,
        second_problem=None,
        after_authorisation=refused("CH_ACCESS_TOKEN_INVALID"),
        after_refresh=None,
    )
    assert decision[0] == DIFFERS
    assert "CH_ACCESS_TOKEN_INVALID" in joined(decision)


def test_1a_differs_when_the_second_pairs_refresh_revokes_the_first_access_token() -> None:
    decision = v.decide_access_after_second_authorisation(
        accepted,
        second_problem=None,
        after_authorisation=accepted,
        after_refresh=refused("CH_ACCESS_TOKEN_INVALID"),
    )
    assert decision[0] == DIFFERS
    assert "refresh" in joined(decision)
    assert "CH_ACCESS_TOKEN_INVALID" in joined(decision)


def test_1a_is_ok_when_the_first_access_token_survives_both() -> None:
    status, _ = v.decide_access_after_second_authorisation(
        accepted,
        second_problem=None,
        after_authorisation=accepted,
        after_refresh=accepted,
    )
    assert status == OK


@pytest.mark.parametrize("after_refresh", [None, failed()])
def test_1a_is_unknown_and_names_the_missing_half_when_the_after_refresh_check_is_missing(
    after_refresh,
) -> None:
    decision = v.decide_access_after_second_authorisation(
        accepted,
        second_problem=None,
        after_authorisation=accepted,
        after_refresh=after_refresh,
    )
    assert decision[0] == UNKNOWN
    assert "after B's refresh" in joined(decision)
    assert "not decided" in joined(decision)


@pytest.mark.parametrize(
    "problem",
    [
        "the second authorisation returned the same pair",
        "pair B was not shown to grant the CTRADER_TRADER_LOGIN account",
    ],
)
def test_1a_1b_and_2_are_unknown_when_pair_b_cannot_answer(problem) -> None:
    decisions = [
        v.decide_access_after_second_authorisation(
            accepted,
            second_problem=problem,
            after_authorisation=refused(),
            after_refresh=refused(),
        ),
        v.decide_refresh_after_second_authorisation(
            second_problem=problem,
            second_refreshed=True,
            first_refresh=refused(),
        ),
        v.decide_refresh_token_reuse(accepted, accepted, second_problem=problem),
    ]
    assert decisions == [(UNKNOWN, (problem,))] * 3


def test_1a_is_unknown_when_the_check_itself_failed() -> None:
    status, _ = v.decide_access_after_second_authorisation(
        accepted,
        second_problem=None,
        after_authorisation=failed(),
        after_refresh=None,
    )
    assert status == UNKNOWN


# -- 1b: the first pair's refresh token after a second authorisation -------------------------


def test_1b_without_a_second_authorisation_is_unknown() -> None:
    status, _ = v.decide_refresh_after_second_authorisation(
        second_problem="no second authorisation was made",
        second_refreshed=False,
        first_refresh=accepted,
    )
    assert status == UNKNOWN


def test_1b_is_ok_when_the_first_refresh_token_still_works() -> None:
    status, _ = v.decide_refresh_after_second_authorisation(
        second_problem=None,
        second_refreshed=True,
        first_refresh=accepted,
    )
    assert status == OK


def test_1b_is_ok_but_narrower_when_the_second_pair_was_never_refreshed() -> None:
    decision = v.decide_refresh_after_second_authorisation(
        second_problem=None,
        second_refreshed=False,
        first_refresh=accepted,
    )
    assert decision[0] == OK
    assert "only" in joined(decision)


def test_1b_differs_when_the_first_refresh_token_is_refused() -> None:
    decision = v.decide_refresh_after_second_authorisation(
        second_problem=None,
        second_refreshed=True,
        first_refresh=refused("CH_ACCESS_TOKEN_INVALID"),
    )
    assert decision[0] == DIFFERS
    assert "CH_ACCESS_TOKEN_INVALID" in joined(decision)


@pytest.mark.parametrize("first_refresh", [None, failed()])
def test_1b_is_unknown_when_the_refresh_was_not_answered(first_refresh) -> None:
    status, _ = v.decide_refresh_after_second_authorisation(
        second_problem=None,
        second_refreshed=True,
        first_refresh=first_refresh,
    )
    assert status == UNKNOWN


# -- 2: a used refresh token ---------------------------------------------------------------


def test_2_is_ok_when_the_used_refresh_token_is_refused() -> None:
    decision = v.decide_refresh_token_reuse(accepted, refused("CH_ACCESS_TOKEN_INVALID"))
    assert decision[0] == OK
    assert "CH_ACCESS_TOKEN_INVALID" in joined(decision)
    assert "replayed after every other token step" in joined(decision)


def test_2_reports_a_pair_the_replay_revoked_without_changing_the_verdict() -> None:
    decision = v.decide_refresh_token_reuse(
        accepted,
        refused("CH_ACCESS_TOKEN_INVALID"),
        after_replay=refused("CH_ACCESS_TOKEN_INVALID"),
    )
    assert decision[0] == OK
    assert "revoked" in joined(decision)


def test_2_differs_when_the_used_refresh_token_is_accepted_again() -> None:
    status, _ = v.decide_refresh_token_reuse(accepted, accepted)
    assert status == DIFFERS


@pytest.mark.parametrize(
    ("first", "reuse"),
    [(None, None), (refused(), None), (failed(), None), (accepted, failed()), (accepted, None)],
)
def test_2_is_unknown_without_a_successful_first_refresh_and_an_answered_reuse(
    first,
    reuse,
) -> None:
    status, _ = v.decide_refresh_token_reuse(first, reuse)
    assert status == UNKNOWN


# -- 3: one access token, one account, two connections --------------------------------------


def watched(name: str, **values) -> v.ConnectionObservation:
    fields = {"account_auth": accepted, "events": (), "lost": None, "trader_read": accepted}
    fields.update(values)
    return v.ConnectionObservation(name, **fields)


def test_3_is_ok_when_both_connections_stay_authorised() -> None:
    status, detail = v.decide_same_account_twice((watched("1"), watched("2")))
    assert status == OK
    assert len(detail) == 2


def test_3_is_unknown_when_it_could_not_run() -> None:
    decision = v.decide_same_account_twice((), skipped="no working pair to test with")
    assert decision == (UNKNOWN, ("no working pair to test with",))


def test_3_is_unknown_when_the_first_connection_cannot_authorise() -> None:
    decision = v.decide_same_account_twice(
        (watched("1", account_auth=refused(), trader_read=None), watched("2", account_auth=None)),
    )
    assert decision[0] == UNKNOWN


def test_3_differs_when_the_second_authorisation_is_refused() -> None:
    decision = v.decide_same_account_twice(
        (watched("1"), watched("2", account_auth=refused("ALREADY_LOGGED_IN"), trader_read=None)),
    )
    assert decision[0] == DIFFERS
    assert "ALREADY_LOGGED_IN" in joined(decision)


@pytest.mark.parametrize(
    "event",
    [
        "ProtoOAAccountDisconnectEvent",
        "ProtoOAClientDisconnectEvent",
        "ProtoOAAccountsTokenInvalidatedEvent",
    ],
)
def test_3_differs_when_a_connection_is_told_it_lost_the_account(event) -> None:
    decision = v.decide_same_account_twice((watched("1", events=(event,)), watched("2")))
    assert decision[0] == DIFFERS
    assert event in joined(decision)


def test_3_differs_when_a_read_after_the_wait_is_refused() -> None:
    decision = v.decide_same_account_twice(
        (watched("1", trader_read=refused("CH_CTID_TRADER_ACCOUNT_NOT_FOUND")), watched("2")),
    )
    assert decision[0] == DIFFERS
    assert "CH_CTID_TRADER_ACCOUNT_NOT_FOUND" in joined(decision)


def test_3_is_unknown_when_a_connection_drops_without_a_venue_signal() -> None:
    decision = v.decide_same_account_twice(
        (watched("1", lost="CTraderConnectionError", trader_read=failed()), watched("2")),
    )
    assert decision[0] == UNKNOWN


def test_3_does_not_count_a_late_error_with_no_request_waiting_for_it() -> None:
    decision = v.decide_same_account_twice(
        (watched("1", events=("ProtoOAErrorRes",)), watched("2"))
    )
    assert decision[0] == OK


def test_3_lists_an_unrelated_event_without_counting_it() -> None:
    decision = v.decide_same_account_twice(
        (watched("1", events=("ProtoOATraderUpdatedEvent",)), watched("2")),
    )
    assert decision[0] == OK
    assert "ProtoOATraderUpdatedEvent" in joined(decision)


# -- Which pair is kept ----------------------------------------------------------------------


def pair(label: str, **values) -> v.TokenPair:
    return v.TokenPair(label, f"access-{label}", f"refresh-{label}", None, **values)


def test_the_newest_pair_whose_refresh_token_is_unused_comes_first() -> None:
    pairs = [
        pair("A", refresh_used=True),
        pair("B", refresh_used=True),
        pair("B'"),
        pair("A'"),
    ]
    assert [p.label for p in v.keep_order(pairs)] == ["A'", "B'"]


def test_a_pair_whose_access_token_was_refused_is_never_kept() -> None:
    pairs = [pair("A"), pair("B'", last_check=accepted), pair("A'", last_check=refused())]
    assert [p.label for p in v.keep_order(pairs)] == ["B'", "A"]


def test_a_token_pair_never_shows_its_tokens() -> None:
    shown = repr(pair("A"))
    assert "access-A" not in shown
    assert "refresh-A" not in shown


# -- Read-only towards trading ---------------------------------------------------------------


class _StubConnection:
    def __init__(self) -> None:
        self.sent: list[object] = []

    async def request(self, payload, **_kwargs):
        self.sent.append(payload)
        return oa.ProtoOAApplicationAuthRes()


@pytest.mark.parametrize(
    "payload",
    [
        oa.ProtoOANewOrderReq(ctidTraderAccountId=1, symbolId=1),
        oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=1),
        oa.ProtoOASymbolsListReq(ctidTraderAccountId=1),
    ],
)
def test_the_requester_refuses_a_request_outside_the_allow_list(payload) -> None:
    connection = _StubConnection()
    requester = v.TokenRequester(connection)

    with pytest.raises(v.RequestNotAllowed):
        asyncio.run(requester.request(payload))

    assert connection.sent == []


def test_the_requester_passes_an_allowed_request_through() -> None:
    connection = _StubConnection()
    asyncio.run(v.TokenRequester(connection).request(oa.ProtoOATraderReq(ctidTraderAccountId=1)))
    assert len(connection.sent) == 1


def test_the_allow_list_is_exactly_auth_account_list_refresh_and_trader_read() -> None:
    assert {cls.__name__ for cls in v.ALLOWED_REQUESTS} == {
        "ProtoOAApplicationAuthReq",
        "ProtoOAGetAccountListByAccessTokenReq",
        "ProtoOAAccountAuthReq",
        "ProtoOARefreshTokenReq",
        "ProtoOATraderReq",
    }


def test_the_script_never_names_an_order_request() -> None:
    source = _SCRIPT_PATH.read_text(encoding="utf-8")
    for name in ("NewOrderReq", "AmendOrder", "AmendPosition", "ClosePositionReq", "CancelOrder"):
        assert name not in source, name


# -- Refusing to start ----------------------------------------------------------------------


def test_without_the_flag_it_refuses_before_reading_or_connecting(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("touched before the flag was checked")

    monkeypatch.setattr(v.get_tokens, "load_env", forbidden)
    monkeypatch.setattr(v, "CTraderConnection", forbidden)
    monkeypatch.setattr(v, "Runner", forbidden)
    monkeypatch.setattr(v.asyncio, "run", forbidden)
    monkeypatch.setattr(v.webbrowser, "open", forbidden)

    assert v.main([]) == 2

    captured = capsys.readouterr()
    assert "--rotate-tokens" in captured.err
    assert captured.out == ""


def test_with_the_flag_it_still_refuses_an_env_file_missing_a_key(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(f"CTRADER_CLIENT_ID={CLIENT_ID}\n", encoding="utf-8")
    monkeypatch.setattr(v, "Runner", lambda *_a, **_k: pytest.fail("a run was built"))

    assert v.main(["--rotate-tokens", "--env-file", str(env_file)]) == 2

    err = capsys.readouterr().err
    assert "CTRADER_REFRESH_TOKEN" in err
    assert CLIENT_ID not in err


# -- Whole runs against a fake token venue ---------------------------------------------------


class TokenVenue:
    """A venue that issues and checks tokens by a stated policy.

    - `reauthorisation_revokes`: a new browser authorisation revokes every earlier pair.
    - `reauthorisation_returns_same`: a new browser authorisation hands back the first pair.
    - `browser_grants_other_account`: pairs from the browser, and their refreshes, grant only
      another account, as a pair from another cTrader ID would.
    - `refresh_single_use`: a refresh token works once.
    - `refuse_browser_refresh`: the refresh token from the browser is refused.
    - `refuse_all_refresh`: every refresh is refused.
    - `refuse_second_account_auth`: the second account authorisation of a run is refused.
    - `revoke_on_second_account_auth`: the second account authorisation revokes the access token
      it uses, after which every trader read is refused.
    """

    OTHER_LOGIN = 5550001

    def __init__(
        self,
        *,
        reauthorisation_revokes: bool = False,
        reauthorisation_returns_same: bool = False,
        browser_grants_other_account: bool = False,
        refresh_single_use: bool = True,
        refuse_browser_refresh: bool = False,
        refuse_all_refresh: bool = False,
        refuse_second_account_auth: bool = False,
        revoke_on_second_account_auth: bool = False,
    ) -> None:
        self.reauthorisation_revokes = reauthorisation_revokes
        self.reauthorisation_returns_same = reauthorisation_returns_same
        self.browser_grants_other_account = browser_grants_other_account
        self.refresh_single_use = refresh_single_use
        self.refuse_browser_refresh = refuse_browser_refresh
        self.refuse_all_refresh = refuse_all_refresh
        self.refuse_second_account_auth = refuse_second_account_auth
        self.revoke_on_second_account_auth = revoke_on_second_account_auth
        self.issued: list[str] = []
        self.access: set[str] = set()
        self.refresh: dict[str, str] = {}
        self.foreign: set[str] = set()
        self.browser_refresh: set[str] = set()
        self.account_auths = 0
        self.revoked = False
        self._serial = itertools.count(1)

        self.server: FakeCTraderServer = account_venue.venue(is_live=True)
        self.server.on(oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, self._list)
        self.server.on(oa_model.PROTO_OA_REFRESH_TOKEN_REQ, self._refresh)
        self.server.on(oa_model.PROTO_OA_ACCOUNT_AUTH_REQ, self._account_auth)
        self.server.on(oa_model.PROTO_OA_TRADER_REQ, self._trader)

    def issue(self, *, foreign: bool = False) -> tuple[str, str]:
        n = next(self._serial)
        access, refresh = f"tok-access-SECRET-{n:04d}", f"tok-refresh-SECRET-{n:04d}"
        self.issued += [access, refresh]
        self.access.add(access)
        self.refresh[refresh] = access
        if foreign:
            self.foreign.add(access)
        return access, refresh

    def authorise(self):
        if self.reauthorisation_returns_same:
            access, refresh = self.issued[0], self.issued[1]
        else:
            if self.reauthorisation_revokes:
                self.access.clear()
                self.refresh.clear()
            access, refresh = self.issue(foreign=self.browser_grants_other_account)
        self.browser_refresh.add(refresh)
        return v.get_tokens.TokenResponse(access, refresh, 2_628_000, "bearer")

    @staticmethod
    def _error(code: str, token: str) -> oa.ProtoOAErrorRes:
        return oa.ProtoOAErrorRes(errorCode=code, description=f"token {token} is not valid")

    def _list(self, request):
        if request.accessToken not in self.access:
            return self._error("CH_ACCESS_TOKEN_INVALID", request.accessToken)
        if request.accessToken in self.foreign:
            return account_venue.account_list(is_live=True, logins=(self.OTHER_LOGIN,))
        return account_venue.account_list(is_live=True)

    def refreshes_of(self, token: str) -> int:
        return sum(
            1
            for m in self.server.received
            if isinstance(m, oa.ProtoOARefreshTokenReq) and m.refreshToken == token
        )

    def _refresh(self, request):
        token = request.refreshToken
        old_access = self.refresh.get(token)
        refused = self.refuse_all_refresh or (
            self.refuse_browser_refresh and token in self.browser_refresh
        )
        if old_access is None or refused:
            return self._error("CH_ACCESS_TOKEN_INVALID", token)
        if self.refresh_single_use:
            del self.refresh[token]
        access, refresh = self.issue(foreign=old_access in self.foreign)
        return oa.ProtoOARefreshTokenRes(
            accessToken=access,
            tokenType="bearer",
            expiresIn=2_628_000,
            refreshToken=refresh,
        )

    def _account_auth(self, request):
        self.account_auths += 1
        if request.accessToken not in self.access:
            return self._error("CH_ACCESS_TOKEN_INVALID", request.accessToken)
        if self.refuse_second_account_auth and self.account_auths == 2:
            return self._error("ALREADY_LOGGED_IN", request.accessToken)
        if self.revoke_on_second_account_auth and self.account_auths == 2:
            self.access.discard(request.accessToken)
            self.revoked = True
        return oa.ProtoOAAccountAuthRes(ctidTraderAccountId=request.ctidTraderAccountId)

    def _trader(self, request):
        if self.revoked:
            return self._error("CH_ACCESS_TOKEN_INVALID", "revoked")
        return account_venue.for_account(
            account_venue.RECORDED["trader"][0],
            request.ctidTraderAccountId,
        )


def write_env(path: pathlib.Path, access: str, refresh: str) -> None:
    path.write_text(
        "# kept as it is\n"
        f"CTRADER_CLIENT_ID={CLIENT_ID}\n"
        f"CTRADER_CLIENT_SECRET={CLIENT_SECRET}\n"
        f"CTRADER_ACCESS_TOKEN={access}\n"
        f"CTRADER_REFRESH_TOKEN={refresh}\n"
        "CTRADER_TOKEN_EXPIRES_AT=1900000000\n"
        f"CTRADER_TRADER_LOGIN={account_venue.TRADER_LOGIN}\n",
        encoding="utf-8",
    )


async def run_against(
    venue: TokenVenue,
    tmp_path: pathlib.Path,
    *,
    authorise=None,
) -> tuple[v.Runner, RecordingLogger, pathlib.Path]:
    env_file = tmp_path / ".env"
    write_env(env_file, *venue.issue())
    settings = v.settings_from_env(v.get_tokens.load_env(env_file))
    logger = RecordingLogger()
    # Started first: the runner is given the port the server binds.
    await venue.server.start()
    runner = v.Runner(
        settings,
        env_file=env_file,
        authorise=venue.authorise if authorise is None else authorise,
        logger=logger,
        listen_secs=0.05,
        demo_host=venue.server.host,
        live_host=venue.server.host,
        port=venue.server.port,
        tls=False,
    )
    try:
        await runner.run()
    finally:
        await venue.server.stop()
    return runner, logger, env_file


def statuses(runner: v.Runner) -> dict[str, str]:
    return {f.item: f.status for f in runner.findings}


def assert_nothing_secret_shown(
    venue: TokenVenue,
    output: str,
    logger: RecordingLogger,
) -> None:
    logged = "\n".join(message for _level, message in logger.lines)
    for secret in [*venue.issued, CLIENT_ID, CLIENT_SECRET]:
        assert secret not in output, secret
        assert secret not in logged, secret
    for identifier in (account_venue.ACCOUNT_ID, account_venue.TRADER_LOGIN):
        assert str(identifier) not in output


async def test_independent_pairs_answer_every_question_and_keep_the_newest_pair(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue()
    runner, logger, env_file = await run_against(venue, tmp_path)

    assert statuses(runner) == {"1a": OK, "1b": OK, "2": OK, "3": OK}
    assert runner.kept is not None and runner.kept.label == "A'"
    env = v.get_tokens.load_env(env_file)
    assert env["CTRADER_ACCESS_TOKEN"] == runner.kept.access_token
    assert env["CTRADER_REFRESH_TOKEN"] == runner.kept.refresh_token
    assert env["CTRADER_ACCESS_TOKEN"] in venue.access
    assert env["CTRADER_CLIENT_ID"] == CLIENT_ID
    assert abs(int(env["CTRADER_TOKEN_EXPIRES_AT"]) - (time.time() + 2_628_000)) < 60
    assert env_file.read_text(encoding="utf-8").startswith("# kept as it is\n")

    # Two connections each authorised the account, with the same access token.
    auths = [m for m in venue.server.received if isinstance(m, oa.ProtoOAAccountAuthReq)]
    assert len(auths) == 2
    assert auths[0].accessToken == auths[1].accessToken == runner.kept.access_token

    captured = capsys.readouterr()
    report = runner.report()
    assert "pair A'" in report
    assert_nothing_secret_shown(venue, captured.out + captured.err + report, logger)


async def test_a_reauthorisation_that_revokes_the_first_pair_keeps_the_second(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue(reauthorisation_revokes=True)
    runner, logger, env_file = await run_against(venue, tmp_path)

    assert statuses(runner) == {"1a": DIFFERS, "1b": DIFFERS, "2": OK, "3": OK}
    assert runner.kept is not None and runner.kept.label == "B'"
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == runner.kept.access_token

    captured = capsys.readouterr()
    assert "CH_ACCESS_TOKEN_INVALID" in runner.report()
    assert_nothing_secret_shown(venue, captured.out + captured.err + runner.report(), logger)


async def test_a_reusable_refresh_token_is_reported(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue(refresh_single_use=False)
    runner, logger, _env_file = await run_against(venue, tmp_path)

    assert statuses(runner)["2"] == DIFFERS
    # The replay's own pair is the newest one, and it works.
    assert runner.kept is not None and runner.kept.label == "B''"
    captured = capsys.readouterr()
    assert_nothing_secret_shown(venue, captured.out + captured.err + runner.report(), logger)


async def test_a_refused_second_authorisation_of_the_account_is_reported(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue(refuse_second_account_auth=True)
    runner, logger, _env_file = await run_against(venue, tmp_path)

    assert statuses(runner)["3"] == DIFFERS
    assert "ALREADY_LOGGED_IN" in runner.report()
    # Step 8 found the kept pair still working, so it stays.
    assert runner.kept is not None and runner.kept.label == "A'"
    captured = capsys.readouterr()
    assert_nothing_secret_shown(venue, captured.out + captured.err + runner.report(), logger)


async def test_a_failed_browser_authorisation_still_rotates_and_keeps_the_first_pair(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue()

    def no_redirect():
        raise TimeoutError("no authorization redirect received within 300s")

    runner, logger, env_file = await run_against(venue, tmp_path, authorise=no_redirect)

    assert statuses(runner) == {"1a": UNKNOWN, "1b": UNKNOWN, "2": UNKNOWN, "3": OK}
    assert runner.kept is not None and runner.kept.label == "A'"
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == runner.kept.access_token
    captured = capsys.readouterr()
    assert "TimeoutError" in captured.out + captured.err
    assert_nothing_secret_shown(venue, captured.out + captured.err + runner.report(), logger)


async def test_with_no_working_pair_it_says_so_and_points_to_get_tokens(
    tmp_path: pathlib.Path,
) -> None:
    venue = TokenVenue()

    def no_redirect():
        raise TimeoutError("no authorization redirect received within 300s")

    env_file = tmp_path / ".env"
    # Neither token was issued by this venue, so both are refused.
    write_env(env_file, "tok-access-SECRET-stale", "tok-refresh-SECRET-stale")
    venue.issued += ["tok-access-SECRET-stale", "tok-refresh-SECRET-stale"]
    await venue.server.start()
    runner = v.Runner(
        v.settings_from_env(v.get_tokens.load_env(env_file)),
        env_file=env_file,
        authorise=no_redirect,
        logger=RecordingLogger(),
        listen_secs=0.05,
        demo_host=venue.server.host,
        live_host=venue.server.host,
        port=venue.server.port,
        tls=False,
    )
    try:
        await runner.run()
    finally:
        await venue.server.stop()

    assert runner.kept is None
    # The stale pair was refused by the venue, not lost to a connection that never opened.
    assert runner.pairs[0].last_check == v.Outcome.refused("CH_ACCESS_TOKEN_INVALID")
    assert statuses(runner)["3"] == UNKNOWN
    report = runner.report()
    assert "get_tokens.py" in report
    # Nothing new was obtained, so the env file is left as it was.
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == "tok-access-SECRET-stale"
    assert "tok-access-SECRET-stale" not in report


async def test_a_run_cut_short_leaves_the_newest_pair_obtained_in_the_env_file(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    venue = TokenVenue()

    async def broken_refresh(*_args, **_kwargs):
        raise RuntimeError("cut short")

    env_file = tmp_path / ".env"
    write_env(env_file, *venue.issue())
    await venue.server.start()
    runner = v.Runner(
        v.settings_from_env(v.get_tokens.load_env(env_file)),
        env_file=env_file,
        authorise=venue.authorise,
        logger=RecordingLogger(),
        listen_secs=0.05,
        demo_host=venue.server.host,
        live_host=venue.server.host,
        port=venue.server.port,
        tls=False,
    )
    monkeypatch.setattr(runner, "_refresh", broken_refresh)
    try:
        with pytest.raises(RuntimeError):
            await runner.run()
    finally:
        await venue.server.stop()

    b = runner.pairs[-1]
    assert b.label == "B"
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == b.access_token
    assert "did not finish" in runner.report()
    assert "pair B" in runner.report()


def test_the_browser_step_fails_at_once_without_a_browser_and_never_prints_the_url(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    settings = v.Settings(CLIENT_ID, CLIENT_SECRET, "access", "refresh", None, 1)
    monkeypatch.setattr(v.webbrowser, "open", lambda _url: False)
    authorise = v.browser_authorisation(
        settings,
        redirect_uri=f"http://127.0.0.1:{port}/callback",
        port=port,
        path="/callback",
        timeout_secs=30.0,
    )

    started = time.monotonic()
    with pytest.raises(v.BrowserNotOpened):
        authorise()

    assert time.monotonic() - started < 5.0
    captured = capsys.readouterr()
    assert CLIENT_ID not in captured.out + captured.err


# -- Review fixes: inferences that could otherwise be wrong without a sign -----------------


def test_a_pair_that_may_belong_to_another_ctrader_id_never_displaces_one_that_fits() -> None:
    pairs = [
        pair("A", refresh_used=True, grants_account=True),
        pair("A'", grants_account=True, last_check=failed()),
        pair("B''", grants_account=False, last_check=accepted),
    ]
    assert [p.label for p in v.keep_order(pairs)] == ["A'"]


def test_without_any_pair_that_fits_the_newest_working_pair_is_still_kept() -> None:
    pairs = [pair("A", refresh_used=True), pair("B'", last_check=accepted)]
    assert [p.label for p in v.keep_order(pairs)] == ["B'"]


def test_two_pairs_are_the_same_if_either_token_matches() -> None:
    a = v.TokenPair("A", "x-access", "x-refresh", None)
    assert v.TokenPair("B", "x-access", "other", None).same_as(a)
    assert v.TokenPair("B", "other", "x-refresh", None).same_as(a)
    assert not v.TokenPair("B", "other", "another", None).same_as(a)


async def test_when_b_is_a_handed_back_items_1a_and_1b_say_so_and_a_is_not_refreshed_twice(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue(reauthorisation_returns_same=True)
    runner, logger, env_file = await run_against(venue, tmp_path)

    assert statuses(runner) == {"1a": UNKNOWN, "1b": UNKNOWN, "2": OK, "3": OK}
    assert "the second authorisation returned the same pair" in runner.report()
    # Sent once as B's refresh and once as the replay, never a third time as A's.
    assert venue.refreshes_of(venue.issued[1]) == 2
    assert runner.kept is not None and runner.kept.label == "B'"
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == runner.kept.access_token
    captured = capsys.readouterr()
    assert_nothing_secret_shown(venue, captured.out + captured.err + runner.report(), logger)


async def test_a_second_pair_from_another_ctrader_id_decides_nothing_and_is_never_used(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue(browser_grants_other_account=True, refresh_single_use=False)
    runner, logger, env_file = await run_against(venue, tmp_path)

    assert statuses(runner) == {"1a": UNKNOWN, "1b": UNKNOWN, "2": UNKNOWN, "3": OK}
    assert "may come from another cTrader ID" in runner.report()
    # Neither refreshed nor replayed: with another cTrader ID's pair that could only do harm.
    assert venue.refreshes_of(venue.issued[3]) == 0
    assert [p.label for p in runner.pairs] == ["A", "B", "A'"]
    assert runner.kept is not None and runner.kept.label == "A'"
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == runner.kept.access_token
    captured = capsys.readouterr()
    # B never reached the env file, not even for a moment.
    assert "pair B written" not in captured.out
    assert_nothing_secret_shown(venue, captured.out + captured.err + runner.report(), logger)


async def test_the_replay_comes_after_every_look_at_pair_a(tmp_path: pathlib.Path) -> None:
    venue = TokenVenue()
    await run_against(venue, tmp_path)

    refreshes = [
        m.refreshToken for m in venue.server.received if isinstance(m, oa.ProtoOARefreshTokenReq)
    ]
    a_refresh, b_refresh = venue.issued[1], venue.issued[3]
    assert refreshes == [b_refresh, a_refresh, b_refresh]
    lists = [
        m.accessToken
        for m in venue.server.received
        if isinstance(m, oa.ProtoOAGetAccountListByAccessTokenReq)
    ]
    # A's access token is never looked at once the replay was sent.
    replay_at = [
        i for i, m in enumerate(venue.server.received) if isinstance(m, oa.ProtoOARefreshTokenReq)
    ][2]
    later = venue.server.received[replay_at:]
    assert venue.issued[0] in lists
    assert not any(
        isinstance(m, oa.ProtoOAGetAccountListByAccessTokenReq) and m.accessToken == venue.issued[0]
        for m in later
    )


async def test_when_b_cannot_be_refreshed_a_prime_is_kept(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue(refuse_browser_refresh=True)
    runner, logger, env_file = await run_against(venue, tmp_path)

    assert statuses(runner) == {"1a": UNKNOWN, "1b": OK, "2": UNKNOWN, "3": OK}
    assert runner.kept is not None and runner.kept.label == "A'"
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == runner.kept.access_token
    captured = capsys.readouterr()
    assert_nothing_secret_shown(venue, captured.out + captured.err + runner.report(), logger)


async def test_when_every_refresh_fails_the_env_file_keeps_b_and_the_report_says_why(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue(refuse_all_refresh=True)
    runner, logger, env_file = await run_against(venue, tmp_path)

    assert runner.kept is None
    b = runner.pairs[1]
    env = v.get_tokens.load_env(env_file)
    assert (env["CTRADER_ACCESS_TOKEN"], env["CTRADER_REFRESH_TOKEN"]) == (
        b.access_token,
        b.refresh_token,
    )
    report = runner.report()
    assert "holds pair B, whose access token was accepted at its last check" in report
    assert "refresh token was already sent" in report
    captured = capsys.readouterr()
    assert_nothing_secret_shown(venue, captured.out + captured.err + report, logger)


async def test_a_kept_pair_that_dies_in_step_7_is_replaced_by_the_next_working_pair(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue(revoke_on_second_account_auth=True)
    runner, logger, env_file = await run_against(venue, tmp_path)

    assert statuses(runner)["3"] == DIFFERS
    assert runner.kept is not None and runner.kept.label == "B'"
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == runner.kept.access_token
    assert "Pair A''s access token was refused after step 7; kept B'." in runner.report()
    captured = capsys.readouterr()
    assert_nothing_secret_shown(venue, captured.out + captured.err + runner.report(), logger)


def _offline_runner(env_file: pathlib.Path) -> v.Runner:
    return v.Runner(
        v.Settings(CLIENT_ID, CLIENT_SECRET, "access", "refresh", None, 1),
        env_file=env_file,
        authorise=lambda: None,
        logger=RecordingLogger(),
        listen_secs=0.05,
    )


def test_a_finished_run_whose_last_check_only_failed_does_not_send_the_owner_to_get_tokens(
    tmp_path: pathlib.Path,
) -> None:
    runner = _offline_runner(tmp_path / ".env")
    runner.pairs.append(pair("A", last_check=failed("CTraderConnectionError")))
    runner._finished = True

    line = runner.report().splitlines()[-1]
    assert "may still work" in line
    assert "get_tokens.py" not in line


def test_a_finished_run_whose_pair_was_refused_sends_the_owner_to_get_tokens(
    tmp_path: pathlib.Path,
) -> None:
    runner = _offline_runner(tmp_path / ".env")
    runner.pairs.append(pair("A", last_check=refused(), refresh_used=True))
    runner._finished = True

    line = runner.report().splitlines()[-1]
    assert "No working pair" in line
    assert "get_tokens.py" in line


def test_a_cut_short_run_says_when_the_pair_it_left_has_a_spent_refresh_token(
    tmp_path: pathlib.Path,
) -> None:
    runner = _offline_runner(tmp_path / ".env")
    runner.pairs.append(pair("A"))
    runner.pairs.append(pair("B", refresh_used=True))
    runner.written = runner.pairs[1]

    line = runner.report().splitlines()[-1]
    assert "did not finish" in line
    assert "pair B" in line
    assert "refresh token was already sent" in line


async def test_a_refresh_response_with_an_unusable_token_is_not_kept(
    tmp_path: pathlib.Path,
) -> None:
    venue = TokenVenue()
    venue.server.on(
        oa_model.PROTO_OA_REFRESH_TOKEN_REQ,
        lambda _r: oa.ProtoOARefreshTokenRes(
            accessToken="tok-access-SECRET-bad\nCTRADER_CLIENT_ID=x",
            tokenType="bearer",
            expiresIn=1,
            refreshToken="tok-refresh-SECRET-bad",
        ),
    )
    runner, _logger, env_file = await run_against(venue, tmp_path)

    assert [p.label for p in runner.pairs] == ["A", "B"]
    assert v.get_tokens.load_env(env_file)["CTRADER_CLIENT_ID"] == CLIENT_ID


async def test_an_env_write_refused_by_windows_is_retried(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / ".env"
    write_env(env_file, "tok-access-SECRET-old", "tok-refresh-SECRET-old")
    real_update = v.get_tokens.update_env_file
    failures = iter([PermissionError(13, "in use"), PermissionError(13, "in use")])

    def flaky_update(path, updates):
        error = next(failures, None)
        if error is not None:
            raise error
        real_update(path, updates)

    monkeypatch.setattr(v.get_tokens, "update_env_file", flaky_update)
    monkeypatch.setattr(v, "_WRITE_RETRY_DELAY_SECS", 0.0)
    runner = _offline_runner(env_file)
    new = v.TokenPair("A'", "tok-access-SECRET-new", "tok-refresh-SECRET-new", None, parent="A")

    assert await runner._write(new)
    assert v.get_tokens.load_env(env_file)["CTRADER_ACCESS_TOKEN"] == "tok-access-SECRET-new"
    assert not runner.write_failed


async def test_an_env_write_that_keeps_failing_tells_the_owner_what_is_lost(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def always_in_use(_path, _updates):
        raise PermissionError(13, "in use")

    monkeypatch.setattr(v.get_tokens, "update_env_file", always_in_use)
    monkeypatch.setattr(v, "_WRITE_RETRY_DELAY_SECS", 0.0)
    runner = _offline_runner(tmp_path / ".env")
    new = v.TokenPair("A'", "tok-access-SECRET-new", "tok-refresh-SECRET-new", None, parent="A")

    assert not await runner._write(new)

    assert runner.write_failed
    err = capsys.readouterr().err
    assert "pair A' could NOT be written" in err
    assert "pair A's refresh token, which produced it, is spent" in err
    assert "tok-" not in err + runner.report()
    assert "could NOT be written" in runner.report()


async def test_a_whole_main_run_prints_no_secret(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    venue = TokenVenue()
    env_file = tmp_path / ".env"
    write_env(env_file, *venue.issue())
    await venue.server.start()
    real_runner = v.Runner

    def runner_against_the_fake_venue(settings, **kwargs):
        kwargs.update(
            authorise=venue.authorise,
            demo_host=venue.server.host,
            live_host=venue.server.host,
            port=venue.server.port,
            tls=False,
        )
        return real_runner(settings, **kwargs)

    monkeypatch.setattr(v, "Runner", runner_against_the_fake_venue)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "")
    try:
        rc = await asyncio.to_thread(
            v.main,
            ["--rotate-tokens", "--env-file", str(env_file), "--listen-secs", "0.05"],
        )
    finally:
        await venue.server.stop()

    assert rc == 0
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "stop every node or process that uses these tokens" in output
    assert "may make the venue revoke EVERY pair of this cTrader ID" in output
    assert "scripts/get_tokens.py would then be needed" in output
    assert "until the report shows those pairs survived" in output
    # The warning comes before the run, which the prompt starts.
    assert output.index("revoke EVERY pair") < output.index("step 1")
    assert "Kept pair A'" in output
    for secret in [*venue.issued, CLIENT_ID, CLIENT_SECRET]:
        assert secret not in output, secret
    for identifier in (account_venue.ACCOUNT_ID, account_venue.TRADER_LOGIN):
        assert str(identifier) not in output


def test_main_does_not_start_when_the_prompt_is_not_answered(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / ".env"
    write_env(env_file, "tok-access-SECRET-x", "tok-refresh-SECRET-x")

    def no_answer(_prompt=""):
        raise EOFError

    monkeypatch.setattr("builtins.input", no_answer)
    monkeypatch.setattr(v, "Runner", lambda *_a, **_k: pytest.fail("a run was built"))

    assert v.main(["--rotate-tokens", "--env-file", str(env_file)]) == 2


def test_2_reports_the_other_grant_after_the_replay() -> None:
    decision = v.decide_refresh_token_reuse(
        accepted,
        refused("CH_ACCESS_TOKEN_INVALID"),
        after_replay=accepted,
        other_after_replay=refused("CH_ACCESS_TOKEN_INVALID"),
    )
    assert decision[0] == OK
    assert "pair A' (the other grant) after the replay: access token refused" in joined(decision)
    assert "every grant of this cTrader ID" in joined(decision)


async def test_a_run_reports_the_other_grant_after_the_replay(tmp_path: pathlib.Path) -> None:
    venue = TokenVenue()
    runner, _logger, _env_file = await run_against(venue, tmp_path)

    assert "pair A' (the other grant) after the replay: access token accepted" in runner.report()
    # A' is looked at after the replay was sent.
    received = venue.server.received
    replay_at = [i for i, m in enumerate(received) if isinstance(m, oa.ProtoOARefreshTokenReq)][2]
    a_prime = runner.pairs[[p.label for p in runner.pairs].index("A'")].access_token
    assert any(
        isinstance(m, oa.ProtoOAGetAccountListByAccessTokenReq) and m.accessToken == a_prime
        for m in received[replay_at:]
    )


async def test_a_run_cut_right_after_bs_refresh_leaves_b_prime_not_the_spent_b(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    venue = TokenVenue()
    env_file = tmp_path / ".env"
    write_env(env_file, *venue.issue())
    await venue.server.start()
    runner = v.Runner(
        v.settings_from_env(v.get_tokens.load_env(env_file)),
        env_file=env_file,
        authorise=venue.authorise,
        logger=RecordingLogger(),
        listen_secs=0.05,
        demo_host=venue.server.host,
        live_host=venue.server.host,
        port=venue.server.port,
        tls=False,
    )
    real_check = runner._check

    async def cut_at_the_first_check_of_b_prime(pair, step):
        if pair.label == "B'":
            raise RuntimeError("cut short")
        return await real_check(pair, step)

    monkeypatch.setattr(runner, "_check", cut_at_the_first_check_of_b_prime)
    try:
        with pytest.raises(RuntimeError):
            await runner.run()
    finally:
        await venue.server.stop()

    b_prime = runner.pairs[-1]
    assert b_prime.label == "B'"
    env = v.get_tokens.load_env(env_file)
    assert (env["CTRADER_ACCESS_TOKEN"], env["CTRADER_REFRESH_TOKEN"]) == (
        b_prime.access_token,
        b_prime.refresh_token,
    )
