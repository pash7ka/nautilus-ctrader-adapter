"""Owner-run check of how cTrader Open API access and refresh tokens behave.

**This script spends refresh tokens and makes a second authorisation of the application.** It
runs only with `--rotate-tokens`, needs the owner at a browser to log in once, and rewrites the
token lines of the env file. Nothing in the test suite runs it against a real venue.

For one application and one cTrader ID it answers:

- 1a, 1b: are pairs from two separate authorisations independent - does the second
  authorisation, or a refresh of its pair, revoke the first pair's access or refresh token?
- 2: is a refresh token single-use?
- 3: can one access token authorise the same trading account on two connections at once?

The steps, each recorded, the run going on wherever it safely can:

1. pair A, the one in the env file: its access token lists the granted accounts, or is refused;
2. pair B: the browser authorisation again; B's and then A's access tokens are checked;
3. B is refreshed into B';
4. A's access token is checked again, and A is refreshed into A';
5. B's used refresh token is replayed, and B' and A' are checked again;
6. the newest working pair that grants the `CTRADER_TRADER_LOGIN` account is kept;
7. with it, two connections authorise that account, listen for disconnect events, then each
   reads the trader;
8. if step 7 saw anything but a clean answer, the kept pair is checked again and replaced by the
   next working pair if it no longer works.

The replay comes last among the token steps: a venue may revoke a whole grant, or every grant of
the user, when a spent refresh token is replayed, and that must not be read as an effect of the
second authorisation.

1a, 1b and 2 are only decided when pair B grants the configured account (so it comes from the
same cTrader ID), and 1a and 1b only when B is not simply pair A handed back again. A pair B that
does not grant the account is never refreshed or replayed: that could only do harm.

New pairs reach the env file as an application persists a refreshed pair, so the file never holds
a spent refresh token while a newer pair exists, except between a refresh's answer and its write:

- a pair refreshed from the pair the file holds inherits that grant and is written at once, before
  it is checked, because its parent's refresh token is now spent;
- any other new pair - B, a new grant, or one refreshed from a pair not in the file - is checked
  first and written only if it is then the best pair to keep.

So pair B replaces A in the file at step 2, before A is rotated. That is acceptable: A stays in
memory for the rest of the run, and B, once checked, is a working pair whose refresh token is
unused.

What is measured is the application-only path: the account list and `ProtoOARefreshTokenReq` on
a connection authenticated at application level only, which is how the adapter refreshes when it
starts or when an account authentication is rejected. Its proactive refresh runs over an
account-authenticated connection instead, which this script does not exercise. Token requests go
to the account's own host, by its `isLive` flag, once an account list has shown it; the first
list goes to the demo host, which serves the list for either. The adapter's start-up refresh goes
to the demo host instead (`CTraderAccountClient`, not yet verified live), so a result here holds
for the account's own host only.

**Read-only towards trading by construction.** Every request goes through `TokenRequester`,
which refuses any payload class outside `ALLOWED_REQUESTS`; no order request is named anywhere in
this module. Tokens, the client id and secret, and account identifiers are never printed; a
refusal is reported by its error code only, never by the venue's description.

    uv run python scripts/verify_tokens.py --rotate-tokens

Exit code: 2 if it refused or was not started; 1 if an item is `DIFFERS`, no working pair is
left, a pair could not be written, or the run failed; 0 otherwise.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib.util
import math
import pathlib
import secrets
import sys
import threading
import time
import webbrowser
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from google.protobuf.message import Message

from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderRequestError
from nautilus_ctrader.constants import DEMO_HOST, LIVE_HOST, PROTOBUF_PORT
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

# scripts/ is not a package; get_tokens.py is loaded by file path, as verify_live_data.py does.
_GET_TOKENS_SPEC = importlib.util.spec_from_file_location(
    "get_tokens",
    pathlib.Path(__file__).resolve().with_name("get_tokens.py"),
)
get_tokens = importlib.util.module_from_spec(_GET_TOKENS_SPEC)
sys.modules[_GET_TOKENS_SPEC.name] = get_tokens
_GET_TOKENS_SPEC.loader.exec_module(get_tokens)


# The only payload classes this script is allowed to send.
ALLOWED_REQUESTS: frozenset[type[Message]] = frozenset(
    {
        oa.ProtoOAApplicationAuthReq,
        oa.ProtoOAGetAccountListByAccessTokenReq,
        oa.ProtoOAAccountAuthReq,
        oa.ProtoOARefreshTokenReq,
        oa.ProtoOATraderReq,
    },
)

OK = "OK"
DIFFERS = "DIFFERS"
UNKNOWN = "UNKNOWN"

ACCEPTED = "accepted"
REFUSED = "refused"
FAILED = "failed"

Decision = tuple[str, tuple[str, ...]]

_TRADER_LOGIN_KEY = "CTRADER_TRADER_LOGIN"
_REQUIRED_KEYS = (
    get_tokens.CLIENT_ID_KEY,
    get_tokens.CLIENT_SECRET_KEY,
    get_tokens.ACCESS_TOKEN_KEY,
    get_tokens.REFRESH_TOKEN_KEY,
    _TRADER_LOGIN_KEY,
)

_OPERATION_TIMEOUT_SECS = 60.0

# `os.replace` on Windows fails with PermissionError while another process holds the file open.
_WRITE_ATTEMPTS = 4
_WRITE_RETRY_DELAY_SECS = 0.5

# Events that tell a connection it lost the account. An uncorrelated error is not one: it is
# most likely the late answer to a request that already timed out.
_LOSS_SIGNALS = frozenset(
    {
        "ProtoOAAccountDisconnectEvent",
        "ProtoOAClientDisconnectEvent",
        "ProtoOAAccountsTokenInvalidatedEvent",
    },
)

_TITLES = {
    "1a": "a second authorisation and its pair's refresh leave the first access token working",
    "1b": "the first refresh token still works after a second authorisation and its refresh",
    "2": "a used refresh token is refused",
    "3": "one access token authorises the same account on two connections at once",
}


class RequestNotAllowed(RuntimeError):
    """A payload outside `ALLOWED_REQUESTS` reached the requester."""


class MissingSetting(Exception):
    """The env file lacks a key the run needs, or holds an unusable value."""


class BrowserNotOpened(RuntimeError):
    """No browser could be opened for the authorisation page."""


class _ApplicationAuthRefused(Exception):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True)
class Outcome:
    """How the venue answered one operation: accepted, refused with an error code, or not at all."""

    status: str
    reason: str | None = None

    @classmethod
    def accepted(cls) -> Outcome:
        return cls(ACCEPTED)

    @classmethod
    def refused(cls, error_code: str) -> Outcome:
        return cls(REFUSED, error_code)

    @classmethod
    def failed(cls, reason: str) -> Outcome:
        return cls(FAILED, reason)

    @property
    def ok(self) -> bool:
        return self.status == ACCEPTED

    @property
    def text(self) -> str:
        if self.status == REFUSED:
            return f"refused with {self.reason}"
        if self.status == FAILED:
            return f"failed: {self.reason}"
        return self.status


@dataclass(frozen=True)
class Finding:
    item: str
    title: str
    status: str
    detail: tuple[str, ...] = ()


@dataclass
class TokenPair:
    """One access/refresh pair as the run knows it. Its repr never shows the tokens.

    - `refresh_used` is set once the refresh token was sent, whatever the answer: a refresh that
      got no answer may still have spent it.
    - `parent` is the label of the pair whose refresh produced this one.
    """

    label: str
    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_at_secs: float | None
    refresh_used: bool = False
    last_check: Outcome | None = None
    grants_account: bool = False
    parent: str | None = None

    def same_as(self, other: TokenPair) -> bool:
        """Whether either token equals `other`'s, compared in memory only."""
        return secrets.compare_digest(
            self.access_token.encode(),
            other.access_token.encode(),
        ) or secrets.compare_digest(self.refresh_token.encode(), other.refresh_token.encode())


@dataclass(frozen=True)
class Settings:
    client_id: str = field(repr=False)
    client_secret: str = field(repr=False)
    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_at_secs: float | None
    trader_login: int = field(repr=False)


@dataclass(frozen=True)
class ConnectionObservation:
    """What one of the two connections in item 3 saw. `events` are message class names."""

    name: str
    account_auth: Outcome | None
    events: tuple[str, ...] = ()
    lost: str | None = None
    trader_read: Outcome | None = None


class QuietLogger:
    """Enough of the Nautilus `Logger` interface for `CTraderConnection`.

    Debug and info are dropped so the run's own lines are the only thing on stdout; an
    exception is reduced to its type, since a venue rejection's message carries its description.
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


class TokenRequester:
    """The one place this script sends a request, and the read-only guarantee itself."""

    def __init__(self, connection: CTraderConnection) -> None:
        self._connection = connection

    async def request(self, payload: Message) -> Message:
        if type(payload) not in ALLOWED_REQUESTS:
            raise RequestNotAllowed(
                f"{type(payload).__name__} is not one of this script's requests"
            )
        return await self._connection.request(payload)


# -- Decisions ------------------------------------------------------------------------------
#
# Each turns outcomes into a verdict and the lines that justify it, with no I/O. A verdict is
# measured against the hope the item's title states: DIFFERS is an answer, not a malfunction.


def _describe(outcome: Outcome | None) -> str:
    return "not run" if outcome is None else outcome.text


def decide_access_after_second_authorisation(
    before: Outcome | None,
    *,
    second_problem: str | None,
    after_authorisation: Outcome | None,
    after_refresh: Outcome | None,
) -> Decision:
    """Item 1a, from A's access-token checks before and after B's authorisation and refresh.

    `second_problem` says why pair B cannot answer the question; `None` when it can.
    """
    if second_problem is not None:
        return UNKNOWN, (second_problem,)
    if before is None or not before.ok:
        return UNKNOWN, (f"A's access token did not work to begin with: {_describe(before)}",)
    if after_authorisation is None or after_authorisation.status == FAILED:
        return UNKNOWN, (
            f"A's access token after the second authorisation: {_describe(after_authorisation)}",
        )
    if after_authorisation.status == REFUSED:
        return DIFFERS, (
            f"A's access token was {after_authorisation.text} after the second authorisation",
        )
    worked = "A's access token still works after the second authorisation"
    if after_refresh is None or after_refresh.status == FAILED:
        return UNKNOWN, (
            worked,
            f"after B's refresh it was {_describe(after_refresh)}, so that half is not decided",
        )
    if after_refresh.status == REFUSED:
        return DIFFERS, (worked, f"but it was {after_refresh.text} after B's refresh")
    return OK, (worked, "and after B's refresh")


def decide_refresh_after_second_authorisation(
    *,
    second_problem: str | None,
    second_refreshed: bool,
    first_refresh: Outcome | None,
) -> Decision:
    """Item 1b, from A's refresh after B's authorisation and, if it happened, B's refresh."""
    if second_problem is not None:
        return UNKNOWN, (second_problem,)
    if first_refresh is None or first_refresh.status == FAILED:
        return UNKNOWN, (f"A's refresh: {_describe(first_refresh)}",)
    after = "the second authorisation" + (" and B's refresh" if second_refreshed else "")
    if first_refresh.status == REFUSED:
        return DIFFERS, (
            f"A's refresh token was {first_refresh.text} after {after}",
            "it was not tried before the second authorisation, so it may already have been "
            "spent before this run",
        )
    if second_refreshed:
        return OK, (f"A's refresh token still worked after {after}",)
    return OK, (
        "A's refresh token still worked after the second authorisation",
        "this covers the authorisation only: B was not refreshed",
    )


def decide_refresh_token_reuse(
    first: Outcome | None,
    reuse: Outcome | None,
    *,
    second_problem: str | None = None,
    after_replay: Outcome | None = None,
    other_after_replay: Outcome | None = None,
) -> Decision:
    """Item 2, from B's first refresh and the replay of the same refresh token.

    `after_replay` and `other_after_replay` are the checks of pairs B' and A' after the replay;
    they are reported, not judged.
    """
    if second_problem is not None:
        return UNKNOWN, (second_problem,)
    if first is None or not first.ok:
        return UNKNOWN, (f"B's first refresh: {_describe(first)}",)
    if reuse is None or reuse.status == FAILED:
        return UNKNOWN, (f"the replay of B's used refresh token: {_describe(reuse)}",)
    detail = (
        [f"B's used refresh token, replayed after every other token step, was {reuse.text}"]
        if reuse.status == REFUSED
        else ["B's used refresh token, replayed after every other token step, was accepted"]
    )
    if after_replay is not None:
        detail.append(f"pair B' after the replay: access token {after_replay.text}")
        if after_replay.status == REFUSED:
            detail.append("so the replay revoked the pair it had already been refreshed into")
    if other_after_replay is not None:
        detail.append(
            f"pair A' (the other grant) after the replay: access token {other_after_replay.text}",
        )
        if other_after_replay.status == REFUSED:
            detail.append("so the replay may revoke every grant of this cTrader ID")
    return (OK if reuse.status == REFUSED else DIFFERS), tuple(detail)


def _signalled(observation: ConnectionObservation) -> bool:
    """A refused request or a loss event; a request that got no answer is not a signal."""
    refused = [
        o
        for o in (observation.account_auth, observation.trader_read)
        if o is not None and o.status == REFUSED
    ]
    return bool(refused) or any(event in _LOSS_SIGNALS for event in observation.events)


def _describe_connection(observation: ConnectionObservation) -> str:
    text = (
        f"connection {observation.name}: account auth {_describe(observation.account_auth)}, "
        f"events {', '.join(observation.events) or 'none'}, "
        f"trader read {_describe(observation.trader_read)}"
    )
    if observation.lost is not None:
        text += f", connection lost ({observation.lost})"
    return text


def decide_same_account_twice(
    observations: Sequence[ConnectionObservation],
    skipped: str | None = None,
) -> Decision:
    """Item 3. A refusal or a loss message from the venue differs; a silent drop decides nothing."""
    if skipped is not None:
        return UNKNOWN, (skipped,)
    if not observations:
        return UNKNOWN, ("no connection was opened",)
    first = observations[0]
    if first.account_auth is None or not first.account_auth.ok:
        return UNKNOWN, (
            f"connection {first.name} could not authorise the account: "
            f"{_describe(first.account_auth)}",
        )
    detail = tuple(_describe_connection(o) for o in observations)
    if any(_signalled(o) for o in observations):
        return DIFFERS, detail
    settled = all(
        o.account_auth is not None
        and o.account_auth.ok
        and o.trader_read is not None
        and o.trader_read.ok
        and o.lost is None
        for o in observations
    )
    if not settled:
        return UNKNOWN, (*detail, "a connection failed without the venue saying why")
    return OK, detail


def keep_order(pairs: Sequence[TokenPair]) -> list[TokenPair]:
    """The pairs worth keeping, best first.

    - Only pairs whose refresh token was never sent and whose access token was not refused.
    - If any of those grants the configured account, only those: a pair that may belong to
      another cTrader ID must never replace one that is known to fit.
    - Newest first within that.
    """
    usable = [
        p
        for p in reversed(pairs)
        if not p.refresh_used and (p.last_check is None or p.last_check.status != REFUSED)
    ]
    granting = [p for p in usable if p.grants_account]
    return granting or usable


async def _in_daemon_thread(function: Callable[[], object]) -> object:
    """Await `function()` run in a daemon thread.

    Not `asyncio.to_thread`: on Ctrl+C, `asyncio.run` waits for its executor's threads, which
    would hold the exit until the browser wait times out. A daemon thread is simply abandoned.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[object] = loop.create_future()

    def settle(result: object, error: BaseException | None) -> None:
        if future.done():
            return
        if error is None:
            future.set_result(result)
        else:
            future.set_exception(error)

    def target() -> None:
        try:
            result, error = function(), None
        except BaseException as e:
            result, error = None, e
        # The loop is closed if the run was abandoned meanwhile; nobody waits for this then.
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(settle, result, error)

    threading.Thread(target=target, daemon=True).start()
    return await future


async def _outcome_of(operation: Callable[[], Awaitable[object]]) -> Outcome:
    """Run `operation`, bounded in time, and say how the venue answered it."""
    try:
        async with asyncio.timeout(_OPERATION_TIMEOUT_SECS):
            await operation()
    except _ApplicationAuthRefused as e:
        return Outcome.failed(f"application auth refused with {e.error_code}")
    except CTraderRequestError as e:
        return Outcome.refused(e.error_code)
    except RequestNotAllowed:
        raise
    except Exception as e:
        # Deliberately broad: one failed step must never stop the steps after it.
        return Outcome.failed(type(e).__name__)
    return Outcome.accepted()


# -- The run --------------------------------------------------------------------------------


@dataclass
class _Watch:
    """One of the two connections in step 6, and what it has seen so far."""

    name: str
    connection: CTraderConnection
    account_auth: Outcome | None = None
    events: list[str] = field(default_factory=list)
    lost: str | None = None
    trader_read: Outcome | None = None

    def __post_init__(self) -> None:
        self.requester = TokenRequester(self.connection)
        self.connection.set_event_handler(self.on_event)
        self.connection.set_disconnect_handler(self.on_disconnect)

    def on_event(self, message: Message) -> None:
        self.events.append(type(message).__name__)

    def on_disconnect(self, error: Exception) -> None:
        self.lost = type(error).__name__

    def observation(self) -> ConnectionObservation:
        return ConnectionObservation(
            self.name,
            self.account_auth,
            tuple(self.events),
            self.lost,
            self.trader_read,
        )


class Runner:
    """Runs the steps, collecting findings and writing each new pair to the env file.

    `authorise` is the blocking browser authorisation; it runs in a daemon thread.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        env_file: pathlib.Path,
        authorise: Callable[[], object],
        logger: object,
        listen_secs: float,
        demo_host: str = DEMO_HOST,
        live_host: str = LIVE_HOST,
        port: int = PROTOBUF_PORT,
        tls: bool = True,
    ) -> None:
        self._settings = settings
        self._env_file = env_file
        self._authorise = authorise
        self._log = logger
        self._listen_secs = listen_secs
        self._demo_host = demo_host
        self._live_host = live_host
        self._port = port
        self._tls = tls
        self._token_host = demo_host
        self._account: tuple[str, int] | None = None
        self._finished = False
        self.pairs: list[TokenPair] = []
        self.findings: list[Finding] = []
        self.kept: TokenPair | None = None
        self.written: TokenPair | None = None
        self.notes: list[str] = []
        self.write_failed = False

    async def run(self) -> None:
        settings = self._settings
        a = TokenPair("A", settings.access_token, settings.refresh_token, settings.expires_at_secs)
        self.pairs.append(a)

        before = await self._check(a, "step 1  pair A")

        b = await self._authorise_again()
        second_problem = self._second_problem(b)
        after_authorisation = None
        same_pair = False
        if b is not None:
            same_pair = b.same_as(a)
            if same_pair:
                self._say("step 2  pair B is pair A handed back again")
            after_authorisation = await self._check(a, "step 2  pair A, after B's authorisation")

        b_refresh = b_new = None
        if b is not None and second_problem is not None:
            self._say("step 3  pair B is not refreshed: it does not fit the configured account")
        elif b is not None:
            b_refresh, b_new = await self._refresh(b, "B'", "step 3", "refresh of pair B")
            if same_pair:
                # A's refresh token may be the one just sent; never send it a second time here.
                a.refresh_used = True

        after_refresh = a_refresh = a_new = None
        if a.refresh_used:
            self._say("step 4  pair A is not refreshed: B is the same pair, so it may be spent")
        else:
            if b_refresh is not None and b_refresh.ok:
                after_refresh = await self._check(a, "step 4  pair A, after B's refresh")
            a_refresh, a_new = await self._refresh(a, "A'", "step 4", "refresh of pair A")

        reuse = after_replay = other_after_replay = None
        if b is not None and b_refresh is not None and b_refresh.ok:
            reuse, _b_again = await self._refresh(
                b,
                "B''",
                "step 5",
                "replay of B's used refresh token",
            )
            if b_new is not None:
                after_replay = await self._check(b_new, "step 5  pair B', after the replay")
            if a_new is not None:
                other_after_replay = await self._check(a_new, "step 5  pair A', after the replay")

        pair_problem = second_problem
        if pair_problem is None and same_pair:
            pair_problem = "the second authorisation returned the same pair"
        self._add(
            "1a",
            decide_access_after_second_authorisation(
                before,
                second_problem=pair_problem,
                after_authorisation=after_authorisation,
                after_refresh=after_refresh,
            ),
        )
        self._add(
            "1b",
            decide_refresh_after_second_authorisation(
                second_problem=pair_problem,
                second_refreshed=b_refresh is not None and b_refresh.ok,
                first_refresh=a_refresh,
            ),
        )
        self._add(
            "2",
            decide_refresh_token_reuse(
                b_refresh,
                reuse,
                second_problem=second_problem,
                after_replay=after_replay,
                other_after_replay=other_after_replay,
            ),
        )

        self.kept = await self._keep("step 6")
        decision = await self._same_account_twice(self.kept)
        self._add("3", decision)
        if decision[0] != OK and self.kept is not None:
            await self._confirm_kept()
        self._finished = True

    def _second_problem(self, b: TokenPair | None) -> str | None:
        """Why pair B cannot answer items 1a, 1b and 2, or `None` when it can."""
        if b is None:
            return "no second authorisation was made"
        if not b.grants_account:
            return (
                f"pair B was not shown to grant the {_TRADER_LOGIN_KEY} account, so it may "
                "come from another cTrader ID"
            )
        return None

    async def _confirm_kept(self) -> None:
        """Step 8: after an unclean step 7, re-check the kept pair and replace it if it is dead."""
        previous = self.kept
        outcome = await self._check(previous, f"step 8  pair {previous.label}, after step 7")
        if outcome.status != REFUSED:
            return
        self.kept = await self._keep("step 8")
        replacement = "no other pair works" if self.kept is None else f"kept {self.kept.label}"
        self.notes.append(
            f"Pair {previous.label}'s access token was refused after step 7; {replacement}.",
        )

    def _add(self, item: str, decision: Decision) -> None:
        status, detail = decision
        self.findings.append(Finding(item, _TITLES[item], status, detail))

    def _say(self, line: str) -> None:
        print(line, flush=True)

    def _connection(self, host: str) -> CTraderConnection:
        return CTraderConnection(host, self._port, logger=self._log, tls=self._tls)

    async def _open(self, connection: CTraderConnection, requester: TokenRequester) -> None:
        await connection.connect()
        try:
            await requester.request(
                oa.ProtoOAApplicationAuthReq(
                    clientId=self._settings.client_id,
                    clientSecret=self._settings.client_secret,
                ),
            )
        except CTraderRequestError as e:
            raise _ApplicationAuthRefused(e.error_code) from None

    async def _token_operation(
        self,
        operation: Callable[[TokenRequester], Awaitable[None]],
    ) -> Outcome:
        """Run `operation` on a fresh connection authenticated at application level only."""
        connection = self._connection(self._token_host)
        requester = TokenRequester(connection)

        async def run() -> None:
            await self._open(connection, requester)
            await operation(requester)

        try:
            return await _outcome_of(run)
        finally:
            await connection.close()

    # -- Steps ------------------------------------------------------------------------------

    async def _check(self, pair: TokenPair, step: str) -> Outcome:
        """Whether `pair`'s access token lists the granted accounts, as the adapter's start does."""
        listed: list[oa.ProtoOAGetAccountListByAccessTokenRes] = []

        async def list_accounts(requester: TokenRequester) -> None:
            listed.append(
                await requester.request(
                    oa.ProtoOAGetAccountListByAccessTokenReq(accessToken=pair.access_token),
                ),
            )

        outcome = await self._token_operation(list_accounts)
        pair.last_check = outcome
        if listed:
            pair.grants_account = self._note_account(listed[0])
        line = f"{step}: access token {outcome.text}"
        if outcome.ok and not pair.grants_account:
            line += f", but it does not grant the {_TRADER_LOGIN_KEY} account"
        self._say(line)
        return outcome

    def _note_account(self, listed: oa.ProtoOAGetAccountListByAccessTokenRes) -> bool:
        """Whether the list grants the configured login; if so, remember its id and host."""
        matched = [
            a
            for a in listed.ctidTraderAccount
            if a.HasField("traderLogin") and a.traderLogin == self._settings.trader_login
        ]
        if len(matched) != 1:
            return False
        entry = matched[0]
        host = self._live_host if entry.isLive else self._demo_host
        self._account = (host, entry.ctidTraderAccountId)
        self._token_host = host
        return True

    async def _authorise_again(self) -> TokenPair | None:
        self._say(
            "step 2  browser authorisation: log in with the same cTrader ID and grant the same "
            "account(s) as before",
        )
        try:
            response = await _in_daemon_thread(self._authorise)
        except get_tokens.AuthorizationError as e:
            # The redirect's own error value, already sanitised; it carries no secret.
            self._say(f"step 2  browser authorisation: not granted ({e})")
            return None
        except Exception as e:
            self._say(f"step 2  browser authorisation failed: {type(e).__name__}")
            return None
        pair = TokenPair(
            "B",
            response.access_token,
            response.refresh_token,
            time.time() + response.expires_in,
        )
        self._say("step 2  browser authorisation: pair B obtained")
        await self._obtained(pair, "step 2")
        return pair

    async def _refresh(
        self,
        pair: TokenPair,
        label: str,
        step: str,
        what: str,
    ) -> tuple[Outcome, TokenPair | None]:
        """Refresh `pair` into a new pair called `label`, as the adapter does it."""
        obtained: list[TokenPair] = []

        async def refresh(requester: TokenRequester) -> None:
            response = await requester.request(
                oa.ProtoOARefreshTokenReq(refreshToken=pair.refresh_token),
            )
            if not (
                get_tokens.is_clean_token(response.accessToken)
                and get_tokens.is_clean_token(response.refreshToken)
            ):
                raise ValueError("the refresh response carries an unusable token")
            obtained.append(
                TokenPair(
                    label,
                    response.accessToken,
                    response.refreshToken,
                    time.time() + response.expiresIn,
                    parent=pair.label,
                ),
            )

        pair.refresh_used = True
        outcome = await self._token_operation(refresh)
        self._say(f"{step}  {what}: {outcome.text}")
        new = obtained[0] if obtained and outcome.ok else None
        if new is not None:
            await self._obtained(new, step)
        return outcome, new

    async def _obtained(self, pair: TokenPair, step: str) -> None:
        """Record a new pair, check it, and put it in the env file as the module docstring says.

        A pair refreshed from the held pair is written before its check, since the held pair's
        refresh token is now spent; any other pair only once checked, so a pair from another
        cTrader ID never replaces one that fits.
        """
        held = self.held
        self.pairs.append(pair)
        if held is not None and pair.parent == held.label:
            await self._write(pair)
            await self._check(pair, f"{step}  pair {pair.label}")
            return
        await self._check(pair, f"{step}  pair {pair.label}")
        best = keep_order(self.pairs)
        if best and best[0] is pair:
            await self._write(pair)

    @property
    def held(self) -> TokenPair | None:
        """The pair the env file holds now: the last one written, else the original."""
        if self.written is not None:
            return self.written
        return self.pairs[0] if self.pairs else None

    async def _write(self, pair: TokenPair) -> bool:
        """Write `pair` to the env file; on failure tell the owner what is lost, naming no token."""
        expires = "" if pair.expires_at_secs is None else str(int(pair.expires_at_secs))
        updates = {
            get_tokens.ACCESS_TOKEN_KEY: pair.access_token,
            get_tokens.REFRESH_TOKEN_KEY: pair.refresh_token,
            get_tokens.TOKEN_EXPIRES_AT_KEY: expires,
        }
        error: OSError | None = None
        for attempt in range(_WRITE_ATTEMPTS):
            try:
                get_tokens.update_env_file(self._env_file, updates)
            except PermissionError as e:
                error = e
                if attempt + 1 < _WRITE_ATTEMPTS:
                    await asyncio.sleep(_WRITE_RETRY_DELAY_SECS)
                continue
            except OSError as e:
                error = e
                break
            self.written = pair
            self._say(f"        pair {pair.label} written to {self._env_file.name}")
            return True

        message = (
            f"pair {pair.label} could NOT be written to {self._env_file.name} "
            f"({type(error).__name__}): it exists only in this run's memory and is lost when the "
            "run ends"
        )
        if pair.parent is not None:
            message += f"; pair {pair.parent}'s refresh token, which produced it, is spent"
        print(f"warning: {message}.", file=sys.stderr, flush=True)
        self.notes.append(f"Warning: {message}.")
        self.write_failed = True
        return False

    async def _keep(self, step: str) -> TokenPair | None:
        """The best working pair by `keep_order`, checked right now and put in the env file."""
        for pair in keep_order(self.pairs):
            if not (await self._check(pair, f"{step}  pair {pair.label}, to keep it")).ok:
                continue
            if pair is not self.held and not await self._write(pair):
                continue
            self._say(f"{step}  keeping pair {pair.label}")
            return pair
        self._say(f"{step}  no pair can be kept")
        return None

    async def _same_account_twice(self, pair: TokenPair | None) -> Decision:
        """Step 7: two connections authorise the same account with one access token."""
        if pair is None:
            return decide_same_account_twice((), skipped="no working pair is left to test with")
        if self._account is None or not pair.grants_account:
            return decide_same_account_twice(
                (),
                skipped=f"the kept pair does not grant the {_TRADER_LOGIN_KEY} account",
            )
        host, account_id = self._account
        watches: list[_Watch] = []
        try:
            for name in ("1", "2"):
                watch = _Watch(name, self._connection(host))
                watches.append(watch)
                watch.account_auth = await self._authorise_account(
                    watch,
                    account_id,
                    pair.access_token,
                )
                self._say(f"step 7  connection {name} account auth: {watch.account_auth.text}")
                if not watches[0].account_auth.ok:
                    break
            if watches[0].account_auth.ok:
                self._say(f"step 7  listening {self._listen_secs:g} s for disconnect events")
                await asyncio.sleep(self._listen_secs)
                for watch in watches:
                    if watch.account_auth.ok:
                        watch.trader_read = await self._read_trader(watch, account_id)
                        read = watch.trader_read.text
                        self._say(f"step 7  connection {watch.name} trader read: {read}")
        finally:
            for watch in watches:
                await watch.connection.close()
        return decide_same_account_twice(tuple(w.observation() for w in watches))

    async def _authorise_account(
        self,
        watch: _Watch,
        account_id: int,
        access_token: str,
    ) -> Outcome:
        async def authorise() -> None:
            await self._open(watch.connection, watch.requester)
            await watch.requester.request(
                oa.ProtoOAAccountAuthReq(ctidTraderAccountId=account_id, accessToken=access_token),
            )

        return await _outcome_of(authorise)

    async def _read_trader(self, watch: _Watch, account_id: int) -> Outcome:
        async def read() -> None:
            await watch.requester.request(oa.ProtoOATraderReq(ctidTraderAccountId=account_id))

        return await _outcome_of(read)

    # -- Report -----------------------------------------------------------------------------

    def report(self) -> str:
        """The findings and which pair the env file holds. Carries no token or identifier."""
        lines = ["cTrader token behaviour (spends refresh tokens; read-only towards trading)", ""]
        for finding in self.findings:
            lines.append(f"{finding.status:<8} {finding.item}. {finding.title}")
            lines.extend(f"         {line}" for line in finding.detail)
        counts = Counter(f.status for f in self.findings)
        lines += [
            "",
            f"{OK} {counts[OK]}, {DIFFERS} {counts[DIFFERS]}, {UNKNOWN} {counts[UNKNOWN]}",
            "",
            *self.notes,
            self._kept_line(),
        ]
        return "\n".join(lines)

    def _kept_line(self) -> str:
        env_name = self._env_file.name
        if self.kept is not None:
            expiry = (
                ""
                if self.kept.expires_at_secs is None
                else ", access token expires "
                + datetime.fromtimestamp(self.kept.expires_at_secs, tz=UTC).isoformat()
            )
            return f"Kept pair {self.kept.label}: {env_name} holds it{expiry}."
        held = self.held
        if held is None:
            return f"The run did not start; {env_name} is unchanged."
        spent = (
            " Its refresh token was already sent, so it cannot be refreshed again: issue a new "
            "pair with scripts/get_tokens.py before its access token expires."
            if held.refresh_used
            else ""
        )
        if not self._finished:
            return f"The run did not finish; {env_name} holds pair {held.label}.{spent}"
        check = held.last_check
        if check is not None and check.status == REFUSED:
            return (
                f"No working pair: {env_name} holds pair {held.label}, whose access token was "
                f"{check.text} at its last check. Issue a new pair with scripts/get_tokens.py."
            )
        if check is not None and check.status == FAILED:
            return (
                f"No pair could be confirmed working: {env_name} holds pair {held.label}, whose "
                f"last check {check.text} without a refusal, so it may still work; check the "
                f"connection before replacing it.{spent}"
            )
        state = "never checked" if check is None else "accepted at its last check"
        return (
            f"No pair could be kept: {env_name} holds pair {held.label}, whose access token was "
            f"{state}.{spent}"
        )


# -- Command line ---------------------------------------------------------------------------


def settings_from_env(env: dict[str, str]) -> Settings:
    """The run's settings from the env file's values; raises `MissingSetting` naming keys only."""
    missing = [key for key in _REQUIRED_KEYS if not env.get(key)]
    if missing:
        raise MissingSetting(f"missing in the env file: {', '.join(missing)}")
    try:
        trader_login = int(env[_TRADER_LOGIN_KEY])
    except ValueError:
        raise MissingSetting(f"{_TRADER_LOGIN_KEY} in the env file is not a number") from None
    try:
        expires_at_secs = float(env.get(get_tokens.TOKEN_EXPIRES_AT_KEY, ""))
    except ValueError:
        expires_at_secs = None
    if expires_at_secs is not None and not math.isfinite(expires_at_secs):
        expires_at_secs = None
    return Settings(
        client_id=env[get_tokens.CLIENT_ID_KEY],
        client_secret=env[get_tokens.CLIENT_SECRET_KEY],
        access_token=env[get_tokens.ACCESS_TOKEN_KEY],
        refresh_token=env[get_tokens.REFRESH_TOKEN_KEY],
        expires_at_secs=expires_at_secs,
        trader_login=trader_login,
    )


def browser_authorisation(
    settings: Settings,
    *,
    redirect_uri: str,
    port: int,
    path: str,
    timeout_secs: float,
) -> Callable[[], object]:
    """`get_tokens.py`'s browser authorisation as one blocking call returning its `TokenResponse`.

    The authorisation URL carries the client id, so it is only ever handed to the browser; with
    no browser to open, the call fails at once instead of waiting out the timeout.
    """

    def authorise() -> object:
        state = secrets.token_urlsafe(32)
        url = get_tokens.build_authorization_url(settings.client_id, redirect_uri, state)

        def open_browser() -> None:
            try:
                opened = webbrowser.open(url)
            except webbrowser.Error:
                opened = False
            if not opened:
                raise BrowserNotOpened("no browser could be opened for the authorisation page")

        code = get_tokens.wait_for_authorization_code(
            "127.0.0.1",
            port,
            path,
            timeout_secs,
            state=state,
            on_listening=open_browser,
        )
        return get_tokens.exchange_code(
            code,
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            redirect_uri=redirect_uri,
        )

    return authorise


def plan(env_file: pathlib.Path, *, timeout_secs: float, listen_secs: float) -> str:
    return "\n".join(
        [
            f"This run rotates the cTrader Open API tokens in {env_file}. It will:",
            "  1. check the access token of the pair now there (pair A);",
            "  2. open the cTrader authorisation page in your browser: log in with the same "
            "cTrader ID and grant the same account(s) as before (pair B); it waits up to "
            f"{timeout_secs:g} s;",
            "  3. refresh pair B;",
            "  4. check pair A again, then refresh it;",
            "  5. replay B's used refresh token once;",
            f"  6. keep the newest working pair that grants the {_TRADER_LOGIN_KEY} account;",
            "  7. with it, authorise that account on two connections at once, listen "
            f"{listen_secs:g} s, and read the account's trader record on each;",
            "  8. if step 7 was not clean, check the kept pair again and replace it if it died.",
            f"The token lines of {env_file.name} (access, refresh, expiry) are rewritten each "
            "time a new pair is obtained, and end holding the kept pair.",
            "No order is sent: the only requests are authentication, the account list, token "
            "refresh and the trader read.",
            "",
            "Before starting, stop every node or process that uses these tokens: a refresh made "
            "elsewhere during the run would spend the tokens under it, and its connections may "
            "be dropped.",
            "",
            "RISK: the second authorisation, or the replay of a spent refresh token, may make the "
            "venue revoke EVERY pair of this cTrader ID, including the token sets of other nodes, "
            "processes and applications.",
            "The run may then end with no working pair; a new browser login through "
            "scripts/get_tokens.py would then be needed.",
            "Do not restart the stopped nodes on their old pairs until the report shows those "
            "pairs survived.",
            "",
        ],
    )


def _positive_float(text: str) -> float:
    value = float(text)
    if not value > 0:
        raise argparse.ArgumentTypeError(f"must be positive: {text!r}")
    return value


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check how cTrader tokens behave. Spends refresh tokens; rewrites .env.",
    )
    parser.add_argument(
        "--rotate-tokens",
        action="store_true",
        help="required: confirms that refresh tokens may be spent and the env file rewritten",
    )
    parser.add_argument("--env-file", type=pathlib.Path, default=_REPO_ROOT / ".env")
    parser.add_argument(
        "--redirect-uri",
        default="http://localhost:8080/callback",
        help="the application's registered redirect URI, as for get_tokens.py",
    )
    parser.add_argument(
        "--timeout-secs",
        type=_positive_float,
        default=300.0,
        help="how long to wait for the browser authorisation (default: 300)",
    )
    parser.add_argument(
        "--listen-secs",
        type=_positive_float,
        default=15.0,
        help="how long both connections listen for disconnect events (default: 15)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    # A presence check rather than `required=True`, so the refusal can say what it is for.
    if not args.rotate_tokens:
        print(
            "Refused: this script spends refresh tokens, makes a second authorisation and "
            "rewrites the token lines of the env file. Add --rotate-tokens to run it.",
            file=sys.stderr,
        )
        return 2
    try:
        port, path = get_tokens.parse_redirect_uri(args.redirect_uri)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2
    try:
        settings = settings_from_env(get_tokens.load_env(args.env_file))
    except FileNotFoundError:
        print(f"Refused: env file not found: {args.env_file}", file=sys.stderr)
        return 2
    except MissingSetting as e:
        print(f"Refused: {e}", file=sys.stderr)
        return 2

    print(plan(args.env_file, timeout_secs=args.timeout_secs, listen_secs=args.listen_secs))
    try:
        input("Press Enter to start, or Ctrl+C to stop: ")
    except (EOFError, KeyboardInterrupt):
        print("\nNot started. Nothing was sent and the env file is unchanged.", file=sys.stderr)
        return 2
    runner = Runner(
        settings,
        env_file=args.env_file,
        authorise=browser_authorisation(
            settings,
            redirect_uri=args.redirect_uri,
            port=port,
            path=path,
            timeout_secs=args.timeout_secs,
        ),
        logger=QuietLogger(),
        listen_secs=args.listen_secs,
    )
    failed = False
    try:
        asyncio.run(runner.run())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        failed = True
    except Exception as e:
        # Only the type: an exception's message can carry a venue description.
        print(f"error: {type(e).__name__}: the run failed", file=sys.stderr)
        failed = True
    print()
    print(runner.report())
    if failed or runner.kept is None or runner.write_failed:
        return 1
    return 1 if any(f.status == DIFFERS for f in runner.findings) else 0


if __name__ == "__main__":
    sys.exit(main())
