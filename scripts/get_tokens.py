"""One-time helper that performs the interactive cTrader Open API OAuth flow.

Run once per application, after registering it and filling `CTRADER_CLIENT_ID` /
`CTRADER_CLIENT_SECRET` into a `.env` file:

    uv run python scripts/get_tokens.py

It opens the authorization page in a browser, exchanges the code the redirect carries back
for an access/refresh token pair, writes the tokens into the same env file, and lists the
accounts the token grants so you can pick one. The application's registered redirect URI
must match `--redirect-uri` exactly (default `http://localhost:8080/callback`).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import html
import http.server
import json
import math
import os
import re
import secrets
import socketserver
import ssl
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderError, CTraderRequestError
from nautilus_ctrader.constants import DEMO_HOST, LIVE_HOST, PROTOBUF_PORT
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model

AUTHORIZATION_URL = "https://openapi.ctrader.com/apps/auth"
TOKEN_URL = "https://openapi.ctrader.com/apps/token"

CLIENT_ID_KEY = "CTRADER_CLIENT_ID"
CLIENT_SECRET_KEY = "CTRADER_CLIENT_SECRET"
ACCESS_TOKEN_KEY = "CTRADER_ACCESS_TOKEN"
REFRESH_TOKEN_KEY = "CTRADER_REFRESH_TOKEN"
TOKEN_EXPIRES_AT_KEY = "CTRADER_TOKEN_EXPIRES_AT"

# The callback server always binds IPv4 127.0.0.1 regardless of the redirect host, so only
# these are accepted as --redirect-uri hosts: a wider host could bind and expose the callback
# port, and "::1" would parse as loopback but never actually receive the redirect.
_LOOPBACK_HOSTNAMES = frozenset({"localhost", "127.0.0.1"})


class AuthorizationError(RuntimeError):
    """The redirect carried `?error=...` instead of a code."""


class TokenExchangeError(RuntimeError):
    """The token endpoint rejected the exchange, or its response was unusable.

    The message carries only the endpoint's own error fields or the bare HTTP status -
    never the client secret, the code, or a token value.
    """


@dataclass(frozen=True)
class TokenResponse:
    access_token: str
    refresh_token: str
    expires_in: int
    token_type: str | None


@dataclass(frozen=True)
class AccountRecord:
    ctid_trader_account_id: int
    is_live: bool | None
    trader_login: int | None
    broker_title_short: str | None


@dataclass(frozen=True)
class AccountsResult:
    permission_scope: int
    accounts: list[AccountRecord]


class _NullLogger:
    """Discards everything. Used when a caller of `list_accounts` has no logger to hand it."""

    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str) -> None:
        pass

    def exception(self, message: str, ex: BaseException) -> None:
        pass


class _PrintLogger:
    """Enough of the Nautilus `Logger` interface for `CTraderConnection`, printed to stderr.

    Debug is dropped: it would only add per-message envelope noise to a one-shot script.
    """

    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        print(message, file=sys.stderr)

    def warning(self, message: str) -> None:
        print(f"warning: {message}", file=sys.stderr)

    def error(self, message: str) -> None:
        print(f"error: {message}", file=sys.stderr)

    def exception(self, message: str, ex: BaseException) -> None:
        print(f"error: {message}: {ex!r}", file=sys.stderr)


def _strip_matching_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


_ENV_LINE_RE = re.compile(r"^([^=\s][^=]*?)\s*=\s*(.*)$")


def load_env(path: Path) -> dict[str, str]:
    """Parse a `KEY=VALUE` env file.

    Blank lines and whole-line `#` comments are ignored. A value may be wrapped in one pair
    of matching quotes, which is stripped. Missing keys are simply absent from the result;
    callers decide what is required.
    """
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _ENV_LINE_RE.match(line)
        if not match:
            continue
        env[match.group(1).strip()] = _strip_matching_quotes(match.group(2).strip())
    return env


def build_authorization_url(client_id: str, redirect_uri: str, state: str) -> str:
    """The one-time authorization page URL. Carries the client id, so it is only ever
    printed with `--print-url`."""
    query = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "trading",
            "state": state,
        },
    )
    return f"{AUTHORIZATION_URL}?{query}"


def _sanitize_for_terminal(value: str, *, max_len: int = 200) -> str:
    """Strip non-printable characters and cap the length, so a redirect's `error` value can't
    smuggle control sequences or an unbounded blob into terminal output."""
    return "".join(ch for ch in value if ch.isprintable())[:max_len]


class _CallbackServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Serves each connection on its own daemon thread, so one idle or slow-drip connection can
    never hold up the real redirect behind it."""

    daemon_threads = True
    # HTTPServer defaults this on; on Windows SO_REUSEADDR lets another process share or steal
    # the port instead of just permitting reuse of one still in TIME_WAIT.
    allow_reuse_address = sys.platform != "win32"


def wait_for_authorization_code(
    host: str,
    port: int,
    path: str,
    timeout_secs: float,
    *,
    state: str,
    on_listening: Callable[[], None] | None = None,
) -> str:
    """Serve exactly `path` until the redirect carries a code, an error, or the timeout ends.

    Every connection is handled on its own daemon thread (`_CallbackServer`), so an idle
    connection (a browser's speculative preconnect) or a slow-drip client can never delay the
    real redirect that arrives alongside it; `timeout_secs` is therefore an exact deadline
    measured from this call, not stretched by whatever a concurrent connection is doing.

    A request to any other path gets 404 and does not end the wait: browsers probe things
    like `/favicon.ico` on their own. So does a request whose `state` is missing or does not
    match `state` exactly (compared in constant time) - it could be a stray page rather than
    the real redirect, and must not be able to inject a code or abort the run. `on_listening`,
    if given, runs once the socket is bound and listening - the caller's cue to open the
    browser (or, in a test, to drive a request).

    TODO(verify): that the authorization endpoint echoes `state` back on the redirect; if it
    doesn't, every real callback will be rejected as a state mismatch.
    """
    outcome: dict[str, str] = {}
    outcome_lock = threading.Lock()
    outcome_ready = threading.Event()

    class _Handler(http.server.BaseHTTPRequestHandler):
        # A connection that opens and sends nothing, or trickles bytes in slowly - browsers do
        # the former for speculative preconnects - would otherwise block its handler thread
        # forever, since the base class leaves this as None. Each connection now has its own
        # thread, so this only ends that one thread; it never affects the deadline below.
        timeout = 5

        def do_GET(self) -> None:
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path != path:
                self.send_response(404)
                self.end_headers()
                return
            params = urllib.parse.parse_qs(parsed.query)
            request_state = params.get("state", [""])[0]
            # `compare_digest` requires both arguments to be either `str` restricted to ASCII
            # or `bytes`; encoding both sides first accepts any `request_state` (a stray
            # non-ASCII value must compare as a mismatch, not raise) while keeping the
            # comparison constant-time.
            if not secrets.compare_digest(request_state.encode(), state.encode()):
                self.send_response(400)
                self.end_headers()
                return
            if "code" in params:
                # Record before answering the browser: `_respond` writes to the socket, and if
                # that write fails (peer gone, RST, a full send window against the 5s socket
                # timeout), the code must already be captured so the caller gets it instead of
                # timing out.
                self._record(code=params["code"][0])
                self._respond("Authorization received. You can close this tab.")
            elif "error" in params:
                error = _sanitize_for_terminal(params["error"][0])
                self._record(error=error)
                self._respond(f"Authorization failed: {html.escape(error)}")
            else:
                self.send_response(400)
                self.end_headers()

        def _record(self, **result: str) -> None:
            # The first valid result wins; a later or concurrent connection must not overwrite
            # it, so the caller's return value can't be raced.
            with outcome_lock:
                if not outcome:
                    outcome.update(result)
                    outcome_ready.set()

        def _respond(self, message: str) -> None:
            body = f"<html><body>{message}</body></html>".encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass  # the default access log would echo the redirect's query string

    server = _CallbackServer((host, port), _Handler)
    # A short poll_interval keeps shutdown() (and so the deadline below) precise; the default
    # 0.5s would otherwise let a pending accept-loop iteration add up to half a second of its
    # own on top of timeout_secs.
    server_thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.1},
        daemon=True,
    )
    try:
        server_thread.start()
        if on_listening is not None:
            on_listening()
        deadline = time.monotonic() + timeout_secs
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not outcome_ready.wait(remaining):
            raise TimeoutError(f"no authorization redirect received within {timeout_secs:g}s")
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    if "error" in outcome:
        raise AuthorizationError(outcome["error"])
    return outcome["code"]


def _is_clean_token(value: object) -> bool:
    """True for a non-empty `str` with no CR or LF - what an access/refresh token must be to
    be written into an env file line and never split it or smuggle a second assignment."""
    return isinstance(value, str) and bool(value) and "\r" not in value and "\n" not in value


def exchange_code(
    code: str,
    *,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    token_url: str = TOKEN_URL,
    timeout_secs: float = 30.0,
) -> TokenResponse:
    """Exchange an authorization code for an access/refresh token pair.

    GET with query parameters, exactly as Spotware's own SDK does it
    (`ctrader_open_api/auth.py`).
    TODO(verify): confirm GET-vs-POST and the response field names against a live exchange;
    this endpoint is outside the protobuf schema this repository vendors.
    """
    query = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    try:
        with urllib.request.urlopen(f"{token_url}?{query}", timeout=timeout_secs) as response:
            body = response.read()
    except urllib.error.HTTPError as e:
        # e.url carries the full request URL, client secret included; chaining from it would
        # put that in any traceback.
        raise TokenExchangeError(f"token endpoint returned HTTP {e.code}") from None
    except urllib.error.URLError as e:
        raise TokenExchangeError(f"token endpoint request failed: {e.reason}") from None

    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        raise TokenExchangeError("token endpoint returned an unparsable response") from e
    if not isinstance(data, dict):
        raise TokenExchangeError("token endpoint response is not a JSON object")

    error_code = data.get("errorCode")
    if error_code:
        description = data.get("description")
        safe_code = _sanitize_for_terminal(str(error_code))
        safe_description = (
            _sanitize_for_terminal(str(description)) if description is not None else None
        )
        raise TokenExchangeError(
            f"token endpoint rejected the code: {safe_code}: {safe_description}",
        )

    access_token = data.get("accessToken")
    refresh_token = data.get("refreshToken")
    if not _is_clean_token(access_token):
        raise TokenExchangeError("token endpoint response has an invalid accessToken")
    if not _is_clean_token(refresh_token):
        raise TokenExchangeError("token endpoint response has an invalid refreshToken")

    expires_in = data.get("expiresIn")
    if (
        not isinstance(expires_in, int | float)
        or isinstance(expires_in, bool)
        or not math.isfinite(expires_in)
        or expires_in < 1
    ):
        raise TokenExchangeError("token endpoint response has an invalid expiresIn")

    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=int(expires_in),
        token_type=data.get("tokenType"),
    )


_ENV_ASSIGNMENT_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=")


def update_env_file(path: Path, updates: dict[str, str]) -> None:
    """Replace or append `KEY=VALUE` lines, keeping every other line, comment and the file's
    order, line-ending style and leading BOM. Written atomically: a temp file in the same
    directory, then `os.replace`. Every existing line for a key is replaced, not just the
    first. Raises `ValueError` if a value contains CR or LF, which would otherwise split into
    extra, unparsable lines.
    """
    for value in updates.values():
        if "\r" in value or "\n" in value:
            raise ValueError("env value must not contain a CR or LF")

    has_bom = path.exists() and path.read_bytes().startswith(b"\xef\xbb\xbf")
    original = path.read_text(encoding="utf-8-sig", newline="") if path.exists() else ""
    newline = "\r\n" if "\r\n" in original else "\n"

    seen: set[str] = set()
    lines: list[str] = []
    for line in original.splitlines():
        match = _ENV_ASSIGNMENT_RE.match(line)
        if match and match.group(1) in updates:
            key = match.group(1)
            lines.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            lines.append(line)
    for key, value in updates.items():
        if key not in seen:
            lines.append(f"{key}={value}")

    new_text = "".join(f"{line}{newline}" for line in lines)
    if has_bom:
        new_text = "\ufeff" + new_text
    _write_atomically(path, new_text)


def _write_atomically(path: Path, text: str) -> None:
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp_name, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp_name)
        raise


async def list_accounts(
    access_token: str,
    client_id: str,
    client_secret: str,
    *,
    host: str,
    port: int = PROTOBUF_PORT,
    tls: bool | ssl.SSLContext = True,
    logger: object | None = None,
) -> AccountsResult:
    """Authenticate the application, then list the accounts the access token grants.

    `ProtoOAGetAccountListByAccessTokenRes` echoes the access token back; only the account
    records and the permission scope are returned, never the token itself.
    """
    connection = CTraderConnection(
        host,
        port,
        logger=logger if logger is not None else _NullLogger(),
        tls=tls,
    )
    await connection.connect()
    try:
        await connection.request(
            oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
        )
        response = await connection.request(
            oa.ProtoOAGetAccountListByAccessTokenReq(accessToken=access_token),
        )
    finally:
        await connection.close()

    accounts = [
        AccountRecord(
            ctid_trader_account_id=account.ctidTraderAccountId,
            is_live=account.isLive if account.HasField("isLive") else None,
            trader_login=account.traderLogin if account.HasField("traderLogin") else None,
            broker_title_short=(
                account.brokerTitleShort if account.HasField("brokerTitleShort") else None
            ),
        )
        for account in response.ctidTraderAccount
    ]
    return AccountsResult(permission_scope=response.permissionScope, accounts=accounts)


def _print_accounts(result: AccountsResult, *, live: bool) -> None:
    try:
        scope_name = oa_model.ProtoOAClientPermissionScope.Name(result.permission_scope)
    except ValueError:
        scope_name = str(result.permission_scope)
    print(f"Permission scope: {scope_name}")

    for account in result.accounts:
        if account.is_live is None:
            is_live_text = "unknown"
        else:
            is_live_text = "live" if account.is_live else "demo"
        parts = [
            f"ctidTraderAccountId={account.ctid_trader_account_id}",
            f"isLive={is_live_text}",
        ]
        if account.trader_login is not None:
            parts.append(f"traderLogin={account.trader_login}")
        if account.broker_title_short is not None:
            parts.append(f"brokerTitleShort={account.broker_title_short}")
        print("  " + " ".join(parts))

        if account.is_live is not None and account.is_live != live:
            used = "live" if live else "demo"
            print(
                f"  warning: account {account.ctid_trader_account_id} is {is_live_text}, "
                f"but the {used} host was used; an account must be authorised on its own "
                "environment",
                file=sys.stderr,
            )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Perform the one-time cTrader OAuth flow and list the granted accounts.",
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--redirect-uri", default="http://localhost:8080/callback")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--timeout-secs", type=float, default=300.0)
    parser.add_argument("--print-url", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    try:
        env = load_env(args.env_file)
    except FileNotFoundError:
        print(f"Env file not found: {args.env_file}", file=sys.stderr)
        return 2

    missing = [key for key in (CLIENT_ID_KEY, CLIENT_SECRET_KEY) if not env.get(key)]
    if missing:
        print(f"Missing required key(s) in {args.env_file}: {', '.join(missing)}", file=sys.stderr)
        return 2
    client_id = env[CLIENT_ID_KEY]
    client_secret = env[CLIENT_SECRET_KEY]

    redirect = urllib.parse.urlsplit(args.redirect_uri)
    # '@' shifts host parsing to whatever follows it (userinfo@host), and a backslash is
    # treated as a literal netloc character by urlsplit but as a path/host separator by some
    # browsers; either lets a netloc like "evil.com\@localhost" parse as host "localhost" while
    # actually addressing something else. Reject both before trusting `.hostname` at all.
    if "@" in redirect.netloc or "\\" in redirect.netloc:
        print(
            f"Refusing --redirect-uri {args.redirect_uri!r}: host must not contain '@' or '\\'.",
            file=sys.stderr,
        )
        return 2
    if redirect.scheme != "http" or redirect.hostname not in _LOOPBACK_HOSTNAMES:
        print(
            f"Refusing --redirect-uri {args.redirect_uri!r}: scheme must be http and host "
            "must be localhost or 127.0.0.1.",
            file=sys.stderr,
        )
        return 2
    try:
        redirect_port = redirect.port
    except ValueError:
        print(
            f"Refusing --redirect-uri {args.redirect_uri!r}: invalid port.",
            file=sys.stderr,
        )
        return 2
    if redirect_port == 0:
        print(
            f"Refusing --redirect-uri {args.redirect_uri!r}: port must not be 0.",
            file=sys.stderr,
        )
        return 2
    redirect_port = redirect_port or 80
    redirect_path = redirect.path or "/"

    state = secrets.token_urlsafe(32)
    auth_url = build_authorization_url(client_id, args.redirect_uri, state)
    if args.print_url:
        print(auth_url)

    def _open_browser() -> None:
        try:
            opened = webbrowser.open(auth_url)
        except webbrowser.Error:
            opened = False
        if not opened:
            print(
                "Could not open a browser automatically; rerun with --print-url and open "
                "the URL yourself.",
                file=sys.stderr,
            )

    try:
        code = wait_for_authorization_code(
            "127.0.0.1",
            redirect_port,
            redirect_path,
            args.timeout_secs,
            state=state,
            on_listening=_open_browser,
        )
    except OSError:
        # Most likely the callback port is already bound by another process. The exception's
        # own text isn't printed - on some platforms it can carry more than the address - the
        # port number and a fixed message are enough to act on.
        print(f"callback port {redirect_port} is already in use", file=sys.stderr)
        return 2
    except AuthorizationError as e:
        print(f"Authorization was not granted: {e}", file=sys.stderr)
        return 1
    except TimeoutError as e:
        print(str(e), file=sys.stderr)
        return 1

    try:
        tokens = exchange_code(
            code,
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=args.redirect_uri,
            token_url=TOKEN_URL,
        )
    except TokenExchangeError as e:
        print(str(e), file=sys.stderr)
        return 1

    expires_at = int(time.time()) + tokens.expires_in
    update_env_file(
        args.env_file,
        {
            ACCESS_TOKEN_KEY: tokens.access_token,
            REFRESH_TOKEN_KEY: tokens.refresh_token,
            TOKEN_EXPIRES_AT_KEY: str(expires_at),
        },
    )
    expires_iso = datetime.fromtimestamp(expires_at, tz=UTC).isoformat()
    print(f"Tokens written to {args.env_file} (expires {expires_iso})")

    account_host = LIVE_HOST if args.live else DEMO_HOST
    try:
        result = asyncio.run(
            list_accounts(
                tokens.access_token,
                client_id,
                client_secret,
                host=account_host,
                port=PROTOBUF_PORT,
                logger=_PrintLogger(),
            ),
        )
    except CTraderRequestError as e:
        print(f"Could not list accounts: {e.error_code}", file=sys.stderr)
        print(
            "Tokens were saved even though the account list could not be retrieved.",
            file=sys.stderr,
        )
        return 1
    except (CTraderError, OSError) as e:
        print(f"Could not list accounts: {type(e).__name__}", file=sys.stderr)
        print(
            "Tokens were saved even though the account list could not be retrieved.",
            file=sys.stderr,
        )
        return 1

    _print_accounts(result, live=args.live)
    print(f"Add CTRADER_ACCOUNT_ID=<id> to {args.env_file} for the account you want to use.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
