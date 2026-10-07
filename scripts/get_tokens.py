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
import os
import re
import secrets
import ssl
import sys
import tempfile
import webbrowser
from datetime import UTC, datetime
from pathlib import Path

from nautilus_ctrader import oauth
from nautilus_ctrader.common.account import (
    AccountRecord,  # noqa: F401 - re-exported: `AccountsResult.accounts` holds these
    AccountsResult,
    list_granted_accounts,
)
from nautilus_ctrader.common.errors import (
    CTraderAuthorizationDenied,
    CTraderAuthorizationTimeout,
    CTraderError,
    CTraderRequestError,
    CTraderTokenExchangeError,
)
from nautilus_ctrader.constants import DEMO_HOST, LIVE_HOST, PROTOBUF_PORT
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model

# Kept here so the tests can point `main()` at a local token endpoint.
TOKEN_URL = oauth.TOKEN_URL

CLIENT_ID_KEY = "CTRADER_CLIENT_ID"
CLIENT_SECRET_KEY = "CTRADER_CLIENT_SECRET"
ACCESS_TOKEN_KEY = "CTRADER_ACCESS_TOKEN"
REFRESH_TOKEN_KEY = "CTRADER_REFRESH_TOKEN"
TOKEN_EXPIRES_AT_KEY = "CTRADER_TOKEN_EXPIRES_AT"


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
    """Authenticate the application on `host`, then list the accounts the access token grants.

    Only the account records and the permission scope are returned, never the token itself.
    """
    return await list_granted_accounts(
        client_id,
        client_secret,
        access_token,
        host=host,
        port=port,
        tls=tls,
        logger=logger,
    )


def _print_accounts(result: AccountsResult, *, live: bool) -> None:
    try:
        scope_name = oa_model.ProtoOAClientPermissionScope.Name(result.permission_scope)
    except ValueError:
        scope_name = str(result.permission_scope)
    print(f"Permission scope: {scope_name}")
    # The login leads each line because it is the one a client is configured with.
    print("Accounts granted (configure a client with the traderLogin):")

    for account in result.accounts:
        if account.is_live is None:
            is_live_text = "unknown"
        else:
            is_live_text = "live" if account.is_live else "demo"
        login_text = "not reported" if account.trader_login is None else str(account.trader_login)
        parts = [
            f"traderLogin={login_text}",
            f"ctidTraderAccountId={account.ctid_trader_account_id}",
            f"isLive={is_live_text}",
        ]
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


def parse_redirect_uri(redirect_uri: str) -> tuple[int, str]:
    """The callback port and path of `redirect_uri`.

    Raises `ValueError`, its message saying why, unless it is plain http on a loopback host.
    """
    try:
        return oauth.parse_redirect_uri(redirect_uri)
    except oauth.RedirectUriError as e:
        raise ValueError(f"Refusing --redirect-uri {redirect_uri!r}: {e.reason}") from None


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

    try:
        redirect_port, _ = parse_redirect_uri(args.redirect_uri)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2

    state = secrets.token_urlsafe(32)
    auth_url = oauth.build_authorization_url(client_id, args.redirect_uri, state)
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
        code = oauth.wait_for_authorization_code(
            args.redirect_uri,
            state,
            args.timeout_secs,
            on_listening=_open_browser,
        )
    except OSError:
        # Most likely the callback port is already bound by another process. The exception's
        # own text isn't printed - on some platforms it can carry more than the address - the
        # port number and a fixed message are enough to act on.
        print(f"callback port {redirect_port} is already in use", file=sys.stderr)
        return 2
    except CTraderAuthorizationDenied as e:
        print(f"Authorization was not granted: {e.error_code}", file=sys.stderr)
        return 1
    except CTraderAuthorizationTimeout as e:
        print(str(e), file=sys.stderr)
        return 1

    try:
        tokens = oauth.exchange_code(
            client_id,
            client_secret,
            code,
            args.redirect_uri,
            token_url=TOKEN_URL,
        )
    except CTraderTokenExchangeError as e:
        # A refusal's description is a field, outside the exception's fixed message.
        detail = f": {e.description}" if e.error_code is not None else ""
        print(f"{e}{detail}", file=sys.stderr)
        return 1

    expires_at = int(tokens.expires_at)
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
    print(
        f"Add CTRADER_TRADER_LOGIN=<traderLogin> to {args.env_file} for the account you want "
        "to use - the traderLogin above, not the ctidTraderAccountId.",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
