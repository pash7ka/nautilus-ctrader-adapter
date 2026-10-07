"""Tests for the one-time OAuth token script.

`scripts/get_tokens.py` is developer tooling, not part of the installed package, so it is
imported by file path rather than as `nautilus_ctrader.*`. Everything here runs offline: the
OAuth redirect and the token exchange are driven against local HTTP servers, and the account
listing step runs against `tests/fake_server.py`.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import http.server
import importlib.util
import json
import socket
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

from nautilus_ctrader.common.errors import CTraderRequestError
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.recording_logger import RecordingLogger

_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "get_tokens.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("get_tokens", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    # Dataclasses resolve string annotations via sys.modules[cls.__module__]; the module must
    # be registered there before exec_module runs the class bodies.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


get_tokens = _load_module()


def _free_port() -> int:
    """A port that is free right now. A local test's window for another process to grab it
    first is small enough to accept."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _get(url: str) -> None:
    with contextlib.suppress(urllib.error.URLError):
        urllib.request.urlopen(url, timeout=2)


def _state_from_authorization_url(url: str) -> str:
    """`main()` generates a fresh OAuth `state` per run; a faked browser must echo it back on
    the redirect, exactly as the real authorization server would."""
    return urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["state"][0]


# --------------------------------------------------------------------------------------
# load_env
# --------------------------------------------------------------------------------------


def test_load_env_parses_quotes_comments_and_blank_lines(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# a comment",
                "",
                'CTRADER_CLIENT_ID="abc123"',
                "CTRADER_CLIENT_SECRET='s3cr3t'",
                "  UNQUOTED = value  ",
            ],
        ),
        encoding="utf-8",
    )

    env = get_tokens.load_env(env_file)

    assert env["CTRADER_CLIENT_ID"] == "abc123"
    assert env["CTRADER_CLIENT_SECRET"] == "s3cr3t"
    assert env["UNQUOTED"] == "value"


def test_load_env_reads_a_bom(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_bytes("CTRADER_CLIENT_ID=abc123\n".encode("utf-8-sig"))

    env = get_tokens.load_env(env_file)

    assert env["CTRADER_CLIENT_ID"] == "abc123"


def test_load_env_omits_missing_keys(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("CTRADER_CLIENT_ID=abc123\n", encoding="utf-8")

    env = get_tokens.load_env(env_file)

    assert "CTRADER_CLIENT_SECRET" not in env


# --------------------------------------------------------------------------------------
# Token endpoint stub, for main()
# --------------------------------------------------------------------------------------


class _StubTokenHandler(http.server.BaseHTTPRequestHandler):
    response_status = 200
    response_body = b"{}"
    captured_path: str | None = None

    def do_GET(self) -> None:
        type(self).captured_path = self.path
        body = type(self).response_body
        self.send_response(type(self).response_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def stub_token_server():
    _StubTokenHandler.response_status = 200
    _StubTokenHandler.response_body = b"{}"
    _StubTokenHandler.captured_path = None
    server = http.server.HTTPServer(("127.0.0.1", 0), _StubTokenHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, f"http://127.0.0.1:{server.server_address[1]}/apps/token"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


# --------------------------------------------------------------------------------------
# update_env_file
# --------------------------------------------------------------------------------------


def test_update_env_file_replaces_existing_and_appends_missing(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# a comment\nCTRADER_CLIENT_ID=abc\nCTRADER_ACCESS_TOKEN=old\n",
        encoding="utf-8",
    )

    get_tokens.update_env_file(
        env_file,
        {"CTRADER_ACCESS_TOKEN": "new", "CTRADER_REFRESH_TOKEN": "rt"},
    )

    text = env_file.read_text(encoding="utf-8")
    lines = text.splitlines()
    assert lines[0] == "# a comment"
    assert lines[1] == "CTRADER_CLIENT_ID=abc"
    assert lines[2] == "CTRADER_ACCESS_TOKEN=new"
    assert "CTRADER_REFRESH_TOKEN=rt" in lines


def test_update_env_file_keeps_lf_line_endings(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_bytes(b"CTRADER_CLIENT_ID=abc\nCTRADER_CLIENT_SECRET=s\n")

    get_tokens.update_env_file(env_file, {"CTRADER_ACCESS_TOKEN": "new"})

    raw = env_file.read_bytes()
    assert b"\r\n" not in raw
    assert b"CTRADER_ACCESS_TOKEN=new\n" in raw


def test_update_env_file_keeps_crlf_line_endings(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_bytes(b"CTRADER_CLIENT_ID=abc\r\nCTRADER_CLIENT_SECRET=s\r\n")

    get_tokens.update_env_file(env_file, {"CTRADER_ACCESS_TOKEN": "new"})

    raw = env_file.read_bytes()
    assert b"CTRADER_ACCESS_TOKEN=new\r\n" in raw
    # Every line uses CRLF, not just the appended one.
    assert raw.count(b"\r\n") == raw.count(b"\n")


def test_update_env_file_writes_an_integer_expiry(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("CTRADER_CLIENT_ID=abc\n", encoding="utf-8")

    get_tokens.update_env_file(env_file, {"CTRADER_TOKEN_EXPIRES_AT": str(1234567890)})

    env = get_tokens.load_env(env_file)
    assert int(env["CTRADER_TOKEN_EXPIRES_AT"]) == 1234567890


def test_update_env_file_replaces_every_duplicate_line(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "CTRADER_ACCESS_TOKEN=old1\nCTRADER_CLIENT_ID=abc\nCTRADER_ACCESS_TOKEN=old2\n",
        encoding="utf-8",
    )

    get_tokens.update_env_file(env_file, {"CTRADER_ACCESS_TOKEN": "new"})

    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "CTRADER_ACCESS_TOKEN=new"
    assert lines[2] == "CTRADER_ACCESS_TOKEN=new"
    assert "old1" not in lines[0] + lines[2]
    assert "old2" not in lines[0] + lines[2]


def test_update_env_file_rejects_a_value_with_cr_or_lf(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("CTRADER_CLIENT_ID=abc\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"CR|LF|newline"):
        get_tokens.update_env_file(env_file, {"CTRADER_ACCESS_TOKEN": "bad\nvalue"})


def test_update_env_file_keeps_a_bom(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_bytes("CTRADER_CLIENT_ID=abc\n".encode("utf-8-sig"))

    get_tokens.update_env_file(env_file, {"CTRADER_ACCESS_TOKEN": "new"})

    raw = env_file.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")
    env = get_tokens.load_env(env_file)
    assert env["CTRADER_ACCESS_TOKEN"] == "new"


# --------------------------------------------------------------------------------------
# list_accounts
# --------------------------------------------------------------------------------------


async def test_list_accounts_returns_records_from_the_venue() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    server.on(
        oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        lambda _r: oa.ProtoOAGetAccountListByAccessTokenRes(
            accessToken="super-secret-token",
            permissionScope=oa_model.SCOPE_TRADE,
            ctidTraderAccount=[
                oa_model.ProtoOACtidTraderAccount(
                    ctidTraderAccountId=123,
                    isLive=False,
                    traderLogin=456,
                    brokerTitleShort="Acme",
                ),
            ],
        ),
    )
    await server.start()
    try:
        result = await get_tokens.list_accounts(
            "super-secret-token",
            "cid",
            "csecret",
            host=server.host,
            port=server.port,
            tls=False,
            logger=RecordingLogger(),
        )
    finally:
        await server.stop()

    assert result.permission_scope == oa_model.SCOPE_TRADE
    assert len(result.accounts) == 1
    account = result.accounts[0]
    assert account.ctid_trader_account_id == 123
    assert account.is_live is False
    assert account.trader_login == 456
    assert account.broker_title_short == "Acme"
    assert "super-secret-token" not in repr(result)


async def test_list_accounts_surfaces_a_rejected_application_auth() -> None:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="CH_CLIENT_AUTH_FAILURE"),
    )
    await server.start()
    try:
        with pytest.raises(CTraderRequestError) as exc_info:
            await get_tokens.list_accounts(
                "token",
                "cid",
                "csecret",
                host=server.host,
                port=server.port,
                tls=False,
                logger=RecordingLogger(),
            )
    finally:
        await server.stop()

    assert exc_info.value.error_code == "CH_CLIENT_AUTH_FAILURE"


def _write_minimal_env(tmp_path: Path) -> Path:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "CTRADER_CLIENT_ID=cid\nCTRADER_CLIENT_SECRET=csecret\n",
        encoding="utf-8",
    )
    return env_file


def test_main_refuses_a_redirect_uri_with_the_wrong_scheme_alone(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = _write_minimal_env(tmp_path)

    rc = get_tokens.main(
        ["--env-file", str(env_file), "--redirect-uri", "https://localhost:8080/callback"],
    )

    assert rc == 2
    message = capsys.readouterr().err
    assert "scheme must be http" in message


def test_main_refuses_a_redirect_uri_with_the_wrong_host_alone(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = _write_minimal_env(tmp_path)

    rc = get_tokens.main(
        ["--env-file", str(env_file), "--redirect-uri", "http://example.com:8080/callback"],
    )

    assert rc == 2
    message = capsys.readouterr().err
    assert "localhost" in message


def test_main_refuses_an_ipv6_loopback_redirect_uri(tmp_path: Path) -> None:
    """The callback server only ever binds IPv4 `127.0.0.1`; `::1` would parse as a loopback
    host but never actually receive the redirect. A short deadline bounds the test in case the
    check is broken and main() falls through to actually waiting for a redirect."""
    env_file = _write_minimal_env(tmp_path)

    rc = get_tokens.main(
        [
            "--env-file",
            str(env_file),
            "--redirect-uri",
            "http://[::1]:8080/callback",
            "--timeout-secs",
            "2",
        ],
    )

    assert rc == 2


def test_main_refuses_a_redirect_uri_with_an_embedded_credential(tmp_path: Path) -> None:
    """`urlsplit(...).hostname` reads `evil.com\\@localhost` as host `localhost`, so the
    loopback check alone would accept it; the raw netloc must be checked for '@' and '\\'
    first. A short deadline bounds the test in case the check is bypassed."""
    env_file = _write_minimal_env(tmp_path)

    rc = get_tokens.main(
        [
            "--env-file",
            str(env_file),
            "--redirect-uri",
            "http://evil.com\\@localhost/cb",
            "--timeout-secs",
            "2",
        ],
    )

    assert rc == 2


@pytest.mark.parametrize(
    "redirect_uri",
    [
        "http://localhost:99999/callback",
        "http://localhost:abc/callback",
        "http://localhost:0/callback",
    ],
)
def test_main_refuses_a_bad_redirect_port(tmp_path: Path, redirect_uri: str) -> None:
    env_file = _write_minimal_env(tmp_path)

    # A short deadline bounds this test in case the port check is broken and main() falls
    # through to actually binding a socket and waiting for a redirect (e.g. port 0 silently
    # becoming port 80).
    rc = get_tokens.main(
        ["--env-file", str(env_file), "--redirect-uri", redirect_uri, "--timeout-secs", "2"],
    )

    assert rc == 2


def test_main_reports_a_busy_callback_port_without_a_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A callback port already in use must exit 2 with a clear message, like every other
    redirect-URI problem, instead of an unhandled `OSError` traceback."""
    env_file = _write_minimal_env(tmp_path)
    port = _free_port()

    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", port))
    blocker.listen(1)
    try:
        rc = get_tokens.main(
            [
                "--env-file",
                str(env_file),
                "--redirect-uri",
                f"http://127.0.0.1:{port}/callback",
                "--timeout-secs",
                "2",
            ],
        )
    finally:
        blocker.close()

    assert rc == 2
    message = capsys.readouterr().err
    assert str(port) in message
    assert "already in use" in message
    assert "Traceback" not in message


def test_main_exits_2_on_missing_keys(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("CTRADER_CLIENT_ID=cid\n", encoding="utf-8")

    rc = get_tokens.main(["--env-file", str(env_file)])

    assert rc == 2


async def test_main_reports_an_authorization_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "CTRADER_CLIENT_ID=cid\nCTRADER_CLIENT_SECRET=csecret\n",
        encoding="utf-8",
    )

    redirect_port = _free_port()
    redirect_uri = f"http://127.0.0.1:{redirect_port}/callback"

    def fake_open(url: str) -> bool:
        state = _state_from_authorization_url(url)
        threading.Thread(
            target=_get,
            args=(f"{redirect_uri}?error=access_denied&state={state}",),
        ).start()
        return True

    monkeypatch.setattr(get_tokens.webbrowser, "open", fake_open)

    rc = await asyncio.to_thread(
        get_tokens.main,
        ["--env-file", str(env_file), "--redirect-uri", redirect_uri, "--timeout-secs", "5"],
    )

    assert rc == 1
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "access_denied" in output
    assert "Traceback" not in output


def test_print_accounts_warns_on_a_live_demo_mismatch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = get_tokens.AccountsResult(
        permission_scope=oa_model.SCOPE_TRADE,
        accounts=[
            get_tokens.AccountRecord(
                ctid_trader_account_id=1,
                is_live=True,
                trader_login=None,
                broker_title_short=None,
            ),
        ],
    )

    get_tokens._print_accounts(result, live=False)

    captured = capsys.readouterr()
    assert "warning" in captured.err.lower()
    assert "1" in captured.err


# --------------------------------------------------------------------------------------
# main() end to end
# --------------------------------------------------------------------------------------


async def test_main_reports_a_connection_refused_while_listing_accounts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    stub_token_server,
) -> None:
    """`list_accounts` can fail with more than `CTraderRequestError` - a closed port raises
    `CTraderConnectionError`, which must be caught too, not left to print a traceback."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "CTRADER_CLIENT_ID=cid\nCTRADER_CLIENT_SECRET=csecret\n",
        encoding="utf-8",
    )

    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {
            "accessToken": "the-access-token",
            "refreshToken": "the-refresh-token",
            "expiresIn": 2419200,
            "tokenType": "bearer",
        },
    ).encode()
    monkeypatch.setattr(get_tokens, "TOKEN_URL", token_url)

    # Nothing listens here, so the connection is refused.
    closed_port = _free_port()
    monkeypatch.setattr(get_tokens, "DEMO_HOST", "127.0.0.1")
    monkeypatch.setattr(get_tokens, "PROTOBUF_PORT", closed_port)

    redirect_port = _free_port()
    redirect_uri = f"http://127.0.0.1:{redirect_port}/callback"

    def fake_open(url: str) -> bool:
        state = _state_from_authorization_url(url)
        threading.Thread(
            target=_get,
            args=(f"{redirect_uri}?code=the-auth-code&state={state}",),
        ).start()
        return True

    monkeypatch.setattr(get_tokens.webbrowser, "open", fake_open)

    rc = await asyncio.to_thread(
        get_tokens.main,
        ["--env-file", str(env_file), "--redirect-uri", redirect_uri, "--timeout-secs", "5"],
    )

    assert rc == 1
    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "saved" in output.lower()
    assert "Traceback" not in output
    assert "csecret" not in output
    assert "the-access-token" not in output
    assert "the-refresh-token" not in output


async def test_main_writes_tokens_and_never_prints_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "CTRADER_CLIENT_ID=the-distinctive-client-id\nCTRADER_CLIENT_SECRET=csecret\n",
        encoding="utf-8",
    )

    # Token exchange stub.
    _StubTokenHandler.response_status = 200
    _StubTokenHandler.response_body = json.dumps(
        {
            "accessToken": "the-access-token",
            "refreshToken": "the-refresh-token",
            "expiresIn": 2419200,
            "tokenType": "bearer",
        },
    ).encode()
    token_server = http.server.HTTPServer(("127.0.0.1", 0), _StubTokenHandler)
    token_thread = threading.Thread(target=token_server.serve_forever, daemon=True)
    token_thread.start()
    monkeypatch.setattr(
        get_tokens,
        "TOKEN_URL",
        f"http://127.0.0.1:{token_server.server_address[1]}/apps/token",
    )

    # Account-listing fake server, wired in place of the real demo host.
    fake_server = FakeCTraderServer()
    fake_server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    fake_server.on(
        oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        lambda _r: oa.ProtoOAGetAccountListByAccessTokenRes(
            accessToken="the-access-token",
            permissionScope=oa_model.SCOPE_TRADE,
            ctidTraderAccount=[
                oa_model.ProtoOACtidTraderAccount(ctidTraderAccountId=1, isLive=False),
            ],
        ),
    )
    await fake_server.start()
    monkeypatch.setattr(get_tokens, "DEMO_HOST", fake_server.host)
    monkeypatch.setattr(get_tokens, "PROTOBUF_PORT", fake_server.port)
    # main() calls list_accounts() without a tls argument, so its True default applies; the
    # fake server speaks plaintext, so the plaintext switch is bound here instead, kept out
    # of the script itself.
    monkeypatch.setattr(
        get_tokens,
        "list_accounts",
        functools.partial(get_tokens.list_accounts, tls=False),
    )

    # The redirect: webbrowser.open is patched to perform the request itself, as if a real
    # browser had followed it and cTrader had redirected back with a code.
    redirect_port = _free_port()
    redirect_uri = f"http://127.0.0.1:{redirect_port}/callback"
    captured_state: dict[str, str] = {}

    def fake_open(url: str) -> bool:
        state = _state_from_authorization_url(url)
        captured_state["state"] = state
        threading.Thread(
            target=_get,
            args=(f"{redirect_uri}?code=the-auth-code&state={state}",),
        ).start()
        return True

    monkeypatch.setattr(get_tokens.webbrowser, "open", fake_open)

    try:
        # main() calls asyncio.run() internally for the account-listing step; run it on a
        # separate thread so that call does not collide with this test's own running loop.
        rc = await asyncio.to_thread(
            get_tokens.main,
            [
                "--env-file",
                str(env_file),
                "--redirect-uri",
                redirect_uri,
                "--timeout-secs",
                "5",
            ],
        )
    finally:
        token_server.shutdown()
        token_thread.join(timeout=2)
        token_server.server_close()
        await fake_server.stop()

    assert rc == 0

    env = get_tokens.load_env(env_file)
    assert env["CTRADER_ACCESS_TOKEN"] == "the-access-token"
    assert env["CTRADER_REFRESH_TOKEN"] == "the-refresh-token"
    assert int(env["CTRADER_TOKEN_EXPIRES_AT"]) > 0
    assert env["CTRADER_CLIENT_ID"] == "the-distinctive-client-id"

    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "the-access-token" not in output
    assert "the-refresh-token" not in output
    assert "csecret" not in output
    assert "the-auth-code" not in output
    assert "the-distinctive-client-id" not in output
    assert captured_state["state"] not in output


async def test_main_reports_a_refused_code_with_its_error_code_and_description(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    stub_token_server,
) -> None:
    env_file = _write_minimal_env(tmp_path)
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"errorCode": "INVALID_GRANT", "description": "code expired"},
    ).encode()
    monkeypatch.setattr(get_tokens, "TOKEN_URL", token_url)

    redirect_port = _free_port()
    redirect_uri = f"http://127.0.0.1:{redirect_port}/callback"

    def fake_open(url: str) -> bool:
        state = _state_from_authorization_url(url)
        threading.Thread(
            target=_get,
            args=(f"{redirect_uri}?code=the-auth-code&state={state}",),
        ).start()
        return True

    monkeypatch.setattr(get_tokens.webbrowser, "open", fake_open)

    rc = await asyncio.to_thread(
        get_tokens.main,
        ["--env-file", str(env_file), "--redirect-uri", redirect_uri, "--timeout-secs", "5"],
    )

    assert rc == 1
    captured = capsys.readouterr()
    assert "token endpoint rejected the code: INVALID_GRANT: code expired" in captured.err
    assert "the-auth-code" not in captured.out + captured.err
    assert "csecret" not in captured.out + captured.err
    assert "CTRADER_ACCESS_TOKEN" not in env_file.read_text(encoding="utf-8")


def test_main_reports_a_redirect_that_never_comes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    env_file = _write_minimal_env(tmp_path)
    monkeypatch.setattr(get_tokens.webbrowser, "open", lambda _url: True)

    rc = get_tokens.main(
        [
            "--env-file",
            str(env_file),
            "--redirect-uri",
            f"http://127.0.0.1:{_free_port()}/callback",
            "--timeout-secs",
            "0.3",
        ],
    )

    assert rc == 1
    assert "no authorization redirect received within 0.3s" in capsys.readouterr().err
