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
import time
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
# build_authorization_url
# --------------------------------------------------------------------------------------


def test_build_authorization_url_encodes_parameters() -> None:
    url = get_tokens.build_authorization_url(
        "my client",
        "http://localhost:8080/callback",
        "the-state",
    )

    parsed = urllib.parse.urlsplit(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "openapi.ctrader.com"
    assert parsed.path == "/apps/auth"
    params = urllib.parse.parse_qs(parsed.query)
    assert params == {
        "client_id": ["my client"],
        "redirect_uri": ["http://localhost:8080/callback"],
        "scope": ["trading"],
        "state": ["the-state"],
    }


# --------------------------------------------------------------------------------------
# wait_for_authorization_code
# --------------------------------------------------------------------------------------


def test_wait_for_authorization_code_returns_the_code() -> None:
    port = _free_port()
    started = {}

    def on_listening() -> None:
        started["thread"] = threading.Thread(
            target=_get,
            args=(f"http://127.0.0.1:{port}/callback?code=the-code&state=s",),
        )
        started["thread"].start()

    code = get_tokens.wait_for_authorization_code(
        "127.0.0.1",
        port,
        "/callback",
        5.0,
        state="s",
        on_listening=on_listening,
    )

    assert code == "the-code"
    started["thread"].join(timeout=2)


def test_wait_for_authorization_code_raises_on_error() -> None:
    port = _free_port()
    started = {}

    def on_listening() -> None:
        started["thread"] = threading.Thread(
            target=_get,
            args=(f"http://127.0.0.1:{port}/callback?error=access_denied&state=s",),
        )
        started["thread"].start()

    with pytest.raises(get_tokens.AuthorizationError) as exc_info:
        get_tokens.wait_for_authorization_code(
            "127.0.0.1",
            port,
            "/callback",
            5.0,
            state="s",
            on_listening=on_listening,
        )

    assert "access_denied" in str(exc_info.value)
    started["thread"].join(timeout=2)


def test_wait_for_authorization_code_ignores_other_paths() -> None:
    port = _free_port()
    started = {}

    def on_listening() -> None:
        def _drive() -> None:
            _get(f"http://127.0.0.1:{port}/favicon.ico")
            _get(f"http://127.0.0.1:{port}/callback?code=the-code&state=s")

        started["thread"] = threading.Thread(target=_drive)
        started["thread"].start()

    code = get_tokens.wait_for_authorization_code(
        "127.0.0.1",
        port,
        "/callback",
        5.0,
        state="s",
        on_listening=on_listening,
    )

    assert code == "the-code"
    started["thread"].join(timeout=2)


def test_wait_for_authorization_code_times_out() -> None:
    port = _free_port()

    with pytest.raises(TimeoutError):
        get_tokens.wait_for_authorization_code("127.0.0.1", port, "/callback", 0.2, state="s")


def test_wait_for_authorization_code_rejects_wrong_state_then_accepts_the_right_one() -> None:
    """A request with a missing or wrong `state` must not end the wait, so a stray page can't
    inject a code or abort the run."""
    port = _free_port()
    started = {}

    def on_listening() -> None:
        def _drive() -> None:
            _get(f"http://127.0.0.1:{port}/callback?code=wrong-state-code&state=nope")
            _get(f"http://127.0.0.1:{port}/callback?code=the-code")  # missing state
            _get(f"http://127.0.0.1:{port}/callback?code=the-code&state=s")

        started["thread"] = threading.Thread(target=_drive)
        started["thread"].start()

    code = get_tokens.wait_for_authorization_code(
        "127.0.0.1",
        port,
        "/callback",
        5.0,
        state="s",
        on_listening=on_listening,
    )

    assert code == "the-code"
    started["thread"].join(timeout=2)


def test_wait_for_authorization_code_ignores_error_with_wrong_state() -> None:
    """A `?error=...` with the wrong state must not abort the run either."""
    port = _free_port()
    started = {}

    def on_listening() -> None:
        def _drive() -> None:
            _get(f"http://127.0.0.1:{port}/callback?error=access_denied&state=nope")
            _get(f"http://127.0.0.1:{port}/callback?code=the-code&state=s")

        started["thread"] = threading.Thread(target=_drive)
        started["thread"].start()

    code = get_tokens.wait_for_authorization_code(
        "127.0.0.1",
        port,
        "/callback",
        5.0,
        state="s",
        on_listening=on_listening,
    )

    assert code == "the-code"
    started["thread"].join(timeout=2)


def test_wait_for_authorization_code_escapes_the_error_in_the_page() -> None:
    port = _free_port()
    page: dict[str, str] = {}

    def _fetch() -> None:
        query = urllib.parse.urlencode({"error": "<script>bad()</script>", "state": "s"})
        with (
            contextlib.suppress(urllib.error.URLError),
            urllib.request.urlopen(f"http://127.0.0.1:{port}/callback?{query}", timeout=5) as r,
        ):
            page["body"] = r.read().decode()

    started = {}

    def on_listening() -> None:
        started["thread"] = threading.Thread(target=_fetch)
        started["thread"].start()

    with pytest.raises(get_tokens.AuthorizationError):
        get_tokens.wait_for_authorization_code(
            "127.0.0.1",
            port,
            "/callback",
            5.0,
            state="s",
            on_listening=on_listening,
        )
    started["thread"].join(timeout=5)

    assert "<script>" not in page["body"]
    assert "&lt;script&gt;" in page["body"]


def test_wait_for_authorization_code_sanitizes_the_error_for_the_terminal() -> None:
    port = _free_port()
    nasty = "bad\x07\x1b[31m" + ("x" * 500)

    def on_listening() -> None:
        query = urllib.parse.urlencode({"error": nasty, "state": "s"})
        threading.Thread(
            target=_get,
            args=(f"http://127.0.0.1:{port}/callback?{query}",),
        ).start()

    with pytest.raises(get_tokens.AuthorizationError) as exc_info:
        get_tokens.wait_for_authorization_code(
            "127.0.0.1",
            port,
            "/callback",
            5.0,
            state="s",
            on_listening=on_listening,
        )

    message = str(exc_info.value)
    assert all(ch.isprintable() for ch in message)
    assert len(message) <= 200


def test_wait_for_authorization_code_survives_an_idle_connection() -> None:
    """A connection that opens and sends nothing (a browser's speculative preconnect) must
    not block the real request that follows it."""
    port = _free_port()
    idle_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result: dict[str, str] = {}

    def send_real_request() -> None:
        # The server may be busy dropping the idle connection for a few seconds before it
        # accepts this one, so give the response more room than `_get`'s default 2s.
        with contextlib.suppress(urllib.error.URLError, TimeoutError):
            urllib.request.urlopen(
                f"http://127.0.0.1:{port}/callback?code=the-code&state=s",
                timeout=10,
            )

    def on_listening() -> None:
        idle_sock.connect(("127.0.0.1", port))
        threading.Thread(target=send_real_request).start()

    def run() -> None:
        result["code"] = get_tokens.wait_for_authorization_code(
            "127.0.0.1",
            port,
            "/callback",
            10.0,
            state="s",
            on_listening=on_listening,
        )

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=8.0)
    idle_sock.close()

    assert not thread.is_alive(), "the idle connection blocked the real request"
    assert result.get("code") == "the-code"


def test_wait_for_authorization_code_returns_promptly_with_an_idle_connection() -> None:
    """Connections are served concurrently, so an idle preconnect must not delay the real
    redirect at all - the code must come back in well under the deadline, not just before it
    times out."""
    port = _free_port()
    idle_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def on_listening() -> None:
        idle_sock.connect(("127.0.0.1", port))
        threading.Thread(
            target=_get,
            args=(f"http://127.0.0.1:{port}/callback?code=the-code&state=s",),
        ).start()

    start = time.monotonic()
    try:
        code = get_tokens.wait_for_authorization_code(
            "127.0.0.1",
            port,
            "/callback",
            2.0,
            state="s",
            on_listening=on_listening,
        )
    finally:
        idle_sock.close()
    elapsed = time.monotonic() - start

    assert code == "the-code"
    assert elapsed < 1.0, f"took {elapsed:.2f}s, expected well under the 2s deadline"


def test_wait_for_authorization_code_returns_promptly_despite_a_slow_drip_client() -> None:
    """A client sending one byte every 0.5s occupies its own connection indefinitely; it must
    not block the real redirect that arrives concurrently."""
    port = _free_port()
    drip_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stop_drip = threading.Event()

    def drip() -> None:
        try:
            drip_sock.connect(("127.0.0.1", port))
            while not stop_drip.is_set():
                with contextlib.suppress(OSError):
                    drip_sock.send(b"x")
                stop_drip.wait(0.5)
        except OSError:
            pass

    def on_listening() -> None:
        threading.Thread(target=drip, daemon=True).start()
        threading.Thread(
            target=_get,
            args=(f"http://127.0.0.1:{port}/callback?code=the-code&state=s",),
        ).start()

    start = time.monotonic()
    try:
        code = get_tokens.wait_for_authorization_code(
            "127.0.0.1",
            port,
            "/callback",
            3.0,
            state="s",
            on_listening=on_listening,
        )
    finally:
        stop_drip.set()
        with contextlib.suppress(OSError):
            drip_sock.close()
    elapsed = time.monotonic() - start

    assert code == "the-code"
    assert elapsed < 1.5, f"took {elapsed:.2f}s, expected the drip to never block the real request"


def test_wait_for_authorization_code_times_out_close_to_the_deadline() -> None:
    """With only an idle connection present, the timeout must fire close to `timeout_secs`,
    not be stretched out by the idle connection's own per-connection read timeout."""
    port = _free_port()
    idle_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def on_listening() -> None:
        idle_sock.connect(("127.0.0.1", port))

    start = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            get_tokens.wait_for_authorization_code(
                "127.0.0.1",
                port,
                "/callback",
                1.0,
                state="s",
                on_listening=on_listening,
            )
    finally:
        idle_sock.close()
    elapsed = time.monotonic() - start

    assert abs(elapsed - 1.0) < 0.5, f"took {elapsed:.2f}s, expected close to the 1.0s deadline"


# --------------------------------------------------------------------------------------
# exchange_code
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


def test_exchange_code_sends_the_documented_query_parameters(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": 2419200, "tokenType": "bearer"},
    ).encode()

    get_tokens.exchange_code(
        "the-code",
        client_id="cid",
        client_secret="csecret",
        redirect_uri="http://localhost:8080/callback",
        token_url=token_url,
    )

    parsed = urllib.parse.urlsplit(_StubTokenHandler.captured_path)
    assert parsed.path == "/apps/token"
    params = urllib.parse.parse_qs(parsed.query)
    assert params == {
        "grant_type": ["authorization_code"],
        "code": ["the-code"],
        "redirect_uri": ["http://localhost:8080/callback"],
        "client_id": ["cid"],
        "client_secret": ["csecret"],
    }


def test_exchange_code_returns_parsed_tokens(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": 2419200, "tokenType": "bearer"},
    ).encode()

    tokens = get_tokens.exchange_code(
        "the-code",
        client_id="cid",
        client_secret="csecret",
        redirect_uri="http://localhost:8080/callback",
        token_url=token_url,
    )

    assert tokens.access_token == "AT"
    assert tokens.refresh_token == "RT"
    assert tokens.expires_in == 2419200
    assert tokens.token_type == "bearer"


def test_exchange_code_raises_on_error_code(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {
            "errorCode": "INVALID_GRANT",
            "description": "code expired",
            "accessToken": "AT-should-not-appear",
            "refreshToken": "RT-should-not-appear",
        },
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError) as exc_info:
        get_tokens.exchange_code(
            "the-code-should-not-appear",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )

    message = str(exc_info.value)
    assert "INVALID_GRANT" in message
    assert "code expired" in message
    assert "csecret" not in message
    assert "AT-should-not-appear" not in message
    assert "RT-should-not-appear" not in message
    assert "the-code-should-not-appear" not in message


def test_exchange_code_raises_on_http_error(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_status = 400
    _StubTokenHandler.response_body = json.dumps({"errorCode": "SHOULD_NOT_APPEAR"}).encode()

    with pytest.raises(get_tokens.TokenExchangeError) as exc_info:
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )

    message = str(exc_info.value)
    assert "400" in message
    assert "SHOULD_NOT_APPEAR" not in message
    assert "csecret" not in message


def test_exchange_code_chains_from_none_on_http_error(stub_token_server) -> None:
    """`HTTPError` carries the full request URL, secret included, in `.url`; chaining from it
    would put that URL in any traceback."""
    _server, token_url = stub_token_server
    _StubTokenHandler.response_status = 500
    _StubTokenHandler.response_body = b"{}"

    with pytest.raises(get_tokens.TokenExchangeError) as exc_info:
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_exchange_code_raises_on_connection_failure(tmp_path: Path) -> None:
    """A `URLError` (for example a closed port) must be wrapped the same way, without
    chaining from the original exception - it too can carry the request URL."""
    closed_port = _free_port()

    with pytest.raises(get_tokens.TokenExchangeError) as exc_info:
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=f"http://127.0.0.1:{closed_port}/apps/token",
        )

    assert "csecret" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_exchange_code_raises_on_a_non_json_body(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = b"not json"

    with pytest.raises(get_tokens.TokenExchangeError):
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )


def test_exchange_code_raises_on_a_json_body_that_is_not_an_object(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(["not", "an", "object"]).encode()

    with pytest.raises(get_tokens.TokenExchangeError):
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )


def test_exchange_code_raises_on_missing_expires_in(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "RT"},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError) as exc_info:
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )

    assert "AT" not in str(exc_info.value)


def test_exchange_code_raises_on_non_positive_expires_in(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": 0},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError):
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )


def test_exchange_code_raises_on_a_nan_expires_in(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": float("nan")},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError) as exc_info:
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )

    assert "AT" not in str(exc_info.value)


def test_exchange_code_raises_on_an_infinite_expires_in(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": float("inf")},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError):
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )


def test_exchange_code_raises_on_a_boolean_expires_in(stub_token_server) -> None:
    """`bool` is a subclass of `int` in Python; `True`/`False` must not be accepted as a
    seconds count."""
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": True},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError):
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )


def test_exchange_code_raises_on_a_sub_one_expires_in(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": 0.5},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError):
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )


def test_exchange_code_raises_on_a_null_access_token(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": None, "refreshToken": "RT", "expiresIn": 3600},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError):
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )


def test_exchange_code_raises_on_an_empty_refresh_token(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT", "refreshToken": "", "expiresIn": 3600},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError):
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )


def test_exchange_code_raises_on_an_access_token_containing_a_newline(stub_token_server) -> None:
    _server, token_url = stub_token_server
    _StubTokenHandler.response_body = json.dumps(
        {"accessToken": "AT\nInjected", "refreshToken": "RT", "expiresIn": 3600},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError) as exc_info:
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )

    assert "Injected" not in str(exc_info.value)


def test_exchange_code_sanitizes_error_code_and_description(stub_token_server) -> None:
    _server, token_url = stub_token_server
    nasty = "bad\x07\x1b[31mtext"
    _StubTokenHandler.response_body = json.dumps(
        {"errorCode": nasty, "description": nasty},
    ).encode()

    with pytest.raises(get_tokens.TokenExchangeError) as exc_info:
        get_tokens.exchange_code(
            "the-code",
            client_id="cid",
            client_secret="csecret",
            redirect_uri="http://localhost:8080/callback",
            token_url=token_url,
        )

    message = str(exc_info.value)
    assert all(ch.isprintable() for ch in message)


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
    assert "http" in message


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
