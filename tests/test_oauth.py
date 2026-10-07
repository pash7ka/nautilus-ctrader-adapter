"""The OAuth flow: authorization URL, the localhost callback, and the code exchange.

Everything runs offline: the redirect and the token endpoint are local HTTP servers.
"""

from __future__ import annotations

import contextlib
import http.server
import json
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable

import pytest

from nautilus_ctrader import oauth
from nautilus_ctrader.common.errors import (
    CTraderAuthError,
    CTraderAuthorizationDenied,
    CTraderAuthorizationTimeout,
    CTraderTokenExchangeError,
)


def _free_port() -> int:
    """A port that is free right now. A local test's window for another process to grab it
    first is small enough to accept."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _get(url: str) -> None:
    with contextlib.suppress(urllib.error.URLError):
        urllib.request.urlopen(url, timeout=2)


def _wait(
    port: int,
    timeout_secs: float,
    on_listening: Callable[[], None] | None = None,
    *,
    state: str = "s",
) -> str:
    return oauth.wait_for_authorization_code(
        f"http://127.0.0.1:{port}/callback",
        state,
        timeout_secs,
        on_listening=on_listening,
    )


def _texts(error: BaseException) -> str:
    """Everything an exception shows: its message, its repr, and its arguments."""
    return " ".join([str(error), repr(error), *map(repr, error.args)])


# --------------------------------------------------------------------------------------
# TokenPair
# --------------------------------------------------------------------------------------


def test_token_pair_repr_masks_both_tokens() -> None:
    pair = oauth.TokenPair("the-access-token", "the-refresh-token", 1_700_000_000.0)

    text = repr(pair) + str(pair)

    assert "the-access-token" not in text
    assert "the-refresh-token" not in text
    assert "1700000000" in text


# --------------------------------------------------------------------------------------
# build_authorization_url
# --------------------------------------------------------------------------------------


def test_build_authorization_url_encodes_parameters() -> None:
    url = oauth.build_authorization_url(
        "my client",
        "http://localhost:8080/callback",
        "the-state",
    )

    parsed = urllib.parse.urlsplit(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "id.ctrader.com"
    assert parsed.path == "/my/settings/openapi/grantingaccess/"
    params = urllib.parse.parse_qs(parsed.query)
    assert params == {
        "client_id": ["my client"],
        "redirect_uri": ["http://localhost:8080/callback"],
        "scope": ["trading"],
        "product": ["web"],
        "state": ["the-state"],
    }


# --------------------------------------------------------------------------------------
# parse_redirect_uri
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("redirect_uri", "expected"),
    [
        ("http://localhost:8080/callback", (8080, "/callback")),
        ("http://127.0.0.1/cb", (80, "/cb")),
        ("http://localhost:8080", (8080, "/")),
    ],
)
def test_parse_redirect_uri_returns_port_and_path(
    redirect_uri: str,
    expected: tuple[int, str],
) -> None:
    assert oauth.parse_redirect_uri(redirect_uri) == expected


@pytest.mark.parametrize(
    ("redirect_uri", "reason"),
    [
        ("https://localhost:8080/callback", "scheme must be http"),
        ("http://example.com:8080/callback", "localhost"),
        ("http://[::1]:8080/callback", "localhost"),
        ("http://evil.com\\@localhost/cb", "'@'"),
        ("http://localhost:99999/callback", "invalid port"),
        ("http://localhost:abc/callback", "invalid port"),
        ("http://localhost:0/callback", "must not be 0"),
    ],
)
def test_parse_redirect_uri_refuses_anything_but_plain_http_on_loopback(
    redirect_uri: str,
    reason: str,
) -> None:
    with pytest.raises(oauth.RedirectUriError) as info:
        oauth.parse_redirect_uri(redirect_uri)

    assert isinstance(info.value, ValueError)
    assert reason in info.value.reason


def test_wait_for_authorization_code_refuses_a_non_loopback_redirect_before_binding() -> None:
    with pytest.raises(oauth.RedirectUriError):
        oauth.wait_for_authorization_code("http://example.com:8080/callback", "s", 0.2)


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

    code = _wait(port, 5.0, on_listening)

    assert code == "the-code"
    started["thread"].join(timeout=2)


def test_wait_for_authorization_code_prints_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    port = _free_port()

    def on_listening() -> None:
        threading.Thread(
            target=_get,
            args=(f"http://127.0.0.1:{port}/callback?code=the-code&state=s",),
        ).start()

    _wait(port, 5.0, on_listening)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_wait_for_authorization_code_raises_denied_on_an_error_redirect() -> None:
    port = _free_port()
    started = {}

    def on_listening() -> None:
        started["thread"] = threading.Thread(
            target=_get,
            args=(f"http://127.0.0.1:{port}/callback?error=access_denied&state=s",),
        )
        started["thread"].start()

    with pytest.raises(CTraderAuthorizationDenied) as exc_info:
        _wait(port, 5.0, on_listening)

    assert isinstance(exc_info.value, CTraderAuthError)
    assert exc_info.value.error_code == "access_denied"
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

    code = _wait(port, 5.0, on_listening)

    assert code == "the-code"
    started["thread"].join(timeout=2)


def test_wait_for_authorization_code_answers_other_paths_with_404() -> None:
    port = _free_port()
    result: dict[str, int] = {}

    def on_listening() -> None:
        def _drive() -> None:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/favicon.ico", timeout=2)
            except urllib.error.HTTPError as e:
                result["status"] = e.code
            _get(f"http://127.0.0.1:{port}/callback?code=the-code&state=s")

        threading.Thread(target=_drive).start()

    _wait(port, 5.0, on_listening)

    assert result["status"] == 404


def test_wait_for_authorization_code_times_out() -> None:
    port = _free_port()

    with pytest.raises(CTraderAuthorizationTimeout) as exc_info:
        _wait(port, 0.2)

    assert isinstance(exc_info.value, CTraderAuthError)
    assert "0.2s" in str(exc_info.value)


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

    code = _wait(port, 5.0, on_listening)

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

    code = _wait(port, 5.0, on_listening)

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

    with pytest.raises(CTraderAuthorizationDenied):
        _wait(port, 5.0, on_listening)
    started["thread"].join(timeout=5)

    assert "<script>" not in page["body"]
    assert "&lt;script&gt;" in page["body"]


def test_wait_for_authorization_code_sanitizes_the_error_code() -> None:
    port = _free_port()
    nasty = "bad\x07\x1b[31m" + ("x" * 500)

    def on_listening() -> None:
        query = urllib.parse.urlencode({"error": nasty, "state": "s"})
        threading.Thread(
            target=_get,
            args=(f"http://127.0.0.1:{port}/callback?{query}",),
        ).start()

    with pytest.raises(CTraderAuthorizationDenied) as exc_info:
        _wait(port, 5.0, on_listening)

    error_code = exc_info.value.error_code
    assert all(ch.isprintable() for ch in error_code)
    assert len(error_code) <= 200
    assert all(ch.isprintable() for ch in _texts(exc_info.value))


def test_the_denied_and_timeout_errors_carry_no_state_or_code() -> None:
    """Their messages are fixed: neither the `state` nor any part of the redirect's query."""
    port = _free_port()

    def on_listening() -> None:
        query = urllib.parse.urlencode({"error": "access_denied", "state": "the-secret-state"})
        threading.Thread(target=_get, args=(f"http://127.0.0.1:{port}/callback?{query}",)).start()

    with pytest.raises(CTraderAuthorizationDenied) as denied:
        _wait(port, 5.0, on_listening, state="the-secret-state")
    with pytest.raises(CTraderAuthorizationTimeout) as timed_out:
        _wait(_free_port(), 0.1, state="the-secret-state")

    for error in (denied.value, timed_out.value):
        assert "the-secret-state" not in _texts(error)
    assert "access_denied" not in _texts(denied.value)


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
        result["code"] = _wait(port, 10.0, on_listening)

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
        code = _wait(port, 2.0, on_listening)
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
        code = _wait(port, 3.0, on_listening)
    finally:
        stop_drip.set()
        with contextlib.suppress(OSError):
            drip_sock.close()
    elapsed = time.monotonic() - start

    assert code == "the-code"
    assert elapsed < 1.5, f"took {elapsed:.2f}s, expected the drip to never block the real request"


def _break_response_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make `send_response` - the first call answering the browser makes - raise `OSError`,
    standing in for a failed socket write. The handler class is local to
    `wait_for_authorization_code` and never overrides it, so patching the base class reaches it.
    """

    def _raise(self, *args: object, **kwargs: object) -> None:
        raise OSError("simulated: peer gone")

    monkeypatch.setattr(http.server.BaseHTTPRequestHandler, "send_response", _raise)


def _get_ignoring_the_broken_response(url: str) -> None:
    # The server closes the connection without writing a response, so the client side raises
    # too; only the server-side outcome matters here.
    with contextlib.suppress(OSError):
        urllib.request.urlopen(url, timeout=2)


def test_wait_for_authorization_code_returns_the_code_even_if_the_response_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The outcome is recorded before the browser is answered, so a failed answer still
    returns the code instead of timing out."""
    port = _free_port()
    _break_response_writes(monkeypatch)

    def on_listening() -> None:
        threading.Thread(
            target=_get_ignoring_the_broken_response,
            args=(f"http://127.0.0.1:{port}/callback?code=the-code&state=s",),
        ).start()

    assert _wait(port, 2.0, on_listening) == "the-code"


def test_wait_for_authorization_code_raises_the_error_even_if_the_response_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    port = _free_port()
    _break_response_writes(monkeypatch)

    def on_listening() -> None:
        threading.Thread(
            target=_get_ignoring_the_broken_response,
            args=(f"http://127.0.0.1:{port}/callback?error=access_denied&state=s",),
        ).start()

    with pytest.raises(CTraderAuthorizationDenied) as exc_info:
        _wait(port, 2.0, on_listening)

    assert exc_info.value.error_code == "access_denied"


def test_wait_for_authorization_code_rejects_non_ascii_state_without_a_traceback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`secrets.compare_digest` raises `TypeError` for `str` arguments containing non-ASCII
    characters; a stray request such as `?state=%C3%A9` must get a clean 400 and let the wait
    continue, not print a `socketserver` traceback to stderr."""
    port = _free_port()
    started = {}

    def _status(url: str) -> int:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                return response.status
        except urllib.error.HTTPError as e:
            return e.code
        except (urllib.error.URLError, OSError):
            # Reset without any response, which this test must report rather than hang on.
            return -1

    result: dict[str, int] = {}

    def on_listening() -> None:
        def _drive() -> None:
            query = urllib.parse.urlencode({"state": "é", "code": "wrong-code"})
            result["status"] = _status(f"http://127.0.0.1:{port}/callback?{query}")
            _get(f"http://127.0.0.1:{port}/callback?code=the-code&state=s")

        started["thread"] = threading.Thread(target=_drive)
        started["thread"].start()

    code = _wait(port, 5.0, on_listening)

    assert code == "the-code"
    started["thread"].join(timeout=2)
    assert result["status"] == 400
    assert "Traceback" not in capsys.readouterr().err


def test_wait_for_authorization_code_times_out_close_to_the_deadline() -> None:
    """With only an idle connection present, the timeout must fire close to `timeout_secs`,
    not be stretched out by the idle connection's own per-connection read timeout."""
    port = _free_port()
    idle_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def on_listening() -> None:
        idle_sock.connect(("127.0.0.1", port))

    start = time.monotonic()
    try:
        with pytest.raises(CTraderAuthorizationTimeout):
            _wait(port, 1.0, on_listening)
    finally:
        idle_sock.close()
    elapsed = time.monotonic() - start

    assert abs(elapsed - 1.0) < 0.5, f"took {elapsed:.2f}s, expected close to the 1.0s deadline"


def test_wait_for_authorization_code_raises_os_error_when_the_port_is_taken() -> None:
    port = _free_port()
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", port))
    blocker.listen(1)
    try:
        with pytest.raises(OSError):
            _wait(port, 1.0)
    finally:
        blocker.close()


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
def token_url():
    _StubTokenHandler.response_status = 200
    _StubTokenHandler.response_body = b"{}"
    _StubTokenHandler.captured_path = None
    server = http.server.HTTPServer(("127.0.0.1", 0), _StubTokenHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/apps/token"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def _respond(body: object, status: int = 200) -> None:
    _StubTokenHandler.response_status = status
    _StubTokenHandler.response_body = body if isinstance(body, bytes) else json.dumps(body).encode()


def _exchange(token_url: str, code: str = "the-code") -> oauth.TokenPair:
    return oauth.exchange_code(
        "cid",
        "csecret",
        code,
        "http://localhost:8080/callback",
        token_url=token_url,
    )


def test_exchange_code_defaults_to_the_documented_endpoint() -> None:
    assert oauth.TOKEN_URL == "https://openapi.ctrader.com/apps/token"


def test_exchange_code_sends_the_documented_query_parameters(token_url: str) -> None:
    _respond({"accessToken": "AT", "refreshToken": "RT", "expiresIn": 2419200})

    _exchange(token_url)

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


def test_exchange_code_returns_the_pair_with_an_absolute_expiry(token_url: str) -> None:
    _respond(
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": 2419200, "tokenType": "bearer"},
    )

    before = time.time()
    tokens = _exchange(token_url)
    after = time.time()

    assert tokens.access_token == "AT"
    assert tokens.refresh_token == "RT"
    assert before + 2419200 <= tokens.expires_at <= after + 2419200


def test_exchange_code_raises_on_error_code(token_url: str) -> None:
    _respond(
        {
            "errorCode": "INVALID_GRANT",
            "description": "code expired",
            "accessToken": "AT-should-not-appear",
            "refreshToken": "RT-should-not-appear",
        },
    )

    with pytest.raises(CTraderTokenExchangeError) as exc_info:
        _exchange(token_url, "the-code-should-not-appear")

    error = exc_info.value
    assert isinstance(error, CTraderAuthError)
    assert error.error_code == "INVALID_GRANT"
    assert error.description == "code expired"
    assert "INVALID_GRANT" in str(error)
    texts = _texts(error)
    for secret in ("csecret", "AT-should-not-appear", "RT-should-not-appear", "the-code-should"):
        assert secret not in texts


def test_exchange_code_raises_on_http_error(token_url: str) -> None:
    _respond({"errorCode": "SHOULD_NOT_APPEAR"}, status=400)

    with pytest.raises(CTraderTokenExchangeError) as exc_info:
        _exchange(token_url)

    assert exc_info.value.http_status == 400
    assert "400" in str(exc_info.value)
    assert "SHOULD_NOT_APPEAR" not in _texts(exc_info.value)
    assert "csecret" not in _texts(exc_info.value)


def test_exchange_code_chains_from_none_on_http_error(token_url: str) -> None:
    """`HTTPError` carries the full request URL, secret included, in `.url`; chaining from it
    would put that URL in any traceback."""
    _respond(b"{}", status=500)

    with pytest.raises(CTraderTokenExchangeError) as exc_info:
        _exchange(token_url)

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_exchange_code_raises_on_connection_failure() -> None:
    """A `URLError` (for example a closed port) must be wrapped the same way, without
    chaining from the original exception - it too can carry the request URL."""
    closed_port = _free_port()

    with pytest.raises(CTraderTokenExchangeError) as exc_info:
        _exchange(f"http://127.0.0.1:{closed_port}/apps/token")

    assert "csecret" not in _texts(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        json.dumps(["not", "an", "object"]).encode(),
        {"accessToken": "AT", "refreshToken": "RT"},
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": 0},
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": float("nan")},
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": float("inf")},
        # `bool` is a subclass of `int`; `True` must not pass as a seconds count.
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": True},
        {"accessToken": "AT", "refreshToken": "RT", "expiresIn": 0.5},
        {"accessToken": None, "refreshToken": "RT", "expiresIn": 3600},
        {"accessToken": "AT", "refreshToken": "", "expiresIn": 3600},
        {"accessToken": "AT\nInjected", "refreshToken": "RT", "expiresIn": 3600},
    ],
)
def test_exchange_code_raises_on_an_unusable_body(token_url: str, body: object) -> None:
    _respond(body)

    with pytest.raises(CTraderTokenExchangeError) as exc_info:
        _exchange(token_url)

    texts = _texts(exc_info.value)
    assert "AT" not in texts
    assert "Injected" not in texts
    assert exc_info.value.error_code is None
    assert exc_info.value.__cause__ is None


def test_exchange_code_sanitizes_error_code_and_description(token_url: str) -> None:
    nasty = "bad\x07\x1b[31mtext"
    _respond({"errorCode": nasty, "description": nasty})

    with pytest.raises(CTraderTokenExchangeError) as exc_info:
        _exchange(token_url)

    error = exc_info.value
    assert all(ch.isprintable() for ch in str(error))
    assert all(ch.isprintable() for ch in error.error_code)
    assert all(ch.isprintable() for ch in error.description)


def test_exchange_code_masks_request_values_the_endpoint_echoes(token_url: str) -> None:
    _respond(
        {
            "errorCode": "BAD the-code csecret",
            "description": "code the-code for cid with csecret, token AT-echo",
            "accessToken": "AT-echo",
        },
    )

    with pytest.raises(CTraderTokenExchangeError) as exc_info:
        _exchange(token_url)

    error = exc_info.value
    texts = " ".join([_texts(error), error.error_code, error.description])
    for secret in ("the-code", "csecret", "cid", "AT-echo"):
        assert secret not in texts
