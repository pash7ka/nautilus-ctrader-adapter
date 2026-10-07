# Setup and discovery

An application needs an access/refresh token pair before any client can connect, and it needs
to know which account to configure and what the broker calls its symbols. The package provides
the protocol parts of that setup as functions; storing the pair and asking the user anything
stay with the application.

- `nautilus_ctrader.oauth` (synchronous, no Nautilus objects): the authorization URL, a
  localhost callback that catches the redirect, and the exchange of the code for a pair.
- `nautilus_ctrader.discovery` (async, read-only): the accounts a token grants, and an
  account's symbols by name.

Every name below is also importable from `nautilus_ctrader`.

## First-time setup

The application's registered redirect URI must be plain `http` on `localhost` or `127.0.0.1`,
for example `http://localhost:8080/callback`. The callback server binds `127.0.0.1` on that
port and serves only that path.

```python
import asyncio
import secrets
import webbrowser

from nautilus_ctrader import (
    build_authorization_url,
    exchange_code,
    list_accounts,
    wait_for_authorization_code,
)

REDIRECT_URI = "http://localhost:8080/callback"

state = secrets.token_urlsafe(32)
url = build_authorization_url(client_id, REDIRECT_URI, state)
code = wait_for_authorization_code(
    REDIRECT_URI,
    state,
    timeout_secs=300.0,
    on_listening=lambda: webbrowser.open(url),
)
tokens = exchange_code(client_id, client_secret, code, REDIRECT_URI)
store_tokens(tokens.access_token, tokens.refresh_token, tokens.expires_at)

for account in asyncio.run(list_accounts(client_id, client_secret, tokens.access_token)):
    print(account.trader_login, account.is_live, account.broker_name, account.deposit_currency)
```

`wait_for_authorization_code` prints nothing, not even for a request that fails. It ignores
requests to other paths and redirects whose `state` does not match. `TokenPair`'s repr masks
both tokens.

`list_accounts` returns one `GrantedAccount` per granted account, in the venue's order. A client
is configured with its `trader_login`. Each account's deposit currency is read on the host its
live flag names, where an unknown flag means demo. An account whose deposit currency cannot be
read is kept, with `deposit_currency=None` and `refusal` set to:

- the venue's error code, when it refuses the account (or the application on that host);
- `"unreachable"`, when that host cannot be reached or its connection is lost;
- `"timeout"`, when a request about the account goes unanswered.

A host that failed is not tried again for the accounts after it. Only a failure to read the
list itself, on the demo host, raises.

## A pasted code

When the redirect cannot reach the machine running the application, the user can open the URL
from `build_authorization_url` anywhere, and paste back the address the browser is sent to.
Check its `state` against the one the URL was built with before using its code, as the callback
server does, so that a code from some other authorization is never exchanged:

```python
import secrets
import urllib.parse

params = urllib.parse.parse_qs(urllib.parse.urlsplit(pasted_redirect_url).query)
if not secrets.compare_digest(params.get("state", [""])[0].encode(), state.encode()):
    raise ValueError("the pasted address does not belong to this authorization")
if "code" not in params:
    raise ValueError(f"authorization not granted: {params.get('error', ['no code'])[0]}")
tokens = exchange_code(client_id, client_secret, params["code"][0], REDIRECT_URI)
```

`REDIRECT_URI` must be the one the authorization URL was built with.

## Symbols by name

```python
from nautilus_ctrader import AccountCredentials, list_symbols

credentials = AccountCredentials(
    client_id=client_id,
    client_secret=client_secret,
    access_token=tokens.access_token,
    refresh_token=tokens.refresh_token,
    token_expires_at=tokens.expires_at,
)
symbols = asyncio.run(list_symbols(credentials, trader_login, names=["EURUSD", "XAUUSD"]))
eurusd = symbols.get("EURUSD")  # absent if the broker does not list the name
```

The account is resolved by its trader login, and the host is chosen, exactly as the account
client does it. `environment` (default `"auto"`) has the same meaning as in the client configs.

- Without `names`, every listed symbol comes back, with its name, id and enabled flag only.
- With `names`, only those symbols come back, with their digits, base and quote asset names, and
  minimum volume, volume step and lot size in units of the base asset.

## Discovery never refreshes a token

A refresh token works once. A refresh would replace the stored pair, and discovery has no way to
hand the new pair back. So discovery never refreshes: `list_accounts` takes the access token
only, and `list_symbols` ignores the refresh token and the expiry in its credentials. A rejected
or expired access token raises `CTraderAuthError` with the venue's error code, saying to refresh
the pair or authorise again first. A connected account client refreshes the pair on its own and reports it through
`add_token_listener()` (see [Persisting refreshed tokens](market_data.md#persisting-refreshed-tokens)).

Discovery sends only authentication, account list, trader, asset and symbol requests. Each
connection is short-lived and closed before the call returns.

## What each function raises

No error carries a token, the code, the client id or the secret, in its message, its arguments
or its fields. A venue's refusal is reported by its error code only, with no cause attached: the
venue's free-text description may echo what it was sent.

| Function | Raises |
|---|---|
| `build_authorization_url` | nothing |
| `wait_for_authorization_code` | `RedirectUriError` (a `ValueError`): the redirect URI is not plain `http` on `localhost` or `127.0.0.1`; `reason` says why. `OSError`: the callback port cannot be bound. `CTraderAuthorizationDenied`: the redirect carries an error; the redirect's error value is in `error_code`. `CTraderAuthorizationTimeout`: no redirect in time. |
| `exchange_code` | `CTraderTokenExchangeError`: the token endpoint refuses the code (`error_code`, `description`), answers with an HTTP error (`http_status`), cannot be reached, or answers with anything but a usable pair. |
| `list_accounts` | `CTraderAuthError`: the application or the access token is rejected. `CTraderConnectionError` or `CTraderTimeoutError`: the demo host, which serves the list, fails. |
| `list_symbols` | `TypeError`: `names` is a single `str`. `ValueError`: an unknown `environment`. `CTraderAuthError`: the application, the token or the trader login is rejected. `CTraderConnectionError`, `CTraderTimeoutError`, `CTraderRequestError` or `CTraderProtocolError`: the account cannot be read. |

`CTraderAuthorizationDenied`, `CTraderAuthorizationTimeout` and `CTraderTokenExchangeError` are
`CTraderAuthError`s, and every `CTrader...Error` is a `CTraderError`.

## Why a connect failed

Nautilus logs a client's connect error when the node starts and carries on, so the application
does not see the exception. The account client keeps it. `last_connect_error` holds the
`CTraderError` the last failed `connect()` raised, and is `None` before any connect and after a
successful one:

```python
from nautilus_ctrader import CTraderAuthError, account_client_from_config

account = account_client_from_config(data_config, logger)  # the same instance the node uses
# ... after node.run() has tried to connect:
error = account.last_connect_error
if isinstance(error, CTraderAuthError):
    # The application or token was rejected with no usable refresh, or the account was refused
    # or disabled: run the setup again rather than retrying.
    ...
```
