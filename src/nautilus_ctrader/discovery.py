"""Read-only discovery of what an access token grants: its accounts, and their symbols by name.

Meant for a first-time setup, after `oauth.exchange_code()`: `list_accounts()` shows which
account to configure, by its trader login, and `list_symbols()` looks the broker's symbols up.

- Read-only by construction: only authentication, account list, trader, asset and symbol
  requests are sent.
- Never refreshes a token: a refresh token is single-use, and nothing here could hand the new
  pair back. A rejected access token raises `CTraderAuthError` instead; refresh the pair through
  a connected account client (or authorise again) and retry.
- Every connection is short-lived, closed before returning, and bounded by the connect and
  request timeouts. No token or secret reaches a log line or an error message.
"""

from __future__ import annotations

import dataclasses
import ssl
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal

from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.account import (
    AccountCredentials,
    CTraderAccountClient,
    NullLogger,
    account_host,
    request_error_code,
    request_granted_accounts,
)
from nautilus_ctrader.common.connection import CTraderConnection
from nautilus_ctrader.common.errors import CTraderAuthError, CTraderRequestError
from nautilus_ctrader.common.parsing import volume_to_units
from nautilus_ctrader.constants import DEMO_HOST, LIVE_HOST, PROTOBUF_PORT, TOKEN_ERROR_CODES
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om


@dataclass(frozen=True)
class GrantedAccount:
    """One account an access token grants.

    - `is_live`: the venue's flag; `None` when it sent none. It decides the host.
    - `deposit_currency`: the deposit asset's name, or `None` when it could not be read.
    - `refusal`: why it could not be read: the venue's error code, or a short reason.
    """

    trader_login: int | None
    ctid_trader_account_id: int
    is_live: bool | None
    broker_name: str | None
    deposit_currency: str | None
    refusal: str | None


@dataclass(frozen=True)
class SymbolInfo:
    """A broker symbol. All but `name`, `symbol_id` and `enabled` are `None` unless the symbol
    was looked up by name, or when the venue does not report them.

    Volumes are in units of the base asset; `lot_size_units` is the size of one lot.
    """

    name: str
    symbol_id: int
    enabled: bool
    base_asset: str | None
    quote_asset: str | None
    digits: int | None
    min_volume_units: Decimal | None
    step_volume_units: Decimal | None
    lot_size_units: Decimal | None


def _token_rejected(error_code: str) -> CTraderAuthError:
    return CTraderAuthError(
        f"access token rejected ({error_code}); discovery never refreshes tokens: refresh the "
        "pair through a connected account client or authorise again, then retry",
    )


async def list_accounts(
    client_id: str,
    client_secret: str,
    access_token: str,
    *,
    demo_host: str = DEMO_HOST,
    live_host: str = LIVE_HOST,
    port: int = PROTOBUF_PORT,
    tls: ssl.SSLContext | bool = True,
    logger: Logger | None = None,
) -> list[GrantedAccount]:
    """Every account `access_token` grants, in the venue's order, with its deposit currency.

    The list is read on `demo_host`, which serves it for either environment. Each account is
    then authenticated on the host its live flag names (an unknown flag means demo) to read its
    deposit asset. An account that refuses keeps its row, with `refusal` saying why.

    The host, port and TLS arguments exist for tests. Raises `CTraderAuthError` if the
    application or the access token is rejected, and `CTraderConnectionError` or
    `CTraderTimeoutError` if a host cannot be reached.
    """
    log = NullLogger() if logger is None else logger
    connections: dict[str, CTraderConnection] = {}

    async def connection_to(host: str) -> CTraderConnection:
        connection = connections.get(host)
        if connection is None:
            connection = CTraderConnection(host, port, logger=log, tls=tls)
            connections[host] = connection
            await connection.connect()
            try:
                await connection.request(
                    oa.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret),
                )
            except CTraderRequestError as e:
                raise CTraderAuthError(f"application auth rejected: {e.error_code}") from e
        return connection

    try:
        try:
            granted = await request_granted_accounts(await connection_to(demo_host), access_token)
        except CTraderRequestError as e:
            if e.error_code in TOKEN_ERROR_CODES:
                raise _token_rejected(e.error_code) from None
            raise CTraderAuthError(f"account list rejected: {e.error_code}") from e

        accounts = []
        for record in granted.accounts:
            host = account_host(record.is_live, demo_host=demo_host, live_host=live_host)
            deposit_currency, refusal = await _read_deposit_currency(
                await connection_to(host),
                record.ctid_trader_account_id,
                access_token,
            )
            accounts.append(
                GrantedAccount(
                    trader_login=record.trader_login,
                    ctid_trader_account_id=record.ctid_trader_account_id,
                    is_live=record.is_live,
                    broker_name=record.broker_title_short,
                    deposit_currency=deposit_currency,
                    refusal=refusal,
                ),
            )
        return accounts
    finally:
        for connection in connections.values():
            await connection.close()


async def _read_deposit_currency(
    connection: CTraderConnection,
    account_id: int,
    access_token: str,
) -> tuple[str | None, str | None]:
    """The account's deposit asset name and `None`, or `None` and why it could not be read."""
    try:
        await connection.request(
            oa.ProtoOAAccountAuthReq(ctidTraderAccountId=account_id, accessToken=access_token),
        )
        trader = (
            await connection.request(oa.ProtoOATraderReq(ctidTraderAccountId=account_id))
        ).trader
        asset_list = await connection.request(
            oa.ProtoOAAssetListReq(ctidTraderAccountId=account_id),
        )
    except CTraderRequestError as e:
        return None, e.error_code
    for asset in asset_list.asset:
        if asset.assetId == trader.depositAssetId:
            return asset.name, None
    return None, "deposit asset missing from the asset list"


async def list_symbols(
    credentials: AccountCredentials,
    trader_login: int,
    *,
    environment: str = "auto",
    names: Iterable[str] | None = None,
    demo_host: str = DEMO_HOST,
    live_host: str = LIVE_HOST,
    port: int = PROTOBUF_PORT,
    tls: ssl.SSLContext | bool = True,
    logger: Logger | None = None,
) -> dict[str, SymbolInfo]:
    """The symbols of the account with `trader_login`, keyed by the broker's symbol name.

    The account is resolved and connected exactly as `CTraderAccountClient` does it, with
    `environment` meaning the same. With `names=None`, every listed symbol is returned with
    its name, id and enabled flag only. With `names`, only those symbols are returned, with
    digits, volumes and asset names filled in; a name the broker does not list is absent.

    The refresh token and expiry in `credentials` are ignored, so nothing is ever refreshed: a
    rejected access token raises `CTraderAuthError` saying to refresh or authorise first. The
    host, port and TLS arguments exist for tests.
    """
    client = CTraderAccountClient(
        trader_login=trader_login,
        credentials=dataclasses.replace(credentials, refresh_token=None, token_expires_at=None),
        environment=environment,
        logger=NullLogger() if logger is None else logger,
        demo_host=demo_host,
        live_host=live_host,
        port=port,
        tls=tls,
    )
    try:
        try:
            await client.connect()
        except CTraderAuthError as e:
            error_code = request_error_code(e)
            if error_code in TOKEN_ERROR_CODES:
                raise _token_rejected(error_code) from None
            raise

        light_symbols = client.light_symbols
        if names is None:
            return {
                name: SymbolInfo(name, light.symbolId, light.enabled, *(None,) * 6)
                for name, light in light_symbols.items()
            }
        wanted = [light_symbols[name] for name in dict.fromkeys(names) if name in light_symbols]
        specs = await client.symbol_specs([light.symbolId for light in wanted])
        return {
            light.symbolName: _symbol_info(light, specs.get(light.symbolId), client.assets)
            for light in wanted
        }
    finally:
        await client.disconnect()


def _symbol_info(
    light: om.ProtoOALightSymbol,
    spec: om.ProtoOASymbol | None,
    assets: Mapping[int, om.ProtoOAAsset],
) -> SymbolInfo:
    def asset_name(asset_id: int) -> str | None:
        asset = assets.get(asset_id)
        return None if asset is None else asset.name

    def units(field: str) -> Decimal | None:
        if spec is None or not spec.HasField(field):
            return None
        return volume_to_units(getattr(spec, field))

    return SymbolInfo(
        name=light.symbolName,
        symbol_id=light.symbolId,
        enabled=light.enabled,
        base_asset=asset_name(light.baseAssetId),
        quote_asset=asset_name(light.quoteAssetId),
        digits=spec.digits if spec is not None and spec.HasField("digits") else None,
        min_volume_units=units("minVolume"),
        step_volume_units=units("stepVolume"),
        lot_size_units=units("lotSize"),
    )
