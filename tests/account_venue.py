"""A fake venue serving recorded reference data, and account clients pointed at it."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from google.protobuf.message import Message

from nautilus_ctrader.common.account import AccountCredentials, CTraderAccountClient
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.fixtures import FAKE_ACCOUNT_ID, load_recorded
from tests.recording_logger import RecordingLogger

ACCOUNT_ID = 7654321
# The recorder clears `traderLogin`, so the fake venue supplies its own. A real venue always
# sends one, and a distinctive value keeps the log-leak check honest.
TRADER_LOGIN = 8901234
RECORDED = load_recorded()


def for_account(message: Message, account_id: int) -> Message:
    """A copy of a recorded reply, re-addressed to `account_id`."""
    copy = type(message)()
    copy.CopyFrom(message)
    copy.ctidTraderAccountId = account_id
    if isinstance(copy, oa.ProtoOATraderRes):
        copy.trader.ctidTraderAccountId = account_id
    return copy


def account_list(*, is_live: bool, listed: bool = True) -> oa.ProtoOAGetAccountListByAccessTokenRes:
    res = oa.ProtoOAGetAccountListByAccessTokenRes()
    res.CopyFrom(RECORDED["account_list"][0])
    del res.ctidTraderAccount[:]
    if listed:
        for recorded in RECORDED["account_list"][0].ctidTraderAccount:
            entry = res.ctidTraderAccount.add()
            entry.CopyFrom(recorded)
            assert entry.ctidTraderAccountId == FAKE_ACCOUNT_ID
            entry.ctidTraderAccountId = ACCOUNT_ID
            entry.traderLogin = TRADER_LOGIN
            entry.isLive = is_live
    return res


def _symbol_by_id(request: oa.ProtoOASymbolByIdReq) -> oa.ProtoOASymbolByIdRes:
    wanted = set(request.symbolId)
    recorded = RECORDED["symbol_specs"][0]
    return oa.ProtoOASymbolByIdRes(
        ctidTraderAccountId=request.ctidTraderAccountId,
        symbol=[s for s in recorded.symbol if s.symbolId in wanted],
    )


def venue(*, is_live: bool = True, listed: bool = True) -> FakeCTraderServer:
    """A server that authenticates, lists the account, and serves recorded reference data."""
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda r: oa.ProtoOAAccountAuthRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    server.on(
        oa_model.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
        lambda _r: account_list(is_live=is_live, listed=listed),
    )
    for payload_type, key in (
        (oa_model.PROTO_OA_TRADER_REQ, "trader"),
        (oa_model.PROTO_OA_ASSET_LIST_REQ, "assets"),
        (oa_model.PROTO_OA_SYMBOLS_LIST_REQ, "symbols"),
        (oa_model.PROTO_OA_SYMBOLS_FOR_CONVERSION_REQ, "conversion_eur_usd"),
    ):
        server.on(
            payload_type,
            lambda r, key=key: for_account(RECORDED[key][0], r.ctidTraderAccountId),
        )
    server.on(oa_model.PROTO_OA_SYMBOL_BY_ID_REQ, _symbol_by_id)
    return server


def credentials(**overrides) -> AccountCredentials:
    values = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "token_expires_at": time.time() + 3600.0,
    }
    values.update(overrides)
    return AccountCredentials(**values)


def account_client(
    server: FakeCTraderServer,
    *,
    environment: str = "auto",
    logger: RecordingLogger | None = None,
    **kwargs,
) -> CTraderAccountClient:
    kwargs.setdefault("credentials", credentials())
    return CTraderAccountClient(
        account_id=ACCOUNT_ID,
        environment=environment,
        logger=RecordingLogger() if logger is None else logger,
        demo_host=server.host,
        live_host=server.host,
        port=server.port,
        tls=False,
        **kwargs,
    )


def received(server: FakeCTraderServer, cls: type[Message]) -> list[Message]:
    return [m for m in server.received if isinstance(m, cls)]


class HeldReplies:
    """Leaves requests of one payload type unanswered until `release()`, holding the caller open.

    The first `answer_first` requests are answered at once; after `stop_holding()` every
    request is.
    """

    def __init__(
        self,
        server: FakeCTraderServer,
        payload_type: int,
        reply: Callable[[Message], Message],
        *,
        answer_first: int = 0,
    ) -> None:
        self._server = server
        self._reply = reply
        self._answer_first = answer_first
        self._holding = True
        self.pending: list[tuple[str, Message]] = []
        self.arrived = asyncio.Event()
        server.on(payload_type, self._handle)

    def _handle(self, request: Message) -> Message | None:
        if not self._holding:
            return self._reply(request)
        if self._answer_first > 0:
            self._answer_first -= 1
            return self._reply(request)
        self.pending.append((self._server.received_client_msg_ids[-1], request))
        self.arrived.set()
        return None

    async def release(self) -> None:
        pending, self.pending = self.pending, []
        self.arrived.clear()
        for client_msg_id, request in pending:
            await self._server.push(self._reply(request), client_msg_id=client_msg_id)

    async def stop_holding(self) -> None:
        self._holding = False
        await self.release()


def hold_account_auth(server: FakeCTraderServer, **kwargs) -> HeldReplies:
    return HeldReplies(
        server,
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda r: oa.ProtoOAAccountAuthRes(ctidTraderAccountId=r.ctidTraderAccountId),
        **kwargs,
    )
