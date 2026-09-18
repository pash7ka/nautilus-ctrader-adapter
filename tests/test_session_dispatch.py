"""Multi-handler event dispatch, historical-bucket routing, and restore retry."""

from __future__ import annotations

import asyncio

from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.rate_limit import RateLimiter
from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.constants import BUCKET_DEFAULT, BUCKET_HISTORICAL
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger

ACCOUNT_ID = 1234567


def _authenticating_server() -> FakeCTraderServer:
    server = FakeCTraderServer()
    server.on(
        oa_model.PROTO_OA_APPLICATION_AUTH_REQ,
        lambda _r: oa.ProtoOAApplicationAuthRes(),
    )
    server.on(
        oa_model.PROTO_OA_ACCOUNT_AUTH_REQ,
        lambda _r: oa.ProtoOAAccountAuthRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    return server


def _session(
    server: FakeCTraderServer,
    logger: Logger | RecordingLogger | None = None,
    **kwargs,
) -> CTraderSession:
    return CTraderSession(
        host=server.host,
        port=server.port,
        client_id="client-id",
        client_secret="client-secret",
        account_id=ACCOUNT_ID,
        access_token="access-token",
        logger=Logger("test") if logger is None else logger,
        tls=False,
        **kwargs,
    )


async def test_two_handlers_for_the_same_type_both_receive_the_event() -> None:
    server = _authenticating_server()
    await server.start()
    session = _session(server)
    first: list[object] = []
    second: list[object] = []
    executions: list[object] = []
    try:
        session.add_event_handler(oa.ProtoOASpotEvent, first.append)
        session.add_event_handler(oa.ProtoOASpotEvent, second.append)
        session.add_event_handler(oa.ProtoOAExecutionEvent, executions.append)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(
            oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=1, bid=1, ask=2),
        )
        await wait_until(lambda: first and second, description="both handlers seeing the spot")

        assert any(isinstance(m, oa.ProtoOASpotEvent) for m in first)
        assert any(isinstance(m, oa.ProtoOASpotEvent) for m in second)
        assert executions == []
    finally:
        await session.stop()
        await server.stop()


async def test_remove_event_handler_stops_delivery() -> None:
    server = _authenticating_server()
    await server.start()
    session = _session(server)
    seen: list[object] = []
    try:
        session.add_event_handler(oa.ProtoOASpotEvent, seen.append)
        session.remove_event_handler(oa.ProtoOASpotEvent, seen.append)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(
            oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=1, bid=1, ask=2),
        )
        await asyncio.sleep(0.2)

        assert seen == []
    finally:
        await session.stop()
        await server.stop()


async def test_a_raising_handler_does_not_stop_the_next_one() -> None:
    server = _authenticating_server()
    await server.start()
    logger = RecordingLogger()
    session = _session(server, logger=logger)
    seen: list[object] = []

    def explode(_payload: object) -> None:
        raise RuntimeError("boom")

    try:
        session.add_event_handler(oa.ProtoOASpotEvent, explode)
        session.add_event_handler(oa.ProtoOASpotEvent, seen.append)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(
            oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=1, bid=1, ask=2),
        )
        await wait_until(lambda: bool(seen), description="the second handler still running")

        assert any(isinstance(m, oa.ProtoOASpotEvent) for m in seen)
        assert logger.errors(), f"no error line recorded; lines were {logger.lines}"
    finally:
        await session.stop()
        await server.stop()


class _RecordingRateLimiter(RateLimiter):
    """A `RateLimiter` that records the buckets it is asked for, then delegates."""

    def __init__(self, rates: dict[str, float]) -> None:
        super().__init__(rates)
        self.acquired: list[str] = []

    async def acquire(self, bucket: str = BUCKET_DEFAULT) -> None:
        self.acquired.append(bucket)
        await super().acquire(bucket)


async def test_a_historical_request_uses_the_historical_bucket() -> None:
    server = _authenticating_server()
    server.on(
        oa_model.PROTO_OA_GET_TRENDBARS_REQ,
        lambda _r: oa.ProtoOAGetTrendbarsRes(ctidTraderAccountId=ACCOUNT_ID, period=oa_model.M1),
    )
    await server.start()
    limiter = _RecordingRateLimiter({BUCKET_DEFAULT: 1000.0, BUCKET_HISTORICAL: 1000.0})
    session = _session(server, rate_limiter=limiter)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await session.request(
            oa.ProtoOAGetTrendbarsReq(
                ctidTraderAccountId=ACCOUNT_ID,
                period=oa_model.M1,
                symbolId=1,
            ),
        )

        assert BUCKET_HISTORICAL in limiter.acquired
    finally:
        await session.stop()
        await server.stop()


async def test_retry_failed_restores_reruns_and_clears_a_succeeding_key() -> None:
    server = _authenticating_server()
    replies = iter([oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="gone")])
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: next(replies, oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID)),
    )
    await server.start()
    session = _session(server, backoff_base_secs=0.05)
    attempts: list[int] = []

    async def subscribe() -> None:
        attempts.append(1)
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore("k", subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert session.failed_restores == frozenset({"k"})
        assert len(attempts) == 1

        await session.retry_failed_restores()

        assert session.failed_restores == frozenset()
        assert len(attempts) == 2
    finally:
        await session.stop()
        await server.stop()
