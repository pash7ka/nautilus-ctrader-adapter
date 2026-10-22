"""Multi-handler event dispatch, historical-group routing, and restore retry."""

from __future__ import annotations

import asyncio

from nautilus_ctrader.common.errors import CTraderConnectionError
from nautilus_ctrader.common.session import CTraderSession, SessionState
from nautilus_ctrader.constants import GROUP_HISTORICAL, GROUP_OTHER
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.polling import wait_until
from tests.recording_logger import RecordingLogger
from tests.session_venue import ACCOUNT_ID, authenticating_server, session_for


async def test_two_handlers_for_the_same_type_both_receive_the_event() -> None:
    server = authenticating_server()
    await server.start()
    session = session_for(server)
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
    server = authenticating_server()
    await server.start()
    session = session_for(server)
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
    server = authenticating_server()
    await server.start()
    logger = RecordingLogger()
    session = session_for(server, logger=logger)
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


async def test_a_handler_removing_itself_during_dispatch_does_not_skip_the_next_one() -> None:
    server = authenticating_server()
    await server.start()
    session = session_for(server)
    second: list[object] = []

    def remove_self(_payload: object) -> None:
        session.remove_event_handler(oa.ProtoOASpotEvent, remove_self)

    try:
        session.add_event_handler(oa.ProtoOASpotEvent, remove_self)
        session.add_event_handler(oa.ProtoOASpotEvent, second.append)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(
            oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=1, bid=1, ask=2),
        )
        await wait_until(lambda: bool(second), description="the second handler still ran")

        assert any(isinstance(m, oa.ProtoOASpotEvent) for m in second)
    finally:
        await session.stop()
        await server.stop()


async def test_remove_event_handler_on_a_live_session_stops_delivery() -> None:
    server = authenticating_server()
    await server.start()
    session = session_for(server)
    seen: list[object] = []
    try:
        session.add_event_handler(oa.ProtoOASpotEvent, seen.append)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        session.remove_event_handler(oa.ProtoOASpotEvent, seen.append)

        await server.push(
            oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=1, bid=1, ask=2),
        )
        await asyncio.sleep(0.2)

        assert seen == []
    finally:
        await session.stop()
        await server.stop()


async def test_an_event_with_no_handler_logs_a_debug_line_naming_the_type() -> None:
    server = authenticating_server()
    await server.start()
    logger = RecordingLogger()
    session = session_for(server, logger=logger)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)

        await server.push(
            oa.ProtoOASpotEvent(ctidTraderAccountId=ACCOUNT_ID, symbolId=1, bid=1, ask=2),
        )
        await wait_until(
            lambda: any(
                level == "debug" and "ProtoOASpotEvent" in message
                for level, message in logger.lines
            ),
            description="a debug line naming the payload type",
        )
    finally:
        await session.stop()
        await server.stop()


def _record_groups(session: CTraderSession) -> list[str]:
    """Make the session's scheduler record the group of every request it is given."""
    scheduler = session._connection._scheduler
    submit = scheduler.submit
    groups: list[str] = []

    async def recording(group, priority, send):
        groups.append(group)
        return await submit(group, priority, send)

    scheduler.submit = recording
    return groups


def _trendbars_req() -> oa.ProtoOAGetTrendbarsReq:
    return oa.ProtoOAGetTrendbarsReq(
        ctidTraderAccountId=ACCOUNT_ID,
        period=oa_model.M1,
        symbolId=1,
    )


async def test_a_historical_request_uses_the_historical_group_and_auth_the_other() -> None:
    server = authenticating_server()
    server.on(
        oa_model.PROTO_OA_GET_TRENDBARS_REQ,
        lambda _r: oa.ProtoOAGetTrendbarsRes(ctidTraderAccountId=ACCOUNT_ID, period=oa_model.M1),
    )
    await server.start()
    session = session_for(server)
    groups = _record_groups(session)
    try:
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        # The two auth requests during bring-up.
        assert groups == [GROUP_OTHER, GROUP_OTHER]

        await session.request(_trendbars_req())

        assert groups == [GROUP_OTHER, GROUP_OTHER, GROUP_HISTORICAL]
    finally:
        await session.stop()
        await server.stop()


async def test_retry_failed_restores_reruns_and_clears_a_succeeding_key() -> None:
    server = authenticating_server()
    replies = iter([oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="gone")])
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: next(replies, oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID)),
    )
    await server.start()
    session = session_for(server, backoff_base_secs=0.05)
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


async def test_retry_keeps_a_key_that_fails_again_and_logs_a_warning() -> None:
    server = authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="gone"),
    )
    await server.start()
    logger = RecordingLogger()
    session = session_for(server, logger=logger, backoff_base_secs=0.05)
    attempts: list[int] = []

    async def subscribe() -> None:
        attempts.append(1)
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore(("spots", 1), subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert session.failed_restores == frozenset({("spots", 1)})

        await session.retry_failed_restores()

        assert session.failed_restores == frozenset({("spots", 1)})
        assert len(attempts) == 2
        warnings = [m for level, m in logger.lines if level == "warning" and "spots" in m]
        assert warnings, f"no warning line recorded; lines were {logger.lines}"
    finally:
        await session.stop()
        await server.stop()


async def test_retry_returns_immediately_when_not_ready() -> None:
    server = authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="gone"),
    )
    await server.start()
    session = session_for(server, backoff_base_secs=0.05)
    attempts: list[int] = []

    async def subscribe() -> None:
        attempts.append(1)
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore(("spots", 1), subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert session.failed_restores == frozenset({("spots", 1)})

        await session.stop()
        assert session.state is not SessionState.READY

        await session.retry_failed_restores()

        assert len(attempts) == 1
        assert session.failed_restores == frozenset({("spots", 1)})
    finally:
        await session.stop()
        await server.stop()


async def test_retry_stops_when_the_connection_is_lost_mid_retry() -> None:
    server = authenticating_server()
    await server.start()
    session = session_for(server, backoff_base_secs=0.05)
    calls: list[int] = []

    async def flaky() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("simulated failure")
        raise CTraderConnectionError("connection lost during retry")

    try:
        session.add_restore("flaky", flaky)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert session.failed_restores == frozenset({"flaky"})

        await session.retry_failed_restores()

        assert session.failed_restores == frozenset({"flaky"})
        assert len(calls) == 2
    finally:
        await session.stop()
        await server.stop()


async def test_overlapping_retries_do_not_rerun_a_factory_twice() -> None:
    server = authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOAErrorRes(errorCode="ENTITY_NOT_FOUND", description="gone"),
    )
    await server.start()
    session = session_for(server, backoff_base_secs=0.05)
    attempts: list[int] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def subscribe() -> None:
        attempts.append(1)
        if len(attempts) == 2:
            entered.set()
            await release.wait()
        await session.request(
            oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
        )

    try:
        session.add_restore(("spots", 1), subscribe)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert len(attempts) == 1

        first = asyncio.create_task(session.retry_failed_restores())
        await wait_until(entered.is_set, description="the first retry's factory in flight")

        second = asyncio.create_task(session.retry_failed_restores())
        await asyncio.sleep(0.05)
        assert second.done(), "an overlapping retry must return immediately"

        release.set()
        await first
        await second

        assert len(attempts) == 2
    finally:
        await session.stop()
        await server.stop()


async def test_a_retry_spanning_a_reconnect_does_not_clear_a_newly_failed_key() -> None:
    # The connection drops while the retry's own factory call is in flight. The supervisor's
    # next bring-up clears `_failed_restores` and marks the same key failed again on its own
    # attempt; the original retry's call then succeeds only because `RESTORING` accepts
    # requests. Its stale result must not overwrite what the newer bring-up decided.
    server = authenticating_server()
    server.on(
        oa_model.PROTO_OA_SUBSCRIBE_SPOTS_REQ,
        lambda _r: oa.ProtoOASubscribeSpotsRes(ctidTraderAccountId=ACCOUNT_ID),
    )
    await server.start()
    session = session_for(server, backoff_base_secs=0.05)
    calls: list[int] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def factory() -> None:
        calls.append(1)
        n = len(calls)
        if n == 1:
            # Bring-up 1: fails, so the key starts in `failed_restores`.
            raise RuntimeError("simulated failure")
        if n == 2:
            # The retry's own call: pauses so the test can force a reconnect underneath it,
            # then issues a real request once resumed.
            entered.set()
            await release.wait()
            await session.request(
                oa.ProtoOASubscribeSpotsReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[1]),
            )
            return
        # Bring-up 2's own attempt: fails too, re-marking the key under a new generation.
        raise RuntimeError("simulated failure again")

    try:
        session.add_restore("flaky", factory)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        assert session.failed_restores == frozenset({"flaky"})
        generation_after_first_bring_up = session._bring_up_generation

        retry_task = asyncio.create_task(session.retry_failed_restores())
        await wait_until(entered.is_set, description="the retry's factory in flight")

        await server.drop_connections()
        await wait_until(lambda: len(calls) >= 3, description="the second bring-up's own attempt")
        await session.wait_ready(timeout_secs=3.0)
        assert session._bring_up_generation != generation_after_first_bring_up
        assert session.failed_restores == frozenset({"flaky"})

        release.set()
        await retry_task

        # The stale retry must not have cleared the key the newer bring-up just re-failed.
        assert session.failed_restores == frozenset({"flaky"})
    finally:
        await session.stop()
        await server.stop()


async def test_mark_restore_failed_marks_only_a_registered_key() -> None:
    server = authenticating_server()
    await server.start()
    session = session_for(server)
    runs = []

    async def restore() -> None:
        runs.append("k")

    try:
        session.add_restore("k", restore)
        await session.start()
        await session.wait_ready(timeout_secs=2.0)
        runs.clear()

        session.mark_restore_failed("k")
        session.mark_restore_failed("unregistered")
        assert session.failed_restores == frozenset({"k"})

        await session.retry_failed_restores()

        assert runs == ["k"]
        assert session.failed_restores == frozenset()
    finally:
        await session.stop()
        await server.stop()
