"""A fake venue that authenticates a session, and the session pointed at it."""

from __future__ import annotations

from nautilus_trader.common.component import Logger

from nautilus_ctrader.common.session import CTraderSession
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as oa_model
from tests.fake_server import FakeCTraderServer
from tests.recording_logger import RecordingLogger

ACCOUNT_ID = 1234567


def authenticating_server() -> FakeCTraderServer:
    """A fake venue that accepts application and account authentication."""
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


def session_for(
    server: FakeCTraderServer,
    logger: Logger | RecordingLogger | None = None,
    **overrides,
) -> CTraderSession:
    """A session for `ACCOUNT_ID` against `server`, over plain TCP.

    `overrides` replace or add any `CTraderSession` argument.
    """
    arguments = {
        "host": server.host,
        "port": server.port,
        "client_id": "client-id",
        "client_secret": "client-secret",
        "account_id": ACCOUNT_ID,
        "access_token": "access-token",
        "logger": Logger("test") if logger is None else logger,
        "tls": False,
    }
    return CTraderSession(**{**arguments, **overrides})
