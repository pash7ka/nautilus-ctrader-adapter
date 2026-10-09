"""Data requests the client could not serve, published for applications.

Nautilus has no way to report a failed request: the requester's callback fires with no data.
So every request this client fails is also published on the message bus under
`REQUEST_FAILED_TOPIC`, before its empty response. An actor subscribes with
`self.msgbus.subscribe(topic=REQUEST_FAILED_TOPIC, handler=...)`.
"""

from __future__ import annotations

from dataclasses import dataclass

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import DataType

REQUEST_FAILED_TOPIC = "ctrader.request_failed"


@dataclass(frozen=True)
class CTraderRequestFailed:
    """One data request that was answered with no data because it failed.

    - `request_id`: the id the requester's callback receives.
    - `data_type`: what was requested, as the response carries it; for bars
      `data_type.metadata["bar_type"]` is the bar type.
    - `reason`: a short English phrase with at most a venue error code, such as
      `"venue refused: INVALID_REQUEST"`, `"timed out"` or `"connection lost"`.
    """

    request_id: UUID4
    data_type: DataType
    reason: str
