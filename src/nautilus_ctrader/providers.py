"""`CTraderInstrumentProvider`: builds Nautilus instruments from broker symbol specs.

Requires `account` to already be connected; every method raises `CTraderConnectionError`
otherwise. Load failures (unknown symbols, protocol errors from `instrument_from_symbol`) are
recorded rather than raised, unless `fail_on_instrument_error` is set.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass

from nautilus_trader.common.component import Logger
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import CurrencyPair, Instrument

from nautilus_ctrader.common.account import CTraderAccountClient
from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderError,
    CTraderProtocolError,
)
from nautilus_ctrader.common.parsing import instrument_from_symbol
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om


class InstrumentLoadError(CTraderError):
    """An instrument, or a currency-conversion chain, could not be built or resolved."""


@dataclass(frozen=True)
class InstrumentLoadFailure:
    symbol: str
    reason: str


def _now_ns() -> int:
    return time.time_ns()


class CTraderInstrumentProvider(InstrumentProvider):
    """
    Builds Nautilus instruments from a connected `CTraderAccountClient`'s reference data.

    `asset_class_overrides` applies only to symbols that resolve to a `Cfd`; an override for a
    symbol that resolves to a `CurrencyPair` is ignored (with a WARNING), since a currency pair
    derives its class from its currencies.
    """

    def __init__(
        self,
        account: CTraderAccountClient,
        config: InstrumentProviderConfig,
        asset_class_overrides: Mapping[str, AssetClass],
        fail_on_instrument_error: bool,
        logger: Logger,
    ) -> None:
        super().__init__(config)
        self._account = account
        self._overrides = dict(asset_class_overrides)
        self._fail_on_instrument_error = fail_on_instrument_error
        self._log = logger
        self._failures: list[InstrumentLoadFailure] = []
        self._by_symbol_id: dict[int, Instrument] = {}

    @property
    def failures(self) -> tuple[InstrumentLoadFailure, ...]:
        return tuple(self._failures)

    def symbol_id(self, instrument_id: InstrumentId) -> int:
        instrument = self.find(instrument_id)
        if instrument is None:
            raise InstrumentLoadError(f"{instrument_id} not loaded")
        return instrument.info["symbol_id"]

    def instrument_for_symbol_id(self, symbol_id: int) -> Instrument | None:
        return self._by_symbol_id.get(symbol_id)

    async def load_all_async(self, filters: dict | None = None) -> None:
        self._require_connected()
        failures = await self._load_names(list(self._account.light_symbols))
        self._raise_if_configured(failures)

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        self._require_connected()
        if not instrument_ids:
            return
        failures: list[InstrumentLoadFailure] = []
        names: list[str] = []
        for instrument_id in instrument_ids:
            if instrument_id.venue != CTRADER_VENUE:
                failures.append(
                    self._record_failure(instrument_id.symbol.value, "not a CTRADER instrument"),
                )
                continue
            names.append(instrument_id.symbol.value)
        failures.extend(await self._load_names(names))
        self._raise_if_configured(failures)

    async def conversion_instruments_for(self, instrument: Instrument) -> list[Instrument]:
        """Chain instruments needed to convert `instrument`'s quote currency into the deposit
        currency (empty if they are already equal). Loads them (they are `CurrencyPair`s by
        the D2 rule). Raises `InstrumentLoadError` if the chain cannot be built.
        """
        self._require_connected()
        quote = instrument.quote_currency
        deposit = self._account.deposit_asset
        if quote.code == deposit.name:
            return []

        chain = await self._account.conversion_chain(
            self._asset_id_for_currency(quote.code),
            deposit.assetId,
        )
        if not chain:
            raise InstrumentLoadError(f"no conversion chain from {quote.code} to {deposit.name}")

        specs = await self._account.symbol_specs([leg.symbolId for leg in chain])
        instruments = []
        for leg in chain:
            spec = specs.get(leg.symbolId)
            if spec is None:
                raise InstrumentLoadError(f"{leg.symbolName}: no symbol spec returned by the venue")
            try:
                built = instrument_from_symbol(
                    spec,
                    leg,
                    self._account.assets,
                    self._overrides,
                    ts_init=_now_ns(),
                )
            except CTraderProtocolError as e:
                raise InstrumentLoadError(str(e)) from e
            self._finish_load(built, spec, leg.symbolName)
            instruments.append(built)
        return instruments

    async def reload(self, symbol_id: int) -> Instrument:
        """Refetch and rebuild the instrument for `symbol_id`, bypassing the spec cache.

        For `ProtoOASymbolChangedEvent`. Raises `InstrumentLoadError` if the symbol is no
        longer offered, or if it fails to build.
        """
        self._require_connected()
        light = next(
            (s for s in self._account.light_symbols.values() if s.symbolId == symbol_id),
            None,
        )
        if light is None:
            raise InstrumentLoadError(
                f"symbol id {symbol_id} not offered by the venue for this account",
            )

        specs = await self._account.symbol_specs([symbol_id], refresh=True)
        spec = specs.get(symbol_id)
        if spec is None:
            raise InstrumentLoadError(f"{light.symbolName}: no symbol spec returned by the venue")
        try:
            instrument = instrument_from_symbol(
                spec,
                light,
                self._account.assets,
                self._overrides,
                ts_init=_now_ns(),
            )
        except CTraderProtocolError as e:
            self._record_failure(light.symbolName, str(e))
            raise InstrumentLoadError(str(e)) from e

        self._finish_load(instrument, spec, light.symbolName)
        return instrument

    def _require_connected(self) -> None:
        if self._account.session is None:
            raise CTraderConnectionError("cannot load instruments: account is not connected")

    def _asset_id_for_currency(self, code: str) -> int:
        for asset_id, asset in self._account.assets.items():
            if asset.name == code:
                return asset_id
        raise InstrumentLoadError(f"no asset id for currency {code}")

    async def _load_names(self, names: list[str]) -> list[InstrumentLoadFailure]:
        if not names:
            return []
        failures: list[InstrumentLoadFailure] = []
        lights: dict[str, om.ProtoOALightSymbol] = {}
        for name in names:
            light = self._account.light_symbols.get(name)
            if light is None:
                failures.append(
                    self._record_failure(name, "not offered by the venue for this account"),
                )
            else:
                lights[name] = light

        specs = await self._account.symbol_specs([light.symbolId for light in lights.values()])

        for name, light in lights.items():
            spec = specs.get(light.symbolId)
            try:
                if spec is None:
                    raise CTraderProtocolError(f"{name}: no symbol spec returned by the venue")
                instrument = instrument_from_symbol(
                    spec,
                    light,
                    self._account.assets,
                    self._overrides,
                    ts_init=_now_ns(),
                )
            except CTraderProtocolError as e:
                failures.append(self._record_failure(name, str(e)))
                continue
            self._finish_load(instrument, spec, name)
        return failures

    def _finish_load(self, instrument: Instrument, spec: om.ProtoOASymbol, name: str) -> None:
        if isinstance(instrument, CurrencyPair) and name in self._overrides:
            self._log.warning(f"{name}: asset-class override ignored for a currency pair")
        if spec.tradingMode != om.ProtoOATradingMode.ENABLED:
            mode = om.ProtoOATradingMode.Name(spec.tradingMode)
            self._log.warning(f"{name} is loaded but not tradable ({mode})")
        self.add(instrument)
        self._by_symbol_id[instrument.info["symbol_id"]] = instrument

    def _record_failure(self, symbol: str, reason: str) -> InstrumentLoadFailure:
        failure = InstrumentLoadFailure(symbol=symbol, reason=reason)
        self._failures.append(failure)
        self._log.error(f"Instrument {symbol} not loaded: {reason}")
        return failure

    def _raise_if_configured(self, new_failures: list[InstrumentLoadFailure]) -> None:
        if self._fail_on_instrument_error and new_failures:
            raise InstrumentLoadError(f"{len(new_failures)} instrument(s) failed to load")
