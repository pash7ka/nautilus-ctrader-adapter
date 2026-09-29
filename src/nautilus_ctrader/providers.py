"""`CTraderInstrumentProvider`: builds Nautilus instruments from broker symbol specs.

Requires `account` to already be connected; every method raises `CTraderConnectionError`
otherwise. Load failures (unknown symbols, protocol errors from `instrument_from_symbol`) are
recorded rather than raised, unless `fail_on_instrument_error` is set.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from nautilus_trader.common.component import Logger
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import CurrencyPair, Instrument

from nautilus_ctrader.common.errors import (
    CTraderConnectionError,
    CTraderError,
    CTraderProtocolError,
)
from nautilus_ctrader.common.parsing import instrument_from_symbol
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om

if TYPE_CHECKING:
    # The account client builds and owns its provider, so it imports this module.
    from nautilus_ctrader.common.account import CTraderAccountClient


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
        # Keyed by symbol name: a later failure for the same symbol replaces the earlier one.
        self._failures: dict[str, InstrumentLoadFailure] = {}
        self._by_symbol_id: dict[int, Instrument] = {}
        # Asset ids, not currency codes: the recorded asset table has both "BTC" and "Bitcoin",
        # so a name is not a safe key.
        self._quote_asset_id: dict[InstrumentId, int] = {}
        self._chains: dict[tuple[int, int], list[om.ProtoOALightSymbol]] = {}
        # Bumped by a reset that may make a resolved chain stale, so a chain fetched before one
        # is not written back after it.
        self._chain_generation = 0
        # Chain queries awaiting the venue: a per-symbol reset cannot tell whether their answers
        # use the symbol, so it treats them as stale.
        self._chain_queries = 0
        # Symbol names dropped by `remove_failed`: refused until explicitly requested again,
        # so a chain or a reload cannot quietly resurrect an instrument that must not trade.
        self._blocked: set[str] = set()

    @property
    def failures(self) -> tuple[InstrumentLoadFailure, ...]:
        return tuple(self._failures.values())

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
        # An explicit request is the one thing that clears a block.
        self._blocked.difference_update(names)
        failures.extend(await self._load_names(names))
        self._raise_if_configured(failures)

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        """Inherited unchanged from `InstrumentProvider`: delegates to `load_ids_async`."""
        await super().load_async(instrument_id, filters)

    async def conversion_instruments_for(self, instrument: Instrument) -> list[Instrument]:
        """Chain instruments needed to convert `instrument`'s quote currency into the deposit
        currency (empty if they are already equal). Loads them (both legs are currencies, so
        they build as `CurrencyPair`s, the instrument kind `Cache.get_xrate` reads rates from).
        The chain is cached per `(quote_asset_id, deposit_asset_id)`, and a leg already loaded
        is reused rather than rebuilt. Raises `InstrumentLoadError` if the chain cannot be
        built, or `instrument` was not loaded by this provider.
        """
        self._require_connected()
        quote_asset_id = self._quote_asset_id.get(instrument.id)
        if quote_asset_id is None:
            raise InstrumentLoadError(f"{instrument.id}: not loaded by this provider")
        deposit_asset_id = self._account.deposit_asset.assetId
        if quote_asset_id == deposit_asset_id:
            return []

        cache_key = (quote_asset_id, deposit_asset_id)
        chain = self._chains.get(cache_key)
        if chain is None:
            generation = self._chain_generation
            self._chain_queries += 1
            try:
                chain = await self._account.conversion_chain(quote_asset_id, deposit_asset_id)
            finally:
                self._chain_queries -= 1
            if not chain:
                raise InstrumentLoadError(
                    f"no conversion chain from {instrument.quote_currency.code} to "
                    f"{self._account.deposit_asset.name}",
                )
            if generation == self._chain_generation:
                self._chains[cache_key] = chain

        missing = [leg for leg in chain if self.instrument_for_symbol_id(leg.symbolId) is None]
        specs = (
            await self._account.symbol_specs([leg.symbolId for leg in missing]) if missing else {}
        )

        instruments = []
        for leg in chain:
            existing = self.instrument_for_symbol_id(leg.symbolId)
            if existing is not None:
                instruments.append(existing)
            else:
                instruments.append(self._build_or_fail(specs.get(leg.symbolId), leg))
        return instruments

    @property
    def conversion_generation(self) -> int:
        """Bumped by every `reset_conversion_cache()` that may have made a resolved chain stale.

        That is every full reset, and a per-symbol one that dropped a chain or overlapped a
        chain query. A caller that resolves a chain across an await reads this first and
        compares after, to tell whether what it resolved is still current.
        """
        return self._chain_generation

    def reset_conversion_cache(self, symbol_id: int | None = None) -> bool:
        """Drop cached conversion chains, keeping the loaded instruments; returns whether any went.

        `symbol_id` limits it to chains that use that symbol. A chain is venue data that the
        broker can change, so the caller decides how long to trust one.

        Advances `conversion_generation` unless the per-symbol form provably changed nothing:
        it dropped no chain and no chain query was awaiting the venue.

        The full form also clears the instruments blocked by `remove_failed`, since it marks a
        fresh start; the per-symbol form deliberately keeps them, because one symbol changing
        at the venue says nothing about why another was dropped.
        """
        stale = [
            key
            for key, chain in self._chains.items()
            if symbol_id is None or any(leg.symbolId == symbol_id for leg in chain)
        ]
        for key in stale:
            del self._chains[key]
        if symbol_id is None or stale or self._chain_queries:
            self._chain_generation += 1
        if symbol_id is None:
            # A full reset is a fresh start for the connection, so a dropped instrument gets
            # another chance to load.
            self._blocked.clear()
        return bool(stale)

    def remove_failed(self, instrument_id: InstrumentId, reason: str) -> None:
        """Unload an instrument that must not be traded, recording it as a load failure.

        For a failure the loader itself cannot see, such as a conversion chain that cannot be
        resolved for an instrument that otherwise built fine.
        """
        self._blocked.add(instrument_id.symbol.value)
        instrument = self._instruments.pop(instrument_id, None)
        if instrument is not None:
            self._by_symbol_id.pop(instrument.info["symbol_id"], None)
            self._quote_asset_id.pop(instrument_id, None)
        self._record_failure(instrument_id.symbol.value, reason)

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
        return self._build_or_fail(specs.get(symbol_id), light)

    def _require_connected(self) -> None:
        if self._account.session is None:
            raise CTraderConnectionError("cannot load instruments: account is not connected")

    def _build_or_fail(
        self,
        spec: om.ProtoOASymbol | None,
        light: om.ProtoOALightSymbol,
    ) -> Instrument:
        """Build the instrument for `light`/`spec`, or record the failure and raise.

        Every place that can fail to build an instrument routes through here, so a failure is
        never raised without first landing in `failures` with an ERROR line.
        """
        name = light.symbolName
        if name in self._blocked:
            # Already in `failures` from the drop that blocked it, so no second ERROR line.
            raise InstrumentLoadError(f"{name}: dropped earlier and not requested again")
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
        except (CTraderProtocolError, ValueError, TypeError) as e:
            # Nautilus validates the constructor arguments itself and reports what it rejects
            # as a `ValueError` or a `TypeError`. Caught here so one unusable symbol is a
            # recorded failure like any other, never the end of the batch it arrived in.
            self._record_failure(name, str(e))
            raise InstrumentLoadError(str(e)) from e
        self._finish_load(instrument, spec, light)
        return instrument

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

        for light in lights.values():
            try:
                self._build_or_fail(specs.get(light.symbolId), light)
            except InstrumentLoadError:
                failures.append(self._failures[light.symbolName])
        return failures

    def _finish_load(
        self,
        instrument: Instrument,
        spec: om.ProtoOASymbol,
        light: om.ProtoOALightSymbol,
    ) -> None:
        name = light.symbolName
        if isinstance(instrument, CurrencyPair) and name in self._overrides:
            self._log.warning(f"{name}: asset-class override ignored for a currency pair")
        if spec.tradingMode != om.ProtoOATradingMode.ENABLED:
            mode = om.ProtoOATradingMode.Name(spec.tradingMode)
            self._log.warning(f"{name} is loaded but not tradable ({mode})")
        self.add(instrument)
        self._by_symbol_id[instrument.info["symbol_id"]] = instrument
        self._quote_asset_id[instrument.id] = light.quoteAssetId

    def _record_failure(self, symbol: str, reason: str) -> InstrumentLoadFailure:
        failure = InstrumentLoadFailure(symbol=symbol, reason=reason)
        self._failures[symbol] = failure
        self._log.error(f"Instrument {symbol} not loaded: {reason}")
        return failure

    def _raise_if_configured(self, new_failures: list[InstrumentLoadFailure]) -> None:
        if self._fail_on_instrument_error and new_failures:
            raise InstrumentLoadError(f"{len(new_failures)} instrument(s) failed to load")
