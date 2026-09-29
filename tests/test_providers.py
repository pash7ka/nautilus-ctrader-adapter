"""Tests for `CTraderInstrumentProvider`: loading, failures, and conversion chains.

Everything runs against the fake server replaying `tests/fixtures/m2_recorded.json`, never
against a hand-built symbol spec.
"""

from __future__ import annotations

import asyncio

import pytest
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import Cfd, CurrencyPair

from nautilus_ctrader import providers
from nautilus_ctrader.common.errors import CTraderConnectionError
from nautilus_ctrader.constants import CTRADER_VENUE
from nautilus_ctrader.messages import OpenApiMessages_pb2 as oa
from nautilus_ctrader.messages import OpenApiModelMessages_pb2 as om
from nautilus_ctrader.providers import CTraderInstrumentProvider, InstrumentLoadError
from tests.account_venue import (
    RECORDED,
    HeldReplies,
    account_client,
    for_account,
    received,
    venue,
)
from tests.recording_logger import RecordingLogger

LIGHT = {s.symbolName: s for s in RECORDED["symbols"][0].symbol}
EURUSD_ID = InstrumentId(Symbol("EURUSD"), CTRADER_VENUE)
GER40_ID = InstrumentId(Symbol("GER40.cash"), CTRADER_VENUE)


def _provider(account, *, overrides=None, fail_on_instrument_error=False, logger=None):
    return CTraderInstrumentProvider(
        account,
        InstrumentProviderConfig(),
        overrides or {},
        fail_on_instrument_error,
        logger or RecordingLogger(),
    )


async def test_load_ids_loads_a_currency_pair_and_a_cfd() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)

        await provider.load_ids_async([EURUSD_ID, GER40_ID])

        by_id = {i.id: i for i in provider.list_all()}
        assert isinstance(by_id[EURUSD_ID], CurrencyPair)
        assert isinstance(by_id[GER40_ID], Cfd)
        assert not provider.failures
    finally:
        await client.disconnect()
        await server.stop()


async def test_unknown_symbol_is_a_recorded_failure() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    logger = RecordingLogger()
    try:
        await client.connect()
        provider = _provider(client, logger=logger)
        nope_id = InstrumentId(Symbol("NOPE"), CTRADER_VENUE)

        await provider.load_ids_async([nope_id])

        assert provider.find(nope_id) is None
        assert len(provider.failures) == 1
        assert provider.failures[0].symbol == "NOPE"
        assert provider.failures[0].reason == "not offered by the venue for this account"
        assert any("Instrument NOPE not loaded" in e for e in logger.errors())
    finally:
        await client.disconnect()
        await server.stop()


async def test_unknown_symbol_raises_when_configured_to_fail() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client, fail_on_instrument_error=True)
        nope_id = InstrumentId(Symbol("NOPE"), CTRADER_VENUE)

        with pytest.raises(InstrumentLoadError):
            await provider.load_ids_async([nope_id])

        assert len(provider.failures) == 1
        assert provider.failures[0].symbol == "NOPE"
    finally:
        await client.disconnect()
        await server.stop()


async def test_wrong_venue_is_a_recorded_failure() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        other_id = InstrumentId(Symbol("EURUSD"), Venue("OTHER"))

        await provider.load_ids_async([other_id])

        assert provider.failures[0].reason == "not a CTRADER instrument"
    finally:
        await client.disconnect()
        await server.stop()


async def test_conversion_instruments_for_a_cfd_returns_the_eur_usd_leg() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([GER40_ID])
        ger40 = provider.find(GER40_ID)

        chain = await provider.conversion_instruments_for(ger40)

        assert len(chain) == 1
        assert isinstance(chain[0], CurrencyPair)
        assert str(chain[0].id) == "EURUSD.CTRADER"
    finally:
        await client.disconnect()
        await server.stop()


async def test_conversion_instruments_for_the_deposit_currency_itself_is_empty() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([EURUSD_ID])
        eurusd = provider.find(EURUSD_ID)

        assert await provider.conversion_instruments_for(eurusd) == []
    finally:
        await client.disconnect()
        await server.stop()


async def test_conversion_instruments_for_raises_when_the_chain_is_empty() -> None:
    server = venue()
    server.on(
        om.PROTO_OA_SYMBOLS_FOR_CONVERSION_REQ,
        lambda r: oa.ProtoOASymbolsForConversionRes(ctidTraderAccountId=r.ctidTraderAccountId),
    )
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([GER40_ID])
        ger40 = provider.find(GER40_ID)

        with pytest.raises(InstrumentLoadError, match="no conversion chain from EUR to USD"):
            await provider.conversion_instruments_for(ger40)
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_broken_conversion_leg_is_recorded_as_a_failure() -> None:
    server = venue()
    eurusd_symbol_id = LIGHT["EURUSD"].symbolId

    def symbol_by_id(request: oa.ProtoOASymbolByIdReq) -> oa.ProtoOASymbolByIdRes:
        wanted = set(request.symbolId)
        symbols = []
        for spec in RECORDED["symbol_specs"][0].symbol:
            if spec.symbolId not in wanted:
                continue
            copy = om.ProtoOASymbol()
            copy.CopyFrom(spec)
            if copy.symbolId == eurusd_symbol_id:
                copy.digits = 6  # unrepresentable at the 1/100000 price scale
            symbols.append(copy)
        return oa.ProtoOASymbolByIdRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
            symbol=symbols,
        )

    server.on(om.PROTO_OA_SYMBOL_BY_ID_REQ, symbol_by_id)
    await server.start()
    client = account_client(server)
    logger = RecordingLogger()
    try:
        await client.connect()
        provider = _provider(client, logger=logger)
        await provider.load_ids_async([GER40_ID])
        ger40 = provider.find(GER40_ID)

        with pytest.raises(InstrumentLoadError):
            await provider.conversion_instruments_for(ger40)

        assert any(f.symbol == "EURUSD" for f in provider.failures)
        assert any("Instrument EURUSD not loaded" in e for e in logger.errors())
    finally:
        await client.disconnect()
        await server.stop()


async def test_conversion_instruments_for_is_cached() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([GER40_ID])
        ger40 = provider.find(GER40_ID)

        first = await provider.conversion_instruments_for(ger40)
        second = await provider.conversion_instruments_for(ger40)

        assert len(first) == 1 and first[0] is second[0]
        assert len(received(server, oa.ProtoOASymbolsForConversionReq)) == 1
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_per_symbol_reset_advances_the_generation_only_when_it_drops_a_chain() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([GER40_ID])
        await provider.conversion_instruments_for(provider.find(GER40_ID))
        generation = provider.conversion_generation

        assert not provider.reset_conversion_cache(LIGHT["GER40.cash"].symbolId)
        assert provider.conversion_generation == generation

        assert provider.reset_conversion_cache(LIGHT["EURUSD"].symbolId)
        assert provider.conversion_generation == generation + 1
    finally:
        await client.disconnect()
        await server.stop()


def test_a_full_reset_advances_the_generation_even_with_nothing_cached() -> None:
    provider = _provider(account_client(venue()))
    generation = provider.conversion_generation

    assert not provider.reset_conversion_cache()
    assert provider.conversion_generation == generation + 1


async def test_a_per_symbol_reset_during_a_chain_query_keeps_its_answer_out_of_the_cache() -> None:
    server = venue()
    held = HeldReplies(
        server,
        om.PROTO_OA_SYMBOLS_FOR_CONVERSION_REQ,
        lambda r: for_account(RECORDED["conversion_eur_usd"][0], r.ctidTraderAccountId),
    )
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([GER40_ID])
        ger40 = provider.find(GER40_ID)
        generation = provider.conversion_generation
        query = asyncio.create_task(provider.conversion_instruments_for(ger40))
        await held.arrived.wait()

        # Nothing is cached yet, but the answer in flight may already be stale.
        provider.reset_conversion_cache(LIGHT["EURUSD"].symbolId)
        await held.stop_holding()
        await query

        assert provider.conversion_generation != generation
        await provider.conversion_instruments_for(ger40)
        assert len(received(server, oa.ProtoOASymbolsForConversionReq)) == 2
    finally:
        await client.disconnect()
        await server.stop()


async def test_asset_class_override_ignored_for_a_currency_pair() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    logger = RecordingLogger()
    try:
        await client.connect()
        provider = _provider(
            client,
            overrides={"EURUSD": AssetClass.COMMODITY},
            logger=logger,
        )

        await provider.load_ids_async([EURUSD_ID])

        eurusd = provider.find(EURUSD_ID)
        assert isinstance(eurusd, CurrencyPair)
        assert logger.lines and any(level == "warning" for level, _ in logger.lines)
    finally:
        await client.disconnect()
        await server.stop()


async def test_close_only_mode_is_loaded_with_a_warning() -> None:
    server = venue()
    ger40_symbol_id = LIGHT["GER40.cash"].symbolId

    def symbol_by_id(request: oa.ProtoOASymbolByIdReq) -> oa.ProtoOASymbolByIdRes:
        wanted = set(request.symbolId)
        symbols = []
        for spec in RECORDED["symbol_specs"][0].symbol:
            if spec.symbolId not in wanted:
                continue
            copy = om.ProtoOASymbol()
            copy.CopyFrom(spec)
            if copy.symbolId == ger40_symbol_id:
                copy.tradingMode = om.ProtoOATradingMode.CLOSE_ONLY_MODE
            symbols.append(copy)
        return oa.ProtoOASymbolByIdRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
            symbol=symbols,
        )

    server.on(om.PROTO_OA_SYMBOL_BY_ID_REQ, symbol_by_id)
    await server.start()
    client = account_client(server)
    logger = RecordingLogger()
    try:
        await client.connect()
        provider = _provider(client, logger=logger)

        await provider.load_ids_async([GER40_ID])

        assert provider.find(GER40_ID) is not None
        assert any(
            level == "warning" and "GER40.cash is loaded but not tradable" in message
            for level, message in logger.lines
        )
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_symbol_without_a_step_volume_fails_alone() -> None:
    # Schema-legal but unusable: Nautilus refuses a zero size increment, and that refusal must
    # stay this symbol's failure rather than end the load every other symbol is part of.
    server = venue()
    ger40_symbol_id = LIGHT["GER40.cash"].symbolId

    def symbol_by_id(request: oa.ProtoOASymbolByIdReq) -> oa.ProtoOASymbolByIdRes:
        wanted = set(request.symbolId)
        symbols = []
        for spec in RECORDED["symbol_specs"][0].symbol:
            if spec.symbolId not in wanted:
                continue
            copy = om.ProtoOASymbol()
            copy.CopyFrom(spec)
            if copy.symbolId == ger40_symbol_id:
                # All four are optional in the schema; a symbol carrying none of them is legal.
                for field in ("stepVolume", "minVolume", "maxVolume", "lotSize"):
                    copy.ClearField(field)
            symbols.append(copy)
        return oa.ProtoOASymbolByIdRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
            symbol=symbols,
        )

    server.on(om.PROTO_OA_SYMBOL_BY_ID_REQ, symbol_by_id)
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)

        await provider.load_ids_async([GER40_ID, EURUSD_ID])

        assert provider.find(GER40_ID) is None
        assert [f.symbol for f in provider.failures] == ["GER40.cash"]
        assert provider.failures[0].reason == "GER40.cash: stepVolume is missing or zero"
        assert provider.find(EURUSD_ID) is not None  # the rest of the batch still loaded
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_value_nautilus_itself_refuses_fails_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The converter cannot know every rule Nautilus enforces on its own arguments, so whatever
    # it rejects has to be caught where an unbuildable symbol already is.
    server = venue()
    await server.start()
    client = account_client(server)
    build = providers.instrument_from_symbol

    def refuse_ger40(symbol, light, *args, **kwargs):
        if light.symbolName == "GER40.cash":
            raise ValueError("'size_increment' not a positive real")
        return build(symbol, light, *args, **kwargs)

    monkeypatch.setattr(providers, "instrument_from_symbol", refuse_ger40)
    try:
        await client.connect()
        provider = _provider(client)

        await provider.load_ids_async([GER40_ID, EURUSD_ID])

        assert [f.symbol for f in provider.failures] == ["GER40.cash"]
        assert provider.find(GER40_ID) is None
        assert provider.find(EURUSD_ID) is not None
    finally:
        await client.disconnect()
        await server.stop()


async def test_reload_bypasses_the_spec_cache() -> None:
    server = venue()
    ger40_symbol_id = LIGHT["GER40.cash"].symbolId
    calls = []

    def symbol_by_id(request: oa.ProtoOASymbolByIdReq) -> oa.ProtoOASymbolByIdRes:
        calls.append(list(request.symbolId))
        wanted = set(request.symbolId)
        symbols = []
        for spec in RECORDED["symbol_specs"][0].symbol:
            if spec.symbolId not in wanted:
                continue
            copy = om.ProtoOASymbol()
            copy.CopyFrom(spec)
            if copy.symbolId == ger40_symbol_id and len(calls) > 1:
                copy.tradingMode = om.ProtoOATradingMode.CLOSE_ONLY_MODE
            symbols.append(copy)
        return oa.ProtoOASymbolByIdRes(
            ctidTraderAccountId=request.ctidTraderAccountId,
            symbol=symbols,
        )

    server.on(om.PROTO_OA_SYMBOL_BY_ID_REQ, symbol_by_id)
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([GER40_ID])
        assert len(calls) == 1

        updated = await provider.reload(ger40_symbol_id)

        assert len(calls) == 2
        assert updated.info["trading_mode"] == "CLOSE_ONLY_MODE"
        assert provider.find(GER40_ID).info["trading_mode"] == "CLOSE_ONLY_MODE"
        assert provider.instrument_for_symbol_id(ger40_symbol_id) is updated
    finally:
        await client.disconnect()
        await server.stop()


async def test_symbol_id_round_trips_through_a_loaded_instrument() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([EURUSD_ID])

        assert provider.symbol_id(EURUSD_ID) == LIGHT["EURUSD"].symbolId
        assert provider.instrument_for_symbol_id(LIGHT["EURUSD"].symbolId).id == EURUSD_ID
        assert provider.instrument_for_symbol_id(999999) is None
    finally:
        await client.disconnect()
        await server.stop()


async def test_using_the_provider_before_connect_fails_clearly() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        provider = _provider(client)

        with pytest.raises(CTraderConnectionError):
            await provider.load_ids_async([EURUSD_ID])
    finally:
        await server.stop()


async def test_a_later_failure_for_the_same_symbol_replaces_the_earlier_one() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        nope_id = InstrumentId(Symbol("NOPE"), CTRADER_VENUE)
        wrong_venue_id = InstrumentId(Symbol("NOPE"), Venue("OTHER"))

        await provider.load_ids_async([nope_id])
        await provider.load_ids_async([wrong_venue_id])

        assert len(provider.failures) == 1
        assert provider.failures[0].reason == "not a CTRADER instrument"
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_dropped_instrument_is_not_rebuilt_as_a_conversion_leg() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([GER40_ID])
        provider.remove_failed(EURUSD_ID, "dropped by the caller")
        ger40 = provider.find(GER40_ID)

        with pytest.raises(InstrumentLoadError, match="not requested again"):
            await provider.conversion_instruments_for(ger40)

        assert provider.find(EURUSD_ID) is None
    finally:
        await client.disconnect()
        await server.stop()


async def test_a_full_conversion_cache_reset_lets_a_dropped_instrument_load_again() -> None:
    server = venue()
    await server.start()
    client = account_client(server)
    try:
        await client.connect()
        provider = _provider(client)
        await provider.load_ids_async([GER40_ID])
        provider.remove_failed(EURUSD_ID, "dropped by the caller")

        provider.reset_conversion_cache()

        chain = await provider.conversion_instruments_for(provider.find(GER40_ID))
        assert [i.id for i in chain] == [EURUSD_ID]
    finally:
        await client.disconnect()
        await server.stop()
