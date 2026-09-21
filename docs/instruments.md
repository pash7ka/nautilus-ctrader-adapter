# Instruments: how a cTrader symbol becomes a Nautilus instrument

`CTraderInstrumentProvider` turns the broker's own symbol specifications into Nautilus
`Instrument` objects. It invents nothing: every field below comes from the venue, and the
adapter only rescales and renames. This document is the mapping table an outside reader needs
to know what they will get.

## Identifier

The broker's symbol name is used verbatim:

```
InstrumentId(Symbol(<broker symbol name>), Venue("CTRADER"))
```

so `EURUSD` becomes `EURUSD.CTRADER` and `GER40.cash` becomes `GER40.cash.CTRADER`.
`raw_symbol` is the same name again. Symbol names are broker-specific — two brokers may spell
the same underlying differently — and mapping them onto application-side names is the
application's job, not the adapter's.

Instruments are loaded through the standard `InstrumentProviderConfig`: `load_ids` for a named
set, `load_all` for everything the account is offered. Full specifications are fetched in
batches; a symbol the account is not offered is a load failure (see
[Load failures](#load-failures)).

## `CurrencyPair` or `Cfd`

A symbol becomes a **`CurrencyPair`** when *both* its base and its quote asset name resolve,
through a strict Nautilus currency lookup, to a fiat or crypto currency. Every other symbol
becomes a **`Cfd`**.

The rule is not cosmetic. Nautilus builds exchange rates in `Cache.get_xrate` from
`CurrencyPair` and `CryptoPerpetual` instruments only; a `Cfd` is never used as a conversion
leg, whatever its `base_currency` says. A symbol whose two sides are both currencies must
therefore be a `CurrencyPair`, or every conversion that would have gone through it silently
stops working — including the account-currency conversion the data client sets up for you.

Only *strict* currency lookups are used. The non-strict form registers an unknown code as a
new global currency, so one junk asset name from the venue would permanently pollute the
process-wide currency table.

Consequences worth knowing:

- FX pairs and crypto pairs are `CurrencyPair`s.
- Metals (`XAU`, `XAG`, `XPT`) are `Cfd`s with the metal as `base_currency`: Nautilus knows
  them as commodity-backed currencies, not as fiat or crypto.
- Indices, energies and single stocks are `Cfd`s with `base_currency = None`, because their
  base asset name is not a currency at all.
- The quote asset *must* resolve to a known currency. If it does not, the instrument is not
  built — there would be no currency to price it in.

## Field mapping

cTrader volumes are integers in **cents of a unit**: the unit amount is the wire value divided
by 100. "Units" below always means that divided value.

| Nautilus field | Source |
|---|---|
| `instrument_id`, `raw_symbol` | `symbolName` |
| `price_precision` | `digits` |
| `price_increment` | `10^-digits` |
| `size_increment` | `stepVolume` in units |
| `size_precision` | the number of decimals `stepVolume` in units needs |
| `min_quantity`, `max_quantity` | `minVolume`, `maxVolume` in units |
| `lot_size` | `lotSize` in units |
| `quote_currency` | the asset name of `quoteAssetId` |
| `base_currency` | the asset name of `baseAssetId` when it is a known currency, otherwise `None` (`CurrencyPair` always has one) |
| `asset_class` | `Cfd` only; see [Asset class](#asset-class) |
| `maker_fee`, `taker_fee` | `0` — the adapter charges nothing and models nothing; the application sets its own fee model |
| `info` | the raw venue fields, see [`info`](#info) |

`digits` above 5 is refused: prices arrive scaled by 1/100000, so a sixth decimal could not be
represented exactly. A `minVolume`, `maxVolume` or `lotSize` that is finer than `stepVolume` is
refused as well, rather than rounded — rounding a minimum size down to zero is the kind of
silent error this adapter would rather not survive.

### Worked example: a EUR-quoted index CFD on a USD account

An index CFD such as `GER40.cash`, quoted in EUR, on an account whose deposit currency is USD.
The venue reports `digits = 2`, `pipPosition = 0`, `lotSize = 100`, `stepVolume = 1`,
`minVolume = 1`, `maxVolume = 100000`, `measurementUnits = "Contracts"`. That becomes:

| Field | Value | From |
|---|---|---|
| `instrument_id` | `GER40.cash.CTRADER` | the symbol name |
| type | `Cfd` | the base asset is an index, not a currency |
| `quote_currency` | `EUR` | `quoteAssetId` |
| `base_currency` | `None` | the base asset name is not a currency |
| `price_precision` / `price_increment` | `2` / `0.01` | `digits` |
| `size_increment` / `size_precision` | `0.01` / `2` | `stepVolume` 1 cent = 0.01 contracts |
| `min_quantity` / `max_quantity` | `0.01` / `1000` | `minVolume`, `maxVolume` in units |
| `lot_size` | `1` | `lotSize` 100 cents = 1 contract |

So a quantity of `1` on this instrument is one contract, and the smallest tradable size is a
hundredth of one.

The account is in USD and the instrument is quoted in EUR, so nothing about it can be valued
until EUR can be converted to USD. With `pipPosition = 0` a pip is one whole price unit, and
one contract moving one pip is worth 1 EUR — that is, 1 EUR × EURUSD in the account currency.
This is why the data client subscribes conversion symbols nobody asked for; see
[Market data](market_data.md).

The adapter does not compute a pip value. `pip_position` is passed through in `info` for an
application that wants to.

## Asset class

`CurrencyPair` derives its own asset class from its currencies (FX, or cryptocurrency if
either side is crypto) and takes no asset class from the adapter. `Cfd` takes one explicitly,
so only a `Cfd` can be given one here.

The default is derived from the asset *names*:

| Condition on the base asset name | `AssetClass` |
|---|---|
| `XAU`, `XAG`, `XPT`, `XPD` | `COMMODITY` |
| a known crypto currency code | `CRYPTOCURRENCY` |
| anything else | `ALTERNATIVE` |

Every symbol on the venue carries the same symbol category, so there is nothing in the
protocol to classify from beyond the asset names. `ALTERNATIVE` is the deliberate fallback: a
visibly generic value that tells the reader "unclassified" is safer than a plausible guess that
is wrong.

Indices, energies and single stocks therefore all land on `ALTERNATIVE`. The Nautilus asset
classes are `FX`, `EQUITY`, `COMMODITY`, `DEBT`, `INDEX`, `CRYPTOCURRENCY` and `ALTERNATIVE`;
an application that wants a symbol classified as one of them says so:

```python
CTraderDataClientConfig(
    ...,
    asset_class_overrides={"US100.cash": "INDEX", "GER40.cash": "INDEX"},
)
```

Keys are broker symbol names; values are Nautilus `AssetClass` member names, and a name that
is not one raises `ValueError` when the client is built, not on the first load. An override for
a symbol that resolves to a `CurrencyPair` is ignored, with a WARNING naming the symbol.

## `info`

Every instrument carries the venue's own fields in `info`, as flat scalars that survive a
round trip through `to_dict`/`from_dict`. They are passed through unread: the adapter does not
price commission, swap or margin, and an application doing venue-parameter reconciliation needs
the raw values rather than the adapter's interpretation of them.

| Key | Meaning |
|---|---|
| `symbol_id` | the venue's numeric symbol id |
| `lot_size_cents`, `min_volume_cents`, `step_volume_cents`, `max_volume_cents` | the unscaled volume fields, in cents of a unit |
| `pip_position` | decimal position of a pip for this symbol |
| `measurement_units` | the venue's free-text unit name, e.g. `"Contracts"` |
| `trading_mode` | the venue's trading-mode name |
| `schedule_time_zone` | the time zone the symbol's trading schedule is expressed in |
| `commission_type`, `precise_trading_commission_rate`, `precise_min_commission` | the venue's commission parameters |
| `swap_long`, `swap_short`, `swap_calculation_type` | the venue's swap parameters |
| `leverage_id` | the leverage profile the symbol belongs to |
| `sl_distance`, `tp_distance`, `distance_set_in` | the venue's minimum protective-level distances, and the unit they are expressed in |

A field the venue did not send is `None`.

## Sizes are not rounded for you

Nautilus rounds an order quantity to `size_precision`, but it does not enforce
`size_increment`. On a symbol whose step is 1000 units, `size_precision` is 0 and a quantity of
`1234` is a perfectly valid `Quantity` that the venue will reject. **Rounding order sizes to
`size_increment` is the application's job.** The adapter reports the step faithfully and does
not second-guess a size it is asked to send.

## Load failures

An instrument that cannot be built correctly is not loaded. Each failure is logged at ERROR
with the symbol name and the reason, and is available afterwards:

```python
for failure in provider.failures:
    print(failure.symbol, failure.reason)
```

An instrument that loads but is not tradable right now (`tradingMode` other than `ENABLED`) is
a WARNING, not a failure: it is still a correct instrument.

The adapter does not decide what a failure means. By default the rest of the instruments load
and the client carries on without the broken one; with `fail_on_instrument_error=True` a
failure raises instead, so bringing up the node fails rather than starting with a gap. Which of
those two is right depends on what the application trades, and that is not something a
transport layer can know.

## Symbol changes

When the venue announces that a symbol changed, the adapter logs a WARNING naming it, re-fetches
its specification and republishes the rebuilt instrument through the data engine. Cached
currency-conversion chains that use the symbol are dropped and resolved again. The new values
are published as they are; judging whether a changed contract size or minimum distance is
acceptable is the application's decision.
