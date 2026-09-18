# CLAUDE.md

Working rules for AI assistants in this repository.

`nautilus-ctrader-adapter` is a **cTrader Open API adapter for NautilusTrader**: a transport
and translation layer between the cTrader protobuf protocol and the Nautilus live client
interfaces. See [README.md](README.md) for what it provides.

The repository is **public and MIT-licensed**. Everything committed here is read by
strangers, including Spotware reviewers and other Nautilus users.

## 1. The boundary rule — the one rule that overrides everything

**This adapter knows the wire protocol and the Nautilus client interfaces. Nothing else.**

It must never learn anything about *what* is traded or *when*. Forbidden here, in code,
comments, tests, docs, fixtures, config examples and commit messages:

- strategies, signals, entry/exit rules, indicators, technical analysis;
- backtesting, historical research, parameter tuning, performance reports;
- risk policies and prop-firm rules (daily loss limits, session windows, leverage caps);
- notification channels (Telegram, email, chat), dashboards;
- deployment and orchestration (Docker, Compose, Redis, supervisors, process managers);
- the name of any specific broker, prop firm, strategy or downstream project.

All of that lives in the application that *uses* the adapter. If a change would be easier
with one of these leaking in, the design is wrong — fix the interface instead.

**Self-check before committing:** if a diff contains a strategy name, an indicator name, a
prop-firm name, or a broker name in anything but a neutral list of examples, the boundary is
broken. Revert it.

Concrete consequences:

- The adapter never decides whether an order *should* be sent. It sends what Nautilus asks
  for, or rejects it with a protocol-level reason.
- The adapter never reads application config files. It takes a typed config object.
- Instrument definitions come from the broker, not from a table in this repo.

In cTrader, stop-loss and take-profit are attributes of a position rather than standalone
orders. Translating between that model and Nautilus orders is protocol work and belongs here;
it is done **literally**, without guessing what the strategy intends:

- a bracket (entry, stop, take-profit) goes out as one order carrying both levels; the stop and
  take-profit legs are accepted once the position exists with them;
- cancelling a leg removes that level from the position at the broker and is reported as
  `OrderCanceled`, or `OrderCancelRejected` if the broker refuses;
- when a position closes, for any reason, its remaining legs are reported as `OrderCanceled`;
- what the venue model cannot express (a second stop on one position, a protective stop with no
  position to attach to) is rejected with a clear reason.

Keeping an open position protected is the strategy's job: close first, then clean up the
remaining legs. The adapter never refuses or ignores a cancel to keep a stop in place; that
would break the Nautilus contract for every other strategy.

Whether the account is hedging or netting is read from the broker and mapped to `OmsType`,
never assumed.

## 2. Language

**Everything written in this repository is in English** — code, comments, docstrings, docs,
tests, commit messages, PR descriptions, and the gitignored `CLAUDE.local.md` alike. The
repository is public, and a single language keeps notes movable between local and committed
files without a translation step.

Conversation with the user happens in whatever language they use.

## 3. Stack and tooling

- **Python 3.13+**, dependencies managed with `uv` (`uv sync`, `uv run ...`).
- **nautilus-trader >= 1.228** — the target platform.
- **pytest** (+ `pytest-asyncio`) for tests, **ruff** for lint and format.
- **asyncio only.** No Twisted, no threads for protocol work. The whole reason for a custom
  transport is that the official SDK is Twisted-based and Nautilus is asyncio.

Do not add a dependency without a reason that survives being written down. Every runtime
dependency of this package becomes a constraint on every user's environment.

## 4. Protobuf definitions

Message definitions come from Spotware's MIT-licensed
[openapi-proto-messages](https://github.com/spotware/openapi-proto-messages). Python bindings
are generated from the `.proto` files with our own `protoc`/`protobuf` version.

**Do not depend on the `ctrader-open-api` PyPI package at runtime.** It pins
`Twisted==24.3.0`, `protobuf==3.20.1` and `requests==2.32.3` exactly; those pins are
unsatisfiable alongside a current Nautilus stack, and its Twisted reactor is the thing we are
deliberately avoiding. Reusing the *protocol definitions* is correct; taking the *package* is
not.

Generated `*_pb2.py` files are build output. Never hand-edit them; regenerate instead with
`scripts/gen_protobuf.py`, which also rewrites the two bare imports protoc emits into
package-absolute ones. Its protobuf version check and the `grpcio-tools` pin in
`pyproject.toml` move together.

## 5. Layout

Mirror the module names Nautilus uses for its own adapters, so the package reads as familiar
to anyone who has looked at `nautilus_trader/adapters/<venue>/`:

| Path | Contents |
|---|---|
| `src/nautilus_ctrader/config.py` | client config classes |
| `src/nautilus_ctrader/constants.py` | venue name, endpoints, protocol constants |
| `src/nautilus_ctrader/enums.py` | protocol enums and their Nautilus mappings |
| `src/nautilus_ctrader/providers.py` | `CTraderInstrumentProvider` |
| `src/nautilus_ctrader/data.py` | `CTraderDataClient` |
| `src/nautilus_ctrader/execution.py` | `CTraderExecutionClient` |
| `src/nautilus_ctrader/common/` | transport, auth, parsing and conversion helpers |
| `tests/` | unit tests, fixtures, fake server |
| `docs/` | long-lived documentation |

`nautilus_trader/adapters/_template/` in the installed package is the reference skeleton for
what a client must implement; `bybit` and `deribit` are good worked examples. Read them
before inventing a structure.

Structure is a guideline. Do not create empty directories ahead of the code that fills them.

## 6. Correctness rules specific to this protocol

These are the places where a plausible-looking implementation is silently wrong. Treat them
as requiring evidence, not reasoning:

- **Scaling.** Volumes are not lots; monetary values carry `moneyDigits`; trendbar prices are
  integers encoded as deltas from `low`. A wrong factor produces an order a hundred times too
  large. Every converter needs a test against a *recorded real response*, not a hand-built
  message.
- **Framing and limits.** Exact length-prefix width, byte order and rate-limit budgets are
  verified against the real endpoint, then frozen into a fixture. Do not treat a number
  written in prose — including in this file — as verified.
- **Reconnect.** After reconnect, both authentication levels and every subscription must be
  re-established. This path is only ever exercised by the fake server, so it must be.
- **Order model translation.** A Nautilus bracket is three orders; in cTrader the protective
  levels are fields on a position. The mapping is asymmetric in both directions and is where
  state-machine bugs will live.

When an implementation detail is not yet confirmed against a live endpoint, mark it in the
code with a `TODO(verify):` comment saying what would confirm it. Do not quietly promote an
assumption to a fact.

## 7. Testing

- Tests must run offline. Fixtures are recorded protobuf payloads; the fake server replays
  them over the real framing.
- Tests requiring a broker connection are marked and skipped by default, and never run in CI.
- **Fixtures are scrubbed.** No real account ids, tokens, client ids, balances or personal
  identifiers in committed test data.
- New protocol handling arrives with a fixture. "It parsed in a manual run" is not coverage.

## 8. Secrets and logging

- `clientId`, `clientSecret`, access and refresh tokens, and account identifiers never appear
  in the repository, in test fixtures, in log output, or in error messages.
- The transport layer never logs payload bytes, only payload type, correlation id and
  length. That one structural guarantee replaces masking, so it cannot be forgotten at a call
  site.
- Use the Nautilus `Logger`. Choose the level by the reaction required, not by how
  interesting the event feels: **ERROR** = a human must intervene; **WARNING** = unusual but
  handled (successful reconnect, retry); **INFO** = normal operation worth keeping;
  **DEBUG** = incident forensics (per-message envelopes, rate-limiter state). No TRACE.

## 9. Docs

`docs/` holds long-lived documentation about this adapter: protocol notes, design decisions,
mapping tables. It is written for an outside reader.

Never reference private or downstream repositories from committed content — no paths, no
document names, no "see the design spec". If a decision needs justifying, write the
justification here in this repo's own words.

## 10. Working style

- Before writing or editing commented code, follow the user's `code-comments` skill.
- Prefer reading the Nautilus adapter sources over guessing at an interface. They are
  installed and available locally.
- When the protocol is ambiguous and no fixture settles it, say so and ask — do not pick a
  plausible interpretation and build on it silently.
- Local paths, credential locations and machine-specific notes belong in `CLAUDE.local.md`
  (gitignored), never here.
