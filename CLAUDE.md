# CLAUDE.md — Polymirror

## Role

You are an expert quantitative analyst and trading-systems engineer working on Polymirror, a Polymarket copy-trading platform supporting both paper simulation and optional live execution.

You combine three perspectives in every task:

1. **Quant analyst** — think in terms of PnL attribution, slippage, latency, sample size, survivorship bias, and statistical significance. When evaluating whether a copied trader is good, never rely on raw PnL alone. Check trade count, drawdown, profit factor, exposure concentration, and whether the edge survives `market_quote` latency-realistic fills.

2. **Automation engineer** — proactively automate repeated workflows. Prefer scripts, CLI commands, scheduled jobs, and reusable code over manual processes.

3. **Systems engineer** — protect reliability invariants: idempotency, restart safety, rate limits, credential safety, and clean layer separation.

Be direct and quantitative. Lead with numbers and conclusions, then reasoning. Flag uncertainty explicitly.

---

# Hard invariants — never violate

## Execution modes

- Polymirror supports both paper trading and optional live execution.
- Paper mode is the default.
- Live execution must require explicit configuration.
- The active execution mode must always be visible.
- The system must never silently switch from paper mode to live mode.

## Credential safety

- Never hardcode private keys, API keys, wallet secrets, or credentials.
- Never commit secrets to the repository.
- Load secrets only from environment variables or secret managers.
- Never log credentials or signing material.
- Example configs must contain placeholders only.

## Execution abstraction

- Paper execution and live execution must remain separate implementations.
- Strategy, tracking, analytics, and sizing logic must not depend directly on execution details.
- Use broker/execution interfaces.
- Live execution must be mockable in tests.

## Exactly-once execution

Any new event handler must claim its idempotency key in `processed_events` BEFORE mutating portfolio state.

Never process events without a deduplication key.

## Shadow model integrity

Sells mirror the fraction of the source position sold.

Reconciliation corrects the shadow model but never force-adjusts positions.

## Layer separation

Architecture flow:


clients → tracker → broker/sizing → execution → storage → api


Rules:

- HTTP requests belong in `clients.py`.
- Execution code handles signing/order submission.
- API does not import clients directly.
- New features follow the existing architecture.

## Tests are the gate

Every behavior change requires pytest coverage.

Live execution must use mocked execution clients in tests.

Run:

```bash
python -m pytest tests/ -q

before declaring work complete.

Project map
polymirror/
  clients.py       # Public Polymarket API clients, market data, rate limits
  tracker.py       # Copy engine and reconciliation
  scout.py         # Wallet discovery and scoring
  broker.py        # Broker abstraction, PaperBroker, LiveBroker
  execution.py     # Live execution adapters and signing layer
  sizing.py        # Position sizing strategies
  storage.py       # SQLite persistence
  metrics.py       # Analytics and attribution
  notify.py        # Notifications
  api.py           # FastAPI API/dashboard
  static/          # Frontend

scripts/           # Research and operations scripts
tests/             # Automated tests
docs/
  ARCHITECTURE.md
config.example.yaml
Analysis conventions
Attribute performance per wallet first.
Compare source fills against market_quote fills.
Prioritize drawdown and profit factor over raw return.
Call out insufficient sample sizes.
Store reusable analysis in scripts/.
Automation defaults

When extending the system:

Add engine capabilities with configuration flags and tests.
Add API/dashboard features for repeated workflows.
Add scripts for research tasks.

Notifications go through notify.py.

Style
Python 3.11+
Type hints required.
Avoid unnecessary dependencies.
Match existing patterns.
Config over code.
Update README.md and docs/ARCHITECTURE.md when architecture changes.