# Polymirror — Polymarket Copy-Trading Platform

Polymirror watches selected Polymarket wallets and mirrors their activity through a configurable execution system.

It supports two modes:

- Paper mode: simulated execution, enabled by default.
- Live mode: optional execution through isolated execution components.

Live mode requires explicit configuration and secure credential handling. Secrets are never stored in the repository.

## What it does

You give it one or more wallet addresses (any address visible on a Polymarket profile URL or leaderboard) — **or let the wallet scout find them for you**. The engine polls each followed wallet's public activity feed, detects new buys, sells, partial exits, full exits and redemptions, and replays them against a virtual balance using your chosen sizing rule. A local dashboard shows equity, open positions, allocation, copied trades, per-wallet performance attribution, scout candidates and a live activity log.

With the scout and auto-manage enabled (both are by default), the loop is fully hands-off: the scout scans the public leaderboard on a schedule, evaluates each candidate's trade history against quantitative criteria, follows the best ones automatically, and auto-manage pauses any followed wallet whose *copied* results breach the risk thresholds — freeing the slot for the next candidate. Start `python run.py` and walk away.

## Quick start

Requires Python 3.11+.

```bash
git clone <this project> polymirror && cd polymirror
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp config.example.yaml config.yaml                    # optional — defaults work
python run.py
```

Open http://127.0.0.1:8400, paste a wallet address (e.g. from a leaderboard profile, `polymarket.com/profile/0x…`), pick a sizing mode and click **Follow**. New trades by that wallet will start appearing as simulated fills within one poll cycle of being indexed.

Run the tests with:

```bash
python -m pytest tests/ -q
```

## Configuration

Everything lives in `config.yaml` (see `config.example.yaml` for the annotated full set). The settings you will most likely touch:

| Key | Default | Meaning |
|---|---|---|
| `starting_balance` | `10000` | Virtual USD the simulated account starts with. |
| `poll_interval_seconds` | `15` | How often each followed wallet's activity is polled. |
| `backfill_seconds` | `0` | Set to e.g. `86400` to replay the last 24h of a wallet's trades when you first follow it. |
| `execution.price_mode` | `source_price` | `source_price` fills your copy at the price the source got (plus slippage); `market_quote` re-quotes the current CLOB best price at copy time — more realistic given polling latency. |
| `execution.slippage_bps` | `20` | Applied against you on every simulated fill (buys fill higher, sells lower). |
| `execution.fee_bps` | `0` | Polymarket charges no trading fee today; raise this to stress-test results. |
| `default_sizing` | `percent_balance / 0.05` | Sizing applied to wallets added without explicit settings. |

### Sizing modes (per wallet, editable via API or when adding)

**`proportional`** — copy the same fraction of portfolio the source used. If a trader with a $500k book bets $5k (1%), your $10k sim account bets $100. This is the mode that most faithfully reproduces the source's risk profile. Requires the source's portfolio value, fetched from the official `/value` endpoint and cached for 5 minutes.

**`fixed_usd`** — every copied buy uses the same notional (`sizing_value` dollars).

**`percent_balance`** — every copied buy uses `sizing_value` (a fraction, e.g. `0.05`) of currently available cash.

Each wallet also has a `multiplier` risk dial applied on top (0.5 = half size, 2 = double). **Sells are never sized by these modes**: exits always mirror the *fraction* of the source's position being sold (they sold 40% → you sell 40% of your copy), which keeps your book structurally synchronized with theirs regardless of entry sizing.

## Stopping trades

Three layers of stop control, all leaving history and open positions intact:

1. **Global stop button** — the header of the dashboard has a `⏸ STOP COPYING` button (also `POST /api/trading {"copying": false}`). While halted, **new buys are blocked but exits keep mirroring**, so risk can still come off. State is persisted — it survives restarts.
2. **Automatic portfolio halts** (`portfolio:` config) — copying halts itself when today's equity loss exceeds `max_daily_loss_pct` (default 10% of starting balance) or the peak-to-trough equity drawdown exceeds `max_total_drawdown_pct` (default 15%, gated by `stop_copy_after_drawdown`). These are deliberately independent rules, not two views of one thing: the daily-loss check is judged against a *fixed* reference (starting balance) over *today only*, so it catches one bad day fast; the drawdown check is judged against a *floating* reference (the all-time peak) over *all time*, so it catches a slow multi-day bleed that no single day is large enough to trip. Neither one subsumes the other — see `PortfolioConfig`'s docstring in `config.py` for a worked example. Both checks (and the `max_drawdown_pct` dashboard stat) are deposit/withdrawal-neutral: a withdrawal can never look like a trading loss and trigger a false halt, and a deposit can never mask a real one — every comparison is against *trading-adjusted* equity (capital moves backed out), not raw cash. If you manually resume, it will only re-halt when equity falls *below the level at which the last halt fired* — new losses, never the same stale breach.
3. **Per-wallet pause** — the Pause button / `PATCH /api/wallets/{addr} {"active": false}`, plus auto-manage below.

Per-trade guards also run before every copied buy: `portfolio.max_open_positions`, `portfolio.max_single_market_pct` (projected exposure per market vs equity), the `copy_filters` entry-price band (`min_entry_price`–`max_entry_price`), the near-resolution filter, a market-liquidity floor (`copy_filters.min_market_liquidity_usd`, via cached Gamma metadata; unknown liquidity passes), and the `copy_buys` / `copy_sells` master switches. Every skipped trade is logged with its reason.

## Automatic wallet discovery (the scout)

The scout closes the loop between "who is worth copying?" and "copy them": no manual wallet entry is needed. Every `scout.interval_seconds` (default 6h) it:

1. **Discovers** candidates from the public PnL leaderboard (`scout.leaderboard_windows` × `scout.categories`) plus any `scout.candidate_wallets` you list.
2. **Evaluates** each candidate's public trade history over `scout.lookback_days` (default 180), reconstructing completed round-trips with average-cost accounting to get: closed-trade count, ROI (vs peak working capital), win rate, profit factor, realized PnL, drawdown, positive-month consistency, the share of buy notional entered at near-resolution prices (win-rate farming), single-market concentration, the volume/liquidity of the markets it trades, and its market-*category* mix (all via cached Gamma metadata).
3. **Gates on `requirements:`** — every criterion must pass (`min_closed_trades` default 30, `min_roi_pct`, `min_profit_factor`, `min_realized_pnl_usd`, `min_win_rate_pct`, `min_win_rate_confidence_pct`, `max_drawdown_pct`, `min_positive_month_ratio`, `max_extreme_entry_share`, `max_market_concentration`, `min_market_volume_usd`, `min_market_liquidity_usd`). The requirements exist to reject lucky streaks and survivorship bias, not to bless winners. `min_win_rate_confidence_pct` is a statistical check layered on top of the raw win-rate bar: a candidate's win rate must also clear a 95% Wilson-score confidence lower bound (see `scout.wilson_lower_bound`), which naturally penalizes thin samples — a 60% win rate over 10 trades produces a much lower bound than 60% over 200, so a lucky small sample can pass the raw win-rate threshold and still get rejected here.
4. **Scores with `scoring:`** — a weighted composite (0–100) over ROI, profit factor, consistency, drawdown, PnL, diversification, win rate and liquidity; candidates below `minimum_score` (default 75) are rejected even if every requirement passes.
5. **Follows** the best passing candidates with your `default_sizing` until `scout.max_auto_wallets` active auto slots are filled — subject to the `diversification:` caps: a candidate whose current exposure overlaps more than `max_market_overlap_pct` with already-followed wallets, whose exposure vector correlates above `max_wallet_correlation` with any of them, or whose dominant market category (e.g. "Sports") would exceed `max_category_share_pct` of the followed roster's dominant categories, is skipped. Auto-added wallets carry an `AUTO` badge.

**Rotation** emerges from the interaction with auto-manage (below): a paused wallet keeps its history but frees its slot, and the scout fills it with the next-best *new* candidate. The scout never re-adds a wallet that was ever followed before.

**Category awareness.** A wallet's requirements/score are computed from its *entire* trade history blended across every market category — a real edge in one vertical (say, sports) can be diluted by mediocre results in another (say, politics), and vice versa. Rather than re-scoring per category (a larger change — it needs per-trade category resolution and a separate round-trip reconstruction per bucket), Polymirror surfaces each candidate's category mix (visible in the scout table and via `category_mix` on `/api/scout`) and caps category concentration at the *portfolio* level via `diversification.max_category_share_pct`, so your auto-followed roster can't end up secretly all one vertical just because that vertical dominates the overall leaderboard. Set `scout.categories` to more than the default `["OVERALL"]` (e.g. add `POLITICS`, `SPORTS`, `CRYPTO`) to actually discover candidates across verticals for this to balance.

Every candidate evaluation is visible in the dashboard's scout table and via `GET /api/scout`, including rejection reasons.

> The leaderboard endpoint is `GET data-api.polymarket.com/v1/leaderboard`, officially documented at [docs.polymarket.com](https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings) (`category`, `timePeriod`, `orderBy`, `limit` ≤ 50, `offset` ≤ 1000). The old `lb-api.polymarket.com/leaderboard` host+path is retired and now 404s — if the scout logs "no parseable wallets" again in the future, run `python scripts/probe_leaderboard.py` to inspect the live response shape against `clients.LeaderboardClient`, and re-check the docs page in case the schema moved again. Similarly, Gamma's market-category field isn't a documented stable contract — if `category_mix` looks wrong (empty, or everything landing in one bucket), run `python scripts/probe_market_category.py <condition_id>` to inspect the live shape against `clients.GammaClient.market_category`.

### Troubleshooting: `CERTIFICATE_VERIFY_FAILED (self-signed certificate)`

Something between Python and Polymarket is re-signing HTTPS traffic — on home machines this is usually antivirus "web protection" or a VPN; in offices, a corporate proxy. Run `python scripts/diagnose_network.py` to see exactly which hosts fail and whose certificate is being presented. The stock fix ships with the app: `pip install -r requirements.txt` installs `truststore`, and `run.py` then validates certificates against the **Windows certificate store** (where your antivirus/proxy root already lives) instead of Python's bundled CA list — verification stays fully on. Restart the engine afterwards; the scout retries on its next sweep.

## Automatic risk management (auto-manage)

Followed wallets are judged on their **copied** performance — what your simulated account actually captured after sizing, slippage and latency, not the source's own results. Once a wallet has `auto_manage.min_closed_trades` closed copies (default 15), it is paused automatically if its profit factor drops below `pause_profit_factor` (default 0.9), its realized loss exceeds `pause_loss_pct` of starting balance (default 8%), or its realized-PnL drawdown exceeds `pause_drawdown_pct` (default 12%). Pausing stops new copies; open positions and history are kept.

Two override rules keep it civil: if **you** resume a paused wallet, auto-manage will not re-pause it until *new* closed trades confirm the breach; and after `resume_after_hours` (default 72h) an auto-paused wallet gets one automatic second chance under the same new-evidence rule. Applies to both manual and auto-followed wallets.

## Per-position take-profit / stop-loss (`position_risk`)

Independent of whether the source trader has sold, each *individual copied position* is checked every mark-to-market cycle against its own unrealized return since entry: if it's down at least `stop_loss_pct` (default 25%), the simulator auto-sells the full position at the current mark. Take-profit is scaled by `take_profit_room_pct` (default 50%) as a fraction of the *remaining room to $1* rather than a flat percentage of entry price — outcome tokens are bounded in (0, 1), so a position bought at 0.90 can never gain 50% of its entry price (max possible upside from there is ~11%); scaling by remaining room keeps the rule reachable across the whole entry-price range instead of being dead weight for expensive entries. This is a per-position safety net layered on top of whatever the copied wallet does — it never touches real funds, only the paper book. Auto-exits are logged as `kind: risk` events and show up in the trade log with `kind: POSITION_RISK`. You can also close any open position manually at any time from the dashboard, independent of these thresholds. Disable auto-exit via `position_risk.enabled: false`.

## Settlement of resolved markets (`settlement`)

Every `settlement.interval_seconds` (default 300s) the engine sweeps its open positions, asks the Gamma API whether each market has resolved, and settles at the real payout — **$1 or $0** — independent of whether the source trader redeemed. Settled rows appear in the trade log with `kind: SETTLE`.

**Leave this on.** Source-driven redemption alone is structurally biased toward winners: a losing outcome token is worthless, so nobody pays gas to claim $0 and no `REDEEM` event is ever emitted for a loss. Without the sweep, losing copies never settle at all — they sit open forever, carried at their entry-era mark (a resolved market returns no CLOB midpoint, so mark-to-market silently leaves the last price in place), while every winner books a full $1. The equity curve can then only ratchet upward, which reads as a spectacular strategy and is really just an accounting artifact. This project's own early paper record was inflated by roughly its entire reported profit that way.

A market settles only when Gamma reports it closed **and** the outcome price for your specific token is decisively 0 or 1. Still-trading, disputed, mid-resolution, or a token id that doesn't match the payload all leave the position open — a market quoting 0.98 is a near-certainty, not a resolution, and an unknown outcome is never treated as a win. Positions still open after `settlement.unresolved_alert_hours` (default 48) raise a warning; that almost always means Gamma's payload shape changed and token ids stopped matching, which you can confirm with:

```bash
python scripts/probe_resolution.py <condition_id>
```

## Auto-withdraw (`auto_withdraw`)

Optional profit-taking sweep: every time cumulative *realized* PnL (closed trades only — unrealized swings don't count) climbs past another increment, that amount is withdrawn from the tradeable cash balance into the same withdrawal ledger a manual dashboard withdrawal uses. It's one-way — a later losing trade never claws back a sweep already made, and losses never let a future sweep fire early against a stale high-water mark — so it behaves like actually banking profit rather than a running high-water mark that can reverse. Disabled by default; enable with `auto_withdraw.enabled: true`. Paper-mode bookkeeping only today.

`auto_withdraw.mode` picks how the increment is expressed: `percent_starting_balance` (recommended, `profit_increment_pct`, default 5%) scales automatically if you change `starting_balance`; `fixed_usd` (`profit_increment_usd`, default $500) is an unchanging dollar figure that stops making sense if you resize the account — $500 is 50% of a $1,000 account and 0.5% of a $100,000 one.

## Copy analytics

`GET /api/analytics` (three stat cards on the dashboard) quantifies what copying costs you versus the source's own fills: **copy latency** (source timestamp → simulated execution), **price difference** in bps (positive = against you), **missed edge** in USD, and **alpha decay** (realized PnL per round-trip bucketed by how stale the entry copy was). Individual trackers toggle under `analytics:`.

## Notifications

All events land in the persistent activity log shown on the dashboard. To push them out, set any of `discord_webhook_url`, `telegram_bot_token` + `telegram_chat_id`, or `generic_webhook_url` under `notifications:`. Notifications fire on detected source trades, executed simulated trades, skipped copies, synchronization drift, and errors. `min_level: warning` silences routine trade notices.

## HTTP API

The dashboard is a thin client over a REST API you can also script against:

```
GET    /api/summary                 metrics + allocation + wallets + per-wallet attribution + trading state
GET    /api/scout                   scout candidates with scores, stats and verdicts
GET    /api/trading                 copying on/off + halt reason
POST   /api/trading                 {"copying": false} halts new buys; {"copying": true} resumes
GET    /api/analytics               copy latency, price difference, missed edge, alpha decay
GET    /api/positions?status=open   open or closed simulated positions
GET    /api/trades?limit=100        copied trade history
GET    /api/equity                  equity curve snapshots
GET    /api/events?level=error      activity / error log
GET    /api/wallets                 followed wallets
POST   /api/wallets                 {address, label?, sizing_mode?, sizing_value?, multiplier?}
PATCH  /api/wallets/{address}       change label / sizing / active
DELETE /api/wallets/{address}       stop following (history preserved)
POST   /api/reset                   wipe the simulation back to starting balance
GET    /api/health
```

Interactive docs at http://127.0.0.1:8400/docs.

## Reliability model

The engine is a single supervised loop in which every cycle is exception-isolated — an API outage, malformed response or bug in one wallet's handling logs an error and the loop continues. HTTP calls retry with exponential backoff and jitter; a client-side rate limiter paces all Polymarket requests. State (cursors, processed events, positions, account) is persisted to SQLite in WAL mode on every change, so you can kill the process at any time and restart without duplicating or losing trades: each activity event is claimed in a durable idempotency table *before* execution, and polling re-scans a 2-minute overlap window behind the cursor to catch late-indexed events. Trades missed during longer downtime are caught by the periodic reconciliation pass, which compares the engine's shadow model of each source trader against the authoritative `/positions` endpoint, corrects drift, and raises a sync notification.

## Safety guarantees

Polymirror separates simulation from execution:

1. Paper mode is always the default.
2. Live execution requires explicit configuration.
3. Credentials are never committed to the repository.
4. Secrets are loaded from environment variables or secret managers.
5. Execution logic is isolated from strategy and analytics.
6. Live execution paths are tested using mocks.
7. Logs never contain private keys or signing material.

The architecture separates:

- market data
- wallet tracking
- strategy
- sizing
- execution
- storage
- analytics

so execution capability can evolve without compromising the simulation environment.

## Live execution (optional, off by default)

Everything above is paper trading. Polymirror can also place real orders on Polymarket's CLOB, but only after every one of the following is done deliberately — there is no single setting that turns it on:

1. `pip install py-clob-client` (optional dependency, not in the default `requirements.txt` install).
2. Set the environment variables `live.*_env` in `config.example.yaml`'s `live:` block point to — at minimum `POLYMARKET_PRIVATE_KEY` (your signing key) and `POLYMARKET_FUNDER_ADDRESS` (your Polymarket proxy/funder wallet). Never put the values themselves in `config.yaml`.
3. In `config.yaml`, set `execution.mode: live` **and** `live.confirm: true` **and** `live.max_live_notional_usd` to a positive per-order cap. `Config.validate()` refuses to start if any one of these is missing.
4. Leave `live.dry_run: true` for a first run — orders are signed and logged but never submitted, so you can confirm the wiring (env vars, wallet, sizing) before real orders go out. Set it to `false` only when you're ready.

Once live, `/api/health` and the dashboard header always show the active mode (`PAPER TRADING`, `LIVE MODE (DRY RUN)`, or `⚠ LIVE TRADING`) — the mode is never silent. `LiveBroker` books whatever the exchange actually reports (real size, price, fee) into the same position/trade tables paper mode uses, so the dashboard, analytics, and auto-manage risk rules work identically in both modes.

**Capital accounting stays honest, not just order submission.** Before the first live order, and again on every reconciliation cycle, Polymirror checks the real numbers rather than trusting its own bookkeeping:
- **Cash**: the very first time live mode runs against a given database, the account's cash/starting_balance is (re)set from the real on-chain USDC balance (`ExecutionClient.get_usdc_balance()`) — `starting_balance` in `config.yaml` is a paper-mode concept and is never trusted for real money. Every check after that compares booked cash to the real balance and reports drift loudly instead of trading on a number that might be wrong.
- **Positions**: your own funder wallet's real holdings (via the same public `/positions` feed used for every followed wallet) are compared against what Polymirror's book shows on every reconciliation cycle.
- Neither of these is ever auto-corrected after the initial bootstrap — per the project's shadow-model invariant, our own recorded state isn't silently patched to match an external number. Drift is surfaced as a `risk`-level notification for a human to investigate (a missed fill, a crash mid-order, and a resting order that filled late are all real possibilities this is meant to catch).

**Order type defaults to `FOK`** (fill-or-kill: fills completely right now or does nothing) rather than `GTC`/`GTD`, which can rest on the book and fill partially or later — Polymirror does not currently poll a resting order for further fills or cancel it on a timeout, so FOK avoids that gap entirely for the common case. Use GTC/GTD only if you understand and accept that limitation.

## Limitations (honest ones)

Detection latency is poll interval plus Polymarket's indexing delay — typically seconds, occasionally longer. You cannot see another wallet's activity in true real time through official means, because the CLOB user websocket only streams the authenticated owner's own orders. Copy fills are simulated, so real-world fills would differ when copying large or illiquid positions; `market_quote` mode plus slippage is the honest approximation. Splits, merges and conversions are not mirrored as trades (they're not trades); they surface through reconciliation instead. See `docs/ARCHITECTURE.md` for the full platform analysis and design rationale.

## Project layout

```
run.py                    entry point
config.example.yaml       annotated configuration
polymirror/
  config.py               config loading & validation
  clients.py              read-only Data/CLOB/Gamma/Leaderboard API clients (retry, rate limit)
  storage.py              SQLite persistence: account, positions, trades, cursors, candidates, log
  sizing.py               position sizing strategies
  broker.py               PaperBroker (simulated fills/slippage/fees/PnL) + LiveBroker (real orders)
  execution.py            live-only: order signing & submission (py-clob-client), credential loading
  tracker.py              engine: polling, dedup, classification, reconciliation, auto-pause
  scout.py                wallet discovery: evaluate leaderboard candidates, auto-follow
  metrics.py              performance metrics, per-wallet attribution & allocation
  notify.py               log + Discord/Telegram/webhook notifications
  api.py                  FastAPI REST layer + dashboard hosting
  static/index.html       dashboard (vanilla JS + Chart.js)
scripts/                  rerunnable research/ops scripts (leaderboard probe, market-category probe)
tests/                    177 unit & integration tests (stubbed APIs, mocked live execution)
docs/ARCHITECTURE.md      platform analysis & design decisions
```
