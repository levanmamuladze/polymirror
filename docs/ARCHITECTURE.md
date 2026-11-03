# Architecture & Platform Analysis

## 1. How to observe another wallet's trades on Polymarket

Polymarket exposes three official API families, all with public read access. The question for a copy trader is which one can reliably tell you *what a specific third-party wallet just did*.

### Candidate data sources compared

**Data API (`data-api.polymarket.com`) — chosen as the primary feed.**
`GET /activity?user=<wallet>&type=TRADE,REDEEM` returns a normalized, per-wallet event stream: each row carries `transactionHash`, `asset` (the outcome-token id), `conditionId`, `side`, `size`, `price`, `usdcSize`, `timestamp`, plus market title/outcome for display. It requires no authentication, supports `start`/`end` timestamp filtering and pagination, and covers exactly the event types a copier needs (TRADE, plus REDEEM for resolved-market settlements; SPLIT/MERGE/CONVERSION also exist). `GET /positions?user=` gives the authoritative current holdings and `GET /value?user=` the wallet's open-position value — both essential for the shadow model and proportional sizing described below.

**CLOB API (`clob.polymarket.com`).**
Read endpoints (midpoint, price, book) are public and are used here for mark-to-market and `market_quote` fills. The CLOB also has a real-time *user channel* websocket — but it is authenticated and only streams the credential owner's own orders. It **cannot observe third-party wallets**, so it is not usable for copy trading, and using it would also require handling API credentials, which this project deliberately avoids.

**On-chain data (Polygon logs / Goldsky subgraph / third-party indexers).**
Every fill is an `OrderFilled` event on the CTF Exchange contract, so this route is maximally trustworthy and near-real-time. The costs: decoding ERC-1155 fills, mapping token ids to markets, handling negative-risk conversions, and running or paying for indexer infrastructure. The Data API exposes the same fills already normalized, so on-chain indexing adds operational weight without adding information for this use case. It remains the right upgrade path if sub-second detection ever matters.

### Consequence: polling, and its honest limitation

Because no official channel pushes third-party activity, the design polls `/activity` per wallet on a configurable interval (default 15s) with client-side rate limiting shared across all wallets. Detection latency is therefore *poll interval + Polymarket indexing delay* — typically seconds. This is a structural limitation of official access, not an implementation choice, and the simulator is honest about it: the `market_quote` price mode fills your copy at the CLOB's *current* best price rather than the source's historical fill, so results reflect what a real copier polling at this cadence could plausibly have achieved.

## 2. System design

```
                ┌────────────────────────────────────────────────────┐
                │                    asyncio Engine                  │
   Data API ───▶│ poll ▶ dedup ▶ classify ▶ size ▶ self.broker       │──▶ SQLite (WAL)
   CLOB API ───▶│ mark-to-market      reconcile ▶ shadow model       │        ▲
   Gamma API ──▶│                                                    │        │
                └───────────────┬────────────────────────────────────┘        │
                                ▼                                             │
                            Notifier (log / Discord / Telegram / webhook)     │
                                                                              │
                FastAPI  ◀── REST /api/* ◀── Dashboard (static SPA) ──────────┘

   self.broker is chosen once, in run.py's composition root, from
   execution.mode:
     mode: paper (default)  -> PaperBroker            (broker.py, no network)
     mode: live  (opt-in)   -> LiveBroker              (broker.py)
                                 └─▶ ExecutionClient    (execution.py — signing,
                                                          order submission; the
                                                          ONLY module that may
                                                          hold key material)
```

Layering follows a strict separation of concerns: `clients.py` (external read-only adapters) → `tracker.py`/`scout.py` (domain logic) → `broker.py`/`sizing.py` (execution domain) → `execution.py` (signing/order submission, live mode only) → `storage.py` (data) → `api.py` (interface). No layer reaches around another; `tracker.Engine` calls `self.broker.buy()`/`.sell()` without knowing or caring whether that's a simulation or a real order, the API doesn't know Polymarket exists, and only `execution.py` ever imports a signing library or reads a credential-bearing environment variable.

### The shadow model — why it exists

A sell event tells you the source sold N shares, but copying it correctly requires knowing *what fraction of their position* that was. The engine therefore maintains a per-wallet **shadow model** of the source trader's holdings: seeded from `/positions` when a wallet is first followed, updated on every observed trade, and periodically reconciled against `/positions` again. When the source sells 40% of a position, the simulator sells 40% of its copy — independent of entry sizing mode — which keeps the two books structurally synchronized. If reconciliation finds drift (missed events during downtime, splits/merges we don't model, token transfers), it corrects the shadow in place and raises a `sync` notification. Simulated positions are intentionally *not* force-adjusted by reconciliation: the trade log remains an honest record of copies actually executed.

### Event classification

For each trade event the engine compares the shadow position before and after: a BUY into no position is `OPEN`, into an existing one `ADD`; a SELL that leaves shares is `PARTIAL_EXIT`, one that empties the position `FULL_EXIT`. `REDEEM` activity settles the mirrored position at the resolved payout — $1 or $0, read from the market's resolution (see *Settlement* below); a payout that cannot be established leaves the position open rather than guessing. Splits, merges and conversions are not trades and are not mirrored; they surface via reconciliation.

### Settlement of resolved markets

Source-driven redemption cannot be the only settlement path, because it is **structurally biased toward winners**: a losing outcome token is worthless, so no trader pays gas to claim $0 and no `REDEEM` event is ever emitted for a loss. Relying on it alone means losing copies never settle — they stay open indefinitely, carried at their entry-era mark, because a resolved market returns no CLOB midpoint for mark-to-market to update. Winners book $1, losers book nothing, and the equity curve can only ratchet upward. This is an accounting artifact, not performance, and it invalidated this project's own early paper record.

`Engine.settle_resolved_positions()` therefore sweeps every open position on its own cadence (`settlement.interval_seconds`, default 300s) and asks Gamma directly whether the market has resolved, settling at the real payout regardless of what the source did. The sweep runs *before* mark-to-market and the risk checks in each cycle, so equity and stop-loss never evaluate a position whose market has already ended.

`clients.parse_resolution()` holds the rules and is deliberately pure — test doubles parse real Gamma payloads through the same function, so a stub cannot quietly disagree with production about what "resolved" means. Two independent conditions are both required before any payout is returned: Gamma reports the market closed/resolved, **and** the outcome price for our specific token is decisively 0 or 1 within `RESOLVED_TOLERANCE`. A market quoting 0.98 is a near-certainty, not a resolution. Our token is located by index in `clobTokenIds`; if it isn't in that list the result is unknown rather than a guess, since matching the wrong index would settle a position at the opposite outcome's payout. Unknown never means "win" — it means leave the position open and, past `settlement.unresolved_alert_hours`, raise a `sync` warning (diagnose with `scripts/probe_resolution.py`). Settlements claim their idempotency key before the fill, so a repeated or interrupted sweep cannot double-book.

Sweep-driven settlements are recorded with `kind: SETTLE`, source-redemption-driven ones with `kind: REDEEM`. Both are excluded from copy-quality analytics (`metrics.SETTLEMENT_KINDS`) because they fill at a resolved payout, so comparing them against the source's fill price is meaningless.

### Exactly-once execution and restart safety

Every activity row gets a deterministic idempotency key: `sha256(txHash | asset | side | size | price | timestamp | type)` — a single transaction can contain multiple fills, so the hash covers every distinguishing field. The key is inserted into a durable `processed_events` table **before** the copy executes; a duplicate insert (unique constraint) means the event is skipped. Polling always re-scans a 120-second overlap window behind the persisted per-wallet cursor, so late-indexed or out-of-order events are picked up and duplicates are suppressed by the same table. The worst case across a crash is a single skipped event (claimed but not executed), never a double execution — and skips are eventually surfaced by reconciliation drift. All state lives in SQLite with WAL journaling; a `kill -9` and restart resumes from the cursors.

### Fill model and accounting

Outcome tokens are long-only assets priced in (0, 1). Positions use average-cost basis; realized PnL on sells is `(fill − avg_cost) × size`, accumulated per position (so partial exits are attributed correctly), with fees subtracted. Slippage (`slippage_bps`, default 20) is always applied *against* the copier — buys fill higher, sells lower — and fees default to 0 to match Polymarket's current no-trading-fee regime while remaining configurable for stress tests. Redemptions settle at the payout with no fee or slippage, matching on-chain redemption mechanics.

### Metrics

Equity = cash + Σ(mark × size) using CLOB midpoints refreshed each mark cycle. A *resolved* market has no midpoint to fetch, so a position left open past resolution would keep its last pre-resolution mark forever — the settlement sweep above exists to close that hole before it reaches the equity curve. The metrics module derives total return, realized/unrealized PnL, win rate and profit factor over completed round-trips (closed positions), and max drawdown over the persisted equity curve. `wallet_metrics()` additionally attributes copied results per followed wallet (closed trades, win rate, profit factor, realized/unrealized PnL) — the numbers the dashboard's wallet table and the auto-manage risk rules run on.

### Wallet scout — automatic discovery and following

The scout (`scout.py`) removes the one remaining manual step: choosing whom to copy. It runs as its own asyncio task beside the engine loop (a full sweep can take minutes under the shared rate limiter and must not stall polling) and repeats a four-stage pipeline:

1. **Discover** — `GET /v1/leaderboard` on `data-api.polymarket.com` (public, unauthenticated, officially documented at docs.polymarket.com but response field names aren't part of that contract so parsing stays defensive; `scripts/probe_leaderboard.py` verifies the live shape) plus statically configured candidates. Looped over `scout.categories` (default just `["OVERALL"]`) as well as `scout.leaderboard_windows`, so opting into more categories (e.g. `POLITICS`, `SPORTS`) is what feeds the category-diversification cap below actual candidates. Wallets already in the follow list — active or not — are excluded up front.
2. **Evaluate** — the candidate's `/activity` history over the lookback window is replayed through `candidate_stats()`, a pure function using the same average-cost conventions as the broker. It reconstructs completed round-trips and derives win rate, profit factor, realized PnL, drawdown (peak-to-trough of cumulative realized PnL, normalized by peak concurrent cost basis as a working-capital proxy), near-resolution entry share, and single-market concentration. Sells with no basis in the window (positions opened earlier) are excluded rather than guessed. A separate step (`resolve_categories` + `category_breakdown`) resolves each of the candidate's largest markets to a category via cached Gamma metadata and produces a buy-notional-weighted mix (e.g. 82% Sports / 18% Politics) — this does *not* re-score per category (that needs per-trade category resolution and a separate round-trip reconstruction per bucket, a larger change), it only informs the portfolio-level diversification cap below. Evaluations are cached in the `scout_candidates` table and refreshed at most every `reeval_hours`.
3. **Requirements + score** — every `requirements:` criterion must pass (sample size — `min_closed_trades` default 30, following the classical n≥30 rule of thumb — ROI, profit factor, win rate, a Wilson-score 95% confidence lower bound on the win rate via `wilson_lower_bound()` (penalizes thin samples directly rather than relying solely on the sample-size cutoff), drawdown, monthly consistency, near-resolution entry share, concentration, and — via cached Gamma metadata — the volume/liquidity of the markets the candidate trades; unknown market data never fails a requirement). Passing candidates are then ranked by the `scoring:` weighted composite (0–100) and must clear `minimum_score`. Failing candidates are stored with human-readable reasons and shown on the dashboard — the scout's judgment is auditable, not a black box.
4. **Follow** — top-scoring passing candidates fill free auto slots (`max_auto_wallets` counts *active* auto wallets) using the default sizing, flagged `auto=1`, subject to `diversification:` caps: market overlap and exposure-vector cosine correlation against wallets already followed, plus category concentration (`max_category_share_pct`) — each active followed wallet with a stored evaluation casts one vote for its single dominant category, and a new candidate is skipped if it would push one category's share of those votes over the cap. The idempotent event pipeline makes auto-adds safe: a wallet added mid-cycle is seeded and copied like any manual add.

### Stop controls and portfolio risk

Stopping is layered and always position-preserving. A **global halt** (manual via `POST /api/trading`, or automatic when the `portfolio:` daily-loss / total-drawdown limits are breached) blocks new buys while exits keep mirroring — the honest interpretation of "stop": no new risk, existing risk can still come off. `max_daily_loss_pct` (default 10%) and `max_total_drawdown_pct` (default 15%) are deliberately independent, not two views of one thing: the former is judged against a fixed reference (`starting_balance`) over today only, so it catches one bad day fast without loosening as the account grows; the latter is judged against a floating reference (the all-time peak) over all time, so it catches a slow multi-day bleed that no single day is large enough to trip. See `PortfolioConfig`'s docstring for a worked numeric example. Halt state lives in the `meta` table, so it survives restarts, and an auto re-halt after a manual resume requires equity to fall *below* the level at which the last halt fired (new losses, not a stale breach). Ahead of every copied buy a gate checks, in cost order: halt state, `copy_filters` (buy/sell master switches, entry-price band, near-resolution filter), `portfolio.max_open_positions`, cached-Gamma market liquidity, and finally the sized notional against `portfolio.max_single_market_pct` of equity. Every skip is logged with its reason. The `analytics` module quantifies copy quality from the stored fill pairs (`price` vs `source_price`, `executed_at` vs `source_ts`): latency, price difference, missed edge, and alpha decay by entry staleness.

**Rotation and the user-override contract.** Auto-manage (in the tracker loop, pure SQLite) pauses any active wallet whose copied performance breaches the thresholds, recording the wallet's closed-trade count in `meta`. A paused auto wallet frees its slot; the scout fills it with the next-best *new* candidate — never by re-adding anything previously followed, so a pause (or user removal) is never fought by the scout. If the user manually resumes a paused wallet, auto-manage stays silent until new closed trades re-confirm the breach. Evaluation measures the *source's* skill (their own fills); auto-manage measures the *copy* (post-latency, post-slippage) — a wallet must survive both to keep its slot, which operationalizes the "skill vs copyability" distinction that `source_price` / `market_quote` fill modes exist for.

**Auto-withdraw (profit-taking).** Orthogonal to the stop controls above — those bound *losses*; `auto_withdraw` (opt-in, disabled by default) is the symmetric mechanism for *winnings*, sweeping a fixed increment of cumulative *realized* PnL out of the tradeable cash balance every time it crosses another multiple, via the same ledger a manual withdrawal uses. It is deliberately one-way (a later loss never claws back a sweep, and a loss never lets a future sweep fire early against a stale high-water mark), which trades away compounding for capital preservation — with `fixed_usd` sizing and realized profit routinely swept back out, equity grows arithmetically rather than geometrically. That is a legitimate choice for steady extraction, but the opposite of a Kelly-style growth-maximizing setup; `auto_withdraw.mode: percent_starting_balance` at least keeps the increment correctly scaled to the account size rather than compounding a second, unrelated problem on top.

### Reliability

The engine loop wraps each phase (poll, mark, reconcile) in its own exception boundary; any failure is logged, notified, and skipped until the next cycle. HTTP calls retry up to five times with exponential backoff and jitter; 429/5xx are treated as retryable. A shared token-pacing rate limiter (default 4 req/s across all APIs) keeps the process a polite API citizen even with many followed wallets. Notification pushes can never interrupt trading logic — failures there are logged and swallowed.

## 3. Safety by construction — paper by default, live is opt-in and gated

Paper trading is the default and requires nothing to disable: with `execution.mode: paper` (the default), the engine never imports `execution.py`, never reads a private key, and every outbound request in `clients.py` is a `GET` against documented public read endpoints. Auditing that guarantee in paper mode takes one grep: there is no `POST` to any Polymarket host anywhere outside `execution.py`.

Live execution (`execution.mode: live`) is supported but deliberately hard to turn on by accident:

* **Config-level gate.** `Config.validate()` refuses to load a live config unless `live.confirm: true` (a separate, deliberate switch from `execution.mode`) *and* `live.max_live_notional_usd > 0` (a hard per-order cap) are both set — see `polymirror/config.py`. Any single omission fails startup with a specific error, before the engine or any client is built.
* **Composition-root gate.** Only `run.py`'s `build_broker()` decides paper vs. live, and only it imports `execution.py`. `tracker.Engine` receives a pre-built broker and never branches on execution mode itself — the same code path runs unmodified whether it's holding a `PaperBroker` or a `LiveBroker` (execution abstraction).
* **Credential isolation.** `LiveConfig` fields are exclusively environment-variable *names* (`private_key_env`, `funder_address_env`, `clob_api_key_env`, …); the actual private key, API secret, and passphrase are read from `os.environ` inside `execution.py` at client construction and are never logged, persisted, or included in a raw exchange response that gets stored (`OrderResult.raw` holds only order metadata).
* **Per-order cap independent of paper's sizing.** `LiveBroker` enforces `live.max_live_notional_usd` on every order in addition to (not instead of) `execution.max_trade_usd` — a live-specific ceiling that defaults to 0 (blocks everything) and must be explicitly raised.
* **Dry-run rehearsal.** `live.dry_run` defaults to `true` even once every other gate is satisfied: orders are built (and, with the real client, signed) but never submitted — only logged — so an operator can watch what *would* be sent before flipping the very last switch.
* **Order type defaults to FOK.** `live.order_type` defaults to `FOK` (fill-or-kill: fills completely now or does nothing) rather than `GTC`/`GTD`. `LiveBroker` books whatever `ExecutionClient.submit_order` returns as a single, final fill and does not poll a resting order for further fills or cancel it on a timeout — GTC/GTD can rest and fill partially or late, which would be under-recorded until the next reconciliation cycle catches the drift. FOK sidesteps that gap entirely for the common case.
* **Visibility.** The active mode is never inferred silently: it's logged at startup (`run.py`), announced by the engine's first notification, and surfaced at `/api/health` and in the dashboard's header badge (green "PAPER TRADING", or a red "LIVE TRADING" / "LIVE (DRY RUN)" banner).

`execution.py` is the only module allowed to hold signing material or call `py-clob-client` (an optional dependency — not required for paper trading, see `requirements.txt`). `broker.LiveBroker` never guesses a fill: it always books exactly what `ExecutionClient.submit_order` reports back (real size, real price, real fee) into the same Store schema `PaperBroker` uses, so tracking, analytics, and the dashboard behave identically regardless of mode.

**Capital accounting is verified against reality, not assumed.** Signing and order submission being safety-gated isn't enough on its own — the cash/position *bookkeeping* also has to reflect real money, not a paper-mode number. `broker.sync_live_cash`, called once at startup (`run.py`'s `build_broker`, before any order can be placed) and again on every reconciliation cycle (`tracker.Engine.reconcile_own_live_account`):
* **Bootstraps** the account's cash/starting_balance from the real on-chain USDC balance (`ExecutionClient.get_usdc_balance()`) the very first time live mode runs against a given `Store` — `config.yaml`'s `starting_balance` is never trusted for real money, and a database previously used in paper mode has its stale balance overwritten at this one bootstrap point (`Store.bootstrap_live_account`).
* **Never auto-corrects after that.** Every subsequent check compares booked cash — and, separately, the funder wallet's real positions via the same public `/positions` feed used for every followed wallet — against the real numbers and reports drift as a `risk`-level notification. This mirrors the source-wallet shadow-model reconciliation's philosophy (detect and surface drift) but deliberately does *not* mirror its auto-correction: per the shadow-model invariant, our own recorded positions/cash must stay an honest record of trades actually executed, so real drift here (a missed fill, a crash mid-order, a resting order that filled late) is surfaced for a human, never silently patched.
* Engine-agnostic by construction: `reconcile_own_live_account` reads `self.broker.client` / `self.broker.funder_address` via plain `getattr`, so it's a no-op for `PaperBroker` (which has neither attribute) without `tracker.py` ever importing `execution.py` or branching on execution mode.

## 4. Deliberate trade-offs

SQLite over Postgres: single-user tool, tiny write volume, zero ops, trivially portable — the Store class isolates SQL so a swap later is contained. Polling over on-chain streaming: official, simple, sufficient for a simulator; the adapter layer means a subgraph or Kafka feed could replace `DataApiClient` without touching the engine. Vanilla-JS dashboard over a React build: one file, no toolchain, served by the same process — appropriate for a local operational console. Source-price fills as the default over market quotes: it answers the cleaner research question ("does this trader's flow make money?") while `market_quote` mode answers the practical one ("could *I* have made money copying them at this latency?"); both are one config line apart.
