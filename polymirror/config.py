"""Configuration layer.

Loads a YAML config file, applies defaults, and exposes a typed Config
object. Environment variable POLYMIRROR_CONFIG can override the path.

Safety note: paper trading (execution.mode: paper) is the default and
requires no keys or secrets. Live execution (execution.mode: live) is
optional and gated behind several simultaneous, explicit switches — see
LiveConfig and Config.validate(). This file NEVER holds a private key,
API secret, or passphrase directly — only the NAMES of environment
variables that hold them (see config.example.yaml).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

VALID_SIZING_MODES = ("proportional", "fixed_usd", "percent_balance", "percent_starting_balance")
VALID_PRICE_MODES = ("source_price", "market_quote")
VALID_EXECUTION_MODES = ("paper", "live")
VALID_LIVE_ORDER_TYPES = ("GTC", "FOK", "GTD")
VALID_AUTO_WITHDRAW_MODES = ("fixed_usd", "percent_starting_balance")
VALID_SIGNATURE_TYPES = (0, 1, 2)  # 0=EOA, 1=email/magic proxy, 2=browser wallet proxy


@dataclass
class NotificationConfig:
    enabled: bool = True
    min_level: str = "info"            # info | warning | error
    discord_webhook_url: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    generic_webhook_url: str | None = None


@dataclass
class ExecutionConfig:
    mode: str = "paper"                # paper | live — see LiveConfig; NEVER flips automatically
    price_mode: str = "source_price"   # source_price | market_quote
    slippage_bps: float = 20.0         # applied against the copier on every fill
    fee_bps: float = 0.0               # Polymarket charges no trading fee today; configurable
    min_trade_usd: float = 1.0         # skip copies below this notional
    max_trade_usd: float | None = None # optional hard cap per copied trade
    # Extra reaction delay before fetching the fill quote (market_quote mode
    # only — in source_price mode the fill is the source's own price by
    # definition). Note the poll interval already dominates total latency.
    simulated_latency_ms: float = 0.0
    randomize_latency: bool = False
    latency_jitter_ms: float = 0.0     # uniform ± jitter when randomize_latency


@dataclass
class LiveConfig:
    """Placeholders and safety gates for OPTIONAL live execution against
    real Polymarket markets with real funds. Inert unless
    execution.mode == "live" (default "paper" — see ExecutionConfig).

    Nothing here ever holds a secret — only the NAMES of environment
    variables that hold them. Set those variables in your shell or a
    secret manager; never put a private key, API secret, or passphrase in
    this file or commit one to the repository (see CLAUDE.md's
    credential-safety rules and config.example.yaml)."""
    confirm: bool = False                  # must be explicitly true to allow live mode to start
    dry_run: bool = True                   # true = build & sign orders, log them, never submit
    private_key_env: str = "POLYMARKET_PRIVATE_KEY"
    funder_address_env: str = "POLYMARKET_FUNDER_ADDRESS"       # Polymarket proxy/funder wallet
    clob_api_key_env: str = "POLYMARKET_CLOB_API_KEY"           # optional: derived if unset
    clob_api_secret_env: str = "POLYMARKET_CLOB_API_SECRET"     # optional: derived if unset
    clob_api_passphrase_env: str = "POLYMARKET_CLOB_API_PASSPHRASE"  # optional: derived if unset
    chain_id: int = 137                    # Polygon mainnet
    signature_type: int = 0                # 0=EOA, 1=email/magic proxy, 2=browser wallet proxy
    # FOK (fill-or-kill) is the default and recommended choice for an
    # unattended copy-bot: it either fills completely right now or does
    # nothing — no resting order to track. GTC/GTD can rest on the book
    # and fill partially or late; LiveBroker books whatever
    # ExecutionClient.submit_order() returns as a single, final fill and
    # does NOT poll a resting order for further fills or cancel it on a
    # timeout, so a GTC/GTD order that fills later (or partially) will be
    # under-recorded until the next reconciliation cycle surfaces the
    # drift (see tracker.Engine.reconcile_own_live_account). Use GTC/GTD
    # only if you understand and accept that gap.
    order_type: str = "FOK"                # GTC | FOK | GTD
    max_live_notional_usd: float = 0.0     # hard per-order cap; 0 blocks every live order


@dataclass
class SizingConfig:
    """Default sizing for newly *auto-followed* wallets (scout picks) —
    manually-added wallets set their own sizing via the dashboard/API.
    fixed_usd is the deliberate default: simplest to reason about, doesn't
    compound risk across a string of mediocre auto-picks, and — unlike
    percent_balance — immune to the fraction-vs-percentage mistake (typing
    20 instead of 0.20) that has caused real, hard-to-diagnose damage in
    this project before (see api.py's _check_sizing validation)."""
    mode: str = "fixed_usd"
    value: float = 100.0               # meaning depends on mode (see sizing.py)
    multiplier: float = 1.0


@dataclass
class ScoutConfig:
    """Automatic wallet discovery mechanics: where candidates come from and
    how often. Pass/fail criteria live in `requirements`, ranking in
    `scoring`. Read-only (public GETs), paper-only as ever.

    categories controls which leaderboard verticals discovery pulls from
    (the leaderboard API accepts category values like OVERALL, POLITICS,
    SPORTS, CRYPTO, ...). The default (["OVERALL"]) preserves the
    historical behavior of only ever looking at the blended overall
    leaderboard; adding more categories here is what feeds
    `diversification.max_category_share_pct` (see DiversificationConfig)
    real candidates to balance across — without it, that cap has nothing
    to work with beyond whatever the overall leaderboard happens to
    surface (which tends to skew toward whichever category trades highest
    volume, typically sports)."""
    enabled: bool = True
    interval_seconds: float = 21_600.0     # full discovery sweep cadence (6h)
    leaderboard_base: str = "https://data-api.polymarket.com"
    leaderboard_windows: list[str] = field(default_factory=lambda: ["1w", "1m", "all"])
    leaderboard_limit: int = 50            # top-N wallets pulled per window (API max is 50)
    categories: list[str] = field(default_factory=lambda: ["OVERALL"])  # leaderboard verticals to discover from
    candidate_wallets: list[str] = field(default_factory=list)  # extra addresses to evaluate
    lookback_days: int = 180               # history window for candidate evaluation
    reeval_hours: float = 24.0             # skip candidates evaluated more recently
    auto_follow: bool = True
    max_auto_wallets: int = 5              # cap on concurrently active auto-followed wallets


@dataclass
class RequirementsConfig:
    """Minimum requirements a candidate wallet must ALL clear before it can
    be auto-followed. Computed from the candidate's public trade history
    over `scout.lookback_days`.

    min_closed_trades=30 (raised from 15) follows the classical n>=30
    rule-of-thumb minimum for a sample's mean to start behaving normally;
    15 round-trips of a binary-ish win/loss outcome is thin enough that
    even a genuinely +EV 55%-win-rate trader has a meaningful chance of
    showing a losing record by chance alone. min_win_rate_confidence_pct
    is a second, complementary defense in the same spirit: rather than
    relying solely on a sample-size cutoff, it directly measures whether
    the observed win rate could plausibly be noise (see
    scout.wilson_lower_bound) — a much lower bar than min_win_rate_pct
    since it answers "is there enough evidence" rather than "is the
    trader good"; a wallet must clear both."""
    min_closed_trades: int = 30            # completed round-trips in the window (matches auto_manage's bar)
    min_roi_pct: float = 15.0              # realized PnL / peak working capital
    min_profit_factor: float = 1.5
    min_realized_pnl_usd: float = 500.0
    min_win_rate_pct: float = 55.0
    min_win_rate_confidence_pct: float = 30.0  # 95% Wilson lower bound on win rate must clear this
    max_drawdown_pct: float = 25.0         # peak-to-trough of realized PnL vs peak working capital
    min_positive_month_ratio: float = 0.70 # months with positive realized PnL (needs >=2 months of data)
    max_extreme_entry_share: float = 0.30  # cap on buy notional at near-resolution prices
    extreme_price_band: float = 0.05       # "near-resolution" = entry >1-band or <band
    max_market_concentration: float = 0.40 # cap on buy notional share in one market
    # Weighted-average market quality of the candidate's buys (Gamma metadata,
    # cached; markets with unknown stats are excluded — no data, no verdict).
    min_market_volume_usd: float = 25_000.0
    min_market_liquidity_usd: float = 10_000.0


@dataclass
class ScoringWeights:
    roi: float = 0.20
    profit_factor: float = 0.20
    consistency: float = 0.15      # positive-month ratio
    drawdown: float = 0.15
    pnl: float = 0.10
    diversification: float = 0.10  # 1 - market concentration
    win_rate: float = 0.05
    liquidity: float = 0.05        # share of buy notional in liquid markets


@dataclass
class ScoringConfig:
    """Composite 0-100 score used to rank passing candidates; candidates
    below `minimum_score` are rejected even if every requirement passes."""
    minimum_score: float = 75.0
    weights: ScoringWeights = field(default_factory=ScoringWeights)


@dataclass
class CopyFiltersConfig:
    """Per-trade filters applied before a copy executes."""
    copy_buys: bool = True
    copy_sells: bool = True                    # exits still mirror when halted; this is the master sell switch
    max_entry_price: float = 0.90              # skip buys above this probability
    min_entry_price: float = 0.10              # skip buys below this probability
    min_market_liquidity_usd: float = 10_000.0 # skip buys in thin markets (Gamma, cached; unknown passes)
    ignore_markets_near_resolution: bool = True  # skip buys at >0.95 / <0.05
    ignore_duplicate_events: bool = True       # informational — dedup can never be disabled


@dataclass
class PortfolioConfig:
    """Portfolio-level risk limits enforced on the simulated account.

    max_daily_loss_pct and max_total_drawdown_pct are deliberately
    independent rules, not two views of the same thing — they use
    different reference points (today's opening equity vs. all-time peak
    equity) and different windows (today only vs. all-time), so neither
    makes the other redundant:
      * max_daily_loss_pct catches a single bad day (or a burst of them)
        fast — evaluated against a FIXED reference (starting_balance), so
        it doesn't loosen as the account grows.
      * max_total_drawdown_pct catches a slow multi-day bleed that no
        single day is large enough to trip — evaluated against a
        FLOATING reference (the all-time peak), so the dollar bar rises
        as the account grows.
    Worked example: an account that grew from a $10k starting balance to
    a $15k peak can fall to $12,750 (-15% of peak, the default
    max_total_drawdown_pct) before that rule fires, while the default
    max_daily_loss_pct=10% only blocks a single day moving more than
    $1,000 (10% of the $10k starting_balance) — a multi-day bleed can
    still pass the daily check on every individual day while the
    drawdown check is what actually stops it. Set both with that in mind
    rather than assuming one subsumes the other."""
    max_open_positions: int = 50
    max_single_market_pct: float = 20.0    # max % of equity in one market (condition)
    max_daily_loss_pct: float = 10.0       # halt copying when today's equity loss exceeds this
    max_total_drawdown_pct: float = 15.0   # halt copying at this peak-to-trough equity drawdown
    stop_copy_after_drawdown: bool = True  # whether the drawdown limit halts copying


@dataclass
class AutoWithdrawConfig:
    """Automatic profit-taking: every time cumulative *realized* PnL (closed
    trades only — unrealized swings never count) crosses another multiple
    of the configured increment past what's already been swept, that fixed
    increment is withdrawn from the tradeable cash balance via the same
    path as a manual dashboard withdrawal (paper-mode bookkeeping only).

    One-way by design: a later loss never claws back a past sweep, and a
    net loss never lets a future sweep happen early against a stale peak —
    each increment is swept exactly once, the moment cumulative realized
    PnL first reaches it.

    mode picks how the increment is expressed — a flat dollar figure
    doesn't scale if you change starting_balance (a $500 sweep is 50% of a
    $1,000 account and 0.5% of a $100,000 one), so percent_starting_balance
    is usually the better default for anything other than the stock $10k
    example; fixed_usd is kept for anyone who wants an exact, unchanging
    dollar figure regardless of account size."""
    enabled: bool = False
    mode: str = "fixed_usd"                # fixed_usd | percent_starting_balance
    profit_increment_usd: float = 500.0    # used when mode == fixed_usd
    profit_increment_pct: float = 5.0      # used when mode == percent_starting_balance (5% of starting_balance)


@dataclass
class DiversificationConfig:
    """Correlation control between followed wallets, enforced when the scout
    is about to auto-follow a new candidate.

    max_category_share_pct caps market-*category* concentration (e.g.
    "don't let 5 of 5 auto slots all be sports specialists") separately
    from max_market_overlap_pct, which only catches overlap in specific
    markets/conditions — a wallet trading 50 different NFL games looks
    perfectly diversified by that metric even though it's 100% one skill
    domain. Each currently-followed wallet counts as one vote for its
    single dominant category (see scout.dominant_category); wallets with
    no stored category evaluation (manually added, or evaluated before
    this existed) cast no vote — unknown never blocks, matching this
    module's usual philosophy. Only wallets that went through the scout's
    evaluate() pipeline (auto-followed ones always have; manually-added
    ones generally won't) can be weighed here."""
    enabled: bool = True
    max_wallet_correlation: float = 0.80   # cosine similarity of market-exposure vectors
    max_market_overlap_pct: float = 60.0   # % of candidate exposure already held via followed wallets
    max_category_share_pct: float = 60.0   # cap on one category's share of followed wallets' dominant categories


@dataclass
class AutoManageConfig:
    """Automatic risk management of followed wallets based on *copied*
    (simulated) performance. Pauses copying; never deletes history."""
    enabled: bool = True
    interval_seconds: float = 600.0
    min_closed_trades: int = 15        # sample size before judging a wallet
    pause_profit_factor: float = 0.90  # pause when PF drops below this
    pause_loss_pct: float = 8.0        # pause when realized loss exceeds this % of starting balance
    pause_drawdown_pct: float = 12.0   # pause when the wallet's realized-PnL drawdown exceeds this % of starting balance
    resume_after_hours: float = 72.0   # auto-resume an auto-paused wallet after this cool-off (0 = never)


@dataclass
class PositionRiskConfig:
    """Automatic take-profit / stop-loss on individual copied positions,
    independent of whether the source trader has exited — a per-position
    safety net on top of whatever the copied wallet does. Checked every
    mark-to-market cycle against each position's unrealized return since
    entry. A full exit only (paper simulation — no real funds involved).

    take_profit_room_pct is a fraction of the *remaining distance to $1*,
    not a flat percentage of entry price — outcome tokens are bounded in
    (0, 1), so a position bought at 0.90 can never gain 50% of its entry
    price (max possible upside is ~11%). Scaling by remaining room keeps
    take-profit reachable across the whole entry-price range instead of
    being structurally dead for expensive entries."""
    enabled: bool = True
    take_profit_room_pct: float = 50.0  # % of remaining room to $1 captured before exit
    stop_loss_pct: float = 25.0         # unrealized loss % (magnitude) that triggers a full exit


@dataclass
class SettlementConfig:
    """Independent settlement of positions whose market has resolved.

    Without this sweep the only path to settlement is the source wallet's
    own on-chain REDEEM event — and traders only ever redeem *winners*,
    because losing outcome tokens are worthless and nobody pays gas to
    claim $0. Losers therefore never settled at all: they sat open forever,
    marked at their entry-era price (a resolved market returns no CLOB
    midpoint, so mark-to-market silently leaves the last mark in place),
    while every winner booked a full $1 payout. The equity curve could only
    ratchet upward, which is not a strategy result — it is an accounting
    artifact, and it inflated this project's own paper record by roughly
    the entire reported profit.

    This sweep asks Gamma directly whether each open position's market has
    resolved and settles at the real payout — $1 or $0 — regardless of what
    the source did. `unresolved_alert_hours` flags positions that are still
    open long after their market closed, which is the signal that a
    condition_id or token id isn't being matched (see
    scripts/probe_resolution.py)."""
    enabled: bool = True
    interval_seconds: float = 300.0       # how often to sweep open positions
    unresolved_alert_hours: float = 48.0  # warn about positions open this long past market close; 0 disables


@dataclass
class AnalyticsConfig:
    """Copy-quality analytics derived from stored trades (see /api/analytics)."""
    track_alpha_decay: bool = True       # realized PnL bucketed by entry copy latency
    track_copy_latency: bool = True      # source timestamp -> simulated execution delay
    track_price_difference: bool = True  # fill vs source price, in bps
    track_missed_edge: bool = True       # USD cost of latency + slippage vs source fills
    equity_snapshot_seconds: float = 60.0  # equity snapshot cadence (min with mark_interval_seconds)


@dataclass
class HealthConfig:
    heartbeat_seconds: float = 30.0      # engine liveness stamp cadence (surfaced in /api/health)
    stale_wallet_minutes: float = 15.0   # warn when an active wallet hasn't been polled successfully
    alert_on_api_failure: bool = True    # notify (vs log-only) on Polymarket API failures


@dataclass
class LoggingConfig:
    level: str = "INFO"                  # DEBUG | INFO | WARNING | ERROR
    rotate_mb: float = 25.0
    backups: int = 10


@dataclass
class Config:
    starting_balance: float = 10_000.0
    poll_interval_seconds: float = 15.0
    mark_interval_seconds: float = 60.0        # mark-to-market + equity snapshot cadence
    reconcile_interval_seconds: float = 300.0  # source-position reconciliation cadence
    backfill_seconds: int = 0                  # replay this much history when a wallet is first added
    db_path: str = "data/polymirror.db"
    log_path: str = "data/polymirror.log"
    host: str = "127.0.0.1"
    port: int = 8400
    data_api_base: str = "https://data-api.polymarket.com"
    clob_api_base: str = "https://clob.polymarket.com"
    gamma_api_base: str = "https://gamma-api.polymarket.com"
    request_timeout_seconds: float = 15.0
    max_requests_per_second: float = 4.0       # client-side rate limit across all API calls
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    live: LiveConfig = field(default_factory=LiveConfig)
    default_sizing: SizingConfig = field(default_factory=SizingConfig)
    portfolio: PortfolioConfig = field(default_factory=PortfolioConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    scout: ScoutConfig = field(default_factory=ScoutConfig)
    requirements: RequirementsConfig = field(default_factory=RequirementsConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    copy_filters: CopyFiltersConfig = field(default_factory=CopyFiltersConfig)
    diversification: DiversificationConfig = field(default_factory=DiversificationConfig)
    auto_withdraw: AutoWithdrawConfig = field(default_factory=AutoWithdrawConfig)
    auto_manage: AutoManageConfig = field(default_factory=AutoManageConfig)
    position_risk: PositionRiskConfig = field(default_factory=PositionRiskConfig)
    settlement: SettlementConfig = field(default_factory=SettlementConfig)
    analytics: AnalyticsConfig = field(default_factory=AnalyticsConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    def validate(self) -> None:
        if self.starting_balance <= 0:
            raise ValueError("starting_balance must be positive")
        if self.execution.price_mode not in VALID_PRICE_MODES:
            raise ValueError(f"price_mode must be one of {VALID_PRICE_MODES}")
        if self.execution.simulated_latency_ms < 0 or self.execution.latency_jitter_ms < 0:
            raise ValueError("execution latency values must be >= 0")
        if self.execution.simulated_latency_ms + self.execution.latency_jitter_ms > 30_000:
            raise ValueError("simulated latency above 30s would stall the poll loop")
        if self.execution.mode not in VALID_EXECUTION_MODES:
            raise ValueError(f"execution.mode must be one of {VALID_EXECUTION_MODES}")

        # Live execution is off by construction: every one of these must be
        # satisfied simultaneously before the engine is allowed to build a
        # LiveBroker. This can never be tripped by a single fat-fingered
        # field — see CLAUDE.md: "must never silently switch from paper to
        # live" and "must require explicit configuration."
        if self.execution.mode == "live":
            if not self.live.confirm:
                raise ValueError(
                    "execution.mode is 'live' but live.confirm is not true — refusing to "
                    "start. Live execution trades REAL funds; set live.confirm: true only "
                    "after you understand and accept that."
                )
            if self.live.max_live_notional_usd <= 0:
                raise ValueError(
                    "live.max_live_notional_usd must be set > 0 to allow any live order "
                    "(0 is the safe default and blocks all live trading)"
                )
            if self.live.signature_type not in VALID_SIGNATURE_TYPES:
                raise ValueError(f"live.signature_type must be one of {VALID_SIGNATURE_TYPES}")
            if self.live.order_type not in VALID_LIVE_ORDER_TYPES:
                raise ValueError(f"live.order_type must be one of {VALID_LIVE_ORDER_TYPES}")
            if not self.live.private_key_env or not self.live.funder_address_env:
                raise ValueError(
                    "live.private_key_env and live.funder_address_env must name the "
                    "environment variables holding those values (not the secrets themselves)"
                )
            if self.live.chain_id <= 0:
                raise ValueError("live.chain_id must be positive")
        if self.default_sizing.mode not in VALID_SIZING_MODES:
            raise ValueError(f"sizing mode must be one of {VALID_SIZING_MODES}")
        if self.poll_interval_seconds < 2:
            raise ValueError("poll_interval_seconds below 2s risks hitting API rate limits")

        if self.settlement.interval_seconds < 30:
            raise ValueError("settlement.interval_seconds below 30s hammers the Gamma API")
        if self.settlement.unresolved_alert_hours < 0:
            raise ValueError("settlement.unresolved_alert_hours must be >= 0 (0 disables)")

        if self.portfolio.max_open_positions < 1:
            raise ValueError("portfolio.max_open_positions must be >= 1")
        for key in ("max_single_market_pct", "max_daily_loss_pct", "max_total_drawdown_pct"):
            if getattr(self.portfolio, key) <= 0:
                raise ValueError(f"portfolio.{key} must be positive")

        if self.scout.interval_seconds < 60:
            raise ValueError("scout.interval_seconds below 60s hammers the public APIs")
        if self.scout.lookback_days <= 0:
            raise ValueError("scout.lookback_days must be positive")
        if self.scout.max_auto_wallets < 0:
            raise ValueError("scout.max_auto_wallets must be >= 0")
        if self.scout.reeval_hours < 0:
            raise ValueError("scout.reeval_hours must be >= 0")
        if not 1 <= self.scout.leaderboard_limit <= 50:
            raise ValueError("scout.leaderboard_limit must be in [1, 50] (the API's own max)")

        if not 0 <= self.requirements.extreme_price_band < 0.5:
            raise ValueError("requirements.extreme_price_band must be in [0, 0.5)")
        if not 0 <= self.requirements.min_positive_month_ratio <= 1:
            raise ValueError("requirements.min_positive_month_ratio must be in [0, 1]")
        if not 0 <= self.requirements.min_win_rate_confidence_pct <= 100:
            raise ValueError("requirements.min_win_rate_confidence_pct must be in [0, 100]")
        if self.requirements.min_closed_trades < 1:
            raise ValueError("requirements.min_closed_trades must be >= 1")

        weights = self.scoring.weights
        values = [getattr(weights, f) for f in weights.__dataclass_fields__]
        if any(v < 0 for v in values) or sum(values) <= 0:
            raise ValueError("scoring.weights must be non-negative and sum to > 0")
        if not 0 <= self.scoring.minimum_score <= 100:
            raise ValueError("scoring.minimum_score must be in [0, 100]")

        cf = self.copy_filters
        if not 0 <= cf.min_entry_price < cf.max_entry_price <= 1:
            raise ValueError("copy_filters entry prices must satisfy 0 <= min < max <= 1")
        if not cf.ignore_duplicate_events:
            raise ValueError("copy_filters.ignore_duplicate_events cannot be disabled — "
                             "exactly-once execution is a core invariant")

        if self.auto_withdraw.mode not in VALID_AUTO_WITHDRAW_MODES:
            raise ValueError(f"auto_withdraw.mode must be one of {VALID_AUTO_WITHDRAW_MODES}")
        if self.auto_withdraw.enabled:
            if (self.auto_withdraw.mode == "fixed_usd"
                    and self.auto_withdraw.profit_increment_usd <= 0):
                raise ValueError(
                    "auto_withdraw.profit_increment_usd must be positive when enabled "
                    "(mode: fixed_usd)")
            if (self.auto_withdraw.mode == "percent_starting_balance"
                    and self.auto_withdraw.profit_increment_pct <= 0):
                raise ValueError(
                    "auto_withdraw.profit_increment_pct must be positive when enabled "
                    "(mode: percent_starting_balance)")

        dv = self.diversification
        if not 0 < dv.max_wallet_correlation <= 1:
            raise ValueError("diversification.max_wallet_correlation must be in (0, 1]")
        if not 0 < dv.max_market_overlap_pct <= 100:
            raise ValueError("diversification.max_market_overlap_pct must be in (0, 100]")
        if not 0 < dv.max_category_share_pct <= 100:
            raise ValueError("diversification.max_category_share_pct must be in (0, 100]")

        if not self.scout.categories:
            raise ValueError("scout.categories must be a non-empty list")

        if self.auto_manage.min_closed_trades < 1:
            raise ValueError("auto_manage.min_closed_trades must be >= 1")
        for key in ("pause_loss_pct", "pause_drawdown_pct"):
            if getattr(self.auto_manage, key) <= 0:
                raise ValueError(f"auto_manage.{key} must be positive")
        if self.auto_manage.resume_after_hours < 0:
            raise ValueError("auto_manage.resume_after_hours must be >= 0 (0 disables)")

        if not 0 < self.position_risk.take_profit_room_pct <= 100:
            raise ValueError("position_risk.take_profit_room_pct must be in (0, 100]")
        if self.position_risk.stop_loss_pct <= 0:
            raise ValueError("position_risk.stop_loss_pct must be positive")

        if self.analytics.equity_snapshot_seconds < 5:
            raise ValueError("analytics.equity_snapshot_seconds below 5s is pointless churn")
        if self.health.heartbeat_seconds < 0 or self.health.stale_wallet_minutes < 0:
            raise ValueError("health cadences must be >= 0")
        if self.logging.level.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            raise ValueError("logging.level must be DEBUG | INFO | WARNING | ERROR")
        if self.logging.rotate_mb <= 0 or self.logging.backups < 0:
            raise ValueError("logging.rotate_mb must be positive and backups >= 0")


def _merge(dc: Any, data: dict[str, Any]) -> None:
    for key, value in (data or {}).items():
        if not hasattr(dc, key):
            raise ValueError(f"Unknown config key: {key}")
        current = getattr(dc, key)
        if isinstance(value, dict) and hasattr(current, "__dataclass_fields__"):
            _merge(current, value)
        else:
            setattr(dc, key, value)


def load_config(path: str | os.PathLike | None = None) -> Config:
    cfg = Config()
    candidate = path or os.environ.get("POLYMIRROR_CONFIG", "config.yaml")
    p = Path(candidate)
    if p.exists():
        with open(p, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        _merge(cfg, raw)
    cfg.validate()
    return cfg
