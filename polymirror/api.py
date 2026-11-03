"""HTTP layer: REST API consumed by the dashboard, plus static hosting.

The API is intentionally thin — it reads from and writes to the Store; all
trading logic lives in the engine. Adding/removing wallets takes effect on
the next poll cycle without a restart.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from . import metrics as metrics_mod
from .config import VALID_SIZING_MODES, Config
from .storage import InsufficientCash, Store
from .tracker import Engine

log = logging.getLogger("polymirror.api")
STATIC_DIR = Path(__file__).parent / "static"
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
PERCENT_SIZING_MODES = ("percent_balance", "percent_starting_balance")


def _check_sizing(mode: str, value: float | None) -> None:
    """percent_* modes size off a fraction of balance (0.20 = 20%, see
    sizing.py). A value > 1 almost certainly means someone typed a whole
    percentage (20) instead of a fraction (0.20) — silently accepting it
    sizes the copy at up to the entire account, which the portfolio's
    max_single_market_pct then blocks on every single trade forever."""
    if mode in PERCENT_SIZING_MODES and value is not None and value > 1.0:
        raise HTTPException(
            422,
            f"sizing_value for {mode} must be a fraction of balance, not a percentage "
            f"(e.g. 0.20 for 20%) — got {value}, did you mean {value / 100:.4g}?",
        )


class WalletIn(BaseModel):
    address: str
    label: str = ""
    sizing_mode: str | None = None
    sizing_value: float | None = Field(default=None, gt=0)
    multiplier: float = Field(default=1.0, gt=0, le=100)

    @field_validator("address")
    @classmethod
    def check_address(cls, v: str) -> str:
        if not ADDRESS_RE.match(v):
            raise ValueError("address must be a 0x-prefixed 40-hex-char wallet address")
        return v.lower()

    @field_validator("sizing_mode")
    @classmethod
    def check_mode(cls, v: str | None) -> str | None:
        if v is not None and v not in VALID_SIZING_MODES:
            raise ValueError(f"sizing_mode must be one of {VALID_SIZING_MODES}")
        return v


class TradingIn(BaseModel):
    copying: bool


class WalletPatch(BaseModel):
    label: str | None = None
    active: bool | None = None
    sizing_mode: str | None = None
    sizing_value: float | None = Field(default=None, gt=0)
    multiplier: float | None = Field(default=None, gt=0, le=100)

    @field_validator("sizing_mode")
    @classmethod
    def check_mode(cls, v: str | None) -> str | None:
        if v is not None and v not in VALID_SIZING_MODES:
            raise ValueError(f"sizing_mode must be one of {VALID_SIZING_MODES}")
        return v


class TransferIn(BaseModel):
    """Paper-only cash transfer. `address` is a free-text label for the
    user's own reference — never validated against a real chain, never
    used to sign or send anything. No real funds are ever involved."""
    amount: float = Field(gt=0)
    address: str = Field(default="", max_length=200)


def create_app(cfg: Config, store: Store | None = None, engine: Engine | None = None) -> FastAPI:
    """`engine`, if given, is started as a background task for the app's
    lifetime and exposed to route handlers via `app.state.engine`. Building
    it (clients, notifier, scout) is the caller's job — see `run.py` — so
    this module never has to import `clients` or construct the trading
    stack itself; it only serves HTTP and reads/writes the Store."""
    store = store or Store(cfg.db_path)
    store.init_account(cfg.starting_balance)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine_task = None
        if engine is not None:
            app.state.engine = engine
            engine_task = asyncio.create_task(engine.run())
        yield
        if engine_task:
            app.state.engine.stop()
            engine_task.cancel()

    app = FastAPI(title="Polymirror — Polymarket paper copy-trader",
                  lifespan=lifespan)

    # ------------------------------------------------------------- dashboard
    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    def trading_state() -> dict:
        halted = store.get_meta("copy_halted") or ""
        return {"copying": not halted, "halt_reason": halted or None}

    # ------------------------------------------------------------------ api
    @app.get("/api/summary")
    async def summary():
        return {
            "metrics": metrics_mod.compute_metrics(store),
            "allocation": metrics_mod.allocation(store),
            "wallets": store.wallets(),
            "wallet_metrics": metrics_mod.wallet_metrics(store),
            "trading": trading_state(),
        }

    @app.get("/api/trading")
    async def get_trading():
        return trading_state()

    @app.post("/api/trading")
    async def set_trading(body: TradingIn):
        if body.copying:
            store.set_meta("copy_halted", "")
            store.log_event("info", "risk", "Copying resumed manually")
        else:
            store.set_meta("copy_halted", "manual")
            store.log_event("warning", "risk",
                            "Copying HALTED manually — new buys blocked, exits still mirror")
        return trading_state()

    @app.get("/api/analytics")
    async def analytics():
        return metrics_mod.copy_analytics(store, cfg)

    @app.get("/api/scout")
    async def scout_candidates(limit: int = 100):
        last_warning = store.last_event("scout")
        # A sweep that only produced warnings/errors (e.g. every candidate
        # source failing) leaves nothing in `candidates` — surface that
        # failure explicitly rather than let the UI read silence as "fine".
        last_failure = (
            {"ts": last_warning["ts"], "level": last_warning["level"],
             "message": last_warning["message"]}
            if last_warning and last_warning["level"] in ("warning", "error")
            else None
        )
        return {
            "enabled": cfg.scout.enabled,
            "auto_follow": cfg.scout.auto_follow,
            "max_auto_wallets": cfg.scout.max_auto_wallets,
            "candidates": store.candidates(limit=min(limit, 500)),
            "last_failure": last_failure,
        }

    @app.get("/api/positions")
    async def positions(status: str = "open"):
        if status == "open":
            rows = store.open_positions()
        elif status == "closed":
            rows = store.closed_positions()
        else:
            raise HTTPException(400, "status must be open or closed")
        for p in rows:
            mark = p["mark_price"] if p["mark_price"] is not None else p["avg_price"]
            p["market_value"] = mark * p["size"]
            p["unrealized_pnl"] = (mark - p["avg_price"]) * p["size"]
        return rows

    @app.post("/api/positions/{position_id}/close")
    async def close_position(position_id: int):
        engine = getattr(app.state, "engine", None)
        if engine is None:
            raise HTTPException(503, "engine not running")
        result = await engine.close_position(position_id)
        if result is None:
            raise HTTPException(404, "position not open")
        return result

    @app.get("/api/trades")
    async def trades(limit: int = 100):
        return store.trades(limit=min(limit, 1000))

    @app.get("/api/equity")
    async def equity(limit: int = 2000):
        return store.equity_curve(limit=min(limit, 10_000))

    @app.get("/api/events")
    async def events(limit: int = 100, level: str | None = None):
        return store.events(limit=min(limit, 1000), level=level)

    @app.get("/api/wallets")
    async def list_wallets():
        return store.wallets()

    @app.post("/api/wallets", status_code=201)
    async def add_wallet(body: WalletIn):
        mode = body.sizing_mode or cfg.default_sizing.mode
        value = body.sizing_value if body.sizing_value is not None else cfg.default_sizing.value
        _check_sizing(mode, value)
        store.add_wallet(body.address, body.label, mode, value, body.multiplier)
        store.log_event("info", "system", f"Wallet {body.address} added to follow list")
        return store.wallet(body.address)

    @app.patch("/api/wallets/{address}")
    async def patch_wallet(address: str, body: WalletPatch):
        existing = store.wallet(address)
        if existing is None:
            raise HTTPException(404, "wallet not followed")
        fields = {k: (int(v) if isinstance(v, bool) else v)
                  for k, v in body.model_dump(exclude_none=True).items()}
        mode = fields.get("sizing_mode", existing["sizing_mode"])
        value = fields.get("sizing_value", existing["sizing_value"])
        _check_sizing(mode, value)
        store.update_wallet(address, **fields)
        if "active" in fields:
            # A user decision supersedes any pending auto-manage cool-off:
            # a manual pause must never be auto-resumed later.
            store.set_meta(f"autopause_at:{address.lower()}", "")
        return store.wallet(address)

    @app.delete("/api/wallets/{address}", status_code=204)
    async def delete_wallet(address: str):
        if store.wallet(address) is None:
            raise HTTPException(404, "wallet not followed")
        store.remove_wallet(address)
        store.log_event("info", "system",
                        f"Wallet {address} removed (history kept; never eligible for scout re-add)")

    @app.post("/api/reset")
    async def reset():
        store.reset_account(cfg.starting_balance)
        store.log_event("info", "system", "Simulated account reset")
        return {"ok": True, "cash": cfg.starting_balance}

    @app.post("/api/deposit")
    async def deposit(body: TransferIn):
        new_cash = store.deposit(body.amount, body.address)
        label = f" (ref: {body.address})" if body.address else ""
        store.log_event("info", "system",
                        f"Paper deposit of ${body.amount:,.2f}{label} — cash now ${new_cash:,.2f}")
        return store.account()

    @app.post("/api/withdraw")
    async def withdraw(body: TransferIn):
        try:
            new_cash = store.withdraw(body.amount, body.address)
        except InsufficientCash as err:
            raise HTTPException(422, str(err)) from err
        label = f" (ref: {body.address})" if body.address else ""
        store.log_event("info", "system",
                        f"Paper withdrawal of ${body.amount:,.2f}{label} — cash now ${new_cash:,.2f}")
        return store.account()

    @app.get("/api/transfers")
    async def transfers(limit: int = 50):
        return store.transfers(limit=min(limit, 500))

    @app.get("/api/health")
    async def health():
        hb = store.get_meta("heartbeat_ts")
        live = cfg.execution.mode == "live"
        return {
            "ok": True,
            "mode": "live" if live else "paper",
            "live_dry_run": cfg.live.dry_run if live else None,
            "engine_heartbeat_age_s": (int(time.time()) - int(hb)) if hb else None,
            **trading_state(),
        }

    return app
