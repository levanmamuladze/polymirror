"""Persistence layer (SQLite, WAL mode).

Single-writer design: the engine loop and the API share one Store instance
guarded by a lock. Volumes here are tiny (a few writes per copied trade),
so SQLite is a deliberate choice over a client/server database — zero ops,
crash-safe with WAL, trivially portable.

Everything the engine needs to survive a restart lives here:
  * followed wallets and their per-wallet sizing config
  * per-wallet polling cursors
  * processed-event keys (idempotency / duplicate suppression)
  * a shadow model of each source trader's positions
  * the simulated account, positions, trades and equity snapshots
  * a notification/event log
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger("polymirror.storage")

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS followed_wallets (
    address        TEXT PRIMARY KEY,
    label          TEXT NOT NULL DEFAULT '',
    active         INTEGER NOT NULL DEFAULT 1,
    sizing_mode    TEXT NOT NULL,
    sizing_value   REAL NOT NULL,
    multiplier     REAL NOT NULL DEFAULT 1.0,
    added_at       INTEGER NOT NULL,
    auto           INTEGER NOT NULL DEFAULT 0   -- 1 = added by the wallet scout
);

-- Permanent record of every address ever followed, independent of whether
-- it's still in `followed_wallets`. A real DELETE removes the row above
-- (and the wallet from the dashboard list) but this table never loses a
-- row, so the scout can never silently re-add something the user
-- explicitly removed.
CREATE TABLE IF NOT EXISTS ever_followed (
    address           TEXT PRIMARY KEY,
    first_followed_at INTEGER NOT NULL
);

-- Wallet-scout evaluations of candidate (not-yet-followed) wallets, built
-- from their public trade history. One row per candidate, refreshed on
-- each evaluation.
CREATE TABLE IF NOT EXISTS scout_candidates (
    wallet              TEXT PRIMARY KEY,
    source              TEXT NOT NULL DEFAULT '',   -- e.g. leaderboard:1m | config
    name                TEXT NOT NULL DEFAULT '',
    evaluated_at        INTEGER NOT NULL,
    closed_trades       INTEGER NOT NULL DEFAULT 0,
    win_rate            REAL,
    profit_factor       REAL,
    realized_pnl        REAL NOT NULL DEFAULT 0,
    max_drawdown_pct    REAL,
    extreme_entry_share REAL,
    concentration       REAL,
    roi_pct             REAL,
    positive_month_ratio REAL,
    score               REAL,                       -- composite 0-100
    passed              INTEGER NOT NULL DEFAULT 0,
    reasons             TEXT NOT NULL DEFAULT '',   -- semicolon-joined fail reasons
    followed            INTEGER NOT NULL DEFAULT 0,
    category_mix        TEXT NOT NULL DEFAULT '{}'  -- JSON {category: buy-notional share}
);

CREATE TABLE IF NOT EXISTS cursors (
    wallet    TEXT PRIMARY KEY,
    last_ts   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_key    TEXT PRIMARY KEY,
    wallet       TEXT NOT NULL,
    ts           INTEGER NOT NULL,
    processed_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_processed_wallet_ts ON processed_events (wallet, ts);

-- Shadow model of the source trader's holdings, rebuilt from their trade
-- stream and corrected by periodic /positions reconciliation.
CREATE TABLE IF NOT EXISTS source_positions (
    wallet       TEXT NOT NULL,
    asset        TEXT NOT NULL,
    condition_id TEXT NOT NULL DEFAULT '',
    size         REAL NOT NULL,
    avg_price    REAL NOT NULL,
    title        TEXT NOT NULL DEFAULT '',
    outcome      TEXT NOT NULL DEFAULT '',
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (wallet, asset)
);

CREATE TABLE IF NOT EXISTS account (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    starting_balance REAL NOT NULL,
    cash             REAL NOT NULL,
    total_deposits    REAL NOT NULL DEFAULT 0.0,
    total_withdrawals REAL NOT NULL DEFAULT 0.0
);

-- Paper-only cash transfers: a "deposit"/"withdraw" only ever adjusts the
-- simulated cash balance. `address` is a free-text label the user types
-- for their own reference — it is never validated against a real chain,
-- never signed, never sent anywhere. No real funds are ever involved.
CREATE TABLE IF NOT EXISTS cash_transfers (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         INTEGER NOT NULL,
    kind       TEXT NOT NULL,   -- deposit | withdraw
    amount     REAL NOT NULL,
    address    TEXT NOT NULL DEFAULT '',
    cash_after REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cash_transfers_ts ON cash_transfers (ts);

CREATE TABLE IF NOT EXISTS sim_positions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet       TEXT NOT NULL,           -- source wallet being mirrored
    asset        TEXT NOT NULL,           -- outcome token id
    condition_id TEXT NOT NULL DEFAULT '',
    title        TEXT NOT NULL DEFAULT '',
    outcome      TEXT NOT NULL DEFAULT '',
    size         REAL NOT NULL,
    avg_price    REAL NOT NULL,
    mark_price   REAL,
    opened_at    INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    status       TEXT NOT NULL DEFAULT 'open',   -- open | closed
    closed_at    INTEGER,
    realized_pnl REAL NOT NULL DEFAULT 0.0
);
CREATE INDEX IF NOT EXISTS idx_sim_positions_status ON sim_positions (status);
-- Only OPEN positions must be unique per (wallet, asset) — a wallet can
-- legitimately open and fully close the same asset many times over its
-- history, so uniqueness must NOT span closed rows (see _migrate() for
-- why a blanket UNIQUE(wallet,asset,status) is actively dangerous: it lets
-- a position's cash already be credited by the broker before the closing
-- UPDATE fails against an earlier closed row for the same asset).
CREATE UNIQUE INDEX IF NOT EXISTS idx_sim_positions_one_open
    ON sim_positions (wallet, asset) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS sim_trades (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key    TEXT NOT NULL,
    wallet       TEXT NOT NULL,
    asset        TEXT NOT NULL,
    condition_id TEXT NOT NULL DEFAULT '',
    title        TEXT NOT NULL DEFAULT '',
    outcome      TEXT NOT NULL DEFAULT '',
    side         TEXT NOT NULL,            -- BUY | SELL
    kind         TEXT NOT NULL,            -- OPEN | ADD | PARTIAL_EXIT | FULL_EXIT | REDEEM
    size         REAL NOT NULL,            -- shares
    price        REAL NOT NULL,            -- simulated fill price (after slippage)
    source_price REAL NOT NULL,            -- price the source trader got
    fee          REAL NOT NULL DEFAULT 0,
    notional     REAL NOT NULL,            -- size * price
    realized_pnl REAL NOT NULL DEFAULT 0,
    source_ts    INTEGER NOT NULL,         -- timestamp of the source trade
    executed_at  INTEGER NOT NULL,
    tx_hash      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_sim_trades_ts ON sim_trades (source_ts);

CREATE TABLE IF NOT EXISTS equity_snapshots (
    ts            INTEGER NOT NULL,
    equity        REAL NOT NULL,
    cash          REAL NOT NULL,
    unrealized    REAL NOT NULL,
    realized      REAL NOT NULL,
    net_transfers REAL NOT NULL DEFAULT 0.0,  -- cumulative deposits - withdrawals as of this snapshot
    PRIMARY KEY (ts)
);

CREATE TABLE IF NOT EXISTS event_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      INTEGER NOT NULL,
    level   TEXT NOT NULL,       -- info | warning | error
    kind    TEXT NOT NULL,       -- trade_detected | trade_copied | trade_skipped | sync | system
    message TEXT NOT NULL,
    data    TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log (ts);
"""


class Store:
    def __init__(self, db_path: str | Path):
        path = Path(db_path)
        if str(path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Additive migrations for databases created before a column existed."""
        for stmt in (
            "ALTER TABLE followed_wallets ADD COLUMN auto INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE scout_candidates ADD COLUMN roi_pct REAL",
            "ALTER TABLE scout_candidates ADD COLUMN positive_month_ratio REAL",
            "ALTER TABLE scout_candidates ADD COLUMN category_mix TEXT NOT NULL DEFAULT '{}'",
            "ALTER TABLE account ADD COLUMN total_deposits REAL NOT NULL DEFAULT 0.0",
            "ALTER TABLE account ADD COLUMN total_withdrawals REAL NOT NULL DEFAULT 0.0",
            "ALTER TABLE equity_snapshots ADD COLUMN net_transfers REAL NOT NULL DEFAULT 0.0",
        ):
            try:
                self._exec(stmt)
            except sqlite3.OperationalError:
                pass  # column already exists
        self._migrate_sim_positions_unique_constraint()

    def _migrate_sim_positions_unique_constraint(self) -> None:
        """Databases created before this fix have a blanket
        UNIQUE(wallet, asset, status) on sim_positions, which only allows
        ONE closed position ever per (wallet, asset) — closing a second
        position on the same asset violates it. SQLite can't drop a
        table-level constraint via ALTER TABLE, so this recreates the
        table without it (data preserved) and adds the correct partial
        unique index instead. Idempotent: a no-op once migrated."""
        row = self._row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='sim_positions'"
        )
        if not row or "UNIQUE (wallet, asset, status)" not in (row["sql"] or ""):
            return
        with self._lock, self._conn:
            self._conn.executescript("""
                CREATE TABLE sim_positions_new (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet       TEXT NOT NULL,
                    asset        TEXT NOT NULL,
                    condition_id TEXT NOT NULL DEFAULT '',
                    title        TEXT NOT NULL DEFAULT '',
                    outcome      TEXT NOT NULL DEFAULT '',
                    size         REAL NOT NULL,
                    avg_price    REAL NOT NULL,
                    mark_price   REAL,
                    opened_at    INTEGER NOT NULL,
                    updated_at   INTEGER NOT NULL,
                    status       TEXT NOT NULL DEFAULT 'open',
                    closed_at    INTEGER,
                    realized_pnl REAL NOT NULL DEFAULT 0.0
                );
                INSERT INTO sim_positions_new
                    SELECT id, wallet, asset, condition_id, title, outcome, size,
                           avg_price, mark_price, opened_at, updated_at, status,
                           closed_at, realized_pnl
                    FROM sim_positions;
                DROP TABLE sim_positions;
                ALTER TABLE sim_positions_new RENAME TO sim_positions;
                CREATE INDEX IF NOT EXISTS idx_sim_positions_status ON sim_positions (status);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_sim_positions_one_open
                    ON sim_positions (wallet, asset) WHERE status = 'open';
            """)
            log.warning(
                "Migrated sim_positions off the blanket UNIQUE(wallet,asset,status) "
                "constraint to a partial unique index (open positions only)."
            )

    # ------------------------------------------------------------------ util
    def _rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(sql, tuple(params))
            return [dict(r) for r in cur.fetchall()]

    def _row(self, sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
        rows = self._rows(sql, params)
        return rows[0] if rows else None

    def _exec(self, sql: str, params: Iterable[Any] = ()) -> None:
        with self._lock, self._conn:
            self._conn.execute(sql, tuple(params))

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --------------------------------------------------------------- account
    def init_account(self, starting_balance: float) -> None:
        with self._lock, self._conn:
            row = self._conn.execute("SELECT 1 FROM account WHERE id = 1").fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO account (id, starting_balance, cash) VALUES (1, ?, ?)",
                    (starting_balance, starting_balance),
                )

    def account(self) -> dict[str, Any]:
        row = self._row(
            "SELECT starting_balance, cash, total_deposits, total_withdrawals "
            "FROM account WHERE id = 1"
        )
        assert row is not None, "account not initialised"
        return row

    def adjust_cash(self, delta: float) -> float:
        """Atomically apply a cash delta; raises if it would go negative."""
        with self._lock, self._conn:
            row = self._conn.execute("SELECT cash FROM account WHERE id = 1").fetchone()
            new_cash = row["cash"] + delta
            if new_cash < -1e-9:
                raise InsufficientCash(f"cash would become {new_cash:.2f}")
            self._conn.execute("UPDATE account SET cash = ? WHERE id = 1", (new_cash,))
            return new_cash

    def apply_fill(self, cash_delta: float, position: dict[str, Any]) -> tuple[float, int]:
        """Adjust cash and save the position for one fill in a SINGLE
        transaction — a failure in either half rolls back both, so cash
        can never be committed without the matching position update (or
        vice versa).

        This closes a real incident: broker.buy/sell used to call
        adjust_cash() and save_position() as two independent transactions.
        When a position's closing UPDATE hit the (now-removed) blanket
        UNIQUE(wallet,asset,status) constraint, the exception surfaced
        after the cash credit had already committed — leaving a phantom
        credit with no matching trade row, repeating on every mark cycle
        the position was re-evaluated. Whatever future failure mode might
        hit this path, apply_fill() guarantees it can't repeat that
        specific damage: cash and position always move together or not
        at all."""
        now = int(time.time())
        with self._lock, self._conn:
            row = self._conn.execute("SELECT cash FROM account WHERE id = 1").fetchone()
            new_cash = row["cash"] + cash_delta
            if new_cash < -1e-9:
                raise InsufficientCash(f"cash would become {new_cash:.2f}")
            self._conn.execute("UPDATE account SET cash = ? WHERE id = 1", (new_cash,))
            if position.get("id"):
                self._conn.execute(
                    """UPDATE sim_positions SET size=?, avg_price=?, mark_price=?, updated_at=?,
                           status=?, closed_at=?, realized_pnl=? WHERE id=?""",
                    (position["size"], position["avg_price"], position.get("mark_price"), now,
                     position["status"], position.get("closed_at"),
                     position.get("realized_pnl", 0.0), position["id"]),
                )
                pos_id = int(position["id"])
            else:
                cur = self._conn.execute(
                    """INSERT INTO sim_positions
                       (wallet, asset, condition_id, title, outcome, size, avg_price,
                        mark_price, opened_at, updated_at, status, realized_pnl)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', 0.0)""",
                    (position["wallet"], position["asset"], position.get("condition_id", ""),
                     position.get("title", ""), position.get("outcome", ""), position["size"],
                     position["avg_price"], position.get("mark_price"), now, now),
                )
                pos_id = int(cur.lastrowid)
            return new_cash, pos_id

    def bootstrap_live_account(self, real_cash: float) -> None:
        """One-time ground-truth reset for live mode: (re)establishes the
        account row with `real_cash` (the actual on-chain USDC balance) as
        both cash and starting_balance, and zeroes the deposit/withdrawal
        counters. Live trading needs a clean starting point sourced from
        the real wallet, not whatever paper-mode history (if any) happens
        to already be sitting in this database — see broker.sync_live_cash,
        which calls this exactly once per Store, guarded by the
        `live_cash_synced_at` meta flag so it can never clobber real
        trading history once live mode is actually running.

        Deliberately a distinct, narrowly-named method rather than a
        general-purpose "set cash" setter — trading code has no legitimate
        reason to ever call this."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO account "
                "(id, starting_balance, cash, total_deposits, total_withdrawals) "
                "VALUES (1, ?, ?, 0.0, 0.0)",
                (real_cash, real_cash),
            )

    def reset_account(self, starting_balance: float) -> None:
        with self._lock, self._conn:
            for table in ("sim_positions", "sim_trades", "equity_snapshots",
                          "processed_events", "source_positions", "cursors", "event_log",
                          "cash_transfers"):
                self._conn.execute(f"DELETE FROM {table}")
            # Risk state derives from the wiped history — clear it too, or a
            # reset account would stay halted / guard-locked on stale evidence.
            self._conn.execute(
                "DELETE FROM meta WHERE key IN ('copy_halted', 'halt_equity_mark', 'halt_day', "
                "'auto_withdraw_swept_usd') OR key LIKE 'autopause%'"
            )
            # total_deposits/total_withdrawals reset to their column defaults (0.0) —
            # INSERT OR REPLACE deletes+reinserts, so omitted columns aren't preserved.
            self._conn.execute(
                "INSERT OR REPLACE INTO account (id, starting_balance, cash) VALUES (1, ?, ?)",
                (starting_balance, starting_balance),
            )

    def deposit(self, amount: float, address: str = "") -> float:
        """Paper-only: adds `amount` to simulated cash. `address` is a
        free-text label for the user's own reference — never validated
        against a real chain, never used to move real funds."""
        if amount <= 0:
            raise ValueError("deposit amount must be positive")
        with self._lock, self._conn:
            row = self._conn.execute("SELECT cash FROM account WHERE id = 1").fetchone()
            new_cash = row["cash"] + amount
            self._conn.execute(
                "UPDATE account SET cash = ?, total_deposits = total_deposits + ? WHERE id = 1",
                (new_cash, amount),
            )
            self._conn.execute(
                "INSERT INTO cash_transfers (ts, kind, amount, address, cash_after) "
                "VALUES (?, 'deposit', ?, ?, ?)",
                (int(time.time()), amount, address, new_cash),
            )
            return new_cash

    def withdraw(self, amount: float, address: str = "") -> float:
        """Paper-only: removes `amount` from simulated cash; raises
        InsufficientCash if it exceeds available cash. Same no-real-funds
        guarantee as deposit() — `address` is a label only."""
        if amount <= 0:
            raise ValueError("withdraw amount must be positive")
        with self._lock, self._conn:
            row = self._conn.execute("SELECT cash FROM account WHERE id = 1").fetchone()
            new_cash = row["cash"] - amount
            if new_cash < -1e-9:
                raise InsufficientCash(f"cash would become {new_cash:.2f}")
            self._conn.execute(
                "UPDATE account SET cash = ?, total_withdrawals = total_withdrawals + ? WHERE id = 1",
                (new_cash, amount),
            )
            self._conn.execute(
                "INSERT INTO cash_transfers (ts, kind, amount, address, cash_after) "
                "VALUES (?, 'withdraw', ?, ?, ?)",
                (int(time.time()), amount, address, new_cash),
            )
            return new_cash

    def transfers(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM cash_transfers ORDER BY id DESC LIMIT ?", (limit,))

    # --------------------------------------------------------------- wallets
    def add_wallet(self, address: str, label: str, sizing_mode: str,
                   sizing_value: float, multiplier: float, auto: bool = False) -> None:
        address = address.lower()
        now = int(time.time())
        self._exec(
            """INSERT INTO followed_wallets
               (address, label, active, sizing_mode, sizing_value, multiplier, added_at, auto)
               VALUES (?, ?, 1, ?, ?, ?, ?, ?)
               ON CONFLICT(address) DO UPDATE SET
                 label=excluded.label, active=1, sizing_mode=excluded.sizing_mode,
                 sizing_value=excluded.sizing_value, multiplier=excluded.multiplier,
                 auto=excluded.auto""",
            (address, label, sizing_mode, sizing_value, multiplier, now, int(auto)),
        )
        self._exec(
            "INSERT OR IGNORE INTO ever_followed (address, first_followed_at) VALUES (?, ?)",
            (address, now),
        )

    def was_ever_followed(self, address: str) -> bool:
        return self._row(
            "SELECT 1 AS x FROM ever_followed WHERE address = ?", (address.lower(),)
        ) is not None

    def ever_followed_addresses(self) -> set[str]:
        return {r["address"] for r in self._rows("SELECT address FROM ever_followed")}

    def update_wallet(self, address: str, **fields: Any) -> None:
        allowed = {"label", "active", "sizing_mode", "sizing_value", "multiplier"}
        sets, params = [], []
        for k, v in fields.items():
            if k not in allowed:
                raise ValueError(f"cannot update field {k}")
            sets.append(f"{k} = ?")
            params.append(v)
        if not sets:
            return
        params.append(address.lower())
        self._exec(f"UPDATE followed_wallets SET {', '.join(sets)} WHERE address = ?", params)

    def remove_wallet(self, address: str) -> None:
        # Real delete: the wallet disappears from the followed list (no more
        # clutter from wallets the user explicitly removed). Trade/position
        # history is untouched — sim_trades/sim_positions key on the address
        # string, not a foreign key into this table. The permanent
        # `ever_followed` row is never removed, so the scout can never
        # silently re-add this address later.
        self._exec("DELETE FROM followed_wallets WHERE address = ?", (address.lower(),))

    def wallets(self, active_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM followed_wallets"
        if active_only:
            sql += " WHERE active = 1"
        return self._rows(sql + " ORDER BY added_at")

    def wallet(self, address: str) -> dict[str, Any] | None:
        return self._row("SELECT * FROM followed_wallets WHERE address = ?", (address.lower(),))

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str) -> str | None:
        row = self._row("SELECT value FROM meta WHERE key = ?", (key,))
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self._exec(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    # ------------------------------------------------------- scout candidates
    def upsert_candidate(self, c: dict[str, Any]) -> None:
        self._exec(
            """INSERT INTO scout_candidates
               (wallet, source, name, evaluated_at, closed_trades, win_rate,
                profit_factor, realized_pnl, max_drawdown_pct, extreme_entry_share,
                concentration, roi_pct, positive_month_ratio, score, passed, reasons, followed,
                category_mix)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(wallet) DO UPDATE SET
                 source=excluded.source, name=excluded.name,
                 evaluated_at=excluded.evaluated_at, closed_trades=excluded.closed_trades,
                 win_rate=excluded.win_rate, profit_factor=excluded.profit_factor,
                 realized_pnl=excluded.realized_pnl, max_drawdown_pct=excluded.max_drawdown_pct,
                 extreme_entry_share=excluded.extreme_entry_share,
                 concentration=excluded.concentration, roi_pct=excluded.roi_pct,
                 positive_month_ratio=excluded.positive_month_ratio, score=excluded.score,
                 passed=excluded.passed, reasons=excluded.reasons,
                 followed=excluded.followed, category_mix=excluded.category_mix""",
            (c["wallet"].lower(), c.get("source", ""), c.get("name", ""),
             c["evaluated_at"], c.get("closed_trades", 0), c.get("win_rate"),
             c.get("profit_factor"), c.get("realized_pnl", 0.0), c.get("max_drawdown_pct"),
             c.get("extreme_entry_share"), c.get("concentration"), c.get("roi_pct"),
             c.get("positive_month_ratio"), c.get("score"),
             int(c.get("passed", False)), c.get("reasons", ""), int(c.get("followed", False)),
             json.dumps(c.get("category_mix") or {})),
        )

    def candidate(self, wallet: str) -> dict[str, Any] | None:
        return self._row("SELECT * FROM scout_candidates WHERE wallet = ?", (wallet.lower(),))

    def candidates(self, limit: int = 200) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT * FROM scout_candidates "
            "ORDER BY passed DESC, score DESC, evaluated_at DESC LIMIT ?",
            (limit,),
        )

    def set_candidate_followed(self, wallet: str) -> None:
        self._exec("UPDATE scout_candidates SET followed = 1 WHERE wallet = ?", (wallet.lower(),))

    # --------------------------------------------------------------- cursors
    def get_cursor(self, wallet: str) -> int:
        row = self._row("SELECT last_ts FROM cursors WHERE wallet = ?", (wallet,))
        return int(row["last_ts"]) if row else 0

    def set_cursor(self, wallet: str, last_ts: int) -> None:
        self._exec(
            "INSERT INTO cursors (wallet, last_ts) VALUES (?, ?) "
            "ON CONFLICT(wallet) DO UPDATE SET last_ts = MAX(last_ts, excluded.last_ts)",
            (wallet, last_ts),
        )

    # ------------------------------------------------------------ idempotency
    def is_processed(self, event_key: str) -> bool:
        return self._row("SELECT 1 AS x FROM processed_events WHERE event_key = ?", (event_key,)) is not None

    def mark_processed(self, event_key: str, wallet: str, ts: int) -> bool:
        """Returns False if the event was already recorded (duplicate)."""
        try:
            self._exec(
                "INSERT INTO processed_events (event_key, wallet, ts, processed_at) VALUES (?, ?, ?, ?)",
                (event_key, wallet, ts, int(time.time())),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    # -------------------------------------------------------- source shadow
    def source_position(self, wallet: str, asset: str) -> dict[str, Any] | None:
        return self._row(
            "SELECT * FROM source_positions WHERE wallet = ? AND asset = ?", (wallet, asset)
        )

    def upsert_source_position(self, wallet: str, asset: str, condition_id: str,
                               size: float, avg_price: float, title: str, outcome: str) -> None:
        if size <= 1e-9:
            self._exec("DELETE FROM source_positions WHERE wallet = ? AND asset = ?", (wallet, asset))
            return
        self._exec(
            """INSERT INTO source_positions
               (wallet, asset, condition_id, size, avg_price, title, outcome, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(wallet, asset) DO UPDATE SET
                 condition_id=excluded.condition_id, size=excluded.size,
                 avg_price=excluded.avg_price, title=excluded.title,
                 outcome=excluded.outcome, updated_at=excluded.updated_at""",
            (wallet, asset, condition_id, size, avg_price, title, outcome, int(time.time())),
        )

    def source_positions(self, wallet: str) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM source_positions WHERE wallet = ?", (wallet,))

    # ----------------------------------------------------------- sim trading
    def open_position(self, wallet: str, asset: str) -> dict[str, Any] | None:
        return self._row(
            "SELECT * FROM sim_positions WHERE wallet = ? AND asset = ? AND status = 'open'",
            (wallet, asset),
        )

    def position(self, position_id: int) -> dict[str, Any] | None:
        return self._row("SELECT * FROM sim_positions WHERE id = ?", (position_id,))

    def open_positions(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM sim_positions WHERE status = 'open' ORDER BY opened_at")

    def closed_positions(self, limit: int = 200) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT * FROM sim_positions WHERE status = 'closed' ORDER BY closed_at DESC LIMIT ?",
            (limit,),
        )

    def positions_for_wallet(self, wallet: str) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT * FROM sim_positions WHERE wallet = ? ORDER BY opened_at",
            (wallet.lower(),),
        )

    def last_trade_ts(self, wallet: str) -> int | None:
        row = self._row(
            "SELECT MAX(source_ts) AS ts FROM sim_trades WHERE wallet = ?", (wallet.lower(),)
        )
        return int(row["ts"]) if row and row["ts"] is not None else None

    def save_position(self, pos: dict[str, Any]) -> int:
        now = int(time.time())
        if pos.get("id"):
            self._exec(
                """UPDATE sim_positions SET size=?, avg_price=?, mark_price=?, updated_at=?,
                       status=?, closed_at=?, realized_pnl=? WHERE id=?""",
                (pos["size"], pos["avg_price"], pos.get("mark_price"), now,
                 pos["status"], pos.get("closed_at"), pos.get("realized_pnl", 0.0), pos["id"]),
            )
            return int(pos["id"])
        with self._lock, self._conn:
            cur = self._conn.execute(
                """INSERT INTO sim_positions
                   (wallet, asset, condition_id, title, outcome, size, avg_price,
                    mark_price, opened_at, updated_at, status, realized_pnl)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', 0.0)""",
                (pos["wallet"], pos["asset"], pos.get("condition_id", ""), pos.get("title", ""),
                 pos.get("outcome", ""), pos["size"], pos["avg_price"], pos.get("mark_price"),
                 now, now),
            )
            return int(cur.lastrowid)

    def set_mark(self, position_id: int, mark_price: float) -> None:
        self._exec(
            "UPDATE sim_positions SET mark_price = ?, updated_at = ? WHERE id = ?",
            (mark_price, int(time.time()), position_id),
        )

    def record_trade(self, trade: dict[str, Any]) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                """INSERT INTO sim_trades
                   (event_key, wallet, asset, condition_id, title, outcome, side, kind, size,
                    price, source_price, fee, notional, realized_pnl, source_ts, executed_at, tx_hash)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade["event_key"], trade["wallet"], trade["asset"], trade.get("condition_id", ""),
                 trade.get("title", ""), trade.get("outcome", ""), trade["side"], trade["kind"],
                 trade["size"], trade["price"], trade["source_price"], trade.get("fee", 0.0),
                 trade["notional"], trade.get("realized_pnl", 0.0), trade["source_ts"],
                 int(time.time()), trade.get("tx_hash", "")),
            )
            return int(cur.lastrowid)

    def trades(self, limit: int = 200) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM sim_trades ORDER BY id DESC LIMIT ?", (limit,))

    def trades_for_wallet(self, wallet: str) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT * FROM sim_trades WHERE wallet = ? ORDER BY id", (wallet.lower(),)
        )

    def all_trades(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM sim_trades ORDER BY id")

    def realized_pnl_total(self) -> float:
        # Positions are the single source of truth: the broker accumulates
        # fee-adjusted realized PnL on each position across partial exits.
        row = self._row("SELECT COALESCE(SUM(realized_pnl), 0) AS pnl FROM sim_positions")
        return float(row["pnl"]) if row else 0.0

    # --------------------------------------------------------------- metrics
    def snapshot_equity(self, ts: int, equity: float, cash: float,
                        unrealized: float, realized: float) -> None:
        acct = self._row("SELECT total_deposits, total_withdrawals FROM account WHERE id = 1")
        net_transfers = (
            float(acct["total_deposits"]) - float(acct["total_withdrawals"]) if acct else 0.0
        )
        self._exec(
            "INSERT OR REPLACE INTO equity_snapshots "
            "(ts, equity, cash, unrealized, realized, net_transfers) VALUES (?, ?, ?, ?, ?, ?)",
            (ts, equity, cash, unrealized, realized, net_transfers),
        )

    def equity_curve(self, limit: int = 2000) -> list[dict[str, Any]]:
        rows = self._rows("SELECT * FROM equity_snapshots ORDER BY ts DESC LIMIT ?", (limit,))
        return list(reversed(rows))

    def first_equity_since(self, ts: int) -> float | None:
        """Earliest *trading-adjusted* equity (deposits/withdrawals backed
        out) at/after `ts` (e.g. start of the UTC day) — a deposit or
        withdrawal must never look like a trading gain or loss."""
        row = self._row(
            "SELECT equity - net_transfers AS e FROM equity_snapshots "
            "WHERE ts >= ? ORDER BY ts LIMIT 1", (ts,)
        )
        return float(row["e"]) if row else None

    def peak_equity(self) -> float | None:
        """Peak *trading-adjusted* equity (deposits/withdrawals backed out)
        — see first_equity_since()."""
        row = self._row("SELECT MAX(equity - net_transfers) AS peak FROM equity_snapshots")
        return float(row["peak"]) if row and row["peak"] is not None else None

    # ------------------------------------------------------------- event log
    def log_event(self, level: str, kind: str, message: str, data: dict | None = None) -> None:
        self._exec(
            "INSERT INTO event_log (ts, level, kind, message, data) VALUES (?, ?, ?, ?, ?)",
            (int(time.time()), level, kind, message, json.dumps(data or {})),
        )

    def events(self, limit: int = 200, level: str | None = None) -> list[dict[str, Any]]:
        if level:
            return self._rows(
                "SELECT * FROM event_log WHERE level = ? ORDER BY id DESC LIMIT ?", (level, limit)
            )
        return self._rows("SELECT * FROM event_log ORDER BY id DESC LIMIT ?", (limit,))

    def last_event(self, kind: str) -> dict[str, Any] | None:
        return self._row(
            "SELECT * FROM event_log WHERE kind = ? ORDER BY id DESC LIMIT 1", (kind,)
        )


class InsufficientCash(Exception):
    pass
