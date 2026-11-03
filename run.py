"""Entry point: python run.py [--config config.yaml]"""
from __future__ import annotations

import argparse
import logging
import logging.handlers
from pathlib import Path

import uvicorn

from polymirror.api import create_app
from polymirror.broker import LiveBroker, PaperBroker, sync_live_cash
from polymirror.clients import build_clients
from polymirror.config import load_config
from polymirror.notify import Notifier
from polymirror.scout import WalletScout
from polymirror.storage import Store
from polymirror.tracker import Engine


def build_broker(cfg, store: Store):
    """Choose the execution backend. This is the ONLY place in the project
    that decides paper vs. live and the only place that imports
    `execution.py` — `tracker.Engine` just receives whichever broker comes
    out of here, so it never has to know or care which mode it's in
    (execution abstraction). `Config.validate()` has already refused to
    load a 'live' config that isn't fully and explicitly opted in, so
    reaching the live branch here means the operator meant it."""
    if cfg.execution.mode != "live":
        return PaperBroker(store, cfg.execution.slippage_bps, cfg.execution.fee_bps)

    from polymirror.execution import LiveExecutionError, build_execution_client
    client = build_execution_client(cfg)

    # Verify (and, on a brand-new Store, bootstrap) the cash ledger against
    # the REAL on-chain USDC balance before anything is allowed to trade.
    # cfg.starting_balance is a paper-mode concept; live mode must never
    # trade against a fictitious number. Failure here is fatal — refusing
    # to start beats trading against an unverified balance.
    try:
        msg = sync_live_cash(store, client)
    except Exception as err:  # noqa: BLE001 — surfaced as our own error type below
        raise LiveExecutionError(
            f"could not verify on-chain USDC balance at startup — refusing to start "
            f"live trading: {err}"
        ) from err
    if msg:
        logging.getLogger("polymirror").warning(msg)

    logging.getLogger("polymirror").warning(
        "LIVE TRADING ENABLED — orders will use REAL funds "
        "(dry_run=%s, max_live_notional_usd=%s)",
        cfg.live.dry_run, cfg.live.max_live_notional_usd,
    )
    return LiveBroker(store, client, cfg.live.max_live_notional_usd,
                      cfg.execution.slippage_bps, cfg.live.order_type,
                      funder_address=client.funder_address)


def build_engine(cfg, store: Store) -> Engine:
    """Wire the trading stack together: clients -> notifier -> scout ->
    broker -> engine. Kept here (not in api.py) so the HTTP layer never has
    to import clients or construct the trading stack itself — it only ever
    receives an already-built Engine."""
    data, clob, gamma, leaderboard = build_clients(cfg)
    notifier = Notifier(cfg.notifications, store)
    scout = (WalletScout(cfg, store, data, leaderboard, notifier, gamma=gamma)
             if cfg.scout.enabled else None)
    broker = build_broker(cfg, store)
    return Engine(cfg, store, data, clob, gamma, notifier, scout=scout, broker=broker)


def setup_logging(cfg) -> None:
    Path(cfg.log_path).parent.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        cfg.log_path, maxBytes=int(cfg.logging.rotate_mb * 1_000_000),
        backupCount=cfg.logging.backups, encoding="utf-8",
    )
    console = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    handler.setFormatter(fmt)
    console.setFormatter(fmt)
    logging.basicConfig(level=getattr(logging, cfg.logging.level.upper(), logging.INFO),
                        handlers=[handler, console])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket copy-trading engine (paper by default; live is opt-in)")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg)

    # Trust the OS certificate store (not just Python's bundled CA list).
    # Antivirus/VPN/corporate software that inspects HTTPS re-signs traffic
    # with its own root CA, which lives in the Windows store — without this,
    # every Polymarket request dies with CERTIFICATE_VERIFY_FAILED. Full
    # certificate verification stays ON; only the trust source changes.
    try:
        import truststore
        truststore.inject_into_ssl()
        logging.getLogger("polymirror").info("TLS: using the operating-system trust store")
    except ImportError:
        logging.getLogger("polymirror").warning(
            "TLS: 'truststore' not installed (pip install -r requirements.txt) — "
            "if requests fail with CERTIFICATE_VERIFY_FAILED, that is why")
    if cfg.execution.mode == "live":
        logging.getLogger("polymirror").warning(
            "Starting in LIVE mode — real signing and real order submission are active."
        )
    else:
        logging.getLogger("polymirror").info(
            "Starting in PAPER-ONLY mode — no keys, no signing, no live orders."
        )
    store = Store(cfg.db_path)
    engine = build_engine(cfg, store)
    app = create_app(cfg, store=store, engine=engine)
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="warning")


if __name__ == "__main__":
    main()
