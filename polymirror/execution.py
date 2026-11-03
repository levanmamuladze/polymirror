"""Live execution: order signing and submission to Polymarket's CLOB.

This is the ONLY module in the project allowed to hold signing material or
talk to a private-key-backed client. It never receives a raw secret from
`Config` — `LiveConfig` only carries the *names* of environment variables;
the actual private key / API credentials are read from `os.environ` at
client construction time and are never logged, persisted, or returned to a
caller.

Layer separation (see CLAUDE.md): this module handles signing and order
submission only. It knows nothing about copy-trading strategy, sizing, or
portfolio risk — `broker.LiveBroker` is the thin adapter that lets
`tracker.Engine` use this instead of `PaperBroker` without either side
knowing the other exists (execution abstraction).

Safety gates (enforced in `Config.validate`, not here — this module trusts
the config it's given has already passed validation):
  * execution.mode must be explicitly "live" (default "paper").
  * live.confirm must be explicitly true.
  * live.max_live_notional_usd must be > 0 (a hard per-order cap enforced
    by LiveBroker, independent of and in addition to execution.max_trade_usd).
  * live.dry_run defaults to true: orders are built and (optionally) signed
    but never submitted to the exchange — only logged — until a human
    flips it off in config.

Mockable: `PolymarketExecutionClient` accepts an injected `clob_client` for
dependency injection in tests. Production code only imports and constructs
the real `py-clob-client` client when one isn't supplied, so the test suite
never needs that optional dependency installed.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Protocol

log = logging.getLogger("polymirror.execution")


class LiveExecutionError(Exception):
    pass


class CredentialError(LiveExecutionError):
    pass


@dataclass
class OrderResult:
    order_id: str
    status: str            # e.g. "matched" | "live" | "unmatched" | "dry_run" | "settled"
    side: str               # BUY | SELL
    filled_size: float      # shares actually filled (0 if resting/unmatched)
    avg_price: float        # actual average fill price
    fee: float
    notional: float
    raw: dict[str, Any]     # raw exchange response, for debugging/audit only


@dataclass
class _OrderArgs:
    """Our own order-args shape — deliberately NOT py-clob-client's
    `OrderArgs`. `submit_order`'s core logic is built against this so it
    never needs the optional SDK importable; only `_RealClobAdapter`
    (constructed exclusively in the real, non-test path) translates to the
    SDK's own types."""
    token_id: str
    side: str        # BUY | SELL
    size: float
    price: float


class _RealClobAdapter:
    """Translates our minimal client shape (`create_order`/`post_order`/
    `get_balance_allowance`) onto the real `py-clob-client` SDK. This is the
    ONLY place that imports SDK-specific types — kept out of
    `PolymarketExecutionClient.submit_order` so that method works
    identically whether it's driving this adapter or a test double."""

    def __init__(self, real_client: Any):
        self._real = real_client

    def create_order(self, args: _OrderArgs) -> Any:
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import BUY, SELL

        sdk_args = OrderArgs(token_id=args.token_id, price=args.price, size=args.size,
                             side=BUY if args.side == "BUY" else SELL)
        return self._real.create_order(sdk_args)

    def post_order(self, signed: Any, order_type: str) -> Any:
        return self._real.post_order(signed, order_type)

    def get_balance_allowance(self) -> Any:
        return self._real.get_balance_allowance()


class ExecutionClient(Protocol):
    """Everything `LiveBroker` needs from a live execution backend. Real
    implementation: `PolymarketExecutionClient`. Tests inject a stub/mock
    satisfying this same shape — never the real client."""

    def submit_order(self, *, token_id: str, side: str, size: float, price: float,
                      order_type: str) -> OrderResult: ...

    def get_usdc_balance(self) -> float: ...


def _read_env(var_name: str, *, required: bool) -> str | None:
    value = os.environ.get(var_name)
    if required and not value:
        raise CredentialError(
            f"environment variable {var_name} is not set — live execution cannot "
            "start without it. Set it in your shell or secret manager; never in "
            "config.yaml."
        )
    return value


class PolymarketExecutionClient:
    """Signs and submits real orders via the optional `py-clob-client`
    package. Constructed once, only when `execution.mode == "live"`;
    `PaperBroker`/paper mode never touches this class."""

    def __init__(self, live_cfg: Any, clob_api_base: str, clob_client: Any | None = None):
        self.dry_run = bool(live_cfg.dry_run)
        self.order_type = live_cfg.order_type
        # Public (non-secret) wallet address — used by tracker.Engine to
        # reconcile our own on-chain positions/balance against Store (see
        # broker.sync_live_cash / Engine.reconcile_own_live_account). Read
        # best-effort here so even the dependency-injection/test path gets
        # it when the env var happens to be set; _build_real_client
        # separately enforces it's required before constructing a real
        # signing client.
        self.funder_address = os.environ.get(live_cfg.funder_address_env, "")
        if clob_client is not None:
            # Dependency injection path — used by tests and by dry_run
            # callers that want to exercise signing against a fake client.
            self._client = clob_client
        else:
            self._client = self._build_real_client(live_cfg, clob_api_base)

    @staticmethod
    def _build_real_client(live_cfg: Any, clob_api_base: str) -> Any:
        try:
            from py_clob_client.client import ClobClient as _RealClobClient
            from py_clob_client.clob_types import ApiCreds
        except ImportError as err:
            raise LiveExecutionError(
                "execution.mode is 'live' but the optional 'py-clob-client' package "
                "is not installed. Run: pip install py-clob-client"
            ) from err

        private_key = _read_env(live_cfg.private_key_env, required=True)
        funder = _read_env(live_cfg.funder_address_env, required=True)
        api_key = _read_env(live_cfg.clob_api_key_env, required=False)
        api_secret = _read_env(live_cfg.clob_api_secret_env, required=False)
        api_passphrase = _read_env(live_cfg.clob_api_passphrase_env, required=False)

        creds = None
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)

        client = _RealClobClient(
            clob_api_base,
            key=private_key,
            chain_id=live_cfg.chain_id,
            creds=creds,
            signature_type=live_cfg.signature_type,
            funder=funder,
        )
        if creds is None:
            # py-clob-client persists no L2 credentials on its own — derive
            # (or create, first run) them from the L1 wallet signature on
            # every process start when they weren't supplied directly.
            derived = client.create_or_derive_api_creds()
            client.set_api_creds(derived)

        log.info(
            "Live execution client initialised (chain_id=%s, signature_type=%s, dry_run=%s) "
            "— credentials read from environment, never logged",
            live_cfg.chain_id, live_cfg.signature_type, live_cfg.dry_run,
        )
        return _RealClobAdapter(client)

    def get_usdc_balance(self) -> float:
        try:
            resp = self._client.get_balance_allowance()
            raw = resp.get("balance", 0.0) if isinstance(resp, dict) else 0.0
            return float(raw) / 1_000_000.0  # USDC uses 6 decimals on Polygon
        except Exception as err:  # noqa: BLE001 — surface as our own error type
            raise LiveExecutionError(f"could not fetch USDC balance: {err}") from err

    def submit_order(self, *, token_id: str, side: str, size: float, price: float,
                      order_type: str = "GTC") -> OrderResult:
        if size <= 0 or price <= 0:
            raise LiveExecutionError(f"invalid order: size={size} price={price}")
        side = side.upper()

        if self.dry_run:
            log.warning(
                "[DRY RUN] would submit %s order: token=%s size=%.4f price=%.4f "
                "notional=$%.2f — NOT sent to the exchange",
                side, token_id, size, price, size * price,
            )
            return OrderResult(
                order_id=f"dry-run-{int(time.time() * 1000)}", status="dry_run",
                side=side, filled_size=size, avg_price=price,
                fee=0.0, notional=size * price, raw={},
            )

        args = _OrderArgs(token_id=token_id, side=side, size=size, price=price)
        signed = self._client.create_order(args)
        resp = self._client.post_order(signed, order_type)

        if not isinstance(resp, dict) or not resp.get("success", False):
            raise LiveExecutionError(f"order rejected by exchange: {resp}")

        filled_size = float(resp.get("makingAmount") or resp.get("size") or size)
        avg_price = float(resp.get("price") or price)
        return OrderResult(
            order_id=str(resp.get("orderID", "")),
            status=str(resp.get("status", "unknown")),
            side=side, filled_size=filled_size, avg_price=avg_price,
            fee=0.0, notional=filled_size * avg_price, raw=resp,
        )


def build_execution_client(cfg: Any) -> PolymarketExecutionClient:
    """Composition-root factory for run.py. Only ever called after
    `Config.validate()` has already enforced the live-mode safety gates;
    this re-checks `execution.mode` defensively so this function itself can
    never be the thing that turns live trading on by accident."""
    if cfg.execution.mode != "live":
        raise LiveExecutionError(
            "build_execution_client() called while execution.mode != 'live'"
        )
    return PolymarketExecutionClient(cfg.live, cfg.clob_api_base)


__all__ = [
    "ExecutionClient", "PolymarketExecutionClient", "OrderResult",
    "LiveExecutionError", "CredentialError", "build_execution_client",
]
