"""Notification layer.

Every notification is always written to the persistent event log (visible
in the dashboard). Optionally it is also pushed to Discord, Telegram, or a
generic webhook, depending on config. Push failures are logged and never
interrupt the engine.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .config import NotificationConfig
from .storage import Store

log = logging.getLogger("polymirror.notify")

_LEVELS = {"info": 0, "warning": 1, "error": 2}


class Notifier:
    def __init__(self, cfg: NotificationConfig, store: Store):
        self.cfg = cfg
        self.store = store
        self._client = httpx.AsyncClient(timeout=10.0)

    async def send(self, level: str, kind: str, message: str,
                   data: dict[str, Any] | None = None) -> None:
        self.store.log_event(level, kind, message, data)
        getattr(log, level if level in ("info", "warning", "error") else "info")(
            "[%s] %s", kind, message
        )
        if not self.cfg.enabled:
            return
        if _LEVELS.get(level, 0) < _LEVELS.get(self.cfg.min_level, 0):
            return
        await asyncio.gather(
            self._discord(level, message),
            self._telegram(level, message),
            self._webhook(level, kind, message, data),
            return_exceptions=True,
        )

    async def _discord(self, level: str, message: str) -> None:
        if not self.cfg.discord_webhook_url:
            return
        try:
            await self._client.post(
                self.cfg.discord_webhook_url,
                json={"content": f"**[{level.upper()}]** {message}"},
            )
        except httpx.HTTPError as err:
            log.warning("Discord notification failed: %s", err)

    async def _telegram(self, level: str, message: str) -> None:
        if not (self.cfg.telegram_bot_token and self.cfg.telegram_chat_id):
            return
        try:
            await self._client.post(
                f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/sendMessage",
                json={"chat_id": self.cfg.telegram_chat_id,
                      "text": f"[{level.upper()}] {message}"},
            )
        except httpx.HTTPError as err:
            log.warning("Telegram notification failed: %s", err)

    async def _webhook(self, level: str, kind: str, message: str,
                       data: dict[str, Any] | None) -> None:
        if not self.cfg.generic_webhook_url:
            return
        try:
            await self._client.post(
                self.cfg.generic_webhook_url,
                json={"level": level, "kind": kind, "message": message, "data": data or {}},
            )
        except httpx.HTTPError as err:
            log.warning("Webhook notification failed: %s", err)

    async def aclose(self) -> None:
        await self._client.aclose()
