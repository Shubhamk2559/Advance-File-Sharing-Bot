"""Process-wide runtime state of the auto-poster. `db` is attached at startup (see integration.attach)."""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from .config import SETTINGS

if TYPE_CHECKING:
    from .database import Database


class AppState:
    def __init__(self) -> None:
        self.db: "Database" = None  # type: ignore[assignment]
        self.manager: Any = None                      # BotManager (needed for the post button's deep link)
        self.posting_locks: dict[int, asyncio.Lock] = {}
        self.title_cache: dict[int, str | None] = {}
        self.media_group_buffers: dict[str, dict[str, Any]] = {}
        self.debounce: dict[str, asyncio.Task] = {}   # upload-counter message refresh, album flush
        self.imports: dict[int, asyncio.Task] = {}    # running channel->posts imports (admin id -> task)
        self.started_monotonic = time.monotonic()

    def lock_for(self, chat_id: int) -> asyncio.Lock:
        lock = self.posting_locks.get(chat_id)
        if lock is None:
            lock = self.posting_locks[chat_id] = asyncio.Lock()
        return lock

    @property
    def uptime_seconds(self) -> int:
        return int(time.monotonic() - self.started_monotonic)

    @property
    def bot_username(self) -> str:
        m = self.manager
        return (m.username if m is not None else None) or ""


STATE = AppState()


def is_admin(user_id: int | None) -> bool:
    return bool(user_id and user_id in SETTINGS.admin_ids)
