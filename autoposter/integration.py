"""The only module the file bot imports to wire the auto-poster in (manager.py, handlers.py, migrations)."""
from __future__ import annotations

import functools
import logging

log = logging.getLogger("autoposter.integration")

# Telegram updates the auto-poster needs on top of the file bot's own ("message", "callback_query")
EXTRA_UPDATES = ["channel_post", "my_chat_member", "chat_join_request"]


def enabled() -> bool:
    from .config import SETTINGS
    return SETTINGS.enabled


def allowed_updates() -> list[str]:
    base = ["message", "callback_query"]
    return base + EXTRA_UPDATES if enabled() else base


def attach(manager) -> None:
    """Called once the Telegram Application exists: shares the file bot's MongoDB connection and the manager."""
    import db as filedb
    from .database import Database
    from .state import STATE

    STATE.manager = manager
    STATE.db = Database(filedb._db)


def register_handlers(app) -> None:
    """Registered BEFORE the file bot's owner handlers: the first matching handler of a group wins, and the mode gate lives in group -1."""
    from telegram.ext import CallbackQueryHandler, ChatJoinRequestHandler, ChatMemberHandler, CommandHandler, MessageHandler, filters

    from config import OWNER_ID
    from . import handlers as h

    owner_private = filters.User(user_id=OWNER_ID) & filters.ChatType.PRIVATE & ~filters.COMMAND & filters.UpdateType.MESSAGE
    app.add_handler(MessageHandler(owner_private, h.mode_gate), group=-1)
    app.add_handler(CallbackQueryHandler(h.handle_callback, pattern=h.CALLBACK_PATTERN))
    app.add_handler(CallbackQueryHandler(h.handle_reaction, pattern=h.REACTION_PATTERN))
    app.add_handler(CommandHandler("register", h.register_current_chat))
    app.add_handler(ChatMemberHandler(h.my_chat_member_handler, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(ChatJoinRequestHandler(h.handle_join_request))
    app.add_handler(MessageHandler(~filters.ChatType.PRIVATE & ~filters.COMMAND & (filters.UpdateType.MESSAGE | filters.UpdateType.CHANNEL_POST), h.any_chat_message))


def loops(manager) -> list[tuple[str, object]]:
    """(name, factory) pairs for BotManager.tasks.start(); empty when AP_ENABLED=false."""
    if not enabled():
        return []
    from . import jobs

    def get_bot():
        return manager.main.bot

    return [(f"ap_{n}", functools.partial(fn, get_bot)) for n, fn in
            (("scheduler", jobs.scheduler_loop), ("housekeeping", jobs.housekeeping_loop), ("daily", jobs.daily_loop), ("archive", jobs.archive_loop))]


async def clear_mode(user_id: int) -> None:
    """Called by the owner menu (/start): cancels any open auto-poster prompt. Never raises."""
    if not enabled():
        return
    try:
        from .handlers import clear_admin_mode
        from .state import STATE
        if STATE.db is not None:
            await clear_admin_mode(user_id)
    except Exception:  # noqa: BLE001
        log.debug("clear_mode failed", exc_info=True)
