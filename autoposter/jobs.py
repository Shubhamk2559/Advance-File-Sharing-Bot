"""Background loops of the auto-poster. They run under the bot's TaskSupervisor (crash -> restart with backoff), not PTB's JobQueue."""
from __future__ import annotations

import asyncio
import gzip
import io
from datetime import datetime, time as dtime, timedelta

from telegram import InputFile
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError

from .config import SETTINGS, TZ, logger
from .database import dumps_json
from .sender import notify_admins, process_chat_batch
from .state import STATE
from .utils import utcnow

MAX_PARALLEL_CHATS = 4
GZIP_ABOVE_BYTES = 15 * 1024 * 1024  # Telegram bots can upload up to 50 MB; compress big backups
ARCHIVE_PER_TICK = 12
BOOT_DELAY = 5


async def scheduler_tick(bot) -> None:
    """Publishes for every chat whose slot has arrived (one indexed query when nothing is due)."""
    db = STATE.db
    due = await db.due_chats(utcnow())
    if not due:
        return
    sem = asyncio.Semaphore(MAX_PARALLEL_CHATS)

    async def run(chat_id: int) -> None:
        async with sem:
            try:
                await process_chat_batch(bot, chat_id)
            except Exception as exc:  # noqa: BLE001 - one broken chat must never stop the others
                await db.log("error", "batch_crashed", chat_id=chat_id, error=f"{type(exc).__name__}: {exc}")
                await db.defer_chat(chat_id, 120, error=f"{type(exc).__name__}: {exc}")

    await asyncio.gather(*(run(chat["chat_id"]) for chat in due))


async def housekeeping_tick(bot) -> None:
    """Removes posts whose auto-delete time has come."""
    db = STATE.db
    for row in await db.due_deletes(utcnow()):
        for message_id in row.get("message_ids", []):
            try:
                await bot.delete_message(chat_id=row["chat_id"], message_id=message_id)
            except RetryAfter:
                return  # try again next tick (the row stays)
            except TelegramError:
                pass  # already deleted by an admin, or too old / no rights
        await db.remove_delete(row["_id"])


def build_backup_file(payload: dict) -> tuple[bytes, str]:
    blob = dumps_json(payload)
    stamp = utcnow().strftime("%Y%m%d-%H%M%S")
    if len(blob) > GZIP_ABOVE_BYTES:
        return gzip.compress(blob), f"ap-backup-{stamp}.json.gz"
    return blob, f"ap-backup-{stamp}.json"


async def daily_job(bot) -> None:
    """03:00 local time: DM a backup to the owner and trim old logs."""
    db = STATE.db
    try:
        removed = await db.cleanup_logs(SETTINGS.log_retention_days)
        payload = await db.backup_payload()
        blob, filename = build_backup_file(payload)
        for admin_id in sorted(SETTINGS.admin_ids):
            try:
                await bot.send_document(
                    admin_id,
                    document=InputFile(io.BytesIO(blob), filename="auto-" + filename),
                    caption=f"🗓 Auto-poster daily backup ✅\nTargets: {len(payload.get('chats', []))}",
                )
            except TelegramError:
                pass  # owner blocked the bot
        await db.log("info", "auto_backup_sent", admins=len(SETTINGS.admin_ids), logs_removed=removed)
    except Exception as exc:  # noqa: BLE001 - never let a backup failure kill the loop
        await db.log("error", "auto_backup_failed", error=f"{type(exc).__name__}: {exc}")


async def archive_tick(bot) -> None:
    """Imported 'copy' posts only *point* at the source channel. With AP_VAULT_CHAT_ID set, each is copied into the vault chat and
    re-pointed at the copy, so deleting it from the source later no longer matters. A few posts per tick (restart-safe)."""
    vault = SETTINGS.vault_chat_id
    if not vault:
        return
    db = STATE.db
    chats = await db.chats.find({"archiving": True}).to_list(length=50)
    budget = ARCHIVE_PER_TICK
    for chat in chats:
        chat_id = chat["chat_id"]
        rows = await db.posts_coll(chat_id).find({"archive_pending": True, "active": True}).limit(budget).to_list(length=budget)
        if not rows:
            await db.chats.update_one({"chat_id": chat_id}, {"$unset": {"archiving": ""}})
            await notify_admins(bot, f"🗄 Archive finished for <b>{chat.get('title', chat_id)}</b> — imported posts are now safe even if deleted from the source.")
            continue
        for post in rows:
            try:
                copied = await bot.copy_message(chat_id=vault, from_chat_id=post["source_chat_id"], message_id=post["source_message_id"], disable_notification=True)
            except RetryAfter:
                return  # try again next tick
            except BadRequest as exc:
                if "not found" in str(exc).lower() or "can't be copied" in str(exc).lower():
                    await db.quarantine_post(chat_id, str(post["_id"]), f"source message gone before archiving: {exc}")
                else:
                    await db.posts_coll(chat_id).update_one({"_id": post["_id"]}, {"$set": {"archive_pending": False, "archive_error": str(exc)[:200]}})
                continue
            except Forbidden as exc:
                await db.log("error", "archive_vault_forbidden", error=str(exc))
                return
            await db.posts_coll(chat_id).update_one(
                {"_id": post["_id"]},
                {"$set": {"archive_pending": False, "archived": True, "origin_chat_id": post["source_chat_id"], "origin_message_id": post["source_message_id"],
                          "source_chat_id": vault, "source_message_id": copied.message_id}},
            )
            budget -= 1
            await asyncio.sleep(1.1)
            if budget <= 0:
                return


# --------------------------------------------------------------------------- #
# Loops (what the TaskSupervisor runs). `get_bot()` is evaluated every round so a re-created Application is always used.
# --------------------------------------------------------------------------- #
async def _forever(name: str, get_bot, tick, interval: float, first: float = BOOT_DELAY) -> None:
    await asyncio.sleep(first)
    while True:
        try:
            await tick(get_bot())
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a failed round must not end the loop
            logger.exception("auto-poster %s round failed", name)
        await asyncio.sleep(interval)


async def scheduler_loop(get_bot) -> None:
    await _forever("scheduler", get_bot, scheduler_tick, SETTINGS.scan_interval_seconds)


async def housekeeping_loop(get_bot) -> None:
    await _forever("housekeeping", get_bot, housekeeping_tick, 60, first=20)


async def archive_loop(get_bot) -> None:
    if not SETTINGS.vault_chat_id:
        return                                    # feature off: the supervisor treats a normal return as "finished"
    await _forever("archive", get_bot, archive_tick, 25, first=30)


def seconds_until_daily(now: datetime | None = None, at: dtime = dtime(3, 0)) -> float:
    now_local = (now or utcnow()).astimezone(TZ)
    target = datetime.combine(now_local.date(), at, tzinfo=TZ)
    if target <= now_local:
        target += timedelta(days=1)
    return (target - now_local).total_seconds()


async def daily_loop(get_bot) -> None:
    while True:
        wait = seconds_until_daily()
        while wait > 0:                           # short sleeps: survives clock changes and cancels quickly
            await asyncio.sleep(min(wait, 3600))
            wait = seconds_until_daily() if wait > 3600 else 0
        try:
            await daily_job(get_bot())
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("auto-poster daily job failed")
        await asyncio.sleep(61)                   # never run twice within the same minute
