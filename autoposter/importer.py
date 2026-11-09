"""Channel -> file posts.

Layout of the source channel (ascending message ids):

    HEAD   video / photo / gif WITH a caption      -> the post itself (its thumbnail). The caption is only the marker.
    FILE   video / document / gif WITHOUT caption  -> the files behind the post's link (everything directly below its HEAD)
    FILE
    HEAD   ...

For every HEAD that has at least one FILE:
  1. the FILEs are copied into PRIMARY + BACKUP channel and registered in the file bot's `files` collection (one link, same as
     the owner menu's "Create Post"),
  2. a `filepost` is put into the selected target's auto-post queue: HEAD as thumbnail + POST_CAPTION + one button with that link
     (sender.send_file_post), scheduled by the normal schedule / batch / window settings.

Telegram bots cannot read channel history, so every message in the range is FORWARDED to a scratch chat (the owner's DM by default),
inspected and deleted again. A rerun is safe: a HEAD already imported (files.src_chat / src_head) is reused, never copied twice, and a
post already in the target's queue is skipped (fingerprint).
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError

from .config import SETTINGS
from .state import STATE
from .utils import MAX_CAPTION_LEN

log = logging.getLogger("autoposter.importer")

HEAD_MEDIA = ("video", "animation", "photo")        # can be a post thumbnail
FILE_MEDIA = ("video", "animation", "document")     # can be a file behind the link
FLUSH_EVERY = 20                                    # queued posts are written in batches (one insert_many each)
DELETE_BATCH = 50                                   # scratch messages are deleted in batches
MAX_CONSECUTIVE_FAILURES = 3
GONE_MARKERS = ("message to forward not found", "message not found", "message can't be forwarded", "message_id_invalid",
                "message to copy not found", "can't be copied")


# --------------------------------------------------------------------------- #
# Pure helpers (no I/O): classification + grouping
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class Item:
    mid: int
    role: str                       # "head" | "file" | "skip"
    media: str | None = None        # video | animation | photo | document
    file_id: str | None = None
    caption: str = ""


@dataclass(slots=True)
class Group:
    head: Item
    files: list[Item] = field(default_factory=list)
    overflow: int = 0


def classify(mid: int, msg: Any) -> Item:
    """Role of one inspected message. `msg` is a telegram.Message (or anything with the same attributes)."""
    caption = (getattr(msg, "caption", None) or "").strip()
    if getattr(msg, "video", None):
        media, fid = "video", msg.video.file_id
    elif getattr(msg, "animation", None):           # before `document`: Telegram sets both for a GIF
        media, fid = "animation", msg.animation.file_id
    elif getattr(msg, "photo", None):
        media, fid = "photo", msg.photo[-1].file_id
    elif getattr(msg, "document", None):
        media, fid = "document", msg.document.file_id
    else:
        return Item(mid, "skip")
    if caption and media in HEAD_MEDIA:
        return Item(mid, "head", media, fid, caption)
    if media in FILE_MEDIA:
        return Item(mid, "file", media, fid, caption)
    return Item(mid, "skip", media, fid, caption)   # a photo without caption: not a post, not a file


class Grouper:
    """Streaming state machine: feed items in ascending id order, collect finished groups."""

    def __init__(self, max_files: int) -> None:
        self.max_files = max(1, max_files)
        self.cur: Group | None = None
        self.orphans = 0            # files before the first head
        self.skipped = 0            # unsupported messages
        self.overflow = 0           # files beyond max_files in one group (dropped)

    def feed(self, item: Item) -> Group | None:
        """Returns the group that this item just finished (if any)."""
        if item.role == "skip":
            self.skipped += 1
            return None
        if item.role == "head":
            done, self.cur = self.cur, Group(item)
            return done
        if self.cur is None:
            self.orphans += 1
            return None
        if len(self.cur.files) >= self.max_files:
            self.cur.overflow += 1
            self.overflow += 1
            return None
        self.cur.files.append(item)
        return None

    def finish(self) -> Group | None:
        done, self.cur = self.cur, None
        return done


def post_caption(head: Item, template: str, use_source: bool | None = None) -> str:
    """Caption shown on the channel post: the POST_CAPTION variable (optionally with the source caption above it)."""
    use_source = SETTINGS.use_source_caption if use_source is None else use_source
    text = f"{head.caption}\n\n{template}" if use_source and head.caption else template
    return text[:MAX_CAPTION_LEN]


@dataclass(slots=True)
class Report:
    total: int = 0                  # ids in the range
    scanned: int = 0
    missing: int = 0                # deleted / service / not forwardable
    heads: int = 0
    created: int = 0                # new file records (files copied)
    reused: int = 0                 # head already imported before: existing link reused
    queued: int = 0
    dups: int = 0                   # already in this target's queue
    no_files: int = 0               # head without a file below it
    orphans: int = 0
    skipped: int = 0
    overflow: int = 0
    failed: int = 0
    error: str | None = None
    stopped: bool = False
    started: float = field(default_factory=time.monotonic)


Progress = Callable[[Report], Awaitable[None]]


# --------------------------------------------------------------------------- #
# Telegram side
# --------------------------------------------------------------------------- #
class ImportAbort(Exception):
    """Fatal for the whole import; the text is shown to the owner."""


def _retry_secs(exc: RetryAfter) -> float:
    v = exc.retry_after
    return float(v.total_seconds()) if hasattr(v, "total_seconds") else float(v)


class _Janitor:
    """Deletes the forwarded scratch messages in batches (fewer API calls); never raises."""

    def __init__(self, bot, chat) -> None:
        self.bot, self.chat, self.ids = bot, chat, []

    async def add(self, message_id: int) -> None:
        self.ids.append(message_id)
        if len(self.ids) >= DELETE_BATCH:
            await self.flush()

    async def flush(self) -> None:
        ids, self.ids = self.ids, []
        if not ids:
            return
        try:
            await self.bot.delete_messages(chat_id=self.chat, message_ids=ids)
            return
        except RetryAfter as exc:
            await asyncio.sleep(min(_retry_secs(exc), 30) + 1)
        except TelegramError:
            pass
        for mid in ids:                                  # fallback: one by one
            try:
                await self.bot.delete_message(chat_id=self.chat, message_id=mid)
            except TelegramError:
                pass


async def inspect(bot, source_chat: int | str, scratch, mid: int, janitor: _Janitor) -> Item | None:
    """Forwards one source message to the scratch chat, classifies it, schedules the copy for deletion. None = gone / not forwardable."""
    for _ in range(5):
        try:
            msg = await bot.forward_message(chat_id=scratch, from_chat_id=source_chat, message_id=mid, disable_notification=True)
        except RetryAfter as exc:
            await asyncio.sleep(min(_retry_secs(exc), 60) + 1)
            continue
        except BadRequest as exc:
            text = str(exc).lower()
            if "protected" in text:
                raise ImportAbort("The source channel has 'Restrict saving content' on, so its messages can't be read. Turn it off, then retry.") from exc
            if any(m in text for m in GONE_MARKERS):
                return None
            raise ImportAbort(f"Telegram refused to read the source: {exc}") from exc
        except Forbidden as exc:
            raise ImportAbort("The bot can't write to the scratch chat (open the bot's chat and press Start, or set AP_IMPORT_SCRATCH_CHAT "
                              f"to a chat where the bot is admin). {exc}") from exc
        await janitor.add(msg.message_id)
        return classify(mid, msg)
    raise ImportAbort("Telegram keeps rate-limiting the import (flood wait). Try again in a few minutes.")


async def _create_file_post(bot, source_chat: int, group: Group, caption: str) -> tuple[str, bool]:
    """Returns (token, created). Reuses the file record when this head was imported before."""
    import db as filedb
    from services.posts import store_videos

    head = group.head.mid
    existing = await filedb.file_by_source(source_chat, head)
    if existing:
        return existing["token"], False
    token = secrets.token_urlsafe(6)
    videos = [{"msg": f.mid, "caption": ""} for f in group.files]
    items, _warns = await store_videos(bot, source_chat, videos, token)        # rolls back its own partial copies on failure
    try:
        await filedb.save_post(token, items, caption, group.head.file_id, thumb_type=group.head.media, src_chat=source_chat, src_head=head)
    except Exception:
        from services.posts import rollback_items
        await rollback_items(bot, items)
        raise
    return token, True


def _queue_doc(source_chat: int, head: Item, token: str, caption: str) -> dict[str, Any]:
    return {
        "media_type": "filepost", "token": token, "caption": caption, "parse_mode": None,
        "fingerprint": f"fp:{source_chat}:{head.mid}", "silent": False, "protect_content": False,
        "pin": False, "nofooter": True, "buttons": [], "source_chat_id": source_chat, "source_message_id": head.mid,
    }


async def import_range(bot, *, target_chat_id: int, source_chat: int, lo: int, hi: int, progress: Progress | None = None,
                       scratch=None, max_files: int | None = None, stop: asyncio.Event | None = None,
                       report: Report | None = None) -> Report:
    """Scans [lo, hi] of `source_chat`, turns every HEAD(+FILEs) into a file post and queues it in `target_chat_id`."""
    from config import POST_CAPTION, POST_MAX_VIDEOS

    db = STATE.db
    rep = report if report is not None else Report()
    rep.total = hi - lo + 1
    scratch = scratch if scratch is not None else (SETTINGS.import_scratch_chat or next(iter(sorted(SETTINGS.admin_ids))))
    janitor = _Janitor(bot, scratch)
    grouper = Grouper(max_files or POST_MAX_VIDEOS)
    buffer: list[dict[str, Any]] = []
    consecutive_failures = 0
    last_report = 0.0

    async def flush() -> None:
        nonlocal buffer
        if not buffer:
            return
        batch, buffer = buffer, []
        inserted, dups = await db.bulk_add_posts(target_chat_id, batch, dedupe=True)
        rep.queued += inserted
        rep.dups += dups

    async def tell(force: bool = False) -> None:
        nonlocal last_report
        if progress and (force or time.monotonic() - last_report >= 5):
            last_report = time.monotonic()
            try:
                await progress(rep)
            except Exception:  # noqa: BLE001 - a failed progress edit must never stop the import
                log.debug("progress callback failed", exc_info=True)

    async def settle(group: Group | None) -> None:
        nonlocal consecutive_failures
        if group is None:
            return
        rep.heads += 1
        if not group.files:
            rep.no_files += 1
            return
        try:
            caption = post_caption(group.head, POST_CAPTION)
            token, created = await _create_file_post(bot, source_chat, group, caption)
        except (TelegramError, ValueError, KeyError) as exc:
            rep.failed += 1
            consecutive_failures += 1
            log.warning("file post failed head=%s: %s: %s", group.head.mid, type(exc).__name__, exc)
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                raise ImportAbort(f"{consecutive_failures} posts failed in a row ({type(exc).__name__}: {exc}). "
                                  "Make the bot admin in PRIMARY and BACKUP channel (and the source), then run it again - finished posts are kept.") from exc
            return
        consecutive_failures = 0
        rep.created += 1 if created else 0
        rep.reused += 0 if created else 1
        rep.overflow += group.overflow
        buffer.append(_queue_doc(source_chat, group.head, token, caption))
        if len(buffer) >= FLUSH_EVERY:
            await flush()

    try:
        for mid in range(lo, hi + 1):
            if stop is not None and stop.is_set():
                rep.stopped = True
                break
            item = await inspect(bot, source_chat, scratch, mid, janitor)
            rep.scanned += 1
            if item is None:
                rep.missing += 1
            else:
                await settle(grouper.feed(item))
            if SETTINGS.import_pace_seconds:
                await asyncio.sleep(SETTINGS.import_pace_seconds)
            await tell()
        if not rep.stopped:                               # a stopped import leaves its last, possibly incomplete, group alone
            await settle(grouper.finish())
    except ImportAbort as exc:
        rep.error = str(exc)
    finally:
        rep.orphans, rep.skipped = grouper.orphans, grouper.skipped
        try:                                            # whatever was finished stays queued, even when stopped / aborted
            await asyncio.shield(flush())
            await asyncio.shield(janitor.flush())
        except Exception:  # noqa: BLE001
            log.warning("final flush failed", exc_info=True)
    await tell(force=True)
    return rep


def render_report(rep: Report, title: str, running: bool = False) -> str:
    """HTML for the progress / result message."""
    import html

    pct = int(100 * rep.scanned / rep.total) if rep.total else 100
    head = f"📥 <b>Channel → Posts • {html.escape(title)}</b>\n━━━━━━━━━━━━━━━━━━\n"
    state = (f"⏳ Scanning… <b>{rep.scanned}/{rep.total}</b> ({pct}%)\n" if running
             else ("⏹ <b>Stopped</b>\n" if rep.stopped else ("❌ <b>Aborted</b>\n" if rep.error else "✅ <b>Done</b>\n")))
    lines = [
        f"📦 Posts queued: <b>{rep.queued}</b>   (new files: {rep.created}, reused: {rep.reused})",
        f"🎬 Heads found: <b>{rep.heads}</b>",
    ]
    for label, n in (("🧬 Already in this queue", rep.dups), ("📭 Head without a file below it", rep.no_files),
                     ("🧩 Files before the first head", rep.orphans), ("🗑 Deleted / unreadable messages", rep.missing),
                     ("➖ Other messages skipped", rep.skipped), ("✂️ Extra files over the per-post limit", rep.overflow),
                     ("⚠️ Failed posts", rep.failed)):
        if n:
            lines.append(f"{label}: <b>{n}</b>")
    tail = f"\n\n❌ {html.escape(rep.error)}" if rep.error else ""
    return head + state + "\n".join(lines) + tail
