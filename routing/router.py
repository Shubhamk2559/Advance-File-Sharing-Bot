"""MongoDB-backed runtime of the multi-bot router (decisions themselves live in policy.py).

Topology: ONE Koyeb service per bot (own MAIN_BOT_TOKEN + own BASE_URL), ALL services share the same MongoDB.

    route_bots    one document per bot, upserted by that bot's own heartbeat every ROUTER_HB_INTERVAL seconds:
                  {_id: <bot id>, username, instance, capacity, active, hb, penalty_until, hb_at}
    route_assign  one document per Telegram user:  {_id: <user id>, bot_id, last_active, ver, exp}

Hot paths never wait on the database:
  * gateway pick (`/f/<token>`)     : in-memory snapshot of route_bots (reloaded every ROUTER_REFRESH_INTERVAL seconds) + signed cookie
  * arrival / activity (`/start`..) : in-process LRU of the user's assignment (15 s) first; one indexed `_id` lookup on a miss; at most ONE
                                      write per user per ROUTER_TOUCH_EVERY seconds while the user stays active
Races between instances are resolved with a compare-and-set on `ver`; the loser re-decides against the winner's row, so a user is never
pinned to two bots at once.
"""
from __future__ import annotations

import asyncio
import random
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from ads import slog
from ads.store import DuplicateKeyError

from . import cookie as cookie_mod
from .policy import Assignment, BotInfo, Decision, RouterConfig, SERVE, decide_arrival, decide_gateway, load, pick, usable

try:
    from pymongo import ReturnDocument
    _AFTER = ReturnDocument.AFTER
except ImportError:  # tests run without pymongo
    _AFTER = True

BOTS, ASSIGN = "route_bots", "route_assign"
ASSIGN_KEEP = 86400                 # housekeeping only: an assignment older than STICKY_TTL is already treated as idle
CACHE_TTL = 15.0                    # seconds an assignment may be served from the in-process cache

# (collection, keys, options) - appended to migrations/registry.py as version 6
ROUTING_INDEXES = [
    (ASSIGN, [("bot_id", 1), ("last_active", 1)], {}),               # heartbeat: count active users of one bot
    (ASSIGN, "exp", {"expireAfterSeconds": 0}),                      # housekeeping
    (BOTS, "hb_at", {"expireAfterSeconds": ASSIGN_KEEP}),            # forget bots that were removed
]


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


@dataclass
class _Cached:
    a: Assignment
    fetched: float                  # when this row was read/written (cache freshness)
    touched: float                  # when last_active was last written to the database


class Router:
    def __init__(self, coll: Callable[[str], object], cfg: RouterConfig, *, bot_id: str, username: str, instance: str, secret: bytes,
                 clock: Callable[[], float] = time.time, rng: Optional[random.Random] = None):
        self.c, self.cfg = coll, cfg
        self.bot_id, self.username, self.instance = str(bot_id), username, instance
        self.secret = secret
        self.clock = clock
        self.rng = rng or random.Random()
        self._snap: dict[str, BotInfo] = {}
        self._snap_at = 0.0
        self._extra: dict[str, deque] = {}
        self._cache: "OrderedDict[int, _Cached]" = OrderedDict()
        self._local_active = 0
        self._penalty_until = 0.0
        self._wake = asyncio.Event()
        self.stats = {"hb_ok": 0, "hb_fail": 0, "refresh_ok": 0, "refresh_fail": 0, "cas_lost": 0, "moves": 0, "serves": 0,
                      "gateway_sticky": 0, "gateway_new": 0, "last_error": None}

    # ------------------------------------------------------------------ view of all bots (memory only)
    def _extra_counts(self, now: float) -> dict[str, int]:
        out = {}
        for bid, dq in list(self._extra.items()):
            while dq and now - dq[0] > self.cfg.extra_window:
                dq.popleft()
            if dq:
                out[bid] = len(dq)
            else:
                del self._extra[bid]
        return out

    def _bump(self, bot_id: str, now: float) -> None:
        """Optimistic +1: users assigned in the last few seconds are not in any heartbeat yet."""
        self._extra.setdefault(bot_id, deque()).append(now)

    def _view(self, now: float) -> tuple[dict[str, BotInfo], float]:
        """(bots, reference time). If the database has been unreachable for a while we keep trusting the last snapshot (aged against ITS
        timestamp) for ROUTER_SNAPSHOT_GRACE seconds, so a database blip does not make every other bot look dead and pile all users on one."""
        ref = self._snap_at if 0 < now - self._snap_at <= self.cfg.snapshot_grace else now
        bots = dict(self._snap)
        bots[self.bot_id] = BotInfo(self.bot_id, self.username, self.cfg.capacity, self._local_active, ref, self._penalty_until)
        return bots, ref

    def usernames(self) -> list[str]:
        """Usernames of every bot that may take new users (this one is always included as the last resort)."""
        now = self.clock()
        bots, ref = self._view(now)
        names = [b.username for b in bots.values() if b.username and usable(b, ref, self.cfg)]
        return names or [self.username]

    def pick_username(self) -> str:
        """Least-loaded bot for a link that gets baked into a channel post."""
        now = self.clock()
        bots, ref = self._view(now)
        b = pick(bots, ref, self.cfg, self.rng, self._extra_counts(now))
        if b is not None:
            self._bump(b.bot_id, now)
        return (b.username if b else None) or self.username

    # ------------------------------------------------------------------ gateway: /f/<token> (user not identified; signed browser hint)
    def gateway(self, cookie_value: Optional[str]) -> tuple[Optional[BotInfo], str, str]:
        """-> (bot, reason, new cookie value). Pure memory, no await: this is what makes the redirect take microseconds."""
        now = self.clock()
        hint_bot, hint_age = cookie_mod.parse(self.secret, cookie_value, now)
        bots, ref = self._view(now)
        extra = self._extra_counts(now)
        b, reason = decide_gateway(hint_bot, hint_age, bots, ref, self.cfg, self.rng, extra)
        if b is None:
            return None, reason, ""
        if reason == "sticky":
            self.stats["gateway_sticky"] += 1
        else:
            self.stats["gateway_new"] += 1
            self._bump(b.bot_id, now)
        return b, reason, cookie_mod.make(self.secret, b.bot_id, now)

    # ------------------------------------------------------------------ assignments
    async def _get(self, user_id: int, now: float) -> Optional[Assignment]:
        ent = self._cache.get(user_id)
        if ent is not None and now - ent.fetched <= CACHE_TTL:
            self._cache.move_to_end(user_id)
            return ent.a
        doc = await self.c(ASSIGN).find_one({"_id": user_id})
        if not doc:
            self._cache.pop(user_id, None)
            return None
        a = Assignment(user_id, str(doc["bot_id"]), float(doc["last_active"]), int(doc.get("ver", 1)))
        self._remember(a, now, touched=a.last_active)
        return a

    def _remember(self, a: Assignment, now: float, touched: float) -> None:
        self._cache[a.user_id] = _Cached(a, now, touched)
        self._cache.move_to_end(a.user_id)
        while len(self._cache) > self.cfg.cache_size:
            self._cache.popitem(last=False)

    async def _write(self, user_id: int, bot_id: str, prev: Optional[Assignment], now: float) -> tuple[bool, Optional[Assignment]]:
        """Compare-and-set on `ver`. -> (won, current row when lost)."""
        col = self.c(ASSIGN)
        row = {"bot_id": bot_id, "last_active": now, "exp": _dt(now + ASSIGN_KEEP)}
        if prev is None:
            try:
                await col.insert_one({"_id": user_id, "ver": 1, **row})
                a = Assignment(user_id, bot_id, now, 1)
                self._remember(a, now, touched=now)
                return True, a
            except DuplicateKeyError:
                pass
        else:
            r = await col.find_one_and_update({"_id": user_id, "ver": prev.ver}, {"$set": row, "$inc": {"ver": 1}}, return_document=_AFTER)
            if r is not None:
                a = Assignment(user_id, bot_id, now, int(r["ver"]))
                self._remember(a, now, touched=now)
                return True, a
        self.stats["cas_lost"] += 1
        self._cache.pop(user_id, None)
        doc = await col.find_one({"_id": user_id})
        cur = Assignment(user_id, str(doc["bot_id"]), float(doc["last_active"]), int(doc.get("ver", 1))) if doc else None
        if cur is not None:
            self._remember(cur, now, touched=cur.last_active)
        return False, cur

    async def on_arrival(self, user_id: int) -> Decision:
        """The user (id known) just opened a link in THIS bot. Serve here, or tell the caller which bot to send them to."""
        now = self.clock()
        a = await self._get(user_id, now)
        bots, ref = self._view(now)
        d = None
        for _ in range(3):
            d = decide_arrival(a, self.bot_id, bots, ref, self.cfg, self.rng, self._extra_counts(now), now=now)
            if not d.write:
                break
            won, cur = await self._write(user_id, d.bot_id, a, now)
            if won:
                if d.bot_id != self.bot_id:
                    self._bump(d.bot_id, now)
                else:
                    self._local_active += 1               # visible to the next decisions before the next heartbeat recounts
                break
            a = cur                                       # lost the race: decide again against the winner's row
            d = None
        if d is None:                                     # three lost races in a row: serve here, never block the user
            d = Decision(SERVE, self.bot_id, self.username, "race_serve_here")
        self.stats["moves" if d.action != SERVE else "serves"] += 1
        if not d.write and d.action == SERVE:
            await self.touch(user_id)                     # sticky_here: slide the activity window (throttled)
        return d

    async def touch(self, user_id: int) -> None:
        """Activity on THIS bot (message, button, Mini App call): slides the user's sticky window. In-memory unless a write is due."""
        now = self.clock()
        ent = self._cache.get(user_id)
        if ent is not None and ent.a.bot_id == self.bot_id and now - ent.touched < self.cfg.touch_every:
            return
        try:
            r = await self.c(ASSIGN).find_one_and_update({"_id": user_id, "bot_id": self.bot_id},
                                                         {"$set": {"last_active": now, "exp": _dt(now + ASSIGN_KEEP)}}, return_document=_AFTER)
        except Exception as e:  # noqa: BLE001 - stickiness is best effort, never break a user request for it
            self.stats["last_error"] = type(e).__name__
            return
        if r is not None:
            self._remember(Assignment(user_id, self.bot_id, now, int(r.get("ver", 1))), now, touched=now)

    async def pin_here(self, user_id: int) -> None:
        """Direct Mini App launch (no /start): the user is demonstrably working on THIS bot, so pin them here."""
        now = self.clock()
        a = await self._get(user_id, now)
        if a is not None and a.bot_id == self.bot_id:
            await self.touch(user_id)
            return
        won, _cur = await self._write(user_id, self.bot_id, a, now)
        if won:
            self._local_active += 1

    def penalize(self, seconds: float) -> None:
        """Telegram answered 429 (flood wait) to THIS bot: it takes no NEW users until the wait is over; sticky users stay."""
        seconds = seconds.total_seconds() if hasattr(seconds, "total_seconds") else seconds
        until = self.clock() + max(1.0, min(float(seconds), 300.0))
        if until > self._penalty_until:
            self._penalty_until = until
            self._wake.set()                              # publish it now, not at the next heartbeat

    # ------------------------------------------------------------------ background: heartbeat + refresh
    async def heartbeat(self) -> None:
        now = self.clock()
        active = await self.c(ASSIGN).count_documents({"bot_id": self.bot_id, "last_active": {"$gte": now - self.cfg.sticky_ttl}})
        await self.c(BOTS).update_one(
            {"_id": self.bot_id},
            {"$set": {"username": self.username, "instance": self.instance, "capacity": self.cfg.capacity, "active": int(active),
                      "hb": now, "hb_at": _dt(now), "penalty_until": self._penalty_until}},
            upsert=True)
        self._local_active = int(active)
        self.stats["hb_ok"] += 1

    async def refresh(self) -> None:
        now = self.clock()
        rows = await self.c(BOTS).find({"hb": {"$gte": now - self.cfg.stale_after * 4}}).to_list(length=500)
        snap = {}
        for r in rows:
            bid = str(r["_id"])
            if bid == self.bot_id or not r.get("username"):
                continue
            snap[bid] = BotInfo(bid, str(r["username"]), int(r.get("capacity") or 1), int(r.get("active") or 0), float(r["hb"]),
                                float(r.get("penalty_until") or 0.0))
        self._snap, self._snap_at = snap, now
        self.stats["refresh_ok"] += 1

    async def heartbeat_loop(self) -> None:
        while True:
            try:
                await self.heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self.stats["hb_fail"] += 1
                self.stats["last_error"] = type(e).__name__
                slog.event("router_heartbeat_failed", level="warning", error=type(e).__name__)
            try:
                await asyncio.wait_for(self._wake.wait(), self.cfg.hb_interval)
                await asyncio.sleep(0.5)                  # flood-wait wake-up: at most 2 early heartbeats per second
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def refresh_loop(self) -> None:
        while True:
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self.stats["refresh_fail"] += 1
                self.stats["last_error"] = type(e).__name__
                slog.event("router_refresh_failed", level="warning", error=type(e).__name__)
            await asyncio.sleep(self.cfg.refresh_interval)

    async def start(self) -> None:
        """Publish ourselves and load the others once before traffic flows. Failures are not fatal: the loops retry."""
        for step in (self.heartbeat, self.refresh):
            try:
                await asyncio.wait_for(step(), 10)
            except Exception as e:  # noqa: BLE001
                self.stats["last_error"] = type(e).__name__
                slog.event("router_start_step_failed", level="warning", step=step.__name__, error=type(e).__name__)

    # ------------------------------------------------------------------ diagnostics (no user ids)
    def snapshot(self) -> dict:
        now = self.clock()
        bots, ref = self._view(now)
        extra = self._extra_counts(now)
        rows = [{"bot": b.username, "self": b.bot_id == self.bot_id, "active": b.active, "capacity": b.capacity,
                 "load": round(load(b, extra.get(b.bot_id, 0)), 3), "usable": usable(b, ref, self.cfg),
                 "heartbeat_age_s": round(max(0.0, ref - b.hb), 1), "flood_wait_s": round(max(0.0, b.penalty_until - ref), 1)}
                for b in sorted(bots.values(), key=lambda x: x.username)]
        return {"self": self.username, "bots": rows, "cached_users": len(self._cache), "stats": dict(self.stats),
                "snapshot_age_s": round(now - self._snap_at, 1) if self._snap_at else None,
                "config": {"sticky_ttl": self.cfg.sticky_ttl, "capacity": self.cfg.capacity, "overload_ratio": self.cfg.overload_ratio}}
