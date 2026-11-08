"""Pure routing policy for several bots (one bot per Koyeb service, one shared MongoDB). No I/O, no clock, no globals: every function takes
its inputs, so it is unit-testable and the decision costs microseconds.

Two decisions
-------------
1. GATEWAY  (`/f/<token>` link click, the user is NOT identified yet - only a signed browser hint may exist)
       valid hint (same bot, younger than STICKY_TTL, bot usable and not full)  -> keep that bot      ("sticky")
       otherwise                                                                -> least-loaded bot    ("new")
2. ARRIVAL  (`/start <token>` reached bot HERE, the Telegram user id IS known)
       active on HERE                          -> serve here
       active on another bot, HERE has room    -> serve here and re-pin (the user is already in this chat: a hop would cost a tap)
       active on another bot, HERE overloaded  -> move to the bot they were pinned to (if usable), else the least-loaded one
       new or idle for STICKY_TTL              -> serve here unless HERE is overloaded and a clearly lighter bot exists

"Least loaded" = power of two choices (two random usable bots, the lighter wins). It spreads a burst instead of stampeding the single
lightest bot while the load numbers are still a few seconds old, and the heaviest bot never wins a pair, so it stops receiving new users.
"""
from __future__ import annotations

import os
import random
from dataclasses import dataclass
from typing import Mapping, Optional

SERVE, MOVE = "serve", "move"


class RouterConfigError(ValueError):
    """Invalid ROUTER_* / STICKY_* / BOT_CAPACITY environment. Raised at startup; names only, never values of secrets."""


@dataclass(frozen=True)
class RouterConfig:
    sticky_ttl: float = 900.0         # seconds without activity after which a user may be moved to a lighter bot
    capacity: int = 1000              # active (sticky) users THIS instance's bot is sized for; other bots publish their own value
    overload_ratio: float = 0.85      # load/capacity above which a bot stops taking NEW users when a better bot exists
    hard_ratio: float = 1.0           # load/capacity at which even sticky gateway hints are re-routed
    move_margin: float = 0.15         # a target must be at least this much lighter (in load ratio) before an arrival is moved
    hb_interval: float = 5.0          # heartbeat period (seconds)
    refresh_interval: float = 2.0     # how often the in-memory view of all bots is reloaded
    stale_after: float = 15.0         # heartbeat older than this = bot is dead
    snapshot_grace: float = 60.0      # database unreachable: keep trusting the last snapshot this long
    touch_every: float = 60.0         # at most one activity write per user per this many seconds
    cache_size: int = 50_000          # per-process LRU of user -> assignment
    extra_window: float = 10.0        # optimistic "just assigned here" counters live this long (covers heartbeat + refresh lag)
    cookie_name: str = "sb"

    @staticmethod
    def from_env(env: Mapping[str, str]) -> "RouterConfig":
        d = RouterConfig()
        problems: list[str] = []

        def num(name: str, default, cast, lo, hi):
            raw = (env.get(name) or "").strip()
            if not raw:
                return default
            try:
                v = cast(raw)
            except ValueError:
                problems.append(f"{name} must be a number")
                return default
            if not (lo <= v <= hi):
                problems.append(f"{name} must be between {lo} and {hi}")
                return default
            return v

        cfg = RouterConfig(
            sticky_ttl=num("STICKY_TTL", d.sticky_ttl, float, 30, 86400),
            capacity=num("BOT_CAPACITY", d.capacity, int, 1, 10_000_000),
            overload_ratio=num("ROUTER_OVERLOAD_RATIO", d.overload_ratio, float, 0.1, 1.0),
            hard_ratio=num("ROUTER_HARD_RATIO", d.hard_ratio, float, 0.1, 5.0),
            move_margin=num("ROUTER_MOVE_MARGIN", d.move_margin, float, 0.0, 1.0),
            hb_interval=num("ROUTER_HB_INTERVAL", d.hb_interval, float, 1, 60),
            refresh_interval=num("ROUTER_REFRESH_INTERVAL", d.refresh_interval, float, 0.5, 60),
            stale_after=num("ROUTER_STALE_AFTER", d.stale_after, float, 3, 600),
            snapshot_grace=num("ROUTER_SNAPSHOT_GRACE", d.snapshot_grace, float, 5, 3600),
            touch_every=num("ROUTER_TOUCH_EVERY", d.touch_every, float, 5, 600),
            cache_size=num("ROUTER_CACHE_SIZE", d.cache_size, int, 100, 5_000_000),
        )
        if cfg.overload_ratio > cfg.hard_ratio:
            problems.append("ROUTER_OVERLOAD_RATIO must not exceed ROUTER_HARD_RATIO")
        if cfg.stale_after < cfg.hb_interval * 2:
            problems.append("ROUTER_STALE_AFTER must be at least twice ROUTER_HB_INTERVAL")
        if cfg.touch_every >= cfg.sticky_ttl:
            problems.append("ROUTER_TOUCH_EVERY must be smaller than STICKY_TTL")
        if problems:
            raise RouterConfigError("; ".join(problems))
        return cfg


@dataclass(frozen=True)
class BotInfo:
    bot_id: str
    username: str
    capacity: int
    active: int                       # users pinned to this bot and active within STICKY_TTL (published by the bot's own heartbeat)
    hb: float                         # last heartbeat (epoch seconds)
    penalty_until: float = 0.0        # Telegram flood-wait: no NEW users until then


@dataclass(frozen=True)
class Assignment:
    user_id: int
    bot_id: str
    last_active: float
    ver: int = 1


@dataclass(frozen=True)
class Decision:
    action: str                       # SERVE | MOVE
    bot_id: str
    username: str
    reason: str
    write: bool = False               # the assignment must be (re)written to bot_id


def load(b: BotInfo, extra: int = 0) -> float:
    return (b.active + extra) / max(1, b.capacity)


def alive(b: BotInfo, ref_now: float, cfg: RouterConfig) -> bool:
    return (ref_now - b.hb) <= cfg.stale_after


def usable(b: BotInfo, ref_now: float, cfg: RouterConfig) -> bool:
    """Alive and not in a Telegram flood-wait: may receive NEW users."""
    return alive(b, ref_now, cfg) and ref_now >= b.penalty_until


def pick(bots: Mapping[str, BotInfo], ref_now: float, cfg: RouterConfig, rng: random.Random, extra: Mapping[str, int] | None = None,
         exclude: tuple[str, ...] = ()) -> Optional[BotInfo]:
    """Power-of-two-choices among usable bots that still have room; if every bot is full the lightest still answers (never refuse a user)."""
    extra = extra or {}
    pool = [b for b in bots.values() if b.bot_id not in exclude and usable(b, ref_now, cfg)]
    if not pool:
        return None
    room = [b for b in pool if load(b, extra.get(b.bot_id, 0)) < cfg.hard_ratio]
    pool = room or pool
    if len(pool) == 1:
        return pool[0]
    a, b = rng.sample(pool, 2)
    la, lb = load(a, extra.get(a.bot_id, 0)), load(b, extra.get(b.bot_id, 0))
    if la == lb:
        return rng.choice((a, b))
    return a if la < lb else b


def decide_gateway(hint_bot: Optional[str], hint_age: Optional[float], bots: Mapping[str, BotInfo], ref_now: float, cfg: RouterConfig,
                   rng: random.Random, extra: Mapping[str, int] | None = None) -> tuple[Optional[BotInfo], str]:
    """Which bot should `/f/<token>` send this browser to? (None only when no bot is usable at all.)"""
    extra = extra or {}
    if hint_bot is not None and hint_age is not None and 0 <= hint_age <= cfg.sticky_ttl:
        b = bots.get(hint_bot)
        if b is not None and usable(b, ref_now, cfg) and load(b, extra.get(b.bot_id, 0)) < cfg.hard_ratio:
            return b, "sticky"
        reason = "hint_unusable"
    else:
        reason = "new" if hint_bot is None else "idle"
    b = pick(bots, ref_now, cfg, rng, extra)
    if b is None:  # nothing usable: a bot in flood-wait is still better than a dead link
        live = [x for x in bots.values() if alive(x, ref_now, cfg)]
        b = min(live, key=lambda x: load(x, extra.get(x.bot_id, 0)), default=None)
        reason = "degraded" if b else "none"
    return b, reason


def decide_arrival(a: Optional[Assignment], here: str, bots: Mapping[str, BotInfo], ref_now: float, cfg: RouterConfig, rng: random.Random,
                   extra: Mapping[str, int] | None = None, now: float | None = None) -> Decision:
    """The user (id known) just reached bot `here`. See the module docstring for the table.
    `ref_now` ages BOT heartbeats (it may be the time of the last fleet snapshot); `now` is the real clock and ages the USER's activity."""
    extra = extra or {}
    now = ref_now if now is None else now
    me = bots.get(here)
    here_user = me.username if me else ""
    here_load = load(me, extra.get(here, 0)) if me else 0.0
    here_ok = me is None or usable(me, ref_now, cfg)          # not in the snapshot yet (just booted) counts as usable
    is_active = a is not None and 0 <= now - a.last_active <= cfg.sticky_ttl

    if is_active and a.bot_id == here:
        return Decision(SERVE, here, here_user, "sticky_here")

    if is_active:                                              # pinned to another bot, but the user is in THIS chat right now
        if here_ok and here_load < cfg.overload_ratio:
            return Decision(SERVE, here, here_user, "follow_user", write=True)
        other = bots.get(a.bot_id)
        if other is not None and usable(other, ref_now, cfg) and load(other, extra.get(other.bot_id, 0)) < cfg.hard_ratio:
            return Decision(MOVE, other.bot_id, other.username, "here_overloaded_back_to_pinned")
        best = pick(bots, ref_now, cfg, rng, extra, exclude=(here,))
        if best is not None and load(best, extra.get(best.bot_id, 0)) + cfg.move_margin <= here_load:
            return Decision(MOVE, best.bot_id, best.username, "here_overloaded_move", write=True)
        return Decision(SERVE, here, here_user, "overloaded_but_alone", write=True)

    # new user, or idle longer than STICKY_TTL: this is the moment to rebalance
    if here_ok and here_load < cfg.overload_ratio:
        return Decision(SERVE, here, here_user, "new_here", write=True)
    best = pick(bots, ref_now, cfg, rng, extra, exclude=(here,))
    if best is not None and load(best, extra.get(best.bot_id, 0)) + cfg.move_margin <= here_load:
        return Decision(MOVE, best.bot_id, best.username, "rebalance", write=True)
    return Decision(SERVE, here, here_user, "new_here_no_better", write=True)


def self_id_from_env(env: Mapping[str, str] = os.environ) -> str:
    """Stable per-service name for logs/heartbeats (Koyeb sets KOYEB_SERVICE_NAME; falls back to the hostname)."""
    return (env.get("INSTANCE_ID") or env.get("KOYEB_SERVICE_NAME") or env.get("HOSTNAME") or "instance")[:64]
