"""Multi-bot sticky routing (routing/): pure policy, signed cookie, and the MongoDB-backed Router with several instances over ONE FakeDB.

Two bots = two Router objects with separate process state over the same FakeDB (= the same MongoDB). FakeDB(yield_ops=True) makes every
operation yield to the event loop, so concurrent tasks really interleave. Time is a fake clock; nothing sleeps.
"""
import asyncio
import random
import sys
import unittest
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from routing import cookie                                                                   # noqa: E402
from routing.policy import (Assignment, BotInfo, MOVE, RouterConfig, RouterConfigError, SERVE, decide_arrival,   # noqa: E402
                            decide_gateway, pick)
from routing.router import ASSIGN, BOTS, Router                                              # noqa: E402
from tests.fakemongo import FakeDB                                                           # noqa: E402

CFG = RouterConfig(sticky_ttl=900, capacity=100, hb_interval=5, refresh_interval=2, stale_after=15, touch_every=60)
SECRET = b"unit-test-secret"
NOW = 10_000.0


def bot(i, active=0, cap=100, hb=NOW, pen=0.0):
    return BotInfo(str(i), f"bot{i}", cap, active, hb, pen)


def fleet(*bots):
    return {b.bot_id: b for b in bots}


class Clock:
    def __init__(self, t=NOW): self.t = t
    def __call__(self): return self.t
    def advance(self, dt): self.t += dt


# =============================================================================== policy
class PickTests(unittest.TestCase):
    def test_the_heaviest_bot_never_wins_a_pair_so_it_gets_no_new_users(self):
        bots = fleet(bot(1, 10), bot(2, 40), bot(3, 90))
        rng = random.Random(1)
        got = [pick(bots, NOW, CFG, rng).bot_id for _ in range(3000)]
        self.assertEqual(got.count("3"), 0)
        self.assertGreater(got.count("1"), got.count("2"))                    # lightest takes the most
        self.assertGreater(got.count("2"), 0)                                 # but the burst is spread, not a stampede

    def test_equal_load_uses_every_bot(self):
        bots = fleet(bot(1, 5), bot(2, 5), bot(3, 5))
        rng = random.Random(2)
        self.assertEqual({pick(bots, NOW, CFG, rng).bot_id for _ in range(300)}, {"1", "2", "3"})

    def test_dead_and_flood_waiting_bots_get_nobody(self):
        bots = fleet(bot(1, 0, hb=NOW - 60), bot(2, 0, pen=NOW + 30), bot(3, 50))
        for _ in range(100):
            self.assertEqual(pick(bots, NOW, CFG, random.Random()).bot_id, "3")
        self.assertIsNone(pick(fleet(bot(1, hb=NOW - 60)), NOW, CFG, random.Random()))

    def test_full_bots_are_skipped_while_one_has_room_and_never_refuse_when_all_are_full(self):
        self.assertEqual(pick(fleet(bot(1, 100), bot(2, 99)), NOW, CFG, random.Random()).bot_id, "2")
        self.assertIsNotNone(pick(fleet(bot(1, 150), bot(2, 120)), NOW, CFG, random.Random()))    # over capacity still answers

    def test_optimistic_counters_count_users_not_yet_in_any_heartbeat(self):
        bots = fleet(bot(1, 0), bot(2, 0))
        got = {pick(bots, NOW, CFG, random.Random(i), extra={"1": 100}).bot_id for i in range(50)}
        self.assertEqual(got, {"2"})                                           # bot 1 is "full" through extra alone


class GatewayTests(unittest.TestCase):
    def test_fresh_hint_on_a_healthy_bot_is_kept(self):
        b, why = decide_gateway("2", 120, fleet(bot(1, 1), bot(2, 60)), NOW, CFG, random.Random())
        self.assertEqual((b.bot_id, why), ("2", "sticky"))                    # even though bot 1 is much lighter

    def test_hint_older_than_the_ttl_is_rebalanced(self):
        seen = {decide_gateway("2", 901, fleet(bot(1, 1), bot(2, 60)), NOW, CFG, random.Random(i))[0].bot_id for i in range(50)}
        self.assertEqual(seen, {"1"})                                         # idle user goes to the lighter bot

    def test_hint_on_a_dead_penalised_or_full_bot_is_dropped(self):
        for dead in (bot(2, 5, hb=NOW - 99), bot(2, 5, pen=NOW + 20), bot(2, 100)):
            b, why = decide_gateway("2", 10, fleet(bot(1, 5), dead), NOW, CFG, random.Random())
            self.assertEqual((b.bot_id, why), ("1", "hint_unusable"))

    def test_no_hint_picks_a_bot_and_total_outage_still_returns_something_alive(self):
        self.assertEqual(decide_gateway(None, None, fleet(bot(1)), NOW, CFG, random.Random())[1], "new")
        b, why = decide_gateway(None, None, fleet(bot(1, pen=NOW + 10)), NOW, CFG, random.Random())
        self.assertEqual((b.bot_id, why), ("1", "degraded"))                  # flood-waiting beats a dead link
        self.assertEqual(decide_gateway(None, None, fleet(bot(1, hb=NOW - 99)), NOW, CFG, random.Random()), (None, "none"))


class ArrivalTests(unittest.TestCase):
    def a(self, bot_id, age):
        return Assignment(7, str(bot_id), NOW - age, 1)

    def test_active_on_this_bot_is_served_here(self):
        d = decide_arrival(self.a(1, 30), "1", fleet(bot(1, 99), bot(2, 0)), NOW, CFG, random.Random())
        self.assertEqual((d.action, d.bot_id, d.reason, d.write), (SERVE, "1", "sticky_here", False))     # never rebalanced while active

    def test_active_elsewhere_but_standing_in_this_chat_follows_the_user(self):
        d = decide_arrival(self.a(2, 30), "1", fleet(bot(1, 10), bot(2, 10)), NOW, CFG, random.Random())
        self.assertEqual((d.action, d.bot_id, d.reason, d.write), (SERVE, "1", "follow_user", True))      # no extra tap for the user

    def test_overloaded_here_sends_an_active_user_back_to_their_pinned_bot(self):
        d = decide_arrival(self.a(2, 30), "1", fleet(bot(1, 95), bot(2, 10)), NOW, CFG, random.Random())
        self.assertEqual((d.action, d.bot_id, d.reason, d.write), (MOVE, "2", "here_overloaded_back_to_pinned", False))

    def test_new_user_is_served_where_they_arrived(self):
        d = decide_arrival(None, "1", fleet(bot(1, 50), bot(2, 0)), NOW, CFG, random.Random())
        self.assertEqual((d.action, d.reason, d.write), (SERVE, "new_here", True))

    def test_idle_user_is_rebalanced_only_when_this_bot_is_overloaded_and_a_clearly_lighter_one_exists(self):
        busy = fleet(bot(1, 90), bot(2, 20))
        d = decide_arrival(self.a(1, 5000), "1", busy, NOW, CFG, random.Random())
        self.assertEqual((d.action, d.bot_id, d.reason, d.write), (MOVE, "2", "rebalance", True))
        near = fleet(bot(1, 90), bot(2, 80))                                   # 0.90 vs 0.80: not 0.15 lighter, a hop is not worth it
        self.assertEqual(decide_arrival(None, "1", near, NOW, CFG, random.Random()).action, SERVE)
        alone = fleet(bot(1, 95))
        self.assertEqual(decide_arrival(None, "1", alone, NOW, CFG, random.Random()).action, SERVE)       # never strand a user

    def test_a_bot_missing_from_the_snapshot_yet_counts_as_usable(self):
        d = decide_arrival(None, "9", fleet(bot(1, 50)), NOW, CFG, random.Random())
        self.assertEqual((d.action, d.bot_id), (SERVE, "9"))


class ConfigTests(unittest.TestCase):
    def test_defaults_and_overrides(self):
        c = RouterConfig.from_env({"STICKY_TTL": "600", "BOT_CAPACITY": "250"})
        self.assertEqual((c.sticky_ttl, c.capacity, c.hb_interval), (600, 250, 5))

    def test_every_problem_is_reported_at_once(self):
        with self.assertRaises(RouterConfigError) as cm:
            RouterConfig.from_env({"STICKY_TTL": "abc", "BOT_CAPACITY": "0", "ROUTER_STALE_AFTER": "5", "ROUTER_HB_INTERVAL": "5",
                                   "ROUTER_OVERLOAD_RATIO": "1.0", "ROUTER_HARD_RATIO": "0.5"})
        m = str(cm.exception)
        for part in ("STICKY_TTL must be a number", "BOT_CAPACITY must be between", "ROUTER_STALE_AFTER", "ROUTER_OVERLOAD_RATIO"):
            self.assertIn(part, m)

    def test_touch_interval_must_be_shorter_than_the_sticky_window(self):
        with self.assertRaises(RouterConfigError):
            RouterConfig.from_env({"STICKY_TTL": "60", "ROUTER_TOUCH_EVERY": "60"})


# =============================================================================== cookie
class CookieTests(unittest.TestCase):
    def test_round_trip_and_age(self):
        v = cookie.make(SECRET, "123456", NOW)
        self.assertEqual(cookie.parse(SECRET, v, NOW + 42), ("123456", 42))

    def test_forgery_and_garbage_are_rejected(self):
        v = cookie.make(SECRET, "123456", NOW)
        bot_id, ts, sig = v.split(".")
        for bad in (f"999999.{ts}.{sig}",                 # steer to another bot
                    f"{bot_id}.{int(ts) + 500}.{sig}",    # claim a younger timestamp
                    f"{bot_id}.{ts}.{'0' * 20}", "", None, "x", "a.b.c", "1" * 300, f"abc.{ts}.{sig}"):
            self.assertEqual(cookie.parse(SECRET, bad, NOW), (None, None), bad)
        self.assertEqual(cookie.parse(b"other-secret", v, NOW), (None, None))
        self.assertEqual(cookie.parse(SECRET, cookie.make(SECRET, "1", NOW + 9999), NOW), (None, None))     # from the future


# =============================================================================== router over a shared MongoDB
def make_router(fdb, clock, bot_id, cfg=CFG, seed=0):
    return Router(lambda n: fdb[n], cfg, bot_id=str(bot_id), username=f"bot{bot_id}", instance=f"svc{bot_id}", secret=SECRET,
                  clock=clock, rng=random.Random(seed))


async def publish(*routers):
    for r in routers:
        await r.heartbeat()
    for r in routers:
        await r.refresh()


class RouterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fdb, self.clock = FakeDB(yield_ops=True), Clock()
        self.r1, self.r2 = make_router(self.fdb, self.clock, 1), make_router(self.fdb, self.clock, 2)
        await publish(self.r1, self.r2)

    def writes(self, coll=ASSIGN):
        """Count write operations on a collection (insert/update/find_one_and_update)."""
        n = {"w": 0}
        col = self.fdb[coll]
        for name in ("insert_one", "update_one", "find_one_and_update"):
            orig = getattr(col, name)

            async def wrapped(*a, _o=orig, **k):
                n["w"] += 1
                return await _o(*a, **k)
            setattr(col, name, wrapped)
        return n

    async def test_fleet_view_contains_both_bots_and_heartbeats_report_active_users(self):
        self.assertEqual(sorted(self.r1.usernames()), ["bot1", "bot2"])
        for uid in range(1, 6):
            await self.r1.on_arrival(uid)
        await self.r1.heartbeat()
        self.assertEqual((await self.fdb[BOTS].find_one({"_id": "1"}))["active"], 5)
        self.clock.advance(901)                                             # all five went idle
        await self.r1.heartbeat()
        self.assertEqual((await self.fdb[BOTS].find_one({"_id": "1"}))["active"], 0)

    async def test_active_user_stays_and_causes_at_most_one_write_per_touch_interval(self):
        d = await self.r1.on_arrival(1)
        self.assertEqual((d.action, d.reason), (SERVE, "new_here"))
        n = self.writes()
        for _ in range(40):                                                 # 40 interactions inside 39 seconds
            self.clock.advance(1)
            d = await self.r1.on_arrival(1)
            await self.r1.touch(1)
            self.assertEqual((d.action, d.reason), (SERVE, "sticky_here"))
        self.assertEqual(n["w"], 0)                                         # all answered from memory
        self.clock.advance(30)                                              # 69 s since the last write: one slide is due
        await self.r1.touch(1)
        self.assertEqual(n["w"], 1)
        row = await self.fdb[ASSIGN].find_one({"_id": 1})
        self.assertEqual(row["last_active"], self.clock.t)

    async def test_user_who_keeps_clicking_is_never_rebalanced_even_when_their_bot_gets_busy(self):
        await self.r1.on_arrival(1)
        for step in range(30):                                              # 30 minutes of activity every minute
            self.clock.advance(60)
            await self.r1.touch(1)
            if step == 5:                                                   # bot 1 fills up, bot 2 is empty
                for u in range(100, 195):
                    await self.fdb[ASSIGN].insert_one({"_id": u, "bot_id": "1", "ver": 1, "last_active": self.clock.t})
                await self.r1.heartbeat(); await self.r2.heartbeat(); await self.r1.refresh()
            d = await self.r1.on_arrival(1)
            self.assertEqual((d.action, d.reason), (SERVE, "sticky_here"), step)

    async def test_idle_user_returning_to_an_overloaded_bot_is_moved_to_the_lighter_one(self):
        await self.r1.on_arrival(1)
        self.clock.advance(1200)                                            # idle for 20 min > STICKY_TTL
        for u in range(100, 195):                                           # meanwhile bot 1 filled up with active users
            await self.fdb[ASSIGN].insert_one({"_id": u, "bot_id": "1", "ver": 1, "last_active": self.clock.t})
        await publish(self.r1, self.r2)
        d = await self.r1.on_arrival(1)
        self.assertEqual((d.action, d.bot_id, d.reason), (MOVE, "2", "rebalance"))
        self.assertEqual((await self.fdb[ASSIGN].find_one({"_id": 1}))["bot_id"], "2")               # pinned to the new bot already
        d2 = await self.r2.on_arrival(1)                                    # ... so when they arrive there nothing moves again
        self.assertEqual((d2.action, d2.reason), (SERVE, "sticky_here"))

    async def test_user_pinned_to_bot1_who_shows_up_at_bot2_is_followed_not_bounced(self):
        await self.r1.on_arrival(1)
        d = await self.r2.on_arrival(1)
        self.assertEqual((d.action, d.reason), (SERVE, "follow_user"))
        self.assertEqual((await self.fdb[ASSIGN].find_one({"_id": 1}))["bot_id"], "2")

    async def test_simultaneous_first_arrivals_on_two_bots_leave_exactly_one_consistent_row(self):
        res = await asyncio.gather(*[(self.r1 if u % 2 else self.r2).on_arrival(u // 2) for u in range(40)])
        self.assertTrue(all(d.action == SERVE for d in res))                # nobody was bounced or blocked
        rows = self.fdb[ASSIGN].docs
        self.assertEqual(len(rows), 20)                                     # one row per user, never two
        self.assertEqual(len({r["_id"] for r in rows}), 20)
        self.assertTrue(all(r["bot_id"] in ("1", "2") and r["ver"] >= 1 for r in rows))

    async def test_lost_compare_and_set_redecides_against_the_winner(self):
        await self.r1.on_arrival(1)
        stale = await self.r2._get(1, self.clock.t)                         # bot 2 read version 1 ...
        await self.r1._write(1, "1", stale, self.clock.t)                   # ... bot 1 bumps it to version 2 meanwhile
        won, cur = await self.r2._write(1, "2", stale, self.clock.t)
        self.assertFalse(won)
        self.assertEqual((cur.bot_id, cur.ver), ("1", 2))
        self.assertEqual(self.r2.stats["cas_lost"], 1)

    async def test_dead_bot_stops_receiving_users_and_a_returning_bot_is_trusted_again(self):
        self.clock.advance(20)                                              # bot 2 stops heartbeating (crash); bot 1 keeps going
        await self.r1.heartbeat(); await self.r1.refresh()
        self.assertEqual(self.r1.usernames(), ["bot1"])
        for i in range(30):
            b, _why, _c = self.r1.gateway(None)
            self.assertEqual(b.bot_id, "1")
        await publish(self.r2, self.r1)                                     # bot 2 is back
        self.assertEqual(sorted(self.r1.usernames()), ["bot1", "bot2"])

    async def test_sticky_cookie_follows_the_user_and_a_dead_bot_forces_failover_even_inside_the_window(self):
        b, why, c = self.r1.gateway(None)
        first = b.bot_id
        self.clock.advance(300)
        for _ in range(20):                                                 # inside the window the browser keeps its bot
            b2, why2, c = self.r1.gateway(c)
            self.assertEqual((b2.bot_id, why2), (first, "sticky"))
        dead = "2" if first == "2" else "1"
        survivor = r_other = self.r1 if dead == "2" else self.r2
        self.clock.advance(20)                                              # the pinned bot dies
        await survivor.heartbeat(); await survivor.refresh()
        b3, why3, _ = survivor.gateway(c)
        self.assertNotEqual(b3.bot_id, dead)
        self.assertEqual(why3, "hint_unusable")

    async def test_database_blip_keeps_the_fleet_view_for_the_grace_period_then_falls_back_to_self(self):
        self.fdb.down = True
        self.clock.advance(10)
        with self.assertRaises(Exception):
            await self.r1.refresh()
        self.assertEqual(sorted(self.r1.usernames()), ["bot1", "bot2"])     # NOT "everything on bot 1" because of a database hiccup
        seen = {self.r1.gateway(None)[0].bot_id for _ in range(60)}
        self.assertEqual(seen, {"1", "2"})
        self.clock.advance(120)                                             # outage longer than ROUTER_SNAPSHOT_GRACE
        self.assertEqual(self.r1.usernames(), ["bot1"])
        self.fdb.down = False
        await publish(self.r1, self.r2)
        self.assertEqual(sorted(self.r1.usernames()), ["bot1", "bot2"])

    async def test_arrival_still_answers_when_the_database_is_down_for_a_cached_user(self):
        await self.r1.on_arrival(1)
        self.fdb.down = True
        d = await self.r1.on_arrival(1)                                     # cache hit + throttled touch: no database access needed
        self.assertEqual((d.action, d.reason), (SERVE, "sticky_here"))

    async def test_telegram_flood_wait_stops_new_users_on_that_bot_but_keeps_existing_ones(self):
        await self.r2.on_arrival(5)
        self.r2.penalize(30)
        await self.r2.heartbeat()                                           # published immediately in production via the wake-up event
        await self.r1.refresh()
        self.assertEqual(self.r1.usernames(), ["bot1"])
        self.assertEqual({self.r1.gateway(None)[0].bot_id for _ in range(30)}, {"1"})
        d = await self.r2.on_arrival(5)
        self.assertEqual((d.action, d.reason), (SERVE, "sticky_here"))      # the user already inside bot 2 is not thrown out
        self.clock.advance(31)
        await publish(self.r1, self.r2)
        self.assertEqual(sorted(self.r1.usernames()), ["bot1", "bot2"])

    async def test_penalize_accepts_timedelta_and_clamps_silly_values(self):
        from datetime import timedelta
        self.r1.penalize(timedelta(seconds=7))
        self.assertAlmostEqual(self.r1._penalty_until - self.clock.t, 7, delta=0.01)
        self.r1.penalize(10 ** 9)
        self.assertLessEqual(self.r1._penalty_until - self.clock.t, 300)

    async def test_pin_here_for_direct_mini_app_launch(self):
        await self.r2.pin_here(77)
        self.assertEqual((await self.fdb[ASSIGN].find_one({"_id": 77}))["bot_id"], "2")
        await self.r1.pin_here(77)                                          # same user later works on bot 1: pinned there, no error
        self.assertEqual((await self.fdb[ASSIGN].find_one({"_id": 77}))["bot_id"], "1")

    async def test_cache_is_bounded(self):
        small = make_router(self.fdb, self.clock, 1, cfg=replace(CFG, cache_size=100))
        for u in range(500):
            await small.on_arrival(u)
        self.assertLessEqual(len(small._cache), 100)

    async def test_snapshot_exposes_no_user_ids(self):
        await self.r1.on_arrival(123456789)
        s = str(self.r1.snapshot())
        self.assertNotIn("123456789", s)
        self.assertIn("bot2", s)

    async def test_nothing_in_the_heartbeat_collection_identifies_users(self):
        await self.r1.on_arrival(123456789)
        await self.r1.heartbeat()
        self.assertNotIn("123456789", str(self.fdb[BOTS].docs))


# =============================================================================== fleet simulations
class SimulationTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_users_spread_evenly_over_three_bots_and_stay_put_while_active(self):
        fdb, clock = FakeDB(), Clock()
        cfg = replace(CFG, capacity=100_000)
        rs = [make_router(fdb, clock, i, cfg=cfg, seed=i) for i in (1, 2, 3)]
        await publish(*rs)
        by_id = {r.bot_id: r for r in rs}
        home: dict[int, str] = {}
        cookies: dict[int, str] = {}
        moves = 0
        for tick in range(150):                                              # 2.5 minutes: 5 new users + 8 returning clicks per second
            clock.advance(1)
            if tick % 5 == 0:
                await publish(*rs)                                           # heartbeat + refresh cycle
            for k in range(5):
                uid = tick * 5 + k
                gw = rs[uid % 3]                                             # clicks land on any gateway instance
                b, _why, cookies[uid] = gw.gateway(None)
                d = await by_id[b.bot_id].on_arrival(uid)
                home[uid] = d.bot_id
            for k in range(8):
                uid = random.Random(tick * 31 + k).randrange(0, tick * 5 + 5)
                b, _why, cookies[uid] = rs[uid % 3].gateway(cookies.get(uid))
                d = await by_id[b.bot_id].on_arrival(uid)
                if d.bot_id != home[uid] or d.action != SERVE:
                    moves += 1
                    home[uid] = d.bot_id
        counts = [list(home.values()).count(i) for i in "123"]
        self.assertGreater(min(counts) / max(counts), 0.85, counts)          # balanced within ~15 %
        self.assertLess(moves / 1_200, 0.01, moves)                           # active users practically never hop (<1 % of 1.2k returning visits)

    async def test_burst_on_one_gateway_is_not_a_stampede_on_the_lightest_bot(self):
        fdb, clock = FakeDB(), Clock()
        cfg = replace(CFG, capacity=100_000)
        rs = [make_router(fdb, clock, i, cfg=cfg, seed=i) for i in (1, 2, 3)]
        for u in range(3000):                                                # bot 1 is already busy
            await fdb[ASSIGN].insert_one({"_id": -u - 1, "bot_id": "1", "ver": 1, "last_active": clock.t})
        await publish(*rs)
        picks = [rs[0].gateway(None)[0].bot_id for _ in range(3000)]        # 3000 clicks inside one refresh interval, no heartbeat in between
        share = [picks.count(i) / 3000 for i in "123"]
        self.assertEqual(picks.count("1"), 0)                                # the busy bot gets nothing
        self.assertGreater(min(share[1], share[2]), 0.3, share)              # the two light bots both take a real share


if __name__ == "__main__":
    unittest.main()
