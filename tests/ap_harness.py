"""Auto-poster tests: the REAL autoposter/*, db.py, services/posts.py, retry.py, config.py with stub telegram / motor / bson (tests/ap_stubs).
Run by tests/test_autoposter.py in a subprocess (like the other harnesses)."""
import asyncio, itertools, os, sys, traceback
ROOT = sys.argv[1]
sys.path.insert(0, ROOT + "/tests/ap_stubs"); sys.path.insert(0, ROOT)
os.environ.update(MAIN_BOT_TOKEN="1:T", OWNER_ID="42", MONGO_URI="mongodb://x", WEBHOOK_SECRET="test-webhook-secret-0123456789", BASE_URL="https://x.example",
                  PRIMARY_CHANNEL="-1001", BACKUP_CHANNEL="-1002", MONETAG_POSTBACK_SECRET="unit-test-secret-0123456789", APP_ENV="development",
                  AP_TIMEZONE="Asia/Kolkata", AP_POST_GAP_SECONDS="0", AP_RETRY_BASE_SECONDS="0", AP_IMPORT_PACE_SECONDS="0", AP_IMPORT_LIMIT="50",
                  POST_CAPTION="Watch FREE\\nTap below", POST_BUTTON_TEXT="Play", POST_MAX_VIDEOS="3")

from datetime import datetime, timedelta, UTC
from types import SimpleNamespace as NS
from telegram.error import BadRequest, Forbidden, RetryAfter, TimedOut
from telegram.ext import ApplicationHandlerStop
import db as filedb
from autoposter.config import SETTINGS
from autoposter.state import STATE
from autoposter.database import Database
from autoposter import utils, sender, ui, handlers, payloads, jobs, importer, integration


class Msg(NS):
    @property
    def message_id(self): return self.id


class FakeBot:
    id = 999
    username = "filebot"
    def __init__(self): self.calls = []; self.fail = []; self.n = itertools.count(100); self.source = {}
    def _maybe_fail(self, name, kw):
        for i, (m, exc) in enumerate(self.fail):
            if m == name:
                e = exc[0]
                if exc[1] is not None:
                    left = exc[1] - 1
                    if left <= 0: self.fail.pop(i)
                    else: self.fail[i] = (m, (e, left))
                raise e
    def __getattr__(self, name):
        async def call(*a, **k):
            self.calls.append((name, a, k)); self._maybe_fail(name, k)
            if name == "send_media_group": return [Msg(id=next(self.n)) for _ in k["media"]]
            if name == "get_chat_member": return NS(status="administrator", can_post_messages=True)
            if name == "get_chat": return NS(id=-1009, type="channel", title="Source", username=None)
            if name in ("pin_chat_message", "delete_message", "delete_messages", "edit_message_text", "unpin_chat_message"): return True
            if name == "forward_message":
                mid = k["message_id"]
                spec = self.source.get(mid)
                if spec is None: raise BadRequest("Message to forward not found")
                return Msg(id=next(self.n), **spec)
            return Msg(id=next(self.n))
        return call
    def sent(self, name=None): return [c for c in self.calls if name is None or c[0] == name]


passed = 0
def check(cond, label):
    global passed
    if not cond: raise AssertionError(label)
    passed += 1


def fresh():
    STATE.db = Database(filedb._db); STATE.title_cache.clear(); STATE.posting_locks.clear(); STATE.imports.clear(); STATE.debounce.clear()
    STATE.manager = NS(username="filebot")
    for name in list(filedb._db.keys()): filedb._db[name].docs.clear()


async def mkchat(cid=-1001, title="Chan", n=3, **opts):
    await STATE.db.ensure_chat(cid, title, "channel", "chan")
    if opts: await STATE.db.set_options(cid, **opts)
    for i in range(n):
        await STATE.db.add_post(cid, {"media_type": "text", "text": f"post {i}", "fingerprint": utils.text_fingerprint(f"post {i}"), "buttons": []})
    await sender.after_posts_added(cid)
    return await STATE.db.get_chat(cid)


# ------------------------------------------------------------------ engine (ported from the stand-alone auto-poster)
async def t_utils():
    n = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
    s = utils.default_schedule(); s.update(start_time="09:00", end_time="21:00", interval_minutes=180)
    nxt = utils.compute_next_run(n, s).astimezone(utils.TZ)
    check(nxt.hour == 18 and nxt.minute == 0, f"next slot {nxt}")
    s2 = {**s, "start_time": "20:00", "end_time": "02:00", "interval_minutes": 60}
    r = utils.compute_next_run(datetime(2026, 10, 25, 19, 0, tzinfo=UTC), s2).astimezone(utils.TZ)
    check(r.hour == 1 and r.day == 26, f"overnight carry {r}")
    body, f = utils.parse_post_markup("Hello\n#silent\n#pin\n---buttons---\nA|example.com ; B|https://x.y")
    check(body == "Hello" and f["silent"] and f["pin"] and len(f["buttons"][0]) == 2, "markup")
    p = utils.parse_schedule_text("interval=2h\nbatch=4\nstart=09:00\nend=22:30\ndays=0,1")
    check(p["interval_minutes"] == 120 and p["batch_size"] == 4, "sched text")
    for bad in ("interval=x", "batch=0", "start=25:00", "days=9", "hello"):
        try: utils.parse_schedule_text(bad); check(False, f"should fail {bad}")
        except ValueError: check(True, "")
    check(SETTINGS.admin_ids == {42} and SETTINGS.timezone == "Asia/Kolkata", "settings from env")


async def t_engine():
    fresh(); bot = FakeBot(); await mkchat(n=3)
    await STATE.db.update_schedule(-1001, batch_size=2)
    check(await sender.process_chat_batch(bot, -1001) == 2, "batch of 2")
    check(await sender.process_chat_batch(bot, -1001) == 1, "remaining 1")
    chat = await STATE.db.get_chat(-1001)
    check(chat["stats"]["total_cycles_completed"] == 1, "cycle counted once")
    check(await sender.process_chat_batch(bot, -1001) == 2, "new cycle")
    check(chat["next_post_time"] is not None and (await STATE.db.get_chat(-1001))["next_post_time"] > utils.utcnow(), "next in future")
    kw = bot.sent("send_message")[0][2]
    check(kw["reply_markup"].inline_keyboard[-1][0].callback_data == "react:0", "reaction row")


async def t_one_pass_and_errors():
    fresh(); bot = FakeBot(); await mkchat(n=2, loop=False)
    await STATE.db.update_schedule(-1001, batch_size=5)
    sent = await sender.process_chat_batch(bot, -1001); chat = await STATE.db.get_chat(-1001)
    check(sent == 2 and chat["is_active"] is False, "one-pass pauses")
    fresh(); bot = FakeBot(); await mkchat(n=3); await STATE.db.update_schedule(-1001, batch_size=3)
    bot.fail.append(("send_message", (Forbidden("kicked"), 1)))
    sent = await sender.process_chat_batch(bot, -1001); chat = await STATE.db.get_chat(-1001)
    check(sent == 0 and not chat["is_active"] and chat["processing_until"] is None, "forbidden pauses, lock released")
    await STATE.db.set_active(-1001, True)
    bot.fail.append(("send_message", (RetryAfter(300), 1)))
    await sender.process_chat_batch(bot, -1001); chat = await STATE.db.get_chat(-1001)
    check(chat["is_active"] and chat["next_post_time"] > utils.utcnow() + timedelta(seconds=250), "flood defers")
    fresh(); bot = FakeBot(); await mkchat(n=1)
    for _ in range(3):
        bot.fail.append(("send_message", (BadRequest("bad"), 1)))
        await sender.process_chat_batch(bot, -1001); await sender.rebuild_and_ensure_schedule(-1001)
    check(await STATE.db.count_posts(-1001) == 0, "quarantined after 3 fails")


async def t_media_and_reactions_off():
    fresh(); bot = FakeBot(); await mkchat(n=0, footer="— @chan")
    opts = utils.chat_opts(await STATE.db.get_chat(-1001))
    for p in ({"media_type": "photo", "file_id": "f", "caption": "c"}, {"media_type": "copy", "source_chat_id": 1, "source_message_id": 2},
              {"media_type": "album", "media_group": [{"media_type": "photo", "file_id": "a"}, {"media_type": "video", "file_id": "b"}], "caption": "x"}):
        check((await sender.send_saved_post(bot, -1001, p, opts)).message_id, p["media_type"])
    check(bot.sent("send_photo")[0][2]["caption"].endswith("— @chan"), "footer appended")
    SETTINGS.reactions = False
    try:
        check(sender.build_post_markup({"buttons": []}) is None, "no reactions + no buttons -> no markup")
        n = len(bot.sent("send_message"))
        await sender.send_saved_post(bot, -1001, {"media_type": "album", "media_group": [{"media_type": "photo", "file_id": "a"}, {"media_type": "photo", "file_id": "b"}], "caption": "x"}, opts)
        check(len(bot.sent("send_message")) == n, "album without reactions has no anchor message")
    finally:
        SETTINGS.reactions = True


# ------------------------------------------------------------------ file posts + importer
async def mkfile(token="tok1", head=500, fid="HEADFID", kind="video"):
    await filedb.save_post(token, [{"locations": [{"channel": -1001, "msg": 1}], "caption": ""}], "cap", fid, thumb_type=kind, src_chat=-1009, src_head=head)


async def t_filepost_send():
    fresh(); bot = FakeBot(); await mkchat(n=0); await mkfile()
    await STATE.db.add_post(-1001, {"media_type": "filepost", "token": "tok1", "fingerprint": "fp:-1009:500", "caption": "x"})
    await sender.after_posts_added(-1001)
    check(await sender.process_chat_batch(bot, -1001) == 1, "file post sent")
    call = bot.sent("send_video")[0][2]
    check(call["video"] == "HEADFID" and call["chat_id"] == -1001, "head video sent to the channel")
    check(call["caption"] == "cap", "caption comes from the file record")
    btn = call["reply_markup"].inline_keyboard[0][0]
    check(btn.url == "https://t.me/filebot?start=tok1" and btn.text == "Play", f"button -> file link {btn.url}")
    f = await filedb.get_file("tok1")
    check(len(f["channel_posts"]) == 1 and f["channel_posts"][0]["channel"] == -1001, "channel post recorded for cleanup")
    await filedb.files.delete_one({"token": "tok1"})                      # the file was deleted in the bot
    await sender.rebuild_and_ensure_schedule(-1001)
    check(await sender.process_chat_batch(bot, -1001) == 0, "nothing to send")
    check(await STATE.db.count_posts(-1001) == 0, "post of a deleted file is quarantined at once (not after 3 tries)")


async def t_filepost_cap_and_label():
    fresh(); await mkfile()
    for _ in range(205): await filedb.add_channel_post("tok1", -1001, 7)
    check(len((await filedb.get_file("tok1"))["channel_posts"]) == 200, "channel_posts capped")
    check("tok1" in ui.post_label({"media_type": "filepost", "token": "tok1"}), "label")


async def t_classify_group():
    V = lambda cap=None: NS(video=NS(file_id="v"), caption=cap, animation=None, photo=None, document=None)
    check(importer.classify(1, V("Title")).role == "head", "video + caption = head")
    check(importer.classify(1, V(None)).role == "file", "video no caption = file")
    check(importer.classify(1, NS(video=None, animation=NS(file_id="g"), photo=None, document=NS(file_id="d"), caption=None)).media == "animation", "gif is not a plain document")
    check(importer.classify(1, NS(video=None, animation=None, photo=[NS(file_id="s"), NS(file_id="b")], document=None, caption="c")).file_id == "b", "photo head uses biggest size")
    check(importer.classify(1, NS(video=None, animation=None, photo=[NS(file_id="s")], document=None, caption=None)).role == "skip", "bare photo skipped")
    check(importer.classify(1, NS(video=None, animation=None, photo=None, document=None, caption="t", text="t")).role == "skip", "text skipped")
    g = importer.Grouper(max_files=2)
    out = [g.feed(importer.Item(i, r)) for i, r in enumerate(["file", "head", "file", "file", "file", "head", "skip", "head"], 1)]
    out = [o for o in out if o] + [g.finish()]
    check(g.orphans == 1 and g.skipped == 1 and g.overflow == 1, "orphan / skipped / overflow counted")
    check([len(o.files) for o in out] == [2, 0, 0] and out[0].overflow == 1, "groups")
    check(importer.post_caption(importer.Item(1, "head", caption="Src"), "TPL", use_source=False) == "TPL", "variable only by default")
    check(importer.post_caption(importer.Item(1, "head", caption="Src"), "TPL", use_source=True) == "Src\n\nTPL", "source caption on top")


def script_source(bot):
    v = lambda cap=None: dict(video=NS(file_id=f"vid{cap}"), caption=cap, animation=None, photo=None, document=None)
    bot.source = {
        9: v(None), 10: v("A"), 11: v(None), 12: dict(video=None, animation=None, photo=None, document=NS(file_id="doc12"), caption=None),
        14: v("B"), 15: v(None), 16: dict(video=None, animation=NS(file_id="gif16"), photo=None, document=None, caption="C"),
        17: dict(video=None, animation=None, photo=[NS(file_id="p")], document=None, caption=None),
    }                                                                       # 13 is deleted (forward raises)


async def t_import_range():
    fresh(); bot = FakeBot(); script_source(bot); await mkchat(n=0)
    rep = await importer.import_range(bot, target_chat_id=-1001, source_chat=-1009, lo=9, hi=17, scratch=42)
    check((rep.heads, rep.created, rep.queued, rep.no_files, rep.orphans, rep.missing, rep.skipped) == (3, 2, 2, 1, 1, 1, 1), f"report {rep}")
    check(len(bot.sent("forward_message")) == 9 and len(bot.sent("delete_messages")) >= 1, "every message inspected, scratch copies deleted")
    check(len(bot.sent("copy_message")) == 6, "3 files x (primary + backup)")
    docs = filedb._db["files"].docs
    check(sorted(d["src_head"] for d in docs) == [10, 14] and all(d["src_chat"] == -1009 for d in docs), "file records remember their source head")
    a = next(d for d in docs if d["src_head"] == 10)
    check(len(a["items"]) == 2 and a["photo"] == "vidA" and a["thumb_type"] == "video" and a["post_caption"] == "Watch FREE\nTap below", "post = head + its 2 files + POST_CAPTION")
    posts = STATE.db.posts_coll(-1001).docs
    check(sorted(p["fingerprint"] for p in posts) == ["fp:-1009:10", "fp:-1009:14"] and all(p["media_type"] == "filepost" for p in posts), "queued as file posts")
    await sender.after_posts_added(-1001)
    # second run: nothing is copied or queued twice
    n_copy = len(bot.sent("copy_message"))
    rep2 = await importer.import_range(bot, target_chat_id=-1001, source_chat=-1009, lo=9, hi=17, scratch=42)
    check((rep2.created, rep2.reused, rep2.queued, rep2.dups) == (0, 2, 0, 2) and len(bot.sent("copy_message")) == n_copy, f"rerun is idempotent {rep2}")
    # same source into ANOTHER target reuses the files (no second copy)
    await mkchat(cid=-1002, title="Other", n=0)
    rep3 = await importer.import_range(bot, target_chat_id=-1002, source_chat=-1009, lo=9, hi=17, scratch=42)
    check((rep3.created, rep3.reused, rep3.queued) == (0, 2, 2) and len(bot.sent("copy_message")) == n_copy, "second target reuses links")
    # the scheduled batch posts them
    await STATE.db.update_schedule(-1001, batch_size=5)
    check(await sender.process_chat_batch(bot, -1001) == 2 and len(bot.sent("send_video")) == 2, "scheduler publishes both file posts")


async def t_import_abort_and_stop():
    fresh(); bot = FakeBot(); script_source(bot); await mkchat(n=0)
    bot.source[18] = dict(video=NS(file_id="vD"), caption="D", animation=None, photo=None, document=None)
    bot.source[19] = dict(video=NS(file_id="v19"), caption=None, animation=None, photo=None, document=None)
    bot.fail.append(("copy_message", (BadRequest("Not enough rights"), None)))
    rep = await importer.import_range(bot, target_chat_id=-1001, source_chat=-1009, lo=10, hi=19, scratch=42)
    check(rep.error and "failed in a row" in rep.error and rep.failed == 3, f"aborts after 3 failures: {rep.error}")
    check(filedb._db["files"].docs == [] and rep.queued == 0, "nothing half-created")
    fresh(); bot = FakeBot(); script_source(bot); await mkchat(n=0)
    stop = asyncio.Event(); stop.set()
    rep = await importer.import_range(bot, target_chat_id=-1001, source_chat=-1009, lo=10, hi=17, scratch=42, stop=stop)
    check(rep.stopped and rep.scanned == 0, "stop flag honoured")
    fresh(); bot = FakeBot(); script_source(bot); await mkchat(n=0)
    bot.fail.append(("forward_message", (BadRequest("Bad Request: protected content"), None)))
    rep = await importer.import_range(bot, target_chat_id=-1001, source_chat=-1009, lo=10, hi=17, scratch=42)
    check(rep.error and "Restrict saving content" in rep.error, "protected channel explained")
    fresh(); bot = FakeBot(); script_source(bot); await mkchat(n=0)
    bot.fail.append(("forward_message", (Forbidden("bot can't initiate conversation"), None)))
    rep = await importer.import_range(bot, target_chat_id=-1001, source_chat=-1009, lo=10, hi=17, scratch=42)
    check(rep.error and "scratch" in rep.error, "scratch chat problem explained")


# ------------------------------------------------------------------ handlers / wiring
class CQ:
    def __init__(self, data, uid=42): self.data = data; self.from_user = NS(id=uid); self.message = NS(chat_id=42, message_id=77); self.edits = []; self.answers = []
    async def answer(self, text=None, show_alert=False): self.answers.append((text, show_alert))
    async def edit_message_text(self, text, reply_markup=None, parse_mode=None): self.edits.append((text, reply_markup))


def upd(cq=None, text=None, uid=42, chat_type="private"):
    msg = None
    if text is not None:
        msg = NS(text=text, text_html=text, caption=None, media_group_id=None, photo=None, video=None, document=None, replies=[])
        async def reply_text(t, **k): msg.replies.append(t)
        msg.reply_text = reply_text
        async def delete(): pass
        msg.delete = delete
    return NS(callback_query=cq, effective_user=NS(id=uid), effective_chat=NS(id=42, type=chat_type), effective_message=msg)


async def t_gate_and_routing():
    fresh(); bot = FakeBot(); ctx = NS(bot=bot)
    await mkchat(n=1)
    # no mode: the gate lets the file bot handle the message
    await handlers.mode_gate(upd(text="hello"), ctx)
    check(True, "gate passes through without a mode")
    await handlers.mode_gate(upd(text="hello", uid=5), ctx)
    # edit_schedule mode: consumed, schedule updated
    await handlers.set_admin_meta(42, mode="edit_schedule", mode_chat_id=-1001)
    try:
        await handlers.mode_gate(upd(text="interval=2h\nbatch=7"), ctx); check(False, "gate must stop propagation")
    except ApplicationHandlerStop:
        check(True, "")
    s = (await STATE.db.get_chat(-1001))["schedule"]
    check(s["interval_minutes"] == 120 and s["batch_size"] == 7, "schedule edited through the gate")
    # non-owner callback is refused; owner navigates
    cq = CQ("nav:dashboard", uid=5); await handlers.handle_callback(NS(callback_query=cq), ctx)
    check(cq.answers[0][0] == "Unauthorized", "non-owner refused")
    cq = CQ("nav:dashboard"); await handlers.handle_callback(NS(callback_query=cq), ctx)
    kb = cq.edits[0][1].inline_keyboard
    flat = [b.callback_data for row in kb for b in row]
    check("ip:start" in flat and "m_home" in flat, "dashboard has Channel->Posts and the way back to the main menu")
    # callback data of this module never collides with the file bot's owner pattern
    import re
    file_bot = re.compile(r"^(m_|f_list:|f:|fd:|fdc:|pc:|p_done$|noop$)")
    from autoposter import ui as _ui
    mine = re.compile(handlers.CALLBACK_PATTERN)
    for data in ("nav:dashboard", "up:start", "ip:go", "chat:open:-1", "pl:-1:0", "pv:-1:abc:0", "pa:now:-1:abc:0", "sc:int:-1", "ss:int:-1:0", "op:loop:-1", "od:-1:0", "mode:cancel", "backup:export", "db:wipe", "target:add"):
        check(mine.match(data) and not file_bot.match(data), f"pattern {data}")
    for data in ("m_home", "f:abc", "pc:abc", "p_done", "noop", "sa:home", "home"):
        check(not mine.match(data), f"not mine {data}")


async def t_import_flow():
    fresh(); bot = FakeBot(); script_source(bot); ctx = NS(bot=bot); await mkchat(n=0)
    await STATE.db.set_active_target(42, -1001)
    cq = CQ("ip:start"); await handlers.handle_callback(NS(callback_query=cq), ctx)
    check((await handlers.admin_doc(42))["mode"] == "post_import_first", "asks for the first link")
    for text in ("https://t.me/c/9/9", "https://t.me/c/9/17"):
        u = upd(text=text)
        try: await handlers.mode_gate(u, ctx)
        except ApplicationHandlerStop: pass
    doc = await handlers.admin_doc(42)
    check(doc["mode"] == "post_import_confirm" and (doc["import_lo"], doc["import_hi"]) == (9, 17), f"range stored {doc}")
    cq = CQ("ip:go"); await handlers.handle_callback(NS(callback_query=cq), ctx)
    run = STATE.imports.get(42); check(run is not None, "import started")
    await run["task"]
    check(42 not in STATE.imports, "run registry cleared")
    final = [c for c in bot.sent("edit_message_text")][-1][2]["text"]
    check("Done" in final and "Posts queued: <b>2</b>" in final, f"final report shown: {final[:80]}")
    check(await STATE.db.count_posts(-1001) == 2, "posts are in the queue")
    # too large range is refused
    await handlers.set_admin_meta(42, mode="post_import_last", mode_chat_id=-1001, source_chat_id=-1009, from_message_id=1)
    u = upd(text="https://t.me/c/9/900")
    try: await handlers.mode_gate(u, ctx)
    except ApplicationHandlerStop: pass
    check("bahut bada" in u.effective_message.replies[0], "range over AP_IMPORT_LIMIT refused")
    # stale confirm button
    await handlers.clear_admin_mode(42)
    cq = CQ("ip:go"); await handlers.handle_callback(NS(callback_query=cq), ctx)
    check(cq.answers and cq.answers[0][1] is True, "stale Start button refused")


async def t_reserved_chats():
    fresh(); check(handlers.is_reserved_chat(-1001) and handlers.is_reserved_chat(-1002) and not handlers.is_reserved_chat(-1555), "storage channels are never targets")
    bot = FakeBot()
    change = NS(chat=NS(id=-1001, type="channel", title="Primary", username=None), new_chat_member=NS(status="administrator"), from_user=NS(id=42))
    await handlers.my_chat_member_handler(NS(my_chat_member=change), NS(bot=bot))
    check(await STATE.db.get_chat(-1001) is None, "adding the bot to PRIMARY does not register a target")
    change = NS(chat=NS(id=-1777, type="channel", title="Real", username="real"), new_chat_member=NS(status="administrator"), from_user=NS(id=42))
    await handlers.my_chat_member_handler(NS(my_chat_member=change), NS(bot=bot))
    check(await STATE.db.get_chat(-1777) is not None, "a normal channel registers")


async def t_jobs_and_wipe():
    fresh(); bot = FakeBot(); await mkchat(n=2, auto_delete_minutes=60)
    await STATE.db.chats.update_one({"chat_id": -1001}, {"$set": {"next_post_time": utils.utcnow() - timedelta(seconds=5)}})
    await jobs.scheduler_tick(bot)
    check(len(bot.sent("send_message")) >= 1 and await STATE.db.deletes.count_documents({}) >= 1, "scheduler tick posts + schedules auto-delete")
    await STATE.db.deletes.update_many({}, {"$set": {"delete_at": utils.utcnow() - timedelta(seconds=1)}})
    await jobs.housekeeping_tick(bot)
    check(len(bot.sent("delete_message")) >= 1 and await STATE.db.deletes.count_documents({}) == 0, "housekeeping deletes due posts")
    await jobs.daily_job(bot)
    check(len(bot.sent("send_document")) == 1 and bot.sent("send_document")[0][1][0] == 42, "daily backup sent to the owner")
    check(0 < jobs.seconds_until_daily() <= 86400, "daily wait")
    filedb._db["files"].docs.append({"token": "keep"})                     # wipe must never touch the file bot's data
    await STATE.db.wipe_all()
    check(await STATE.db.list_chats() == [] and filedb._db["files"].docs == [{"token": "keep"}], "wipe_all removes auto-poster data only")


async def t_backup_restore():
    fresh(); await mkchat(n=2)
    payload = await STATE.db.backup_payload()
    await STATE.db.wipe_all()
    chats, posts = await STATE.db.restore_payload(payload)
    check((chats, posts) == (1, 2), "restore")


async def t_integration():
    fresh()
    check(integration.allowed_updates() == ["message", "callback_query", "channel_post", "my_chat_member", "chat_join_request"], "webhook updates")
    names = [n for n, _ in integration.loops(NS(main=NS(bot=None)))]
    check(names == ["ap_scheduler", "ap_housekeeping", "ap_daily", "ap_archive"], names)
    app = NS(handlers=[], groups=[])
    app.add_handler = lambda h, group=0: app.handlers.append((group, h))
    integration.register_handlers(app)
    check(app.handlers[0][0] == -1 and len(app.handlers) == 7, "mode gate in group -1 + 6 more handlers")
    SETTINGS.enabled = False
    try:
        check(integration.allowed_updates() == ["message", "callback_query"] and integration.loops(None) == [], "AP_ENABLED=false changes nothing")
    finally:
        SETTINGS.enabled = True
    await handlers.set_admin_meta(42, mode="footer_text", mode_chat_id=-1)
    await integration.clear_mode(42)
    check("mode" not in (await handlers.admin_doc(42)), "/start clears an open prompt")


async def main():
    for name, fn in sorted((n, f) for n, f in globals().items() if n.startswith("t_") and asyncio.iscoroutinefunction(f)):
        try:
            await fn()
        except Exception:
            print("FAILED", name); traceback.print_exc(); sys.exit(1)
    print(f"OK {passed} checks")

asyncio.run(main())
