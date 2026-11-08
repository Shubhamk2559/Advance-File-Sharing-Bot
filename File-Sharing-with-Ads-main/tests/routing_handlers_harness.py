"""Runs the REAL handlers.py with telegram / db / services stubbed and a STUB router; prints one JSON report (subprocess, like
handlers_harness.py). Covers: MOVE reply, serve, router exception, router timeout, no router, activity touches. Not a test module itself."""
import asyncio
import json
import os
import sys
import types

ROOT = sys.argv[1]
sys.path.insert(0, ROOT)
sys.path.insert(0, ROOT + "/tests")
for k, v in {"MAIN_BOT_TOKEN": "1:T", "OWNER_ID": "1", "MONGO_URI": "mongodb://x", "WEBHOOK_SECRET": "test-webhook-secret-0123456789", "BASE_URL": "https://x.example",
             "PRIMARY_CHANNEL": "-1001", "BACKUP_CHANNEL": "-1002", "MONETAG_POSTBACK_SECRET": "unit-test-secret-0123456789"}.items():
    os.environ.setdefault(k, v)


def mod(name, **attrs):
    m = types.ModuleType(name)
    m.__dict__.update(attrs)
    sys.modules[name] = m
    return m


class Magic:
    """Records constructor arguments so the test can read the button URL."""
    def __init__(self, *a, **k): self.a, self.k = a, k
    def __and__(self, o): return Magic()
    __or__ = __rand__ = __ror__ = __and__
    def __invert__(self): return Magic()
    def __getattr__(self, n): return Magic()
    def __call__(self, *a, **k): return Magic()


class Err(Exception): pass


mod("telegram", Update=Magic, CopyTextButton=Magic, InlineKeyboardButton=Magic, InlineKeyboardMarkup=Magic, WebAppInfo=Magic)
mod("telegram.constants", ParseMode=types.SimpleNamespace(HTML="HTML"))
mod("telegram.error", BadRequest=type("BadRequest", (Err,), {}), TelegramError=Err)
mod("telegram.ext", Application=Magic, CallbackQueryHandler=Magic, CommandHandler=Magic, ContextTypes=types.SimpleNamespace(DEFAULT_TYPE=None),
    MessageHandler=Magic, filters=Magic())

from tests.fakemongo import FakeDB  # noqa: E402

fdb = FakeDB()
created = []


async def create_session(**kw): created.append(kw)
async def get_file(token): return {"token": token}

mod("db", get_file=get_file, create_session=create_session, _db=fdb)
mod("monetag_store")
mod("delivery", send_file=None)
mod("services")
mod("services.stats"); mod("services.files", delete_file_everywhere=None)
mod("services.posts", post_button=None, resolve_link=None, send_thumb_post=None, store_to_channels=None, store_videos=None)
mod("ads.panel", register=lambda *a, **k: None)

from ads import store  # noqa: E402

store.set_database(fdb)
import handlers  # noqa: E402

replies = []


class Msg:
    async def reply_text(self, text, **k):
        replies.append({"text": text, "markup": k.get("reply_markup")})
        return Msg()
    async def edit_text(self, *a, **k): pass
    async def set_reaction(self, *a, **k): pass
    message_id = 5


class Chat: type, id = "private", 10


class StubRouter:
    def __init__(self, mode):
        self.mode, self.arrivals, self.touches = mode, [], []

    async def on_arrival(self, uid):
        self.arrivals.append(uid)
        if self.mode == "move":
            return types.SimpleNamespace(action="move", username="otherbot", bot_id="2", reason="rebalance")
        if self.mode == "serve":
            return types.SimpleNamespace(action="serve", username="bot", bot_id="1", reason="new_here")
        if self.mode == "boom":
            raise RuntimeError("mongo exploded")
        if self.mode == "hang":
            await asyncio.sleep(30)

    async def touch(self, uid): self.touches.append(uid)


def ctx(router, args=("tok12345",)):
    c = types.SimpleNamespace(args=list(args), bot=types.SimpleNamespace(username="bot"))
    c.application = types.SimpleNamespace(bot_data={"manager": types.SimpleNamespace(router=router) if router is not False else types.SimpleNamespace(),
                                                    "bot_id": 1})
    return c


def upd(uid):
    return types.SimpleNamespace(effective_message=Msg(), effective_chat=Chat(), effective_user=types.SimpleNamespace(id=uid, first_name="n"))


class Q:
    def __init__(self, uid): self.from_user, self.data, self.message, self.answers = types.SimpleNamespace(id=uid, first_name="n"), "help", Msg(), []
    async def answer(self, text=None, **k): self.answers.append(text)
    async def edit_message_text(self, *a, **k): pass


out = {}


def url_of(markup):
    """InlineKeyboardMarkup([[InlineKeyboardButton(text, url=...)]]) as recorded by Magic."""
    try:
        return markup.a[0][0][0].k.get("url")
    except Exception:  # noqa: BLE001
        return None


async def scenario():
    handlers._ROUTER_TIMEOUT = 0.05

    r = StubRouter("move"); replies.clear(); n0 = len(created)
    await handlers.deliver_start(upd(1001), ctx(r))
    out["move"] = {"sessions": len(created) - n0, "arrivals": r.arrivals, "text": [x["text"] for x in replies],
                   "url": url_of(replies[-1]["markup"]) if replies else None}

    r = StubRouter("serve"); replies.clear(); n0 = len(created)
    await handlers.deliver_start(upd(1002), ctx(r))
    out["serve"] = {"sessions": len(created) - n0, "arrivals": r.arrivals}

    for mode in ("boom", "hang"):
        r = StubRouter(mode); n0 = len(created)
        await handlers.deliver_start(upd(1003), ctx(r))
        out[mode] = {"sessions": len(created) - n0}

    n0 = len(created)
    await handlers.deliver_start(upd(1004), ctx(None))
    out["no_router"] = {"sessions": len(created) - n0}
    n0 = len(created)
    await handlers.deliver_start(upd(1005), ctx(False))                       # manager object without a `router` attribute at all
    out["no_attr"] = {"sessions": len(created) - n0}

    # plain /start (no token) must not touch routing
    r = StubRouter("move")
    await handlers.deliver_start(upd(1006), ctx(r, args=()))
    out["plain_start_arrivals"] = r.arrivals

    # activity: a menu tap and a stray text message slide the sticky window (fire-and-forget)
    r = StubRouter("serve")
    await handlers.on_callback(types.SimpleNamespace(callback_query=Q(1007)), ctx(r, args=()))
    await handlers.fallback_text(upd(1008), ctx(r, args=()))
    await asyncio.sleep(0.05)
    out["touches"] = sorted(r.touches)


asyncio.run(scenario())
print(json.dumps(out, default=str))
