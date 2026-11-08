"""Runs the REAL main.py (FastAPI / Telegram / Mongo stubbed) with a REAL routing.Router over FakeDB and checks the `/f/<token>` gateway:
cookie attributes, stickiness, forged cookie, router off. Prints one JSON report (subprocess). Not a test module itself."""
import asyncio
import hashlib
import json
import os
import random
import sys
import types

ROOT = sys.argv[1]
sys.path.insert(0, ROOT)
sys.path.insert(0, ROOT + "/tests")
for k, v in {"MAIN_BOT_TOKEN": "123456:TEST-TOKEN", "OWNER_ID": "1", "MONGO_URI": "mongodb://unused", "WEBHOOK_SECRET": "test-webhook-secret-0123456789",
             "BASE_URL": "https://example.test", "PRIMARY_CHANNEL": "-1001", "BACKUP_CHANNEL": "-1002",
             "MONETAG_POSTBACK_SECRET": "s3cr3t-Postback_0123456789abcdef", "STICKY_TTL": "900", "BOT_CAPACITY": "100"}.items():
    os.environ[k] = v


def mod(name, **attrs):
    m = types.ModuleType(name)
    m.__dict__.update(attrs)
    sys.modules[name] = m
    return m


class Resp:
    def __init__(self, content="", status_code=200, headers=None, media_type=None, **_):
        self.body, self.status_code, self.headers, self.cookies = content, status_code, headers or {}, []

    def set_cookie(self, key, value, **kw): self.cookies.append({"key": key, "value": value, **kw})


class HTTPException(Exception):
    def __init__(self, status_code=500, detail=None): self.status_code = status_code


class FastAPI:
    def __init__(self, **_): self.routes = {}
    def _reg(self, method):
        def deco(path, **kw):
            def wrap(fn): self.routes[(method, path)] = fn; return fn
            return wrap
        return deco
    get = property(lambda s: s._reg("GET")); post = property(lambda s: s._reg("POST"))
    def api_route(self, path, methods=("GET",), **kw): return self._reg(methods[0])(path)
    def exception_handler(self, exc): return lambda fn: fn
    def add_middleware(self, cls, **kw): pass


mod("pymongo")
mod("pymongo.errors", PyMongoError=type("PyMongoError", (Exception,), {}))
mod("fastapi", FastAPI=FastAPI, Header=lambda default=None: default, HTTPException=HTTPException, Request=type("Request", (), {}))
mod("fastapi.responses", HTMLResponse=Resp, PlainTextResponse=Resp, JSONResponse=Resp, Response=Resp)
mod("fastapi.exceptions", RequestValidationError=type("RVE", (Exception,), {}))
mod("fastapi.exception_handlers", request_validation_exception_handler=None)
mod("telegram", Update=type("Update", (), {}))
mod("telegram.error", Forbidden=type("Forbidden", (Exception,), {}), TelegramError=type("TelegramError", (Exception,), {}))

from tests.fakemongo import FakeDB, FakeSessions  # noqa: E402

fdb = FakeDB()
sess = FakeSessions(fdb)

import observability as _obs  # noqa: E402
from routing.router import Router  # noqa: E402
from config import ROUTER_CFG, WEBHOOK_SECRET  # noqa: E402


class Clock:
    t = 50_000.0
    def __call__(self): return self.t


clock = Clock()
SECRET = hashlib.sha256(f"{WEBHOOK_SECRET}:route".encode()).digest()


def make_router(i): return Router(lambda n: fdb[n], ROUTER_CFG, bot_id=str(i), username=f"bot{i}", instance=f"svc{i}", secret=SECRET, clock=clock,
                                  rng=random.Random(i))


r1, r2 = make_router(1), make_router(2)


class Manager:
    main = types.SimpleNamespace(bot=types.SimpleNamespace(token="123456:TEST-TOKEN", username="bot1"), bot_data={"bot_id": 1},
                                 update_queue=types.SimpleNamespace(put=None))
    secret, alerts = "s", _obs.ALERTS
    db_dep, webhook_dep, webhook_info = _obs.Dependency("mongodb", 3), _obs.Dependency("telegram_webhook", 3), {}
    tasks = types.SimpleNamespace(snapshot=lambda: {})
    router = r1
    async def start(self): pass
    async def stop(self): pass
    async def active_usernames(self): return ["legacybot"]


mod("manager", BotManager=Manager)
mod("delivery", send_file=None)
mod("db", get_session=sess.get_session, get_file=sess.get_file, create_session=sess.create_session, start_ad=sess.start_ad,
    complete_ad=sess.complete_ad, claim_delivery=sess.claim_delivery, unclaim_delivery=sess.unclaim_delivery, grant_grace=sess.grant_grace, _db=fdb)


async def _ping(): pass


sys.modules["db"].ping = _ping

import main  # noqa: E402
import monetag_store  # noqa: E402

monetag_store._DB = fdb
link = main.app.routes[("GET", "/f/{token}")]


class Req:
    def __init__(self, cookie=None, ip="9.9.9.9"):
        self.headers, self.client = {"x-forwarded-for": ip}, types.SimpleNamespace(host=ip)
        self.cookies = {ROUTER_CFG.cookie_name: cookie} if cookie else {}
        self.query_params, self.url, self.method = {}, types.SimpleNamespace(path="/f/x"), "GET"


def target_bot(resp):
    for b in ("bot1", "bot2", "legacybot"):
        if f"https://t.me/{b}?start=" in resp.body:
            return b
    return None


out = {}


async def scenario():
    await r1.heartbeat(); await r2.heartbeat(); await r1.refresh(); await r2.refresh()

    resp = await link("abcd1234", Req())
    c = resp.cookies[0]
    out["first"] = {"status": resp.status_code, "bot": target_bot(resp), "cookie": {k: v for k, v in c.items() if k != "value"},
                    "cookie_has_no_user_data": c["value"].count(".") == 2 and c["value"].split(".")[0].isdigit()}

    # the same browser keeps its bot for every following click, whichever bot is lighter meanwhile
    first_bot, value = target_bot(resp), c["value"]
    seen = set()
    for i in range(30):
        clock.t += 20
        await r1.refresh()
        resp = await link("abcd1234", Req(value, ip=f"9.9.8.{i}"))
        seen.add(target_bot(resp)); value = resp.cookies[0]["value"]
    out["sticky_bots"] = sorted(seen)
    out["sticky_expected"] = first_bot

    # forged / garbage cookies are ignored (treated as a new browser), never an error
    forged = []
    for bad in ("999999.1.deadbeef", "x", "a" * 500):
        resp = await link("abcd1234", Req(bad, ip="9.9.7.1"))
        forged.append([resp.status_code, target_bot(resp) in ("bot1", "bot2")])
    out["forged"] = forged

    # a token that is not a valid token is refused before routing
    resp = await link("bad token!", Req(ip="9.9.7.2"))
    out["bad_token"] = resp.status_code

    # routing off => the old behaviour (active_usernames), no cookie
    main.manager.router = None
    resp = await link("abcd1234", Req(ip="9.9.7.3"))
    out["router_off"] = {"bot": target_bot(resp), "cookies": len(resp.cookies)}


asyncio.run(scenario())
print(json.dumps(out))
