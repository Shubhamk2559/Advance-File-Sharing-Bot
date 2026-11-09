"""Environment + runtime (DB-stored, admin-editable) configuration. No I/O, no import of config.py."""
from __future__ import annotations

import copy
import os
import re

NETWORKS = ("monetag",)
NET_LABEL = {"monetag": "Monetag"}
CLIENT_ATTESTED, SERVER_POSTBACK = "client_attested", "server_postback"
TRUST_RANK = {CLIENT_ATTESTED: 1, SERVER_POSTBACK: 2}


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


SMART_ADS_ENABLED = _bool("SMART_ADS_ENABLED", True)   # production default: Monetag ad gate ON (set false for the old Monetag-only flow)
ADS_ADMIN_TOKEN = os.getenv("ADS_ADMIN_TOKEN", "").strip() or os.getenv("MONETAG_ADMIN_TOKEN", "").strip()
ADS_OPT_INTERVAL = max(60, _int("ADS_OPT_INTERVAL", 300))
ADS_UNSHOWN_LOCK_SECONDS = _int("ADS_UNSHOWN_LOCK_SECONDS", 25)    # attempt that never displayed frees the user slot
ADS_SHOWN_LOCK_SECONDS = _int("ADS_SHOWN_LOCK_SECONDS", 180)       # displayed ad (video + verification wait)
STATS_TZ_OFFSET_MIN = _int("STATS_TZ_OFFSET_MIN", 330)

SESSION_TTL = _int("SESSION_TTL", 1800)                            # must equal config.SESSION_TTL (the Mongo TTL index on sessions.created)
ATTEMPT_MAX_AGE = max(30, _int("AD_ATTEMPT_MAX_AGE", 600))         # an ad attempt can't be claimed/verified later than this after it was issued

# Verification speed: while waiting for Monetag's postback the server holds the request open this many seconds and answers the
# moment the postback arrives (instead of the app polling every ~4 s). 0 = off. Must stay below the client timeout (SAPI_TIMEOUT_MS).
VERIFY_LONGPOLL = max(0.0, min(15.0, float(os.getenv("VERIFY_LONGPOLL_SECONDS", "6") or 0)))

# ---- Ad gate: retries + ad-blocker check + "no ad available" grace ----
GATE_ROUNDS = max(1, min(5, _int("GATE_ROUNDS", 3)))                 # Monetag rounds before giving up
GATE_GRACE = _bool("GATE_GRACE", True)                               # no ad after all rounds AND no ad-blocker => deliver the file
GATE_GRACE_REASONS = frozenset(x.strip() for x in os.getenv("GATE_GRACE_REASONS", "no_fill,timeout").split(",") if x.strip())
GATE_MIN_TOTAL_SECONDS = _int("GATE_MIN_TOTAL_SECONDS", 8)          # grace only this long after the first ad attempt (no instant forged "no ad")
GATE_CHALLENGE_TTL = _int("GATE_CHALLENGE_TTL", 90)                  # seconds a solved ad-blocker challenge stays valid
GATE_GRACE_MAX_PER_HOUR = _int("GATE_GRACE_MAX_PER_HOUR", 20)        # per user, abuse cap
# Hosts that DNS ad-blockers (AdGuard DNS, NextDNS, Private DNS, Pi-hole) block almost without exception. Unreachable = blocker.
# Comma separated https URLs; empty = off. Set to empty if your users are in a region where Google hosts are unreachable.
GATE_EXTRA_PROBES = tuple(u.strip() for u in os.getenv(
    "GATE_EXTRA_PROBES", "https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js,https://googleads.g.doubleclick.net/pagead/id"
).split(",") if u.strip().startswith("https://"))

# Filled by routes.register() from the real config.py (kept out of here so tests need no env).
# "strict" defaults to True (fail closed). routes.register() sets it from the Policy; tests that exercise the development
# profile (client-attested rewards) say so explicitly.
RUNTIME = {"monetag_postback": False, "strict": True}

_ID_RE: dict = {}   # per-network unit-id validators (none needed for Monetag)

DEFAULT_CONFIG = {
    "mode": "priority",                 # priority = fixed network order (default) | auto = performance-based | manual = forced first pick
    "strategy": "thompson",             # auto mode: thompson = maximise expected earnings (bandit) | weighted = classic score blend
    "manual_network": None,
    "priority": list(NETWORKS),
    "fallback_enabled": True,
    "max_attempts": 3,                  # attempts per user request (each only if the previous never displayed an ad)
    "networks": {
        "monetag": {"enabled": True, "allow_client_reward": True, "unit_id": None, "ecpm_usd": None},     # legacy behaviour preserved
    },
    "optimization": {
        "window_days": 7, "recent_hours": 24, "min_requests": 100, "min_revenue_impressions": 100,
        "explore_share": 0.15, "cooldown_minutes": 30, "fail_threshold": 0.7, "fail_min_attempts": 15,
        "weights": {"revenue": 0.55, "fill": 0.15, "completion": 0.15, "errors": 0.05, "recent": 0.10},
        # --- thompson strategy ---
        "half_life_days": 3.0,          # evidence from N days ago counts half as much every half-life
        "prior_strength": 10.0,         # pseudo-requests pulling a young network toward the pooled conversion rate
        "min_share": 0.03,              # every eligible network keeps at least this share of first picks
        "probation_share": 0.10,        # cap for a network that just came back from a cool-down
        "use_monetag_estimate": False,  # let Monetag's postback estimate stand in for revenue when nothing else is known
    },
}

_OPT_RANGES = {
    "window_days": (1, 90, int), "recent_hours": (1, 48, int), "min_requests": (10, 100000, int),
    "min_revenue_impressions": (10, 1000000, int), "explore_share": (0.0, 0.5, float),
    "cooldown_minutes": (1, 1440, int), "fail_threshold": (0.3, 1.0, float), "fail_min_attempts": (5, 10000, int),
    "half_life_days": (0.5, 60, float), "prior_strength": (1, 500, float), "min_share": (0.0, 0.2, float),
    "probation_share": (0.01, 0.5, float),
}


def merged(stored: dict | None) -> dict:
    """Defaults <- stored overrides (deep for the known nested dicts)."""
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    s = stored or {}
    for k in ("mode", "strategy", "manual_network", "fallback_enabled", "max_attempts"):
        if k in s:
            cfg[k] = s[k]
    if cfg["manual_network"] not in NETWORKS:       # e.g. an old stored value naming a network that no longer exists
        cfg["manual_network"] = None
    if isinstance(s.get("priority"), list) and sorted(s["priority"]) == sorted(NETWORKS):
        cfg["priority"] = list(s["priority"])
    for n in NETWORKS:
        cfg["networks"][n].update({k: v for k, v in (s.get("networks", {}).get(n) or {}).items() if k in cfg["networks"][n]})
        if RUNTIME["strict"]:
            # strict mode never accepts client-only rewards
            cfg["networks"][n]["allow_client_reward"] = False
    opt = s.get("optimization") or {}
    cfg["optimization"].update({k: v for k, v in opt.items() if k in cfg["optimization"] and k != "weights"})
    if isinstance(opt.get("weights"), dict):
        cfg["optimization"]["weights"].update({k: v for k, v in opt["weights"].items() if k in cfg["optimization"]["weights"]})
    return cfg


def unit_id(cfg: dict, net: str) -> str | None:
    """Public identifier of the ad unit. Not a secret."""
    if net == "monetag":
        return "zone"
    return cfg["networks"][net].get("unit_id") or None


def is_configured(cfg: dict, net: str) -> bool:
    return bool(unit_id(cfg, net))


def trust_for(net: str) -> str:
    if net == "monetag":
        return SERVER_POSTBACK if RUNTIME["monetag_postback"] else CLIENT_ATTESTED
    return CLIENT_ATTESTED


def trust_allowed(cfg: dict, net: str) -> bool:
    if trust_for(net) != CLIENT_ATTESTED:
        return True
    return (not RUNTIME["strict"]) and bool(cfg["networks"][net]["allow_client_reward"])


def servable_networks(cfg: dict) -> list:
    """Networks that can actually receive traffic: enabled + configured + reward verification allowed (no cool-down logic)."""
    return [n for n in NETWORKS
            if cfg["networks"][n]["enabled"] and is_configured(cfg, n) and trust_allowed(cfg, n)]


def validate_patch(patch: dict, current: dict) -> tuple[dict, list]:
    """Returns (new full config, errors). Pure; nothing is applied if errors is non-empty."""
    errors: list = []
    new = copy.deepcopy(current)
    if not isinstance(patch, dict):
        return current, ["body must be a JSON object"]
    allowed = {"mode", "strategy", "manual_network", "priority", "fallback_enabled", "max_attempts", "networks", "optimization"}
    for k in patch:
        if k not in allowed:
            errors.append(f"unknown key: {k}")
    if "mode" in patch:
        if patch["mode"] in ("auto", "priority", "manual"):
            new["mode"] = patch["mode"]
        else:
            errors.append("mode must be auto|priority|manual")
    if "strategy" in patch:
        if patch["strategy"] in ("thompson", "weighted"):
            new["strategy"] = patch["strategy"]
        else:
            errors.append("strategy must be thompson|weighted")
    if "manual_network" in patch:
        if patch["manual_network"] is None or patch["manual_network"] in NETWORKS:
            new["manual_network"] = patch["manual_network"]
        else:
            errors.append("manual_network must be null or a known network")
    if "priority" in patch:
        p = patch["priority"]
        if isinstance(p, list) and sorted(p) == sorted(NETWORKS):
            new["priority"] = list(p)
        else:
            errors.append("priority must list each network exactly once")
    if "fallback_enabled" in patch:
        if isinstance(patch["fallback_enabled"], bool):
            new["fallback_enabled"] = patch["fallback_enabled"]
        else:
            errors.append("fallback_enabled must be boolean")
    if "max_attempts" in patch:
        v = patch["max_attempts"]
        if isinstance(v, int) and not isinstance(v, bool) and 1 <= v <= 3:
            new["max_attempts"] = v
        else:
            errors.append("max_attempts must be 1..3")
    for n, nv in (patch.get("networks") or {}).items():
        if n not in NETWORKS or not isinstance(nv, dict):
            errors.append(f"unknown network: {n}")
            continue
        for k, v in nv.items():
            if k == "allow_client_reward" and v is True and RUNTIME["strict"]:
                errors.append(f"networks.{n}.allow_client_reward: not allowed in strict mode (browser-only rewards can't be verified)")
            elif k in ("enabled", "allow_client_reward") and isinstance(v, bool):
                new["networks"][n][k] = v
            elif k == "ecpm_usd" and (v is None or v == "" or (isinstance(v, (int, float)) and not isinstance(v, bool) and 0 <= v <= 1000)):
                new["networks"][n][k] = None if v in (None, "") else float(v)
            elif k == "unit_id" and n in _ID_RE and (v is None or (isinstance(v, str) and (v == "" or _ID_RE[n].match(v)))):
                new["networks"][n][k] = v or None
            else:
                errors.append(f"networks.{n}.{k}: invalid")
    opt = patch.get("optimization")
    if opt is not None:
        if not isinstance(opt, dict):
            errors.append("optimization must be an object")
        else:
            for k, v in opt.items():
                if k == "weights":
                    w = v if isinstance(v, dict) else {}
                    if set(w) - set(DEFAULT_CONFIG["optimization"]["weights"]) or not w:
                        errors.append("weights: unknown/empty")
                        continue
                    if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not 0 <= x <= 1 for x in w.values()):
                        errors.append("weights must be numbers in 0..1")
                        continue
                    merged_w = dict(new["optimization"]["weights"], **{a: float(b) for a, b in w.items()})
                    if sum(merged_w.values()) <= 0:
                        errors.append("weights must not all be zero")
                        continue
                    new["optimization"]["weights"] = merged_w
                elif k == "use_monetag_estimate":
                    if isinstance(v, bool):
                        new["optimization"][k] = v
                    else:
                        errors.append("optimization.use_monetag_estimate must be boolean")
                elif k in _OPT_RANGES:
                    lo, hi, typ = _OPT_RANGES[k]
                    if isinstance(v, bool) or not isinstance(v, (int, float)) or not lo <= v <= hi:
                        errors.append(f"optimization.{k} must be {lo}..{hi}")
                    else:
                        new["optimization"][k] = typ(v)
                else:
                    errors.append(f"optimization.{k}: unknown")
    # cross-field checks on the resulting config
    if new["mode"] == "manual" and not new["manual_network"]:
        errors.append("manual mode needs manual_network")
    # Never let the admin switch off the LAST network that can unlock files: every user would then get
    # "No ad provider is available". (Fixing an already-broken config is still allowed.)
    if servable_networks(current) and not servable_networks(new):
        errors.append("cannot switch off the last usable ad network: no other network would be left to serve ads.")
    return (current if errors else new), errors
