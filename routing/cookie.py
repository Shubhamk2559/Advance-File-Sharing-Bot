"""Signed stickiness hint for the browser (`/f/<token>` gateway). It carries NO user id: only "which bot did we last send this browser to, and
when". HMAC-signed so a client cannot steer traffic to an arbitrary bot id or fake a young timestamp."""
from __future__ import annotations

import hashlib
import hmac
from typing import Optional


def _sig(secret: bytes, payload: str) -> str:
    return hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()[:20]


def make(secret: bytes, bot_id: str, now: float) -> str:
    payload = f"{bot_id}.{int(now)}"
    return f"{payload}.{_sig(secret, payload)}"


def parse(secret: bytes, value: Optional[str], now: float) -> tuple[Optional[str], Optional[float]]:
    """-> (bot_id, age_seconds), or (None, None) for anything missing, malformed, forged or from the future."""
    if not value or len(value) > 120:
        return None, None
    try:
        bot_id, ts, sig = value.rsplit(".", 2)
        ts_i = int(ts)
    except ValueError:
        return None, None
    if not bot_id.isdigit() or not hmac.compare_digest(sig, _sig(secret, f"{bot_id}.{ts_i}")):
        return None, None
    age = now - ts_i
    return (bot_id, age) if age >= -5 else (None, None)
