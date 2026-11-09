"""Auto-poster settings. Every variable is AP_-prefixed so it can never clash with the file-sharing bot's own settings.

Invalid values raise RuntimeError with a readable message (names only): the bot's startup reports it and stops, like every other setting.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config import OWNER_ID

logger = logging.getLogger("autoposter")


def _clean(value: str | None) -> str:
    return (value or "").strip().strip("\"'").strip()


def _env(name: str, default: str = "") -> str:
    return os.getenv(f"AP_{name}", default)


def _env_bool(name: str, default: bool) -> bool:
    raw = _clean(_env(name))
    return default if not raw else raw.lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = _env(name).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"AP_{name} must be an integer (got {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise RuntimeError(f"AP_{name} must be >= {minimum} (got {value})")
    return value


def _env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = _env(name).strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"AP_{name} must be a number (got {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise RuntimeError(f"AP_{name} must be >= {minimum} (got {value})")
    return value


def _env_hhmm(name: str, default: str) -> str:
    raw = _env(name, default).strip() or default
    try:
        hour, minute = raw.split(":", 1)
        time(hour=int(hour), minute=int(minute))
    except ValueError as exc:
        raise RuntimeError(f"AP_{name} must look like HH:MM (got {raw!r})") from exc
    return raw


@dataclass(slots=True)
class Settings:
    enabled: bool
    admin_ids: set[int]
    timezone: str
    scan_interval_seconds: int
    default_interval_minutes: int
    default_batch_size: int
    default_start_time: str
    default_end_time: str
    default_days: list[int]
    scheduler_lock_seconds: int
    retry_attempts: int
    retry_base_seconds: float
    post_gap_seconds: float
    log_retention_days: int
    reactions: bool
    import_limit: int
    import_pace_seconds: float
    import_scratch_chat: int | str | None
    use_source_caption: bool
    vault_chat_id: int | None = None
    admin_password: str | None = None
    password_timeout_seconds: int = 300

    @classmethod
    def from_env(cls) -> "Settings":
        timezone = _env("TIMEZONE", "Asia/Kolkata").strip() or "Asia/Kolkata"
        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
            raise RuntimeError(f"AP_TIMEZONE {timezone!r} is not a valid IANA timezone (example: Asia/Kolkata)") from exc

        days_raw = _env("DEFAULT_DAYS", "0,1,2,3,4,5,6")
        try:
            days = sorted({int(x) for x in days_raw.split(",") if x.strip()})
        except ValueError as exc:
            raise RuntimeError(f"AP_DEFAULT_DAYS must be comma separated 0-6 (got {days_raw!r})") from exc
        if not days or any(d not in range(7) for d in days):
            raise RuntimeError(f"AP_DEFAULT_DAYS must be comma separated 0-6 (got {days_raw!r})")

        scratch_raw = _clean(_env("IMPORT_SCRATCH_CHAT"))
        scratch: int | str | None = None
        if scratch_raw:
            scratch = int(scratch_raw) if scratch_raw.lstrip("-").isdigit() else scratch_raw
        return cls(
            enabled=_env_bool("ENABLED", True),
            admin_ids={OWNER_ID},
            timezone=timezone,
            scan_interval_seconds=_env_int("SCAN_INTERVAL_SECONDS", 30, 5),
            default_interval_minutes=_env_int("DEFAULT_INTERVAL_MINUTES", 180, 1),
            default_batch_size=_env_int("DEFAULT_BATCH_SIZE", 5, 1),
            default_start_time=_env_hhmm("DEFAULT_START_TIME", "00:00"),
            default_end_time=_env_hhmm("DEFAULT_END_TIME", "21:00"),
            default_days=days,
            scheduler_lock_seconds=_env_int("SCHEDULER_LOCK_SECONDS", 300, 30),
            retry_attempts=_env_int("RETRY_ATTEMPTS", 3, 1),
            retry_base_seconds=_env_float("RETRY_BASE_SECONDS", 2.0, 0.0),
            post_gap_seconds=_env_float("POST_GAP_SECONDS", 1.0, 0.0),
            log_retention_days=_env_int("LOG_RETENTION_DAYS", 14, 1),
            reactions=_env_bool("REACTIONS", True),
            import_limit=_env_int("IMPORT_LIMIT", 2000, 1),
            import_pace_seconds=_env_float("IMPORT_PACE_SECONDS", 0.5, 0.0),
            import_scratch_chat=scratch,
            use_source_caption=_env_bool("POST_USE_SOURCE_CAPTION", False),
            vault_chat_id=_env_int("VAULT_CHAT_ID", 0) or None,
            admin_password=(_clean(_env("ADMIN_PASSWORD")) or None),
            password_timeout_seconds=_env_int("PASSWORD_TIMEOUT_SECONDS", 300, 30),
        )


SETTINGS = Settings.from_env()
TZ = ZoneInfo(SETTINGS.timezone)
