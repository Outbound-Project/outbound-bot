from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import time
from typing import Dict, Iterable

from googleapiclient.errors import HttpError


def load_state(state_path: str) -> Dict:
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "processed_zip_ids": [],
            "last_processed_zip_time": None,
            "page_token": None,
            "channel_id": None,
            "channel_resource_id": None,
            "channel_expiration": None,
        }


def save_state(state_path: str, state: Dict) -> None:
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[{ts}] {msg}")


def _should_retry_http_error(exc: Exception) -> bool:
    if isinstance(exc, HttpError):
        try:
            return 500 <= int(exc.resp.status) < 600
        except Exception:
            return True
    return False


def with_retries(func, label: str, attempts: int = 5):
    delay = 1.0
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:
            if attempt >= attempts or not _should_retry_http_error(exc):
                raise
            log(f"{label} failed (attempt {attempt}/{attempts}); retrying in {delay:.1f}s: {exc}")
            time.sleep(delay)
            delay *= 2


def parse_rfc3339(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def format_status_timestamp(dt: datetime) -> str:
    hour = dt.strftime("%I").lstrip("0") or "12"
    minute = dt.strftime("%M")
    ampm = dt.strftime("%p")
    mon = dt.strftime("%b")
    day = dt.strftime("%d").lstrip("0")
    return f"{hour}:{minute} {ampm} {mon}-{day}"


@dataclass
class DedupeCache:
    ttl_seconds: int = 600
    max_size: int = 2048

    def __post_init__(self) -> None:
        self._entries: Dict[str, float] = {}

    def seen(self, key: str) -> bool:
        now = time.time()
        self._prune(now)
        ts = self._entries.get(key)
        if ts is not None and (now - ts) <= self.ttl_seconds:
            return True
        self._entries[key] = now
        if len(self._entries) > self.max_size:
            self._prune(now, aggressive=True)
        return False

    def _prune(self, now: float, aggressive: bool = False) -> None:
        if not self._entries:
            return
        cutoff = now - self.ttl_seconds
        if aggressive:
            cutoff = now - (self.ttl_seconds / 2)
        for key, ts in list(self._entries.items()):
            if ts < cutoff:
                self._entries.pop(key, None)


def build_dedupe_key(parts: Iterable[str]) -> str:
    clean = [p for p in parts if p]
    return ":".join(clean)
