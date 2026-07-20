"""
Costco AU proxy pool.

Static residential proxies (a fixed list of host:port endpoints, each with its
own AU IP). Each Celery worker picks **one** proxy and reuses it across many
requests so cookies and session state stay attached to a single IP — the same
pattern as the old desktop ``01/`` .. ``10/`` shards.

Configuration (precedence, highest first):

    COSTCO_AU_PROXY_URLS  ``http://user:pass@host:port,...``  — full URLs
    PROXY_URLS            same shape — generic fallback
    COSTCO_AU_PROXY_URL   single URL
    PROXY_URL             single URL
    PROXY_ENDPOINTS       ``host:port,...`` combined with PROXY_USER/PASS/SCHEME

The pool is read once per process. ``acquire()`` returns a sticky proxy for the
current process, ``rotate()`` advances to the next one when a proxy gets
blocked. Per-process rate limiting (``COSTCO_AU_MIN_REQUEST_GAP_SEC``) is
enforced via ``ProxyAssignment.wait_for_gap()``.

This module is deliberately dependency-free (only ``os`` / ``time`` / ``random``
/ ``threading``) so it can be imported and tested without touching network or
Django.
"""
from __future__ import annotations

import logging
import os
import random
import re
import threading
import time
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import quote, urlparse, urlunparse

logger = logging.getLogger("scrapers.costco_au.proxies")


# ---------------------------------------------------------------------------
# Env parsing
# ---------------------------------------------------------------------------

def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip()


def _split(raw: str) -> list[str]:
    """Split a comma/newline separated string and return non-empty entries."""
    if not raw:
        return []
    parts = re.split(r"[\s,]+", raw)
    return [p.strip() for p in parts if p and p.strip()]


def _normalize_proxy(raw: str, default_scheme: str = "http",
                     default_user: str = "", default_pass: str = "") -> str | None:
    """Normalize a single proxy URL or ``host:port`` into a valid URL.

    Returns ``None`` for inputs that cannot be parsed. Auth credentials in the
    URL are URL-encoded so colon/at-sign in passwords don't break ``urlparse``.
    """
    if not raw or not isinstance(raw, str):
        return None
    raw = raw.strip()
    if not raw:
        return None

    # If it already has a scheme, parse and re-emit (encoding credentials).
    if "://" in raw:
        try:
            parts = urlparse(raw)
        except Exception:
            return None
        host = parts.hostname
        if not host:
            return None
        port = parts.port
        user = parts.username
        pwd = parts.password
        scheme = (parts.scheme or default_scheme).lower()
        netloc = host if not port else f"{host}:{port}"
        if user:
            cred = quote(user, safe="")
            if pwd is not None:
                cred = f"{cred}:{quote(pwd, safe='')}"
            netloc = f"{cred}@{netloc}"
        return urlunparse((scheme, netloc, "", "", "", ""))

    # Bare host:port (no scheme).
    if ":" not in raw:
        return None
    host, _, port = raw.partition(":")
    host = host.strip()
    port = port.strip()
    if not host or not port.isdigit():
        return None
    netloc = f"{host}:{port}"
    if default_user:
        cred = quote(default_user, safe="")
        if default_pass:
            cred = f"{cred}:{quote(default_pass, safe='')}"
        netloc = f"{cred}@{netloc}"
    scheme = (default_scheme or "http").lower()
    return f"{scheme}://{netloc}"


def load_proxy_urls(env: Optional[dict] = None) -> list[str]:
    """Read proxy URLs from environment in precedence order. Pure-function."""
    env = env if env is not None else os.environ

    def get(name: str, default: str = "") -> str:
        v = env.get(name)
        if v is None:
            return default
        return str(v).strip()

    default_user = get("PROXY_USER")
    default_pass = get("PROXY_PASS")
    default_scheme = (get("PROXY_SCHEME") or "http").lower()

    candidates: list[str] = []
    for var in ("COSTCO_AU_PROXY_URLS", "PROXY_URLS"):
        raw = get(var)
        if raw:
            candidates.extend(_split(raw))
            if candidates:
                break

    if not candidates:
        for var in ("COSTCO_AU_PROXY_URL", "PROXY_URL"):
            raw = get(var)
            if raw:
                candidates.append(raw)
                break

    if not candidates:
        endpoints = _split(get("PROXY_ENDPOINTS"))
        if endpoints:
            candidates.extend(endpoints)

    out: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        norm = _normalize_proxy(raw, default_scheme, default_user, default_pass)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------

@dataclass
class ProxyAssignment:
    """A proxy currently held by one worker session.

    ``key`` is a stable opaque token (the normalized URL) so callers don't pass
    raw credentials around in logs. ``url`` is the full proxy URL ready to be
    given to ``requests`` / ``curl_cffi``. ``index`` is the position in the
    pool (0-based).
    """
    index: int
    url: str
    label: str  # host[:port] only, no credentials — safe to log

    def as_requests_proxy(self) -> dict:
        """Return a dict suitable for ``requests`` / ``curl_cffi`` proxies kwarg."""
        return {"http": self.url, "https": self.url}


class ProxyBlockedError(Exception):
    """Raised by callers when the active proxy is consistently blocked."""


class CostcoAuProxyPool:
    """Manages a small list of static residential proxies, thread-safely.

    Each call to :py:meth:`acquire` returns either an existing sticky proxy
    pinned to the current thread (so a Celery prefork child reuses one IP
    across hundreds of URLs) or, if the previous proxy was banned, the next
    one in round-robin order. Rate limiting is per-proxy (so two threads each
    pinned to a different proxy can fetch concurrently without throttling
    each other).
    """

    def __init__(self, urls: Iterable[str], *, min_gap_sec: float = 0.0) -> None:
        self._urls = [u for u in urls if u]
        self._min_gap_sec = max(0.0, float(min_gap_sec or 0.0))
        self._lock = threading.Lock()
        # Random starting cursor per process so prefork children don't all land on
        # index 0 — in production we saw 4 workers all sticky on proxy 0 while
        # proxies 1-9 sat idle. Each fork inherits the parent _POOL but creates a
        # new sticky assignment on first acquire(); randomising _cursor spreads
        # those initial picks across the pool.
        self._cursor = random.randint(0, max(0, len(self._urls) - 1)) if self._urls else 0
        # Per-thread sticky assignment (process can have multiple Celery worker
        # threads with --pool=threads; prefork has 1 thread per child).
        self._sticky: dict[int, int] = {}
        # Track temporarily-blocked proxies and the time they cool down.
        self._cooldown_until: dict[int, float] = {}
        # Per-proxy "last request started" timestamp (monotonic).
        self._last_req_at: dict[int, float] = {}

    @property
    def size(self) -> int:
        return len(self._urls)

    @property
    def urls(self) -> tuple[str, ...]:
        return tuple(self._urls)

    def _label(self, url: str) -> str:
        try:
            p = urlparse(url)
            host = p.hostname or ""
            port = p.port
            return f"{host}:{port}" if port else host
        except Exception:
            return "unknown"

    def _next_free_index(self, now: float, *, exclude: Optional[int] = None) -> Optional[int]:
        n = len(self._urls)
        if n == 0:
            return None
        start = self._cursor % n
        for i in range(n):
            idx = (start + i) % n
            if exclude is not None and idx == exclude:
                continue
            cd = self._cooldown_until.get(idx, 0.0)
            if cd <= now:
                return idx
        # All proxies in cooldown — return the one whose cooldown ends first.
        candidates = [(idx, self._cooldown_until.get(idx, 0.0)) for idx in range(n)
                      if exclude is None or idx != exclude]
        if not candidates:
            return None
        return min(candidates, key=lambda kv: kv[1])[0]

    def acquire(self, *, force_rotate: bool = False) -> Optional[ProxyAssignment]:
        """Return a :class:`ProxyAssignment` for the current thread.

        ``force_rotate=True`` releases any sticky proxy held by this thread
        and picks the next one — used after the active proxy gets blocked.
        """
        if not self._urls:
            return None
        tid = threading.get_ident()
        now = time.monotonic()
        with self._lock:
            if force_rotate:
                self._sticky.pop(tid, None)
            current = self._sticky.get(tid)
            if current is not None and current < len(self._urls):
                cd = self._cooldown_until.get(current, 0.0)
                if cd <= now:
                    url = self._urls[current]
                    return ProxyAssignment(index=current, url=url, label=self._label(url))
                # In cooldown — fall through to picking next.
            idx = self._next_free_index(now, exclude=current if force_rotate else None)
            if idx is None:
                return None
            self._sticky[tid] = idx
            self._cursor = idx + 1
            url = self._urls[idx]
            return ProxyAssignment(index=idx, url=url, label=self._label(url))

    def mark_blocked(self, assignment: ProxyAssignment, cooldown_sec: float = 300.0) -> None:
        """Mark ``assignment`` as banned for ``cooldown_sec`` seconds.

        Logs at ``warning`` only for long cooldowns (real Cloudflare-style bans).
        Short cooldowns (used for transient HTTP / parse errors) log at ``debug``
        to avoid flooding the worker log during catalog runs.
        """
        if assignment is None:
            return
        now = time.monotonic()
        with self._lock:
            self._cooldown_until[assignment.index] = now + max(0.0, float(cooldown_sec))
        log_fn = logger.warning if cooldown_sec >= 300.0 else logger.debug
        log_fn(
            "Costco AU proxy %d (%s) cooled down for %.0fs after block",
            assignment.index, assignment.label, cooldown_sec,
        )

    def mark_success(self, assignment: ProxyAssignment) -> None:
        if assignment is None:
            return
        with self._lock:
            self._cooldown_until.pop(assignment.index, None)

    def wait_for_gap(self, assignment: ProxyAssignment, *, jitter_pct: float = 0.20) -> None:
        """Sleep until ``min_gap_sec`` has passed since this proxy's last request.

        Adds a small jitter so two workers don't synchronize their requests.
        """
        if assignment is None or self._min_gap_sec <= 0:
            self._record_request(assignment)
            return
        now = time.monotonic()
        with self._lock:
            last = self._last_req_at.get(assignment.index, 0.0)
        elapsed = now - last
        gap = self._min_gap_sec
        if jitter_pct > 0:
            gap += random.uniform(0, gap * jitter_pct)
        wait = gap - elapsed
        if wait > 0:
            logger.debug("Costco AU proxy %d sleeping %.2fs for min-gap", assignment.index, wait)
            time.sleep(wait)
        self._record_request(assignment)

    def _record_request(self, assignment: Optional[ProxyAssignment]) -> None:
        if assignment is None:
            return
        with self._lock:
            self._last_req_at[assignment.index] = time.monotonic()


# ---------------------------------------------------------------------------
# Module-level singleton (lazy)
# ---------------------------------------------------------------------------

_POOL_LOCK = threading.Lock()
_POOL: Optional[CostcoAuProxyPool] = None


def _min_gap_sec_from_env() -> float:
    raw = os.environ.get("COSTCO_AU_MIN_REQUEST_GAP_SEC", "20")
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 20.0


def get_pool() -> Optional[CostcoAuProxyPool]:
    """Return the process-wide pool, building it on first call.

    Returns ``None`` if no proxies are configured — the scraper will then
    refuse to run (we never hit Costco AU from a raw datacenter IP).
    """
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is None:
            urls = load_proxy_urls()
            if not urls:
                return None
            _POOL = CostcoAuProxyPool(urls, min_gap_sec=_min_gap_sec_from_env())
            logger.info(
                "Costco AU proxy pool initialised: %d proxies, min_gap=%.1fs",
                len(urls), _POOL._min_gap_sec,
            )
    return _POOL


def reset_pool_for_tests() -> None:
    """Reset the module singleton — for tests only."""
    global _POOL
    with _POOL_LOCK:
        _POOL = None


__all__ = [
    "ProxyAssignment",
    "ProxyBlockedError",
    "CostcoAuProxyPool",
    "load_proxy_urls",
    "get_pool",
    "reset_pool_for_tests",
]
