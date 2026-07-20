"""
Residential proxy pool for eBay AU (``ebay.com.au``).

Reuses the same URL normalisation and sticky pool logic as Costco AU
(``costco_au_proxies``) but with eBay-specific env and a much shorter
per-proxy gap so catalog scrapes can hit ~15+ products/minute.

Configuration (precedence for URLs):

    EBAY_AU_PROXY_URLS   — eBay-only list (optional)
    COSTCO_AU_PROXY_URLS — shared with Costco when eBay list unset
    PROXY_URLS           — generic fallback

    EBAY_AU_MIN_REQUEST_GAP_SEC — default 2 (Costco uses 20)
    EBAY_AU_HTTP_RETRIES        — HTTP rotations before giving up (default 2)
    EBAY_AU_PROXY_BLOCK_COOLDOWN_SEC — cooldown after block (default 120)
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Optional
from urllib.parse import urlparse

from .costco_au_proxies import (
    CostcoAuProxyPool,
    ProxyAssignment,
    load_proxy_urls,
)

logger = logging.getLogger("scrapers.ebay_au.proxies")

_POOL_LOCK = threading.Lock()
_POOL: Optional[CostcoAuProxyPool] = None

_SESSION_PROXY_URL = "ebay_au_proxy_url"
_SESSION_PROXY_INDEX = "ebay_au_proxy_index"


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v.strip() if v is not None else default


def _trueish(val: str | None) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "on")


def ebay_au_proxy_disabled() -> bool:
    return _trueish(_env("EBAY_AU_DISABLE_PROXIES"))


def load_ebay_au_proxy_urls(env: dict | None = None) -> list[str]:
    """Proxy URLs for eBay AU — eBay-specific list first, then Costco/shared."""
    if env is None:
        ebay_only = _split_env_urls(_env("EBAY_AU_PROXY_URLS"))
        if ebay_only:
            return ebay_only
        return load_proxy_urls()
    get = env.get
    ebay_only = _split_env_urls(get("EBAY_AU_PROXY_URLS", ""))
    if ebay_only:
        from . import costco_au_proxies as cap

        out: list[str] = []
        for raw in ebay_only:
            norm = cap._normalize_proxy(  # noqa: SLF001 — shared normaliser
                raw,
                (get("PROXY_SCHEME") or "http").lower(),
                get("PROXY_USER", ""),
                get("PROXY_PASS", ""),
            )
            if norm:
                out.append(norm)
        return _dedupe(out)
    return load_proxy_urls(env)


def _split_env_urls(raw: str) -> list[str]:
    if not raw:
        return []
    import re

    return [p.strip() for p in re.split(r"[\s,]+", raw) if p.strip()]


def _dedupe(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _min_gap_sec() -> float:
    raw = _env("EBAY_AU_MIN_REQUEST_GAP_SEC", "2")
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 2.0


def _block_cooldown_sec() -> float:
    raw = _env("EBAY_AU_PROXY_BLOCK_COOLDOWN_SEC", "120")
    try:
        return max(10.0, float(raw))
    except ValueError:
        return 120.0


def _http_retries() -> int:
    raw = _env("EBAY_AU_HTTP_RETRIES", "2")
    try:
        return max(0, int(raw))
    except ValueError:
        return 2


def get_ebay_au_pool() -> Optional[CostcoAuProxyPool]:
    if ebay_au_proxy_disabled():
        return None
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is None:
            urls = load_ebay_au_proxy_urls()
            if not urls:
                return None
            _POOL = CostcoAuProxyPool(urls, min_gap_sec=_min_gap_sec())
            logger.info(
                "eBay AU proxy pool initialised: %d proxies, min_gap=%.1fs",
                len(urls),
                _POOL._min_gap_sec,
            )
    return _POOL


def proxies_configured() -> bool:
    pool = get_ebay_au_pool()
    return pool is not None and pool.size > 0


def reset_pool_for_tests() -> None:
    global _POOL
    with _POOL_LOCK:
        _POOL = None


def acquire_proxy(session: dict | None, *, force_rotate: bool = False) -> Optional[ProxyAssignment]:
    pool = get_ebay_au_pool()
    if pool is None:
        return None
    return pool.acquire(force_rotate=force_rotate)


def remember_proxy(session: dict | None, assignment: ProxyAssignment | None) -> None:
    if session is None or assignment is None:
        return
    session[_SESSION_PROXY_URL] = assignment.url
    session[_SESSION_PROXY_INDEX] = assignment.index


def proxy_chrome_arg(proxy_url: str) -> str:
    """Chrome ``--proxy-server`` value (credentials stripped — use IP allowlist)."""
    p = urlparse(proxy_url)
    port = p.port or (443 if p.scheme == "https" else 80)
    return f"{p.scheme}://{p.hostname}:{port}"


def mark_proxy_success(assignment: ProxyAssignment | None) -> None:
    pool = get_ebay_au_pool()
    if pool and assignment:
        pool.mark_success(assignment)


def mark_proxy_blocked(assignment: ProxyAssignment | None) -> None:
    pool = get_ebay_au_pool()
    if pool and assignment:
        pool.mark_blocked(assignment, cooldown_sec=_block_cooldown_sec())


def bind_curl_client(client, assignment: ProxyAssignment) -> None:
    client.proxies = assignment.as_requests_proxy()


def http_retry_limit() -> int:
    return _http_retries() + 1


def session_proxy_assignment(session: dict | None) -> Optional[ProxyAssignment]:
    if not session:
        return None
    url = session.get(_SESSION_PROXY_URL)
    idx = session.get(_SESSION_PROXY_INDEX)
    if url is None or idx is None:
        return None
    try:
        p = urlparse(url)
        host = p.hostname or ""
        port = p.port
        label = f"{host}:{port}" if port else host
    except Exception:
        label = "unknown"
    return ProxyAssignment(index=int(idx), url=url, label=label)


def rotate_proxy(session: dict | None) -> Optional[ProxyAssignment]:
    """Force the next acquire to pick a different proxy (after a block)."""
    assignment = acquire_proxy(session, force_rotate=True)
    remember_proxy(session, assignment)
    return assignment


__all__ = [
    "ProxyAssignment",
    "acquire_proxy",
    "bind_curl_client",
    "ebay_au_proxy_disabled",
    "get_ebay_au_pool",
    "http_retry_limit",
    "load_ebay_au_proxy_urls",
    "mark_proxy_blocked",
    "mark_proxy_success",
    "proxies_configured",
    "proxy_chrome_arg",
    "remember_proxy",
    "reset_pool_for_tests",
    "rotate_proxy",
    "session_proxy_assignment",
]
