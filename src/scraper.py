"""eBay listing page scraper (ebay.com)."""
from __future__ import annotations

import re
from typing import Any

from .helpers import fail, fetch_html, ok, parse_money, soup


def scrape_product(
    *,
    url: str,
    region: str,
    vendor: str,
    proxy_urls: list[str],
    timeout_secs: int,
    max_retries: int,
    actor_input: dict[str, Any],
) -> dict:
    last_err = "unknown"
    for attempt in range(max(1, max_retries + 1)):
        try:
            html, status = fetch_html(url, proxy_urls=proxy_urls, timeout_secs=timeout_secs)
            if status >= 400:
                last_err = f"HTTP {status}"
                continue
            low = html.lower()
            if "checking your browser" in low or "captcha" in low:
                last_err = "blocked_or_captcha"
                continue
            doc = soup(html)
            title_el = (
                doc.select_one("h1.x-item-title__mainTitle span")
                or doc.select_one("h1[itemprop=name]")
                or doc.select_one("#itemTitle")
                or doc.select_one("h1")
            )
            title = title_el.get_text(strip=True) if title_el else None

            price = None
            for sel in (
                "div.x-price-primary span.ux-textspans",
                "[itemprop=price]",
                "#prcIsum",
                "#mm-saleDscPrc",
                ".x-bin-price__content span",
            ):
                el = doc.select_one(sel)
                if el:
                    price = parse_money(el.get("content") or el.get_text())
                    if price is not None:
                        break
            if price is None:
                m = re.search(r'"value"\s*:\s*([0-9.]+)', html)
                if m:
                    price = float(m.group(1))

            stock = 1
            qty = doc.select_one("#qtySubTxt") or doc.select_one(".x-quantity__availability")
            qty_text = (qty.get_text(" ", strip=True) if qty else "").lower()
            if "out of stock" in qty_text or "ended" in low:
                stock = 0
            else:
                m = re.search(r"(\d+)\s+available", qty_text)
                if m:
                    stock = int(m.group(1))

            if price is None and not title:
                last_err = "parse_failed"
                continue

            return ok(
                price,
                stock,
                title,
                vendor=vendor,
                region=region,
                url=url,
                host="ebay.com",
                attempt=attempt + 1,
            )
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc)
    return fail("ebay_scrape_failed", last_err, vendor=vendor, region=region, url=url)
