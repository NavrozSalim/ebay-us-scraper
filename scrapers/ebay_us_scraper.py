"""
eBay US scraper (ebay.com, ebay.ca fallback).

Public API:
    scrape_ebay_us(vendor_url, region, session=None)
    close_ebay_us_session(session)
"""
import logging

from .ebay_common import (
    EBAY_MARKET_US,
    SESSION_DEBUG_HTML_KEY,
    close_ebay_market_session,
    scrape_ebay_for_market,
)

logger = logging.getLogger("scrapers.ebay_us")


def scrape_ebay_us(vendor_url: str, region: str, session: dict = None) -> dict:
    return scrape_ebay_for_market(
        vendor_url,
        region or "USA",
        session,
        market=EBAY_MARKET_US,
    )


def close_ebay_us_session(session: dict):
    close_ebay_market_session(session, EBAY_MARKET_US)


__all__ = [
    "SESSION_DEBUG_HTML_KEY",
    "scrape_ebay_us",
    "close_ebay_us_session",
]
