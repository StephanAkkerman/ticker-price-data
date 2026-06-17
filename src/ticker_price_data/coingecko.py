"""Crypto price lookups via CoinGecko.

Uses the CoinGecko website search endpoint (``www.coingecko.com/en/search_v2``)
rather than the public API (``api.coingecko.com/api/v3``). The website endpoint
returns price, 24h change, and volume in a single response and is far less prone
to the free-tier rate limiting that affects the public API. Yahoo Finance and
TradingView are used as fallbacks when the search endpoint yields nothing usable.
"""

import asyncio
import logging
import re
import time
from typing import Any, Optional

import aiohttp

from .tradingview_quote import get_tradingview_quote
from .yahoo import get_stock_info

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://www.coingecko.com/en/search_v2"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

_CACHE_TTL_SECONDS = 120
_NEGATIVE_CACHE_TTL_SECONDS = 30

_cache: dict[str, tuple[float, Optional[dict]]] = {}
_cache_lock = asyncio.Lock()
_request_semaphore = asyncio.Semaphore(4)

_MONEY_RE = re.compile(r"[^0-9.\-]")


def _parse_money(value: Any) -> float:
    """Parse a CoinGecko money string such as ``"$1,234.56"`` into a float.

    Strips currency symbols and digit grouping while preserving the decimal
    point. Values that are already numeric pass through unchanged. Anything that
    cannot be parsed returns ``0.0``.
    """
    if isinstance(value, (int, float)):
        return float(value)

    if not isinstance(value, str):
        return 0.0

    cleaned = _MONEY_RE.sub("", value)
    if not cleaned or cleaned in {".", "-", "-."}:
        return 0.0

    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _pick_best_coin(coins: list[dict], ticker: str) -> dict | None:
    """Choose the most relevant coin for ``ticker`` from search results."""
    ticker_lower = ticker.lower()

    for coin in coins:
        if str(coin.get("symbol") or "").lower() == ticker_lower:
            return coin

    for coin in coins:
        if str(coin.get("name") or "").lower() == ticker_lower:
            return coin

    return coins[0] if coins else None


def _quote_from_coin(coin: dict) -> Optional[dict]:
    data = coin.get("data") or {}
    price = _parse_money(data.get("price"))
    if price <= 0:
        return None

    change = data.get("price_change_percentage_24h")
    if isinstance(change, dict):
        change = change.get("usd")
    change_percent = _parse_money(change)

    coin_id = str(coin.get("id") or "").strip()
    return {
        "price": price,
        "change_percent": change_percent,
        "volume": _parse_money(data.get("total_volume")),
        "website": (
            f"https://www.coingecko.com/en/coins/{coin_id}"
            if coin_id
            else "https://www.coingecko.com"
        ),
        "source": "coingecko",
    }


async def _get_cached(ticker: str) -> tuple[bool, Optional[dict]]:
    async with _cache_lock:
        item = _cache.get(ticker)
        if item is None:
            return False, None

        ts, payload = item
        if time.time() - ts > _CACHE_TTL_SECONDS:
            _cache.pop(ticker, None)
            return False, None

        return True, payload


async def _set_cached(ticker: str, payload: Optional[dict]) -> None:
    async with _cache_lock:
        _cache[ticker] = (time.time(), payload)


async def _set_negative_cached(ticker: str) -> None:
    async with _cache_lock:
        # Keep a short negative cache to avoid tight retry loops when upstream is down.
        _cache[ticker] = (
            time.time() - (_CACHE_TTL_SECONDS - _NEGATIVE_CACHE_TTL_SECONDS),
            None,
        )


async def _fallback_to_yahoo(ticker: str) -> Optional[dict]:
    yahoo_symbol = f"{ticker}-USD"
    info = await get_stock_info(yahoo_symbol)
    if info is None:
        return None

    return {
        "price": info.get("price", 0.0),
        "change_percent": info.get("change_percent", 0.0),
        "volume": info.get("volume", 0.0),
        "website": info.get(
            "website", f"https://finance.yahoo.com/quote/{yahoo_symbol}"
        ),
        "source": info.get("source") or "yahoo",
    }


async def _fallback_to_tradingview(ticker: str) -> Optional[dict]:
    for candidate in (f"{ticker}USD", f"{ticker}-USD", ticker):
        payload = await get_tradingview_quote(candidate, asset_hint="crypto")
        if payload is not None:
            logger.info("[coingecko] using tradingview fallback for %s", ticker)
            return payload

    return None


async def _fallback_quote(ticker: str) -> Optional[dict]:
    yahoo_payload = await _fallback_to_yahoo(ticker)
    if yahoo_payload is not None:
        return yahoo_payload
    return await _fallback_to_tradingview(ticker)


def _reset_cache_for_tests() -> None:
    _cache.clear()


async def _search_coins(
    session: aiohttp.ClientSession, ticker: str
) -> Optional[list[dict]]:
    async with session.get(
        _SEARCH_URL,
        params={"query": ticker},
        headers=_HEADERS,
    ) as response:
        if response.status == 429:
            logger.warning("[coingecko] search rate-limited for %s", ticker)
            return None

        if response.status != 200:
            logger.debug("[coingecko] search status=%s for %s", response.status, ticker)
            return None

        data = await response.json()

    coins = data.get("coins") if isinstance(data, dict) else None
    return coins if isinstance(coins, list) else []


async def get_crypto_info(ticker: str) -> Optional[dict]:
    ticker = (ticker or "").upper()
    if not ticker:
        return None

    found, cached = await _get_cached(ticker)
    if found:
        return cached

    try:
        async with _request_semaphore:
            async with aiohttp.ClientSession() as session:
                coins = await _search_coins(session, ticker)

            if not coins:
                fallback = await _fallback_quote(ticker)
                await _set_cached(ticker, fallback)
                return fallback

            coin = _pick_best_coin(coins, ticker)
            payload = _quote_from_coin(coin) if coin else None
            if payload is None:
                fallback = await _fallback_quote(ticker)
                await _set_cached(ticker, fallback)
                return fallback

            await _set_cached(ticker, payload)
            return payload
    except Exception as exc:
        logger.debug("[coingecko] %s fetch failed: %r", ticker, exc)
        fallback = await _fallback_quote(ticker)
        await _set_cached(ticker, fallback)
        return fallback


if __name__ == "__main__":
    tickers = ["BTC", "ETH", "INVALID"]
    for t in tickers:
        print(f"{t}: {asyncio.run(get_crypto_info(t))}")
