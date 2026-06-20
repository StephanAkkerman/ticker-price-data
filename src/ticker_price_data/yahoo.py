import asyncio
import logging
import time
from typing import Optional

import aiohttp

from .market_session import get_us_stock_session
from .tradingview_quote import get_tradingview_quote

logger = logging.getLogger(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.57"
}

_CACHE_TTL_SECONDS = 60
_STALE_CACHE_TTL_SECONDS = 900
_REQUEST_SEMAPHORE = asyncio.Semaphore(8)

_cache: dict[str, tuple[float, Optional[dict]]] = {}
_cache_lock = asyncio.Lock()

_SYMBOL_LOOKUP_OVERRIDES = {
    "DXY": "DX-Y.NYB",
    "VIX": "^VIX",
    "SPX": "^GSPC",
}


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def _clone_payload(payload: Optional[dict]) -> Optional[dict]:
    if payload is None:
        return None
    return dict(payload)


def _last_non_null(values: object) -> Optional[float]:
    if not isinstance(values, list):
        return None

    for value in reversed(values):
        try:
            if value is None:
                continue
            return float(value)
        except (TypeError, ValueError):
            continue

    return None


def _lookup_candidates(symbol: str) -> list[str]:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        return []

    override = _SYMBOL_LOOKUP_OVERRIDES.get(normalized)
    if override and override != normalized:
        return [override, normalized]
    return [normalized]


async def _get_cached(symbol: str, *, allow_stale: bool) -> tuple[bool, Optional[dict]]:
    async with _cache_lock:
        item = _cache.get(symbol)
        if item is None:
            return False, None

        ts, payload = item
        age = time.time() - ts

        if age <= _CACHE_TTL_SECONDS:
            return True, _clone_payload(payload)

        if allow_stale and age <= _STALE_CACHE_TTL_SECONDS:
            return True, _clone_payload(payload)

        if age > _STALE_CACHE_TTL_SECONDS:
            _cache.pop(symbol, None)

        return False, None


async def _set_cached(symbol: str, payload: Optional[dict]) -> None:
    async with _cache_lock:
        _cache[symbol] = (time.time(), _clone_payload(payload))


def _reset_cache_for_tests() -> None:
    _cache.clear()


async def _fetch_yahoo_chart(lookup_symbol: str) -> Optional[dict]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{lookup_symbol}?range=1d&interval=1m&includePrePost=true"
    timeout = aiohttp.ClientTimeout(total=8)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                if response.status == 429:
                    logger.info("[yahoo] rate-limited for %s", lookup_symbol)
                return None

            data = await response.json()

    result = data.get("chart", {}).get("result")
    if not result:
        return None

    chart = result[0]
    meta = chart.get("meta", {})
    quote = chart.get("indicators", {}).get("quote", [{}])[0]

    current_price = meta.get("regularMarketPrice")
    if current_price is None:
        current_price = _last_non_null(quote.get("close"))

    if current_price is None:
        return None

    ext_price_raw = _last_non_null(quote.get("close"))

    prev_close = meta.get("previousClose")
    if prev_close is None:
        prev_close = meta.get("chartPreviousClose", current_price)

    change = 0.0
    if prev_close and prev_close != 0:
        change = ((current_price - prev_close) / prev_close) * 100

    volume = meta.get("regularMarketVolume", 0) * current_price if current_price else 0

    payload: dict = {
        "price": current_price,
        "last_close": prev_close,
        "change_percent": change,
        "volume": volume,
        "website": f"https://finance.yahoo.com/quote/{lookup_symbol}",
        "source": "yahoo",
        "_ext_price": ext_price_raw,
    }

    return payload


def _inject_session(payload: dict) -> dict:
    """Return a copy of payload with current session and extended-hours fields added.

    Strips the private _ext_price key and injects session, extended_price,
    extended_change_percent based on the current time. Extended fields are shown
    whenever session is not "regular" and the extended price differs from the
    regular close — this keeps after-hours data visible through weekends/holidays
    until pre-market begins.
    """
    result = dict(payload)
    ext_price = result.pop("_ext_price", None)

    session = get_us_stock_session()
    result["session"] = session

    price = result.get("price")
    if (
        session != "regular"
        and ext_price is not None
        and price
        and price != 0
        and ext_price != price
    ):
        result["extended_price"] = ext_price
        result["extended_change_percent"] = (ext_price - price) / price * 100

    return result


async def get_stock_info(ticker: str) -> Optional[dict]:
    symbol = _normalize_symbol(ticker)
    if not symbol:
        return None

    found, cached = await _get_cached(symbol, allow_stale=False)
    if found:
        return _inject_session(cached)

    async with _REQUEST_SEMAPHORE:
        # Re-check cache while waiting in the queue.
        found, cached = await _get_cached(symbol, allow_stale=False)
        if found:
            return _inject_session(cached)

        try:
            for lookup_symbol in _lookup_candidates(symbol):
                payload = await _fetch_yahoo_chart(lookup_symbol)
                if payload is None:
                    continue

                await _set_cached(symbol, payload)
                if lookup_symbol != symbol:
                    await _set_cached(lookup_symbol, payload)
                return _inject_session(payload)
        except Exception as exc:
            logger.debug("[yahoo] %s fetch failed: %r", symbol, exc)

        found, stale = await _get_cached(symbol, allow_stale=True)
        if found:
            logger.info("[yahoo] serving stale cache for %s", symbol)
            return _inject_session(stale)

        tradingview_payload = await get_tradingview_quote(symbol, asset_hint="stock")
        if tradingview_payload is not None:
            logger.info("[yahoo] using tradingview fallback for %s", symbol)
            await _set_cached(symbol, tradingview_payload)
            return tradingview_payload

        await _set_cached(symbol, None)
        return None


if __name__ == "__main__":
    import asyncio

    tickers = ["AAPL", "TSLA", "BTC-USD", "ETH-USD", "INVALID"]
    for ticker in tickers:
        info = asyncio.run(get_stock_info(ticker))
        print(f"{ticker}: {info}")
