import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    if ":" in s:
        return s

    if s.endswith("=X"):
        s = s[:-2]

    if s.startswith("^"):
        s = s[1:]

    if s.endswith("-USD"):
        s = s.replace("-", "")

    return s


def _scanner_priority(asset_hint: str | None) -> list[str]:
    hint = str(asset_hint or "").strip().lower()
    if hint == "crypto":
        return ["crypto", "global"]
    if hint == "forex":
        return ["forex", "global"]
    if hint in {"index", "future"}:
        return ["global"]
    return ["america", "global"]


def _pick_best_row(
    rows: list[dict], asset_hint: str | None, requested_symbol_full: str | None = None
) -> Optional[dict]:
    hint = str(asset_hint or "").strip().lower()
    best: tuple[float, dict] | None = None

    for row in rows:
        price = _to_float(row.get("close"))
        if price is None or price <= 0:
            continue

        score = 0.0
        row_type = str(row.get("type") or "").lower()
        row_symbol = str(row.get("symbol") or "").upper()
        volume = _to_float(row.get("volume"))

        if volume is not None and volume > 0:
            score += 1.0

        if hint == "crypto" and "crypto" in row_type:
            score += 5.0
            if row_symbol.endswith("USD") or row_symbol.endswith("USDT"):
                score += 1.0
        elif hint == "forex" and "forex" in row_type:
            score += 5.0
        elif hint in {"stock", "equity", "index", "future", ""}:
            if "stock" in row_type:
                score += 4.0
            elif "index" in row_type:
                score += 3.0

        if best is None or score > best[0]:
            best = (score, row)

    # If a requested full symbol was provided (e.g., 'CRYPTOCAP:TOTAL'), prefer exact matches
    if requested_symbol_full:
        rsf = str(requested_symbol_full).strip().upper()
        try:
            rp = rsf.split(":", 1)[0]
        except Exception:
            rp = None

        # prefer exact symbol match first
        for row in rows:
            try:
                row_symbol = str(row.get("symbol") or "").upper()
                if row_symbol == rsf:
                    return row
            except Exception:
                continue

        # fallback: prefer rows from the same exchange/prefix
        if rp:
            for row in rows:
                try:
                    row_exchange = str(row.get("exchange") or "").upper()
                    row_symbol = str(row.get("symbol") or "").upper()
                    if row_exchange == rp or row_symbol.startswith(f"{rp}:"):
                        return row
                except Exception:
                    continue

        # Strict mode for qualified symbols: if no exact/prefix match was found,
        # do not fall back to an unrelated exchange (e.g., OTC:PCCYF for USI:PCC).
        return None

    return best[1] if best else None


def _fetch_symbol_market_row_sync(
    symbol: str, asset_hint: str | None, requested_symbol_full: str | None = None
) -> Optional[dict]:
    try:
        from tradingview_scraper.symbols.symbol_markets import SymbolMarkets
    except Exception as exc:
        logger.debug("[tradingview] package import failed: %r", exc)
        return None

    markets = SymbolMarkets()
    rows: list[dict] = []

    for scanner in _scanner_priority(asset_hint):
        try:
            result = markets.scrape(symbol=symbol, scanner=scanner, limit=25)
        except TypeError:
            result = markets.scrape(symbol=symbol, limit=25)
        except Exception as exc:
            logger.debug(
                "[tradingview] symbol_markets scrape failed for %s/%s: %r",
                symbol,
                scanner,
                exc,
            )
            continue

        data = result.get("data") if isinstance(result, dict) else result
        if isinstance(data, list):
            rows.extend([r for r in data if isinstance(r, dict)])

        # If we have a specific requested symbol (e.g., 'TVC:VIX'), keep accumulating
        # rows from all scanners to ensure we find the exact match.
        # Otherwise, do early returns for performance.
        if requested_symbol_full is None:
            best = _pick_best_row(rows, asset_hint, requested_symbol_full)
            if best is not None:
                return best

    return _pick_best_row(rows, asset_hint, requested_symbol_full)


_TV_SESSION_MAP: dict[str, str] = {
    "pre_market": "pre-market",
    "market": "regular",
    "post_market": "after-hours",
    "out_of_session": "closed",
}


async def get_tradingview_quote(
    symbol: str, asset_hint: str | None = None, prefer_realtime: bool = True
) -> Optional[dict]:
    """Best-effort TradingView quote fallback.

    Returns data in the same shape as yahoo/coingecko quote helpers:
    ``{price, change_percent, volume, website}``. When the websocket pool is
    used, also includes ``session``, and ``extended_price`` /
    ``extended_change_percent`` when pre/after-hours data is available.
    """

    normalized = _normalize_symbol(symbol)
    if not normalized:
        return None

    # determine requested full symbol from original symbol (e.g., 'CRYPTOCAP:TOTAL', 'AMEX:SPY')
    requested_symbol_full = None
    try:
        if ":" in symbol:
            requested_symbol_full = str(symbol).strip().upper()
    except Exception:
        requested_symbol_full = None

    # Prefer the shared websocket pool first. This keeps one TradingView socket
    # alive per process and avoids the scanner picking an approximate row.
    try:
        from .tradingview_stream import get_shared_pool
    except Exception:
        get_shared_pool = None

    if prefer_realtime and get_shared_pool is not None:
        try:
            pool_symbol = requested_symbol_full or normalized
            realtime = await get_shared_pool().get_quote(
                pool_symbol, asset_hint=asset_hint, timeout=6.0
            )
        except Exception as exc:
            logger.debug(
                "[tradingview] realtime pool failed for %s: %r",
                requested_symbol_full or normalized,
                exc,
            )
            realtime = None

        if isinstance(realtime, dict) and _to_float(realtime.get("price")) is not None:
            price = _to_float(realtime.get("price"))
            if price is not None:
                raw = realtime.get("raw") or {}
                tv_session = str(raw.get("current_session") or "")
                session = _TV_SESSION_MAP.get(tv_session, "closed")

                quote: dict = {
                    "price": price,
                    "change_percent": _to_float(realtime.get("change_percent")) or 0.0,
                    "volume": _to_float(realtime.get("volume")) or 0.0,
                    "website": str(
                        realtime.get("website")
                        or f"https://www.tradingview.com/symbols/{normalized.replace(':', '-')}/"
                    ),
                    "source": "tradingview",
                    "session": session,
                }

                rtc_price = _to_float(raw.get("rtc"))
                rchp = _to_float(raw.get("rchp"))
                if (
                    session != "regular"
                    and rtc_price is not None
                    and rtc_price != price
                ):
                    quote["extended_price"] = rtc_price
                    quote["extended_change_percent"] = (
                        rchp if rchp is not None else (rtc_price - price) / price * 100
                    )

                return quote

    row = await asyncio.to_thread(
        _fetch_symbol_market_row_sync, normalized, asset_hint, requested_symbol_full
    )
    if row is None and requested_symbol_full and ":" in requested_symbol_full:
        # Some scanners only match on the ticker part. Keep this as fallback,
        # but only after trying the fully-qualified symbol first.
        ticker_only = requested_symbol_full.split(":", 1)[1]
        row = await asyncio.to_thread(
            _fetch_symbol_market_row_sync,
            ticker_only,
            asset_hint,
            requested_symbol_full,
        )
    if row is None:
        return None

    price = _to_float(row.get("close"))
    if price is None:
        return None

    change = _to_float(row.get("change"))
    volume = _to_float(row.get("volume"))
    tv_symbol = str(row.get("symbol") or normalized).upper()

    return {
        "price": price,
        "change_percent": change if change is not None else 0.0,
        "volume": volume if volume is not None else 0.0,
        "website": f"https://www.tradingview.com/symbols/{tv_symbol.replace(':', '-')}/",
        "source": "tradingview",
    }


if __name__ == "__main__":
    import json

    async def main() -> None:
        quote = await get_tradingview_quote("USI:PCC", "stock")
        print(json.dumps(quote, indent=2))

    asyncio.run(main())
