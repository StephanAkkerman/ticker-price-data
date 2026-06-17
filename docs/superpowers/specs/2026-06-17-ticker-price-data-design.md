# Design: `ticker-price-data` — unified ticker pricing package

**Date:** 2026-06-17
**Status:** Approved (implementation authorized)

## Purpose

Extract all ticker pricing logic from `fintwit-web` into a standalone, pip-installable
package `ticker-price-data` so it can be reused across repos (fintwit-web, fintwit-bot,
future projects). The package fetches a normalized price quote for any ticker using:

- **Yahoo Finance** for stocks/indices/forex/futures
- **CoinGecko** for crypto
- **TradingView** (realtime websocket pool + scraper) as a universal fallback

`fintwit-web` becomes a consumer of the package (single source of truth — the old service
files are deleted).

## Goals

1. Move the 4 pricing modules out of `fintwit-web/app/services/` into the package, intact.
2. Add a new unified `get_price(ticker, asset_type)` router.
3. `asset_type="auto"` uses `ticker-classifier` to decide stock vs crypto vs forex.
4. Fix the recurring **CoinGecko rate-limiting** by switching to the website's
   `search_v2` endpoint (the fintwit-bot approach).
5. Make `fintwit-web` depend on the package and delete its old pricing files.

Out of scope: `tradingview-ta` rate limits (separate `tradingview_ta_service.py`, stays in
fintwit-web).

## Normalized quote shape

All functions return `Optional[dict]` with this shape (unchanged from current fintwit-web,
so it is a true drop-in):

```python
{
    "price": float,
    "change_percent": float,
    "volume": float,
    "website": str,
    "source": str,          # "yahoo" | "coingecko" | "tradingview"
    # yahoo also includes:
    "last_close": float | None,
}
```

A `Quote` `TypedDict` is added for typing only; the runtime value stays a plain dict.

## Package layout (src-layout, matching the template)

```
ticker-price-data/
  src/ticker_price_data/
    __init__.py            # public API exports
    router.py              # NEW: get_price(), shared TickerClassifier
    yahoo.py               # get_stock_info  (moved as-is)
    coingecko.py           # get_crypto_info (rewritten primary path)
    tradingview_quote.py   # get_tradingview_quote (moved as-is)
    tradingview_stream.py  # RealTimePool, get_shared_pool, close_shared_pool (moved as-is)
  tests/
    test_yahoo.py
    test_coingecko.py
    test_tradingview.py
    test_router.py
    fixtures/coingecko_search_v2.json
  pyproject.toml           # add [build-system] + [project] + deps
  requirements.txt
  README.md
```

Delete the template's `src/main.py` and `tests/test_sample.py`.

## Public API (`__init__.py`)

```python
from .router import get_price
from .yahoo import get_stock_info
from .coingecko import get_crypto_info
from .tradingview_quote import get_tradingview_quote
from .tradingview_stream import RealTimePool, get_shared_pool, close_shared_pool

__all__ = [
    "get_price",
    "get_stock_info",
    "get_crypto_info",
    "get_tradingview_quote",
    "RealTimePool",
    "get_shared_pool",
    "close_shared_pool",
    "Quote",
]
```

## Module details

### yahoo.py (moved as-is)
- `get_stock_info(ticker)`: 60s cache + 900s stale fallback, semaphore(8), symbol overrides
  (DXY/VIX/SPX), Yahoo v8 chart endpoint, TradingView fallback.
- Drop the `try/except ImportError: from app.services...` shim; use the package-relative
  import `from .tradingview_quote import get_tradingview_quote`.

### tradingview_quote.py / tradingview_stream.py (moved as-is)
- `get_tradingview_quote(symbol, asset_hint, prefer_realtime)`: prefers the shared websocket
  pool, falls back to the `tradingview_scraper` `SymbolMarkets` scanner with row scoring.
- `RealTimePool` / `get_shared_pool` / `close_shared_pool`: single shared websocket pool.
- Drop the `app.services...` fallback imports; use package-relative imports.

### coingecko.py (rewritten primary path — rate-limit fix)
Replace the rate-limited two-call flow (`api.coingecko.com/api/v3/search` →
`/api/v3/simple/price`) with a single call to the website search endpoint:

```
GET https://www.coingecko.com/en/search_v2?query={ticker}
Headers: browser User-Agent
```

Parse `coins[0]`:
- `id`     → `website = https://www.coingecko.com/en/coins/{id}`, canonical `symbol`
- `data.price`                         → price
- `data.price_change_percentage_24h.usd` → change_percent
- `data.total_volume`                  → volume

**Numeric parsing:** write a correct parser that strips `$` and `,` but **preserves the
decimal point** (the fintwit-bot `sanitize_currency_value` strips `.` too, corrupting small
prices like `$0.0123` → `123`; do not copy that bug). Handle values that are already numbers.

Preserve from the current implementation:
- 120s positive cache, 30s negative cache, `asyncio.Semaphore(4)`, `_reset_cache_for_tests`.
- Fallback chain when search_v2 yields nothing usable:
  `_fallback_to_yahoo` (`{TICKER}-USD`) → `_fallback_to_tradingview`
  (`{TICKER}USD`, `{TICKER}-USD`, `{TICKER}`, asset_hint="crypto").
- Treat an HTTP 429 / rate-limit status body the same as "no result" → go to fallback chain.

### router.py (new)

```python
async def get_price(ticker: str, asset_type: str = "stock") -> Optional[Quote]: ...
```

- `"stock"`  → `get_stock_info(ticker)`
- `"crypto"` → `get_crypto_info(ticker)`
- `"auto"`   → classify, then route:
  - call shared `TickerClassifier().classify_async([ticker])`, take `[0]`
  - `category == "CRYPTO"` → `get_crypto_info(classification["ticker"])`
  - `category in {EQUITY, ETF, INDEX, FUTURE, MUTUALFUND, Forex}` →
    `get_stock_info(classification["yahoo_lookup"] or ticker)`
  - `category == "Unknown"` / `None` → best-effort: `get_stock_info(ticker)`, then
    `get_crypto_info(ticker)`, else `None`
- Shared classifier via lazily-created module singleton `get_shared_classifier()`, with an
  optional `classifier=` parameter on `get_price` for test injection.

Note: classification category strings come straight from `ticker_classifier` (equity types
are upper-cased Yahoo `quoteType`s; crypto is `"CRYPTO"`; forex is `"Forex"`). The router
matches case-insensitively to be safe.

## Packaging (`pyproject.toml`)

Add to the existing template file (which only has isort/ruff config):

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "ticker-price-data"
version = "0.1.0"
description = "Unified ticker price data from Yahoo, CoinGecko, and TradingView."
requires-python = ">=3.10"
dependencies = [
    "aiohttp",
    "tradingview-scraper>=0.4.20",
    "ticker-classifier @ git+https://github.com/StephanAkkerman/ticker-classifier.git",
]

[tool.hatch.build.targets.wheel]
packages = ["src/ticker_price_data"]
```

`requirements.txt` lists the same runtime deps for the `pip install -r` path.

## Tests

Port the pricing tests from `fintwit-web/tests/test_services.py`, repointed to
`ticker_price_data`, plus the aiohttp mock helpers (`_mock_response`, `_mock_session`):

- `test_yahoo.py`: the 8 `test_get_stock_info_*` cases (success, zero-change, missing price,
  empty result, http error, DXY override, exception, tradingview fallback).
- `test_coingecko.py`: rewrite the 7 `test_get_crypto_info_*` cases for the search_v2 flow
  (success from a captured `fixtures/coingecko_search_v2.json`, no coins → yahoo fallback,
  http/429 error → yahoo fallback, → tradingview when yahoo None, cache hit on 2nd call,
  exception → None). Add a parser unit test proving `$0.0123` stays `0.0123`.
- `test_tradingview.py`: `get_tradingview_quote` row-scoring / normalization (mock
  `SymbolMarkets`, no network).
- `test_router.py`: `get_price` routing for stock/crypto/auto with an injected fake
  classifier (no network) — assert CRYPTO→get_crypto_info, EQUITY→get_stock_info,
  Unknown→best-effort.

CI already present in the template (`pytest.yml`, `ruff-check.yml`, `pyversions.yml`).

## fintwit-web integration (delete old, depend on package)

1. Add to `fintwit-web/requirements.txt`:
   `ticker-price-data @ git+https://github.com/StephanAkkerman/ticker-price-data.git`
   (local dev: `pip install -e ../ticker-price-data`).
2. Delete `app/services/{yahoo,coingecko,tradingview_quote,tradingview_stream}.py`.
3. Repoint imports to `ticker_price_data` in:
   - `app/runtime/enricher.py` (`get_crypto_info`, `get_stock_info`)
   - `app/api/main.py` (`get_stock_info`)
   - `app/api/overview.py` (`get_tradingview_quote`)
   - `app/services/macro_market.py` (`get_tradingview_quote`)
   - `scripts/inspect_tradingview.py` (`RealTimePool`)
4. Update `fintwit-web/tests`:
   - Remove the ported `test_get_stock_info_*` / `test_get_crypto_info_*` from
     `test_services.py` and the yahoo/coingecko cache-reset bits of the autouse fixture.
   - Repoint any `patch("app.services.coingecko...")` / `...yahoo...` targets in
     `test_enricher.py`, `test_macro_market.py`, and remaining `test_services.py` to
     `ticker_price_data...`.
5. Update `fintwit-web/CLAUDE.md` and `docs/` to note pricing now lives in the external
   `ticker-price-data` package.

## Verification

- `ticker-price-data`: `pytest -q` green; `ruff check .` clean; package imports
  (`python -c "import ticker_price_data; print(ticker_price_data.__all__)"`).
- `fintwit-web`: `pytest --maxfail=1 -q` green after editable-install of the package;
  `ruff check .` clean; grep shows no remaining `app.services.{yahoo,coingecko,tradingview_quote,tradingview_stream}` references.

## Risks / open points

- **search_v2 response shape** is an undocumented website endpoint; it can change. Mitigated
  by a captured fixture + parser tests, and the Yahoo/TradingView fallback chain.
- **ticker-classifier as a dependency** pulls in its own aiohttp/requests stack and writes a
  SQLite cache; acceptable since fintwit-web already depends on it.
