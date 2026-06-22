"""Unified ticker price data from Yahoo Finance, CoinGecko, and TradingView."""

from typing import Any, Optional, TypedDict

from .coingecko import get_crypto_info
from .router import (
    get_price,
    get_shared_classifier,
    get_ticker,
    price_from_classification,
)
from .tradingview_quote import get_tradingview_quote
from .tradingview_stream import RealTimePool, close_shared_pool, get_shared_pool
from .yahoo import get_stock_info

__version__ = "0.1.1"


class Quote(TypedDict, total=False):
    """Normalized price quote returned by the pricing helpers."""

    price: float
    change_percent: float
    volume: float
    website: str
    source: str  # "yahoo" | "coingecko" | "tradingview"
    last_close: Optional[float]  # previous day's close; yahoo only
    session: str  # "regular" | "pre-market" | "after-hours" | "closed"; yahoo only
    extended_price: Optional[float]  # after-hours or pre-market price; yahoo only
    extended_change_percent: Optional[
        float
    ]  # (extended_price − price) / price × 100; yahoo only


class TickerInfo(TypedDict, total=False):
    """Everything known about a ticker: classification metadata + live quote."""

    ticker: Optional[str]  # canonical symbol
    category: str  # EQUITY | ETF | INDEX | FUTURE | FOREX | CRYPTO | UNKNOWN
    name: Optional[str]
    market_cap: Optional[float]
    sector: Optional[str]
    industry: Optional[str]
    company_profile: Optional[dict[str, Any]]
    alternatives: list[str]
    quote: Optional[Quote]


__all__ = [
    "Quote",
    "TickerInfo",
    "get_price",
    "get_ticker",
    "price_from_classification",
    "get_shared_classifier",
    "get_stock_info",
    "get_crypto_info",
    "get_tradingview_quote",
    "RealTimePool",
    "get_shared_pool",
    "close_shared_pool",
]
