"""Unified price router.

``get_price`` is the single entry point that callers should prefer. It dispatches
to Yahoo (stocks) or CoinGecko (crypto) based on ``asset_type``. With
``asset_type="auto"`` it uses ``ticker_classifier.TickerClassifier`` to decide
whether a symbol is a stock, crypto, or forex instrument before routing.
"""

import logging
import threading
from typing import Any, Mapping, Optional

from .coingecko import get_crypto_info
from .yahoo import get_stock_info

logger = logging.getLogger(__name__)

# Classifier categories that should be priced via Yahoo Finance. Crypto uses
# CoinGecko. These mirror the upper-cased Yahoo ``quoteType`` values plus the
# forex marker emitted by ``ticker_classifier``.
_STOCK_CATEGORIES = {"EQUITY", "ETF", "INDEX", "FUTURE", "MUTUALFUND", "FOREX"}
_CRYPTO_CATEGORIES = {"CRYPTO"}

_shared_classifier = None
_classifier_lock = threading.Lock()


def get_shared_classifier():
    """Return a lazily-created process-wide ``TickerClassifier`` instance."""
    global _shared_classifier
    with _classifier_lock:
        if _shared_classifier is None:
            from ticker_classifier.classifier import TickerClassifier

            _shared_classifier = TickerClassifier()
        return _shared_classifier


async def _classify(ticker: str, classifier) -> Optional[dict]:
    cls = classifier or get_shared_classifier()
    try:
        results = await cls.classify_async([ticker])
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("[router] classification failed for %s: %r", ticker, exc)
        return None
    return results[0] if results else None


async def _best_effort(ticker: str) -> Optional[dict]:
    """Try stock first, then crypto, for unclassifiable symbols."""
    stock = await get_stock_info(ticker)
    if stock is not None:
        return stock
    return await get_crypto_info(ticker)


async def price_from_classification(classification: Mapping[str, Any]) -> Optional[dict]:
    """Fetch a quote for an already-classified symbol.

    Routes crypto to CoinGecko and everything else to Yahoo (using
    ``yahoo_lookup`` when present). Accepts either a ``ticker_classifier`` result
    (``category``/``ticker``) or a consumer's own classification entry
    (``kind``/``symbol``), so callers that already classify in batch can delegate
    just the price routing here instead of duplicating it.

    Returns ``None`` when no symbol can be determined.
    """
    category = str(
        classification.get("category") or classification.get("kind") or ""
    ).upper()
    symbol = str(
        classification.get("ticker") or classification.get("symbol") or ""
    ).strip()
    if not symbol:
        return None

    if category in _CRYPTO_CATEGORIES:
        return await get_crypto_info(symbol)

    lookup = classification.get("yahoo_lookup") or symbol
    return await get_stock_info(lookup)


async def _quote_for_classification(classification: dict, ticker: str) -> Optional[dict]:
    """Route an auto-classified result, falling back to best-effort if unknown."""
    category = str(classification.get("category") or "").upper()

    if category in _CRYPTO_CATEGORIES or category in _STOCK_CATEGORIES:
        return await price_from_classification(classification)

    return await _best_effort(ticker)


def _build_ticker_info(classification: dict, quote: Optional[dict]) -> dict:
    """Project a classifier result onto the package's own ``TickerInfo`` shape."""
    return {
        "ticker": str(classification.get("ticker") or "").upper() or None,
        "category": str(classification.get("category") or "").upper() or "UNKNOWN",
        "name": classification.get("name"),
        "market_cap": classification.get("market_cap"),
        "sector": classification.get("sector"),
        "industry": classification.get("industry"),
        "company_profile": classification.get("company_profile"),
        "alternatives": classification.get("alternatives") or [],
        "quote": quote,
    }


async def get_price(
    ticker: str, asset_type: str = "stock", *, classifier=None
) -> Optional[dict]:
    """Fetch a normalized price quote for ``ticker``.

    Parameters
    ----------
    ticker : str
        The symbol to look up.
    asset_type : str, optional
        One of ``"stock"`` (default), ``"crypto"``, or ``"auto"``. ``"auto"``
        classifies the symbol via ``ticker_classifier`` before routing.
    classifier : optional
        A ``TickerClassifier``-like instance used when ``asset_type="auto"``.
        Defaults to a shared process-wide instance. Primarily for testing.

    Returns
    -------
    dict | None
        A quote dict ``{price, change_percent, volume, website, source}`` or
        ``None`` when no quote could be resolved.
    """
    ticker = (ticker or "").strip()
    if not ticker:
        return None

    kind = (asset_type or "stock").strip().lower()

    if kind == "stock":
        return await get_stock_info(ticker)
    if kind == "crypto":
        return await get_crypto_info(ticker)
    if kind != "auto":
        raise ValueError(f"Unknown asset_type: {asset_type!r}")

    classification = await _classify(ticker, classifier)
    if not classification:
        return await _best_effort(ticker)

    return await _quote_for_classification(classification, ticker)


async def get_ticker(ticker: str, *, classifier=None) -> Optional[dict]:
    """Fetch everything known about ``ticker``: classification metadata + quote.

    Classifies the symbol once (via ``ticker_classifier``), routes to the
    appropriate price source, and merges the result into a single ``TickerInfo``
    dict containing identity/metadata (sector, industry, market cap, company
    profile, ...) alongside the live ``quote``.

    Parameters
    ----------
    ticker : str
        The symbol to look up.
    classifier : optional
        A ``TickerClassifier``-like instance. Defaults to a shared process-wide
        instance. Primarily for testing.

    Returns
    -------
    dict | None
        A ``TickerInfo`` dict, or ``None`` when neither classification nor a
        quote could be resolved.
    """
    ticker = (ticker or "").strip()
    if not ticker:
        return None

    classification = await _classify(ticker, classifier)
    if not classification:
        quote = await _best_effort(ticker)
        if quote is None:
            return None
        return _build_ticker_info({"ticker": ticker, "category": "UNKNOWN"}, quote)

    quote = await _quote_for_classification(classification, ticker)
    return _build_ticker_info(classification, quote)
