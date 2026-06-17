"""Unified price router.

``get_price`` is the single entry point that callers should prefer. It dispatches
to Yahoo (stocks) or CoinGecko (crypto) based on ``asset_type``. With
``asset_type="auto"`` it uses ``ticker_classifier.TickerClassifier`` to decide
whether a symbol is a stock, crypto, or forex instrument before routing.
"""

import logging
import threading
from typing import Optional

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

    category = str(classification.get("category") or "").upper()

    if category in _CRYPTO_CATEGORIES:
        symbol = classification.get("ticker") or ticker
        return await get_crypto_info(symbol)

    if category in _STOCK_CATEGORIES:
        symbol = classification.get("yahoo_lookup") or classification.get("ticker")
        return await get_stock_info(symbol or ticker)

    return await _best_effort(ticker)
