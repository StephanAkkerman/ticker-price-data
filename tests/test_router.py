from unittest.mock import AsyncMock, patch

import pytest

from ticker_price_data.router import get_price

STOCK_QUOTE = {"price": 185.0, "source": "yahoo"}
CRYPTO_QUOTE = {"price": 65000.0, "source": "coingecko"}


class FakeClassifier:
    """Minimal stand-in for ticker_classifier.TickerClassifier."""

    def __init__(self, classification: dict | None):
        self._classification = classification

    async def classify_async(self, symbols):
        return [self._classification for _ in symbols]


@pytest.mark.asyncio
async def test_get_price_stock_calls_get_stock_info():
    with patch(
        "ticker_price_data.router.get_stock_info",
        new=AsyncMock(return_value=STOCK_QUOTE),
    ) as mock_stock:
        result = await get_price("AAPL", "stock")

    mock_stock.assert_awaited_once_with("AAPL")
    assert result == STOCK_QUOTE


@pytest.mark.asyncio
async def test_get_price_crypto_calls_get_crypto_info():
    with patch(
        "ticker_price_data.router.get_crypto_info",
        new=AsyncMock(return_value=CRYPTO_QUOTE),
    ) as mock_crypto:
        result = await get_price("BTC", "crypto")

    mock_crypto.assert_awaited_once_with("BTC")
    assert result == CRYPTO_QUOTE


@pytest.mark.asyncio
async def test_get_price_auto_crypto_routes_to_crypto_with_canonical_symbol():
    classifier = FakeClassifier(
        {"category": "CRYPTO", "ticker": "BTC", "yahoo_lookup": "BTC-USD"}
    )
    with patch(
        "ticker_price_data.router.get_crypto_info",
        new=AsyncMock(return_value=CRYPTO_QUOTE),
    ) as mock_crypto:
        result = await get_price("bitcoin", "auto", classifier=classifier)

    mock_crypto.assert_awaited_once_with("BTC")
    assert result == CRYPTO_QUOTE


@pytest.mark.asyncio
async def test_get_price_auto_equity_routes_to_stock_with_yahoo_lookup():
    classifier = FakeClassifier(
        {"category": "EQUITY", "ticker": "AAPL", "yahoo_lookup": "AAPL"}
    )
    with patch(
        "ticker_price_data.router.get_stock_info",
        new=AsyncMock(return_value=STOCK_QUOTE),
    ) as mock_stock:
        result = await get_price("AAPL", "auto", classifier=classifier)

    mock_stock.assert_awaited_once_with("AAPL")
    assert result == STOCK_QUOTE


@pytest.mark.asyncio
async def test_get_price_auto_forex_routes_to_stock_with_yahoo_lookup():
    classifier = FakeClassifier(
        {"category": "Forex", "ticker": "EUR", "yahoo_lookup": "EURUSD=X"}
    )
    with patch(
        "ticker_price_data.router.get_stock_info",
        new=AsyncMock(return_value=STOCK_QUOTE),
    ) as mock_stock:
        await get_price("EUR", "auto", classifier=classifier)

    mock_stock.assert_awaited_once_with("EURUSD=X")


@pytest.mark.asyncio
async def test_get_price_auto_unknown_falls_back_to_crypto_when_stock_none():
    classifier = FakeClassifier({"category": "Unknown", "ticker": "XYZ"})
    with (
        patch(
            "ticker_price_data.router.get_stock_info",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "ticker_price_data.router.get_crypto_info",
            new=AsyncMock(return_value=CRYPTO_QUOTE),
        ),
    ):
        result = await get_price("XYZ", "auto", classifier=classifier)

    assert result == CRYPTO_QUOTE


@pytest.mark.asyncio
async def test_get_price_auto_none_classification_falls_back_to_stock():
    classifier = FakeClassifier(None)
    with patch(
        "ticker_price_data.router.get_stock_info",
        new=AsyncMock(return_value=STOCK_QUOTE),
    ) as mock_stock:
        result = await get_price("AAPL", "auto", classifier=classifier)

    mock_stock.assert_awaited_once_with("AAPL")
    assert result == STOCK_QUOTE


@pytest.mark.asyncio
async def test_get_price_empty_ticker_returns_none():
    assert await get_price("", "auto") is None
