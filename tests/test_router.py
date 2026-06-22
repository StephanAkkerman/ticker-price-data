from unittest.mock import AsyncMock, patch

import pytest

from ticker_price_data.router import get_price, get_ticker, price_from_classification

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


# ---------------------------------------------------------------------------
# get_ticker — classification metadata + quote in one call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_ticker_equity_merges_metadata_and_quote():
    classifier = FakeClassifier(
        {
            "category": "EQUITY",
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "market_cap": 3_000_000_000_000,
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "company_profile": {"sector": "Technology"},
            "yahoo_lookup": "AAPL",
            "alternatives": [],
        }
    )
    with patch(
        "ticker_price_data.router.get_stock_info",
        new=AsyncMock(return_value=STOCK_QUOTE),
    ) as mock_stock:
        result = await get_ticker("AAPL", classifier=classifier)

    mock_stock.assert_awaited_once_with("AAPL")
    assert result["ticker"] == "AAPL"
    assert result["category"] == "EQUITY"
    assert result["name"] == "Apple Inc."
    assert result["market_cap"] == 3_000_000_000_000
    assert result["sector"] == "Technology"
    assert result["industry"] == "Consumer Electronics"
    assert result["company_profile"] == {"sector": "Technology"}
    assert result["quote"] == STOCK_QUOTE
    # TickerInfo exposes our own shape, not raw classifier internals.
    assert "yahoo_lookup" not in result


@pytest.mark.asyncio
async def test_get_ticker_crypto_uses_crypto_quote():
    classifier = FakeClassifier(
        {"category": "CRYPTO", "ticker": "BTC", "name": "Bitcoin", "market_cap": 1}
    )
    with patch(
        "ticker_price_data.router.get_crypto_info",
        new=AsyncMock(return_value=CRYPTO_QUOTE),
    ) as mock_crypto:
        result = await get_ticker("bitcoin", classifier=classifier)

    mock_crypto.assert_awaited_once_with("BTC")
    assert result["category"] == "CRYPTO"
    assert result["quote"] == CRYPTO_QUOTE


@pytest.mark.asyncio
async def test_get_ticker_unknown_classification_best_effort():
    classifier = FakeClassifier(None)
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
        result = await get_ticker("XYZ", classifier=classifier)

    assert result["category"] == "UNKNOWN"
    assert result["ticker"] == "XYZ"
    assert result["sector"] is None
    assert result["quote"] == CRYPTO_QUOTE


@pytest.mark.asyncio
async def test_get_ticker_returns_none_when_unknown_and_no_quote():
    classifier = FakeClassifier(None)
    with (
        patch(
            "ticker_price_data.router.get_stock_info",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "ticker_price_data.router.get_crypto_info",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = await get_ticker("XYZ", classifier=classifier)

    assert result is None


@pytest.mark.asyncio
async def test_get_ticker_empty_returns_none():
    assert await get_ticker("") is None


# ---------------------------------------------------------------------------
# price_from_classification — price an already-classified entry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_price_from_classification_crypto_uses_crypto_info():
    with patch(
        "ticker_price_data.router.get_crypto_info",
        new=AsyncMock(return_value=CRYPTO_QUOTE),
    ) as mock_crypto:
        result = await price_from_classification(
            {"category": "CRYPTO", "ticker": "BTC"}
        )

    mock_crypto.assert_awaited_once_with("BTC")
    assert result == CRYPTO_QUOTE


@pytest.mark.asyncio
async def test_price_from_classification_uses_yahoo_lookup():
    with patch(
        "ticker_price_data.router.get_stock_info",
        new=AsyncMock(return_value=STOCK_QUOTE),
    ) as mock_stock:
        result = await price_from_classification(
            {"category": "COMMODITY", "ticker": "USOIL", "yahoo_lookup": "CL=F"}
        )

    mock_stock.assert_awaited_once_with("CL=F")
    assert result == STOCK_QUOTE


@pytest.mark.asyncio
async def test_price_from_classification_accepts_enricher_entry_shape():
    # Enricher cache entries use "kind"/"symbol" rather than "category"/"ticker".
    with patch(
        "ticker_price_data.router.get_stock_info",
        new=AsyncMock(return_value=STOCK_QUOTE),
    ) as mock_stock:
        result = await price_from_classification(
            {"kind": "FUTURE", "symbol": "NQ", "yahoo_lookup": "NQ=F"}
        )

    mock_stock.assert_awaited_once_with("NQ=F")
    assert result == STOCK_QUOTE


@pytest.mark.asyncio
async def test_price_from_classification_crypto_enricher_shape():
    with patch(
        "ticker_price_data.router.get_crypto_info",
        new=AsyncMock(return_value=CRYPTO_QUOTE),
    ) as mock_crypto:
        result = await price_from_classification({"kind": "CRYPTO", "symbol": "ETH"})

    mock_crypto.assert_awaited_once_with("ETH")
    assert result == CRYPTO_QUOTE


@pytest.mark.asyncio
async def test_price_from_classification_empty_symbol_returns_none():
    assert await price_from_classification({"category": "EQUITY"}) is None
