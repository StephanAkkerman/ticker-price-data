import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from helpers import mock_response, mock_session

import ticker_price_data.coingecko as coingecko_service
from ticker_price_data.coingecko import get_crypto_info

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "coingecko_search_v2.json").read_text()
)


@pytest.fixture(autouse=True)
def _reset_coingecko_cache():
    with patch(
        "ticker_price_data.coingecko.get_tradingview_quote",
        new=AsyncMock(return_value=None),
    ):
        coingecko_service._reset_cache_for_tests()
        yield
        coingecko_service._reset_cache_for_tests()


# ---------------------------------------------------------------------------
# Numeric parsing
# ---------------------------------------------------------------------------


def test_parse_money_keeps_small_decimal():
    assert coingecko_service._parse_money("$0.0123") == pytest.approx(0.0123)


def test_parse_money_strips_grouping_and_dollar():
    assert coingecko_service._parse_money("$26,245,075,399") == 26_245_075_399.0


def test_parse_money_passes_through_numbers():
    assert coingecko_service._parse_money(3.2) == 3.2


def test_parse_money_handles_garbage():
    assert coingecko_service._parse_money("N/A") == 0.0


# ---------------------------------------------------------------------------
# get_crypto_info via search_v2
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_crypto_info_success_from_search_v2():
    with patch(
        "ticker_price_data.coingecko.aiohttp.ClientSession",
        mock_session(mock_response(200, FIXTURE)),
    ):
        result = await get_crypto_info("BTC")

    assert result is not None
    assert result["price"] == pytest.approx(65_993.78)
    assert result["change_percent"] == pytest.approx(0.16155310769260334)
    assert result["volume"] == 26_245_075_399.0
    assert "bitcoin" in result["website"]
    assert result["source"] == "coingecko"


@pytest.mark.asyncio
async def test_get_crypto_info_uses_single_request_and_caches():
    mock_cs = mock_session(mock_response(200, FIXTURE))
    with patch("ticker_price_data.coingecko.aiohttp.ClientSession", mock_cs):
        first = await get_crypto_info("BTC")
        second = await get_crypto_info("BTC")

    assert first == second
    session_obj = mock_cs.return_value.__aenter__.return_value
    # search_v2 returns everything in one call; second call is a cache hit.
    assert session_obj.get.call_count == 1


@pytest.mark.asyncio
async def test_get_crypto_info_no_coins_uses_yahoo_fallback():
    yahoo_fallback = {
        "price": 45000.0,
        "change_percent": 1.0,
        "volume": 10_000_000.0,
        "website": "https://finance.yahoo.com/quote/BTC-USD",
        "source": "yahoo",
    }
    with (
        patch(
            "ticker_price_data.coingecko.aiohttp.ClientSession",
            mock_session(mock_response(200, {"coins": []})),
        ),
        patch(
            "ticker_price_data.coingecko.get_stock_info",
            new=AsyncMock(return_value=yahoo_fallback),
        ),
    ):
        result = await get_crypto_info("BTC")

    assert result == yahoo_fallback


@pytest.mark.asyncio
async def test_get_crypto_info_http_error_uses_yahoo_fallback():
    yahoo_fallback = {
        "price": 45000.0,
        "change_percent": 1.0,
        "volume": 10_000_000.0,
        "website": "https://finance.yahoo.com/quote/BTC-USD",
        "source": "yahoo",
    }
    with (
        patch(
            "ticker_price_data.coingecko.aiohttp.ClientSession",
            mock_session(mock_response(429, {})),
        ),
        patch(
            "ticker_price_data.coingecko.get_stock_info",
            new=AsyncMock(return_value=yahoo_fallback),
        ),
    ):
        result = await get_crypto_info("BTC")

    assert result == yahoo_fallback


@pytest.mark.asyncio
async def test_get_crypto_info_uses_tradingview_when_yahoo_none():
    tv_fallback = {
        "price": 45100.0,
        "change_percent": 1.2,
        "volume": 20_000_000_000.0,
        "website": "https://www.tradingview.com/symbols/BINANCE-BTCUSDT/",
        "source": "tradingview",
    }
    with (
        patch(
            "ticker_price_data.coingecko.aiohttp.ClientSession",
            mock_session(mock_response(429, {})),
        ),
        patch(
            "ticker_price_data.coingecko.get_stock_info",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "ticker_price_data.coingecko.get_tradingview_quote",
            new=AsyncMock(return_value=tv_fallback),
        ),
    ):
        result = await get_crypto_info("BTC")

    assert result == tv_fallback


@pytest.mark.asyncio
async def test_get_crypto_info_exception_returns_none():
    with (
        patch(
            "ticker_price_data.coingecko.aiohttp.ClientSession",
            side_effect=Exception("Network error"),
        ),
        patch(
            "ticker_price_data.coingecko.get_stock_info",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = await get_crypto_info("BTC")

    assert result is None
