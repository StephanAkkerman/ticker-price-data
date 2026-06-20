from unittest.mock import AsyncMock, patch

import pytest
from helpers import mock_response, mock_session

import ticker_price_data.yahoo as yahoo_service
from ticker_price_data.yahoo import get_stock_info

YAHOO_RESPONSE = {
    "chart": {
        "result": [
            {
                "meta": {
                    "regularMarketPrice": 185.0,
                    "previousClose": 182.0,
                    "regularMarketVolume": 50_000_000,
                }
            }
        ]
    }
}


@pytest.fixture(autouse=True)
def _reset_yahoo_cache():
    with patch(
        "ticker_price_data.yahoo.get_tradingview_quote",
        new=AsyncMock(return_value=None),
    ):
        yahoo_service._reset_cache_for_tests()
        yield
        yahoo_service._reset_cache_for_tests()


@pytest.mark.asyncio
async def test_get_stock_info_success():
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, YAHOO_RESPONSE)),
    ):
        result = await get_stock_info("AAPL")

    assert result is not None
    assert result["price"] == 185.0
    assert abs(result["change_percent"] - ((185.0 - 182.0) / 182.0 * 100)) < 0.01
    assert result["volume"] == 50_000_000 * 185.0
    assert "yahoo" in result["website"]
    assert "AAPL" in result["website"]
    assert result["source"] == "yahoo"


@pytest.mark.asyncio
async def test_get_stock_info_zero_change_when_no_previous_close():
    data = {
        "chart": {
            "result": [
                {"meta": {"regularMarketPrice": 185.0, "regularMarketVolume": 1}}
            ]
        }
    }
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_stock_info("AAPL")

    assert result is not None
    assert result["change_percent"] == 0.0


@pytest.mark.asyncio
async def test_get_stock_info_missing_price_returns_none():
    data = {"chart": {"result": [{"meta": {"regularMarketPrice": None}}]}}
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_stock_info("AAPL")
    assert result is None


@pytest.mark.asyncio
async def test_get_stock_info_empty_result_returns_none():
    data = {"chart": {"result": None}}
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_stock_info("INVALID")
    assert result is None


@pytest.mark.asyncio
async def test_get_stock_info_http_error_returns_none():
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(404, {})),
    ):
        result = await get_stock_info("AAPL")
    assert result is None


@pytest.mark.asyncio
async def test_get_stock_info_uses_lookup_override_for_dxy():
    data = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 104.1,
                        "previousClose": 103.0,
                        "regularMarketVolume": 1,
                    }
                }
            ]
        }
    }
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_stock_info("DXY")

    assert result is not None
    assert "DX-Y.NYB" in result["website"]


@pytest.mark.asyncio
async def test_get_stock_info_exception_returns_none():
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        side_effect=Exception("Network error"),
    ):
        result = await get_stock_info("AAPL")
    assert result is None


@pytest.mark.asyncio
async def test_get_stock_info_uses_tradingview_fallback_when_yahoo_unavailable():
    tv_fallback = {
        "price": 185.5,
        "change_percent": 0.9,
        "volume": 12_000_000.0,
        "website": "https://www.tradingview.com/symbols/NASDAQ-AAPL/",
        "source": "tradingview",
    }

    with (
        patch(
            "ticker_price_data.yahoo.aiohttp.ClientSession",
            mock_session(mock_response(429, {})),
        ),
        patch(
            "ticker_price_data.yahoo.get_tradingview_quote",
            new=AsyncMock(return_value=tv_fallback),
        ),
    ):
        result = await get_stock_info("AAPL")

    assert result == tv_fallback


@pytest.mark.asyncio
async def test_session_field_present_during_regular_hours():
    with (
        patch(
            "ticker_price_data.yahoo.aiohttp.ClientSession",
            mock_session(mock_response(200, YAHOO_RESPONSE)),
        ),
        patch(
            "ticker_price_data.yahoo.get_us_stock_session",
            return_value="regular",
        ),
    ):
        result = await get_stock_info("AAPL")

    assert result is not None
    assert result["session"] == "regular"
    assert "extended_price" not in result
    assert "extended_change_percent" not in result


@pytest.mark.asyncio
async def test_after_hours_with_post_market_price():
    data = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 185.0,
                        "previousClose": 182.0,
                        "regularMarketVolume": 50_000_000,
                        "postMarketPrice": 186.5,
                    }
                }
            ]
        }
    }
    with (
        patch(
            "ticker_price_data.yahoo.aiohttp.ClientSession",
            mock_session(mock_response(200, data)),
        ),
        patch(
            "ticker_price_data.yahoo.get_us_stock_session",
            return_value="after-hours",
        ),
    ):
        result = await get_stock_info("AAPL")

    assert result is not None
    assert result["session"] == "after-hours"
    assert result["extended_price"] == 186.5
    expected_change = (186.5 - 185.0) / 185.0 * 100
    assert abs(result["extended_change_percent"] - expected_change) < 0.001


@pytest.mark.asyncio
async def test_after_hours_without_post_market_price():
    # Yahoo has no postMarketPrice yet (no after-hours trades)
    with (
        patch(
            "ticker_price_data.yahoo.aiohttp.ClientSession",
            mock_session(mock_response(200, YAHOO_RESPONSE)),
        ),
        patch(
            "ticker_price_data.yahoo.get_us_stock_session",
            return_value="after-hours",
        ),
    ):
        result = await get_stock_info("AAPL")

    assert result is not None
    assert result["session"] == "after-hours"
    assert "extended_price" not in result
    assert "extended_change_percent" not in result


@pytest.mark.asyncio
async def test_pre_market_with_pre_market_price():
    data = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 185.0,
                        "previousClose": 182.0,
                        "regularMarketVolume": 50_000_000,
                        "preMarketPrice": 184.0,
                    }
                }
            ]
        }
    }
    with (
        patch(
            "ticker_price_data.yahoo.aiohttp.ClientSession",
            mock_session(mock_response(200, data)),
        ),
        patch(
            "ticker_price_data.yahoo.get_us_stock_session",
            return_value="pre-market",
        ),
    ):
        result = await get_stock_info("AAPL")

    assert result is not None
    assert result["session"] == "pre-market"
    assert result["extended_price"] == 184.0
    expected_change = (184.0 - 185.0) / 185.0 * 100
    assert abs(result["extended_change_percent"] - expected_change) < 0.001


@pytest.mark.asyncio
async def test_pre_market_without_pre_market_price():
    with (
        patch(
            "ticker_price_data.yahoo.aiohttp.ClientSession",
            mock_session(mock_response(200, YAHOO_RESPONSE)),
        ),
        patch(
            "ticker_price_data.yahoo.get_us_stock_session",
            return_value="pre-market",
        ),
    ):
        result = await get_stock_info("AAPL")

    assert result is not None
    assert result["session"] == "pre-market"
    assert "extended_price" not in result
    assert "extended_change_percent" not in result
