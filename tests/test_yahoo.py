from unittest.mock import AsyncMock, patch

import pytest
from helpers import mock_response, mock_session

import ticker_price_data.yahoo as yahoo_service
from ticker_price_data.yahoo import get_price_history, get_stock_info

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
async def test_inject_session_skips_none_payload():
    # Negative cache: second lookup for an invalid symbol must return None, not TypeError
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(404, {})),
    ):
        first = await get_stock_info("INVALID_XYZ")
        assert first is None
        # Cache now holds None for this symbol; second call must not raise
        second = await get_stock_info("INVALID_XYZ")
        assert second is None


@pytest.mark.asyncio
async def test_inject_session_skips_non_yahoo_payload():
    # TradingView fallback cached then served on second call must not gain session field
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
            mock_session(mock_response(429, {}), mock_response(429, {})),
        ),
        patch(
            "ticker_price_data.yahoo.get_tradingview_quote",
            new=AsyncMock(return_value=tv_fallback),
        ),
    ):
        first = await get_stock_info("AAPL")
        assert first == tv_fallback
        assert "session" not in first
        second = await get_stock_info("AAPL")
        assert second == tv_fallback
        assert "session" not in second


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
                    },
                    "indicators": {"quote": [{"close": [184.0, 184.5, 186.5]}]},
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
async def test_after_hours_no_extended_price_when_candle_matches_regular():
    # After-hours session but no extended-hours trades — last candle == regularMarketPrice
    data = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 185.0,
                        "previousClose": 182.0,
                        "regularMarketVolume": 50_000_000,
                    },
                    "indicators": {"quote": [{"close": [184.0, 185.0, 185.0]}]},
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
                    },
                    "indicators": {"quote": [{"close": [183.0, 183.5, 184.0]}]},
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
async def test_pre_market_no_extended_price_when_candle_matches_regular():
    data = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 185.0,
                        "previousClose": 182.0,
                        "regularMarketVolume": 50_000_000,
                    },
                    "indicators": {"quote": [{"close": [185.0, 185.0, 185.0]}]},
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
    assert "extended_price" not in result
    assert "extended_change_percent" not in result


@pytest.mark.asyncio
async def test_get_price_history_success():
    data = {
        "chart": {
            "result": [
                {
                    "timestamp": [1700000000, 1700000060, 1700000120],
                    "indicators": {
                        "quote": [
                            {
                                "close": [185.0, 185.5, 186.0],
                                "high": [185.2, 185.7, 186.2],
                                "low": [184.8, 185.3, 185.8],
                            }
                        ]
                    },
                }
            ]
        }
    }
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_price_history("AAPL")

    assert result is not None
    assert len(result) == 3
    assert result[0]["close"] == 185.0
    assert result[0]["high"] == 185.2
    assert result[0]["low"] == 184.8
    assert result[-1]["close"] == 186.0
    # 1m is intraday: "t" carries a full timestamp, not just a date.
    assert "T" in result[0]["t"]


@pytest.mark.asyncio
async def test_get_price_history_missing_symbol_returns_none():
    result = await get_price_history("")
    assert result is None


@pytest.mark.asyncio
async def test_get_price_history_http_error_returns_none():
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(404, {})),
    ):
        result = await get_price_history("AAPL")
    assert result is None


@pytest.mark.asyncio
async def test_get_price_history_exception_returns_none():
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        side_effect=Exception("Network error"),
    ):
        result = await get_price_history("AAPL")
    assert result is None


@pytest.mark.asyncio
async def test_get_price_history_uses_lookup_override_for_dxy():
    data = {
        "chart": {
            "result": [
                {
                    "timestamp": [1700000000],
                    "indicators": {"quote": [{"close": [104.1]}]},
                }
            ]
        }
    }
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_price_history("DXY")

    assert result is not None
    assert result[0]["close"] == 104.1


@pytest.mark.asyncio
async def test_get_price_history_falls_back_through_lookup_candidates():
    data = {
        "chart": {
            "result": [
                {
                    "timestamp": [1700000000],
                    "indicators": {"quote": [{"close": [104.1]}]},
                }
            ]
        }
    }
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        # DX-Y.NYB (the override) fails, DXY (the plain symbol) succeeds.
        mock_session(mock_response(404, {}), mock_response(200, data)),
    ):
        result = await get_price_history("DXY")

    assert result is not None
    assert result[0]["close"] == 104.1


@pytest.mark.asyncio
async def test_get_price_history_daily_interval_uses_date_only():
    data = {
        "chart": {
            "result": [
                {
                    "timestamp": [1700000000],
                    "indicators": {"quote": [{"close": [185.0]}]},
                }
            ]
        }
    }
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_price_history("AAPL", range_="1y", interval="1d")

    assert result is not None
    assert "T" not in result[0]["t"]


@pytest.mark.asyncio
async def test_get_price_history_skips_null_closes():
    data = {
        "chart": {
            "result": [
                {
                    "timestamp": [1700000000, 1700000060, 1700000120],
                    "indicators": {"quote": [{"close": [185.0, None, 186.0]}]},
                }
            ]
        }
    }
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_price_history("AAPL")

    assert result is not None
    assert len(result) == 2
    assert [p["close"] for p in result] == [185.0, 186.0]


@pytest.mark.asyncio
async def test_get_price_history_empty_result_returns_none():
    data = {"chart": {"result": None}}
    with patch(
        "ticker_price_data.yahoo.aiohttp.ClientSession",
        mock_session(mock_response(200, data)),
    ):
        result = await get_price_history("INVALID")
    assert result is None


@pytest.mark.asyncio
async def test_closed_session_shows_extended_price_from_candles():
    # Simulates weekend: session="closed" but last candle carries Friday's after-hours price
    data = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 185.0,
                        "previousClose": 182.0,
                        "regularMarketVolume": 50_000_000,
                    },
                    "indicators": {"quote": [{"close": [184.0, 185.5, 186.2]}]},
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
            return_value="closed",
        ),
    ):
        result = await get_stock_info("MU")

    assert result is not None
    assert result["session"] == "closed"
    assert result["extended_price"] == 186.2
    expected_change = (186.2 - 185.0) / 185.0 * 100
    assert abs(result["extended_change_percent"] - expected_change) < 0.001
