from unittest.mock import MagicMock, patch

import pytest

from ticker_price_data import tradingview_quote as tv


def test_pick_best_row_prefers_crypto_when_hint_crypto():
    rows = [
        {"symbol": "BTCUSD", "type": "crypto", "close": 100.0, "volume": 5.0},
        {"symbol": "BTC", "type": "stock", "close": 50.0, "volume": 5.0},
    ]
    best = tv._pick_best_row(rows, "crypto")
    assert best["symbol"] == "BTCUSD"


def test_pick_best_row_skips_non_positive_price():
    rows = [
        {"symbol": "AAA", "type": "stock", "close": 0.0, "volume": 1.0},
        {"symbol": "BBB", "type": "stock", "close": 12.0, "volume": 1.0},
    ]
    best = tv._pick_best_row(rows, "stock")
    assert best["symbol"] == "BBB"


def test_pick_best_row_exact_full_symbol_match_wins():
    rows = [
        {"symbol": "OTC:PCCYF", "type": "stock", "close": 1.0, "volume": 1.0},
        {"symbol": "USI:PCC", "type": "stock", "close": 2.0, "volume": 1.0},
    ]
    best = tv._pick_best_row(rows, "stock", requested_symbol_full="USI:PCC")
    assert best["symbol"] == "USI:PCC"


def test_pick_best_row_qualified_symbol_no_match_returns_none():
    rows = [{"symbol": "OTC:PCCYF", "type": "stock", "close": 1.0, "volume": 1.0}]
    best = tv._pick_best_row(rows, "stock", requested_symbol_full="USI:PCC")
    assert best is None


@pytest.mark.asyncio
async def test_get_tradingview_quote_normalizes_scraper_row():
    fake_markets = MagicMock()
    fake_markets.scrape.return_value = {
        "data": [
            {
                "symbol": "AMEX:SPY",
                "type": "stock",
                "close": 500.5,
                "change": 1.25,
                "volume": 1000.0,
            }
        ]
    }
    fake_module = MagicMock()
    fake_module.SymbolMarkets.return_value = fake_markets

    with patch.dict(
        "sys.modules",
        {"tradingview_scraper.symbols.symbol_markets": fake_module},
    ):
        quote = await tv.get_tradingview_quote(
            "SPY", asset_hint="stock", prefer_realtime=False
        )

    assert quote["price"] == 500.5
    assert quote["change_percent"] == 1.25
    assert quote["volume"] == 1000.0
    assert quote["source"] == "tradingview"


@pytest.mark.asyncio
async def test_get_tradingview_quote_empty_symbol_returns_none():
    assert await tv.get_tradingview_quote("", prefer_realtime=False) is None
