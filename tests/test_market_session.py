from unittest.mock import patch

import pandas as pd
import pytest

from ticker_price_data.market_session import get_us_stock_session


def _ts(iso: str) -> pd.Timestamp:
    return pd.Timestamp(iso, tz="UTC")


# 2025-01-07: Tuesday, regular NYSE session
# EST = UTC-5 in January


def test_regular_session():
    # 15:00 UTC = 10:00 AM ET — well within regular hours
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-07 15:00:00")):
        assert get_us_stock_session() == "regular"


def test_pre_market():
    # 13:00 UTC = 8:00 AM ET — pre-market window
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-07 13:00:00")):
        assert get_us_stock_session() == "pre-market"


def test_pre_market_start_boundary():
    # 09:00 UTC = 4:00 AM ET — pre-market opens
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-07 09:00:00")):
        assert get_us_stock_session() == "pre-market"


def test_pre_market_end_boundary():
    # 14:29 UTC = 9:29 AM ET — one minute before regular open
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-07 14:29:00")):
        assert get_us_stock_session() == "pre-market"


def test_after_hours():
    # 22:00 UTC = 5:00 PM ET — after-hours window
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-07 22:00:00")):
        assert get_us_stock_session() == "after-hours"


def test_after_hours_start_boundary():
    # 21:00 UTC = 4:00 PM ET — after-hours opens
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-07 21:00:00")):
        assert get_us_stock_session() == "after-hours"


def test_after_hours_end_boundary_is_closed():
    # 2025-01-08 01:00 UTC = 8:00 PM ET 2025-01-07 — after-hours window ends
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-08 01:00:00")):
        assert get_us_stock_session() == "closed"


def test_closed_weekend():
    # 2025-01-11: Saturday
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-11 15:00:00")):
        assert get_us_stock_session() == "closed"


def test_closed_holiday():
    # 2025-07-04: Independence Day (Friday), 15:00 UTC = 11:00 AM ET
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-07-04 15:00:00")):
        assert get_us_stock_session() == "closed"


def test_closed_before_pre_market():
    # 07:00 UTC = 2:00 AM ET — too early for pre-market
    with patch("ticker_price_data.market_session._now_utc", return_value=_ts("2025-01-07 07:00:00")):
        assert get_us_stock_session() == "closed"
