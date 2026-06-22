from typing import Literal

import exchange_calendars as xcals
import pandas as pd

_US_PRE_MARKET_START = (4, 0)
_US_PRE_MARKET_END = (9, 30)
_US_AFTER_HOURS_START = (16, 0)
_US_AFTER_HOURS_END = (20, 0)

_calendar: xcals.ExchangeCalendar | None = None


def _get_calendar() -> xcals.ExchangeCalendar:
    global _calendar
    if _calendar is None:
        _calendar = xcals.get_calendar("XNYS")
    return _calendar


def _now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def get_us_stock_session() -> Literal["regular", "pre-market", "after-hours", "closed"]:
    """Return the current US stock market session.

    Returns
    -------
    str
        ``"regular"`` during NYSE/NASDAQ trading hours,
        ``"pre-market"`` 04:00–09:29 ET on trading days,
        ``"after-hours"`` 16:00–19:59 ET on trading days,
        ``"closed"`` otherwise (nights, weekends, holidays).
    """
    try:
        cal = _get_calendar()
        now = _now_utc()

        try:
            if cal.is_open_on_minute(now):
                return "regular"
        except Exception:
            pass

        eastern = now.tz_convert("America/New_York")
        h, m = eastern.hour, eastern.minute

        in_pre = _US_PRE_MARKET_START <= (h, m) < _US_PRE_MARKET_END
        in_ah = _US_AFTER_HOURS_START <= (h, m) < _US_AFTER_HOURS_END

        if in_pre or in_ah:
            try:
                if cal.is_session(str(eastern.date())):
                    return "pre-market" if in_pre else "after-hours"
            except Exception:
                pass
    except Exception:
        pass

    return "closed"
