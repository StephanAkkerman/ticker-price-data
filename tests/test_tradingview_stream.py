import threading
import types

from ticker_price_data.tradingview_stream import RealTimePool, _QuoteRequest


def test_handle_raw_message_acknowledges_heartbeat_and_parses_json_frame():
    pool = object.__new__(RealTimePool)
    sent_messages: list[str] = []
    pool._socket = types.SimpleNamespace(send_raw=sent_messages.append)
    pool._pending = {}
    pool._lock = threading.Lock()
    pool._running = False

    pool._handle_raw_message("~h~6")
    assert sent_messages == ["~h~6"]

    request = _QuoteRequest(
        symbol="SPY",
        asset_hint="stock",
        timeout=1.0,
        quote_session="qs_test",
        chart_session="cs_test",
    )
    pool._pending["qs_test"] = request

    pool._handle_raw_message(
        '~m~74~m~{"m":"qsd","p":["qs_test",{"lp":123.45,"chp":1.25,"volume":10}]}'
    )

    assert request.error is None
    assert request.result is not None
    assert request.result["price"] == 123.45
    assert request.result["change_percent"] == 1.25
    assert request.result["volume"] == 10.0
    assert request.result["source"] == "tradingview"
