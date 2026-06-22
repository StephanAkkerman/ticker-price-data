import threading
import types
from queue import Queue
from unittest.mock import MagicMock

import ticker_price_data.tradingview_stream as tvs
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


def test_realtime_pool_init_defers_socket_connection(monkeypatch):
    def explode(*args, **kwargs):
        raise AssertionError("socket connection should not happen in __init__")

    monkeypatch.setattr(tvs.threading.Thread, "start", lambda self: None)
    monkeypatch.setattr(tvs, "_TradingViewSocket", explode)

    pool = RealTimePool()

    assert pool._socket is None


def test_tradingview_socket_sets_short_recv_timeout(monkeypatch):
    fake_ws = MagicMock()
    monkeypatch.setattr(tvs, "create_connection", MagicMock(return_value=fake_ws))

    socket = tvs._TradingViewSocket()

    tvs.create_connection.assert_called_once()
    fake_ws.settimeout.assert_called_once_with(0.05)
    assert socket.ws is fake_ws


def test_realtime_pool_close_is_null_safe():
    pool = object.__new__(RealTimePool)
    pool._socket = None
    pool._running = False
    pool._commands = Queue()
    pool._thread = types.SimpleNamespace(is_alive=lambda: False)

    pool.close()


def test_request_session_ids_use_class_helper(monkeypatch):
    pool = object.__new__(RealTimePool)
    generated = iter(["qs_alpha", "cs_beta"])
    monkeypatch.setattr(
        tvs._TradingViewSocket,
        "generate_session",
        staticmethod(lambda prefix: next(generated)),
    )

    assert pool._request_session_ids() == ("qs_alpha", "cs_beta")


def test_handle_socket_failure_closes_socket_and_fails_pending_requests():
    pool = object.__new__(RealTimePool)
    closed = []
    request = _QuoteRequest(
        symbol="SPY",
        asset_hint="stock",
        timeout=1.0,
        quote_session="qs_test",
        chart_session="cs_test",
    )
    pool._socket = types.SimpleNamespace(close=lambda: closed.append(True))
    pool._pending = {"qs_test": request}
    pool._lock = threading.Lock()
    pool._running = True

    pool._handle_socket_failure(ConnectionError("closed"))

    assert closed == [True]
    assert pool._socket is None
    assert request.error is not None
    assert request.result is None
