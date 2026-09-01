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


def _idle_pool() -> RealTimePool:
    """A pool with no worker thread, ready for direct `_run` stepping."""
    pool = object.__new__(RealTimePool)
    pool._socket = None
    pool._commands = Queue()
    pool._pending = {}
    pool._lock = threading.Lock()
    pool._running = True
    pool._retry_delay = tvs._CONNECT_RETRY_DELAY_SECONDS
    return pool


def _request(**overrides) -> _QuoteRequest:
    defaults = {
        "symbol": "NASDAQ:AAPL",
        "asset_hint": "stock",
        "timeout": 6.0,
        "quote_session": "qs_test",
        "chart_session": "cs_test",
    }
    defaults.update(overrides)
    return _QuoteRequest(**defaults)


def test_has_waiters_is_false_when_nothing_is_queued_or_pending():
    assert _idle_pool()._has_waiters() is False


def test_has_waiters_sees_queued_and_pending_work():
    queued = _idle_pool()
    queued._commands.put(("add", _request()))
    assert queued._has_waiters() is True

    pending = _idle_pool()
    pending._pending["qs_test"] = _request()
    assert pending._has_waiters() is True


def test_worker_does_not_connect_while_idle(monkeypatch):
    """The bug: it reconnected on a timer regardless of demand.

    An unreachable TradingView therefore produced a connection attempt every
    second for the life of the process.
    """
    pool = _idle_pool()
    attempts = []
    monkeypatch.setattr(
        RealTimePool, "_connect_socket", lambda self: attempts.append(1) or False
    )

    # Stop the loop the first time it parks, so this cannot spin.
    def stop_and_sleep(_seconds):
        pool._running = False

    monkeypatch.setattr(tvs.time, "sleep", stop_and_sleep)
    pool._run()

    assert attempts == [], "worker opened a socket with no caller waiting"


def test_worker_connects_when_a_caller_is_waiting(monkeypatch):
    pool = _idle_pool()
    pool._commands.put(("add", _request()))

    attempts = []
    monkeypatch.setattr(
        RealTimePool, "_connect_socket", lambda self: attempts.append(1) or False
    )

    def stop_and_sleep(_seconds):
        pool._running = False

    monkeypatch.setattr(tvs.time, "sleep", stop_and_sleep)
    pool._run()

    assert attempts == [1]


def test_connect_failures_back_off_up_to_the_ceiling(monkeypatch):
    pool = _idle_pool()
    pool._commands.put(("add", _request()))

    monkeypatch.setattr(RealTimePool, "_connect_socket", lambda self: False)

    slept: list[float] = []

    def record(seconds):
        slept.append(seconds)
        if len(slept) >= 8:
            pool._running = False

    monkeypatch.setattr(tvs.time, "sleep", record)
    pool._run()

    assert slept[:4] == [1.0, 2.0, 4.0, 8.0], slept
    # And it stops growing rather than backing off forever.
    assert max(slept) <= tvs._MAX_CONNECT_RETRY_DELAY_SECONDS
    assert slept[-1] == tvs._MAX_CONNECT_RETRY_DELAY_SECONDS


def test_backoff_resets_after_a_successful_connect(monkeypatch):
    pool = _idle_pool()
    pool._commands.put(("add", _request()))
    pool._retry_delay = 16.0

    def connect(self):
        socket = MagicMock()
        socket.ws.recv.side_effect = tvs.WebSocketTimeoutException()
        self._socket = socket
        return True

    monkeypatch.setattr(RealTimePool, "_connect_socket", connect)

    # Leave the loop once the queued request has been handed to the socket.
    def stop(self, request):
        pool._running = False

    monkeypatch.setattr(RealTimePool, "_send_request_setup", stop)
    pool._run()

    assert pool._retry_delay == tvs._CONNECT_RETRY_DELAY_SECONDS


def test_abandoned_requests_are_dropped_so_the_worker_can_go_idle():
    """A timed-out caller must not leave the worker retrying forever."""
    pool = _idle_pool()

    abandoned = _request(quote_session="qs_gone")
    abandoned.event.set()  # get_quote_sync completed it on timeout
    waiting = _request(quote_session="qs_live")

    pool._commands.put(("add", abandoned))
    pool._commands.put(("add", waiting))

    pool._discard_abandoned_commands()

    remaining = []
    while not pool._commands.empty():
        remaining.append(pool._commands.get_nowait())

    assert [request.quote_session for _, request in remaining] == ["qs_live"]


def test_worker_goes_idle_once_every_caller_has_given_up(monkeypatch):
    pool = _idle_pool()
    abandoned = _request()
    abandoned.event.set()
    pool._commands.put(("add", abandoned))

    attempts = []
    monkeypatch.setattr(
        RealTimePool, "_connect_socket", lambda self: attempts.append(1) or False
    )

    sleeps: list[float] = []

    def record(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 3:
            pool._running = False

    monkeypatch.setattr(tvs.time, "sleep", record)
    pool._run()

    # No attempt at all: the abandoned command is discarded before the worker
    # decides whether a connection is warranted, so it parks straight away.
    assert attempts == []
    assert sleeps == [tvs._IDLE_POLL_SECONDS] * 3
