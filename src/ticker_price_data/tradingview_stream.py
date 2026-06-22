"""TradingView websocket pool for quote lookups.

This module keeps one websocket connection alive and multiplexes symbol
lookups over it so callers do not open a socket per symbol.

The pool is optimized for snapshot-style quote fetches:
- create one websocket connection per process
- create a quote/chart session per request
- wait for the quote session's `qsd` packet
- return a normalized quote dict

`symbol_resolved` packets are treated as setup metadata only; they are not the
final result.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import string
import threading
import time
from dataclasses import dataclass, field
from queue import Empty, Queue
from typing import Any, Optional

from websocket import (
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
    create_connection,
)

logger = logging.getLogger(__name__)

_HEARTBEAT_RE = re.compile(r"^~h~\d+$")
_MESSAGE_SPLIT_RE = re.compile(r"~m~\d+~m~")

_TV_WS_URL = "wss://data.tradingview.com/socket.io/websocket?from=screener%2F"
_CONNECT_RETRY_DELAY_SECONDS = 1.0
_TV_REQUEST_HEADERS = {
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9,fa;q=0.8",
    "Cache-Control": "no-cache",
    "Connection": "Upgrade",
    "Host": "data.tradingview.com",
    "Origin": "https://www.tradingview.com",
    "Pragma": "no-cache",
    "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
    "Upgrade": "websocket",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
    ),
}


class _TradingViewSocket:
    def __init__(self) -> None:
        self.ws = create_connection(
            _TV_WS_URL, headers=_TV_REQUEST_HEADERS, timeout=1.0
        )
        self.ws.settimeout(0.05)

    @staticmethod
    def generate_session(prefix: str) -> str:
        random_string = "".join(
            random.choice(string.ascii_lowercase) for _ in range(12)
        )
        return prefix + random_string

    @staticmethod
    def _prepend_header(message: str) -> str:
        return f"~m~{len(message)}~m~{message}"

    @staticmethod
    def _construct_message(func: str, param_list: list) -> str:
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def send_message(self, func: str, args: list) -> None:
        message = self._prepend_header(self._construct_message(func, args))
        self.ws.send(message)

    def send_raw(self, message: str) -> None:
        self.ws.send(message)

    def _get_quote_fields(self) -> list[str]:
        return [
            "ch",
            "chp",
            "current_session",
            "description",
            "local_description",
            "language",
            "exchange",
            "fractional",
            "is_tradable",
            "lp",
            "lp_time",
            "minmov",
            "minmove2",
            "original_name",
            "pricescale",
            "pro_name",
            "short_name",
            "type",
            "update_mode",
            "volume",
            "currency_code",
            "rchp",
            "rtc",
        ]

    def _initialize_sessions(self, quote_session: str, chart_session: str) -> None:
        self.send_message("set_auth_token", ["unauthorized_user_token"])
        self.send_message("set_locale", ["en", "US"])
        self.send_message("chart_create_session", [chart_session, ""])
        self.send_message("quote_create_session", [quote_session])
        self.send_message(
            "quote_set_fields", [quote_session, *self._get_quote_fields()]
        )
        self.send_message("quote_hibernate_all", [quote_session])

    def _add_symbol_to_sessions(
        self, quote_session: str, chart_session: str, exchange_symbol: str
    ) -> None:
        resolve_symbol = json.dumps({"adjustment": "splits", "symbol": exchange_symbol})
        self.send_message("quote_add_symbols", [quote_session, f"={resolve_symbol}"])
        self.send_message(
            "resolve_symbol", [chart_session, "sds_sym_1", f"={resolve_symbol}"]
        )
        self.send_message(
            "create_series", [chart_session, "sds_1", "s1", "sds_sym_1", "1", 10, ""]
        )
        self.send_message("quote_fast_symbols", [quote_session, exchange_symbol])
        self.send_message(
            "create_study",
            [
                chart_session,
                "st1",
                "st1",
                "sds_1",
                "Volume@tv-basicstudies-246",
                {"length": 20, "col_prev_close": "false"},
            ],
        )
        self.send_message("quote_hibernate_all", [quote_session])

    def close(self) -> None:
        try:
            self.ws.close()
        except Exception:
            pass


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    if ":" in s:
        return s
    return s


def _extract_quote_payload(packet: Any) -> Optional[dict[str, Any]]:
    """Extract a nested quote-like dict from a TradingView packet.

    TradingView packet payloads differ by message type and library version.
    We look for a dict that contains any standard quote keys such as `lp` or
    `chp` and return that dict.
    """

    if isinstance(packet, dict):
        quote_keys = {
            "lp",
            "ch",
            "chp",
            "rchp",
            "lp_time",
            "current_session",
            "exchange",
        }
        if quote_keys.intersection(packet.keys()):
            return packet

        for value in packet.values():
            found = _extract_quote_payload(value)
            if found is not None:
                return found

    elif isinstance(packet, list):
        for item in packet:
            found = _extract_quote_payload(item)
            if found is not None:
                return found

    return None


@dataclass
class _QuoteRequest:
    symbol: str
    asset_hint: str | None
    timeout: float
    quote_session: str
    chart_session: str
    event: threading.Event = field(default_factory=threading.Event)
    result: dict[str, Any] | None = None
    error: Exception | None = None


class RealTimePool:
    """Single TradingView websocket connection shared across quote requests."""

    def __init__(self) -> None:
        self._socket: _TradingViewSocket | None = None
        self._commands: Queue[tuple[str, _QuoteRequest | None]] = Queue()
        self._pending: dict[str, _QuoteRequest] = {}
        self._lock = threading.Lock()
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _connect_socket(self) -> bool:
        if self._socket is not None:
            return True

        try:
            self._socket = _TradingViewSocket()
            return True
        except Exception as exc:
            logger.warning("[tradingview] socket connect failed: %r", exc)
            self._socket = None
            return False

    def _close_socket(self) -> None:
        socket = self._socket
        self._socket = None
        if socket is None:
            return

        try:
            socket.close()
        except Exception:
            pass

    def _fail_pending_requests(self, error: Exception) -> None:
        with self._lock:
            pending_requests = list(self._pending.values())
            self._pending.clear()

        for request in pending_requests:
            self._finish_request(request, error=error)

    def _handle_socket_failure(self, exc: Exception) -> None:
        if not self._running:
            return

        logger.warning("[tradingview] connection lost, reconnecting: %r", exc)
        self._close_socket()
        self._fail_pending_requests(ConnectionError(str(exc)))

    def _send_request_setup(self, request: _QuoteRequest) -> None:
        socket = self._socket
        if socket is None:
            raise RuntimeError("TradingView socket is not connected")

        socket._initialize_sessions(request.quote_session, request.chart_session)
        socket._add_symbol_to_sessions(
            request.quote_session,
            request.chart_session,
            request.symbol,
        )

    def _build_quote(
        self, request: _QuoteRequest, payload: dict[str, Any]
    ) -> dict[str, Any]:
        price = _to_float(
            payload.get("lp") or payload.get("close") or payload.get("price")
        )
        change = _to_float(payload.get("ch"))
        change_percent = _to_float(
            payload.get("chp") or payload.get("rchp") or payload.get("change_percent")
        )
        volume = _to_float(payload.get("volume"))
        if volume is not None and volume > 1e20:
            volume = 0.0

        website_symbol = request.symbol.replace(":", "-")
        resolved_symbol = (
            str(
                payload.get("full_name")
                or payload.get("pro_name")
                or payload.get("short_name")
                or request.symbol
            )
            .upper()
            .replace(":", "-")
        )

        return {
            "symbol": request.symbol,
            "resolved_symbol": resolved_symbol,
            "price": price,
            "change": change,
            "change_percent": change_percent if change_percent is not None else 0.0,
            "volume": volume if volume is not None else 0.0,
            "website": f"https://www.tradingview.com/symbols/{website_symbol}/",
            "source": "tradingview",
            "raw": payload,
        }

    def _finish_request(
        self,
        request: _QuoteRequest,
        result: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        request.result = result
        request.error = error
        request.event.set()
        with self._lock:
            self._pending.pop(request.quote_session, None)

    def _handle_packet(self, packet: Any) -> None:
        if not isinstance(packet, dict):
            return

        packet_type = str(packet.get("m") or "")
        params = packet.get("p")
        if not isinstance(params, list) or not params:
            return

        quote_session = str(params[0])
        with self._lock:
            request = self._pending.get(quote_session)

        if request is None:
            return

        # `symbol_resolved` is setup metadata only. Wait for quote data.
        if packet_type == "symbol_resolved":
            return

        payload = _extract_quote_payload(params[1:])
        if payload is None:
            return

        price = _to_float(
            payload.get("lp") or payload.get("close") or payload.get("price")
        )
        if price is None:
            return

        result = self._build_quote(request, payload)
        self._finish_request(request, result=result)

    def _handle_raw_message(self, raw_message: Any) -> None:
        if not isinstance(raw_message, str):
            return

        if _HEARTBEAT_RE.fullmatch(raw_message):
            try:
                socket = self._socket
                if socket is not None:
                    socket.send_raw(raw_message)
            except Exception as exc:
                logger.debug("[tradingview] heartbeat ack failed: %r", exc)
            return

        for item in (part for part in _MESSAGE_SPLIT_RE.split(raw_message) if part):
            if _HEARTBEAT_RE.fullmatch(item):
                try:
                    socket = self._socket
                    if socket is not None:
                        socket.send_raw(item)
                except Exception as exc:
                    logger.debug("[tradingview] heartbeat ack failed: %r", exc)
                continue

            if not item.lstrip().startswith(("{", "[")):
                logger.debug("[tradingview] skipping non-JSON frame: %r", item)
                continue

            try:
                packet = json.loads(item)
            except json.JSONDecodeError as exc:
                logger.debug(
                    "[tradingview] malformed JSON frame skipped: %r (%s)", item, exc
                )
                continue

            self._handle_packet(packet)

    def _run(self) -> None:
        while self._running:
            if self._socket is None:
                if not self._connect_socket():
                    time.sleep(_CONNECT_RETRY_DELAY_SECONDS)
                    continue

            try:
                cmd, request = self._commands.get(timeout=0.05)
            except Empty:
                cmd = None
                request = None

            if cmd == "add" and request is not None:
                try:
                    with self._lock:
                        self._pending[request.quote_session] = request
                    self._send_request_setup(request)
                except Exception as exc:
                    logger.debug(
                        "[tradingview] request setup failed for %s: %r",
                        request.symbol,
                        exc,
                    )
                    self._finish_request(request, error=exc)

            try:
                socket = self._socket
                if socket is None:
                    continue
                raw_message = socket.ws.recv()
            except WebSocketTimeoutException:
                continue
            except WebSocketConnectionClosedException as exc:
                self._handle_socket_failure(exc)
                time.sleep(_CONNECT_RETRY_DELAY_SECONDS)
                continue
            except Exception as exc:
                if not self._running:
                    break
                self._handle_socket_failure(exc)
                time.sleep(_CONNECT_RETRY_DELAY_SECONDS)
                continue

            try:
                self._handle_raw_message(raw_message)
            except Exception as exc:
                logger.debug("[tradingview] message handling error: %r", exc)
                time.sleep(0.1)
                continue

        try:
            self._close_socket()
        except Exception:
            pass

    def _request_session_ids(self) -> tuple[str, str]:
        return _TradingViewSocket.generate_session(
            "qs_"
        ), _TradingViewSocket.generate_session("cs_")

    def get_quote_sync(
        self, symbol: str, asset_hint: str | None = None, timeout: float = 6.0
    ) -> dict[str, Any] | None:
        """Blocking quote lookup using the shared websocket."""

        normalized = _normalize_symbol(symbol)
        if not normalized:
            return None

        quote_session, chart_session = self._request_session_ids()
        request = _QuoteRequest(
            symbol=normalized,
            asset_hint=asset_hint,
            timeout=timeout,
            quote_session=quote_session,
            chart_session=chart_session,
        )

        self._commands.put(("add", request))
        if not request.event.wait(timeout):
            self._finish_request(
                request,
                error=TimeoutError(
                    f"Timed out waiting for TradingView quote: {normalized}"
                ),
            )

        if request.error is not None:
            return None
        return request.result

    async def get_quote(
        self, symbol: str, asset_hint: str | None = None, timeout: float = 6.0
    ) -> dict[str, Any] | None:
        return await asyncio.to_thread(self.get_quote_sync, symbol, asset_hint, timeout)

    def close(self) -> None:
        self._running = False
        try:
            self._commands.put(("stop", None))
        except Exception:
            pass
        try:
            if self._thread.is_alive():
                self._thread.join(timeout=1.0)
        except Exception:
            pass


_SHARED_POOL: RealTimePool | None = None
_SHARED_POOL_LOCK = threading.Lock()


def get_shared_pool() -> RealTimePool:
    """Return a lazily-created process-wide pool instance."""

    global _SHARED_POOL
    with _SHARED_POOL_LOCK:
        if _SHARED_POOL is None:
            _SHARED_POOL = RealTimePool()
        return _SHARED_POOL


def close_shared_pool() -> None:
    """Close and clear the shared pool instance, if one exists."""

    global _SHARED_POOL
    with _SHARED_POOL_LOCK:
        if _SHARED_POOL is not None:
            _SHARED_POOL.close()
            _SHARED_POOL = None


__all__ = ["RealTimePool", "get_shared_pool", "close_shared_pool"]
