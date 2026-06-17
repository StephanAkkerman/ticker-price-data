"""TradingView websocket pool for quote lookups.

`RealTimeData` opens a websocket per instance. This module keeps one instance
alive and multiplexes symbol lookups over it so callers do not open a socket
per symbol.

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
import logging
import threading
import time
from dataclasses import dataclass, field
from queue import Empty, Queue
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:
    from tradingview_scraper.symbols.stream import RealTimeData
except Exception:  # pragma: no cover - optional dependency
    RealTimeData = None  # type: ignore


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
        if RealTimeData is None:
            raise RuntimeError("tradingview_scraper RealTimeData is not available")

        self._rtd = RealTimeData()
        self._commands: Queue[tuple[str, _QuoteRequest | None]] = Queue()
        self._pending: dict[str, _QuoteRequest] = {}
        self._lock = threading.Lock()
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _send_request_setup(self, request: _QuoteRequest) -> None:
        self._rtd._initialize_sessions(request.quote_session, request.chart_session)
        self._rtd._add_symbol_to_sessions(
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

    def _run(self) -> None:
        data_iter = self._rtd.get_data()
        while self._running:
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
                packet = next(data_iter)
            except StopIteration:
                break
            except Exception as exc:
                logger.debug("[tradingview] recv error: %r", exc)
                time.sleep(0.1)
                continue

            self._handle_packet(packet)

        try:
            self._rtd.ws.close()
        except Exception:
            pass

    def _request_session_ids(self) -> tuple[str, str]:
        return self._rtd.generate_session("qs_"), self._rtd.generate_session("cs_")

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
