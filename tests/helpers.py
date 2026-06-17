"""Shared aiohttp mock helpers for the test suite."""

from unittest.mock import AsyncMock, MagicMock


def mock_response(status: int, json_data: dict) -> MagicMock:
    """Return a mock async context manager that behaves like an aiohttp response."""
    response = MagicMock()
    response.status = status
    response.json = AsyncMock(return_value=json_data)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def mock_session(*responses) -> MagicMock:
    """Return a mock ``aiohttp.ClientSession`` async context manager.

    Each positional argument is a response context manager returned in order
    by successive calls to ``session.get()``.
    """
    session = MagicMock()
    if len(responses) == 1:
        session.get = MagicMock(return_value=responses[0])
    else:
        resp_iter = iter(responses)
        session.get = MagicMock(side_effect=lambda *a, **kw: next(resp_iter))
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=session)
