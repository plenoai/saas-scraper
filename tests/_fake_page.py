"""Shared fake Page helper used across connector unit tests.

Records ``goto`` and ``wait_for_selector`` calls so tests can assert on
which selectors / URLs the connector hit, and dispatches ``evaluate``
results from a substring-keyed dict so tests stay readable when JS
bodies evolve.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock


class FakeResponse:
    """Stand-in for playwright.async_api.Response. Carries a body."""

    def __init__(self, body: bytes = b"") -> None:
        self._body = body

    async def body(self) -> bytes:
        return self._body


class FakePage:
    def __init__(
        self,
        *,
        js_results: dict[str, Any] | None = None,
        wait_for_selector_handler: Callable[[str], Awaitable[Any]] | None = None,
        goto_response: FakeResponse | None = None,
    ) -> None:
        self.gotos: list[str] = []
        self.waited_for: list[str] = []
        self._js_results = js_results or {}
        self._wait_handler = wait_for_selector_handler
        self._goto_response = goto_response
        self.closed = False

    async def goto(self, url: str, **_: Any) -> FakeResponse | None:
        self.gotos.append(url)
        return self._goto_response

    async def wait_for_selector(self, selector: str, **_: Any) -> object:
        self.waited_for.append(selector)
        if self._wait_handler is not None:
            return await self._wait_handler(selector)
        # Default behaviour: any non-app-shell login selector pends so
        # the asyncio.wait() race in wait_for_signed_in_or_raise picks
        # the app shell selector. The login selector substring "signin",
        # "sign_in", "login", "loginform" all funnel here.
        if any(s in selector for s in ("signin", "sign_in", "login", "loginform")):
            await asyncio.sleep(60)
            return None
        return object()

    async def evaluate(self, js: str) -> Any:
        for key, value in self._js_results.items():
            if key in js:
                return value
        raise AssertionError(f"unexpected js evaluate: {js[:120]!r}")

    async def close(self) -> None:
        self.closed = True


def session_with_page(page: FakePage) -> Any:
    session = AsyncMock()
    session.new_page = AsyncMock(return_value=page)
    return session


def session_with_pages(pages: list[FakePage]) -> Any:
    """Returns a session that hands out pages in order across new_page() calls."""
    session = AsyncMock()
    session.new_page = AsyncMock(side_effect=pages)
    return session
