"""Slack connector tests against a fake Page.

We can't drive a real Slack workspace from CI, and Playwright + Chromium
in CI is heavy. Instead we mock ``BrowserSession.new_page`` to return a
hand-rolled fake Page that satisfies just enough of the Playwright
contract to exercise ``SlackConnector.discover`` and ``SlackConnector.fetch``.

The fake captures the calls the connector makes (``goto``, ``evaluate``)
and verifies the connector's structural behaviour: which URLs it visits,
which selectors it queries, and how it shapes ``DocumentRef`` /
``Document`` from the JS results.

A real-browser smoke pass against ``app.slack.com`` is the user's job
(``saas-scraper fetch slack --workspace <slug> --headed``); the package
README documents the workflow.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock

import pytest

from saas_scraper.connectors.slack import NotLoggedInError, SlackConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter


class _FakePage:
    """Minimal stand-in for playwright.async_api.Page.

    Records ``goto`` and ``wait_for_selector`` calls so tests can assert
    on which selectors the connector polled. ``evaluate`` is dispatched
    against ``js_results`` keyed by the first 64 characters of the JS
    body — the connector's ``evaluate`` calls are a closed set, so this
    keeps the fake tiny without re-implementing a JS engine.
    """

    def __init__(
        self,
        *,
        js_results: dict[str, Any],
        wait_for_selector_handler: Callable[[str], Awaitable[Any]] | None = None,
    ) -> None:
        self.gotos: list[str] = []
        self.waited_for: list[str] = []
        self._js_results = js_results
        self._wait_handler = wait_for_selector_handler
        self.closed = False

    async def goto(self, url: str, **_: Any) -> None:
        self.gotos.append(url)

    async def wait_for_selector(self, selector: str, **_: Any) -> object:
        self.waited_for.append(selector)
        if self._wait_handler is not None:
            return await self._wait_handler(selector)
        # Default: sidebar resolves immediately; login pends so the
        # asyncio.wait race in _goto_workspace picks the sidebar.
        if "signin" in selector:
            import asyncio as _asyncio

            await _asyncio.sleep(60)
            return None
        return object()

    async def evaluate(self, js: str) -> Any:
        # Match by selector substring so tests stay readable when the
        # connector evolves its JS bodies.
        for key, value in self._js_results.items():
            if key in js:
                return value
        raise AssertionError(f"unexpected js evaluate: {js!r}")

    async def close(self) -> None:
        self.closed = True


def _session_with_page(page: _FakePage) -> Any:
    """Build a fake BrowserSession whose new_page() returns ``page``."""
    session = AsyncMock()
    session.new_page = AsyncMock(return_value=page)
    return session


@pytest.mark.asyncio
async def test_discover_yields_visible_channels() -> None:
    page = _FakePage(
        js_results={
            "channel_sidebar_name_": [
                {
                    "channel_id": "C01",
                    "name": "general",
                    "href": "https://acme.slack.com/archives/C01",
                },
                {
                    "channel_id": "C02",
                    "name": "random",
                    "href": "https://acme.slack.com/archives/C02",
                },
            ],
        }
    )
    session = _session_with_page(page)
    conn = SlackConnector(session=session, workspace="acme")

    refs = [r async for r in conn.discover(SourceFilter())]

    assert len(refs) == 2
    assert refs[0].source_kind == "slack"
    assert refs[0].path == "channel:C01"
    assert refs[0].metadata["channel_name"] == "general"
    assert refs[0].native_url == "https://acme.slack.com/archives/C01"
    assert page.gotos == ["https://acme.slack.com/"]
    assert page.closed


@pytest.mark.asyncio
async def test_discover_filters_include_exclude() -> None:
    page = _FakePage(
        js_results={
            "channel_sidebar_name_": [
                {"channel_id": "C01", "name": "eng-platform", "href": "u1"},
                {"channel_id": "C02", "name": "eng-frontend", "href": "u2"},
                {"channel_id": "C03", "name": "random", "href": "u3"},
                {"channel_id": "C04", "name": "eng-archive", "href": "u4"},
            ],
        }
    )
    session = _session_with_page(page)
    conn = SlackConnector(session=session, workspace="acme")

    refs = [
        r
        async for r in conn.discover(
            SourceFilter(include=("eng-*",), exclude=("*archive*",))
        )
    ]
    names = [r.metadata["channel_name"] for r in refs]
    assert names == ["eng-platform", "eng-frontend"]


@pytest.mark.asyncio
async def test_discover_raises_when_not_logged_in() -> None:
    """The login form selector winning the race surfaces NotLoggedInError."""

    async def wait_handler(selector: str) -> object:
        # Fire the login selector immediately, leave the sidebar pending
        # forever (asyncio.wait will pick the first to complete).
        if "signin" in selector:
            return object()
        # Block until cancelled by asyncio.wait's pending-task cancel.
        import asyncio as _asyncio

        await _asyncio.sleep(60)
        return None

    page = _FakePage(js_results={}, wait_for_selector_handler=wait_handler)
    session = _session_with_page(page)
    conn = SlackConnector(session=session, workspace="acme")

    with pytest.raises(NotLoggedInError, match="acme"):
        async for _ in conn.discover(SourceFilter()):
            pass
    assert page.closed


@pytest.mark.asyncio
async def test_fetch_returns_pane_text() -> None:
    page = _FakePage(
        js_results={"message_pane": "alice: hello\nbob: hi"},
    )
    session = _session_with_page(page)
    conn = SlackConnector(session=session, workspace="acme")

    ref = DocumentRef(
        source_id="slack:acme",
        source_kind="slack",
        path="channel:C01",
        native_url="https://acme.slack.com/archives/C01",
        metadata={"channel_id": "C01", "channel_name": "general"},
    )
    docs: list[Document] = [d async for d in conn.fetch(ref)]
    assert len(docs) == 1
    assert docs[0].text == "alice: hello\nbob: hi"
    assert docs[0].fetched_at is not None
    assert page.gotos == ["https://acme.slack.com/archives/C01"]


@pytest.mark.asyncio
async def test_fetch_reconstructs_url_when_native_missing() -> None:
    page = _FakePage(js_results={"message_pane": ""})
    session = _session_with_page(page)
    conn = SlackConnector(session=session, workspace="acme")

    ref = DocumentRef(
        source_id="slack:acme",
        source_kind="slack",
        path="channel:C99",
    )
    async def _drain(it: AsyncIterator[Document]) -> None:
        async for _ in it:
            pass

    await _drain(conn.fetch(ref))
    assert page.gotos == ["https://acme.slack.com/archives/C99"]
