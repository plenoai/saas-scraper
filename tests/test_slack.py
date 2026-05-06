"""Slack connector tests against a fake Page (see tests/_fake_page.py)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from saas_scraper.connectors.slack import NotLoggedInError, SlackConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from tests._fake_page import FakePage as _FakePage
from tests._fake_page import session_with_page as _session_with_page


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
