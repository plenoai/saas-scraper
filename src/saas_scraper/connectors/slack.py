"""Slack connector — Chrome-driven channel walk and message scrape.

The persistent BrowserSession profile must be already logged in to the
target workspace. Run ``saas-scraper fetch slack --workspace <slug> --headed``
once interactively to complete SSO; the cookies persist for subsequent
headless runs.

What this implementation does:

* ``discover()`` opens ``https://<slug>.slack.com/`` (which redirects
  into the web client) and reads the channel sidebar's currently
  rendered DOM. Yields one ``DocumentRef`` per visible channel.
* ``fetch()`` opens a channel's permalink, waits for the message pane,
  and returns the rendered ``innerText`` of the visible message list as
  a single ``Document``.

What it deliberately does **not** do (yet):

* No virtual-list scrolling. v0.4.0 adds scroll-walking.
* No threaded-reply expansion.
* No file/attachment download.

The selectors below target the current ``app.slack.com`` web client.
Slack rotates DOM occasionally; selectors are isolated in module-level
constants so the next time the UI shifts, fixing the connector is one
diff in this file.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

from playwright.async_api import Page

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import (
    BaseConnector,
    NotLoggedInError,
    apply_name_filter,
    wait_for_signed_in_or_raise,
)
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry

# Re-export so existing callers `from saas_scraper.connectors.slack import
# NotLoggedInError` keep working after the helper moved to _base.
__all__ = ["NotLoggedInError", "SlackConnector"]

# CSS selectors. Kept here, not inlined, because Slack churns the DOM
# every few months and bumping these is the entire patch when it happens.
_SIDEBAR_SELECTOR = '[data-qa="channel_sidebar"]'
_CHANNEL_LINK_SELECTOR = '[data-qa="channel_sidebar"] a[data-qa^="channel_sidebar_name_"]'
_MESSAGE_PANE_SELECTOR = '[data-qa="message_pane"]'
_LOGIN_FORM_SELECTOR = 'form[data-qa="signin_form"], button[data-qa="signin_button"]'

_LOAD_TIMEOUT_MS = 15_000
_PAGE_TIMEOUT_MS = 8_000


class SlackConnector(BaseConnector):
    """Slack workspace scraper backed by Chrome.

    Construct via the registry: ``registry.create("slack", session=...,
    workspace="acme")``. The ``workspace`` slug is the subdomain segment
    of the Slack URL (``acme`` for ``acme.slack.com``).
    """

    kind = "slack"

    def __init__(
        self,
        *,
        session: BrowserSession,
        workspace: str,
        source_id: str | None = None,
    ) -> None:
        super().__init__(session=session, source_id=source_id or f"slack:{workspace}")
        self.workspace = workspace
        self._workspace_url = f"https://{workspace}.slack.com/"

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        """Yield one DocumentRef per visible channel in the sidebar."""
        page = await self.session.new_page()
        try:
            await self._goto_workspace(page)
            channels = await self._list_sidebar_channels(page)
        finally:
            await page.close()

        for ch in channels:
            if not apply_name_filter(ch["name"], filter):
                continue
            yield DocumentRef(
                source_id=self.id,
                source_kind=self.kind,
                path=f"channel:{ch['channel_id']}",
                native_url=ch["href"],
                content_type="text/plain",
                metadata={"channel_name": ch["name"], "channel_id": ch["channel_id"]},
            )

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        """Open a channel page and yield its visible messages as one Document."""
        page = await self.session.new_page()
        try:
            url = ref.native_url or self._channel_url(ref)
            text = await self._read_channel(page, url)
        finally:
            await page.close()

        yield Document(ref=ref, text=text, fetched_at=datetime.now(UTC))

    # --- internals ---------------------------------------------------

    async def _goto_workspace(self, page: Page) -> None:
        await page.goto(self._workspace_url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
        await wait_for_signed_in_or_raise(
            page,
            app_selector=_SIDEBAR_SELECTOR,
            login_selector=_LOGIN_FORM_SELECTOR,
            provider=f"slack workspace {self.workspace!r}",
            timeout_ms=_LOAD_TIMEOUT_MS,
        )

    async def _list_sidebar_channels(self, page: Page) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await page.evaluate(
            f"""() => Array.from(document.querySelectorAll({_CHANNEL_LINK_SELECTOR!r}))
                .map(a => ({{
                    channel_id: (a.getAttribute('data-qa') || '').replace('channel_sidebar_name_', ''),
                    name: (a.textContent || '').trim(),
                    href: a.href,
                }}))
                .filter(c => c.channel_id && c.name)"""
        )
        return result

    async def _read_channel(self, page: Page, url: str) -> str:
        await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
        await page.wait_for_selector(_MESSAGE_PANE_SELECTOR, timeout=_PAGE_TIMEOUT_MS)
        text: str = await page.evaluate(
            f"""() => {{
                const pane = document.querySelector({_MESSAGE_PANE_SELECTOR!r});
                return pane ? pane.innerText : "";
            }}"""
        )
        return text

    def _channel_url(self, ref: DocumentRef) -> str:
        cid = ref.metadata.get("channel_id") or ref.path.removeprefix("channel:")
        return f"{self._workspace_url}archives/{cid}"


registry.register("slack", SlackConnector)
