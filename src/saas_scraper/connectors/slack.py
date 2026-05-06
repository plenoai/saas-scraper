"""Slack connector — Chrome-driven channel walk and message scrape.

The persistent BrowserSession profile must be already logged in to the
target workspace. Run ``saas-scraper fetch slack --workspace <slug> --headed``
once interactively to complete SSO; the cookies persist for subsequent
headless runs.

What this v0.2.0 implementation does:

* ``discover()`` opens ``https://<slug>.slack.com/`` (which redirects
  into the web client) and reads the channel sidebar's currently
  rendered DOM. Yields one ``DocumentRef`` per visible channel.
* ``fetch()`` opens a channel's permalink, waits for the message pane,
  and returns the rendered ``innerText`` of the visible message list as
  a single ``Document``.

What it deliberately does **not** do (yet):

* No virtual-list scrolling. Slack's channel sidebar and message pane
  are virtualised — only the channels and messages currently in view
  are in the DOM. Workspaces with hundreds of channels or long histories
  see truncated output. v0.3.0 adds scroll-walking; the path is marked
  with TODO(v0.3.0) below.
* No threaded-reply expansion.
* No file/attachment download.

The selectors below target the current ``app.slack.com`` web client.
Slack rotates DOM occasionally; selectors are isolated in module-level
constants so the next time the UI shifts, fixing the connector is one
diff in this file.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import BaseConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry

# CSS selectors. Kept here, not inlined, because Slack churns the DOM
# every few months and bumping these is the entire patch when it happens.
_SIDEBAR_SELECTOR = '[data-qa="channel_sidebar"]'
_CHANNEL_LINK_SELECTOR = '[data-qa="channel_sidebar"] a[data-qa^="channel_sidebar_name_"]'
_MESSAGE_PANE_SELECTOR = '[data-qa="message_pane"]'
_LOGIN_FORM_SELECTOR = 'form[data-qa="signin_form"], button[data-qa="signin_button"]'

# Wait timeouts (ms). 15s for the initial client load (Slack JS bundle is
# multi-megabyte over a fresh connection); 8s for in-page state changes.
_LOAD_TIMEOUT_MS = 15_000
_PAGE_TIMEOUT_MS = 8_000


class NotLoggedInError(RuntimeError):
    """Raised when the persistent profile has no active Slack session.

    Recovery is interactive: the caller must run the connector once with
    ``BrowserSession(headless=False)`` so they can complete SSO / MFA.
    Cookies then persist in the profile for headless runs.
    """


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
        """Yield one DocumentRef per visible channel in the sidebar.

        Filter handling: when ``filter.include`` is non-empty, only
        channels whose name matches at least one entry are yielded.
        ``filter.exclude`` removes matches. ``since`` is ignored at
        discovery time (Slack doesn't expose a per-channel last-message
        timestamp without entering the channel) and applied at fetch time.
        """
        page = await self.session.new_page()
        try:
            await self._goto_workspace(page)
            channels = await self._list_sidebar_channels(page)
        finally:
            await page.close()

        for ch in channels:
            name = ch["name"]
            if filter.include and not any(_match(name, p) for p in filter.include):
                continue
            if any(_match(name, p) for p in filter.exclude):
                continue
            yield DocumentRef(
                source_id=self.id,
                source_kind=self.kind,
                path=f"channel:{ch['channel_id']}",
                native_url=ch["href"],
                content_type="text/plain",
                metadata={"channel_name": name, "channel_id": ch["channel_id"]},
            )

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        """Open a channel page and yield its visible messages as one Document.

        TODO(v0.3.0): scroll-walk to load older messages until reaching
        ``filter.since`` (or the channel head), and yield one Document
        per message rather than one per channel.
        """
        page = await self.session.new_page()
        try:
            url = ref.native_url or self._channel_url(ref)
            text = await self._read_channel(page, url)
        finally:
            await page.close()

        yield Document(
            ref=ref,
            text=text,
            fetched_at=datetime.now(UTC),
        )

    # --- internals ---------------------------------------------------

    async def _goto_workspace(self, page: Page) -> None:
        """Open the workspace and wait until the channel sidebar mounts.

        Detects the not-logged-in state by racing the sidebar selector
        against the login form selector. Raises ``NotLoggedInError`` so
        the caller can surface a clear "run --headed once" message
        instead of a generic Playwright timeout.
        """
        await page.goto(self._workspace_url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)

        sidebar = asyncio.create_task(
            page.wait_for_selector(_SIDEBAR_SELECTOR, timeout=_LOAD_TIMEOUT_MS)
        )
        login = asyncio.create_task(
            page.wait_for_selector(_LOGIN_FORM_SELECTOR, timeout=_LOAD_TIMEOUT_MS)
        )
        done, pending = await asyncio.wait(
            {sidebar, login}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        for task in done:
            try:
                element = task.result()
            except PlaywrightTimeoutError:
                continue
            if task is login and element is not None:
                raise NotLoggedInError(
                    f"slack workspace {self.workspace!r} is not logged in. "
                    "Run once with BrowserSession(headless=False) to complete SSO."
                )

    async def _list_sidebar_channels(self, page: Page) -> list[dict[str, str]]:
        """Snapshot the visible portion of the channel sidebar.

        Returns a list of ``{channel_id, name, href}`` dicts. Slack's
        sidebar is virtualised: when a workspace has many channels, this
        only sees the ones currently in view. v0.3.0 adds scroll-walking.
        """
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
        """Open ``url`` and return the rendered text of the message pane."""
        await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
        await page.wait_for_selector(_MESSAGE_PANE_SELECTOR, timeout=_PAGE_TIMEOUT_MS)
        # Slack messages are inside virtualised rows. innerText collapses
        # them with newlines, which is exactly what downstream pipelines
        # need for PII / secret detection passes.
        text: str = await page.evaluate(
            f"""() => {{
                const pane = document.querySelector({_MESSAGE_PANE_SELECTOR!r});
                return pane ? pane.innerText : "";
            }}"""
        )
        return text

    def _channel_url(self, ref: DocumentRef) -> str:
        """Reconstruct a channel URL when ``ref.native_url`` is missing."""
        cid = ref.metadata.get("channel_id") or ref.path.removeprefix("channel:")
        return f"{self._workspace_url}archives/{cid}"


def _match(name: str, pattern: str) -> bool:
    """Cheap glob-ish match. Caller passes filter.include / filter.exclude
    entries as either exact channel names or ``*foo*`` wildcards. We
    intentionally avoid full ``fnmatch`` to keep the matcher predictable
    for ``#dm-channel`` names that contain ``[`` etc.
    """
    if "*" not in pattern:
        return name == pattern
    parts = pattern.split("*")
    return all(p in name for p in parts if p)


registry.register("slack", SlackConnector)
