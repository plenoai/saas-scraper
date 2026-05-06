"""Notion connector — Chrome-driven sidebar walk and page-content scrape.

Notion's web app renders the sidebar as a virtualised tree, and pages
are inside an editable canvas with custom rendering. We don't try to
parse the block tree; we let the browser render the page and grab the
visible text via ``innerText``. That covers comments and UI-only blocks
the API hides.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

from playwright.async_api import Page

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import (
    BaseConnector,
    apply_name_filter,
    wait_for_signed_in_or_raise,
)
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry

_APP_SELECTOR = '.notion-sidebar, [class*="notion-sidebar"]'
_LOGIN_FORM_SELECTOR = 'form[data-testid="login-form"], a[href*="/login"]'
_PAGE_LINK_SELECTOR = '.notion-sidebar a[href^="/"], [class*="notion-sidebar"] a[href^="/"]'
_PAGE_BODY_SELECTOR = '.notion-page-content, [class*="notion-page-content"]'

_LOAD_TIMEOUT_MS = 20_000
_PAGE_TIMEOUT_MS = 10_000


class NotionConnector(BaseConnector):
    """Notion workspace scraper backed by Chrome.

    Construct via the registry: ``registry.create("notion",
    session=..., workspace="acme")``. ``workspace`` is the slug at
    ``notion.so/<workspace>``.
    """

    kind = "notion"

    def __init__(
        self,
        *,
        session: BrowserSession,
        workspace: str,
        source_id: str | None = None,
    ) -> None:
        super().__init__(session=session, source_id=source_id or f"notion:{workspace}")
        self.workspace = workspace
        self.base_url = "https://www.notion.so"

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        page = await self.session.new_page()
        try:
            await self._goto_workspace(page)
            pages = await self._list_pages(page)
        finally:
            await page.close()

        for p in pages:
            if not apply_name_filter(p["title"], filter):
                continue
            yield DocumentRef(
                source_id=self.id,
                source_kind=self.kind,
                path=f"page:{p['page_id']}",
                native_url=p["href"],
                content_type="text/html",
                metadata={"title": p["title"], "page_id": p["page_id"]},
            )

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        page = await self.session.new_page()
        try:
            url = ref.native_url or f"{self.base_url}{ref.path}"
            await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
            await page.wait_for_selector(_PAGE_BODY_SELECTOR, timeout=_PAGE_TIMEOUT_MS)
            text: str = await page.evaluate(
                f"""() => {{
                    const el = document.querySelector({_PAGE_BODY_SELECTOR!r});
                    return el ? el.innerText : "";
                }}"""
            )
        finally:
            await page.close()

        yield Document(ref=ref, text=text, fetched_at=datetime.now(UTC))

    async def _goto_workspace(self, page: Page) -> None:
        await page.goto(self.base_url + "/", wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
        await wait_for_signed_in_or_raise(
            page,
            app_selector=_APP_SELECTOR,
            login_selector=_LOGIN_FORM_SELECTOR,
            provider=f"notion workspace {self.workspace!r}",
            timeout_ms=_LOAD_TIMEOUT_MS,
        )

    async def _list_pages(self, page: Page) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await page.evaluate(
            f"""() => Array.from(document.querySelectorAll({_PAGE_LINK_SELECTOR!r}))
                .map(a => {{
                    // Notion page hrefs end with a 32-char hex id, optionally
                    // dash-separated. Capture the trailing 32 chars.
                    const href = a.getAttribute('href') || '';
                    const m = href.match(/([0-9a-f]{{32}})(?:\\?|$)/i);
                    return {{
                        href: a.href,
                        page_id: m ? m[1] : '',
                        title: (a.textContent || '').trim(),
                    }};
                }})
                .filter(p => p.page_id && p.title)"""
        )
        return result


registry.register("notion", NotionConnector)
