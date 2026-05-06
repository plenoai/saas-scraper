"""Confluence (Atlassian Cloud) connector — Chrome-driven page-tree walk.

``discover()`` opens the space's page tree and enumerates pages.
``fetch()`` opens a page and returns the rendered ``innerText`` of the
content area.
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

_PAGE_TREE_SELECTOR = (
    '[data-testid="page-tree"], '
    '[data-testid="space-navigation"]'
)
_LOGIN_FORM_SELECTOR = 'form[name="loginform"], #login-form'
_PAGE_LINK_SELECTOR = (
    '[data-testid="page-tree"] a[href*="/wiki/spaces/"], '
    '[data-testid="space-navigation"] a[href*="/wiki/spaces/"]'
)
_PAGE_BODY_SELECTOR = (
    '[data-testid="page-content"], '
    '#main-content, '
    '.wiki-content'
)

_LOAD_TIMEOUT_MS = 20_000
_PAGE_TIMEOUT_MS = 10_000


class ConfluenceConnector(BaseConnector):
    """Confluence Cloud scraper backed by Chrome.

    Construct via the registry: ``registry.create("confluence",
    session=..., site="acme", space="ENG")``.
    """

    kind = "confluence"

    def __init__(
        self,
        *,
        session: BrowserSession,
        site: str,
        space: str | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(session=session, source_id=source_id or f"confluence:{site}")
        self.site = site
        self.space = space
        self.base_url = f"https://{site}.atlassian.net/wiki"

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        page = await self.session.new_page()
        try:
            await self._goto_space(page)
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
            url = ref.native_url or f"{self.base_url}/spaces/{self.space or ''}/overview"
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

    async def _goto_space(self, page: Page) -> None:
        url = (
            f"{self.base_url}/spaces/{self.space}/overview"
            if self.space
            else f"{self.base_url}/home"
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
        await wait_for_signed_in_or_raise(
            page,
            app_selector=_PAGE_TREE_SELECTOR,
            login_selector=_LOGIN_FORM_SELECTOR,
            provider=f"confluence site {self.site!r}",
            timeout_ms=_LOAD_TIMEOUT_MS,
        )

    async def _list_pages(self, page: Page) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await page.evaluate(
            f"""() => Array.from(document.querySelectorAll({_PAGE_LINK_SELECTOR!r}))
                .map(a => {{
                    const href = a.href;
                    const m = href.match(/\\/pages\\/(\\d+)/);
                    return {{
                        href,
                        page_id: m ? m[1] : '',
                        title: (a.textContent || '').trim(),
                    }};
                }})
                .filter(p => p.page_id && p.title)"""
        )
        return result


registry.register("confluence", ConfluenceConnector)
