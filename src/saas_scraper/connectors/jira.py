"""Jira (Atlassian Cloud) connector — Chrome-driven issue walk.

``discover()`` opens the project's issue list and yields one
``DocumentRef`` per issue. ``fetch()`` opens the issue and returns the
rendered description plus comment thread as one Document.

Selectors target the modern Jira Software Cloud UI. Server / Data Centre
deployments use a different UI tree and are not supported in v0.3.0.
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

_ISSUE_LIST_SELECTOR = (
    '[data-testid="native-issue-table.ui.issue-table"], '
    '[data-testid="issue-list"]'
)
_LOGIN_FORM_SELECTOR = 'form[name="loginform"], #login-form'
_ISSUE_ROW_SELECTOR = (
    '[data-testid="native-issue-table.ui.issue-table"] a[href*="/browse/"], '
    '[data-testid="issue-list"] a[href*="/browse/"]'
)
_ISSUE_BODY_SELECTOR = (
    '[data-testid="issue.views.issue-base.foundation.summary.heading"]'
)

_LOAD_TIMEOUT_MS = 20_000
_PAGE_TIMEOUT_MS = 10_000


class JiraConnector(BaseConnector):
    """Jira Cloud scraper backed by Chrome.

    Construct via the registry: ``registry.create("jira", session=...,
    site="acme", project="ENG")``. ``site`` is the Atlassian sub-domain
    (``acme`` for ``acme.atlassian.net``).
    """

    kind = "jira"

    def __init__(
        self,
        *,
        session: BrowserSession,
        site: str,
        project: str | None = None,
        source_id: str | None = None,
    ) -> None:
        super().__init__(session=session, source_id=source_id or f"jira:{site}")
        self.site = site
        self.project = project
        self.base_url = f"https://{site}.atlassian.net"

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        page = await self.session.new_page()
        try:
            await self._goto_issue_list(page)
            issues = await self._list_issues(page)
        finally:
            await page.close()

        for issue in issues:
            if not apply_name_filter(issue["key"], filter):
                continue
            yield DocumentRef(
                source_id=self.id,
                source_kind=self.kind,
                path=f"issue:{issue['key']}",
                native_url=issue["href"],
                content_type="text/html",
                metadata={"key": issue["key"], "summary": issue["summary"]},
            )

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        page = await self.session.new_page()
        try:
            url = ref.native_url or f"{self.base_url}/browse/{ref.metadata.get('key', ref.path)}"
            await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
            await page.wait_for_selector(_ISSUE_BODY_SELECTOR, timeout=_PAGE_TIMEOUT_MS)
            text: str = await page.evaluate("() => document.body.innerText")
        finally:
            await page.close()

        yield Document(ref=ref, text=text, fetched_at=datetime.now(UTC))

    async def _goto_issue_list(self, page: Page) -> None:
        url = (
            f"{self.base_url}/jira/your-work"
            if not self.project
            else f"{self.base_url}/jira/software/projects/{self.project}/issues"
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
        await wait_for_signed_in_or_raise(
            page,
            app_selector=_ISSUE_LIST_SELECTOR,
            login_selector=_LOGIN_FORM_SELECTOR,
            provider=f"jira site {self.site!r}",
            timeout_ms=_LOAD_TIMEOUT_MS,
        )

    async def _list_issues(self, page: Page) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await page.evaluate(
            f"""() => Array.from(document.querySelectorAll({_ISSUE_ROW_SELECTOR!r}))
                .map(a => {{
                    const href = a.href;
                    const m = href.match(/\\/browse\\/([A-Z][A-Z0-9_]*-\\d+)/);
                    return {{
                        href,
                        key: m ? m[1] : '',
                        summary: (a.textContent || '').trim(),
                    }};
                }})
                .filter(r => r.key)"""
        )
        return result


registry.register("jira", JiraConnector)
