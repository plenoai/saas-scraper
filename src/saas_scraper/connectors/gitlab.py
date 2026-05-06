"""GitLab connector — Chrome-driven repo file walk.

Works against gitlab.com or any self-hosted GitLab via ``base_url``.
The web UI's file tree is rendered server-side (unlike GitHub's React
tree), which makes the selectors slightly different but the overall
shape identical: walk directories, yield blob refs, fetch raw URLs.
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

_FILE_TREE_SELECTOR = '.tree-list, [data-testid="repository-tree-content"]'
_LOGIN_FORM_SELECTOR = 'form#new_user, a[href^="/users/sign_in"]'
_FILE_ROW_SELECTOR = (
    '.tree-list a.tree-item-link, '
    '[data-testid="repository-tree-content"] a[href]'
)

_LOAD_TIMEOUT_MS = 15_000
_PAGE_TIMEOUT_MS = 8_000


class GitLabConnector(BaseConnector):
    """GitLab project scraper backed by Chrome."""

    kind = "gitlab"

    def __init__(
        self,
        *,
        session: BrowserSession,
        project: str | None = None,
        group: str | None = None,
        branch: str = "main",
        base_url: str = "https://gitlab.com",
        max_depth: int | None = 8,
        source_id: str | None = None,
    ) -> None:
        scope = project or group or base_url
        super().__init__(session=session, source_id=source_id or f"gitlab:{scope}")
        self.project = project
        self.group = group
        self.branch = branch
        self.base_url = base_url.rstrip("/")
        self.max_depth = max_depth

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if not self.project:
            yield DocumentRef(
                source_id=self.id,
                source_kind=self.kind,
                path=f"group:{self.group or ''}",
                native_url=f"{self.base_url}/{self.group or ''}",
                content_type="text/html",
                metadata={"group": self.group or ""},
            )
            return

        page = await self.session.new_page()
        try:
            await self._goto_project_root(page)
            paths = await self._walk_tree(page, "")
        finally:
            await page.close()

        for path in paths:
            if not apply_name_filter(path, filter):
                continue
            yield DocumentRef(
                source_id=self.id,
                source_kind=self.kind,
                path=path,
                native_url=self._raw_url(path),
                content_type="application/octet-stream",
                metadata={"project": self.project, "branch": self.branch},
            )

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        page = await self.session.new_page()
        try:
            url = ref.native_url or self._raw_url(ref.path)
            response = await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
            body = await response.body() if response else b""
        finally:
            await page.close()

        text: str | None
        binary: bytes | None
        try:
            text = body.decode("utf-8")
            binary = None
        except UnicodeDecodeError:
            text = None
            binary = body

        yield Document(ref=ref, text=text, binary=binary, fetched_at=datetime.now(UTC))

    async def _goto_project_root(self, page: Page) -> None:
        url = f"{self.base_url}/{self.project}/-/tree/{self.branch}"
        await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
        await wait_for_signed_in_or_raise(
            page,
            app_selector=_FILE_TREE_SELECTOR,
            login_selector=_LOGIN_FORM_SELECTOR,
            provider=f"gitlab project {self.project!r}",
            timeout_ms=_LOAD_TIMEOUT_MS,
        )

    async def _walk_tree(self, page: Page, sub_path: str) -> list[str]:
        if sub_path:
            url = f"{self.base_url}/{self.project}/-/tree/{self.branch}/{sub_path}"
            await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
            await page.wait_for_selector(_FILE_TREE_SELECTOR, timeout=_PAGE_TIMEOUT_MS)

        rows = await self._list_rows(page)
        out: list[str] = []
        for row in rows:
            kind = row["kind"]
            href = row["href"]
            name = row["name"]
            if kind == "blob":
                out.append(f"{sub_path}/{name}".lstrip("/"))
            elif kind == "tree" and (self.max_depth is None or href.count("/") <= self.max_depth):
                child_sub_path = href.split(f"/-/tree/{self.branch}/", 1)[-1]
                out.extend(await self._walk_tree(page, child_sub_path))
        return out

    async def _list_rows(self, page: Page) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await page.evaluate(
            f"""() => Array.from(document.querySelectorAll({_FILE_ROW_SELECTOR!r}))
                .map(a => {{
                    const href = a.getAttribute('href') || '';
                    const isBlob = href.includes('/-/blob/');
                    const isTree = href.includes('/-/tree/');
                    return {{
                        href,
                        name: (a.textContent || '').trim(),
                        kind: isBlob ? 'blob' : (isTree ? 'tree' : 'other'),
                    }};
                }})
                .filter(r => r.kind !== 'other' && r.name)"""
        )
        return result

    def _raw_url(self, path: str) -> str:
        return f"{self.base_url}/{self.project}/-/raw/{self.branch}/{path.lstrip('/')}"


registry.register("gitlab", GitLabConnector)
