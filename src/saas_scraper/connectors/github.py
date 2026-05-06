"""GitHub connector — Chrome-driven repo file walk.

Scrapes a repository's file tree via the web UI, then fetches raw blob
content over the same browser session (so SAML-locked enterprises that
restrict PAT scopes are still reachable). Public repos work without
login; private / SAML repos require a one-time ``--headed`` SSO step.

What ``discover()`` does:

* Opens ``<base_url>/<owner>/<repo>``.
* Recursively walks the file tree by visiting subdirectory URLs and
  reading the rendered ``[role="row"]`` entries. Yields one
  ``DocumentRef`` per blob with ``path`` set to the repo-relative path.

What ``fetch()`` does:

* Navigates to ``<base_url>/<owner>/<repo>/raw/refs/heads/<branch>/<path>``
  and returns the response body as a ``Document``.

Limits:

* Tree walk is depth-bounded (``max_depth=8``) to prevent runaway
  recursion on adversarial repos. Set ``max_depth=None`` to disable.
* Binary blobs are returned with ``binary=`` populated and ``text=None``;
  decoding decisions belong to downstream pipelines.
* No issue / PR / wiki / gist scrape yet — those land in subsequent
  releases under the same connector or as siblings.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from urllib.parse import urljoin

from playwright.async_api import Page

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import (
    BaseConnector,
    apply_name_filter,
    wait_for_signed_in_or_raise,
)
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry

_FILE_TREE_SELECTOR = (
    '[aria-labelledby="folders-and-files"], '
    'table[aria-labelledby^="folders-and-files"]'
)
_LOGIN_FORM_SELECTOR = 'form[action="/session"], a[href^="/login"]'
_FILE_ROW_SELECTOR = '[aria-labelledby="folders-and-files"] [role="row"] a[href]'

_LOAD_TIMEOUT_MS = 15_000
_PAGE_TIMEOUT_MS = 8_000


class GitHubConnector(BaseConnector):
    """GitHub repo scraper backed by Chrome.

    Construct via the registry:
    ``registry.create("github", session=..., owner="plenoai", repo="saas-scraper")``.
    Drop ``repo=`` to walk the org's repo list (returns one
    ``DocumentRef`` for the org landing page in v0.3.0 — full org walk
    lands later).
    """

    kind = "github"

    def __init__(
        self,
        *,
        session: BrowserSession,
        owner: str,
        repo: str | None = None,
        branch: str = "main",
        base_url: str = "https://github.com",
        max_depth: int | None = 8,
        source_id: str | None = None,
    ) -> None:
        scope = f"{owner}/{repo}" if repo else owner
        super().__init__(session=session, source_id=source_id or f"github:{scope}")
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self.base_url = base_url.rstrip("/")
        self.max_depth = max_depth

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if not self.repo:
            yield DocumentRef(
                source_id=self.id,
                source_kind=self.kind,
                path=f"org:{self.owner}",
                native_url=f"{self.base_url}/{self.owner}",
                content_type="text/html",
                metadata={"owner": self.owner},
            )
            return

        page = await self.session.new_page()
        try:
            await self._goto_repo_root(page)
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
                metadata={
                    "owner": self.owner,
                    "repo": self.repo or "",
                    "branch": self.branch,
                },
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

        yield Document(
            ref=ref,
            text=text,
            binary=binary,
            fetched_at=datetime.now(UTC),
        )

    # --- internals ---------------------------------------------------

    async def _goto_repo_root(self, page: Page) -> None:
        url = f"{self.base_url}/{self.owner}/{self.repo}"
        await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
        await wait_for_signed_in_or_raise(
            page,
            app_selector=_FILE_TREE_SELECTOR,
            login_selector=_LOGIN_FORM_SELECTOR,
            provider=f"github repo {self.owner}/{self.repo}",
            timeout_ms=_LOAD_TIMEOUT_MS,
        )

    async def _walk_tree(self, page: Page, sub_path: str) -> list[str]:
        """Depth-first traversal. Returns blob paths only.

        Directories are walked but not yielded as Documents because their
        content is the union of children, which the caller already gets
        one ref per child.
        """
        if sub_path:
            url = f"{self.base_url}/{self.owner}/{self.repo}/tree/{self.branch}/{sub_path}"
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
            elif kind == "tree" and (self.max_depth is None or _depth(href) <= self.max_depth):
                child_sub_path = href.split(f"/tree/{self.branch}/", 1)[-1]
                out.extend(await self._walk_tree(page, child_sub_path))
        return out

    async def _list_rows(self, page: Page) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await page.evaluate(
            f"""() => Array.from(document.querySelectorAll({_FILE_ROW_SELECTOR!r}))
                .map(a => {{
                    const href = a.getAttribute('href') || '';
                    const isBlob = href.includes('/blob/');
                    const isTree = href.includes('/tree/');
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
        return urljoin(
            f"{self.base_url}/",
            f"{self.owner}/{self.repo}/raw/refs/heads/{self.branch}/{path.lstrip('/')}",
        )


def _depth(href: str) -> int:
    if "/tree/" not in href:
        return 0
    tail = href.split("/tree/", 1)[-1]
    return tail.count("/")


registry.register("github", GitHubConnector)
