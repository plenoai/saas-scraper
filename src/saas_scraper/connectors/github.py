"""GitHub connector — Chrome-driven repo file walk + issue / PR scrape.

Scrapes a repository's file tree, issue list, and pull-request list via
the web UI, then fetches body content over the same browser session (so
SAML-locked enterprises that restrict PAT scopes are still reachable).
Public repos work without login; private / SAML repos require a one-time
``--headed`` SSO step.

What ``discover()`` yields, controlled by the ``resources`` set:

* ``"code"`` (default) — one ref per blob in the file tree, walked
  depth-first up to ``max_depth``.
* ``"issues"`` — one ref per issue in
  ``<owner>/<repo>/issues?state=all``, paginated until exhausted or
  ``max_issue_pages`` is reached.
* ``"prs"`` — one ref per pull request in
  ``<owner>/<repo>/pulls?state=all``, paginated similarly.

What ``fetch()`` returns, dispatched by ``ref.metadata["resource_type"]``:

* ``"code"`` — raw blob bytes (text-decoded when valid UTF-8).
* ``"issue"`` — title + body + every visible comment, concatenated.
* ``"pr"`` — title + body + every visible comment + diff text.

Limits:

* Tree walk is depth-bounded (``max_depth=8``).
* Issue / PR enumeration is page-bounded (``max_issue_pages=20`` by
  default ≈ 500 items — bump for big projects).
* Conversation extraction reads currently-rendered comments only;
  threads with "show more" buttons require a follow-up scroll pass that
  this connector doesn't yet drive.
* No wiki / gist scrape yet — future addition under the same connector.
"""

from __future__ import annotations

import re
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

# Issue / PR list pages share the same React shell. The "Issues" / "Pull
# requests" tab nav is stable enough to use as the app-shell signal.
_CONVERSATION_LIST_SELECTOR = (
    'div[aria-label="Issues"], '
    'div[aria-label="Pull requests"], '
    '[data-testid="issue-row"], '
    '[data-testid="pull-request-row"], '
    'a[id^="issue_"], '
    'a[id^="pull_"]'
)

# Issue / PR page shell. The title element has multiple acceptable
# selectors because GitHub keeps shipping React refactors that rename it.
_CONVERSATION_PAGE_SELECTOR = (
    'bdi.js-issue-title, '
    '[data-testid="issue-title"], '
    '[data-testid="markdown-title"], '
    'h1.gh-header-title'
)

_LOAD_TIMEOUT_MS = 15_000
_PAGE_TIMEOUT_MS = 8_000

DEFAULT_RESOURCES: frozenset[str] = frozenset({"code"})
SUPPORTED_RESOURCES: frozenset[str] = frozenset({"code", "issues", "prs"})


class GitHubConnector(BaseConnector):
    """GitHub repo scraper backed by Chrome.

    Construct via the registry::

        registry.create(
            "github",
            session=...,
            owner="plenoai",
            repo="saas-scraper",
            resources={"code", "issues", "prs"},
        )

    Drop ``repo=`` to walk the org landing page only (full org walk lands
    in a later release).
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
        resources: frozenset[str] | set[str] | None = None,
        max_issue_pages: int = 20,
        source_id: str | None = None,
    ) -> None:
        scope = f"{owner}/{repo}" if repo else owner
        super().__init__(session=session, source_id=source_id or f"github:{scope}")
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self.base_url = base_url.rstrip("/")
        self.max_depth = max_depth
        self.resources = frozenset(resources) if resources else DEFAULT_RESOURCES
        unknown = self.resources - SUPPORTED_RESOURCES
        if unknown:
            raise ValueError(
                f"unknown resources {sorted(unknown)}; "
                f"supported: {sorted(SUPPORTED_RESOURCES)}"
            )
        self.max_issue_pages = max_issue_pages

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if not self.repo:
            yield DocumentRef(
                source_id=self.id,
                source_kind=self.kind,
                path=f"org:{self.owner}",
                native_url=f"{self.base_url}/{self.owner}",
                content_type="text/html",
                metadata={"owner": self.owner, "resource_type": "org"},
            )
            return

        if "code" in self.resources:
            async for ref in self._discover_code(filter):
                yield ref
        if "issues" in self.resources:
            async for ref in self._discover_conversations(filter, kind="issues"):
                yield ref
        if "prs" in self.resources:
            async for ref in self._discover_conversations(filter, kind="prs"):
                yield ref

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        resource_type = ref.metadata.get("resource_type", "code")
        if resource_type in {"issue", "pr"}:
            async for doc in self._fetch_conversation(ref):
                yield doc
            return
        # Fallthrough = code blob.
        async for doc in self._fetch_blob(ref):
            yield doc

    # --- code (file tree) -------------------------------------------------

    async def _discover_code(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
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
                    "resource_type": "code",
                },
            )

    async def _fetch_blob(self, ref: DocumentRef) -> AsyncIterator[Document]:
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

    # --- issues / pull requests ------------------------------------------

    async def _discover_conversations(
        self, filter: SourceFilter, *, kind: str
    ) -> AsyncIterator[DocumentRef]:
        """Yield one DocumentRef per issue (kind="issues") or PR (kind="prs")."""
        list_path, item_segment, resource_type = (
            ("issues", "issues", "issue") if kind == "issues" else ("pulls", "pull", "pr")
        )
        page = await self.session.new_page()
        try:
            for page_num in range(1, self.max_issue_pages + 1):
                url = (
                    f"{self.base_url}/{self.owner}/{self.repo}/{list_path}"
                    f"?q=is%3A{item_segment.rstrip('s')}+is%3Aall&page={page_num}"
                )
                # Issue / PR list pages can race a login form too.
                await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
                if page_num == 1:
                    await wait_for_signed_in_or_raise(
                        page,
                        app_selector=_CONVERSATION_LIST_SELECTOR,
                        login_selector=_LOGIN_FORM_SELECTOR,
                        provider=f"github {list_path} {self.owner}/{self.repo}",
                        timeout_ms=_LOAD_TIMEOUT_MS,
                    )
                items = await self._list_conversation_items(page, item_segment=item_segment)
                if not items:
                    break
                for number, title in items:
                    path = f"{item_segment}/{number}"
                    if not apply_name_filter(path, filter):
                        continue
                    yield DocumentRef(
                        source_id=self.id,
                        source_kind=self.kind,
                        path=path,
                        native_url=f"{self.base_url}/{self.owner}/{self.repo}/{path}",
                        content_type="text/html",
                        metadata={
                            "owner": self.owner,
                            "repo": self.repo or "",
                            "resource_type": resource_type,
                            "title": title,
                            "number": str(number),
                        },
                    )
        finally:
            await page.close()

    async def _list_conversation_items(
        self, page: Page, *, item_segment: str
    ) -> list[tuple[int, str]]:
        """Return (number, title) tuples extracted from an issue/PR list page.

        Uses two selector strategies: the legacy ``a#issue_<n>`` /
        ``a#pull_<n>`` anchors that have been stable since 2018, and the
        modern React rows surfacing ``data-testid``. Whichever returns
        results wins; we don't try both — production pages render one or
        the other depending on which UI bucket the repo is in.
        """
        rows: list[dict[str, str]] = await page.evaluate(
            f"""() => {{
                const out = [];
                const seen = new Set();
                const harvest = (a) => {{
                    const href = a.getAttribute('href') || '';
                    const m = href.match(new RegExp('/' + {item_segment!r} + '/(\\\\d+)(?:[/?#]|$)'));
                    if (!m) return;
                    const num = m[1];
                    if (seen.has(num)) return;
                    seen.add(num);
                    out.push({{ number: num, title: (a.textContent || '').trim() }});
                }};
                document.querySelectorAll('a[id^="issue_"], a[id^="pull_"]').forEach(harvest);
                document
                    .querySelectorAll('[data-testid="issue-row"] a[href], [data-testid="pull-request-row"] a[href]')
                    .forEach(harvest);
                return out;
            }}"""
        )
        return [(int(r["number"]), r["title"]) for r in rows if r.get("number")]

    async def _fetch_conversation(self, ref: DocumentRef) -> AsyncIterator[Document]:
        url = ref.native_url or f"{self.base_url}/{self.owner}/{self.repo}/{ref.path}"
        page = await self.session.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=_LOAD_TIMEOUT_MS)
            await page.wait_for_selector(_CONVERSATION_PAGE_SELECTOR, timeout=_PAGE_TIMEOUT_MS)
            text = await self._extract_conversation_text(page)
        finally:
            await page.close()

        yield Document(
            ref=ref,
            text=text,
            fetched_at=datetime.now(UTC),
        )

    async def _extract_conversation_text(self, page: Page) -> str:
        """Concat title + every visible comment body into one text blob.

        We deliberately use innerText (not textContent) so that line
        breaks in rendered markdown survive — secret regexes are
        line-anchored and lose hits when comments collapse to one line.
        """
        result: dict[str, object] = await page.evaluate(
            """() => {
                const titleEl = document.querySelector(
                    'bdi.js-issue-title, [data-testid="issue-title"], '
                    + '[data-testid="markdown-title"], h1.gh-header-title'
                );
                const title = (titleEl && titleEl.innerText) ? titleEl.innerText.trim() : '';
                const bodies = Array.from(
                    document.querySelectorAll(
                        '.markdown-body, .comment-body, [data-testid="comment-body"], [data-testid="markdown-body"]'
                    )
                ).map(el => (el.innerText || '').trim()).filter(Boolean);
                // PR diff text. /files tab isn't visited here; we only
                // capture the inline diff that the conversation page
                // surfaces in review comments. A future revision can
                // navigate to /files for full-diff scanning.
                const diffs = Array.from(
                    document.querySelectorAll(
                        '.blob-code-inner, [data-testid="diff-content"]'
                    )
                ).map(el => (el.innerText || '').trim()).filter(Boolean);
                return { title, bodies, diffs };
            }"""
        )
        title = str(result.get("title", "")).strip()
        bodies = result.get("bodies") or []
        diffs = result.get("diffs") or []
        parts: list[str] = []
        if title:
            parts.append(title)
        if isinstance(bodies, list):
            parts.extend(str(b) for b in bodies)
        if isinstance(diffs, list):
            parts.extend(str(d) for d in diffs)
        return "\n\n".join(parts)

    # --- shared internals -------------------------------------------------

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


_NUMBER_RE = re.compile(r"/(?:issues|pull)/(\d+)")  # exposed for tests if needed


registry.register("github", GitHubConnector)
