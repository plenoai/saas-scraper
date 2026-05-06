"""GitHub connector — Chrome-driven repo + issue + PR + wiki + gist walk.

Scrapes UI-only views (rendered code review threads, organisation insights)
that the API exposes incompletely. SAML-locked enterprises that block PAT
scopes are reachable via this path because the browser session inherits SSO.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import BaseConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry


class GitHubConnector(BaseConnector):
    kind = "github"

    def __init__(
        self,
        *,
        session: BrowserSession,
        owner: str,
        repo: str | None = None,
        base_url: str = "https://github.com",
        source_id: str | None = None,
    ) -> None:
        scope = f"{owner}/{repo}" if repo else owner
        super().__init__(session=session, source_id=source_id or f"github:{scope}")
        self.owner = owner
        self.repo = repo
        self.base_url = base_url.rstrip("/")

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if False:  # pragma: no cover
            yield

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        if False:  # pragma: no cover
            yield


registry.register("github", GitHubConnector)
