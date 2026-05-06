"""GitLab connector — works against gitlab.com or self-hosted via base_url."""

from __future__ import annotations

from collections.abc import AsyncIterator

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import BaseConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry


class GitLabConnector(BaseConnector):
    kind = "gitlab"

    def __init__(
        self,
        *,
        session: BrowserSession,
        group: str | None = None,
        project: str | None = None,
        base_url: str = "https://gitlab.com",
        source_id: str | None = None,
    ) -> None:
        scope = project or group or base_url
        super().__init__(session=session, source_id=source_id or f"gitlab:{scope}")
        self.group = group
        self.project = project
        self.base_url = base_url.rstrip("/")

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if False:  # pragma: no cover
            yield

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        if False:  # pragma: no cover
            yield


registry.register("gitlab", GitLabConnector)
