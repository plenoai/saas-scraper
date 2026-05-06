"""Jira connector — Chrome-driven issue + comment scrape.

Sees field renderings (rendered ADF, custom fields) the API exposes only
in raw form, plus internal-only comments behind permission scopes the
scanner role may not have.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import BaseConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry


class JiraConnector(BaseConnector):
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

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if False:  # pragma: no cover
            yield 

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        if False:  # pragma: no cover
            yield 


registry.register("jira", JiraConnector)
