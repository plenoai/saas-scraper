"""Confluence connector — Chrome-driven page-tree walk."""

from __future__ import annotations

from collections.abc import AsyncIterator

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import BaseConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry


class ConfluenceConnector(BaseConnector):
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

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if False:  # pragma: no cover
            yield 

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        if False:  # pragma: no cover
            yield 


registry.register("confluence", ConfluenceConnector)
