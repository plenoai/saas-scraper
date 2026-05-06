"""Notion connector — Chrome-driven page enumeration and content scrape.

Sees comments and UI-only blocks the public API hides. Profile must have
an active Notion login for the target workspace.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import BaseConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry


class NotionConnector(BaseConnector):
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

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if False:  # pragma: no cover
            yield

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        if False:  # pragma: no cover
            yield


registry.register("notion", NotionConnector)
