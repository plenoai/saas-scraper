"""Bitbucket Cloud connector."""

from __future__ import annotations

from collections.abc import AsyncIterator

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import BaseConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry


class BitbucketConnector(BaseConnector):
    kind = "bitbucket"

    def __init__(
        self,
        *,
        session: BrowserSession,
        workspace: str,
        repo: str | None = None,
        source_id: str | None = None,
    ) -> None:
        scope = f"{workspace}/{repo}" if repo else workspace
        super().__init__(session=session, source_id=source_id or f"bitbucket:{scope}")
        self.workspace = workspace
        self.repo = repo

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        if False:  # pragma: no cover
            yield 

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        if False:  # pragma: no cover
            yield 


registry.register("bitbucket", BitbucketConnector)
