"""Connector base class with shared boilerplate.

Implements the `Connector` protocol's `discover_and_fetch` default flow on
top of a connector's `discover` + `fetch` so subclasses only need to
write provider-specific scraping logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import ClassVar

from saas_scraper.browser import BrowserSession
from saas_scraper.core import Document, DocumentRef, SourceFilter


class BaseConnector(ABC):
    """Common base for built-in connectors.

    Subclasses set `kind` as a class variable, accept connector-specific
    kwargs in `__init__`, and override `discover` + `fetch`. The compound
    `discover_and_fetch` is provided here so most connectors don't need
    to think about it.
    """

    kind: ClassVar[str] = ""

    def __init__(self, *, session: BrowserSession, source_id: str) -> None:
        self.session = session
        self.id = source_id

    @abstractmethod
    def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        """Yield refs matching `filter`. Metadata-only."""
        ...

    @abstractmethod
    def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        """Yield the body for one ref. Single-element iter in v0.1.0."""
        ...

    async def discover_and_fetch(
        self, filter: SourceFilter | None = None
    ) -> AsyncIterator[Document]:
        """Default end-to-end flow: discover, then fetch each ref."""
        flt = filter or SourceFilter()
        async for ref in self.discover(flt):
            async for doc in self.fetch(ref):
                yield doc

    async def close(self) -> None:
        """Default no-op. Override to release per-connector resources."""
        return None
