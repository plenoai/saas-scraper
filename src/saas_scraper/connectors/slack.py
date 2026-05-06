"""Slack connector.

Drives the Slack web client UI to enumerate channels and scrape recent
messages. Relies on the BrowserSession's persistent profile being already
logged into the target workspace (one-time interactive login is the user's
responsibility).

v0.1.0 scaffold: protocol-compliant, exercises the BrowserSession plumbing,
but the actual channel walk / message extraction remains TODO. Hardening
lands incrementally.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from saas_scraper.browser import BrowserSession
from saas_scraper.connectors._base import BaseConnector
from saas_scraper.core import Document, DocumentRef, SourceFilter
from saas_scraper.registry import registry


class SlackConnector(BaseConnector):
    """Slack workspace scraper backed by Chrome."""

    kind = "slack"

    def __init__(
        self,
        *,
        session: BrowserSession,
        workspace: str,
        source_id: str | None = None,
    ) -> None:
        super().__init__(session=session, source_id=source_id or f"slack:{workspace}")
        self.workspace = workspace

    async def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        """Walk the channel sidebar and yield one ref per channel.

        TODO(v0.2.0): implement the channel walk. Current scaffold
        terminates immediately so callers can wire the connector and
        observe the empty-stream contract.
        """
        if False:  # pragma: no cover — scaffold
            yield 

    async def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        """Scrape the channel page rendered by Slack's web client.

        TODO(v0.2.0): implement message extraction.
        """
        if False:  # pragma: no cover — scaffold
            yield 


registry.register("slack", SlackConnector)
