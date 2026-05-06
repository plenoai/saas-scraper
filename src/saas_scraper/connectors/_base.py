"""Connector base class with shared boilerplate.

Implements the `Connector` protocol's `discover_and_fetch` default flow on
top of a connector's `discover` + `fetch` so subclasses only need to
write provider-specific scraping logic.

Also exposes ``wait_for_signed_in_or_raise``, which every connector that
backs onto an authenticated SaaS UI uses to convert "the login form
appeared instead of the app shell" into a clean, actionable
``NotLoggedInError`` rather than a generic Playwright timeout.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import ClassVar

from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from saas_scraper.browser import BrowserSession
from saas_scraper.core import Document, DocumentRef, SourceFilter


class NotLoggedInError(RuntimeError):
    """Raised when the persistent profile has no active session for the SaaS.

    Recovery is interactive: the caller must run the connector once with
    ``BrowserSession(headless=False)`` so they can complete SSO / MFA.
    Cookies then persist in the profile for headless runs.
    """


async def wait_for_signed_in_or_raise(
    page: Page,
    *,
    app_selector: str,
    login_selector: str,
    provider: str,
    timeout_ms: int = 15_000,
) -> None:
    """Race ``app_selector`` against ``login_selector`` and act on the winner.

    If the app shell selector wins, the function returns and the caller
    continues to scrape. If the login form wins, ``NotLoggedInError`` is
    raised so the caller can surface a clear "run --headed once" message.

    The loser of the race is cancelled before this function returns so we
    don't leak a pending ``wait_for_selector`` task into Playwright's
    event loop.
    """
    app_task = asyncio.create_task(page.wait_for_selector(app_selector, timeout=timeout_ms))
    login_task = asyncio.create_task(page.wait_for_selector(login_selector, timeout=timeout_ms))
    try:
        done, _pending = await asyncio.wait(
            {app_task, login_task}, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        for task in (app_task, login_task):
            if not task.done():
                task.cancel()

    for task in done:
        try:
            element = task.result()
        except PlaywrightTimeoutError:
            continue
        if task is login_task and element is not None:
            raise NotLoggedInError(
                f"{provider} session is not logged in. "
                "Run once with BrowserSession(headless=False) to complete SSO."
            )


def glob_match(name: str, pattern: str) -> bool:
    """Cheap glob-ish match shared by every connector's filter handling.

    Avoids ``fnmatch`` because real channel/page names contain ``[``,
    ``]``, ``?``, etc. that ``fnmatch`` would interpret as character
    classes. The contract is: ``*`` matches any substring, everything
    else is literal.
    """
    if "*" not in pattern:
        return name == pattern
    parts = pattern.split("*")
    return all(p in name for p in parts if p)


def apply_name_filter(name: str, filter: SourceFilter) -> bool:
    """Return True if ``name`` should be kept under ``filter``.

    ``include`` is OR-of-glob; an empty include list keeps everything.
    ``exclude`` is OR-of-glob; any match removes the candidate. Used by
    every connector's ``discover()`` to keep filter semantics uniform.
    """
    if filter.include and not any(glob_match(name, p) for p in filter.include):
        return False
    if any(glob_match(name, p) for p in filter.exclude):
        return False
    return True


class BaseConnector(ABC):
    """Common base for built-in connectors.

    Subclasses set ``kind`` as a class variable, accept connector-specific
    kwargs in ``__init__``, and override ``discover`` + ``fetch``. The
    compound ``discover_and_fetch`` is provided here so most connectors
    don't need to think about it.
    """

    kind: ClassVar[str] = ""

    def __init__(self, *, session: BrowserSession, source_id: str) -> None:
        self.session = session
        self.id = source_id

    @abstractmethod
    def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        """Yield refs matching ``filter``. Metadata-only."""
        ...

    @abstractmethod
    def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        """Yield the body for one ref. Single-element iter in v0.x."""
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
