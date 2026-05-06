"""Chrome session manager backed by Playwright.

The session manages one persistent browser context per profile directory so
that SSO / MFA cookies survive across runs. Connectors open Pages from this
shared context.

Profile directory resolution order:
1. `profile_dir=` constructor arg
2. `SAAS_SCRAPER_PROFILE` env var
3. `$XDG_DATA_HOME/saas-scraper/profile` (or `~/.local/share/...` fallback)
"""

from __future__ import annotations

import os
from pathlib import Path
from types import TracebackType
from typing import Self

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    ViewportSize,
    async_playwright,
)


def default_profile_dir() -> Path:
    """Resolve the persistent Chrome profile directory.

    XDG-compliant on Linux, sensible fallback on macOS / Windows. Created
    on first access so callers don't have to handle the not-exists case.
    """
    if env := os.environ.get("SAAS_SCRAPER_PROFILE"):
        p = Path(env).expanduser()
    else:
        base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
        p = Path(base) / "saas-scraper" / "profile"
    p.mkdir(parents=True, exist_ok=True)
    return p


class BrowserSession:
    """Lifecycle wrapper around a persistent Playwright Chromium context.

    Use as an async context manager; share one session across all
    connectors in a single scrape so they reuse cookies and a single
    Chromium process. Construction is cheap; `start()` actually launches
    Chromium and is idempotent.
    """

    def __init__(
        self,
        *,
        profile_dir: Path | str | None = None,
        headless: bool = True,
        slow_mo_ms: int = 0,
        viewport_width: int = 1440,
        viewport_height: int = 900,
        user_agent: str | None = None,
    ) -> None:
        self.profile_dir: Path = (
            Path(profile_dir).expanduser() if profile_dir else default_profile_dir()
        )
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self.headless = headless
        self.slow_mo_ms = slow_mo_ms
        self.viewport: ViewportSize = {"width": viewport_width, "height": viewport_height}
        self.user_agent = user_agent

        self._pw: Playwright | None = None
        self._context: BrowserContext | None = None
        self._browser: Browser | None = None

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def start(self) -> None:
        """Launch Chromium and a persistent context.

        Idempotent — repeat calls are no-ops. Persistent context means
        cookies, localStorage and IndexedDB live in `profile_dir`, so a
        previously-completed SSO survives a process restart.
        """
        if self._context is not None:
            return
        self._pw = await async_playwright().start()
        self._context = await self._pw.chromium.launch_persistent_context(
            user_data_dir=str(self.profile_dir),
            headless=self.headless,
            slow_mo=self.slow_mo_ms,
            viewport=self.viewport,
            user_agent=self.user_agent,
        )

    async def close(self) -> None:
        """Tear down the context and the Playwright driver."""
        if self._context is not None:
            await self._context.close()
            self._context = None
        if self._pw is not None:
            await self._pw.stop()
            self._pw = None

    async def new_page(self) -> Page:
        """Open a fresh Page on the shared persistent context."""
        if self._context is None:
            raise RuntimeError("BrowserSession.new_page() before start() / __aenter__")
        return await self._context.new_page()

    @property
    def context(self) -> BrowserContext:
        """The underlying Playwright context. None until `start()`."""
        if self._context is None:
            raise RuntimeError("BrowserSession context accessed before start()")
        return self._context
