"""Virtual-list scroll helpers shared across connectors.

Most modern SaaS UIs (Slack, Notion, Jira) render long lists inside a
virtualised scroller. The DOM at any moment holds only the rows
currently visible plus a small overscan. To enumerate the full list, we
have to scroll the container, wait for the new rows, harvest them, and
repeat — terminating when:

* a full scroll cycle yields no new rows (we hit the end), or
* a caller-provided ``max_iterations`` cap is reached, or
* a caller-provided ``predicate`` returns False (e.g. "stop once we've
  seen the channel we want").

The functions here keep that loop in one place so each connector can
drop its hand-rolled "scroll N times then read" code.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from playwright.async_api import Page

# Default per-iteration settle delay — the scroller paints new rows
# asynchronously. 250 ms is roughly two animation frames at 60 Hz, which
# is enough for Slack and Notion in our manual measurements.
_SETTLE_MS = 250


async def scroll_collect[T](
    page: Page,
    *,
    container_selector: str,
    extract: Callable[[Page], Awaitable[list[T]]],
    key: Callable[[T], str],
    max_iterations: int = 200,
    settle_ms: int = _SETTLE_MS,
    predicate: Callable[[list[T]], bool] | None = None,
) -> list[T]:
    """Scroll ``container_selector`` until ``extract`` stops finding new items.

    Args:
        page: open Playwright page.
        container_selector: CSS for the scroll container. The function
            scrolls it down via ``element.scrollTop = element.scrollHeight``
            and re-reads after each settle.
        extract: async callable that returns the currently-visible items.
            Called once after the initial render and once per scroll
            iteration.
        key: maps each item to a stable string used for de-duplication;
            two items with the same key are treated as the same row.
        max_iterations: hard cap so an infinite-scroll bug doesn't lock
            up the scrape. Default 200 is enough for ~10k visible rows
            on Slack-sized scrollers.
        settle_ms: how long to wait after each scroll for new rows to
            paint. Tune higher on slow networks.
        predicate: optional callback that receives the current
            accumulated list. Returning False stops scrolling early.

    Returns:
        The accumulated items in first-seen order.
    """
    seen: dict[str, T] = {}
    current = await extract(page)
    for item in current:
        seen.setdefault(key(item), item)

    for _ in range(max_iterations):
        if predicate is not None and not predicate(list(seen.values())):
            break

        before = len(seen)
        await page.evaluate(
            f"""() => {{
                const el = document.querySelector({container_selector!r});
                if (el) el.scrollTop = el.scrollHeight;
            }}"""
        )
        await asyncio.sleep(settle_ms / 1000)

        current = await extract(page)
        for item in current:
            seen.setdefault(key(item), item)

        if len(seen) == before:
            break  # full scroll cycle without new rows = end of list

    return list(seen.values())
