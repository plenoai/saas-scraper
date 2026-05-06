"""scroll_collect helper tests.

Drives the helper against a fake Page whose ``evaluate`` returns a
sliding window over a pre-built list, simulating a virtualised scroller
that progressively reveals more rows on each scroll.
"""

from __future__ import annotations

from typing import Any

import pytest

from saas_scraper.connectors._scroll import scroll_collect


class _ScrollerPage:
    """Yields successively larger slices on each evaluate() call.

    Mimics the scroller behaviour: scroll once, see more rows; scroll
    again, see even more; eventually plateau when the list ends.
    """

    def __init__(self, slices: list[list[dict[str, str]]]) -> None:
        self._slices = slices
        self._idx = 0
        self.scroll_calls = 0

    async def evaluate(self, js: str) -> Any:
        if "scrollHeight" in js:
            self.scroll_calls += 1
            return None
        # Extract call: return current slice, advance for next time.
        out = self._slices[min(self._idx, len(self._slices) - 1)]
        self._idx += 1
        return out


@pytest.mark.asyncio
async def test_scroll_collect_terminates_when_no_new_rows() -> None:
    page = _ScrollerPage(
        slices=[
            [{"id": "a"}, {"id": "b"}],
            [{"id": "a"}, {"id": "b"}, {"id": "c"}],
            [{"id": "a"}, {"id": "b"}, {"id": "c"}],  # stable → stop
        ]
    )

    async def _extract(p: Any) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await p.evaluate("dummy")
        return result

    items = await scroll_collect(
        page,  # type: ignore[arg-type]
        container_selector=".scroller",
        extract=_extract,
        key=lambda d: d["id"],
        settle_ms=0,
    )
    assert [d["id"] for d in items] == ["a", "b", "c"]
    assert page.scroll_calls >= 1


@pytest.mark.asyncio
async def test_scroll_collect_respects_max_iterations() -> None:
    page = _ScrollerPage(
        slices=[
            [{"id": str(i)} for i in range(n)] for n in range(1, 50)
        ]
    )

    async def _extract(p: Any) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await p.evaluate("dummy")
        return result

    items = await scroll_collect(
        page,  # type: ignore[arg-type]
        container_selector=".scroller",
        extract=_extract,
        key=lambda d: d["id"],
        max_iterations=5,
        settle_ms=0,
    )
    # 1 initial extract + 5 scroll iterations = first-seen ids 0..5
    assert [d["id"] for d in items] == ["0", "1", "2", "3", "4", "5"]


@pytest.mark.asyncio
async def test_scroll_collect_stops_via_predicate() -> None:
    page = _ScrollerPage(
        slices=[
            [{"id": "a"}],
            [{"id": "a"}, {"id": "b"}],
            [{"id": "a"}, {"id": "b"}, {"id": "c"}],
        ]
    )

    async def _extract(p: Any) -> list[dict[str, str]]:
        result: list[dict[str, str]] = await p.evaluate("dummy")
        return result

    items = await scroll_collect(
        page,  # type: ignore[arg-type]
        container_selector=".scroller",
        extract=_extract,
        key=lambda d: d["id"],
        predicate=lambda acc: len(acc) < 2,  # stop once we have 2+ items
        settle_ms=0,
    )
    assert {d["id"] for d in items} == {"a", "b"}
