"""GitHub connector tests against a fake Page."""

from __future__ import annotations

import pytest

from saas_scraper.connectors._base import NotLoggedInError
from saas_scraper.connectors.github import GitHubConnector
from saas_scraper.core import SourceFilter
from tests._fake_page import FakePage, FakeResponse, session_with_page


def _row(href: str, name: str, kind: str) -> dict[str, str]:
    return {"href": href, "name": name, "kind": kind}


@pytest.mark.asyncio
async def test_discover_org_only_yields_landing_ref() -> None:
    page = FakePage(js_results={})  # never used; not invoked
    session = session_with_page(page)
    conn = GitHubConnector(session=session, owner="plenoai")
    refs = [r async for r in conn.discover(SourceFilter())]
    assert len(refs) == 1
    assert refs[0].path == "org:plenoai"
    assert refs[0].native_url == "https://github.com/plenoai"


@pytest.mark.asyncio
async def test_discover_repo_walks_root_only() -> None:
    page = FakePage(
        js_results={
            "_FILE_ROW_SELECTOR": [],  # not matched
            "querySelectorAll": [
                _row("/plenoai/saas-scraper/blob/main/README.md", "README.md", "blob"),
                _row("/plenoai/saas-scraper/blob/main/pyproject.toml", "pyproject.toml", "blob"),
            ],
        }
    )
    session = session_with_page(page)
    conn = GitHubConnector(session=session, owner="plenoai", repo="saas-scraper")
    refs = [r async for r in conn.discover(SourceFilter())]
    paths = sorted(r.path for r in refs)
    assert paths == ["README.md", "pyproject.toml"]
    raw = refs[0].native_url
    assert raw is not None
    assert raw.startswith("https://github.com/plenoai/saas-scraper/raw/refs/heads/main/")


@pytest.mark.asyncio
async def test_discover_recurses_into_subdirs() -> None:
    # First evaluate (root) returns a tree; second evaluate (subdir)
    # returns a blob. The fake matches by substring, so we can only
    # return one value per JS body. Use a counter via a queue.
    seen: list[list[dict[str, str]]] = [
        [_row("/plenoai/saas-scraper/tree/main/src", "src", "tree")],
        [_row("/plenoai/saas-scraper/blob/main/src/__init__.py", "__init__.py", "blob")],
    ]

    async def evaluate(_: str) -> list[dict[str, str]]:
        return seen.pop(0)

    page = FakePage(js_results={})
    page.evaluate = evaluate  # type: ignore[assignment, method-assign]
    session = session_with_page(page)
    conn = GitHubConnector(session=session, owner="plenoai", repo="saas-scraper", max_depth=2)
    refs = [r async for r in conn.discover(SourceFilter())]
    assert [r.path for r in refs] == ["src/__init__.py"]


@pytest.mark.asyncio
async def test_discover_raises_not_logged_in() -> None:
    """The login form selector winning the race surfaces NotLoggedInError."""

    async def wait_handler(selector: str) -> object:
        if "/session" in selector or "/login" in selector:
            return object()
        import asyncio as _asyncio

        await _asyncio.sleep(60)
        return None

    page = FakePage(js_results={}, wait_for_selector_handler=wait_handler)
    session = session_with_page(page)
    conn = GitHubConnector(session=session, owner="plenoai", repo="saas-scraper")
    with pytest.raises(NotLoggedInError, match="github"):
        async for _ in conn.discover(SourceFilter()):
            pass


@pytest.mark.asyncio
async def test_fetch_decodes_text() -> None:
    page = FakePage(js_results={}, goto_response=FakeResponse(b"hello world"))
    session = session_with_page(page)
    conn = GitHubConnector(session=session, owner="plenoai", repo="saas-scraper")
    from saas_scraper.core import DocumentRef

    ref = DocumentRef(
        source_id="github:plenoai/saas-scraper",
        source_kind="github",
        path="README.md",
        native_url="https://github.com/plenoai/saas-scraper/raw/refs/heads/main/README.md",
    )
    docs = [d async for d in conn.fetch(ref)]
    assert len(docs) == 1
    assert docs[0].text == "hello world"
    assert docs[0].binary is None


@pytest.mark.asyncio
async def test_fetch_falls_back_to_binary() -> None:
    page = FakePage(js_results={}, goto_response=FakeResponse(b"\x00\x01\x02\xff"))
    session = session_with_page(page)
    conn = GitHubConnector(session=session, owner="plenoai", repo="saas-scraper")
    from saas_scraper.core import DocumentRef

    ref = DocumentRef(source_id="x", source_kind="github", path="bin")
    docs = [d async for d in conn.fetch(ref)]
    assert docs[0].binary == b"\x00\x01\x02\xff"
    assert docs[0].text is None
