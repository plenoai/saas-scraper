"""GitHub connector tests against a fake Page."""

from __future__ import annotations

import pytest

from saas_scraper.connectors._base import NotLoggedInError
from saas_scraper.connectors.github import GitHubConnector
from saas_scraper.core import SourceFilter
from tests._fake_page import FakePage, FakeResponse, session_with_page, session_with_pages


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


def test_unknown_resource_rejected() -> None:
    """Unknown resource names fail loudly at construction time so a typo
    doesn't silently scan nothing."""
    with pytest.raises(ValueError, match="unknown resources"):
        GitHubConnector(
            session=session_with_page(FakePage()),
            owner="plenoai",
            repo="saas-scraper",
            resources={"discussions"},
        )


@pytest.mark.asyncio
async def test_discover_issues_paginates_until_empty() -> None:
    """Issue list walk paginates and stops on the first empty page."""
    pages_payload: list[list[dict[str, str]]] = [
        [
            {"number": "1", "title": "first issue"},
            {"number": "2", "title": "second issue"},
        ],
        [
            {"number": "3", "title": "third issue"},
        ],
        [],  # exhausted — the loop must break here, not refetch.
    ]

    async def evaluate(_: str) -> list[dict[str, str]]:
        return pages_payload.pop(0)

    page = FakePage(js_results={})
    page.evaluate = evaluate  # type: ignore[assignment, method-assign]
    session = session_with_page(page)
    conn = GitHubConnector(
        session=session,
        owner="plenoai",
        repo="saas-scraper",
        resources={"issues"},
    )
    refs = [r async for r in conn.discover(SourceFilter())]
    assert [r.path for r in refs] == ["issues/1", "issues/2", "issues/3"]
    assert all(r.metadata["resource_type"] == "issue" for r in refs)
    assert refs[0].metadata["title"] == "first issue"
    assert refs[0].metadata["number"] == "1"
    # Page 1, 2, 3 navigations all happened. No fourth navigation.
    assert sum("/issues?" in g for g in page.gotos) == 3


@pytest.mark.asyncio
async def test_discover_prs_uses_pull_segment() -> None:
    """Pull requests use /pull/<n> URLs and resource_type='pr'."""

    pages_payload = [
        [{"number": "42", "title": "fix the thing"}],
        [],
    ]

    async def evaluate(_: str) -> list[dict[str, str]]:
        return pages_payload.pop(0)

    page = FakePage(js_results={})
    page.evaluate = evaluate  # type: ignore[assignment, method-assign]
    session = session_with_page(page)
    conn = GitHubConnector(
        session=session,
        owner="plenoai",
        repo="saas-scraper",
        resources={"prs"},
    )
    refs = [r async for r in conn.discover(SourceFilter())]
    assert [r.path for r in refs] == ["pull/42"]
    assert refs[0].metadata["resource_type"] == "pr"
    assert refs[0].native_url is not None
    assert refs[0].native_url.endswith("/pull/42")
    # PR list page goto was issued.
    assert any("/pulls?" in g for g in page.gotos)


@pytest.mark.asyncio
async def test_discover_combined_resources_yields_each() -> None:
    """Combining resources={code, issues} yields refs from both sets in
    order: code first, then conversations."""

    discover_pages = [
        # _discover_code path: list_rows
        [
            {
                "href": "/plenoai/saas-scraper/blob/main/README.md",
                "name": "README.md",
                "kind": "blob",
            }
        ],
        # _discover_conversations path: list_conversation_items page 1
        [{"number": "7", "title": "title"}],
        # page 2 (empty → break)
        [],
    ]

    async def evaluate(_: str) -> object:
        return discover_pages.pop(0)

    code_page = FakePage(js_results={})
    code_page.evaluate = evaluate  # type: ignore[assignment, method-assign]
    issues_page = FakePage(js_results={})
    issues_page.evaluate = evaluate  # type: ignore[assignment, method-assign]
    session = session_with_pages([code_page, issues_page])
    conn = GitHubConnector(
        session=session,
        owner="plenoai",
        repo="saas-scraper",
        resources={"code", "issues"},
    )
    refs = [r async for r in conn.discover(SourceFilter())]
    paths = [r.path for r in refs]
    assert paths == ["README.md", "issues/7"]
    assert refs[0].metadata["resource_type"] == "code"
    assert refs[1].metadata["resource_type"] == "issue"


@pytest.mark.asyncio
async def test_fetch_issue_concatenates_title_and_bodies() -> None:
    """fetch() on an issue ref joins title + comments into one Document.text."""

    async def evaluate(_: str) -> dict[str, object]:
        return {
            "title": "Leaked AKIA token in CI",
            "bodies": ["I accidentally pasted AKIAIOSFODNN7EXAMPLE", "thanks for reporting"],
            "diffs": [],
        }

    page = FakePage(js_results={})
    page.evaluate = evaluate  # type: ignore[assignment, method-assign]
    session = session_with_page(page)
    conn = GitHubConnector(session=session, owner="plenoai", repo="saas-scraper")
    from saas_scraper.core import DocumentRef

    ref = DocumentRef(
        source_id="github:plenoai/saas-scraper",
        source_kind="github",
        path="issues/1",
        native_url="https://github.com/plenoai/saas-scraper/issues/1",
        content_type="text/html",
        metadata={"resource_type": "issue", "number": "1", "title": "Leaked AKIA token in CI"},
    )
    docs = [d async for d in conn.fetch(ref)]
    assert len(docs) == 1
    text = docs[0].text or ""
    assert "Leaked AKIA token in CI" in text
    assert "AKIAIOSFODNN7EXAMPLE" in text
    assert "thanks for reporting" in text


@pytest.mark.asyncio
async def test_fetch_pr_includes_diff_text() -> None:
    """PR fetch concatenates title + bodies + diff hunks."""

    async def evaluate(_: str) -> dict[str, object]:
        return {
            "title": "ci: rotate CI token",
            "bodies": ["Replaces the leaked token."],
            "diffs": ["-OLD_TOKEN=ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "+NEW_TOKEN=<from-vault>"],
        }

    page = FakePage(js_results={})
    page.evaluate = evaluate  # type: ignore[assignment, method-assign]
    session = session_with_page(page)
    conn = GitHubConnector(session=session, owner="plenoai", repo="saas-scraper")
    from saas_scraper.core import DocumentRef

    ref = DocumentRef(
        source_id="x",
        source_kind="github",
        path="pull/42",
        metadata={"resource_type": "pr"},
    )
    docs = [d async for d in conn.fetch(ref)]
    text = docs[0].text or ""
    assert "ci: rotate CI token" in text
    assert "Replaces the leaked token." in text
    assert "OLD_TOKEN=ghp_" in text
    assert "<from-vault>" in text


@pytest.mark.asyncio
async def test_default_resources_unchanged() -> None:
    """Default resources={code} keeps backwards-compat: an instance built
    without resources= behaves identically to v0.4."""
    conn = GitHubConnector(
        session=session_with_page(FakePage()),
        owner="plenoai",
        repo="saas-scraper",
    )
    assert conn.resources == frozenset({"code"})
