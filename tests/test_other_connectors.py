"""Smoke tests for jira / confluence / notion / gitlab / bitbucket.

The full GitHub coverage demonstrates the FakePage pattern; the rest of
the connectors share the same shape, so we keep their tests narrow:
discover()'s URL choice and the shape of yielded refs from a canned JS
result. Detailed behaviour (recursion, login race) is covered once in
``test_github.py`` and ``test_slack.py`` since the helpers are shared.
"""

from __future__ import annotations

import pytest

from saas_scraper.connectors.bitbucket import BitbucketConnector
from saas_scraper.connectors.confluence import ConfluenceConnector
from saas_scraper.connectors.gitlab import GitLabConnector
from saas_scraper.connectors.jira import JiraConnector
from saas_scraper.connectors.notion import NotionConnector
from saas_scraper.core import SourceFilter
from tests._fake_page import FakePage, session_with_page


@pytest.mark.asyncio
async def test_jira_discover_yields_issue_refs() -> None:
    page = FakePage(
        js_results={
            "querySelectorAll": [
                {
                    "href": "https://acme.atlassian.net/browse/ENG-123",
                    "key": "ENG-123",
                    "summary": "Investigate pii leak",
                },
            ]
        }
    )
    session = session_with_page(page)
    conn = JiraConnector(session=session, site="acme", project="ENG")
    refs = [r async for r in conn.discover(SourceFilter())]
    assert len(refs) == 1
    assert refs[0].path == "issue:ENG-123"
    assert refs[0].metadata["summary"] == "Investigate pii leak"
    assert page.gotos == ["https://acme.atlassian.net/jira/software/projects/ENG/issues"]


@pytest.mark.asyncio
async def test_confluence_discover_yields_page_refs() -> None:
    page = FakePage(
        js_results={
            "querySelectorAll": [
                {
                    "href": "https://acme.atlassian.net/wiki/spaces/ENG/pages/12345",
                    "page_id": "12345",
                    "title": "Onboarding",
                }
            ]
        }
    )
    session = session_with_page(page)
    conn = ConfluenceConnector(session=session, site="acme", space="ENG")
    refs = [r async for r in conn.discover(SourceFilter())]
    assert refs[0].path == "page:12345"
    assert refs[0].metadata["title"] == "Onboarding"


@pytest.mark.asyncio
async def test_notion_discover_yields_page_refs() -> None:
    page = FakePage(
        js_results={
            "querySelectorAll": [
                {
                    "href": "https://www.notion.so/Onboarding-abcdef0123456789abcdef0123456789",
                    "page_id": "abcdef0123456789abcdef0123456789",
                    "title": "Onboarding",
                }
            ]
        }
    )
    session = session_with_page(page)
    conn = NotionConnector(session=session, workspace="acme")
    refs = [r async for r in conn.discover(SourceFilter())]
    assert refs[0].metadata["page_id"] == "abcdef0123456789abcdef0123456789"


@pytest.mark.asyncio
async def test_gitlab_discover_walks_root() -> None:
    page = FakePage(
        js_results={
            "querySelectorAll": [
                {
                    "href": "/g/p/-/blob/main/README.md",
                    "name": "README.md",
                    "kind": "blob",
                },
                {
                    "href": "/g/p/-/blob/main/pyproject.toml",
                    "name": "pyproject.toml",
                    "kind": "blob",
                },
            ]
        }
    )
    session = session_with_page(page)
    conn = GitLabConnector(session=session, project="g/p")
    refs = [r async for r in conn.discover(SourceFilter())]
    assert sorted(r.path for r in refs) == ["README.md", "pyproject.toml"]


@pytest.mark.asyncio
async def test_bitbucket_discover_walks_root() -> None:
    page = FakePage(
        js_results={
            "querySelectorAll": [
                {
                    "href": "/ws/repo/src/main/README.md",
                    "name": "README.md",
                    "kind": "blob",
                }
            ]
        }
    )
    session = session_with_page(page)
    conn = BitbucketConnector(session=session, workspace="ws", repo="repo")
    refs = [r async for r in conn.discover(SourceFilter())]
    assert refs[0].path == "README.md"
    raw = refs[0].native_url
    assert raw is not None
    assert raw.startswith("https://bitbucket.org/ws/repo/raw/main/")
