# saas-scraper

Chrome-driven SaaS content scraper. Yields a uniform `Document` stream for
downstream pipelines (e.g. [pleno-anonymize](https://github.com/plenoai/pleno-anonymize),
[pleno-secret-scanner](https://github.com/plenoai/pleno-secret-scanner)).

Where API-based connectors stop — locked-down workspaces, SSO-only sessions,
content only visible in the UI — `saas-scraper` keeps going by driving a real
Chrome session via Playwright. Reuses your existing browser profile so login,
MFA and SSO flows are inherited rather than re-implemented per provider.

## Install

```sh
uv add saas-scraper
# one-time browser binary install
uv run playwright install chromium
```

Or as a CLI:

```sh
pipx install saas-scraper
playwright install chromium
```

## Usage

```sh
# List available connectors
saas-scraper list

# Scrape a Slack workspace and stream Documents to stdout (NDJSON)
saas-scraper fetch slack --workspace acme --since 7d

# Save to a file for downstream consumption
saas-scraper fetch notion --workspace acme > docs.ndjson
```

Programmatic use:

```python
import asyncio
from saas_scraper import BrowserSession, registry

async def main() -> None:
    async with BrowserSession() as session:
        connector = registry.create("slack", session=session, workspace="acme")
        async for doc in connector.discover_and_fetch():
            print(doc.ref.path, len(doc.text or b""))

asyncio.run(main())
```

## Connectors

| Connector | Status | Notes |
|---|---|---|
| slack | implemented (v0.2) | channel sidebar walk, message pane scrape |
| github | implemented (v0.5) | code (file tree) + issues + PRs (title/body/comments/diff). Pass `resources={"code","issues","prs"}` |
| gitlab | implemented (v0.3) | gitlab.com or self-hosted via `base_url` |
| bitbucket | implemented (v0.3) | bitbucket.org file walk |
| jira | implemented (v0.3) | Atlassian Cloud issue list + body |
| confluence | implemented (v0.3) | Atlassian Cloud space page-tree |
| notion | implemented (v0.3) | sidebar page enumeration + body |

All connectors share a single `BrowserSession` so cookies and SSO state
inherit across providers. Virtualised lists (Slack sidebar, Notion
sidebar) only see the currently-visible portion in v0.3 — scroll-walking
landed in v0.4. GitHub issue / PR scrape landed in v0.5.

### GitHub multi-resource example

```python
async with BrowserSession() as session:
    gh = registry.create(
        "github",
        session=session,
        owner="plenoai",
        repo="saas-scraper",
        resources={"code", "issues", "prs"},
    )
    async for doc in gh.discover_and_fetch():
        kind = doc.ref.metadata.get("resource_type")
        print(kind, doc.ref.path, len(doc.text or ""))
```

`metadata["resource_type"]` is one of `code`, `issue`, `pr`. Issue and
PR documents concatenate title + body + every visible comment (PRs also
include the inline diff hunks) into a single `Document.text` so the
downstream secret/PII scanners run unchanged.

The v0.1.0 release ships the `Document` protocol, the Chrome session manager,
and a working scaffold per connector. Additional providers and per-connector
hardening land in subsequent releases — see [issues](https://github.com/plenoai/saas-scraper/issues).

## Why Chrome and not the API?

- **Inherits SSO / MFA / SCIM-locked sessions** that don't cleanly expose API
  tokens to a scanner role.
- **Bypasses API quota tiers** that throttle org-wide content enumeration.
- **Reaches UI-only surfaces** (Notion comments, Slack canvas, Jira views).

When an official API exists and is sufficient, prefer that — `saas-scraper`
is the fallback for the cases where it isn't.

## Development

```sh
uv sync --all-extras
uv run playwright install chromium
uv run pytest
uv run ruff check
uv run mypy src
```

The default `pytest` pass exercises plumbing only (Document protocol,
registry wiring, CLI helpers). Live browser scrapes against real SaaS
providers are not part of CI; run them locally with
`saas-scraper fetch <connector> --headed` so a real Chromium window
opens for first-time SSO.

## Release

`vX.Y.Z` tag pushes trigger PyPI trusted publishing via GitHub Actions —
no manual token. The first publish requires a one-time Trusted Publisher
configuration at <https://pypi.org/manage/account/publishing/>:

| Field | Value |
| --- | --- |
| PyPI Project Name | `saas-scraper` |
| Owner | `plenoai` |
| Repository name | `saas-scraper` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

After that, every tag matching `v*` will publish automatically.

## License

AGPL-3.0-or-later.
