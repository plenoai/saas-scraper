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

| Connector | Status |
|---|---|
| github | scaffolded |
| gitlab | scaffolded |
| slack | scaffolded |
| jira | scaffolded |
| confluence | scaffolded |
| notion | scaffolded |
| bitbucket | scaffolded |

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

## Release

`vX.Y.Z` tag pushes trigger PyPI trusted publishing via GitHub Actions. No
manual PyPI token. See `.github/workflows/release.yml`.

## License

AGPL-3.0-or-later.
