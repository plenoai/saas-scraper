# saas-retriever

API-first SaaS content retriever. Yields a uniform `Document` stream
from SaaS providers via their official APIs — **no scraping, no
Playwright, no Chrome**. Downstream pipelines
([pleno-anonymize](https://github.com/plenoai/pleno-anonymize),
[pleno-secret-scanner](https://github.com/plenoai/pleno-secret-scanner))
consume the same `Document` shape they always have.

> **Heads up:** `saas-retriever` is the API-only successor of
> `saas-scraper` (PyPI 0.1–0.5, deprecated). The browser-driven
> connectors are gone; everything in this package goes through
> documented APIs. Pin `saas-scraper<=0.5` if you specifically need the
> old behaviour.

## Install

```sh
uv add saas-retriever
# or, as a CLI:
pipx install saas-retriever
```

## Usage

### CLI

```sh
# Org-wide GitHub scan (default = code + issues + PRs across every repo)
GITHUB_TOKEN=ghp_... saas-retriever fetch github --owner plenoai

# Single repo, only issues
saas-retriever fetch github --owner plenoai --repo saas-retriever \
    --resource issues

# Filter to recently-updated content
saas-retriever fetch github --owner plenoai --since 7d
```

`fetch` streams Documents as NDJSON to stdout (or `--out FILE`). One
line per Document: `ref`, `text` or `binary_b64`, `fetched_at`,
`content_hash`, `created_by`, `extra`.

### Programmatic

```python
import asyncio
from saas_retriever import registry

async def main() -> None:
    gh = registry.create(
        "github",
        owner="plenoai",
        resources={"code", "issues", "prs"},
    )
    try:
        async for doc in gh.discover_and_fetch():
            kind = doc.ref.metadata.get("resource_type")
            print(kind, doc.ref.path, len(doc.text or ""))
    finally:
        await gh.close()

asyncio.run(main())
```

## Auth

Token resolution order:

1. `token=` constructor argument (`--token` on the CLI)
2. `GITHUB_TOKEN` environment variable
3. `gh auth token` if the GitHub CLI is on PATH

Anonymous (token-less) requests work for public content but are
rate-limited to 60/h — fine for a smoke test, not enough for an
org-wide scan. Use a fine-grained PAT with `metadata:read` +
`contents:read` + `issues:read` + `pull_requests:read` for the minimum
viable scope.

## Connectors

| Connector | Status | What it covers |
|---|---|---|
| **github** | implemented (v0.1) | Org-wide repo enumeration + per-repo code (recursive tree), issues (title + body + comments), pull requests (title + body + comments + review comments + diff). Default: all three resources. |

Slack, Jira, Confluence, Notion, GitLab, Bitbucket land in subsequent
releases as standalone API-based connectors. The `Document` /
`DocumentRef` / `Connector` protocol is stable and downstream consumers
won't need to change.

## Rate-limit handling

The connector reads `X-RateLimit-Remaining` / `X-RateLimit-Reset` and
sleeps until the bucket resets on `403` secondary rate-limit
responses. Hard `429`s honour `Retry-After`. 5xx errors retry with
exponential backoff (3 attempts).

## Development

```sh
uv sync --all-extras
uv run ruff check
uv run mypy src
uv run pytest
```

The default `pytest` pass uses `httpx.MockTransport` for every HTTP
call — no live API access in CI. A live smoke test against a real
public org runs as a manual step before release.

## Release

`vX.Y.Z` tag pushes trigger PyPI trusted publishing via GitHub Actions
— no manual token. The first publish requires a one-time Trusted
Publisher configuration at <https://pypi.org/manage/account/publishing/>:

| Field | Value |
| --- | --- |
| PyPI Project Name | `saas-retriever` |
| Owner | `plenoai` |
| Repository name | `saas-retriever` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

## License

AGPL-3.0-or-later.
