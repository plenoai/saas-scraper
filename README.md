# saas-retriever

API-first SaaS content retriever. Yields a uniform `Document` stream
from seven SaaS providers via their official REST APIs. Downstream
pipelines ([pleno-anonymize](https://github.com/plenoai/pleno-anonymize),
[pleno-dlp](https://github.com/plenoai/pleno-dlp)) consume the same
`Document` shape regardless of which provider produced it.

## Install

```sh
uv add saas-retriever
# or, as a CLI:
pipx install saas-retriever
```

## Connectors

| kind | targets | resources |
|---|---|---|
| **github** | org or single repo | code, issues, pull requests (title + body + comments + diff) |
| **gitlab** | group (recursive) or single project | code, issues, merge requests (with per-file diff) |
| **bitbucket** | Cloud workspace or Server project, optionally pinned to a repo | code, pull requests, issues (Cloud only) |
| **notion** | search / explicit pages / database query (combinable) | page tree + database row properties → Markdown |
| **confluence** | Cloud or Data Center; spaces enumerated then pages | page body (storage XHTML → text) + comments + attachment refs |
| **jira** | Cloud (`/rest/api/3` + ADF) or Data Center (`/rest/api/2` + storage XHTML) | issues + comments + attachment URLs |
| **slack** | xoxb (bot) or xoxp (user) tokens | channels → history → threads → optional file refs |

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

Every connector exposes the same `Connector` protocol — swap `"github"`
for `"gitlab"`, `"slack"`, etc. and the loop above keeps working.

## Auth

Each connector accepts either a typed `Credential` or the discrete
constructor kwargs (`token=`, `username=`, `email=`, `api_token=`, …).
Credential payload keys are auto-redacted in `repr`/`str`.

| connector | accepted credential shapes |
|---|---|
| github | `token=` (PAT). CLI also resolves `GITHUB_TOKEN` env var or `gh auth token`. |
| gitlab | `token=` + `auth=` ∈ {`pat`, `project`, `oauth`}. Bearer for OAuth, `PRIVATE-TOKEN` otherwise. |
| bitbucket | Cloud: `token=` (Bearer) or `username=`/`app_password=` (Basic). Server: `token=` or `username=`/`password=`. |
| notion | `token=` (Bearer integration token). |
| confluence | Cloud: `token=` (Bearer) or `email=`/`api_token=` (Basic). DC: `token=` (Bearer PAT) or `username=`/`password=`. |
| jira | `access_token=` (Bearer); Cloud: `email=`/`api_token=`; DC: `username=`/`password=`. |
| slack | `token=` (xoxb-… or xoxp-…). |

## Cursors and incremental scans

Connectors that advertise `Capabilities.incremental` round-trip an
opaque resume token through `discover(filter, cursor=...)`:

* **gitlab / github** — server-side filters where available.
* **confluence / jira** — JSON cursor anchored on `version.when` /
  `updated`. Stale or malformed cursors fall back to a full re-walk.
* **slack** — JSON `{channel_id: latest_ts}` per channel, fed back into
  Slack's `oldest=` parameter.
* **notion** — search cursor round-tripped on every emitted ref via
  `metadata["_cursor"]`.

Persist `cursor_after_run()` (when the connector exposes it) and pass
the same string back on the next scan to resume.

## Rate limiting

`saas_retriever.AdaptiveTokenBucket` + `GlobalRateLimiter` provide an
AIMD bucket per `BucketKey(connector_kind, tenant_id)`. Connectors
raise `RateLimited` on persistent throttle (429 on most providers,
plus 503 on Atlassian Data Center where their reverse proxy emits
overload signals over 429 by policy). Callers can shrink the
effective rate via `on_throttle_signal(factor=0.5)` and grow it back
with `on_success(recovery=...)`.

## Development

```sh
uv sync --all-extras
uv run ruff check
uv run mypy src
uv run pytest
```

The default `pytest` pass uses `httpx.MockTransport` for every HTTP
call — no live API access in CI.

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
