# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Planned

- Slack API connector (Conversations + Files + Discovery API).
- Jira / Confluence Cloud API connectors.
- Notion API connector.
- GitLab + Bitbucket API connectors.

## [0.1.0] - 2026-05-06

Initial API-first release. Hard reboot of the package: the previous
``saas-scraper`` codebase shipped browser-driven scrapers (Playwright +
Chrome) for seven providers. That entire approach is gone — this
package goes through documented APIs only.

If you specifically need the old browser-based behaviour, pin
``saas-scraper<=0.5``. Future releases of ``saas-retriever`` will not
ship Playwright.

### Added

- ``GitHubConnector`` — REST-API driven, replaces every former
  scraping path:
  - **Org-wide enumeration** is the default. Construct with
    ``owner=plenoai`` (no ``repo``) to walk every repository under that
    org via ``/orgs/{owner}/repos`` (falling back to
    ``/users/{owner}/repos`` for personal accounts). Archived repos are
    skipped unless ``include_archived=True``.
  - **Per-repo resources**: ``{"code", "issues", "prs"}``, all enabled
    by default. Refs carry ``metadata["resource_type"]`` so downstream
    pipelines can dispatch.
  - **Code**: recursive Git Tree API. ``fetch`` returns raw blob bytes
    via ``/git/blobs/{sha}`` with ``Accept: application/vnd.github.raw``;
    UTF-8 decoded when valid, binary fallback otherwise.
  - **Issues**: title + body + every issue comment, joined into one
    ``Document.text``. PRs surfaced through ``/issues`` are filtered
    out so they aren't double-emitted.
  - **Pull requests**: title + body + conversation comments + review
    comments + the unified diff via ``Accept: application/vnd.github.diff``.
- Auth: ``token=`` arg → ``GITHUB_TOKEN`` env → ``gh auth token``.
  Anonymous works for public content (60/h cap).
- Pagination via ``Link`` header. Rate-limit handling reads
  ``X-RateLimit-Reset`` / ``Retry-After`` and sleeps; 5xx retries with
  exponential backoff (3 attempts).
- Test suite (37 tests) drives every code path through
  ``httpx.MockTransport`` — no live API access in CI.
- Live smoke test against ``plenoai`` org on real api.github.com:
  org-wide enumeration, code blob fetch, issue body retrieval, PR
  diff retrieval, all confirmed end-to-end.

### Removed

- ``saas_scraper`` package, ``BrowserSession``, every Playwright
  selector, ``_fake_page.py`` test harness, scroll-walking helper, the
  six remaining browser connectors (slack / gitlab / bitbucket / jira /
  confluence / notion). Each will return as an API-based connector.

### Renamed

- PyPI project: ``saas-scraper`` → ``saas-retriever``.
- Repository: ``plenoai/saas-scraper`` → ``plenoai/saas-retriever``.
- Python module: ``saas_scraper`` → ``saas_retriever``.
- CLI: ``saas-scraper`` → ``saas-retriever``.

[Unreleased]: https://github.com/plenoai/saas-retriever/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/plenoai/saas-retriever/releases/tag/v0.1.0
