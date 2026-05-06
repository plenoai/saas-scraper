# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-05-07

Six new API-only connectors join GitHub. The public protocol surface
is now stable: ``Connector`` / ``Capabilities`` / ``Cursor`` /
``DocumentChunk`` / ``Subsource`` / ``Credential`` / ``RateLimited``.

### Added

- ``BitbucketConnector`` — Cloud (REST 2.0) + Server (REST 1.0)
  flavors. Resources: ``code`` (depth-first ``/src`` walk on Cloud,
  flat ``/files`` on Server), ``prs`` (both flavors), ``issues``
  (Cloud only).
- ``GitLabConnector`` — SaaS gitlab.com + self-managed CE/EE.
  Resources: ``code`` (recursive Tree API), ``issues``, ``mrs`` (with
  per-file diff). Three auth modes: PAT, project access token, OAuth2.
- ``NotionConnector`` — pinned ``Notion-Version: 2022-06-28``. Three
  discovery modes (search / explicit pages / database query). Block
  tree + database properties materialised into Markdown via the
  vendored ``notion_markdown`` converter.
- ``ConfluenceConnector`` — Cloud + Data Center, single kind. Storage
  format XHTML → text via vendored ``confluence_storage``. Cursor on
  ``version.when``; 429-Cloud + 503-DC backoff.
- ``JiraConnector`` — Cloud (``/rest/api/3`` + ADF) + Data Center
  (``/rest/api/2`` + storage XHTML). JQL ``updated >=`` for
  incremental scan; comments + attachment URLs.
- ``SlackConnector`` — REST-only (no ``slack_sdk`` dependency).
  Supports ``xoxb-`` and ``xoxp-`` tokens; ``conversations.list``
  → ``conversations.history`` → ``conversations.replies`` for threaded
  messages. Cursor is per-channel ``latest_ts``.

### Changed

- Core protocol extended for v1.0: ``Capabilities`` (incremental,
  binary, content_hash_delta, max_concurrent_fetches, streaming),
  ``DocumentChunk`` (streamed slice), ``Subsource`` +
  ``SUBSOURCE_METADATA_KEY`` (sub-unit fingerprinting),
  ``IncrementalConnector`` (optional Protocol).
- ``Connector.discover()`` now takes ``cursor: Cursor | None``;
  connectors round-trip the cursor on every ref via ``metadata["_cursor"]``.

### Added (rate limit + credentials)

- ``AdaptiveTokenBucket`` + ``GlobalRateLimiter`` — AIMD bucket per
  ``BucketKey(connector_kind, tenant_id)``. Connectors raise
  ``RateLimited`` on persistent throttle so the bucket can shrink.
- ``Credential`` dataclass with auto-redacting ``__repr__``;
  ``CredentialError`` + ``CredentialNotFoundError`` +
  ``CredentialMisconfiguredError``.

### Tested

- 205 hermetic tests passing (``httpx.MockTransport`` on every
  connector). ``ruff`` clean, ``mypy --strict`` clean.

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

[Unreleased]: https://github.com/plenoai/saas-retriever/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/plenoai/saas-retriever/releases/tag/v1.0.0
[0.1.0]: https://github.com/plenoai/saas-retriever/releases/tag/v0.1.0
