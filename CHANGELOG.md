# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-05-07

The public protocol surface is now SemVer-stable: ``Connector`` /
``IncrementalConnector`` / ``Capabilities`` / ``Cursor`` /
``DocumentChunk`` / ``Subsource`` / ``Credential`` / ``RateLimited``.
Per-connector configuration knobs (rate-limit budgets, retry counts,
page sizes) are not part of the SemVer surface and may be tuned
between minors.

### Added — connectors

- ``GitHubConnector`` — owner-wide or single-repo. Resources:
  ``code`` (recursive Git Tree API + raw blob fetch), ``issues``
  (title + body + comments), ``prs`` (title + body + conversation
  comments + review comments + unified diff).
- ``BitbucketConnector`` — Cloud (REST 2.0) + Server (REST 1.0)
  flavors selected at construction. Resources: ``code`` (depth-first
  ``/src`` walk on Cloud, flat ``/files`` walk on Server), ``prs``
  (both flavors), ``issues`` (Cloud only — Server has no native
  issue tracker; downgraded silently).
- ``GitLabConnector`` — SaaS gitlab.com + self-managed CE/EE.
  Targets: ``project="ns/path"`` or ``group="ns"`` (recursive
  subgroup walk). Resources: ``code`` (recursive Tree API + raw file
  fetch), ``issues`` (description + non-system notes), ``mrs``
  (description + notes + per-file diff). Three auth modes: PAT,
  project access token, OAuth2.
- ``NotionConnector`` — pinned ``Notion-Version: 2022-06-28``. Three
  discovery modes (search / explicit page list / database query) that
  may be combined. Block tree + database row properties materialised
  into Markdown via the vendored ``notion_markdown`` converter.
- ``ConfluenceConnector`` — Cloud + Data Center, single kind. Pipeline
  enumerates spaces → pages → comments + attachment refs; storage
  format XHTML → text via the vendored ``confluence_storage``
  converter. Cursor anchored on ``version.when``; honours
  429 (Cloud) and 503 (DC reverse-proxy) throttle signals.
- ``JiraConnector`` — Cloud (``/rest/api/3`` + ADF body) and Data
  Center (``/rest/api/2`` + storage XHTML body). Pipeline enumerates
  projects → JQL ``updated >= cursor`` per project → comments per
  issue. Attachment URLs surfaced; bodies never downloaded.
  Three auth modes: ``access_token`` (Bearer), Cloud
  ``email`` + ``api_token`` (Basic), DC ``username`` + ``password``
  (Basic).
- ``SlackConnector`` — REST-only, no third-party SDK. Supports
  ``xoxb-`` (bot) and ``xoxp-`` (user) tokens. Pipeline:
  ``conversations.list`` → ``conversations.history`` per channel
  (server-side ``oldest=`` for incremental) → ``conversations.replies``
  for threaded messages. ``users.info`` resolves a ``Principal``
  on demand (with cache).

### Added — protocol

- ``Capabilities`` — connector self-description: ``incremental``,
  ``binary``, ``content_hash_delta``, ``max_concurrent_fetches``,
  ``streaming``.
- ``Cursor`` — opaque per-connector resume token (str). Persisted
  verbatim by callers and round-tripped through
  ``discover(filter, cursor=...)``.
- ``DocumentChunk`` — streamed slice of a document payload, with
  ``byte_range`` and ``is_final``. Yielded in order by ``fetch()``
  for documents that exceed the in-memory size budget.
- ``Subsource`` + ``SUBSOURCE_METADATA_KEY`` (``"_subsource_id"``) —
  sub-unit fingerprinting for hierarchical sources (org → repos,
  workspace → channels, …).
- ``IncrementalConnector`` (``runtime_checkable`` Protocol) — optional
  extension that exposes ``list_subsources()`` and
  ``set_subsource_skip(frozenset[str])``.

### Added — rate limit + credentials

- ``AdaptiveTokenBucket`` + ``GlobalRateLimiter`` — AIMD bucket
  keyed on ``BucketKey(connector_kind, tenant_id)``. Connectors raise
  ``RateLimited`` on persistent throttle so the bucket can shrink
  the effective rate.
- ``Credential`` dataclass with auto-redacting ``__repr__`` /
  ``__str__`` for any payload key matching token / secret / password /
  private / session / pem.
- ``CredentialError``, ``CredentialNotFoundError``,
  ``CredentialMisconfiguredError`` — typed errors raised at
  construction so misconfigured profiles fail loudly instead of
  silently 401-ing every request.

### Tested

- 205 hermetic tests, every HTTP call routed through
  ``httpx.MockTransport`` — no live API access in CI.
- ``ruff`` clean, ``mypy --strict`` clean.

[Unreleased]: https://github.com/plenoai/saas-retriever/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/plenoai/saas-retriever/releases/tag/v1.0.0
