# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Planned

- GitHub wiki / gist scrape under the same connector (companion to
  v0.5.0 issue/PR support).
- Notion sidebar scroll-walking (helper exists; not yet wired into
  ``NotionConnector``).
- Integration test harness (Playwright + recorded HAR fixtures).

## [0.5.0] - 2026-05-06

### Added

- ``GitHubConnector`` learns ``resources={"code", "issues", "prs"}``
  (default ``{"code"}`` for backwards compat). One ``DocumentRef`` per
  issue / PR with ``metadata["resource_type"]`` and ``metadata["title"]``;
  ``fetch()`` returns title + body + every visible comment as a single
  ``Document.text``. PR fetch additionally captures inline diff hunks.
- Issue / PR enumeration paginates via ``?page=N`` until the response is
  empty or ``max_issue_pages`` (default 20 ≈ 500 items) is reached.
- Construction validates the resources set; an unknown resource raises
  ``ValueError`` with the supported list so a typo never silently
  scans nothing.

### Changed

- ``GitHubConnector.discover()`` yields ``code`` refs first (when
  enabled), then issues, then PRs. Existing callers see no change.

### Known limits

- Conversation extraction reads currently-rendered comments only; long
  threads with "show more" require a follow-up scroll pass.
- Wiki and gist scrape are still pending.

## [0.4.0] - 2026-05-06

### Added

- ``saas_scraper.connectors._scroll.scroll_collect`` — generic
  virtual-list helper. Scrolls a container, harvests rows after each
  settle, dedups by caller-supplied key, terminates on a stable cycle
  / max-iterations / predicate. PEP 695 type parameter syntax.
- Slack ``discover()`` now uses ``scroll_collect`` to walk the full
  channel sidebar instead of only the currently-rendered portion.

### Changed

- N/A (backwards compatible; existing callers see more channels).

## [0.3.0] - 2026-05-06

### Added

- **GitHub connector** real implementation: file-tree walk via the web
  UI with depth bound, raw-blob fetch, ``NotLoggedInError`` race for
  private/SAML repos, text/binary auto-detection.
- **GitLab connector** real implementation: same shape as GitHub, works
  against gitlab.com or self-hosted via ``base_url``.
- **Bitbucket** real implementation: ``src/`` walk with ``raw/`` fetch.
- **Jira** real implementation: issue-list walk, full issue page text.
- **Confluence** real implementation: space page-tree walk, page body.
- **Notion** real implementation: sidebar page enumeration, page body.
- ``saas_scraper.connectors._base`` now exposes shared
  ``NotLoggedInError``, ``wait_for_signed_in_or_raise``, ``glob_match``,
  and ``apply_name_filter`` helpers so each connector is just selectors
  + URLs without re-implementing the SSO race.
- Unit tests for every connector via ``tests/_fake_page.py`` — no real
  Chromium needed for CI.

### Changed

- Slack connector consolidated onto the shared ``_base.py`` helpers;
  ``NotLoggedInError`` re-exported from ``slack`` for backwards compat.

### Known limits

- Virtual-list scrolling is still pending. Workspaces with many sidebar
  channels, long message histories, or large Notion sidebars only see
  the currently-visible portion. v0.4.0 adds scroll-walking.
- Selectors target each provider's current web UI. A UI shift requires
  bumping the constants at the top of the connector module.

## [0.2.0] - 2026-05-06

### Added

- **Slack connector**: real implementation that opens
  ``https://<workspace>.slack.com/`` via the persistent BrowserSession,
  detects the not-logged-in state vs. mounted client by racing the
  sidebar selector against the login form, and returns visible channels
  from ``discover()`` and visible message-pane text from ``fetch()``.
- ``NotLoggedInError`` exception with an actionable "run once with
  ``--headed``" message.
- ``filter.include`` / ``filter.exclude`` glob support during discovery.
- Test suite for the connector via a fake Page that records ``goto`` /
  ``wait_for_selector`` and dispatches ``evaluate`` results — no real
  Chromium required for unit tests.

### Known limits

- Sidebar and message pane reads are virtualised: only currently-visible
  channels / messages are captured. Scroll-walking lands in v0.3.0.
- No threaded-reply or attachment handling yet.

## [0.1.3] - 2026-05-06

### Fixed

- `import saas_scraper` no longer leaves `registry.names()` empty.
  Connector self-registration is triggered from the package
  `__init__.py` so callers don't need a second import to populate the
  registry.

### Added

- Test guarding the import-side-effect contract above.

## [0.1.2] - 2026-05-06

### Changed

- Re-tag of 0.1.1 after PyPI Pending Publisher was registered. Functional
  scope identical to 0.1.1; this is the first release that actually
  reaches PyPI.

## [0.1.1] - 2026-05-06

### Fixed

- CI release pipeline: switched `pypa/gh-action-pypi-publish` from a SHA
  reference to `v1.12.4` because the action's container image is tagged
  with git tags (not commit SHAs); SHA pinning yielded `manifest unknown`
  on `ghcr.io`.

## [0.1.0] - 2026-05-06

Initial scaffold.

### Added

- `Document`, `DocumentRef`, `SourceFilter`, `Principal` data classes,
  aligned with `pleno-anonymize`'s `pleno_pii_scanner.sources.base` so a
  single Document type flows into either pipeline without translation.
- `Connector` runtime-checkable Protocol + `BaseConnector` ABC with a
  default `discover_and_fetch` flow.
- `BrowserSession`: persistent-profile Chromium session via Playwright,
  resolving the profile directory from constructor arg / env / XDG fallback.
- Registry with last-write-wins semantics so downstream packages can swap
  built-in connectors for hardened versions.
- Seven connector scaffolds (github, gitlab, slack, jira, confluence,
  notion, bitbucket) — protocol-compliant, registry-wired, with v0.2+
  scrape logic deferred.
- Typer CLI: `saas-scraper list`, `saas-scraper fetch <connector>`,
  `saas-scraper version`. NDJSON output of `Document` records.
- GitHub Actions: ruff + mypy + pytest matrix on Python 3.12 / 3.13.
- Tag-pushed PyPI trusted publishing via `pypa/gh-action-pypi-publish`.
- Dependabot for `github-actions` and `pip`.

[Unreleased]: https://github.com/plenoai/saas-scraper/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/plenoai/saas-scraper/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/plenoai/saas-scraper/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/plenoai/saas-scraper/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/plenoai/saas-scraper/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/plenoai/saas-scraper/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/plenoai/saas-scraper/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/plenoai/saas-scraper/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/plenoai/saas-scraper/releases/tag/v0.1.0
