# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Planned

- Real Slack message scrape (channel walk + virtualised list extraction).
- Notion page-tree walk and block-level content capture.
- GitHub repo / issue / PR / wiki / gist scrape with SSO inheritance.
- Integration test harness (Playwright + recorded HAR fixtures).

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

[Unreleased]: https://github.com/plenoai/saas-scraper/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/plenoai/saas-scraper/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/plenoai/saas-scraper/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/plenoai/saas-scraper/releases/tag/v0.1.0
