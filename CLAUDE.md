# saas-scraper

Chrome-driven SaaS content scraper. Yields a uniform Document protocol so
downstream pipelines (pleno-anonymize, pleno-secret-scanner) can scan
content from connectors that don't expose adequate APIs.

## Workflow rules

- Python 3.12+, packaged with hatchling, managed with `uv`.
- All connectors live under `src/saas_scraper/connectors/<name>.py` and
  self-register in their module-level code via `registry.register`.
- The Document / DocumentRef / Connector protocol in `core.py` is
  SemVer-stable. Connector internals are not — selectors and page
  navigation may shift between minor versions as upstream UIs evolve.
- Tests in `tests/` must pass `uv run pytest`. Connectors that hit a real
  browser must be marked and skipped by default; the default test pass
  exercises plumbing only.
- Releases are triggered exclusively by `vX.Y.Z` tag pushes that fan out
  to PyPI via GitHub Actions trusted publishing. `main` push runs tests
  only.

## Change history

| Date | Change | Target | Reason |
|------|--------|--------|--------|
| 2026-05-06 | Initial scaffold (Document protocol, BrowserSession, 7 connector stubs, CLI, CI/release pipeline) | repo-wide | Spun up as a shared SaaS content layer for pleno-anonymize and pleno-secret-scanner |
