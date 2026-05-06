"""saas-scraper — Chrome-driven SaaS content scraper.

Public API:

* `Document`, `DocumentRef`, `Connector` — the wire contract every connector
  honours. Downstream pipelines (pleno-anonymize, pleno-secret-scanner)
  consume this contract directly.
* `BrowserSession` — a Playwright-backed Chrome session manager with
  persistent profile reuse so SSO / MFA logins survive across runs.
* `registry` — a mapping of connector names to factories.

Versioning: SemVer on the public API surface above. Connector internals
(per-provider selectors, page navigation) are *not* covered — they may shift
between minor versions as upstream UIs evolve.
"""

# Importing the connectors package triggers each connector's module-level
# `registry.register` call. Doing it here means `import saas_scraper` is
# enough — programmatic users don't have to remember a second import to
# populate the registry. Selective opt-in is still possible by importing
# individual connector modules instead of the top-level package.
from saas_scraper import connectors as _connectors  # noqa: F401
from saas_scraper.browser import BrowserSession
from saas_scraper.core import (
    Connector,
    Document,
    DocumentRef,
    Principal,
    SourceFilter,
)
from saas_scraper.registry import registry

__all__ = [
    "BrowserSession",
    "Connector",
    "Document",
    "DocumentRef",
    "Principal",
    "SourceFilter",
    "registry",
]

__version__ = "0.2.0"
