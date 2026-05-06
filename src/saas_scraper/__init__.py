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

__version__ = "0.1.2"
