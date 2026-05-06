"""saas-retriever — API-first SaaS content retriever.

Yields a uniform ``Document`` stream from SaaS providers via their
official APIs (no scraping, no Playwright, no Chrome). Downstream
pipelines (pleno-anonymize, pleno-secret-scanner) consume the same
``Document`` shape they always have.

Public API:

* ``Document``, ``DocumentRef``, ``Connector``, ``Principal``,
  ``SourceFilter`` — the wire contract every connector honours.
* ``registry`` — a mapping of connector names to factories. Importing
  this package registers every built-in connector (currently: GitHub).

Versioning: SemVer on the public API surface above. Per-connector
configuration knobs (rate-limit budgets, retry counts) are not part of
the SemVer surface and may be tuned between minors.
"""

# Importing the connectors package triggers each connector's module-level
# `registry.register` call. Doing it here means `import saas_retriever`
# is enough — programmatic users don't have to remember a second import
# to populate the registry.
from saas_retriever import connectors as _connectors  # noqa: F401
from saas_retriever.core import (
    Connector,
    Document,
    DocumentRef,
    Principal,
    SourceFilter,
)
from saas_retriever.registry import registry

__all__ = [
    "Connector",
    "Document",
    "DocumentRef",
    "Principal",
    "SourceFilter",
    "registry",
]

__version__ = "0.1.0"
