"""Document / Connector protocol — wire contract for every connector.

Aligned with pleno-anonymize's `pleno_pii_scanner.sources.base` so a single
Document type can flow into either pipeline without translation.

We drop `IncrementalSourceConnector` (subsource fingerprinting) for the
v0.1.0 surface; the GitHub API connector ships with no cursor support
yet, but every provider's API exposes etags / since timestamps that a
later revision can plug into ``discover()`` without breaking this
protocol.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class Principal:
    """Identity that produced or owns a document."""

    id: str
    display_name: str | None = None
    email: str | None = None


@dataclass(frozen=True, slots=True)
class SourceFilter:
    """Discover-time include / exclude / since filter.

    Connectors apply server-side when the provider supports it (Slack
    `oldest=`, Jira `updated >= since`); otherwise they apply client-side
    so behaviour is uniform.
    """

    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    since: datetime | None = None
    max_size: int | None = None


@dataclass(frozen=True, slots=True)
class DocumentRef:
    """Cheap metadata-only handle.

    Holds enough information to render a partial finding location even
    before the body is available. Compatible with pleno-anonymize's
    DocumentRef shape so the two pipelines can share fingerprints.
    """

    source_id: str
    source_kind: str
    path: str
    native_url: str | None = None
    parent_chain: tuple[str, ...] = ()
    content_type: str = "text/plain"
    size: int | None = None
    etag: str | None = None
    last_modified: datetime | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    def fingerprint(self) -> str:
        """Stable 32-char hex hash for downstream dedup keys."""
        h = sha256()
        h.update(self.source_id.encode())
        h.update(b"\0")
        h.update(self.source_kind.encode())
        h.update(b"\0")
        h.update(self.path.encode())
        if self.etag:
            h.update(b"\0")
            h.update(self.etag.encode())
        return h.hexdigest()[:32]


@dataclass(frozen=True, slots=True)
class Document:
    """Full payload returned by a connector for a single document.

    Exactly one of `text` / `binary` is populated — enforced in
    `__post_init__`. Streaming chunks are out of scope for v0.1.0; raise
    or pre-bound on max_size in the connector when the body is too large.
    """

    ref: DocumentRef
    text: str | None = None
    binary: bytes | None = None
    fetched_at: datetime | None = None
    content_hash: str | None = None
    created_by: Principal | None = None
    extra: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (self.text is None) == (self.binary is None):
            raise ValueError(
                "Document must populate exactly one of `text` or `binary`; "
                f"got text={self.text is not None}, binary={self.binary is not None}"
            )


@runtime_checkable
class Connector(Protocol):
    """Contract every connector implements.

    Construction is the connector's responsibility — the registry forwards
    provider-specific kwargs (token, owner, base_url, ...). Connectors
    own their own HTTP client; there is no shared session in v0.1.x.

    Connectors must be safe to call from a single asyncio task. Concurrent
    use of one connector instance is undefined; create one per worker.
    """

    id: str
    kind: str

    def discover(self, filter: SourceFilter) -> AsyncIterator[DocumentRef]:
        """Enumerate document refs matching `filter`. Metadata only.

        Cheap-as-possible. Connectors paginate the provider's UI, parse
        the listing, and yield refs. No payload download.
        """
        ...

    def fetch(self, ref: DocumentRef) -> AsyncIterator[Document]:
        """Retrieve the payload for one ref. Single-document async iter.

        Yields once for in-memory documents. Async iter (rather than a
        plain coroutine) leaves room for chunk streaming in a later
        major version without breaking the protocol.
        """
        ...

    def discover_and_fetch(
        self, filter: SourceFilter | None = None
    ) -> AsyncIterator[Document]:
        """Convenience compound that yields full Documents end-to-end.

        Default implementation drives `discover` then `fetch` per ref;
        connectors with a more efficient single-pass flow (e.g. an HTML
        listing that already embeds the payload) override this.
        """
        ...

    async def close(self) -> None:
        """Release per-connector resources (HTTP clients, sockets)."""
        ...
