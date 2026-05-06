"""saas-scraper command-line interface.

Surface (v0.1.0):

    saas-scraper list
    saas-scraper fetch <connector> [--workspace ...] [--since 7d] [--out -]

`fetch` streams Documents as NDJSON to stdout (or `--out PATH`). Each line
is one Document: ref, text or binary (base64), fetched_at, content_hash,
created_by, extra. The schema follows `saas_scraper.core.Document` 1:1.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import sys
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from saas_scraper import __version__

# Importing the connectors package triggers per-connector registry.register
# calls. Done at module import time so `registry.names()` is populated by
# the time typer dispatches a subcommand.
from saas_scraper import connectors as _connectors  # noqa: F401
from saas_scraper.browser import BrowserSession
from saas_scraper.core import Document, SourceFilter
from saas_scraper.registry import registry

app = typer.Typer(
    name="saas-scraper",
    help="Chrome-driven SaaS content scraper.",
    no_args_is_help=True,
    add_completion=False,
)


@app.command("list")
def cmd_list() -> None:
    """List registered connectors."""
    console = Console()
    table = Table("name", "kind", title=f"saas-scraper {__version__} — registered connectors")
    for name in registry.names():
        # The factory itself is the class — pulling its `kind` class var
        # doesn't require instantiation, which keeps `list` cheap.
        kind = getattr(registry._factories[name], "kind", "?")
        table.add_row(name, kind)
    console.print(table)


@app.command("fetch")
def cmd_fetch(
    connector: str = typer.Argument(..., help="Connector name (see `saas-scraper list`)."),
    workspace: str | None = typer.Option(None, "--workspace", help="Workspace / org / site identifier."),
    project: str | None = typer.Option(None, "--project", help="Project / repo (connector-specific)."),
    since: str | None = typer.Option(None, "--since", help="Filter to docs newer than this (e.g. 7d, 24h, ISO8601)."),
    include: list[str] = typer.Option([], "--include", help="Glob/path include filter (repeatable)."),
    exclude: list[str] = typer.Option([], "--exclude", help="Glob/path exclude filter (repeatable)."),
    out: Path | None = typer.Option(None, "--out", help="Write NDJSON to this file instead of stdout."),
    headed: bool = typer.Option(False, "--headed", help="Run Chromium in headed mode (useful for first-time login)."),
    profile_dir: Path | None = typer.Option(None, "--profile-dir", help="Persistent Chrome profile directory."),
) -> None:
    """Scrape a connector and stream NDJSON Documents."""
    if connector not in registry.names():
        typer.echo(f"unknown connector: {connector!r}. available: {', '.join(registry.names())}", err=True)
        raise typer.Exit(2)

    flt = SourceFilter(
        include=tuple(include),
        exclude=tuple(exclude),
        since=_parse_since(since),
    )

    connector_kwargs: dict[str, Any] = {}
    if workspace is not None:
        # Different connectors call the equivalent slot different things.
        # Prefer the most-specific keyword the registered connector accepts;
        # the BaseConnector signature is the source of truth.
        connector_kwargs["workspace"] = workspace
    if project is not None:
        connector_kwargs["project"] = project

    asyncio.run(
        _run_fetch(
            connector=connector,
            connector_kwargs=connector_kwargs,
            filter=flt,
            out=out,
            headless=not headed,
            profile_dir=profile_dir,
        )
    )


async def _run_fetch(
    *,
    connector: str,
    connector_kwargs: dict[str, Any],
    filter: SourceFilter,
    out: Path | None,
    headless: bool,
    profile_dir: Path | None,
) -> None:
    """Drive one full discover_and_fetch and emit NDJSON.

    Kept as a private async helper rather than inlined in `cmd_fetch` so
    tests can call it without going through the typer entry point.
    """
    sink = sys.stdout if out is None else out.open("w", encoding="utf-8")
    try:
        async with BrowserSession(headless=headless, profile_dir=profile_dir) as session:
            # Strip kwargs the registered connector doesn't accept rather
            # than failing loudly — the alternative (introspecting __init__
            # signatures) is fragile against subclasses.
            kwargs = _filter_supported_kwargs(connector, connector_kwargs)
            scraper = registry.create(connector, session=session, **kwargs)
            try:
                async for doc in scraper.discover_and_fetch(filter):
                    sink.write(_encode_document(doc) + "\n")
                    sink.flush()
            finally:
                await scraper.close()
    finally:
        if out is not None:
            sink.close()


def _filter_supported_kwargs(connector: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Drop kwargs the connector's __init__ doesn't declare.

    The CLI takes a generic --workspace / --project pair; only the
    connectors that actually accept them should receive them.
    """
    factory = registry._factories[connector]
    init = getattr(factory, "__init__", None)
    if init is None:
        return {}
    code = init.__code__
    accepted = set(code.co_varnames[: code.co_argcount + code.co_kwonlyargcount])
    return {k: v for k, v in kwargs.items() if k in accepted}


_SINCE_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$")


def _parse_since(spec: str | None) -> datetime | None:
    """Parse a `--since` spec.

    Accepts a relative form (`7d`, `24h`, `30m`, `4w`) or an ISO 8601
    datetime. Returns None when `spec is None`.
    """
    if spec is None:
        return None
    if m := _SINCE_RE.match(spec):
        n = int(m.group(1))
        unit = m.group(2)
        delta = {
            "s": timedelta(seconds=n),
            "m": timedelta(minutes=n),
            "h": timedelta(hours=n),
            "d": timedelta(days=n),
            "w": timedelta(weeks=n),
        }[unit]
        return datetime.now(UTC) - delta
    try:
        return datetime.fromisoformat(spec)
    except ValueError as exc:
        raise typer.BadParameter(f"unrecognised --since value: {spec!r}") from exc


def _encode_document(doc: Document) -> str:
    """Serialise one Document to a single NDJSON line.

    Binary payloads are base64-encoded under `binary_b64`; the original
    `binary` field is dropped from the JSON to avoid invalid UTF-8.
    """
    payload: dict[str, Any] = {
        "ref": _to_jsonable(doc.ref),
        "text": doc.text,
        "binary_b64": base64.b64encode(doc.binary).decode("ascii") if doc.binary else None,
        "fetched_at": doc.fetched_at.isoformat() if doc.fetched_at else None,
        "content_hash": doc.content_hash,
        "created_by": _to_jsonable(doc.created_by) if doc.created_by else None,
        "extra": dict(doc.extra),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _to_jsonable(obj: Any) -> Any:
    """Cheap dataclass → dict; tuples flatten to lists for JSON."""
    if is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, tuple):
        return list(obj)
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    return obj


@app.command("version")
def cmd_version() -> None:
    """Print the package version."""
    typer.echo(__version__)


def main() -> None:
    """Entry point exposed by the `saas-scraper` console script."""
    app()


if __name__ == "__main__":
    main()
