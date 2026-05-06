"""Registry and connector-package wiring."""

from __future__ import annotations

import pytest

# Importing the connectors package triggers all built-in registrations.
import saas_scraper.connectors  # noqa: F401
from saas_scraper.registry import registry


def test_builtin_connectors_registered() -> None:
    expected = {"slack", "notion", "jira", "confluence", "github", "gitlab", "bitbucket"}
    assert expected.issubset(set(registry.names()))


def test_unknown_connector_raises() -> None:
    with pytest.raises(KeyError):
        registry.create("does-not-exist", session=None)  # type: ignore[arg-type]


def test_register_overrides_last_write_wins() -> None:
    sentinel = object()

    def factory(**kwargs: object) -> object:
        return sentinel

    registry.register("__test_override__", factory)  # type: ignore[arg-type]
    assert registry.create("__test_override__", session=None) is sentinel  # type: ignore[arg-type]
