"""Guards the import-side-effect contract.

`import saas_scraper` alone must populate the registry. Without this, a
fresh interpreter doing `from saas_scraper import registry; registry.names()`
would silently report `[]`, which is a worse failure mode than KeyError.

Driven through a fresh subprocess so per-test state from earlier imports
in this session can't mask the regression.
"""

from __future__ import annotations

import subprocess
import sys


def test_top_level_import_populates_registry() -> None:
    code = (
        "import saas_scraper as s; "
        "names = s.registry.names(); "
        "print(','.join(names))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )
    names = result.stdout.strip().split(",")
    for expected in ("slack", "notion", "jira", "confluence", "github", "gitlab", "bitbucket"):
        assert expected in names, f"{expected!r} missing from {names!r}"
