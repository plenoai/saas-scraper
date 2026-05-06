"""Importing this package triggers registration of every built-in connector.

Adding a connector means:

1. Drop a `<name>.py` next to this file with a class implementing
   `Connector` and a module-level call to `registry.register`.
2. Add a `from . import <name>  # noqa: F401` line below.

The CLI imports this package once at startup; programmatic users can do
the same to opt in to the full registry, or import individual modules to
opt in selectively.
"""

from saas_scraper.connectors import (  # noqa: F401
    bitbucket,
    confluence,
    github,
    gitlab,
    jira,
    notion,
    slack,
)
