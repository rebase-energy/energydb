"""Sphinx configuration for EnergyDB."""

from __future__ import annotations

import importlib.metadata

project = "EnergyDB"
author = "Rebase Energy"
copyright = "Rebase Energy"

try:
    release = importlib.metadata.version("energydb")
except importlib.metadata.PackageNotFoundError:
    release = "0.0.0"
version = release

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "myst_parser",
]

autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}
autodoc_typehints = "description"
napoleon_google_docstring = True
napoleon_numpy_docstring = False

myst_enable_extensions = ["colon_fence", "deflist"]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_title = f"EnergyDB {version}"
html_static_path: list[str] = []

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "polars": ("https://docs.pola.rs/api/python/stable/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
}

# Imports that may fail in the RTD build environment — mocked so autodoc can
# still introspect the package without a live Postgres / ClickHouse.
autodoc_mock_imports = [
    "psycopg",
    "sqlalchemy",
    "timedb",
    "timedatamodel",
    "energydatamodel",
    "polars",
]
