# energydb Documentation

This directory contains the Sphinx documentation for energydb.

## Building the Documentation Locally

### Prerequisites

Install the documentation dependencies:

```bash
pip install -r requirements.txt
```

Or install energydb with documentation extras:

```bash
pip install energydb[docs]
```

### Building

From the `docs` directory:

```bash
# Using make (Linux/macOS)
make html

# Or using sphinx-build directly
sphinx-build -b html . _build/html
```

The built documentation will be in `_build/html/`. Open `_build/html/index.html` in your browser.

### Cleaning

To clean the build directory:

```bash
make clean
```

## Documentation Structure

- `index.rst` — Main documentation index
- `installation.rst` — Installation instructions
- `sdk.rst` — SDK usage documentation
- `reference.rst` — Auto-generated API reference
- `examples.rst` — Notebook example index
- `conf.py` — Sphinx configuration

## Notebooks

The example notebooks live in the project root under `examples/*.ipynb` and
are auto-copied into `docs/notebooks/` at build time by the snippet at the top
of `conf.py`. Edit the notebooks at the source — the in-docs copy is
regenerated.

## Read the Docs

This documentation is configured for Read the Docs. The configuration is in
`.readthedocs.yaml` at the project root.

## Contributing

When adding or updating documentation:

1. Edit the relevant `.rst` files
2. Build locally to verify the changes
3. Commit and push to trigger the Read the Docs build
