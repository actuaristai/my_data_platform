# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This project uses [`just`](https://github.com/casey/just) as a task runner (install: `winget install --id Casey.Just --exact`). All commands use `uv` for Python environment management.

```sh
just lint       # ruff check + fix (src/ and tests/)
just test       # pytest with coverage and doctest-modules
just docs       # build quartodoc API docs + quarto preview
just clean      # remove cache directories
```

**Initial setup** (after cloning):
```sh
just init-project   # uv sync + pre-commit install
```

**Run a single test file:**
```sh
uv run --only-group test pytest tests/path/to/test_file.py -v
```

**Release** (maintainers only):
```sh
just cd-release 'yyyy.mm.dd'   # bumps version, changelog, merges develop→main, tags
```

## Architecture

### Data Layer (Medallion)

Data flows through four layers in `data/`:
- `01_raw/` — original source data
- `02_bronze/` — landed/ingested, minimal transformation
- `03_silver/` — cleaned, validated
- `04_gold/` — aggregated, business-ready

### Source Code

- `src/my_data_platform/` — the installable Python package
- `conf/config.py` — dynaconf singleton (`conf`); import with `from conf.config import conf`
- `conf/parameters.toml` — non-secret parameters (committed to git)
- `.secrets.toml` — secrets (gitignored; never commit)

### Configuration Pattern

```python
from conf.config import conf
value = conf['MY_PARAM']   # reads from parameters.toml, .secrets.toml, or env vars
```

### Documentation

`docs/data_product.qmd` is a Quarto notebook executed with the `my_data_platform` Jupyter kernel. It renders to GitHub Pages via CI.

## Code Conventions

- **Line length:** 120 characters
- **Docstrings:** Google style (enforced by ruff `pydocstyle`)
- **Quotes:** single quotes for inline strings (ruff `flake8-quotes`)
- **Logging:** use `loguru` (`from loguru import logger`)
- **Doctests:** functions should include `Examples:` sections — pytest runs them via `--doctest-modules`

## Git & CI

- Default development branch: `develop`; production branch: `main`
- Commit messages **must** pass commitizen (enforced by pre-commit `commit-msg` hook). Use conventional commits (`feat:`, `fix:`, `chore:`, etc.)
- Pre-commit runs autopep8 and ruff on every commit
- CI runs: lint → test (with codecov) → docs build → package build on all branches; publishes to gh-pages when the `gh-pages` branch exists

## Template

This project was generated from `git@github.com:actuaristai/template.git` (v1.4.1). Update with:
```sh
just update-template
```
