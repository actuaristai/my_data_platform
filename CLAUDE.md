# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This project uses [`just`](https://github.com/casey/just) as a task runner (install: `winget install --id Casey.Just --exact`). All commands use `uv` for Python environment management.

```sh
just ingest     # download raw ATP/WTA CSVs into data/01_raw/
just run        # local SQLmesh plan --auto-apply (incremental, DuckLake on disk)
just stage      # SQLmesh plan dev on MotherDuck (isolated *__dev schemas)
just deploy     # SQLmesh plan prod on MotherDuck (promote to production)
just validate   # pointblank checks across all layers
just publish    # push gold tables to pins board
just lint       # ruff check + sqlmesh lint
just test       # pytest + sqlmesh test
just docs       # build quartodoc API docs + quarto preview
just clean      # remove cache directories
```

**Initial setup** (after cloning):
```sh
just init-project   # uv sync + pre-commit install + create data dirs
just ingest         # download raw data
just run            # build the full pipeline locally
```

**Run a single pytest test:**
```sh
uv run --only-group test pytest tests/path/to/test_file.py -v
```

**Run a single SQLmesh model test:**
```sh
uv run sqlmesh test tests/test_bronze_matches.yaml
```

**Release** (maintainers only):
```sh
just cd-release 'yyyy.mm.dd'   # bumps version, changelog, merges develop→main, tags
```

## Architecture

### Data Layer (Medallion)

Data flows through four layers managed by SQLmesh + DuckLake:
- `data/01_raw/` — CSV files downloaded by `ingest.py`; referenced by SQLmesh SEED models in `models/raw/`
- `data/catalog.ducklake` — DuckLake catalog file; `data/storage/` holds Parquet data files
- `models/bronze/` — union ATP + WTA tables, add `tour` column
- `models/silver/` — cast dates, normalise surfaces, derive full names
- `models/gold/` — aggregations: surface stats, H2H records, rankings history, tournament stats

### Source Code

- `src/my_data_platform/` — installable Python package: `ingest.py`, `validate.py`, `publish.py`
- `models/` — SQLmesh Python models (bronze/silver/gold) + `_util.py` shared utilities
- `validations/` — pointblank check functions; `connection.py` creates ibis DuckDB connection
- `config.yaml` — SQLmesh gateway config: `local_gateway` (DuckLake on disk) + `motherduck`
- `conf/config.py` — dynaconf singleton (`conf`); import with `from conf.config import conf`
- `conf/parameters.toml` — non-secret parameters (committed to git)
- `.secrets.toml` — secrets (gitignored; never commit)

### SQLmesh + Ibis Pattern

Python models use ibis to build queries and return SQL via `.to_sql(dialect='duckdb')`:

```python
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName
from models._util import GATEWAY_CATALOG, SOME_SCHEMA, _build_table

@model('layer.name', is_sql=True, kind={'name': ModelKindName.FULL}, ...)
def entrypoint(evaluator: MacroEvaluator) -> str:
    catalog = GATEWAY_CATALOG.get(evaluator.gateway or 'local_gateway', 'my_lakehouse')
    t = _build_table(evaluator, catalog, 'table_name', 'schema', SOME_SCHEMA)
    return t.mutate(...).to_sql(dialect='duckdb')
```

Schemas in `models/_util.py` are plain `dict[str, str]` (not `ibis.Schema`) so SQLmesh can serialize them. Use `table['col']` for column references — `from ibis import _` conflicts with SQLmesh's macro namespace.

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
