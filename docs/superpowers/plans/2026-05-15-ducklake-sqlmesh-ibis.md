# Data Lake (DuckLake + SQLmesh + Ibis + Tennis) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working open-source data lake in `my_data_platform` using DuckLake + SQLmesh + Ibis, with ATP/WTA tennis data flowing through raw → bronze → silver → gold layers, pins at the edges for ingest and publishing, and pointblank for validation.

**Architecture:** SQLmesh manages incremental execution and column-level lineage via a local DuckLake catalog (`data/catalog.ducklake`), with a MotherDuck gateway for staging (`dev` environment) and production. All Ibis models use `is_sql=True` returning `.to_sql(dialect='duckdb')` to preserve column-level lineage. pins handles raw data download from GitHub and gold layer publishing for consumers.

**Tech Stack:** Python 3.13, uv, SQLmesh, DuckLake, ibis-framework[duckdb], pins, pointblank, dynaconf, pandas, requests, just, ruff, pytest

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `pyproject.toml` | Modify | Add sqlmesh, ibis, pins, pointblank, requests, pandas deps; remove dvc |
| `config.yaml` | Create | SQLmesh local + MotherDuck gateway config |
| `conf/parameters.toml` | Modify | Add ducklake, pins, tennis config sections |
| `justfile` | Modify | Add run/stage/deploy/ingest/validate/publish commands |
| `models/_util.py` | Create | Shared `_build_table()`, `GATEWAY_CATALOG`, all table schemas |
| `models/raw/atp_matches.sql` | Create | SEED model pointing at data/01_raw/atp_matches.csv |
| `models/raw/wta_matches.sql` | Create | SEED model pointing at data/01_raw/wta_matches.csv |
| `models/raw/atp_players.sql` | Create | SEED model |
| `models/raw/wta_players.sql` | Create | SEED model |
| `models/raw/atp_rankings.sql` | Create | SEED model |
| `models/raw/wta_rankings.sql` | Create | SEED model |
| `models/bronze/matches.py` | Create | Ibis: union ATP+WTA, add tour column |
| `models/bronze/players.py` | Create | Ibis: union ATP+WTA players |
| `models/bronze/rankings.py` | Create | Ibis: union ATP+WTA rankings |
| `models/silver/matches.py` | Create | Ibis: cast date, normalise surface, filter null scores |
| `models/silver/players.py` | Create | Ibis: full_name, filter null player_id |
| `models/silver/rankings.py` | Create | Ibis: cast ranking_date to DATE |
| `models/gold/player_surface_stats.py` | Create | Ibis: win rate by player + surface |
| `models/gold/head_to_head.py` | Create | Ibis: H2H records between player pairs |
| `models/gold/rankings_history.py` | Create | Ibis: career ranking trajectory |
| `models/gold/tournament_stats.py` | Create | Ibis: tournament-level aggregates |
| `validations/connection.py` | Create | `get_connection()` reads catalog_path from conf |
| `validations/checks.py` | Create | pointblank validate chains per layer |
| `src/my_data_platform/ingest.py` | Create | Download ATP/WTA CSVs from GitHub → data/01_raw/ |
| `src/my_data_platform/publish.py` | Create | Push gold tables → pins board |
| `src/my_data_platform/validate.py` | Create | CLI entry point: runs all pointblank checks |
| `tests/conftest.py` | Create | `con` fixture: in-memory DuckDB for pytest |
| `tests/test_ingest.py` | Create | Unit tests for ingest functions |
| `tests/test_validations.py` | Create | Tests for connection.py |
| `tests/test_bronze_matches.yaml` | Create | SQLmesh unit test: bronze.matches |
| `tests/test_silver_matches.yaml` | Create | SQLmesh unit test: silver.matches |
| `tests/test_gold_surface_stats.yaml` | Create | SQLmesh unit test: gold.player_surface_stats |

---

### Task 1: Foundation — dependencies, SQLmesh config, justfile

**Files:**
- Modify: `pyproject.toml`
- Create: `config.yaml`
- Modify: `conf/parameters.toml`
- Modify: `justfile`

- [ ] **Step 1: Update pyproject.toml**

Replace the `[project]` block and `[dependency-groups]`:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "my_data_platform"
version = "0.1.0"
description = "Opinionated guide to best practices set up of open source data platform with best data engineering practices"
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "duckdb>=1.2.0",
    "dynaconf>=3.2.10",
    "griffe<1.0.0",
    "ibis-framework[duckdb]>=11.0.0",
    "ipykernel>=6.29.5",
    "loguru>=0.7.3",
    "nbclient>=0.10.2",
    "nbformat>=5.10.4",
    "pandas>=2.2.0",
    "pins>=0.9.1",
    "pointblank>=0.17.0",
    "pytest>=8.3.5",
    "pytest-cov>=6.0.0",
    "quartodoc>=0.9.1",
    "requests>=2.32.0",
    "ruff>=0.11.2",
    "sqlmesh[duckdb]>=0.228.2",
    "toml>=0.10.2",
    "typer>=0.15.2",
    "autopep8>=2.3.2",
    "commitizen>=4.4.1",
]

[[project.authors]]
name = "Actuarist AI"
email = "human@actuarist.ai"

[dependency-groups]
lint = ["ruff"]
test = [
    "pytest", "pytest-cov", "dynaconf", "typer", "requests", "loguru",
    "toml", "ibis-framework[duckdb]", "pointblank", "pandas",
]
dev = [
    "ipykernel", "nbclient", "nbformat", "ruff", "autopep8", "commitizen",
    "pytest", "pytest-cov", "quartodoc", "toml", "typer",
    "sqlmesh[duckdb]", "ibis-framework[duckdb]", "pins", "pointblank",
    "requests", "pandas",
]

[tool.uv]
default-groups = "all"

[tool.ruff]
src = ["src/my_data_platform", "tests", "conf", "models", "validations"]
line-length = 120
exclude = [".git", ".ruff_cache", ".venv", "__pypackages__", "__init__.py"]

[tool.autopep8]
max_line_length = 120

[tool.ruff.lint]
select = ["ALL"]
ignore = []

[tool.ruff.lint.pydocstyle]
convention = "google"

[tool.ruff.lint.flake8-quotes]
inline-quotes = "single"

[tool.pytest.ini_options]
pythonpath = ["."]

[tool.hatch.build.targets.wheel]
packages = ["src/my_data_platform", "conf"]
```

- [ ] **Step 2: Run uv sync**

```bash
uv sync
```

Expected: resolves and installs sqlmesh, ibis-framework[duckdb], pins, pointblank, pandas, requests.

- [ ] **Step 3: Create config.yaml**

```yaml
gateways:
  local_gateway:
    connection:
      type: duckdb
      catalogs:
        my_lakehouse:
          type: ducklake
          path: data/catalog.ducklake
          data_path: data/storage
      extensions:
        - ducklake
    state_connection:
      type: duckdb
      database: data/sqlmesh_state.db

  motherduck:
    connection:
      type: motherduck
      catalogs:
        my_lakehouse: "md:my_lakehouse"
      token: "{{ env_var('MOTHERDUCK_TOKEN') }}"
    state_connection:
      type: motherduck
      database: "md:sqlmesh_state"

default_gateway: local_gateway

model_defaults:
  dialect: duckdb
  start: 2015-01-01
  cron: '@daily'

linter:
  enabled: true
  rules:
    - ambiguousorinvalidcolumn
    - invalidselectstarexpansion
    - noambiguousprojections
```

- [ ] **Step 4: Update conf/parameters.toml**

```toml
LOGURU_FORMAT = '<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>'

[ducklake]
catalog_path = "data/catalog.ducklake"
data_path = "data/storage"

[sqlmesh]
state_db = "data/sqlmesh_state.db"

[pins]
board_type = "local"
board_path = "data/pins_board"

[tennis]
year_start = 2015
year_end = 2025
tours = ["atp", "wta"]
atp_url = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
wta_url = "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master"
```

- [ ] **Step 5: Update justfile**

```
set shell := ["pwsh", "-c"]

PROJECT_NAME := "my_data_platform"
REMOTE_REPO := "git@github.com:actuaristai/my_data_platform.git"
DESCRIPTION := "Opinionated guide to best practices set up of open source data platform with best data engineering practices"

POWERSHELL_SHEBANG := if os() == 'windows' {
  'pwsh.exe'
} else {
  '/usr/bin/env pwsh'
}

# List available commands
help:
    just --list --unsorted

# --- Pipeline ---

# Run pipeline locally (DuckLake on disk, incremental)
run:
    mkdir -p data/storage data/01_raw data/pins_board
    uv run sqlmesh --gateway local_gateway plan --auto-apply

# Run pipeline on MotherDuck dev environment (isolated *__dev schemas)
stage:
    uv run sqlmesh --gateway motherduck plan dev --auto-apply

# Promote dev to production on MotherDuck
deploy:
    uv run sqlmesh --gateway motherduck plan --auto-apply

# Download raw ATP/WTA tennis CSVs to data/01_raw/
ingest:
    uv run python -m my_data_platform.ingest

# Run pointblank validation checks across all layers
validate:
    uv run python -m my_data_platform.validate

# Publish gold tables to pins board
publish:
    uv run python -m my_data_platform.publish

# --- Quality ---

# Lint using ruff + sqlmesh
lint:
    uv run --only-group lint ruff check src/{{PROJECT_NAME}} --fix
    uv run --only-group lint ruff check tests --fix
    uv run sqlmesh lint

# Run pytest + sqlmesh tests
test:
    uv run --only-group test pytest --cov-report term-missing --cov={{PROJECT_NAME}} -v -p no:faulthandler -W ignore::DeprecationWarning --verbose --doctest-modules
    uv run sqlmesh test

# --- Docs ---

_docs-build:
    uv run quartodoc build
    uv run quarto render

docs: _docs-build
    uv run quarto preview

# --- Init (run once after cloning) ---

init-project: init-env init-pre-commit
    mkdir -p data/01_raw data/02_bronze data/03_silver data/04_gold data/storage data/pins_board

init-env:
    uv sync
    uv run python -m ipykernel install --user --name {{PROJECT_NAME}}

init-pre-commit:
    uvx pre-commit install --hook-type pre-commit --hook-type commit-msg
    uvx pre-commit autoupdate
    uvx pre-commit run --all-files

# --- CD ---

cd-publish:
    $env:PRE_COMMIT_ALLOW_NO_CONFIG = "1"; uv run quarto publish gh-pages

cd-release VERSION:
    git checkout -b release-{{VERSION}} develop
    uv run python bump_version.py {{VERSION}}
    uv sync
    uv run cz changelog --incremental
    git commit -a -m "chore: Bumped version number to {{VERSION}}"
    git checkout main
    git merge --no-ff release-{{VERSION}}
    git push
    git tag -a {{VERSION}} -m "add version tag"
    git push origin {{VERSION}}
    git checkout develop
    git merge --no-ff main
    git branch -d release-{{VERSION}}
    git push

update-template *COPIER_OPTIONS:
    uvx copier update --trust --skip-tasks --skip-answered

clean:
    #!{{POWERSHELL_SHEBANG}}
    Remove-Item -Path "_freeze" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Remove-Item -Path ".pytest_cache" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Remove-Item -Path ".ruff_cache" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Remove-Item -Path "__pycache__" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Remove-Item -Path ".quarto" -Recurse -Confirm -Erroraction 'silentlycontinue'
    Get-ChildItem -Path . -Filter "__pycache__" -Recurse -Directory | Remove-Item -Recurse -Force
```

- [ ] **Step 6: Create directory skeleton**

```bash
mkdir -p models/raw models/bronze models/silver models/gold
mkdir -p data/01_raw data/02_bronze data/03_silver data/04_gold data/storage data/pins_board
mkdir -p validations tests
touch audits/.gitkeep macros/.gitkeep
```

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml config.yaml conf/parameters.toml justfile models/ audits/ macros/ data/
git commit -m "feat: add sqlmesh + ducklake + ibis foundation"
```

---

### Task 2: Shared model utilities

**Files:**
- Create: `models/_util.py`
- Create: `tests/conftest.py`
- Create: `tests/test_util.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_util.py
from unittest.mock import MagicMock
import ibis
from models._util import GATEWAY_CATALOG, MATCHES_SCHEMA, PLAYERS_SCHEMA, RANKINGS_SCHEMA, _build_table


def test_gateway_catalog_contains_expected_keys():
    assert 'local_gateway' in GATEWAY_CATALOG
    assert 'motherduck' in GATEWAY_CATALOG
    assert GATEWAY_CATALOG['local_gateway'] == 'my_lakehouse'
    assert GATEWAY_CATALOG['motherduck'] == 'my_lakehouse'


def test_build_table_loading_stage_uses_fallback_schema():
    evaluator = MagicMock()
    evaluator.runtime_stage = 'loading'
    evaluator.gateway = 'local_gateway'

    table = _build_table(evaluator, 'my_lakehouse', 'atp_matches', 'raw', MATCHES_SCHEMA)

    assert table.get_name() == 'atp_matches'
    assert 'tourney_id' in table.schema()
    assert 'winner_id' in table.schema()


def test_build_table_exception_during_eval_uses_fallback():
    evaluator = MagicMock()
    evaluator.runtime_stage = 'evaluating'
    evaluator.engine_adapter.connection = None  # forces exception in from_connection

    table = _build_table(evaluator, 'my_lakehouse', 'atp_players', 'raw', PLAYERS_SCHEMA)

    assert 'player_id' in table.schema()
    assert 'first_name' in table.schema()


def test_schemas_have_expected_columns():
    assert 'tourney_date' in MATCHES_SCHEMA
    assert 'player_id' in PLAYERS_SCHEMA
    assert 'ranking_date' in RANKINGS_SCHEMA
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_util.py -v
```

Expected: `ModuleNotFoundError: No module named 'models._util'`

- [ ] **Step 3: Create tests/conftest.py**

```python
"""Shared pytest fixtures."""
import ibis
import pytest


@pytest.fixture
def con():
    """In-memory DuckDB connection — no DuckLake required."""
    conn = ibis.duckdb.connect()
    yield conn
    conn.disconnect()
```

- [ ] **Step 4: Create models/_util.py**

```python
"""Shared utilities for all SQLmesh + Ibis models."""
import ibis
from sqlmesh.core.macros import MacroEvaluator

GATEWAY_CATALOG: dict[str, str] = {
    'local_gateway': 'my_lakehouse',
    'motherduck': 'my_lakehouse',
}

# Schema for raw.atp_matches / raw.wta_matches (tourney_date is stored as YYYYMMDD int)
MATCHES_SCHEMA = ibis.Schema({
    'tourney_id': 'string',
    'tourney_name': 'string',
    'surface': 'string',
    'draw_size': 'int32',
    'tourney_level': 'string',
    'tourney_date': 'int32',
    'match_num': 'int32',
    'winner_id': 'int32',
    'winner_seed': 'float64',
    'winner_entry': 'string',
    'winner_name': 'string',
    'winner_hand': 'string',
    'winner_ht': 'float64',
    'winner_ioc': 'string',
    'winner_age': 'float64',
    'loser_id': 'int32',
    'loser_seed': 'float64',
    'loser_entry': 'string',
    'loser_name': 'string',
    'loser_hand': 'string',
    'loser_ht': 'float64',
    'loser_ioc': 'string',
    'loser_age': 'float64',
    'score': 'string',
    'best_of': 'int32',
    'round': 'string',
    'minutes': 'float64',
    'w_ace': 'float64',
    'w_df': 'float64',
    'w_svpt': 'float64',
    'w_1stIn': 'float64',
    'w_1stWon': 'float64',
    'w_2ndWon': 'float64',
    'w_SvGm': 'float64',
    'w_bpSaved': 'float64',
    'w_bpFaced': 'float64',
    'l_ace': 'float64',
    'l_df': 'float64',
    'l_svpt': 'float64',
    'l_1stIn': 'float64',
    'l_1stWon': 'float64',
    'l_2ndWon': 'float64',
    'l_SvGm': 'float64',
    'l_bpSaved': 'float64',
    'l_bpFaced': 'float64',
    'winner_rank': 'float64',
    'winner_rank_points': 'float64',
    'loser_rank': 'float64',
    'loser_rank_points': 'float64',
})

# bronze.matches adds 'tour' to MATCHES_SCHEMA
BRONZE_MATCHES_SCHEMA = ibis.Schema({**MATCHES_SCHEMA, 'tour': 'string'})

# silver.matches: tourney_date promoted to date, tour added
SILVER_MATCHES_SCHEMA = ibis.Schema({
    **{k: v for k, v in MATCHES_SCHEMA.items() if k != 'tourney_date'},
    'tourney_date': 'date',
    'tour': 'string',
})

PLAYERS_SCHEMA = ibis.Schema({
    'player_id': 'int32',
    'first_name': 'string',
    'last_name': 'string',
    'hand': 'string',
    'dob': 'float64',
    'ioc': 'string',
    'height': 'float64',
    'wikidata_id': 'string',
})

# silver.players adds 'full_name' and 'tour'
SILVER_PLAYERS_SCHEMA = ibis.Schema({
    **PLAYERS_SCHEMA,
    'full_name': 'string',
    'tour': 'string',
})

RANKINGS_SCHEMA = ibis.Schema({
    'ranking_date': 'int32',
    'ranking': 'int32',
    'player_id': 'int32',
    'points': 'float64',
})

# silver.rankings: ranking_date promoted to date, tour added
SILVER_RANKINGS_SCHEMA = ibis.Schema({
    'ranking_date': 'date',
    'ranking': 'int32',
    'player_id': 'int32',
    'points': 'float64',
    'tour': 'string',
})


def _build_table(evaluator: MacroEvaluator,
                 catalog: str,
                 table: str,
                 database: str,
                 fallback_schema: ibis.Schema) -> ibis.Table:
    """Return an Ibis unbound table, reusing SQLMesh's connection at runtime.

    During the loading stage the engine adapter is unavailable, so fallback_schema
    is used. At runtime the live schema is read from the already-attached catalog
    via SQLMesh's own connection, avoiding a second DuckLake file lock.
    """
    if evaluator.runtime_stage == 'loading':
        schema = fallback_schema
    else:
        try:
            con = ibis.duckdb.from_connection(evaluator.engine_adapter.connection)
            schema = con.table(table, database=f'{catalog}.{database}').schema()
        except Exception:
            schema = fallback_schema
    return ibis.table(schema=schema, name=table, catalog=catalog, database=database)
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_util.py -v
```

Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add models/_util.py tests/conftest.py tests/test_util.py
git commit -m "feat: add shared model utilities and ibis schemas"
```

---

### Task 3: Data ingestion

**Files:**
- Create: `src/my_data_platform/ingest.py`
- Create: `tests/test_ingest.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_ingest.py
import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from my_data_platform.ingest import download_tour_matches, download_tour_players, download_tour_rankings


def _csv_bytes(rows: list[dict]) -> bytes:
    return pd.DataFrame(rows).to_csv(index=False).encode()


MATCH_ROW = {
    'tourney_id': '2024-540', 'tourney_name': 'Wimbledon', 'surface': 'Grass',
    'tourney_date': 20240701, 'match_num': 1, 'winner_id': 104925, 'loser_id': 105453,
    'score': '6-3 6-4',
}
PLAYER_ROW = {'player_id': 104925, 'first_name': 'Novak', 'last_name': 'Djokovic'}
RANKING_ROW = {'ranking_date': 20240701, 'ranking': 1, 'player_id': 104925, 'points': 9000}


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_matches_concatenates_years(mock_get, tmp_path):
    mock_get.return_value = MagicMock(status_code=200, content=_csv_bytes([MATCH_ROW]))
    out = tmp_path / 'atp_matches.csv'

    download_tour_matches('atp', 'https://example.com', 2024, 2024, out)

    df = pd.read_csv(out)
    assert len(df) == 1
    assert df.iloc[0]['tourney_id'] == '2024-540'
    mock_get.assert_called_once_with('https://example.com/atp_matches_2024.csv', timeout=30)


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_matches_skips_404(mock_get, tmp_path):
    mock_get.side_effect = [
        MagicMock(status_code=404),
        MagicMock(status_code=200, content=_csv_bytes([MATCH_ROW])),
    ]
    out = tmp_path / 'atp_matches.csv'

    download_tour_matches('atp', 'https://example.com', 2023, 2024, out)

    df = pd.read_csv(out)
    assert len(df) == 1


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_players(mock_get, tmp_path):
    mock_get.return_value = MagicMock(status_code=200, content=_csv_bytes([PLAYER_ROW]))
    out = tmp_path / 'atp_players.csv'

    download_tour_players('atp', 'https://example.com', out)

    df = pd.read_csv(out)
    assert df.iloc[0]['first_name'] == 'Novak'


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_rankings(mock_get, tmp_path):
    mock_get.return_value = MagicMock(status_code=200, content=_csv_bytes([RANKING_ROW]))
    out = tmp_path / 'atp_rankings.csv'

    download_tour_rankings('atp', 'https://example.com', 2024, 2024, out)

    df = pd.read_csv(out)
    assert df.iloc[0]['ranking'] == 1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_ingest.py -v
```

Expected: `ImportError: cannot import name 'download_tour_matches'`

- [ ] **Step 3: Create src/my_data_platform/ingest.py**

```python
"""Download ATP/WTA tennis CSVs from GitHub into data/01_raw/."""
from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

from conf.config import conf


def download_tour_matches(tour: str, base_url: str, year_start: int, year_end: int, output_path: Path) -> None:
    """Download and concatenate per-year match CSVs into a single file."""
    frames = []
    for year in range(year_start, year_end + 1):
        url = f'{base_url}/{tour}_matches_{year}.csv'
        logger.info(f'Downloading {url}')
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            logger.warning(f'Not found (skipping): {url}')
            continue
        resp.raise_for_status()
        frames.append(pd.read_csv(io.BytesIO(resp.content), low_memory=False))
    if not frames:
        raise RuntimeError(f'No match data found for {tour} {year_start}–{year_end}')
    pd.concat(frames, ignore_index=True).to_csv(output_path, index=False)
    logger.info(f'Saved {output_path} ({sum(len(f) for f in frames):,} rows)')


def download_tour_players(tour: str, base_url: str, output_path: Path) -> None:
    """Download the players master CSV for a tour."""
    url = f'{base_url}/{tour}_players.csv'
    logger.info(f'Downloading {url}')
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    pd.read_csv(io.BytesIO(resp.content), low_memory=False).to_csv(output_path, index=False)
    logger.info(f'Saved {output_path}')


def download_tour_rankings(tour: str, base_url: str, year_start: int, year_end: int, output_path: Path) -> None:
    """Download and concatenate per-year rankings CSVs into a single file."""
    frames = []
    for year in range(year_start, year_end + 1):
        url = f'{base_url}/{tour}_rankings_{year}s.csv'
        logger.info(f'Downloading {url}')
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            logger.warning(f'Not found (skipping): {url}')
            continue
        resp.raise_for_status()
        frames.append(pd.read_csv(io.BytesIO(resp.content), low_memory=False))
    if not frames:
        raise RuntimeError(f'No rankings data found for {tour} {year_start}–{year_end}')
    pd.concat(frames, ignore_index=True).to_csv(output_path, index=False)
    logger.info(f'Saved {output_path} ({sum(len(f) for f in frames):,} rows)')


def main() -> None:
    """Entry point: download all raw tennis data to data/01_raw/."""
    raw_dir = Path('data/01_raw')
    raw_dir.mkdir(parents=True, exist_ok=True)

    year_start: int = conf['tennis.year_start']
    year_end: int = conf['tennis.year_end']
    tours: list[str] = conf['tennis.tours']
    urls = {'atp': conf['tennis.atp_url'], 'wta': conf['tennis.wta_url']}

    for tour in tours:
        base = urls[tour]
        download_tour_matches(tour, base, year_start, year_end, raw_dir / f'{tour}_matches.csv')
        download_tour_players(tour, base, raw_dir / f'{tour}_players.csv')
        download_tour_rankings(tour, base, year_start, year_end, raw_dir / f'{tour}_rankings.csv')

    logger.info('Ingest complete.')


if __name__ == '__main__':
    main()
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_ingest.py -v
```

Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/my_data_platform/ingest.py tests/test_ingest.py
git commit -m "feat: add ATP/WTA data ingest from GitHub"
```

---

### Task 4: Raw SEED models

**Files:**
- Create: `models/raw/atp_matches.sql`, `models/raw/wta_matches.sql`
- Create: `models/raw/atp_players.sql`, `models/raw/wta_players.sql`
- Create: `models/raw/atp_rankings.sql`, `models/raw/wta_rankings.sql`

Note: Run `just ingest` before `just run` to populate `data/01_raw/`.

- [ ] **Step 1: Create models/raw/atp_matches.sql**

```sql
MODEL (
    name raw.atp_matches,
    kind SEED (
        path 'data/01_raw/atp_matches.csv'
    ),
    columns (
        tourney_id TEXT,
        tourney_name TEXT,
        surface TEXT,
        draw_size INT,
        tourney_level TEXT,
        tourney_date INT,
        match_num INT,
        winner_id INT,
        winner_seed DOUBLE,
        winner_entry TEXT,
        winner_name TEXT,
        winner_hand TEXT,
        winner_ht DOUBLE,
        winner_ioc TEXT,
        winner_age DOUBLE,
        loser_id INT,
        loser_seed DOUBLE,
        loser_entry TEXT,
        loser_name TEXT,
        loser_hand TEXT,
        loser_ht DOUBLE,
        loser_ioc TEXT,
        loser_age DOUBLE,
        score TEXT,
        best_of INT,
        round TEXT,
        minutes DOUBLE,
        w_ace DOUBLE,
        w_df DOUBLE,
        w_svpt DOUBLE,
        w_1stIn DOUBLE,
        w_1stWon DOUBLE,
        w_2ndWon DOUBLE,
        w_SvGm DOUBLE,
        w_bpSaved DOUBLE,
        w_bpFaced DOUBLE,
        l_ace DOUBLE,
        l_df DOUBLE,
        l_svpt DOUBLE,
        l_1stIn DOUBLE,
        l_1stWon DOUBLE,
        l_2ndWon DOUBLE,
        l_SvGm DOUBLE,
        l_bpSaved DOUBLE,
        l_bpFaced DOUBLE,
        winner_rank DOUBLE,
        winner_rank_points DOUBLE,
        loser_rank DOUBLE,
        loser_rank_points DOUBLE
    )
);
```

- [ ] **Step 2: Create models/raw/wta_matches.sql**

```sql
MODEL (
    name raw.wta_matches,
    kind SEED (
        path 'data/01_raw/wta_matches.csv'
    ),
    columns (
        tourney_id TEXT,
        tourney_name TEXT,
        surface TEXT,
        draw_size INT,
        tourney_level TEXT,
        tourney_date INT,
        match_num INT,
        winner_id INT,
        winner_seed DOUBLE,
        winner_entry TEXT,
        winner_name TEXT,
        winner_hand TEXT,
        winner_ht DOUBLE,
        winner_ioc TEXT,
        winner_age DOUBLE,
        loser_id INT,
        loser_seed DOUBLE,
        loser_entry TEXT,
        loser_name TEXT,
        loser_hand TEXT,
        loser_ht DOUBLE,
        loser_ioc TEXT,
        loser_age DOUBLE,
        score TEXT,
        best_of INT,
        round TEXT,
        minutes DOUBLE,
        w_ace DOUBLE,
        w_df DOUBLE,
        w_svpt DOUBLE,
        w_1stIn DOUBLE,
        w_1stWon DOUBLE,
        w_2ndWon DOUBLE,
        w_SvGm DOUBLE,
        w_bpSaved DOUBLE,
        w_bpFaced DOUBLE,
        l_ace DOUBLE,
        l_df DOUBLE,
        l_svpt DOUBLE,
        l_1stIn DOUBLE,
        l_1stWon DOUBLE,
        l_2ndWon DOUBLE,
        l_SvGm DOUBLE,
        l_bpSaved DOUBLE,
        l_bpFaced DOUBLE,
        winner_rank DOUBLE,
        winner_rank_points DOUBLE,
        loser_rank DOUBLE,
        loser_rank_points DOUBLE
    )
);
```

- [ ] **Step 3: Create models/raw/atp_players.sql**

```sql
MODEL (
    name raw.atp_players,
    kind SEED (
        path 'data/01_raw/atp_players.csv'
    ),
    columns (
        player_id INT,
        first_name TEXT,
        last_name TEXT,
        hand TEXT,
        dob DOUBLE,
        ioc TEXT,
        height DOUBLE,
        wikidata_id TEXT
    )
);
```

- [ ] **Step 4: Create models/raw/wta_players.sql**

```sql
MODEL (
    name raw.wta_players,
    kind SEED (
        path 'data/01_raw/wta_players.csv'
    ),
    columns (
        player_id INT,
        first_name TEXT,
        last_name TEXT,
        hand TEXT,
        dob DOUBLE,
        ioc TEXT,
        height DOUBLE,
        wikidata_id TEXT
    )
);
```

- [ ] **Step 5: Create models/raw/atp_rankings.sql**

```sql
MODEL (
    name raw.atp_rankings,
    kind SEED (
        path 'data/01_raw/atp_rankings.csv'
    ),
    columns (
        ranking_date INT,
        ranking INT,
        player_id INT,
        points DOUBLE
    )
);
```

- [ ] **Step 6: Create models/raw/wta_rankings.sql**

```sql
MODEL (
    name raw.wta_rankings,
    kind SEED (
        path 'data/01_raw/wta_rankings.csv'
    ),
    columns (
        ranking_date INT,
        ranking INT,
        player_id INT,
        points DOUBLE
    )
);
```

- [ ] **Step 7: Verify SQLmesh parses models**

```bash
uv run sqlmesh --gateway local_gateway plan --select-model "raw.*"
```

Expected: SQLmesh lists the 6 raw SEED models to be created. Type `y` to apply, or `Ctrl+C` to cancel (apply with `just run` after ingest).

- [ ] **Step 8: Commit**

```bash
git add models/raw/
git commit -m "feat: add raw SEED models for ATP and WTA tennis data"
```

---

### Task 5: Bronze models

**Files:**
- Create: `models/bronze/matches.py`
- Create: `models/bronze/players.py`
- Create: `models/bronze/rankings.py`
- Create: `tests/test_bronze_matches.yaml`

- [ ] **Step 1: Write SQLmesh unit test**

```yaml
# tests/test_bronze_matches.yaml
test_bronze_matches_combines_tours_and_adds_tour_column:
  model: bronze.matches
  inputs:
    raw.atp_matches:
      columns:
        tourney_id: TEXT
        tourney_name: TEXT
        surface: TEXT
        draw_size: INT
        tourney_level: TEXT
        tourney_date: INT
        match_num: INT
        winner_id: INT
        winner_seed: DOUBLE
        winner_entry: TEXT
        winner_name: TEXT
        winner_hand: TEXT
        winner_ht: DOUBLE
        winner_ioc: TEXT
        winner_age: DOUBLE
        loser_id: INT
        loser_seed: DOUBLE
        loser_entry: TEXT
        loser_name: TEXT
        loser_hand: TEXT
        loser_ht: DOUBLE
        loser_ioc: TEXT
        loser_age: DOUBLE
        score: TEXT
        best_of: INT
        round: TEXT
        minutes: DOUBLE
        w_ace: DOUBLE
        w_df: DOUBLE
        w_svpt: DOUBLE
        w_1stIn: DOUBLE
        w_1stWon: DOUBLE
        w_2ndWon: DOUBLE
        w_SvGm: DOUBLE
        w_bpSaved: DOUBLE
        w_bpFaced: DOUBLE
        l_ace: DOUBLE
        l_df: DOUBLE
        l_svpt: DOUBLE
        l_1stIn: DOUBLE
        l_1stWon: DOUBLE
        l_2ndWon: DOUBLE
        l_SvGm: DOUBLE
        l_bpSaved: DOUBLE
        l_bpFaced: DOUBLE
        winner_rank: DOUBLE
        winner_rank_points: DOUBLE
        loser_rank: DOUBLE
        loser_rank_points: DOUBLE
      rows:
        - tourney_id: "2024-wimbledon"
          match_num: 1
          winner_id: 104925
          loser_id: 105453
          score: "6-3 6-4"
          surface: "Grass"
          tourney_date: 20240701
          tourney_name: "Wimbledon"
          tourney_level: "G"
          draw_size: 128
          best_of: 5
          round: "F"
          winner_seed: ~
          winner_entry: ~
          winner_name: "Novak Djokovic"
          winner_hand: "R"
          winner_ht: 188.0
          winner_ioc: "SRB"
          winner_age: 37.1
          loser_seed: ~
          loser_entry: ~
          loser_name: "Andy Murray"
          loser_hand: "R"
          loser_ht: 190.0
          loser_ioc: "GBR"
          loser_age: 37.3
          minutes: 135.0
          w_ace: 8.0
          w_df: 2.0
          w_svpt: 85.0
          w_1stIn: 55.0
          w_1stWon: 42.0
          w_2ndWon: 20.0
          w_SvGm: 12.0
          w_bpSaved: 3.0
          w_bpFaced: 4.0
          l_ace: 3.0
          l_df: 4.0
          l_svpt: 80.0
          l_1stIn: 50.0
          l_1stWon: 32.0
          l_2ndWon: 15.0
          l_SvGm: 11.0
          l_bpSaved: 1.0
          l_bpFaced: 6.0
          winner_rank: 2.0
          winner_rank_points: 7500.0
          loser_rank: 50.0
          loser_rank_points: 1000.0
    raw.wta_matches:
      columns:
        tourney_id: TEXT
        tourney_name: TEXT
        surface: TEXT
        draw_size: INT
        tourney_level: TEXT
        tourney_date: INT
        match_num: INT
        winner_id: INT
        winner_seed: DOUBLE
        winner_entry: TEXT
        winner_name: TEXT
        winner_hand: TEXT
        winner_ht: DOUBLE
        winner_ioc: TEXT
        winner_age: DOUBLE
        loser_id: INT
        loser_seed: DOUBLE
        loser_entry: TEXT
        loser_name: TEXT
        loser_hand: TEXT
        loser_ht: DOUBLE
        loser_ioc: TEXT
        loser_age: DOUBLE
        score: TEXT
        best_of: INT
        round: TEXT
        minutes: DOUBLE
        w_ace: DOUBLE
        w_df: DOUBLE
        w_svpt: DOUBLE
        w_1stIn: DOUBLE
        w_1stWon: DOUBLE
        w_2ndWon: DOUBLE
        w_SvGm: DOUBLE
        w_bpSaved: DOUBLE
        w_bpFaced: DOUBLE
        l_ace: DOUBLE
        l_df: DOUBLE
        l_svpt: DOUBLE
        l_1stIn: DOUBLE
        l_1stWon: DOUBLE
        l_2ndWon: DOUBLE
        l_SvGm: DOUBLE
        l_bpSaved: DOUBLE
        l_bpFaced: DOUBLE
        winner_rank: DOUBLE
        winner_rank_points: DOUBLE
        loser_rank: DOUBLE
        loser_rank_points: DOUBLE
      rows:
        - tourney_id: "2024-wta-wimbledon"
          match_num: 1
          winner_id: 200001
          loser_id: 200002
          score: "7-5 6-3"
          surface: "Grass"
          tourney_date: 20240701
          tourney_name: "Wimbledon"
          tourney_level: "G"
          draw_size: 128
          best_of: 3
          round: "F"
          winner_seed: 1.0
          winner_entry: ~
          winner_name: "Iga Swiatek"
          winner_hand: "R"
          winner_ht: 175.0
          winner_ioc: "POL"
          winner_age: 23.1
          loser_seed: 5.0
          loser_entry: ~
          loser_name: "Aryna Sabalenka"
          loser_hand: "R"
          loser_ht: 182.0
          loser_ioc: "BLR"
          loser_age: 26.2
          minutes: 90.0
          w_ace: 5.0
          w_df: 1.0
          w_svpt: 70.0
          w_1stIn: 45.0
          w_1stWon: 35.0
          w_2ndWon: 16.0
          w_SvGm: 10.0
          w_bpSaved: 2.0
          w_bpFaced: 3.0
          l_ace: 4.0
          l_df: 3.0
          l_svpt: 65.0
          l_1stIn: 40.0
          l_1stWon: 28.0
          l_2ndWon: 13.0
          l_SvGm: 9.0
          l_bpSaved: 0.0
          l_bpFaced: 5.0
          winner_rank: 1.0
          winner_rank_points: 9000.0
          loser_rank: 5.0
          loser_rank_points: 5000.0
  outputs:
    partial: true
    rows:
      - tourney_id: "2024-wimbledon"
        match_num: 1
        tour: "ATP"
      - tourney_id: "2024-wta-wimbledon"
        match_num: 1
        tour: "WTA"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run sqlmesh test tests/test_bronze_matches.yaml
```

Expected: Error — model `bronze.matches` not found.

- [ ] **Step 3: Create models/bronze/matches.py**

```python
"""Bronze matches: combine ATP + WTA into a single table with tour label."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, MATCHES_SCHEMA, _build_table


@model('bronze.matches',
       is_sql=True,
       kind='INCREMENTAL_BY_UNIQUE_KEY',
       unique_key=['tourney_id', 'match_num', 'tour'],
       description='Combined ATP and WTA matches with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union atp_matches and wta_matches, tagging each row with its tour."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    atp = _build_table(evaluator, catalog, 'atp_matches', 'raw', MATCHES_SCHEMA)
    wta = _build_table(evaluator, catalog, 'wta_matches', 'raw', MATCHES_SCHEMA)

    return ibis.union(atp.mutate(tour=ibis.literal('ATP')), wta.mutate(tour=ibis.literal('WTA')),
                      distinct=False).to_sql(dialect='duckdb')
```

- [ ] **Step 4: Create models/bronze/players.py**

```python
"""Bronze players: combine ATP + WTA player rosters with tour label."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, PLAYERS_SCHEMA, _build_table


@model('bronze.players',
       is_sql=True,
       kind='INCREMENTAL_BY_UNIQUE_KEY',
       unique_key=['player_id', 'tour'],
       description='Combined ATP and WTA player rosters with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union atp_players and wta_players, tagging each row with its tour."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    atp = _build_table(evaluator, catalog, 'atp_players', 'raw', PLAYERS_SCHEMA)
    wta = _build_table(evaluator, catalog, 'wta_players', 'raw', PLAYERS_SCHEMA)

    return ibis.union(atp.mutate(tour=ibis.literal('ATP')), wta.mutate(tour=ibis.literal('WTA')),
                      distinct=False).to_sql(dialect='duckdb')
```

- [ ] **Step 5: Create models/bronze/rankings.py**

```python
"""Bronze rankings: combine ATP + WTA weekly rankings with tour label."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, RANKINGS_SCHEMA, _build_table


@model('bronze.rankings',
       is_sql=True,
       kind='INCREMENTAL_BY_UNIQUE_KEY',
       unique_key=['ranking_date', 'player_id', 'tour'],
       description='Combined ATP and WTA weekly rankings with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union atp_rankings and wta_rankings, tagging each row with its tour."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    atp = _build_table(evaluator, catalog, 'atp_rankings', 'raw', RANKINGS_SCHEMA)
    wta = _build_table(evaluator, catalog, 'wta_rankings', 'raw', RANKINGS_SCHEMA)

    return ibis.union(atp.mutate(tour=ibis.literal('ATP')), wta.mutate(tour=ibis.literal('WTA')),
                      distinct=False).to_sql(dialect='duckdb')
```

- [ ] **Step 6: Run SQLmesh test**

```bash
uv run sqlmesh test tests/test_bronze_matches.yaml
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add models/bronze/ tests/test_bronze_matches.yaml
git commit -m "feat: add bronze models combining ATP and WTA data"
```

---

### Task 6: Silver models

**Files:**
- Create: `models/silver/matches.py`
- Create: `models/silver/players.py`
- Create: `models/silver/rankings.py`
- Create: `tests/test_silver_matches.yaml`

- [ ] **Step 1: Write SQLmesh unit test for silver.matches**

```yaml
# tests/test_silver_matches.yaml
test_silver_matches_casts_date_normalises_surface_filters_null_scores:
  model: silver.matches
  inputs:
    bronze.matches:
      columns:
        tourney_id: TEXT
        tourney_name: TEXT
        surface: TEXT
        draw_size: INT
        tourney_level: TEXT
        tourney_date: INT
        match_num: INT
        tour: TEXT
        winner_id: INT
        winner_seed: DOUBLE
        winner_entry: TEXT
        winner_name: TEXT
        winner_hand: TEXT
        winner_ht: DOUBLE
        winner_ioc: TEXT
        winner_age: DOUBLE
        loser_id: INT
        loser_seed: DOUBLE
        loser_entry: TEXT
        loser_name: TEXT
        loser_hand: TEXT
        loser_ht: DOUBLE
        loser_ioc: TEXT
        loser_age: DOUBLE
        score: TEXT
        best_of: INT
        round: TEXT
        minutes: DOUBLE
        w_ace: DOUBLE
        w_df: DOUBLE
        w_svpt: DOUBLE
        w_1stIn: DOUBLE
        w_1stWon: DOUBLE
        w_2ndWon: DOUBLE
        w_SvGm: DOUBLE
        w_bpSaved: DOUBLE
        w_bpFaced: DOUBLE
        l_ace: DOUBLE
        l_df: DOUBLE
        l_svpt: DOUBLE
        l_1stIn: DOUBLE
        l_1stWon: DOUBLE
        l_2ndWon: DOUBLE
        l_SvGm: DOUBLE
        l_bpSaved: DOUBLE
        l_bpFaced: DOUBLE
        winner_rank: DOUBLE
        winner_rank_points: DOUBLE
        loser_rank: DOUBLE
        loser_rank_points: DOUBLE
      rows:
        - tourney_id: "2024-540"
          match_num: 1
          tour: "ATP"
          surface: "grass"
          tourney_date: 20240701
          score: "6-3 6-4"
          winner_id: 104925
          loser_id: 105453
          tourney_name: "Wimbledon"
          tourney_level: "G"
          draw_size: 128
          best_of: 5
          round: "F"
          winner_seed: ~
          winner_entry: ~
          winner_name: "Novak Djokovic"
          winner_hand: "R"
          winner_ht: 188.0
          winner_ioc: "SRB"
          winner_age: 37.1
          loser_seed: ~
          loser_entry: ~
          loser_name: "Andy Murray"
          loser_hand: "R"
          loser_ht: 190.0
          loser_ioc: "GBR"
          loser_age: 37.3
          minutes: 135.0
          w_ace: 8.0
          w_df: 2.0
          w_svpt: 85.0
          w_1stIn: 55.0
          w_1stWon: 42.0
          w_2ndWon: 20.0
          w_SvGm: 12.0
          w_bpSaved: 3.0
          w_bpFaced: 4.0
          l_ace: 3.0
          l_df: 4.0
          l_svpt: 80.0
          l_1stIn: 50.0
          l_1stWon: 32.0
          l_2ndWon: 15.0
          l_SvGm: 11.0
          l_bpSaved: 1.0
          l_bpFaced: 6.0
          winner_rank: 2.0
          winner_rank_points: 7500.0
          loser_rank: 50.0
          loser_rank_points: 1000.0
        - tourney_id: "2024-540"
          match_num: 2
          tour: "ATP"
          surface: "Grass"
          tourney_date: 20240701
          score: ~
          winner_id: 105223
          loser_id: 106421
          tourney_name: "Wimbledon"
          tourney_level: "G"
          draw_size: 128
          best_of: 5
          round: "SF"
          winner_seed: ~
          winner_entry: ~
          winner_name: "Carlos Alcaraz"
          winner_hand: "R"
          winner_ht: 185.0
          winner_ioc: "ESP"
          winner_age: 21.1
          loser_seed: ~
          loser_entry: ~
          loser_name: "Jannik Sinner"
          loser_hand: "R"
          loser_ht: 188.0
          loser_ioc: "ITA"
          loser_age: 22.9
          minutes: ~
          w_ace: ~
          w_df: ~
          w_svpt: ~
          w_1stIn: ~
          w_1stWon: ~
          w_2ndWon: ~
          w_SvGm: ~
          w_bpSaved: ~
          w_bpFaced: ~
          l_ace: ~
          l_df: ~
          l_svpt: ~
          l_1stIn: ~
          l_1stWon: ~
          l_2ndWon: ~
          l_SvGm: ~
          l_bpSaved: ~
          l_bpFaced: ~
          winner_rank: ~
          winner_rank_points: ~
          loser_rank: ~
          loser_rank_points: ~
  outputs:
    partial: true
    rows:
      - tourney_id: "2024-540"
        match_num: 1
        surface: "Grass"
        tourney_date: "2024-07-01"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run sqlmesh test tests/test_silver_matches.yaml
```

Expected: Error — model `silver.matches` not found.

- [ ] **Step 3: Create models/silver/matches.py**

```python
"""Silver matches: cast date, normalise surface capitalisation, filter walkovers."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import BRONZE_MATCHES_SCHEMA, GATEWAY_CATALOG, _build_table


@model('silver.matches',
       is_sql=True,
       kind='INCREMENTAL_BY_TIME_RANGE',
       time_column='tourney_date',
       description='Cleaned matches: date cast, surface normalised, null scores removed.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Cast tourney_date to DATE, normalise surface, drop rows with no score."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    bronze = _build_table(evaluator, catalog, 'matches', 'bronze', BRONZE_MATCHES_SCHEMA)

    surface_clean = bronze.surface.lower().cases(('hard', 'Hard'), ('clay', 'Clay'), ('grass', 'Grass'),
                                                 ('carpet', 'Carpet'),
                                                 else_=bronze.surface)
    result = (bronze.filter(bronze.score.notnull()).mutate(
        tourney_date=bronze.tourney_date.cast('string').to_timestamp('%Y%m%d').date(), surface=surface_clean))

    return result.to_sql(dialect='duckdb')
```

- [ ] **Step 4: Create models/silver/players.py**

```python
"""Silver players: derive full_name, filter null player IDs."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, PLAYERS_SCHEMA, _build_table

# bronze.players has all PLAYERS_SCHEMA columns plus 'tour'
_BRONZE_PLAYERS_SCHEMA = ibis.Schema({**PLAYERS_SCHEMA, 'tour': 'string'})


@model('silver.players',
       is_sql=True,
       kind='INCREMENTAL_BY_UNIQUE_KEY',
       unique_key=['player_id', 'tour'],
       description='Player roster with full_name derived, null player_id rows removed.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Derive full_name, drop rows with null player_id."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    bronze = _build_table(evaluator, catalog, 'players', 'bronze', _BRONZE_PLAYERS_SCHEMA)

    result = (bronze.filter(bronze.player_id.notnull()).mutate(full_name=(bronze.first_name + ibis.literal(' ') +
                                                                          bronze.last_name).strip()))

    return result.to_sql(dialect='duckdb')
```

- [ ] **Step 5: Create models/silver/rankings.py**

```python
"""Silver rankings: cast ranking_date to DATE."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, RANKINGS_SCHEMA, _build_table

_BRONZE_RANKINGS_SCHEMA = ibis.Schema({**RANKINGS_SCHEMA, 'tour': 'string'})


@model('silver.rankings',
       is_sql=True,
       kind='INCREMENTAL_BY_TIME_RANGE',
       time_column='ranking_date',
       description='Weekly rankings with ranking_date cast to DATE.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Cast ranking_date from YYYYMMDD int to DATE."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    bronze = _build_table(evaluator, catalog, 'rankings', 'bronze', _BRONZE_RANKINGS_SCHEMA)

    result = bronze.mutate(ranking_date=bronze.ranking_date.cast('string').to_timestamp('%Y%m%d').date())

    return result.to_sql(dialect='duckdb')
```

- [ ] **Step 6: Run SQLmesh test**

```bash
uv run sqlmesh test tests/test_silver_matches.yaml
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add models/silver/ tests/test_silver_matches.yaml
git commit -m "feat: add silver models with date casting and surface normalisation"
```

---

### Task 7: Gold models

**Files:**
- Create: `models/gold/player_surface_stats.py`
- Create: `models/gold/head_to_head.py`
- Create: `models/gold/rankings_history.py`
- Create: `models/gold/tournament_stats.py`
- Create: `tests/test_gold_surface_stats.yaml`

- [ ] **Step 1: Write SQLmesh unit test for gold.player_surface_stats**

```yaml
# tests/test_gold_surface_stats.yaml
test_player_surface_stats_calculates_win_rate:
  model: gold.player_surface_stats
  inputs:
    silver.matches:
      columns:
        tourney_id: TEXT
        tourney_name: TEXT
        surface: TEXT
        draw_size: INT
        tourney_level: TEXT
        tourney_date: DATE
        match_num: INT
        tour: TEXT
        winner_id: INT
        winner_seed: DOUBLE
        winner_entry: TEXT
        winner_name: TEXT
        winner_hand: TEXT
        winner_ht: DOUBLE
        winner_ioc: TEXT
        winner_age: DOUBLE
        loser_id: INT
        loser_seed: DOUBLE
        loser_entry: TEXT
        loser_name: TEXT
        loser_hand: TEXT
        loser_ht: DOUBLE
        loser_ioc: TEXT
        loser_age: DOUBLE
        score: TEXT
        best_of: INT
        round: TEXT
        minutes: DOUBLE
        w_ace: DOUBLE
        w_df: DOUBLE
        w_svpt: DOUBLE
        w_1stIn: DOUBLE
        w_1stWon: DOUBLE
        w_2ndWon: DOUBLE
        w_SvGm: DOUBLE
        w_bpSaved: DOUBLE
        w_bpFaced: DOUBLE
        l_ace: DOUBLE
        l_df: DOUBLE
        l_svpt: DOUBLE
        l_1stIn: DOUBLE
        l_1stWon: DOUBLE
        l_2ndWon: DOUBLE
        l_SvGm: DOUBLE
        l_bpSaved: DOUBLE
        l_bpFaced: DOUBLE
        winner_rank: DOUBLE
        winner_rank_points: DOUBLE
        loser_rank: DOUBLE
        loser_rank_points: DOUBLE
      rows:
        - winner_id: 104925
          loser_id: 105453
          surface: "Grass"
          tour: "ATP"
          tourney_id: "2024-540"
          match_num: 1
          tourney_date: "2024-07-01"
          score: "6-3 6-4"
          tourney_name: "Wimbledon"
          tourney_level: "G"
          draw_size: 128
          best_of: 5
          round: "F"
          winner_seed: ~
          winner_entry: ~
          winner_name: "Novak Djokovic"
          winner_hand: "R"
          winner_ht: 188.0
          winner_ioc: "SRB"
          winner_age: 37.1
          loser_seed: ~
          loser_entry: ~
          loser_name: "Andy Murray"
          loser_hand: "R"
          loser_ht: 190.0
          loser_ioc: "GBR"
          loser_age: 37.3
          minutes: 135.0
          w_ace: 8.0
          w_df: 2.0
          w_svpt: 85.0
          w_1stIn: 55.0
          w_1stWon: 42.0
          w_2ndWon: 20.0
          w_SvGm: 12.0
          w_bpSaved: 3.0
          w_bpFaced: 4.0
          l_ace: 3.0
          l_df: 4.0
          l_svpt: 80.0
          l_1stIn: 50.0
          l_1stWon: 32.0
          l_2ndWon: 15.0
          l_SvGm: 11.0
          l_bpSaved: 1.0
          l_bpFaced: 6.0
          winner_rank: 2.0
          winner_rank_points: 7500.0
          loser_rank: 50.0
          loser_rank_points: 1000.0
        - winner_id: 104925
          loser_id: 200999
          surface: "Grass"
          tour: "ATP"
          tourney_id: "2024-540"
          match_num: 2
          tourney_date: "2024-07-01"
          score: "6-2 6-1"
          tourney_name: "Wimbledon"
          tourney_level: "G"
          draw_size: 128
          best_of: 5
          round: "SF"
          winner_seed: ~
          winner_entry: ~
          winner_name: "Novak Djokovic"
          winner_hand: "R"
          winner_ht: 188.0
          winner_ioc: "SRB"
          winner_age: 37.1
          loser_seed: ~
          loser_entry: ~
          loser_name: "Someone Else"
          loser_hand: "R"
          loser_ht: 185.0
          loser_ioc: "USA"
          loser_age: 25.0
          minutes: 80.0
          w_ace: 5.0
          w_df: 1.0
          w_svpt: 70.0
          w_1stIn: 50.0
          w_1stWon: 40.0
          w_2ndWon: 15.0
          w_SvGm: 10.0
          w_bpSaved: 2.0
          w_bpFaced: 2.0
          l_ace: 1.0
          l_df: 3.0
          l_svpt: 55.0
          l_1stIn: 30.0
          l_1stWon: 20.0
          l_2ndWon: 10.0
          l_SvGm: 7.0
          l_bpSaved: 0.0
          l_bpFaced: 8.0
          winner_rank: 2.0
          winner_rank_points: 7500.0
          loser_rank: 100.0
          loser_rank_points: 500.0
  outputs:
    partial: true
    rows:
      - player_id: 104925
        surface: "Grass"
        tour: "ATP"
        wins: 2
        losses: 0
        matches_played: 2
        win_rate: 1.0
      - player_id: 105453
        surface: "Grass"
        tour: "ATP"
        wins: 0
        losses: 1
        matches_played: 1
        win_rate: 0.0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run sqlmesh test tests/test_gold_surface_stats.yaml
```

Expected: Error — model `gold.player_surface_stats` not found.

- [ ] **Step 3: Create models/gold/player_surface_stats.py**

```python
"""Gold: win rate per player per surface."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, SILVER_MATCHES_SCHEMA, _build_table


@model('gold.player_surface_stats',
       is_sql=True,
       kind='FULL',
       description='Win rate, wins, losses, and matches played per player per surface.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Calculate win/loss record and win_rate per player, tour, and surface."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    matches = _build_table(evaluator, catalog, 'matches', 'silver', SILVER_MATCHES_SCHEMA)

    wins = (matches.group_by(['winner_id', 'tour',
                              'surface']).aggregate(wins=matches.match_num.count()).rename(player_id='winner_id'))
    losses = (matches.group_by(['loser_id', 'tour',
                                'surface']).aggregate(losses=matches.match_num.count()).rename(player_id='loser_id'))
    joined = wins.outer_join(losses, ['player_id', 'tour', 'surface'])
    resolved = joined.mutate(player_id=ibis.coalesce(wins.player_id, losses.player_id),
                             tour=ibis.coalesce(wins.tour, losses.tour),
                             surface=ibis.coalesce(wins.surface, losses.surface),
                             wins=wins.wins.fillna(0).cast('int64'),
                             losses=losses.losses.fillna(0).cast('int64'))
    result = resolved.mutate(matches_played=ibis._.wins + ibis._.losses,
                             win_rate=(ibis._.wins.cast('float64') / (ibis._.wins + ibis._.losses)))[[
                                 'player_id', 'tour', 'surface', 'wins', 'losses', 'matches_played', 'win_rate'
                             ]]

    return result.to_sql(dialect='duckdb')
```

- [ ] **Step 4: Create models/gold/head_to_head.py**

```python
"""Gold: head-to-head win records between player pairs."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, SILVER_MATCHES_SCHEMA, _build_table


@model('gold.head_to_head',
       is_sql=True,
       kind='FULL',
       description='H2H records: wins for player1 vs player2 (player1_id < player2_id).')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Count wins for each canonical (player1, player2) pair where player1_id < player2_id."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    matches = _build_table(evaluator, catalog, 'matches', 'silver', SILVER_MATCHES_SCHEMA)

    canonical = matches.mutate(player1_id=ibis.least(matches.winner_id, matches.loser_id),
                               player2_id=ibis.greatest(matches.winner_id, matches.loser_id),
                               player1_won=(matches.winner_id < matches.loser_id).cast('int64'))
    result = (canonical.group_by(['player1_id', 'player2_id', 'tour']).aggregate(
        player1_wins=canonical.player1_won.sum(),
        total_matches=canonical.match_num.count()).mutate(
            player2_wins=ibis._.total_matches - ibis._.player1_wins))

    return result.to_sql(dialect='duckdb')
```

- [ ] **Step 5: Create models/gold/rankings_history.py**

```python
"""Gold: career ranking history per player."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, SILVER_RANKINGS_SCHEMA, _build_table


@model('gold.rankings_history',
       is_sql=True,
       kind='INCREMENTAL_BY_TIME_RANGE',
       time_column='ranking_date',
       description='Weekly ranking snapshots with best-ever rank per player.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Pass through silver rankings and add career_best_rank window column."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    rankings = _build_table(evaluator, catalog, 'rankings', 'silver', SILVER_RANKINGS_SCHEMA)

    career_window = ibis.window(group_by=['player_id', 'tour'], order_by='ranking_date')
    result = rankings.mutate(career_best_rank=rankings.ranking.min().over(career_window))

    return result.to_sql(dialect='duckdb')
```

- [ ] **Step 6: Create models/gold/tournament_stats.py**

```python
"""Gold: aggregate stats per tournament edition."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, SILVER_MATCHES_SCHEMA, _build_table


@model('gold.tournament_stats',
       is_sql=True,
       kind='FULL',
       description='Match count, avg duration, and distinct winner count per tournament.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Aggregate matches by tourney_id, name, surface, and year."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    matches = _build_table(evaluator, catalog, 'matches', 'silver', SILVER_MATCHES_SCHEMA)

    matches_with_year = matches.mutate(tourney_year=matches.tourney_date.year())
    result = (matches_with_year.group_by(['tourney_id', 'tourney_name', 'surface', 'tour', 'tourney_year'
                                          ]).aggregate(matches_played=matches_with_year.match_num.count(),
                                                       avg_match_minutes=matches_with_year.minutes.mean(),
                                                       distinct_winners=matches_with_year.winner_id.nunique()))

    return result.to_sql(dialect='duckdb')
```

- [ ] **Step 7: Run SQLmesh test**

```bash
uv run sqlmesh test tests/test_gold_surface_stats.yaml
```

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add models/gold/ tests/test_gold_surface_stats.yaml
git commit -m "feat: add gold models for surface stats, H2H, rankings history, and tournament stats"
```

---

### Task 8: Validations

**Files:**
- Create: `validations/__init__.py`
- Create: `validations/connection.py`
- Create: `validations/checks.py`
- Create: `src/my_data_platform/validate.py`
- Create: `tests/test_validations.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_validations.py
from unittest.mock import MagicMock, patch
from validations.connection import get_connection


@patch('validations.connection.ibis.duckdb.connect')
def test_get_connection_uses_catalog_path_from_conf(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    get_connection()

    mock_connect.assert_called_once_with(extensions=['ducklake'])
    mock_conn.attach.assert_called_once()
    call_args = mock_conn.attach.call_args[0][0]
    assert call_args.startswith('ducklake:')
    assert 'catalog.ducklake' in call_args
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_validations.py -v
```

Expected: `ModuleNotFoundError: No module named 'validations.connection'`

- [ ] **Step 3: Create validations/__init__.py**

```python
"""Data quality validation functions using pointblank."""
```

- [ ] **Step 4: Create validations/connection.py**

```python
"""Ibis connection factory for validation queries."""
import ibis

from conf.config import conf


def get_connection(read_only: bool = True) -> ibis.BaseBackend:
    """Return an Ibis DuckDB connection with the local DuckLake catalog attached.

    Catalog path is read from conf/parameters.toml [ducklake] catalog_path.
    """
    catalog_path: str = conf['ducklake.catalog_path']
    con = ibis.duckdb.connect(extensions=['ducklake'])
    con.attach(f'ducklake:{catalog_path}', name='my_lakehouse', read_only=read_only)
    return con
```

- [ ] **Step 5: Create validations/checks.py**

```python
"""Pointblank validation chains for each layer of the data lake."""
from __future__ import annotations

import ibis
import pointblank as pb

from validations.connection import get_connection


def validate_raw_matches(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate raw.atp_matches and raw.wta_matches have required columns non-null."""
    _con = con or get_connection()
    atp = _con.table('atp_matches', database='my_lakehouse.raw')
    v = pb.Validate(data=atp, tbl_name='raw.atp_matches', label='Raw ATP Matches')
    v = v.col_vals_not_null(columns='tourney_id')
    v = v.col_vals_not_null(columns='winner_id')
    v = v.col_vals_not_null(columns='loser_id')
    v = v.col_vals_not_null(columns='tourney_date')
    return v.interrogate()


def validate_bronze_matches(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate bronze.matches: no duplicate unique keys, tour column valid."""
    _con = con or get_connection()
    table = _con.table('matches', database='my_lakehouse.bronze')
    v = pb.Validate(data=table, tbl_name='bronze.matches', label='Bronze Matches')
    v = v.col_vals_not_null(columns='tourney_id')
    v = v.col_vals_not_null(columns='match_num')
    v = v.col_vals_not_null(columns='tour')
    v = v.col_vals_in_set(columns='tour', set=['ATP', 'WTA'])
    v = v.rows_distinct(columns_subset=['tourney_id', 'match_num', 'tour'])
    return v.interrogate()


def validate_silver_matches(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate silver.matches: surface in known set, no null scores."""
    _con = con or get_connection()
    table = _con.table('matches', database='my_lakehouse.silver')
    v = pb.Validate(data=table, tbl_name='silver.matches', label='Silver Matches')
    v = v.col_vals_not_null(columns='score')
    v = v.col_vals_in_set(columns='surface', set=['Hard', 'Clay', 'Grass', 'Carpet'])
    v = v.col_vals_not_null(columns='tourney_date')
    return v.interrogate()


def validate_gold_surface_stats(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate gold.player_surface_stats: win_rate in [0, 1], matches_played > 0."""
    _con = con or get_connection()
    table = _con.table('player_surface_stats', database='my_lakehouse.gold')
    v = pb.Validate(data=table, tbl_name='gold.player_surface_stats', label='Gold Surface Stats')
    v = v.col_vals_between(columns='win_rate', left=0.0, right=1.0)
    v = v.col_vals_gt(columns='matches_played', value=0)
    v = v.rows_distinct(columns_subset=['player_id', 'surface', 'tour'])
    return v.interrogate()
```

- [ ] **Step 6: Create src/my_data_platform/validate.py**

```python
"""CLI entry point: run all pointblank validation checks across every layer."""
from __future__ import annotations

import sys

from loguru import logger

from validations.checks import (
    validate_bronze_matches,
    validate_gold_surface_stats,
    validate_raw_matches,
    validate_silver_matches,
)
from validations.connection import get_connection


def main() -> None:
    """Run all layer validations. Exit 1 if any check fails."""
    con = get_connection()
    checks = [
        validate_raw_matches(con),
        validate_bronze_matches(con),
        validate_silver_matches(con),
        validate_gold_surface_stats(con),
    ]
    failed = [v for v in checks if not v.all_passed()]
    if failed:
        for v in failed:
            logger.error(f'Validation failed: {v.label}')
        sys.exit(1)
    logger.info('All validation checks passed.')


if __name__ == '__main__':
    main()
```

- [ ] **Step 7: Run tests**

```bash
uv run pytest tests/test_validations.py -v
```

Expected: PASS (1 test)

- [ ] **Step 8: Commit**

```bash
git add validations/ src/my_data_platform/validate.py tests/test_validations.py
git commit -m "feat: add pointblank validations and validate CLI"
```

---

### Task 9: Publishing

**Files:**
- Create: `src/my_data_platform/publish.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_publish.py
from unittest.mock import MagicMock, patch
from my_data_platform.publish import publish_gold_tables


@patch('my_data_platform.publish.get_connection')
@patch('my_data_platform.publish.pins.board_folder')
def test_publish_gold_tables_writes_all_four_tables(mock_board_folder, mock_get_conn):
    mock_con = MagicMock()
    mock_get_conn.return_value = mock_con
    mock_board = MagicMock()
    mock_board_folder.return_value = mock_board

    publish_gold_tables(board_path='data/pins_board')

    assert mock_board.pin_write.call_count == 4
    pin_names = [call.args[1] for call in mock_board.pin_write.call_args_list]
    assert 'gold/player_surface_stats' in pin_names
    assert 'gold/head_to_head' in pin_names
    assert 'gold/rankings_history' in pin_names
    assert 'gold/tournament_stats' in pin_names
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_publish.py -v
```

Expected: `ImportError: cannot import name 'publish_gold_tables'`

- [ ] **Step 3: Create src/my_data_platform/publish.py**

```python
"""Push gold layer tables to the pins board for downstream consumers."""
from __future__ import annotations

import pins
from loguru import logger

from conf.config import conf
from validations.connection import get_connection

_GOLD_TABLES = [
    'player_surface_stats',
    'head_to_head',
    'rankings_history',
    'tournament_stats',
]


def publish_gold_tables(board_path: str | None = None) -> None:
    """Read each gold table and write to the configured pins board as Parquet."""
    path = board_path or conf['pins.board_path']
    board = pins.board_folder(path)
    con = get_connection()

    for table_name in _GOLD_TABLES:
        logger.info(f'Publishing gold.{table_name}')
        df = con.table(table_name, database='my_lakehouse.gold').execute()
        board.pin_write(df, f'gold/{table_name}', type='parquet')
        logger.info(f'Pinned gold/{table_name} ({len(df):,} rows)')

    con.disconnect()
    logger.info('Publish complete.')


def main() -> None:
    """Entry point for just publish."""
    publish_gold_tables()


if __name__ == '__main__':
    main()
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_publish.py -v
```

Expected: PASS (1 test)

- [ ] **Step 5: Run full test suite**

```bash
just test
```

Expected: All pytest tests PASS. SQLmesh tests PASS.

- [ ] **Step 6: Update CLAUDE.md pipeline commands section**

In `CLAUDE.md`, replace the Commands section's pipeline commands with:

```markdown
## Commands

This project uses [`just`](https://github.com/casey/just) as a task runner. All commands use `uv` for Python environment management.

```sh
just ingest     # download raw ATP/WTA CSVs into data/01_raw/
just run        # local SQLmesh plan --auto-apply (incremental, DuckLake on disk)
just stage      # SQLmesh plan dev on MotherDuck (isolated *__dev schemas)
just deploy     # SQLmesh plan prod on MotherDuck (promote to production)
just validate   # pointblank checks across all layers
just publish    # push gold tables to pins board
just lint       # ruff check + sqlmesh lint
just test       # pytest + sqlmesh test
just docs       # quartodoc build + quarto preview
```

**Initial setup** (after cloning):
```sh
just init-project   # uv sync + pre-commit install + create data dirs
just ingest         # download raw data
just run            # build the full pipeline locally
```

**Run a single pytest test:**
```sh
uv run --only-group test pytest tests/path/to/test.py::test_name -v
```

**Run a single SQLmesh model test:**
```sh
uv run sqlmesh test tests/test_bronze_matches.yaml
```
```

- [ ] **Step 7: Final commit**

```bash
git add src/my_data_platform/publish.py tests/test_publish.py CLAUDE.md
git commit -m "feat: add gold publishing via pins and complete pipeline"
```

---

## End-to-end smoke test

After all tasks complete, verify the full pipeline runs:

```bash
just ingest          # downloads ~10 years of ATP + WTA data
just run             # builds raw → bronze → silver → gold (first run ~2-3 min)
just validate        # all pointblank checks pass
just publish         # gold tables written to data/pins_board/
just run             # second run — all models skip (nothing changed)
```
