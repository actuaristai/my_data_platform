# Data Lake (DuckLake + SQLmesh + Ibis + Tennis) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working open-source data lake in `my_data_platform` using DuckLake + SQLmesh + Ibis, with ATP/WTA tennis data flowing through bronze → silver → gold layers, pins at the edges for ingest and publishing, and pointblank for validation.

**Architecture:** SQLmesh manages incremental execution and column-level lineage via a local DuckLake catalog (`data/catalog.ducklake`), with a MotherDuck gateway for staging (`dev` environment) and production. All Ibis models use `is_sql=True` returning `.to_sql(dialect='duckdb')` to preserve column-level lineage. Bronze layer uses SQL SEED models (one per CSV) plus SQL UNION ALL models to combine ATP+WTA. Silver and gold layers use Python `@model` decorators with Ibis. pins handles raw data download from GitHub and gold layer publishing for consumers.

**Tech Stack:** Python 3.13, uv, SQLmesh, DuckLake, ibis-framework[duckdb], polars, pins, pointblank, dynaconf, requests, just, ruff, pytest

## Implementation Notes (Architecture Corrections)

The following deviates from the original plan spec. All implemented code uses these patterns:

- **No raw layer**: SEEDs are in `models/bronze/` (not `models/raw/`). Bronze = SEED models + SQL UNION ALL models.
- **`winner_seed`/`loser_seed` are VARCHAR** in bronze SEEDs — JeffSackmann data contains `'Q'` (qualifier) values. Silver uses `.try_cast('float64')` to coerce.
- **`dict[str, str]` schemas**: `MATCHES_SCHEMA` etc. are plain `dict[str, str]`, not `ibis.Schema`. SQLmesh cannot serialize `ibis.Schema` in Python model state.
- **`from ibis import _` inside `def entrypoint`**: Must be a local import — module-level `from ibis import _` conflicts with SQLmesh's macro `_` namespace.
- **`as_timestamp('%Y%m%d')`**: ibis 12.x API (not `to_timestamp`). Used for `int32 → DATE` conversion.
- **No `validate_raw_matches`**: The raw layer doesn't exist. Use `validate_bronze_seeds` to check `my_lakehouse.bronze.atp_matches` and `my_lakehouse.bronze.wta_matches`.

---

## File Map

| File | Status | Purpose |
|------|--------|---------|
| `pyproject.toml` | DONE | sqlmesh, ibis, pins, pointblank, requests deps |
| `config.yaml` | DONE | SQLmesh local + MotherDuck gateway config |
| `conf/parameters.toml` | DONE | ducklake, pins, tennis config sections |
| `justfile` | DONE | run/stage/deploy/ingest/validate/publish commands |
| `models/_util.py` | DONE | Shared `_build_table()`, `GATEWAY_CATALOG`, all table schemas (dict[str,str]) |
| `models/bronze/atp_matches.sql` | DONE | SEED pointing at data/01_raw/atp_matches.csv (VARCHAR seed cols) |
| `models/bronze/wta_matches.sql` | DONE | SEED pointing at data/01_raw/wta_matches.csv (VARCHAR seed cols) |
| `models/bronze/atp_players.sql` | DONE | SEED pointing at data/01_raw/atp_players.csv |
| `models/bronze/wta_players.sql` | DONE | SEED pointing at data/01_raw/wta_players.csv |
| `models/bronze/atp_rankings.sql` | DONE | SEED pointing at data/01_raw/atp_rankings.csv |
| `models/bronze/wta_rankings.sql` | DONE | SEED pointing at data/01_raw/wta_rankings.csv |
| `models/bronze/matches.sql` | DONE | SQL UNION ALL: atp_matches ∪ wta_matches + tour label |
| `models/bronze/players.sql` | DONE | SQL UNION ALL: atp_players ∪ wta_players + tour label |
| `models/bronze/rankings.sql` | DONE | SQL UNION ALL: atp_rankings ∪ wta_rankings + tour label |
| `models/silver/matches.py` | DONE | Ibis: cast date, normalise surface, try_cast seeds, filter null scores |
| `models/silver/players.py` | DONE | Ibis: full_name, filter null player_id |
| `models/silver/rankings.py` | DONE | Ibis: cast ranking_date to DATE |
| `models/gold/player_surface_stats.py` | DONE | Ibis: win rate by player + surface |
| `models/gold/head_to_head.py` | DONE | Ibis: H2H records between player pairs |
| `models/gold/rankings_history.py` | DONE | Ibis: career ranking trajectory |
| `models/gold/tournament_stats.py` | DONE | Ibis: tournament-level aggregates |
| `validations/connection.py` | DONE | `get_connection()` reads catalog_path from conf |
| `validations/checks.py` | **FIX** | Replace `validate_raw_matches` with `validate_bronze_seeds` |
| `src/my_data_platform/ingest.py` | DONE | Download ATP/WTA CSVs from GitHub → data/01_raw/ |
| `src/my_data_platform/validate.py` | **FIX** | Remove `validate_raw_matches` import/call, use `validate_bronze_seeds` |
| `src/my_data_platform/publish.py` | DONE | Push gold tables → pins board |
| `tests/conftest.py` | DONE | `con` fixture: in-memory DuckDB for pytest |
| `tests/test_ingest.py` | DONE | Unit tests for ingest functions |
| `tests/test_validations.py` | DONE | Tests for connection.py |
| `tests/test_bronze_matches.yaml` | **CREATE** | SQLmesh unit test: bronze.matches |
| `tests/test_silver_matches.yaml` | DONE | SQLmesh unit test: silver.matches |
| `tests/test_gold_surface_stats.yaml` | DONE | SQLmesh unit test: gold.player_surface_stats |
| `tests/test_publish.py` | DONE | Unit tests for publish.py |

---

### Task 1: Foundation — dependencies, SQLmesh config, justfile [DONE]

Tasks 1–7 are complete. The pipeline runs successfully (`just run` builds all 16 models).

---

### Task 2: Shared model utilities [DONE]

`models/_util.py` uses `dict[str, str]` schemas (not `ibis.Schema`) so SQLmesh can serialize them.
`_build_table` uses `ibis.Schema(fallback_schema)` internally when constructing the table object.

```python
MATCHES_SCHEMA: dict[str, str] = {'tourney_id': 'string', ...}
BRONZE_MATCHES_SCHEMA: dict[str, str] = {**MATCHES_SCHEMA,
                                          'winner_seed': 'string',   # VARCHAR: 'Q' values exist
                                          'loser_seed': 'string',
                                          'tour': 'string'}
```

---

### Task 3: Data ingestion [DONE]

`src/my_data_platform/ingest.py` downloads per-year CSVs and concatenates them via polars.

---

### Task 4: Bronze SEED models [DONE]

SEEDs are in `models/bronze/` (not `models/raw/`). `winner_seed` and `loser_seed` are `VARCHAR` to handle qualifier values like `'Q'`.

```sql
-- models/bronze/atp_matches.sql
MODEL (name bronze.atp_matches,
       kind SEED (path '../../data/01_raw/atp_matches.csv'),
       columns (tourney_id VARCHAR,
                ...
                winner_seed VARCHAR,  -- handles 'Q', 'WC', etc.
                loser_seed  VARCHAR,
                ...));
```

---

### Task 5: Bronze union SQL models [DONE]

Bronze union models are plain SQL UNION ALL (not Python ibis). SQLmesh FULL models.

```sql
-- models/bronze/matches.sql
MODEL (name bronze.matches, kind FULL,
       description 'Combined ATP and WTA matches with tour label.');
SELECT *, 'ATP' AS tour FROM bronze.atp_matches
UNION ALL
SELECT *, 'WTA' AS tour FROM bronze.wta_matches
```

---

### Task 6: Silver models [DONE]

Key implementation details:
- `from ibis import _` inside `def entrypoint` (local import — avoids SQLmesh macro namespace conflict)
- `as_timestamp('%Y%m%d')` not `to_timestamp` (ibis 12.x API)
- `try_cast('float64')` for `winner_seed`/`loser_seed` (handles `'Q'` → `NULL`)
- `_['col']` syntax for column references (not `bronze.col` which can cause ambiguity)

```python
@model('silver.matches', is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_TIME_RANGE, 'time_column': 'tourney_date'}, ...)
def entrypoint(evaluator: MacroEvaluator) -> str:
    from ibis import _  # noqa: PLC0415
    ...
    result = bronze \
        .filter(_['score'].notnull()) \
        .mutate(tourney_date=_['tourney_date'].cast('string').as_timestamp('%Y%m%d').date(),
                surface=_['surface'].lower().cases(...),
                winner_seed=_['winner_seed'].try_cast('float64'),
                loser_seed=_['loser_seed'].try_cast('float64'))
```

---

### Task 7: Gold models [DONE]

All gold models follow the same `from ibis import _` local import pattern.

---

### Task 8: Fix validations — replace validate_raw_matches

**Files:**
- Modify: `validations/checks.py`
- Modify: `src/my_data_platform/validate.py`

The `validate_raw_matches` function references `my_lakehouse.raw` which does not exist (no raw layer).
Replace it with `validate_bronze_seeds` that validates the individual SEED tables in `my_lakehouse.bronze`.

- [ ] **Step 1: Update validations/checks.py**

Replace the entire file with:

```python
"""Pointblank validation chains for each layer of the data lake."""
from __future__ import annotations

import ibis
import pointblank as pb

from validations.connection import get_connection


def validate_bronze_seeds(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate bronze SEED tables: required columns non-null, tour values valid."""
    _con = con or get_connection()
    atp = _con.table('atp_matches', database='my_lakehouse.bronze')
    v = pb.Validate(data=atp, tbl_name='bronze.atp_matches', label='Bronze ATP Matches') \
        .col_vals_not_null(columns='tourney_id') \
        .col_vals_not_null(columns='winner_id') \
        .col_vals_not_null(columns='loser_id') \
        .col_vals_not_null(columns='tourney_date')
    return v.interrogate()


def validate_bronze_matches(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate bronze.matches: no duplicate unique keys, tour column valid."""
    _con = con or get_connection()
    table = _con.table('matches', database='my_lakehouse.bronze')
    v = pb.Validate(data=table, tbl_name='bronze.matches', label='Bronze Matches') \
        .col_vals_not_null(columns='tourney_id') \
        .col_vals_not_null(columns='match_num') \
        .col_vals_not_null(columns='tour') \
        .col_vals_in_set(columns='tour', set=['ATP', 'WTA']) \
        .rows_distinct(columns_subset=['tourney_id', 'match_num', 'tour'])
    return v.interrogate()


def validate_silver_matches(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate silver.matches: surface in known set, no null scores."""
    _con = con or get_connection()
    table = _con.table('matches', database='my_lakehouse.silver')
    v = pb.Validate(data=table, tbl_name='silver.matches', label='Silver Matches') \
        .col_vals_not_null(columns='score') \
        .col_vals_in_set(columns='surface', set=['Hard', 'Clay', 'Grass', 'Carpet']) \
        .col_vals_not_null(columns='tourney_date')
    return v.interrogate()


def validate_gold_surface_stats(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate gold.player_surface_stats: win_rate in [0, 1], matches_played > 0."""
    _con = con or get_connection()
    table = _con.table('player_surface_stats', database='my_lakehouse.gold')
    v = pb.Validate(data=table, tbl_name='gold.player_surface_stats', label='Gold Surface Stats') \
        .col_vals_between(columns='win_rate', left=0.0, right=1.0) \
        .col_vals_gt(columns='matches_played', value=0) \
        .rows_distinct(columns_subset=['player_id', 'surface', 'tour'])
    return v.interrogate()
```

- [ ] **Step 2: Update src/my_data_platform/validate.py**

Replace `validate_raw_matches` with `validate_bronze_seeds`:

```python
"""CLI entry point: run all pointblank validation checks across every layer."""
from __future__ import annotations

import sys

from loguru import logger

from validations.checks import (validate_bronze_matches,
                                validate_bronze_seeds,
                                validate_gold_surface_stats,
                                validate_silver_matches)
from validations.connection import get_connection


def main() -> None:
    """Run all layer validations. Exit 1 if any check fails."""
    con = get_connection()
    checks = [validate_bronze_seeds(con),
              validate_bronze_matches(con),
              validate_silver_matches(con),
              validate_gold_surface_stats(con)]
    failed = [v for v in checks if not v.all_passed()]
    if failed:
        for v in failed:
            logger.error(f'Validation failed: {v.label}')
        sys.exit(1)
    logger.info('All validation checks passed.')


if __name__ == '__main__':
    main()
```

- [ ] **Step 3: Update tests/test_validations.py**

The existing test checks `get_connection` — verify it still passes with the new check name:

```bash
uv run --only-group test pytest tests/test_validations.py -v
```

Expected: PASS

- [ ] **Step 4: Create tests/test_bronze_matches.yaml**

```yaml
test_bronze_matches_combines_tours_and_adds_tour_column:
  model: bronze.matches
  inputs:
    bronze.atp_matches:
      columns:
        tourney_id: TEXT
        tourney_name: TEXT
        surface: TEXT
        draw_size: INT
        tourney_level: TEXT
        tourney_date: INT
        match_num: INT
        winner_id: INT
        winner_seed: TEXT
        winner_entry: TEXT
        winner_name: TEXT
        winner_hand: TEXT
        winner_ht: DOUBLE
        winner_ioc: TEXT
        winner_age: DOUBLE
        loser_id: INT
        loser_seed: TEXT
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
    bronze.wta_matches:
      columns:
        tourney_id: TEXT
        tourney_name: TEXT
        surface: TEXT
        draw_size: INT
        tourney_level: TEXT
        tourney_date: INT
        match_num: INT
        winner_id: INT
        winner_seed: TEXT
        winner_entry: TEXT
        winner_name: TEXT
        winner_hand: TEXT
        winner_ht: DOUBLE
        winner_ioc: TEXT
        winner_age: DOUBLE
        loser_id: INT
        loser_seed: TEXT
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
          winner_seed: "1"
          winner_entry: ~
          winner_name: "Iga Swiatek"
          winner_hand: "R"
          winner_ht: 175.0
          winner_ioc: "POL"
          winner_age: 23.1
          loser_seed: "5"
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

- [ ] **Step 5: Run SQLmesh bronze test**

```bash
uv run sqlmesh test tests/test_bronze_matches.yaml
```

Expected: PASS

- [ ] **Step 6: Run full test suite**

```bash
uv run --only-group test pytest -v -p no:faulthandler -W ignore::DeprecationWarning --doctest-modules
```

Expected: All tests PASS

- [ ] **Step 7: Run SQLmesh tests**

```bash
uv run sqlmesh test
```

Expected: All SQLmesh tests PASS

- [ ] **Step 8: Commit**

```bash
git add validations/checks.py src/my_data_platform/validate.py tests/test_bronze_matches.yaml
git commit -m "fix: replace validate_raw_matches with validate_bronze_seeds (no raw layer)"
```

---

### Task 9: End-to-end integration

**Files:** None new — this is smoke-test + publish.

- [ ] **Step 1: Run validate**

```bash
just validate
```

Expected: All pointblank checks pass, exit 0.

- [ ] **Step 2: Run publish**

```bash
just publish
```

Expected: 4 gold tables written to `data/pins_board/`.

- [ ] **Step 3: Verify pins board**

```bash
uv run python -c "import pins; b = pins.board_folder('data/pins_board'); print(b.pin_list())"
```

Expected: `['gold/head_to_head', 'gold/player_surface_stats', 'gold/rankings_history', 'gold/tournament_stats']`

- [ ] **Step 4: Final commit**

```bash
git add data/pins_board/
git commit -m "chore: add published gold pins artefacts"
```

---

## End-to-end smoke test

```bash
just run             # confirm no changes needed (already built)
just validate        # all pointblank checks pass
just publish         # gold tables written to data/pins_board/
```
