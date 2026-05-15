# Data Lake Design: DuckLake + SQLmesh + Ibis

**Date:** 2026-05-15
**Status:** Approved

## Overview

Add a fully functional open-source data lake to `my_data_platform` using DuckLake as the lakehouse catalog, SQLmesh for pipeline orchestration and incremental execution, and Ibis for portable transformations. The implementation uses Jeff Sackmann's public ATP/WTA tennis dataset as the working example. The architecture is local-first (DuckLake on disk) with a clear promotion path to MotherDuck for staging and production.

This serves two purposes: a working reference implementation and a reusable template pattern that other projects can clone and adapt.

---

## Stack

| Concern | Tool | Reason |
|---------|------|--------|
| Lakehouse catalog | DuckLake | ACID transactions, time travel, Parquet-native, local→MotherDuck |
| Pipeline orchestration + incremental | SQLmesh | Built-in state tracking, column-level lineage, skip-if-unchanged, native MotherDuck |
| Transformations | Ibis (`is_sql=True`) | Portable Python API, compiles to SQL for lineage preservation |
| Data ingestion | pins | Versioned pull from external source into raw layer |
| Gold publishing | pins | Versioned push from gold layer to consumers |
| Data validation | pointblank | Declarative quality checks per layer |
| Config | dynaconf | Local parameters + secrets separation |
| Package manager | uv | Fast, lockfile-based |
| Task runner | just | Consistent CLI for all pipeline commands |

---

## Architecture

```
GitHub (JeffSackmann/tennis_atp + tennis_wta)
    ↓  just ingest  →  ingest.py (pins.pin_download)
data/01_raw/   raw CSVs
    ↓  SQLmesh SEED models
raw.atp_matches, raw.wta_matches
raw.atp_players, raw.wta_players
raw.atp_rankings, raw.wta_rankings
    ↓  bronze Ibis models (INCREMENTAL_BY_UNIQUE_KEY)
bronze.matches     ← ATP + WTA combined, tour column, cast types
bronze.players     ← ATP + WTA combined
bronze.rankings    ← ATP + WTA combined
    ↓  silver Ibis models (INCREMENTAL_BY_TIME_RANGE)
silver.matches     ← nulls cleaned, surface normalised, stats derived
silver.players     ← full profiles, nationality standardised
silver.rankings    ← weekly snapshots
    ↓  gold Ibis models (INCREMENTAL_BY_TIME_RANGE or FULL)
gold.player_surface_stats    ← win rate by surface per player
gold.head_to_head            ← H2H records between player pairs
gold.rankings_history        ← career ranking trajectory
gold.tournament_stats        ← tournament-level aggregates
    ↓  just publish  →  publish.py (pins.pin_write)
pins board (consumed by Quarto reports, dashboards, other projects)
```

---

## Data Source

**Jeff Sackmann tennis datasets**
- ATP: `github.com/JeffSackmann/tennis_atp`
- WTA: `github.com/JeffSackmann/tennis_wta`

Files ingested:
- `atp_matches_YYYY.csv` / `wta_matches_YYYY.csv` — one per year
- `atp_players.csv` / `wta_players.csv` — player master
- `atp_rankings_YYYY.csv` / `wta_rankings_YYYY.csv` — weekly rankings

Default year range: last 10 years (configurable via `conf/parameters.toml`). Full history back to 1968 is available but not ingested by default.

---

## SQLmesh Model Structure

```
models/
  raw/
    atp_matches.sql       SEED, path per year in data/01_raw/
    wta_matches.sql       SEED
    atp_players.sql       SEED
    wta_players.sql       SEED
    atp_rankings.sql      SEED
    wta_rankings.sql      SEED
  bronze/
    matches.py            Ibis, INCREMENTAL_BY_UNIQUE_KEY (tourney_id + match_num + tour)
    players.py            Ibis, INCREMENTAL_BY_UNIQUE_KEY (player_id)
    rankings.py           Ibis, INCREMENTAL_BY_UNIQUE_KEY (player_id + ranking_date)
  silver/
    matches.py            Ibis, INCREMENTAL_BY_TIME_RANGE (tourney_date)
    players.py            Ibis, INCREMENTAL_BY_UNIQUE_KEY (player_id)
    rankings.py           Ibis, INCREMENTAL_BY_TIME_RANGE (ranking_date)
  gold/
    player_surface_stats.py   Ibis, FULL
    head_to_head.py           Ibis, FULL
    rankings_history.py       Ibis, INCREMENTAL_BY_TIME_RANGE (ranking_date)
    tournament_stats.py       Ibis, FULL
```

### Ibis Model Pattern

All Python models use `is_sql=True` so SQLmesh can parse the returned SQL for column-level lineage. Schema is resolved via a shared `_build_table()` helper (carried over from the tutorial) that handles the SQLmesh loading stage restriction and falls back to a hardcoded schema when needed.

```python
@model('bronze.matches', is_sql=True, kind='INCREMENTAL_BY_UNIQUE_KEY',
       unique_key='tourney_id + match_num + tour')
def entrypoint(evaluator: MacroEvaluator) -> str:
    atp = _build_table(evaluator, catalog, 'atp_matches', 'raw')
    wta = _build_table(evaluator, catalog, 'wta_matches', 'raw')
    query = ibis.union(
        atp.mutate(tour=ibis.literal('ATP')),
        wta.mutate(tour=ibis.literal('WTA')),
    ).to_sql()
    return query
```

A shared `models/_util.py` provides `_build_table()` and `GATEWAY_CATALOG` so they are not duplicated across model files.

---

## Gateways and Environments

```yaml
# config.yaml (key sections)
gateways:
  local_gateway:
    connection:
      type: duckdb
      catalogs:
        my_lakehouse:
          type: ducklake
          path: data/catalog.ducklake
          data_path: data/storage   # DuckLake manages Parquet here; raw CSVs stay in data/01_raw/
    state_connection:
      type: duckdb
      database: data/sqlmesh_state.db

  motherduck:
    connection:
      type: motherduck
      catalogs:
        my_lakehouse: "md:my_lakehouse"
      token: {{ env_var('MOTHERDUCK_TOKEN') }}
    state_connection:
      type: motherduck
      database: "md:sqlmesh_state"

default_gateway: local_gateway   # safe default — never MotherDuck by accident
```

| Command | Gateway | SQLmesh environment | Effect |
|---------|---------|--------------------|-|
| `just run` | local_gateway | prod | DuckLake on disk, full incremental run |
| `just stage` | motherduck | dev | Isolated `*__dev` schemas on MotherDuck |
| `just deploy` | motherduck | prod | Promotes dev → prod on MotherDuck |
| `just plan` | local_gateway | prod | Preview only, no apply |

---

## pins Integration

**Ingest (`src/my_data_platform/ingest.py`)**
Downloads CSVs from GitHub for the configured year range and writes them to `data/01_raw/`. Uses `pins.pin_download()` so each file is versioned and cached locally. The board type (local filesystem, Azure, S3) is set in `conf/parameters.toml`.

**Publish (`src/my_data_platform/publish.py`)**
Reads gold tables via `validations/connection.py` and writes them to the pins board with `pins.pin_write()`. Consumers read with `pins.pin_read()` — no DuckDB or DuckLake setup required on their end.

---

## Validation

`validations/` carries the pattern from the tutorial, wired up to the pipeline:

- `connection.py` — `get_connection()` reads `catalog_path` from `conf/parameters.toml` (not directory walking)
- `checks.py` — pointblank `Validate` chains for each layer (raw, bronze, silver, gold)
- `just validate` runs all checks and exits non-zero on failure

Validation checks per layer:
- **raw**: not-null on key IDs (`tourney_id`, `winner_id`, `loser_id`), `tourney_date` parseable, `score` not null
- **bronze**: no duplicates on unique key, `tour` in `{ATP, WTA}`, numeric stats non-negative
- **silver**: referential integrity (match `winner_id`/`loser_id` exist in `silver.players`), `surface` in `{Hard, Clay, Grass, Carpet}`
- **gold**: `win_rate` between 0 and 1, `matches_played` > 0, no duplicate `(player_id, surface)` in `player_surface_stats`

---

## Project Structure

```
my_data_platform/
  models/
    _util.py              shared _build_table(), GATEWAY_CATALOG
    raw/
    bronze/
    silver/
    gold/
    storage/              DuckLake-managed Parquet files (do not edit manually)
  validations/
    __init__.py
    connection.py
    checks.py
  src/my_data_platform/
    __init__.py
    ingest.py             pins download → data/01_raw/
    publish.py            gold → pins board
  conf/
    __init__.py
    config.py             dynaconf singleton
    parameters.toml       catalog_path, data_path, pins config, year_range
    .secrets.toml         MOTHERDUCK_TOKEN (gitignored)
  data/
    01_raw/
    02_bronze/
    03_silver/
    04_gold/
    catalog.ducklake
    sqlmesh_state.db
  docs/
    data_product.qmd      Quarto report (reads from gold via pins)
  config.yaml             SQLmesh gateway config
  pyproject.toml
  justfile
  CLAUDE.md
```

---

## justfile Commands

```
just run        # sqlmesh --gateway local_gateway plan --auto-apply
just stage      # sqlmesh --gateway motherduck plan dev --auto-apply
just deploy     # sqlmesh --gateway motherduck plan --auto-apply
just plan       # sqlmesh --gateway local_gateway plan (preview only)
just ingest     # uv run python -m my_data_platform.ingest
just validate   # uv run python -m my_data_platform.validate (all layers)
just publish    # uv run python -m my_data_platform.publish
just lint       # ruff check + sqlmesh lint
just test       # pytest + sqlmesh test
just docs       # quartodoc build + quarto preview
```

---

## Configuration (dynaconf)

```toml
# conf/parameters.toml
[ducklake]
catalog_path = "data/catalog.ducklake"
data_path = "data"

[sqlmesh]
state_db = "data/sqlmesh_state.db"

[pins]
board_type = "local"        # "azure" | "s3" for cloud
board_path = "data/pins_board"

[tennis]
year_start = 2015
year_end = 2025
tours = ["atp", "wta"]
```

---

## Testing

- **SQLmesh model tests** (`tests/`) — `sqlmesh test` runs YAML-defined unit tests per model using fixture data
- **pytest** — tests for `ingest.py`, `publish.py`, and `validations/checks.py` using an in-memory DuckDB connection
- **Ibis in-memory fixture** — `conftest.py` provides `ibis_connection` fixture: `ibis.duckdb.connect()` with no DuckLake, loads small fixture DataFrames for fast unit tests

---

## Decisions and Constraints

- **`is_sql=True` is mandatory** for all Ibis models — returning DataFrames breaks column-level lineage
- **`default_gateway: local_gateway`** — never MotherDuck by default; cloud is always explicit
- **MotherDuck state in MotherDuck** — avoids local state file drift across machines
- **Year range default 10 years** — full history (1968→) is available but makes the template slow to initialise
- **DVC removed** — DuckLake replaces DVC for data versioning; pins replaces DVC remote for artifact sharing
- **`polars` excluded** — not needed; Ibis+DuckDB handles all transformations
