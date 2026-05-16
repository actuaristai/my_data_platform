# my_data_platform

An opinionated open-source data platform using DuckLake + SQLmesh + Ibis, with ATP/WTA tennis data as the working example. Local-first with a clear promotion path to MotherDuck.

---

## Stack

| Concern | Tool |
|---------|------|
| Lakehouse catalog | DuckLake (ACID, time travel, Parquet-native, local → MotherDuck) |
| Pipeline orchestration | SQLmesh (incremental execution, column-level lineage, skip-if-unchanged) |
| Transformations | Ibis with `is_sql=True` (portable Python API, compiles to SQL for lineage) |
| Data ingestion | `requests` + `polars` (downloads CSVs from GitHub) |
| Gold publishing | pins (versioned Parquet artefacts for downstream consumers) |
| Data validation | pointblank (declarative quality checks per layer) |
| Config | dynaconf (parameters + secrets separation) |
| Package manager | uv |
| Task runner | just |

---

## Architecture

```
GitHub (JeffSackmann/tennis_atp + tennis_wta)
    ↓  just ingest  →  ingest.py (requests + polars)
data/01_raw/   raw CSVs
    ↓  SQLmesh SEED models  (models/bronze/)
bronze.atp_matches, bronze.wta_matches
bronze.atp_players, bronze.wta_players
bronze.atp_rankings, bronze.wta_rankings
    ↓  SQL UNION ALL models  (models/bronze/)
bronze.matches    ← ATP + WTA combined, tour column added
bronze.players    ← ATP + WTA combined
bronze.rankings   ← ATP + WTA combined
    ↓  Silver Ibis models  (INCREMENTAL_BY_TIME_RANGE / UNIQUE_KEY)
silver.matches    ← date cast, surface normalised, null scores removed, seeds try-cast
silver.players    ← full_name derived, null player_id removed
silver.rankings   ← ranking_date cast to DATE
    ↓  Gold Ibis models  (FULL / INCREMENTAL_BY_TIME_RANGE)
gold.player_surface_stats    ← win rate by player + surface
gold.head_to_head            ← H2H records between player pairs
gold.rankings_history        ← career ranking trajectory with best-ever rank
gold.tournament_stats        ← match count, avg duration, distinct winners per tournament
    ↓  just publish  →  publish.py (pins.pin_write)
data/pins_board/   Parquet pins consumed by Quarto reports, dashboards, other projects
```

---

## Quickstart

```sh
just init-project   # uv sync + pre-commit install + create data dirs
just ingest         # download raw ATP/WTA CSVs to data/01_raw/
just run            # build full pipeline locally (first run ~30 min for SEED load)
just validate       # pointblank checks across all layers
just publish        # write gold tables to data/pins_board/
```

---

## Commands

```sh
just ingest     # download raw ATP/WTA CSVs into data/01_raw/
just run        # local SQLmesh plan --auto-apply (DuckLake on disk)
just stage      # SQLmesh plan dev on MotherDuck (isolated *__dev schemas)
just deploy     # SQLmesh plan prod on MotherDuck (promote to production)
just validate   # pointblank checks across all layers
just publish    # push gold tables to pins board
just lint       # ruff check + sqlmesh lint
just test       # pytest + sqlmesh test
just docs       # quartodoc build + quarto preview
```

**Run a single pytest test:**
```sh
uv run --only-group test pytest tests/path/to/test_file.py -v
```

**Run a single SQLmesh model test:**
```sh
uv run sqlmesh test tests/test_bronze_matches.yaml
```

---

## Model Structure

```
models/
  _util.py              shared _build_table(), GATEWAY_CATALOG, all schemas
  bronze/
    atp_matches.sql     SEED → data/01_raw/atp_matches.csv (winner_seed/loser_seed as VARCHAR)
    wta_matches.sql     SEED → data/01_raw/wta_matches.csv
    atp_players.sql     SEED → data/01_raw/atp_players.csv
    wta_players.sql     SEED → data/01_raw/wta_players.csv
    atp_rankings.sql    SEED → data/01_raw/atp_rankings.csv
    wta_rankings.sql    SEED → data/01_raw/wta_rankings.csv
    matches.sql         SQL UNION ALL: atp_matches ∪ wta_matches + tour label (FULL)
    players.sql         SQL UNION ALL: atp_players ∪ wta_players + tour label (FULL)
    rankings.sql        SQL UNION ALL: atp_rankings ∪ wta_rankings + tour label (FULL)
  silver/
    matches.py          Ibis, INCREMENTAL_BY_TIME_RANGE (tourney_date)
    players.py          Ibis, INCREMENTAL_BY_UNIQUE_KEY (player_id, tour)
    rankings.py         Ibis, INCREMENTAL_BY_TIME_RANGE (ranking_date)
  gold/
    player_surface_stats.py   Ibis, FULL
    head_to_head.py           Ibis, FULL
    rankings_history.py       Ibis, INCREMENTAL_BY_TIME_RANGE (ranking_date)
    tournament_stats.py       Ibis, FULL
```

### Ibis model pattern

All Python models use `is_sql=True` so SQLmesh can parse returned SQL for column-level lineage. `from ibis import _` is a **local import inside `entrypoint`** — a module-level import conflicts with SQLmesh's macro namespace. Schemas in `_util.py` are `dict[str, str]`, not `ibis.Schema`, so SQLmesh can serialize them.

```python
@model('silver.matches', is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_TIME_RANGE, 'time_column': 'tourney_date'})
def entrypoint(evaluator: MacroEvaluator) -> str:
    from ibis import _  # local import — avoids SQLmesh macro namespace conflict
    catalog = GATEWAY_CATALOG.get(evaluator.gateway or 'local_gateway', 'my_lakehouse')
    bronze = _build_table(evaluator, catalog, 'matches', 'bronze', BRONZE_MATCHES_SCHEMA)
    result = bronze \
        .filter(_['score'].notnull()) \
        .mutate(tourney_date=_['tourney_date'].cast('string').as_timestamp('%Y%m%d').date(),
                winner_seed=_['winner_seed'].try_cast('float64'))
    return result.to_sql(dialect='duckdb')
```

---

## Gateways

```yaml
# config.yaml
gateways:
  local_gateway:       # default — DuckLake on disk
    connection:
      type: duckdb
      catalogs:
        my_lakehouse:
          type: ducklake
          path: data/catalog.ducklake
          data_path: data/storage
  motherduck:          # cloud — requires MOTHERDUCK_TOKEN in .secrets.toml
    connection:
      type: motherduck
      catalogs:
        my_lakehouse: "md:my_lakehouse"
default_gateway: local_gateway
```

| Command | Gateway | Environment | Effect |
|---------|---------|-------------|--------|
| `just run` | local_gateway | prod | DuckLake on disk, full incremental run |
| `just stage` | motherduck | dev | Isolated `*__dev` schemas on MotherDuck |
| `just deploy` | motherduck | prod | Promotes dev → prod on MotherDuck |

---

## Data Source

Jeff Sackmann's public tennis datasets:
- ATP: `github.com/JeffSackmann/tennis_atp`
- WTA: `github.com/JeffSackmann/tennis_wta`

Files: `{tour}_matches_{year}.csv`, `{tour}_players.csv`, `{tour}_rankings_{decade}s.csv`.
Default range: 2015–2025 (configurable in `conf/parameters.toml`). Full history back to 1968 is available.

> **Note:** `winner_seed` and `loser_seed` are stored as `VARCHAR` in bronze SEEDs — the source data contains qualifier values like `'Q'` and `'WC'`. Silver casts them to `float64` via `try_cast`, which coerces non-numeric values to `NULL`.

---

## Validation

`validations/checks.py` runs pointblank chains per layer via `just validate`:

- **bronze seeds**: `tourney_id`, `winner_id`, `loser_id`, `tourney_date` not null
- **bronze.matches**: no duplicate `(tourney_id, match_num, tour)`, `tour` in `{ATP, WTA}`
- **silver.matches**: `score` not null, non-null `surface` values in `{Hard, Clay, Grass, Carpet}`, `tourney_date` not null
- **gold.player_surface_stats**: `win_rate` in `[0, 1]`, `matches_played > 0`, no duplicate `(player_id, surface, tour)`

---

## Testing

```sh
just test   # runs both pytest and sqlmesh test
```

- **pytest** (`tests/test_*.py`) — unit tests for `ingest.py`, `publish.py`, `_util.py`, `connection.py` using mocks and an in-memory DuckDB fixture
- **SQLmesh model tests** (`tests/*.yaml`) — fixture-based unit tests for `bronze.matches`, `silver.matches`, `gold.player_surface_stats`

---

## Configuration

```toml
# conf/parameters.toml
[ducklake]
catalog_path = "data/catalog.ducklake"
data_path = "data/storage"

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

Secrets (e.g. `MOTHERDUCK_TOKEN`) go in `.secrets.toml` — gitignored.

---

## DuckDB UI

```sh
uvx --from duckdb-cli duckdb.exe -ui
ATTACH 'ducklake:data/catalog.ducklake' AS my_lakehouse;
```

---

## Design Decisions

- **`is_sql=True` is mandatory** — returning DataFrames from Ibis models breaks SQLmesh's column-level lineage
- **Bronze = SQL, not Python** — SEED models and UNION ALL are simpler and more reliable than Python ibis for the raw→bronze step; no schema inference needed
- **No raw layer** — SEEDs live in `bronze` directly; a separate raw schema added complexity with no benefit
- **`default_gateway: local_gateway`** — never MotherDuck by accident; cloud is always explicit
- **DVC removed** — DuckLake replaces DVC for data versioning; pins replaces DVC remote for artefact sharing
