# Docstrings & Docs Design

**Date:** 2026-05-17
**Branch:** `chore/docstrings`

## Goal

Clean up docstrings across the Python package and make `just docs` produce a working Quarto site with a live data product summary, pointblank integrity validation tables, and an auto-generated ER diagram.

---

## Part 1: SQLmesh grain/references

Add `grain` and `references` to every Python model's `@model()` decorator. These fields drive both the ER diagram and the pointblank checks in the notebook.

| Model | Grain (PK) | References (FK → silver.players.player_id) |
|---|---|---|
| `silver.players` | `player_id, tour` | — |
| `silver.matches` | `tourney_id, match_num, tour` | `winner_id AS player_id`, `loser_id AS player_id` |
| `silver.rankings` | `ranking_date, player_id, tour` | `player_id` |
| `gold.player_surface_stats` | `player_id, tour, surface` | `player_id` |
| `gold.head_to_head` | `player1_id, player2_id, tour` | `player1_id AS player_id`, `player2_id AS player_id` |
| `gold.rankings_history` | `ranking_date, player_id, tour` | `player_id` |
| `gold.tournament_stats` | `tourney_id, tourney_name, surface, tour, tourney_year` | — |

Bronze models are not annotated — they are upstream seed/union tables whose integrity is already checked by existing `validate_bronze_*` functions.

---

## Part 2: data_product.qmd — three sections

The notebook uses the `my_data_platform` Jupyter kernel and `freeze: auto` (already set), so it re-executes only when source changes.

### 2a: Database summary

A Python cell queries `INFORMATION_SCHEMA` (or ibis catalog introspection) per schema — bronze, silver, gold — and renders a table of table name + row count.

### 2b: Integrity checks

A Python cell:
1. Loads `sqlmesh.Context` pointing at the project root
2. Iterates over all models that have `grain` defined
3. For each model, fetches the live table via ibis, builds a `pb.Validate` chain:
   - `col_vals_not_null` for each grain column
   - `rows_distinct` on all grain columns combined
   - `col_vals_not_null` for each reference column
4. Calls `.interrogate()` and displays the result inline

One interrogation table per model, rendered sequentially in the notebook. No changes to `validations/checks.py` — this is documentation-only, not CI.

### 2c: ER diagram

A Python cell:
1. Loads `sqlmesh.Context`
2. For each model, reads `grain` (PK columns) and `references` (FK columns + aliased target)
3. Emits a Mermaid `erDiagram` block: entities with PK columns marked `PK`, FK relationships as `||--o{` lines
4. Renders via Quarto's native Mermaid support (a `{mermaid}` code cell, written to a temp file or inline via `IPython.display`)

---

## Part 3: Docstring cleanup

Convention: Google-style (enforced by ruff `pydocstyle`). All public functions get Args, Returns, and an Examples section. Private functions get Args and Returns only. Model `entrypoint` functions keep one-line docstrings (not public API).

Files to update:
- `src/my_data_platform/validate.py` — `main()` is already adequate; add to any new public helpers if added
- `src/my_data_platform/publish.py` — `publish_gold_tables()` needs Args/Returns
- `src/my_data_platform/ingest.py` — `_etag_paths()` and `_concat_to_csv()` need Args/Returns

---

## Part 4: quartodoc wiring

- `_quarto.yml`: replace `hello` in quartodoc sections with `ingest`, `validate`, `publish`
- Delete `src/my_data_platform/hello.py` (template placeholder, not used)
- Verify `just docs` runs cleanly end-to-end: `quartodoc build` → `quarto render` → `quarto preview`

---

## Out of scope

- Bronze model grain/references annotations
- Changes to `validations/checks.py` or `just validate` behaviour
- New gold models or schema changes
