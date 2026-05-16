"""Shared utilities for all SQLmesh + Ibis models."""
import ibis
from sqlmesh.core.macros import MacroEvaluator

_IBIS_TO_DUCKDB: dict[str, str] = {
    'string': 'VARCHAR',
    'int32': 'INTEGER',
    'int64': 'BIGINT',
    'float64': 'DOUBLE',
    'date': 'DATE',
}


def _read_csv_sql(csv_path: str, schema: dict[str, str]) -> str:
    """SQL subquery that reads a CSV and casts each column to the declared type.

    Uses ALL_VARCHAR=TRUE then TRY_CAST so mixed-type columns (e.g. winner_seed
    which can contain 'Q') become NULL rather than raising an error.
    """
    casts = ', '.join(
        f'TRY_CAST("{k}" AS {_IBIS_TO_DUCKDB.get(v, "VARCHAR")}) AS "{k}"'
        for k, v in schema.items()
    )
    return f"(SELECT {casts} FROM read_csv('{csv_path}', ALL_VARCHAR=TRUE))"

GATEWAY_CATALOG: dict[str, str] = {'local_gateway': 'my_lakehouse',
                                   'motherduck': 'my_lakehouse'}

MATCHES_SCHEMA: dict[str, str] = {'tourney_id': 'string',
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
                                   'loser_rank_points': 'float64'}

BRONZE_MATCHES_SCHEMA: dict[str, str] = {**MATCHES_SCHEMA, 'tour': 'string'}

SILVER_MATCHES_SCHEMA: dict[str, str] = {**{k: v for k, v in MATCHES_SCHEMA.items()
                                             if k != 'tourney_date'},
                                          'tourney_date': 'date',
                                          'tour': 'string'}

PLAYERS_SCHEMA: dict[str, str] = {'player_id': 'int32',
                                   'first_name': 'string',
                                   'last_name': 'string',
                                   'hand': 'string',
                                   'dob': 'float64',
                                   'ioc': 'string',
                                   'height': 'float64',
                                   'wikidata_id': 'string'}

SILVER_PLAYERS_SCHEMA: dict[str, str] = {**PLAYERS_SCHEMA,
                                          'full_name': 'string',
                                          'tour': 'string'}

RANKINGS_SCHEMA: dict[str, str] = {'ranking_date': 'int32',
                                    'ranking': 'int32',
                                    'player_id': 'int32',
                                    'points': 'float64'}

SILVER_RANKINGS_SCHEMA: dict[str, str] = {'ranking_date': 'date',
                                           'ranking': 'int32',
                                           'player_id': 'int32',
                                           'points': 'float64',
                                           'tour': 'string'}


def _build_table(evaluator: MacroEvaluator,
                 catalog: str,
                 table: str,
                 database: str,
                 fallback_schema: dict[str, str]) -> ibis.Table:
    """Return an Ibis unbound table, reusing SQLMesh's connection at runtime.

    During the loading stage the engine adapter is unavailable, so fallback_schema
    is used. At runtime the live schema is read from the already-attached catalog
    via SQLMesh's own connection, avoiding a second DuckLake file lock.
    """
    if evaluator.runtime_stage == 'loading':
        schema = ibis.Schema(fallback_schema)
    else:
        try:
            con = ibis.duckdb.from_connection(evaluator.engine_adapter.connection)
            schema = con.table(table, database=f'{catalog}.{database}').schema()
        except Exception:  # noqa: BLE001
            schema = ibis.Schema(fallback_schema)
    return ibis.table(schema=schema, name=table, catalog=catalog, database=database)
