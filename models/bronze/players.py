"""Bronze players: combine ATP + WTA player rosters with tour label."""
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import PLAYERS_SCHEMA, _read_csv_sql

_BRONZE_PLAYERS_SCHEMA = {**PLAYERS_SCHEMA, 'tour': 'string'}


@model('bronze.players',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, 'unique_key': ['player_id', 'tour']},
       description='Combined ATP and WTA player rosters with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union ATP and WTA player CSVs, tagging each row with its tour."""
    if evaluator.runtime_stage == 'loading':
        cols = ', '.join(f'NULL AS "{k}"' for k in _BRONZE_PLAYERS_SCHEMA)
        return f'SELECT {cols} WHERE FALSE'

    tour_col = '"tour"'
    player_cols = ', '.join(f'"{k}"' for k in PLAYERS_SCHEMA)
    atp_subq = _read_csv_sql('data/01_raw/atp_players.csv', PLAYERS_SCHEMA)
    wta_subq = _read_csv_sql('data/01_raw/wta_players.csv', PLAYERS_SCHEMA)
    atp_sql = f"SELECT {player_cols}, 'ATP' AS {tour_col} FROM {atp_subq}"
    wta_sql = f"SELECT {player_cols}, 'WTA' AS {tour_col} FROM {wta_subq}"
    return f'{atp_sql} UNION ALL {wta_sql}'
