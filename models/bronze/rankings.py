"""Bronze rankings: combine ATP + WTA weekly rankings with tour label."""
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import RANKINGS_SCHEMA, _read_csv_sql

_BRONZE_RANKINGS_SCHEMA = {**RANKINGS_SCHEMA, 'tour': 'string'}


@model('bronze.rankings',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, 'unique_key': ['ranking_date', 'player_id', 'tour']},
       description='Combined ATP and WTA weekly rankings with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union ATP and WTA rankings CSVs, tagging each row with its tour."""
    if evaluator.runtime_stage == 'loading':
        cols = ', '.join(f'NULL AS "{k}"' for k in _BRONZE_RANKINGS_SCHEMA)
        return f'SELECT {cols} WHERE FALSE'

    ranking_cols = ', '.join(f'"{k}"' for k in RANKINGS_SCHEMA)
    atp_sql = (f"SELECT {ranking_cols}, 'ATP' AS \"tour\""
               f" FROM {_read_csv_sql('data/01_raw/atp_rankings.csv', RANKINGS_SCHEMA)}")
    wta_sql = (f"SELECT {ranking_cols}, 'WTA' AS \"tour\""
               f" FROM {_read_csv_sql('data/01_raw/wta_rankings.csv', RANKINGS_SCHEMA)}")
    return f'{atp_sql} UNION ALL {wta_sql}'
