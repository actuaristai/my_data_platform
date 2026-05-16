"""Bronze matches: combine ATP + WTA into a single table with tour label."""
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import BRONZE_MATCHES_SCHEMA, MATCHES_SCHEMA, _read_csv_sql


@model('bronze.matches',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, 'unique_key': ['tourney_id', 'match_num', 'tour']},
       description='Combined ATP and WTA matches with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union ATP and WTA match CSVs, tagging each row with its tour."""
    if evaluator.runtime_stage == 'loading':
        cols = ', '.join(f'NULL AS "{k}"' for k in BRONZE_MATCHES_SCHEMA)
        return f'SELECT {cols} WHERE FALSE'

    match_cols = ', '.join(f'"{k}"' for k in MATCHES_SCHEMA)
    atp_sql = (f"SELECT {match_cols}, 'ATP' AS \"tour\""
               f" FROM {_read_csv_sql('data/01_raw/atp_matches.csv', MATCHES_SCHEMA)}")
    wta_sql = (f"SELECT {match_cols}, 'WTA' AS \"tour\""
               f" FROM {_read_csv_sql('data/01_raw/wta_matches.csv', MATCHES_SCHEMA)}")
    return f'{atp_sql} UNION ALL {wta_sql}'
