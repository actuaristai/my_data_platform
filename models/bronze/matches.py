"""Bronze matches: combine ATP + WTA into a single table with tour label."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import GATEWAY_CATALOG, MATCHES_SCHEMA, _build_table


@model('bronze.matches',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, 'unique_key': ['tourney_id', 'match_num', 'tour']},
       description='Combined ATP and WTA matches with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union atp_matches and wta_matches, tagging each row with its tour."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    atp = _build_table(evaluator, catalog, 'atp_matches', 'raw', MATCHES_SCHEMA)
    wta = _build_table(evaluator, catalog, 'wta_matches', 'raw', MATCHES_SCHEMA)

    return ibis.union(atp.mutate(tour=ibis.literal('ATP')),
                      wta.mutate(tour=ibis.literal('WTA')),
                      distinct=False) \
        .to_sql(dialect='duckdb')
