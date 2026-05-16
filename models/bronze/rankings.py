"""Bronze rankings: combine ATP + WTA weekly rankings with tour label."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import GATEWAY_CATALOG, RANKINGS_SCHEMA, _build_table


@model('bronze.rankings',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, 'unique_key': ['ranking_date', 'player_id', 'tour']},
       description='Combined ATP and WTA weekly rankings with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union atp_rankings and wta_rankings, tagging each row with its tour."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    atp = _build_table(evaluator, catalog, 'atp_rankings', 'raw', RANKINGS_SCHEMA)
    wta = _build_table(evaluator, catalog, 'wta_rankings', 'raw', RANKINGS_SCHEMA)

    return ibis.union(atp.mutate(tour=ibis.literal('ATP')),
                      wta.mutate(tour=ibis.literal('WTA')),
                      distinct=False) \
        .to_sql(dialect='duckdb')
