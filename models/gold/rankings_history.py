"""Gold: career ranking history per player."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import GATEWAY_CATALOG, SILVER_RANKINGS_SCHEMA, _build_table


@model('gold.rankings_history',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_TIME_RANGE, 'time_column': 'ranking_date'},
       description='Weekly ranking snapshots with best-ever rank per player.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Pass through silver rankings and add career_best_rank window column."""
    from ibis import _
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    rankings = _build_table(evaluator, catalog, 'rankings', 'silver', SILVER_RANKINGS_SCHEMA)

    career_window = ibis.window(group_by=['player_id', 'tour'], order_by='ranking_date')
    result = rankings.mutate(career_best_rank=_['ranking'].min().over(career_window))

    return result.to_sql(dialect='duckdb')
