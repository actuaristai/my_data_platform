"""Silver rankings: cast ranking_date to DATE."""
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import GATEWAY_CATALOG, RANKINGS_SCHEMA, _build_table

_BRONZE_RANKINGS_SCHEMA: dict[str, str] = {**RANKINGS_SCHEMA, 'tour': 'string'}


@model('silver.rankings',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_TIME_RANGE, 'time_column': 'ranking_date'},
       description='Weekly rankings with ranking_date cast to DATE.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Cast ranking_date from YYYYMMDD int to DATE."""
    from ibis import _
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    bronze = _build_table(evaluator, catalog, 'rankings', 'bronze', _BRONZE_RANKINGS_SCHEMA)

    result = bronze.mutate(ranking_date=_['ranking_date'].cast('string').as_timestamp('%Y%m%d').date())

    return result.to_sql(dialect='duckdb')
