"""Silver players: derive full_name, filter null player IDs."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import GATEWAY_CATALOG, PLAYERS_SCHEMA, _build_table

_BRONZE_PLAYERS_SCHEMA: dict[str, str] = {**PLAYERS_SCHEMA, 'tour': 'string'}


@model('silver.players',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, 'unique_key': ['player_id', 'tour']},
       grain=['player_id', 'tour'],
       description='Player roster with full_name derived, null player_id rows removed.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Derive full_name, drop rows with null player_id."""
    from ibis import _  # noqa: PLC0415
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    bronze = _build_table(evaluator, catalog, 'players', 'bronze', _BRONZE_PLAYERS_SCHEMA)

    result = bronze \
        .filter(_['player_id'].notnull()) \
        .mutate(full_name=(_['first_name'] + ibis.literal(' ') + _['last_name']).strip())

    return result.to_sql(dialect='duckdb')
