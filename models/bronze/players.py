"""Bronze players: combine ATP + WTA player rosters with tour label."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import GATEWAY_CATALOG, PLAYERS_SCHEMA, _build_table


@model('bronze.players',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, 'unique_key': ['player_id', 'tour']},
       description='Combined ATP and WTA player rosters with tour label.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Union atp_players and wta_players, tagging each row with its tour."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    atp = _build_table(evaluator, catalog, 'atp_players', 'raw', PLAYERS_SCHEMA)
    wta = _build_table(evaluator, catalog, 'wta_players', 'raw', PLAYERS_SCHEMA)

    return ibis.union(atp.mutate(tour=ibis.literal('ATP')),
                      wta.mutate(tour=ibis.literal('WTA')),
                      distinct=False) \
        .to_sql(dialect='duckdb')
