"""Gold: head-to-head win records between player pairs."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, SILVER_MATCHES_SCHEMA, _build_table


@model('gold.head_to_head',
       is_sql=True,
       kind='FULL',
       description='H2H records: wins for player1 vs player2 (player1_id < player2_id).')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Count wins for each canonical (player1, player2) pair where player1_id < player2_id."""
    from ibis import _  # noqa: PLC0415
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    matches = _build_table(evaluator, catalog, 'matches', 'silver', SILVER_MATCHES_SCHEMA)

    canonical = matches.mutate(
        player1_id=ibis.least(_['winner_id'], _['loser_id']),
        player2_id=ibis.greatest(_['winner_id'], _['loser_id']),
        player1_won=(_['winner_id'] < _['loser_id']).cast('int64'),
    )
    agg = canonical \
        .group_by(['player1_id', 'player2_id', 'tour']) \
        .aggregate(player1_wins=_['player1_won'].sum(),
                   total_matches=_['match_num'].count())
    result = agg.mutate(player2_wins=_['total_matches'] - _['player1_wins'])

    return result.to_sql(dialect='duckdb')
