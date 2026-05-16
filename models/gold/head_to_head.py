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
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    matches = _build_table(evaluator, catalog, 'matches', 'silver', SILVER_MATCHES_SCHEMA)

    canonical = matches \
        .mutate(player1_id=ibis.least(matches['winner_id'], matches['loser_id']),
                player2_id=ibis.greatest(matches['winner_id'], matches['loser_id']),
                player1_won=(matches['winner_id'] < matches['loser_id']).cast('int64'))
    agg = canonical \
        .group_by(['player1_id', 'player2_id', 'tour']) \
        .aggregate(player1_wins=canonical['player1_won'].sum(),
                   total_matches=canonical['match_num'].count())
    result = agg \
        .mutate(player2_wins=agg['total_matches'] - agg['player1_wins'])

    return result.to_sql(dialect='duckdb')
