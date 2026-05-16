"""Gold: win rate per player per surface."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, SILVER_MATCHES_SCHEMA, _build_table


@model('gold.player_surface_stats',
       is_sql=True,
       kind='FULL',
       description='Win rate, wins, losses, and matches played per player per surface.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Calculate win/loss record and win_rate per player, tour, and surface."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    matches = _build_table(evaluator, catalog, 'matches', 'silver', SILVER_MATCHES_SCHEMA)

    wins = matches \
        .group_by(['winner_id', 'tour', 'surface']) \
        .aggregate(wins=matches['match_num'].count()) \
        .rename(player_id='winner_id')
    losses = matches \
        .group_by(['loser_id', 'tour', 'surface']) \
        .aggregate(losses=matches['match_num'].count()) \
        .rename(player_id='loser_id')
    joined = wins \
        .outer_join(losses, ['player_id', 'tour', 'surface'])
    resolved = joined \
        .mutate(player_id=ibis.coalesce(wins['player_id'], losses['player_id']),
                tour=ibis.coalesce(wins['tour'], losses['tour']),
                surface=ibis.coalesce(wins['surface'], losses['surface']),
                wins=joined['wins'].fill_null(0).cast('int64'),
                losses=joined['losses'].fill_null(0).cast('int64'))
    result = resolved \
        .mutate(matches_played=resolved['wins'] + resolved['losses'],
                win_rate=(resolved['wins'].cast('float64') / (resolved['wins'] + resolved['losses']))) \
        .select(['player_id', 'tour', 'surface', 'wins', 'losses', 'matches_played', 'win_rate'])

    return result.to_sql(dialect='duckdb')
