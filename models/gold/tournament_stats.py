"""Gold: aggregate stats per tournament edition."""
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

from models._util import GATEWAY_CATALOG, SILVER_MATCHES_SCHEMA, _build_table


@model('gold.tournament_stats',
       is_sql=True,
       kind='FULL',
       description='Match count, avg duration, and distinct winner count per tournament.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Aggregate matches by tourney_id, name, surface, and year."""
    from ibis import _
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    matches = _build_table(evaluator, catalog, 'matches', 'silver', SILVER_MATCHES_SCHEMA)

    matches_with_year = matches.mutate(tourney_year=_['tourney_date'].year())
    result = matches_with_year \
        .group_by(['tourney_id', 'tourney_name', 'surface', 'tour', 'tourney_year']) \
        .aggregate(matches_played=_['match_num'].count(),
                   avg_match_minutes=_['minutes'].mean(),
                   distinct_winners=_['winner_id'].nunique())

    return result.to_sql(dialect='duckdb')
