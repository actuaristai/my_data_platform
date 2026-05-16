"""Silver matches: cast date, normalise surface capitalisation, filter walkovers."""
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import BRONZE_MATCHES_SCHEMA, GATEWAY_CATALOG, _build_table


@model('silver.matches',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_TIME_RANGE, 'time_column': 'tourney_date'},
       description='Cleaned matches: date cast, surface normalised, null scores removed.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Cast tourney_date to DATE, normalise surface, try-cast seed columns, drop null scores."""
    from ibis import _  # noqa: PLC0415
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    bronze = _build_table(evaluator, catalog, 'matches', 'bronze', BRONZE_MATCHES_SCHEMA)

    result = bronze \
        .filter(_['score'].notnull()) \
        .mutate(tourney_date=_['tourney_date'].cast('string').as_timestamp('%Y%m%d').date(),
                surface=_['surface'].lower().cases(('hard', 'Hard'),
                                                   ('clay', 'Clay'),
                                                   ('grass', 'Grass'),
                                                   ('carpet', 'Carpet'),
                                                   else_=_['surface']),
                winner_seed=_['winner_seed'].try_cast('float64'),
                loser_seed=_['loser_seed'].try_cast('float64'))

    return result.to_sql(dialect='duckdb')
