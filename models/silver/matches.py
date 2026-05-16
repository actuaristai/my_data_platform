"""Silver matches: cast date, normalise surface capitalisation, filter walkovers."""
import ibis
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

from models._util import BRONZE_MATCHES_SCHEMA, GATEWAY_CATALOG, _build_table


@model('silver.matches',
       is_sql=True,
       kind={'name': ModelKindName.INCREMENTAL_BY_TIME_RANGE, 'time_column': 'tourney_date'},
       description='Cleaned matches: date cast, surface normalised, null scores removed.')
def entrypoint(evaluator: MacroEvaluator) -> str:
    """Cast tourney_date to DATE, normalise surface, drop rows with no score."""
    gateway = evaluator.gateway or 'local_gateway'
    catalog = GATEWAY_CATALOG.get(gateway, 'my_lakehouse')

    bronze = _build_table(evaluator, catalog, 'matches', 'bronze', BRONZE_MATCHES_SCHEMA)

    result = bronze \
        .filter(bronze['score'].notnull()) \
        .mutate(tourney_date=bronze['tourney_date'].cast('string').as_timestamp('%Y%m%d').date(),
                surface=bronze['surface'].lower().cases(('hard', 'Hard'),
                                                      ('clay', 'Clay'),
                                                      ('grass', 'Grass'),
                                                      ('carpet', 'Carpet'),
                                                      else_=bronze['surface']))

    return result.to_sql(dialect='duckdb')
