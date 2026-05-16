"""Tests for shared model utilities."""
from unittest.mock import MagicMock

from models._util import GATEWAY_CATALOG, MATCHES_SCHEMA, PLAYERS_SCHEMA, RANKINGS_SCHEMA, _build_table


def test_gateway_catalog_contains_expected_keys():
    """Verify gateway catalog maps both gateways to my_lakehouse."""
    assert 'local_gateway' in GATEWAY_CATALOG
    assert 'motherduck' in GATEWAY_CATALOG
    assert GATEWAY_CATALOG['local_gateway'] == 'my_lakehouse'
    assert GATEWAY_CATALOG['motherduck'] == 'my_lakehouse'


def test_build_table_loading_stage_uses_fallback_schema():
    """During loading stage, _build_table should return table with fallback schema."""
    evaluator = MagicMock()
    evaluator.runtime_stage = 'loading'
    evaluator.gateway = 'local_gateway'

    table = _build_table(evaluator, 'my_lakehouse', 'atp_matches', 'raw', MATCHES_SCHEMA)

    assert 'atp_matches' in table.get_name()
    assert 'tourney_id' in table.schema()
    assert 'winner_id' in table.schema()


def test_build_table_exception_during_eval_uses_fallback():
    """When engine adapter raises, _build_table falls back to schema."""
    evaluator = MagicMock()
    evaluator.runtime_stage = 'evaluating'
    evaluator.engine_adapter.connection = None

    table = _build_table(evaluator, 'my_lakehouse', 'atp_players', 'raw', PLAYERS_SCHEMA)

    assert 'player_id' in table.schema()
    assert 'first_name' in table.schema()


def test_schemas_have_expected_columns():
    """Verify key columns exist in each schema."""
    assert 'tourney_date' in MATCHES_SCHEMA
    assert 'player_id' in PLAYERS_SCHEMA
    assert 'ranking_date' in RANKINGS_SCHEMA
