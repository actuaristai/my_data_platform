"""Tests for validation connection factory."""
from unittest.mock import MagicMock, patch

from validations.connection import get_connection


@patch('validations.connection.ibis.duckdb.connect')
def test_get_connection_uses_catalog_path_from_conf(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    get_connection()

    mock_connect.assert_called_once_with(extensions=['ducklake'])
    mock_conn.attach.assert_called_once()
    call_args = mock_conn.attach.call_args[0][0]
    assert call_args.startswith('ducklake:')
    assert 'catalog.ducklake' in call_args
