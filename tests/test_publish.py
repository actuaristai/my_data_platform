"""Tests for gold layer publishing via pins."""
from unittest.mock import MagicMock, patch

from my_data_platform.publish import publish_gold_tables


@patch('my_data_platform.publish.get_connection')
@patch('my_data_platform.publish.pins.board_folder')
def test_publish_gold_tables_writes_all_four_tables(mock_board_folder, mock_get_conn):
    mock_con = MagicMock()
    mock_get_conn.return_value = mock_con
    mock_board = MagicMock()
    mock_board_folder.return_value = mock_board

    publish_gold_tables(board_path='data/pins_board')

    assert mock_board.pin_write.call_count == 4
    pin_names = [call.args[1] for call in mock_board.pin_write.call_args_list]
    assert 'gold/player_surface_stats' in pin_names
    assert 'gold/head_to_head' in pin_names
    assert 'gold/rankings_history' in pin_names
    assert 'gold/tournament_stats' in pin_names
