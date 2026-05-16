"""Tests for ATP/WTA data ingestion."""
from unittest.mock import MagicMock, patch

import polars as pl
from my_data_platform.ingest import download_tour_matches, download_tour_players, download_tour_rankings


def _csv_bytes(rows: list[dict]) -> bytes:
    """Build CSV bytes from a list of dicts using polars."""
    return pl.DataFrame(rows).write_csv().encode()


MATCH_ROW = {'tourney_id': '2024-540', 'tourney_name': 'Wimbledon', 'surface': 'Grass',
             'tourney_date': 20240701, 'match_num': 1, 'winner_id': 104925, 'loser_id': 105453,
             'score': '6-3 6-4'}
PLAYER_ROW = {'player_id': 104925, 'first_name': 'Novak', 'last_name': 'Djokovic'}
RANKING_ROW = {'ranking_date': 20240701, 'ranking': 1, 'player_id': 104925, 'points': 9000}


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_matches_concatenates_years(mock_get, tmp_path):
    """Single-year download produces a CSV with the expected row."""
    mock_get.return_value = MagicMock(status_code=200, content=_csv_bytes([MATCH_ROW]))
    out = tmp_path / 'atp_matches.csv'

    download_tour_matches('atp', 'https://example.com', 2024, 2024, out)

    df = pl.read_csv(out)
    assert len(df) == 1
    assert df['tourney_id'][0] == '2024-540'
    mock_get.assert_called_once_with('https://example.com/atp_matches_2024.csv', timeout=30)


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_matches_skips_404(mock_get, tmp_path):
    """404 years are skipped; only 200 rows are written."""
    mock_get.side_effect = [MagicMock(status_code=404),
                            MagicMock(status_code=200, content=_csv_bytes([MATCH_ROW]))]
    out = tmp_path / 'atp_matches.csv'

    download_tour_matches('atp', 'https://example.com', 2023, 2024, out)

    assert len(pl.read_csv(out)) == 1


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_players(mock_get, tmp_path):
    """Players CSV is written directly from response bytes."""
    mock_get.return_value = MagicMock(status_code=200, content=_csv_bytes([PLAYER_ROW]))
    out = tmp_path / 'atp_players.csv'

    download_tour_players('atp', 'https://example.com', out)

    assert pl.read_csv(out)['first_name'][0] == 'Novak'


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_rankings(mock_get, tmp_path):
    """Rankings CSV is written with correct data."""
    mock_get.return_value = MagicMock(status_code=200, content=_csv_bytes([RANKING_ROW]))
    out = tmp_path / 'atp_rankings.csv'

    download_tour_rankings('atp', 'https://example.com', 2024, 2024, out)

    assert pl.read_csv(out)['ranking'][0] == 1
