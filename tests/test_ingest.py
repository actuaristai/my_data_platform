"""Tests for ATP/WTA data ingestion."""
from unittest.mock import MagicMock, call, patch

import polars as pl
import pytest
from my_data_platform.ingest import (
    _fetch_with_etag,
    download_tour_matches,
    download_tour_players,
    download_tour_rankings,
)


def _csv_bytes(rows: list[dict]) -> bytes:
    """Build CSV bytes from a list of dicts using polars."""
    return pl.DataFrame(rows).write_csv().encode()


def _mock_200(content: bytes, etag: str = '"test-etag"') -> MagicMock:
    return MagicMock(status_code=200, content=content, headers={'ETag': etag})


def _mock_304() -> MagicMock:
    return MagicMock(status_code=304)


def _mock_404() -> MagicMock:
    return MagicMock(status_code=404)


MATCH_ROW = {'tourney_id': '2024-540', 'tourney_name': 'Wimbledon', 'surface': 'Grass',
             'tourney_date': 20240701, 'match_num': 1, 'winner_id': 104925, 'loser_id': 105453,
             'score': '6-3 6-4'}
PLAYER_ROW = {'player_id': 104925, 'first_name': 'Novak', 'last_name': 'Djokovic'}
RANKING_ROW = {'ranking_date': 20240701, 'ranking': 1, 'player_id': 104925, 'points': 9000}


# --- _fetch_with_etag unit tests ---

@patch('my_data_platform.ingest.requests.get')
def test_fetch_with_etag_200_saves_cache_and_etag(mock_get, tmp_path):
    """200 response writes content to cache file and ETag to sidecar."""
    content = b'col1,col2\na,b'
    mock_get.return_value = _mock_200(content, etag='"abc123"')
    etag_dir = tmp_path / '.etags'

    result = _fetch_with_etag('https://example.com/data.csv', etag_dir)

    assert result == content
    assert (etag_dir / 'data.csv').read_bytes() == content
    assert (etag_dir / 'data.csv.etag').read_text() == '"abc123"'
    mock_get.assert_called_once_with('https://example.com/data.csv', headers={}, timeout=30)


@patch('my_data_platform.ingest.requests.get')
def test_fetch_with_etag_sends_if_none_match(mock_get, tmp_path):
    """Stored ETag is sent as If-None-Match header on subsequent requests."""
    etag_dir = tmp_path / '.etags'
    etag_dir.mkdir()
    (etag_dir / 'data.csv.etag').write_text('"stored-etag"')
    (etag_dir / 'data.csv').write_bytes(b'old content')
    mock_get.return_value = _mock_200(b'new content', etag='"new-etag"')

    _fetch_with_etag('https://example.com/data.csv', etag_dir)

    mock_get.assert_called_once_with(
        'https://example.com/data.csv',
        headers={'If-None-Match': '"stored-etag"'},
        timeout=30,
    )


@patch('my_data_platform.ingest.requests.get')
def test_fetch_with_etag_304_returns_cached_bytes(mock_get, tmp_path):
    """304 response returns cached content without downloading the body."""
    cached = b'cached content'
    etag_dir = tmp_path / '.etags'
    etag_dir.mkdir()
    (etag_dir / 'data.csv.etag').write_text('"stored-etag"')
    (etag_dir / 'data.csv').write_bytes(cached)
    mock_get.return_value = _mock_304()

    result = _fetch_with_etag('https://example.com/data.csv', etag_dir)

    assert result == cached
    mock_get.assert_called_once()  # no second request made


@patch('my_data_platform.ingest.requests.get')
def test_fetch_with_etag_304_no_cache_refetches_unconditionally(mock_get, tmp_path):
    """304 with missing cache file falls back to unconditional GET."""
    content = b'refetched content'
    etag_dir = tmp_path / '.etags'
    etag_dir.mkdir()
    (etag_dir / 'data.csv.etag').write_text('"stored-etag"')
    # No cache file - simulates cleared cache
    mock_get.side_effect = [_mock_304(), _mock_200(content, etag='"stored-etag"')]

    result = _fetch_with_etag('https://example.com/data.csv', etag_dir)

    assert result == content
    assert mock_get.call_count == 2
    # Second call must not send If-None-Match
    assert mock_get.call_args_list[1] == call('https://example.com/data.csv', timeout=30)


@patch('my_data_platform.ingest.requests.get')
def test_fetch_with_etag_404_returns_none(mock_get, tmp_path):
    """404 response returns None without raising."""
    mock_get.return_value = _mock_404()

    result = _fetch_with_etag('https://example.com/missing.csv', tmp_path / '.etags')

    assert result is None


# --- download_tour_matches ---

@patch('my_data_platform.ingest.requests.get')
def test_download_tour_matches_concatenates_years(mock_get, tmp_path):
    """Single-year download produces a CSV with the expected row."""
    mock_get.return_value = _mock_200(_csv_bytes([MATCH_ROW]))
    out = tmp_path / 'atp_matches.csv'

    download_tour_matches('atp', 'https://example.com', 2024, 2024, out,
                          etag_dir=tmp_path / '.etags')

    df = pl.read_csv(out)
    assert len(df) == 1
    assert df['tourney_id'][0] == '2024-540'


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_matches_skips_404(mock_get, tmp_path):
    """404 years are skipped; only 200 rows are written."""
    mock_get.side_effect = [_mock_404(), _mock_200(_csv_bytes([MATCH_ROW]))]
    out = tmp_path / 'atp_matches.csv'

    download_tour_matches('atp', 'https://example.com', 2023, 2024, out,
                          etag_dir=tmp_path / '.etags')

    assert len(pl.read_csv(out)) == 1


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_matches_304_rebuilds_from_cache(mock_get, tmp_path):
    """304 responses use cached per-URL content; combined output is still written."""
    etag_dir = tmp_path / '.etags'
    etag_dir.mkdir()
    (etag_dir / 'atp_matches_2024.csv').write_bytes(_csv_bytes([MATCH_ROW]))
    (etag_dir / 'atp_matches_2024.csv.etag').write_text('"cached-etag"')
    mock_get.return_value = _mock_304()
    out = tmp_path / 'atp_matches.csv'

    download_tour_matches('atp', 'https://example.com', 2024, 2024, out, etag_dir=etag_dir)

    assert len(pl.read_csv(out)) == 1
    mock_get.assert_called_once()  # no body downloaded


# --- download_tour_players ---

@patch('my_data_platform.ingest.requests.get')
def test_download_tour_players(mock_get, tmp_path):
    """Players CSV is written from response bytes."""
    mock_get.return_value = _mock_200(_csv_bytes([PLAYER_ROW]))
    out = tmp_path / 'atp_players.csv'

    download_tour_players('atp', 'https://example.com', out, etag_dir=tmp_path / '.etags')

    assert pl.read_csv(out)['first_name'][0] == 'Novak'


@patch('my_data_platform.ingest.requests.get')
def test_download_tour_players_404_raises(mock_get, tmp_path):
    """404 on players file raises RuntimeError."""
    mock_get.return_value = _mock_404()

    with pytest.raises(RuntimeError, match='not found'):
        download_tour_players('atp', 'https://example.com', tmp_path / 'out.csv',
                              etag_dir=tmp_path / '.etags')


# --- download_tour_rankings ---

@patch('my_data_platform.ingest.requests.get')
def test_download_tour_rankings(mock_get, tmp_path):
    """Rankings CSV is written with correct data."""
    mock_get.return_value = _mock_200(_csv_bytes([RANKING_ROW]))
    out = tmp_path / 'atp_rankings.csv'

    download_tour_rankings('atp', 'https://example.com', 2024, 2024, out,
                           etag_dir=tmp_path / '.etags')

    assert pl.read_csv(out)['ranking'][0] == 1
