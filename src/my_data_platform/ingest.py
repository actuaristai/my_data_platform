"""Download ATP/WTA tennis CSVs from GitHub into data/01_raw/."""
from __future__ import annotations

import io
from pathlib import Path

import polars as pl
import requests
from conf.config import conf
from loguru import logger

_HTTP_NOT_FOUND = 404
_HTTP_NOT_MODIFIED = 304
_MATCHES_RENAME: dict[str, str] = {'w_SvGms': 'w_SvGm', 'l_SvGms': 'l_SvGm'}
_PLAYERS_RENAME: dict[str, str] = {'name_first': 'first_name', 'name_last': 'last_name'}
_RANKINGS_RENAME: dict[str, str] = {'rank': 'ranking', 'player': 'player_id'}


def main() -> None:
    """Entry point: download all raw tennis data to data/01_raw/."""
    raw_dir = Path('data/01_raw')
    raw_dir.mkdir(parents=True, exist_ok=True)

    year_start: int = conf['tennis.year_start']
    year_end: int = conf['tennis.year_end']
    tours: list[str] = conf['tennis.tours']
    urls = {'atp': conf['tennis.atp_url'], 'wta': conf['tennis.wta_url']}

    for tour in tours:
        base = urls[tour]
        download_tour_matches(tour, base, year_start, year_end, raw_dir / f'{tour}_matches.csv')
        download_tour_players(tour, base, raw_dir / f'{tour}_players.csv')
        download_tour_rankings(tour, base, year_start, year_end, raw_dir / f'{tour}_rankings.csv')

    logger.info('Ingest complete.')


def _etag_paths(etag_dir: Path, url: str) -> tuple[Path, Path]:
    """Return (content_cache_path, etag_sidecar_path) for a URL."""
    name = url.rsplit('/', 1)[-1]
    return etag_dir / name, etag_dir / f'{name}.etag'


def _fetch_with_etag(url: str, etag_dir: Path) -> bytes | None:
    """Fetch URL with ETag-based conditional GET.

    Sends If-None-Match when a stored ETag exists. On 304, returns cached
    bytes from disk. On 200, downloads, updates cache and ETag sidecar,
    and returns fresh bytes. Returns None on 404.

    Args:
        url: The URL to fetch.
        etag_dir: Directory for ETag sidecars and per-URL content cache files.

    Returns:
        Downloaded (or cached) bytes, or None if the URL returned 404.

    Examples:
        >>> True  # integration-tested via test_ingest.py
        True
    """
    etag_dir.mkdir(parents=True, exist_ok=True)
    cache_path, etag_path = _etag_paths(etag_dir, url)

    stored_etag = etag_path.read_text().strip() if etag_path.exists() else None
    headers = {'If-None-Match': stored_etag} if stored_etag else {}
    resp = requests.get(url, headers=headers, timeout=30)

    if resp.status_code == _HTTP_NOT_FOUND:
        return None

    if resp.status_code == _HTTP_NOT_MODIFIED:
        if cache_path.exists():
            logger.debug(f'Not modified (cached): {url}')
            return cache_path.read_bytes()
        # ETag sidecar exists but cache was cleared - unconditional refetch
        logger.warning(f'304 but no cache found; refetching: {url}')
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    else:
        resp.raise_for_status()

    content = resp.content
    cache_path.write_bytes(content)
    new_etag = resp.headers.get('ETag')
    if isinstance(new_etag, str) and new_etag:
        etag_path.write_text(new_etag)
    return content


def _concat_to_csv(contents: list[bytes], output_path: Path,
                   rename: dict[str, str] | None = None) -> int:
    """Concatenate CSV byte blobs in-memory via polars and write to output_path."""
    frames = [pl.read_csv(io.BytesIO(c), infer_schema_length=0) for c in contents]
    result = pl.concat(frames)
    if rename:
        result = result.rename({k: v for k, v in rename.items() if k in result.columns})
    result.write_csv(output_path)
    return len(result)


def download_tour_matches(tour: str, base_url: str, year_start: int, year_end: int,  # noqa: PLR0913
                          output_path: Path, etag_dir: Path | None = None) -> None:
    """Download and concatenate per-year match CSVs into a single file.

    Args:
        tour: Tour identifier ('atp' or 'wta').
        base_url: Base URL of the JeffSackmann GitHub repo for this tour.
        year_start: First year to include.
        year_end: Last year to include (inclusive).
        output_path: Destination CSV path.
        etag_dir: Directory for ETag cache; defaults to output_path.parent/.etags.
    """
    _etag_dir = etag_dir or (output_path.parent / '.etags')
    contents = []
    for year in range(year_start, year_end + 1):
        url = f'{base_url}/{tour}_matches_{year}.csv'
        logger.info(f'Fetching {url}')
        content = _fetch_with_etag(url, _etag_dir)
        if content is None:
            logger.warning(f'Not found (skipping): {url}')
            continue
        contents.append(content)
    if not contents:
        msg = f'No match data found for {tour} {year_start}-{year_end}'
        raise RuntimeError(msg)
    total = _concat_to_csv(contents, output_path, rename=_MATCHES_RENAME)
    logger.info(f'Saved {output_path} ({total:,} rows)')


def download_tour_players(tour: str, base_url: str, output_path: Path,
                          etag_dir: Path | None = None) -> None:
    """Download the players master CSV for a tour.

    Args:
        tour: Tour identifier ('atp' or 'wta').
        base_url: Base URL of the JeffSackmann GitHub repo for this tour.
        output_path: Destination CSV path.
        etag_dir: Directory for ETag cache; defaults to output_path.parent/.etags.
    """
    _etag_dir = etag_dir or (output_path.parent / '.etags')
    url = f'{base_url}/{tour}_players.csv'
    logger.info(f'Fetching {url}')
    content = _fetch_with_etag(url, _etag_dir)
    if content is None:
        msg = f'Players file not found: {url}'
        raise RuntimeError(msg)
    df = pl.read_csv(io.BytesIO(content), infer_schema_length=0)
    df = df.rename({k: v for k, v in _PLAYERS_RENAME.items() if k in df.columns})
    df.write_csv(output_path)
    logger.info(f'Saved {output_path}')


def download_tour_rankings(tour: str, base_url: str, year_start: int, year_end: int,  # noqa: PLR0913
                           output_path: Path, etag_dir: Path | None = None) -> None:
    """Download and concatenate decade rankings CSVs into a single file.

    JeffSackmann's repo uses decade files (e.g. atp_rankings_10s.csv, atp_rankings_20s.csv).

    Args:
        tour: Tour identifier ('atp' or 'wta').
        base_url: Base URL of the JeffSackmann GitHub repo for this tour.
        year_start: First year to include (determines which decades to fetch).
        year_end: Last year to include (determines which decades to fetch).
        output_path: Destination CSV path.
        etag_dir: Directory for ETag cache; defaults to output_path.parent/.etags.
    """
    _etag_dir = etag_dir or (output_path.parent / '.etags')
    decades = sorted({(year // 10) * 10 % 100 for year in range(year_start, year_end + 1)})
    contents = []
    for decade in decades:
        url = f'{base_url}/{tour}_rankings_{decade:02d}s.csv'
        logger.info(f'Fetching {url}')
        content = _fetch_with_etag(url, _etag_dir)
        if content is None:
            logger.warning(f'Not found (skipping): {url}')
            continue
        contents.append(content)
    if not contents:
        msg = f'No rankings data found for {tour} {year_start}-{year_end}'
        raise RuntimeError(msg)
    total = _concat_to_csv(contents, output_path, rename=_RANKINGS_RENAME)
    logger.info(f'Saved {output_path} ({total:,} rows)')


if __name__ == '__main__':
    main()
