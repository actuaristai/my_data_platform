"""Download ATP/WTA tennis CSVs from GitHub into data/01_raw/."""
from __future__ import annotations

import io
from pathlib import Path

import polars as pl
import requests
from conf.config import conf
from loguru import logger


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


_HTTP_NOT_FOUND = 404
_MATCHES_RENAME: dict[str, str] = {'w_SvGms': 'w_SvGm', 'l_SvGms': 'l_SvGm'}
_PLAYERS_RENAME: dict[str, str] = {'name_first': 'first_name', 'name_last': 'last_name'}
_RANKINGS_RENAME: dict[str, str] = {'rank': 'ranking', 'player': 'player_id'}


def _concat_to_csv(contents: list[bytes], output_path: Path,
                   rename: dict[str, str] | None = None) -> int:
    """Concatenate CSV byte blobs in-memory via polars and write to output_path."""
    frames = [pl.read_csv(io.BytesIO(c), infer_schema_length=0) for c in contents]
    result = pl.concat(frames)
    if rename:
        result = result.rename({k: v for k, v in rename.items() if k in result.columns})
    result.write_csv(output_path)
    return len(result)


def download_tour_matches(tour: str, base_url: str, year_start: int, year_end: int, output_path: Path) -> None:
    """Download and concatenate per-year match CSVs into a single file."""
    contents = []
    for year in range(year_start, year_end + 1):
        url = f'{base_url}/{tour}_matches_{year}.csv'
        logger.info(f'Downloading {url}')
        resp = requests.get(url, timeout=30)
        if resp.status_code == _HTTP_NOT_FOUND:
            logger.warning(f'Not found (skipping): {url}')
            continue
        resp.raise_for_status()
        contents.append(resp.content)
    if not contents:
        msg = f'No match data found for {tour} {year_start}-{year_end}'
        raise RuntimeError(msg)
    total = _concat_to_csv(contents, output_path, rename=_MATCHES_RENAME)
    logger.info(f'Saved {output_path} ({total:,} rows)')


def download_tour_players(tour: str, base_url: str, output_path: Path) -> None:
    """Download the players master CSV for a tour."""
    url = f'{base_url}/{tour}_players.csv'
    logger.info(f'Downloading {url}')
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    df = pl.read_csv(io.BytesIO(resp.content), infer_schema_length=0)
    df = df.rename({k: v for k, v in _PLAYERS_RENAME.items() if k in df.columns})
    df.write_csv(output_path)
    logger.info(f'Saved {output_path}')


def download_tour_rankings(tour: str, base_url: str, year_start: int, year_end: int, output_path: Path) -> None:
    """Download and concatenate decade rankings CSVs into a single file.

    JeffSackmann's repo uses decade files (e.g. atp_rankings_10s.csv, atp_rankings_20s.csv).
    """
    decades = sorted({(year // 10) * 10 % 100 for year in range(year_start, year_end + 1)})
    contents = []
    for decade in decades:
        url = f'{base_url}/{tour}_rankings_{decade:02d}s.csv'
        logger.info(f'Downloading {url}')
        resp = requests.get(url, timeout=30)
        if resp.status_code == _HTTP_NOT_FOUND:
            logger.warning(f'Not found (skipping): {url}')
            continue
        resp.raise_for_status()
        contents.append(resp.content)
    if not contents:
        msg = f'No rankings data found for {tour} {year_start}-{year_end}'
        raise RuntimeError(msg)
    total = _concat_to_csv(contents, output_path, rename=_RANKINGS_RENAME)
    logger.info(f'Saved {output_path} ({total:,} rows)')


if __name__ == '__main__':
    main()
