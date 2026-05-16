"""Download ATP/WTA tennis CSVs from GitHub into data/01_raw/."""
from __future__ import annotations

import io
from pathlib import Path

import polars as pl
import requests
from loguru import logger

from conf.config import conf


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


def _concat_to_csv(contents: list[bytes], output_path: Path) -> int:
    """Concatenate CSV byte blobs in-memory via polars and write to output_path."""
    result = pl.concat([pl.read_csv(io.BytesIO(c)) for c in contents])
    result.write_csv(output_path)
    return len(result)


def download_tour_matches(tour: str, base_url: str, year_start: int, year_end: int, output_path: Path) -> None:
    """Download and concatenate per-year match CSVs into a single file."""
    contents = []
    for year in range(year_start, year_end + 1):
        url = f'{base_url}/{tour}_matches_{year}.csv'
        logger.info(f'Downloading {url}')
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            logger.warning(f'Not found (skipping): {url}')
            continue
        resp.raise_for_status()
        contents.append(resp.content)
    if not contents:
        raise RuntimeError(f'No match data found for {tour} {year_start}–{year_end}')
    total = _concat_to_csv(contents, output_path)
    logger.info(f'Saved {output_path} ({total:,} rows)')


def download_tour_players(tour: str, base_url: str, output_path: Path) -> None:
    """Download the players master CSV for a tour."""
    url = f'{base_url}/{tour}_players.csv'
    logger.info(f'Downloading {url}')
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    output_path.write_bytes(resp.content)
    logger.info(f'Saved {output_path}')


def download_tour_rankings(tour: str, base_url: str, year_start: int, year_end: int, output_path: Path) -> None:
    """Download and concatenate per-year rankings CSVs into a single file."""
    contents = []
    for year in range(year_start, year_end + 1):
        url = f'{base_url}/{tour}_rankings_{year}s.csv'
        logger.info(f'Downloading {url}')
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            logger.warning(f'Not found (skipping): {url}')
            continue
        resp.raise_for_status()
        contents.append(resp.content)
    if not contents:
        raise RuntimeError(f'No rankings data found for {tour} {year_start}–{year_end}')
    total = _concat_to_csv(contents, output_path)
    logger.info(f'Saved {output_path} ({total:,} rows)')


if __name__ == '__main__':
    main()
