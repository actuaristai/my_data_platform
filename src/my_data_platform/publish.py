"""Push gold layer tables to the pins board for downstream consumers."""
from __future__ import annotations

import pins
from loguru import logger

from conf.config import conf
from validations.connection import get_connection

_GOLD_TABLES = ['player_surface_stats',
                'head_to_head',
                'rankings_history',
                'tournament_stats']


def main() -> None:
    """Entry point for just publish."""
    publish_gold_tables()


def publish_gold_tables(board_path: str | None = None) -> None:
    """Read each gold table and write to the configured pins board as Parquet."""
    path = board_path or conf['pins.board_path']
    board = pins.board_folder(path)
    con = get_connection()

    for table_name in _GOLD_TABLES:
        logger.info(f'Publishing gold.{table_name}')
        arrow_table = con.table(table_name, database='my_lakehouse.gold').to_pyarrow()
        board.pin_write(arrow_table, f'gold/{table_name}', type='parquet')
        logger.info(f'Pinned gold/{table_name} ({arrow_table.num_rows} rows)')

    con.disconnect()
    logger.info('Publish complete.')


if __name__ == '__main__':
    main()
