"""CLI entry point: run all pointblank validation checks across every layer."""
from __future__ import annotations

import sys

from loguru import logger

from validations.checks import (validate_bronze_matches,
                                validate_bronze_seeds,
                                validate_gold_surface_stats,
                                validate_silver_matches)
from validations.connection import get_connection


def main() -> None:
    """Run all layer validations. Exit 1 if any check fails."""
    con = get_connection()
    checks = [validate_bronze_seeds(con),
              validate_bronze_matches(con),
              validate_silver_matches(con),
              validate_gold_surface_stats(con)]
    failed = [v for v in checks if not v.all_passed()]
    if failed:
        for v in failed:
            logger.error(f'Validation failed: {v.label}')
        sys.exit(1)
    logger.info('All validation checks passed.')


if __name__ == '__main__':
    main()
