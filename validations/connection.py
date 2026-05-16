"""Ibis connection factory for validation queries."""
import ibis

from conf.config import conf


def get_connection(read_only: bool = True) -> ibis.BaseBackend:
    """Return an Ibis DuckDB connection with the local DuckLake catalog attached.

    Catalog path is read from conf/parameters.toml [ducklake] catalog_path.
    """
    catalog_path: str = conf['ducklake.catalog_path']
    con = ibis.duckdb.connect(extensions=['ducklake'])
    con.attach(f'ducklake:{catalog_path}', name='my_lakehouse', read_only=read_only)
    return con
