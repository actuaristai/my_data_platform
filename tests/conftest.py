"""Shared pytest fixtures."""
import ibis
import pytest


@pytest.fixture
def con():
    """In-memory DuckDB connection — no DuckLake required."""
    conn = ibis.duckdb.connect()
    yield conn
    conn.disconnect()
