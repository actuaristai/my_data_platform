"""Pointblank validation chains for each layer of the data lake."""
from __future__ import annotations

import ibis
import pointblank as pb

from validations.connection import get_connection


def validate_bronze_seeds(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate bronze SEED tables: required columns non-null."""
    _con = con or get_connection()
    atp = _con.table('atp_matches', database='my_lakehouse.bronze')
    v = pb.Validate(data=atp, tbl_name='bronze.atp_matches', label='Bronze ATP Matches') \
        .col_vals_not_null(columns='tourney_id') \
        .col_vals_not_null(columns='winner_id') \
        .col_vals_not_null(columns='loser_id') \
        .col_vals_not_null(columns='tourney_date')
    return v.interrogate()


def validate_bronze_matches(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate bronze.matches: no duplicate unique keys, tour column valid."""
    _con = con or get_connection()
    table = _con.table('matches', database='my_lakehouse.bronze')
    v = pb.Validate(data=table, tbl_name='bronze.matches', label='Bronze Matches') \
        .col_vals_not_null(columns='tourney_id') \
        .col_vals_not_null(columns='match_num') \
        .col_vals_not_null(columns='tour') \
        .col_vals_in_set(columns='tour', set=['ATP', 'WTA']) \
        .rows_distinct(columns_subset=['tourney_id', 'match_num', 'tour'])
    return v.interrogate()


def validate_silver_matches(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate silver.matches: surface in known set, no null scores."""
    _con = con or get_connection()
    table = _con.table('matches', database='my_lakehouse.silver')
    v = pb.Validate(data=table, tbl_name='silver.matches', label='Silver Matches') \
        .col_vals_not_null(columns='score') \
        .col_vals_in_set(columns='surface', set=['Hard', 'Clay', 'Grass', 'Carpet']) \
        .col_vals_not_null(columns='tourney_date')
    return v.interrogate()


def validate_gold_surface_stats(con: ibis.BaseBackend | None = None) -> pb.Validate:
    """Validate gold.player_surface_stats: win_rate in [0, 1], matches_played > 0."""
    _con = con or get_connection()
    table = _con.table('player_surface_stats', database='my_lakehouse.gold')
    v = pb.Validate(data=table, tbl_name='gold.player_surface_stats', label='Gold Surface Stats') \
        .col_vals_between(columns='win_rate', left=0.0, right=1.0) \
        .col_vals_gt(columns='matches_played', value=0) \
        .rows_distinct(columns_subset=['player_id', 'surface', 'tour'])
    return v.interrogate()
