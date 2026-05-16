MODEL (name bronze.players,
       kind FULL,
       description 'Combined ATP and WTA player rosters with tour label.');

SELECT *, 'ATP' AS tour FROM bronze.atp_players
UNION ALL
SELECT *, 'WTA' AS tour FROM bronze.wta_players
