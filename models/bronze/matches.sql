MODEL (
  name bronze.matches,
  kind FULL,
  description 'Combined ATP and WTA matches with tour label.'
);

SELECT *, 'ATP' AS tour FROM bronze.atp_matches
UNION ALL
SELECT *, 'WTA' AS tour FROM bronze.wta_matches
