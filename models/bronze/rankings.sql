MODEL (
  name bronze.rankings,
  kind FULL,
  description 'Combined ATP and WTA weekly rankings with tour label.'
);

SELECT *, 'ATP' AS tour FROM bronze.atp_rankings
UNION ALL
SELECT *, 'WTA' AS tour FROM bronze.wta_rankings
