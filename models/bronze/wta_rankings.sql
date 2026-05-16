MODEL (
  name bronze.wta_rankings,
  kind SEED (
    path '../../data/01_raw/wta_rankings.csv'
  ),
  columns (
    ranking_date INT,
    ranking INT,
    player_id INT,
    points DOUBLE
  )
);
