MODEL (name bronze.atp_rankings,
  kind SEED (path '../../data/01_raw/atp_rankings.csv'),
  columns (ranking_date INT,
           ranking INT,
           player_id INT,
           points DOUBLE));
