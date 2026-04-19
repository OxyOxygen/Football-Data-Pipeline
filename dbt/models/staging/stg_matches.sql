-- Staging model for matches
-- Objective: Basic cleanup and renaming

WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'raw_matches') }}
)

SELECT
    match_id,
    competition_name,
    season,
    matchday,
    utc_date::timestamp AS match_timestamp,
    status,
    home_team,
    away_team,
    COALESCE(score_full_time_home, 0) AS home_score,
    COALESCE(score_full_time_away, 0) AS away_score,
    winner,
    extracted_at
FROM raw_data
