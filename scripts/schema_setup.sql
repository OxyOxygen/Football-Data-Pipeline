-- Raw match data table
CREATE TABLE IF NOT EXISTS public.raw_matches (
    match_id INT PRIMARY KEY,
    competition_name TEXT,
    season TEXT,
    matchday INT,
    utc_date TIMESTAMP,
    status TEXT,
    home_team TEXT,
    away_team TEXT,
    score_full_time_home INT,
    score_full_time_away INT,
    winner TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for performance (Optional for learning, but good practice)
CREATE INDEX IF NOT EXISTS idx_raw_matches_date ON public.raw_matches(utc_date);
