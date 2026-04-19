-- Fact table: League Standings
-- Logic: Aggregate points from matches

WITH match_results AS (
    SELECT
        home_team AS team_name,
        CASE 
            WHEN winner = 'HOME_TEAM' THEN 3
            WHEN winner = 'DRAW' THEN 1
            ELSE 0
        END AS points,
        CASE WHEN winner = 'HOME_TEAM' THEN 1 ELSE 0 END AS win,
        CASE WHEN winner = 'DRAW' THEN 1 ELSE 0 END AS draw,
        CASE WHEN winner = 'AWAY_TEAM' THEN 1 ELSE 0 END AS loss,
        home_score AS goals_for,
        away_score AS goals_against
    FROM {{ ref('stg_matches') }}
    WHERE status = 'FINISHED'

    UNION ALL

    SELECT
        away_team AS team_name,
        CASE 
            WHEN winner = 'AWAY_TEAM' THEN 3
            WHEN winner = 'DRAW' THEN 1
            ELSE 0
        END AS points,
        CASE WHEN winner = 'AWAY_TEAM' THEN 1 ELSE 0 END AS win,
        CASE WHEN winner = 'DRAW' THEN 1 ELSE 0 END AS draw,
        CASE WHEN winner = 'HOME_TEAM' THEN 1 ELSE 0 END AS loss,
        away_score AS goals_for,
        home_score AS goals_against
    FROM {{ ref('stg_matches') }}
    WHERE status = 'FINISHED'
)

SELECT
    team_name,
    COUNT(*) AS matches_played,
    SUM(win) AS wins,
    SUM(draw) AS draws,
    SUM(loss) AS losses,
    SUM(goals_for) AS gf,
    SUM(goals_against) AS ga,
    SUM(goals_for) - SUM(goals_against) AS gd,
    SUM(points) AS total_points
FROM match_results
GROUP BY 1
ORDER BY total_points DESC, gd DESC
