CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete 
    COMMENT "Dimensional table for athletes" AS 
SELECT 
    athlete_id, 
    MAX_BY(athlete_year_of_birth, event_dates)  AS athlete_year_of_birth, 
    MAX_BY(athlete_gender, event_dates)  AS athlete_gender, 
    MAX_BY(athlete_age_category, event_dates)  AS athlete_age_category, 
    MAX_BY(athlete_country, event_dates) AS athlete_country, 
    MAX_BY(athlete_country_name, event_dates)  AS athlete_country_name, 
    MAX_BY(athlete_club, event_dates) AS athlete_club, 
    MAX_BY(athlete_performance, event_dates) AS athlete_performance


FROM 
    marathos.silver.marathos_obt
GROUP BY 
    athlete_id;  