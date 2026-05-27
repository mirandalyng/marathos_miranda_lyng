CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_race
    COMMENT "Dimenstional table for race" AS
SELECT
    race_id, 

    MAX_BY(event_dates, event_dates) AS event_dates, 
    MAX_BY(year_of_event, event_dates) AS year_of_event
FROM
    marathos.silver.marathos_obt
GROUP BY 
    race_id