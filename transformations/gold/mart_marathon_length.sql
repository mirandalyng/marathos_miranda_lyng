USE CATALOG marathos;

USE SCHEMA gold;


CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_marathon_length
  COMMENT "Serving view for marathons in distance category" AS

SELECT 
    fr.result_id,
    fr.race_id,
    a.athlete_gender,
    a.athlete_age_category,
    a.athlete_country_name,
    a.athlete_club,
    fr.athlete_performance, 
    e.event_name,
    e.event_country_name,
    e.event_distance_h,
    r.event_dates,
    r.year_of_event,
    fr.athlete_performance_distance_km,
    fr.athlete_average_speed

FROM 
    fct_results fr

LEFT JOIN dim_athlete a ON fr.athlete_id = a.athlete_id
LEFT JOIN dim_event e ON fr.event_id = e.event_id
LEFT JOIN dim_race r ON fr.race_id = r.race_id
WHERE 
  e.event_unit = 'h'

 