CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
  COMMENT "Fact table - gold layer" AS
SELECT
    result_id, 
    race_id, 
    event_id, 
    athlete_id, 
    performance_unit, 
    athlete_performance, 
    athlete_performance_distance_km, 
    athlete_performance_time_h, 
    athlete_average_speed
FROM
  STREAM marathos.silver.marathos_obt;

