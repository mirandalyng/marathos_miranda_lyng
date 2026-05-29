CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Dimensional table for event" AS
SELECT
  event_id,
  MAX_BY(event_name, event_dates) AS event_name,
  MAX_BY(event_country, event_dates) AS event_country,
  MAX_BY(event_country_name, event_dates) AS event_country_name,
  MAX_BY(event_unit, event_dates) AS event_unit,
  MAX_BY(event_distance_km, event_dates) AS event_distance_km,
  MAX_BY(event_distance_h, event_dates) AS event_distance_h,
  MAX_BY(event_number_of_finishers, event_dates) AS event_number_of_finisher


FROM
  marathos.silver.marathos_obt


GROUP BY
  event_id;