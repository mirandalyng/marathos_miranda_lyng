USE CATALOG marathos;

USE SCHEMA gold;


CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_swedish_athletes
  COMMENT "Serving view for marathons in distance category" AS

SELECT 
  a.athlete_id, 
  a.athlete_gender,
  a.athlete_age_category,
  a.athlete_country_name,
  a.athlete_club,
  a.athlete_performance
FROM 
    fct_results fr

LEFT JOIN dim_athlete a ON fr.athlete_id = a.athlete_id

WHERE
 a.athlete_country = 'SWE' 
