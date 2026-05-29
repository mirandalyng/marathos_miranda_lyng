from utils.utils import (
    rename_columns_to_snakecase,
    filter_null_on_raw,
    check_athlete_age,
    filter_event_dates,
    filter_performance_distance,
)
from utils.transformation import (
    transform_event,
    generate_event_ids,
    generate_race_ids,
    join_event_country,
    join_athlete_country,
    transform_athlete,
    transform_performance,
    generate_result_ids,
)

from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    regexp_replace,
    try_to_date,
    coalesce,
    round as spark_round,
    regexp_extract,
    sha2,
    concat_ws,
)


@dp.table(
    name="marathos.silver.marathos_obt",
    comment="Cleaned supply chain data for DataCo",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)


def cleaned_marathos():
    df = spark.sql("FROM STREAM marathos.bronze.raw_marathos")
    df_countries = spark.sql("SELECT * FROM marathos.bronze.raw_countries")
    
    df = spark.sql("FROM STREAM marathos.bronze.raw_marathos")
    df_countries = spark.sql("SELECT * FROM marathos.bronze.raw_countries")

    df = rename_columns_to_snakecase(df)
    df = filter_null_on_raw(df)
    df = transform_event(df)
    df = filter_event_dates(df)
    df = filter_performance_distance(df)
    df = generate_event_ids(df)
    df = generate_race_ids(df)
    df = transform_athlete(df)
    df = check_athlete_age(df)
    df = transform_performance(df)
    df = join_athlete_country(df, df_countries)
    df = join_event_country(df, df_countries)
    df = generate_result_ids(df)

    df = df.select(
        "event_id",
        "race_id",
        "event_name",
        "event_dates",
        "year_of_event",
        "event_country",
        "event_country_name",
        "event_unit",
        "event_distance_km",
        "event_distance_h",
        "event_number_of_finishers",
        "athlete_id",
        "athlete_year_of_birth",
        "athlete_gender",
        "athlete_age_category",
        "athlete_country",
        "athlete_country_name",
        "athlete_club",
        "result_id",
        "athlete_performance",
        "performance_unit",
        "athlete_performance_distance_km",
        "athlete_performance_time_h",
        "athlete_average_speed",
    )

    return df


 
