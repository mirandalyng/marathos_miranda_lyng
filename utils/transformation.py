import re
from pyspark.sql.functions import (
    to_timestamp,
    col,
    coalesce,
    lit,
    when,
    round as spark_round,
    regexp_replace,
    regexp_extract,
    trim,
    try_to_date,
    sha2,
    concat_ws,
)
from pyspark.sql import DataFrame

####################
## EVENT
####################


### TRANSFORM EVENT COLUMNS
def transform_event(df: DataFrame) -> DataFrame:

    # clean event_name from special characters
    df = (
        df.withColumn(
            "event_name",
            coalesce(
                when(
                    trim(regexp_replace(col("event_name"), r'^#|"|\*', "")) == "", None
                ).otherwise(trim(regexp_replace(col("event_name"), r'^#|"|\*', ""))),
                lit("unknown"),
            ),
        )
        .withColumn("event_dates", try_to_date(trim(col("event_dates")), "dd.MM.yyyy"))
        # event-unit is created from event_distance/length
        .withColumn(
            "event_unit",
            when(col("event_distance/length").contains("km"), "km")
            .when(col("event_distance/length").contains("mi"), "mi")
            .when(col("event_distance/length").contains("h"), "h")
            .otherwise("unknown"),
            # event distance/length is cleaned from km/mi/h
        )
        .withColumn(
            "event_distance/length",
            regexp_replace(col("event_distance/length"), r"km|mi|h", ""),
        )
        ## Event distance is converted to km as a double
        .withColumn(
            "event_distance_km",
            when(
                col("event_unit") == "km",
                regexp_replace(col("event_distance/length"), "[^0-9.]", "").cast(
                    "double"
                ),
            )
            ## Event distance in mi is converted to km as a double
            .when(
                col("event_unit") == "mi",
                regexp_replace(col("event_distance/length"), "[^0-9.]", "").cast(
                    "double"
                )
                * 1.60934,
            ).otherwise(None),
        )
        ## Event distance is converted to hours as a double for timed events
        ## HH:MM format (e.g. 15:30) is converted to decimal hours (e.g. 15.5)
        .withColumn(
            "event_distance_h",
            when(
                col("event_unit") == "h",
                when(
                    col("event_distance/length").contains(":"),
                    regexp_extract(col("event_distance/length"), r"(\d+):", 1).cast(
                        "double"
                    )
                    + regexp_extract(col("event_distance/length"), r":(\d+)", 1).cast(
                        "double"
                    )
                    / 60.0,
                ).otherwise(
                    regexp_replace(col("event_distance/length"), "[^0-9.]", "").cast(
                        "double"
                    )
                ),
            ).otherwise(None),
        )  ## Event country is extracted from the event name
        .withColumn(
            "event_country",
            when(
                regexp_extract(col("event_name"), r"\(([A-Z]{3})\)", 1) == "", None
            ).otherwise(regexp_extract(col("event_name"), r"\(([A-Z]{3})\)", 1)),
        )
    )

    return df


## GENERATE EVENT ID:S
def generate_event_ids(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "event_id",
        sha2(col("event_name"), 256),
    )
    return df


## JOIN
def join_event_country(df: DataFrame, df_countries_event: DataFrame) -> DataFrame:

    df_countries_event = df_countries_event.select(
        col("country_code_alpha3").alias("event_country_code"),
        col("name_long").alias("event_country_long"),
    )

    df = df.join(
        df_countries_event,
        df["event_country"] == df_countries_event["event_country_code"],
        "left",
    )

    df = df.withColumn(
        "event_country_name",
        when(
            col("event_country_long").isNotNull(), col("event_country_long")
        ).otherwise(col("event_country")),
    )

    df = df.drop("event_country_code", "event_country_long")

    return df


####################
## ATHLETE
####################


## JOIN
def join_athlete_country(df: DataFrame, df_countries_athlete: DataFrame) -> DataFrame:

    df_countries_athlete = df_countries_athlete.select(
        col("country_code_alpha3").alias("athlete_country_code"),
        col("name_long").alias("athlete_country_long"),
    )

    df = df.join(
        df_countries_athlete,
        (df["athlete_country"] == df_countries_athlete["athlete_country_code"])
        | (df["athlete_country"] == df_countries_athlete["athlete_country_long"]),
        "left",
    )

    df = df.withColumn(
        "athlete_country_name",
        when(
            col("athlete_country_long").isNotNull(), col("athlete_country_long")
        ).otherwise(col("athlete_country")),
    )

    df = df.drop("athlete_country_code", "athlete_country_long")

    return df


## TRANSFORM
def transform_athlete(df: DataFrame) -> DataFrame:

    df = (
        df.withColumn(
            "athlete_year_of_birth",
            col("athlete_year_of_birth").cast("int"),
        )
        .withColumn(
            "athlete_club",
            coalesce(
                trim(regexp_replace(col("athlete_club"), r'^#|"|\*', "")),
                lit("unknown"),
            ),
        )
        .withColumn(
            "athlete_country", coalesce(trim(col("athlete_country")), lit("unknown"))
        )
        .withColumn(
            "athlete_gender", coalesce(trim(col("athlete_gender")), lit("unknown"))
        )
        .withColumn(
            "athlete_age_category",
            coalesce(trim(col("athlete_age_category")), lit("unknown")),
        )
    )
    return df


####################
## PERFORMANCE
####################
def transform_performance(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn(
            "performance_unit",
            when(col("athlete_performance").contains("km"), "km")
            .when(col("athlete_performance").contains("h"), "h")
            .otherwise("unknown"),
        )
        .withColumn(
            "athlete_performance",
            regexp_replace(col("athlete_performance"), r"km|mi|h", ""),
        )
        .withColumn(
            "is_valid_performance",
            when(col("athlete_performance").contains("d"), False)
            .when(
                col("event_unit").isin("km", "mi") & (col("performance_unit") == "h"),
                True,
            )
            .when(col("event_unit").isin("h") & (col("performance_unit") == "km"), True)
            .otherwise(False),
        )
        .withColumn(
            "athlete_performance_distance_km",
            when(
                col("performance_unit") == "km",
                regexp_replace(col("athlete_performance"), "[^0-9.]", "").cast(
                    "double"
                ),
            ).otherwise(None),
        )
        .withColumn(
            "performance_clean",
            when(
                col("performance_unit") == "h", trim(col("athlete_performance"))
            ).otherwise(None),
        )
        .withColumn(
            "_h", regexp_extract(col("performance_clean"), r"^(\d+):", 1).cast("double")
        )
        .withColumn(
            "_m", regexp_extract(col("performance_clean"), r":(\d+):", 1).cast("double")
        )
        .withColumn(
            "_s", regexp_extract(col("performance_clean"), r":(\d+)$", 1).cast("double")
        )
        ## Athlete performance time is converted to decimal hours as a double
        .withColumn(
            "athlete_performance_time_h",
            spark_round(
                when(
                    col("performance_unit") == "h",
                    col("_h") + (col("_m") / 60) + (col("_s") / 3600),
                ).otherwise(None),
                2,
            ),
        )
        .withColumn(
            "athlete_average_speed",
            spark_round(
                when(
                    col("event_unit").isin("km", "mi"),
                    col("event_distance_km") / col("athlete_performance_time_h"),
                )
                .when(
                    col("event_unit") == "h",
                    col("athlete_performance_distance_km") / col("event_distance_h"),
                )
                .otherwise(None),
                2,
            ),
        )
        ## Filter and keep the valid performances, then drop the column
        .filter(col("is_valid_performance") == True)
        .drop("is_valid_performance", "performance_clean", "_h", "_m", "_s")
    )

    return df


## ID RACE & RESULT


def generate_race_ids(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "race_id",
        sha2(concat_ws("_", col("event_name"), col("event_dates")), 256),
    )
    return df


def generate_result_ids(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "result_id",
        sha2(concat_ws("_", col("race_id"), col("athlete_id").cast("string")), 256),
    )

    return df
