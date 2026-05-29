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
    sha2
)

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


## GENERATE EVENT ID:S
def generate_event_ids(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "event_id",
        sha2(col("event_name"), 256),
    )

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
## JOIN
####################
