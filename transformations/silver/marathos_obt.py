from utils.utils import rename_columns_to_snakecase
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
    df = rename_columns_to_snakecase(df)

    df_countries = spark.sql("SELECT * FROM marathos.bronze.raw_countries")

    df_countries_athlete = df_countries.select(
        col("country_code_alpha3").alias("athlete_country_code"),
        col("name_long").alias("athlete_country_long"),
    )

    df_countries_event = df_countries.select(
        col("country_code_alpha3").alias("event_country_code"),
        col("name_long").alias("event_country_long"),
    )

    df = (
        df.join(
            df_countries_athlete,
            (df["athlete_country"] == df_countries_athlete["athlete_country_code"])
            | (df["athlete_country"] == df_countries_athlete["athlete_country_long"]),
            "left",
        )
        #### EVENT ############
        ## Event name is cleaned from special characters
        .withColumn(
            "event_name",
            coalesce(
                when(
                    trim(regexp_replace(col("event_name"), r'^#|"|\*', "")) == "", None
                ).otherwise(trim(regexp_replace(col("event_name"), r'^#|"|\*', ""))),
                lit("unknown"),
            ),
        )
        ## Event dates is converted to a date
        .withColumn("event_dates", try_to_date(trim(col("event_dates")), "dd.MM.yyyy"))
        ## Event country is extracted from the event name
        .withColumn(
            "event_country",
            when(
                regexp_extract(col("event_name"), r"\(([A-Z]{3})\)", 1) == "", None
            ).otherwise(regexp_extract(col("event_name"), r"\(([A-Z]{3})\)", 1)),
        )
        ## Event unit is determined based on the event distance/length column
        .withColumn(
            "event_unit",
            when(col("event_distance/length").contains("km"), "km")
            .when(col("event_distance/length").contains("mi"), "mi")
            .when(col("event_distance/length").contains("h"), "h")
            .otherwise("unknown"),
        )
        ## Event distance/length is cleaned to remove km, mi and h
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
            .when(
                col("event_unit") == "mi",
                regexp_replace(col("event_distance/length"), "[^0-9.]", "").cast(
                    "double"
                )
                * 1.60934,
            )
            .otherwise(None),
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
        )
        ## Event ID is generated from the event name
        .withColumn(
            "event_id",
            sha2(col("event_name"), 256),
        )
        ## Race ID is generated from the event name and date
        .withColumn(
            "race_id",
            sha2(concat_ws("_", col("event_name"), col("event_dates")), 256),
        )
        #### ATHLETE ############
        ## Athlete year of birth is converted to an integer, null replaces unknown values
        .withColumn(
            "athlete_year_of_birth",
            col("athlete_year_of_birth").cast("int"),
        )
        ## Athlete club is cleaned from special characters
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
        ## Athlete country name is mapped from the country code
        .withColumn(
            "athlete_country_name",
            when(
                col("athlete_country_long").isNotNull(), col("athlete_country_long")
            ).otherwise(col("athlete_country")),
        )
        .withColumn(
            "athlete_gender", coalesce(trim(col("athlete_gender")), lit("unknown"))
        )
        .withColumn(
            "athlete_age_category",
            coalesce(trim(col("athlete_age_category")), lit("unknown")),
        )
        .drop("athlete_country_code", "athlete_country_long")
        #### PERFORMANCE ############
        ## Performance unit is determined based on the athlete performance column
        .withColumn(
            "performance_unit",
            when(col("athlete_performance").contains("km"), "km")
            .when(col("athlete_performance").contains("h"), "h")
            .otherwise("unknown"),
        )
        ## Athlete performance is cleaned to remove km, mi and h
        .withColumn(
            "athlete_performance",
            regexp_replace(col("athlete_performance"), r"km|mi|h", ""),
        )
        ## Athlete performance is valid if the athlete performance contains km or h and the event unit is km or mi
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
        ## Filter and keep the valid performances, then drop the column
        .filter(col("is_valid_performance") == True)
        .drop("is_valid_performance")
        .filter(
            ~col("event_distance/length").rlike(r"(?i)etappen|tage|days|/|,") &
            col("athlete_id").isNotNull() &
            col("year_of_event").isNotNull() &
            col("event_dates").isNotNull() & 
            (col("year_of_event") - col("athlete_year_of_birth") >= 18) & 
            (col("year_of_event") - col("athlete_year_of_birth") <= 83) &
            (col("athlete_year_of_birth") >= 1700)
        )
        ## Athlete performance distance is converted to km as a double
        .withColumn(
            "athlete_performance_distance_km",
            when(
                col("performance_unit") == "km",
                regexp_replace(col("athlete_performance"), "[^0-9.]", "").cast(
                    "double"
                ),
            ).otherwise(None),
        )
        ## Athlete performance time is converted to decimal hours as a double
        ## By creating temporary columns for hours, minutes and seconds
        ## and adding them together as decimal hours
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
        ## Dropping temp columns
        .drop("performance_clean", "_h", "_m", "_s")
        ## Calculate average speed and replace it into the athlete_average_speed column
        ## Therefore there is a correct calc and less null values
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
    )

    ## Event country name is mapped from the country code
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
    ## adding result id from race id and athlete id
    df = df.withColumn(
        "result_id",
        sha2(concat_ws("_", col("race_id"), col("athlete_id").cast("string")), 256),
    ).drop("event_country_code", "event_country_long")

    # ordering the columns
    df = df.select(
        # EVENT
        "event_id",
        "race_id",
        "event_name",
        "event_dates",
        "year_of_event",
        "event_country",
        "event_country_name",
        "event_unit",
        "event_distance/length",
        "event_distance_km",
        "event_distance_h",
        "event_number_of_finishers",
        # ATHLETE
        "athlete_id",
        "athlete_year_of_birth",
        "athlete_gender",
        "athlete_age_category",
        "athlete_country",
        "athlete_country_name",
        "athlete_club",
        # PERFORMANCE
        "result_id",
        "athlete_performance",
        "performance_unit",
        "athlete_performance_distance_km",
        "athlete_performance_time_h",
        "athlete_average_speed",
    )

    return df
