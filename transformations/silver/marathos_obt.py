from utils.utils import rename_columns_to_snakecase
from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    to_timestamp,
    col,
    coalesce,
    lit,
    when,
    round as spark_round,
    lower,
    trim,
    regexp_replace,
    dense_rank,
    lit,
    try_to_date,
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

    df_countries = df_countries.select("country_code_alpha3", "name_long")

    df = df.join(
        df_countries,
        (df["athlete_country"] == df_countries["country_code_alpha3"])
        | (df["athlete_country"] == df_countries["name_long"]),
        "left",
    )

    df = (
        df.withColumn(
            "athlete_year_of_birth",
            coalesce(
                col("athlete_year_of_birth").cast("int").cast("string"), lit("unknown")
            ),
        )
        .withColumn("event_dates", try_to_date(trim(col("event_dates")), "dd.MM.yyyy"))
        .withColumn(
            "event_unit",
            when(col("event_distance/length").contains("km"), "km")
            .when(col("event_distance/length").contains("mi"), "mi")
            .when(col("event_distance/length").contains("h"), "h")
            .otherwise("unknown"),
        )
        .withColumn(
            "performance_unit",
            when(col("athlete_performance").contains("km"), "km")
            .when(col("athlete_performance").contains("h"), "h")
            .otherwise("unknown"),
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
        .filter(col("is_valid_performance") == True)
        .withColumn(
            "athlete_performance_distance_km",
            when(
                col("athlete_performance").contains("km"),
                col("athlete_performance").cast("double"),
            ).otherwise(None),
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
            "performance_time",
            when(
                col("performance_unit") == "h",
                trim(regexp_replace(col("athlete_performance"), "h", "")),
            ).otherwise(None),
        )
        .withColumn("event_name", coalesce(trim(col("event_name")), lit("unknown")))
        .withColumn(
            "athlete_club",
            coalesce(trim(col("athlete_club")), lit("unknown")),  # Fixat parentes här
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
        .withColumn(
            "athlete_average_speed",
            coalesce(trim(col("athlete_average_speed")), lit("unknown")),
        )
        .withColumn(
            "athlete_country_name",
            when(col("name_long").isNotNull(), col("name_long")).otherwise(
                col("athlete_country")
            ),
        )
        .drop(
            "performance_unit",
            "performance_time",
            "athlete_performance_distance_km",
            "athlete_age_category",
            "is_valid_performance",
            "performance_unit", 
            "country_code_alpha3",
            "name_long"
        )
    )

    w_event = Window.orderBy("event_name")

    w_race = Window.partitionBy("event_name").orderBy("event_dates")

    df = df.withColumn("event_id", dense_rank().over(w_event)).withColumn(
        "race_id", dense_rank().over(w_race)
    )

    return df
