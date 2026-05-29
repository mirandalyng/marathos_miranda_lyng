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
)

from pyspark.sql import DataFrame

################
#### RENAME
################


def to_snake_case(name: str) -> str:
    snakecase = snakecase = re.sub(r"[\s]+", "_", name.strip().lower())
    return snakecase


## This is not used but can be used for camelCase
def to_camelCase(name: str) -> str:
    snakecase = to_snake_case(name)
    words = snakecase.split("_")
    camelcase = words[0].lower() + "".join(word.capitalize() for word in words[1:])
    return camelcase


def rename_columns_to_snakecase(df: DataFrame) -> DataFrame:
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)


#####################
## FILTER FUNCTIONS
#####################


def filter_null_on_raw(df: DataFrame) -> DataFrame:
    df = df.filter(col("athlete_id").isNotNull() & col("year_of_event").isNotNull())
    return df


def check_athlete_age(df: DataFrame) -> DataFrame:
    df = df.filter(
        (col("year_of_event") - col("athlete_year_of_birth") >= 18)
        & (col("year_of_event") - col("athlete_year_of_birth") <= 83)
        & (col("athlete_year_of_birth") >= 1700)
    )

    return df


def filter_event_dates(df: DataFrame) -> DataFrame:
    df = df.filter(col("event_dates").isNotNull())
    return df


def filter_performance_distance(df: DataFrame) -> DataFrame:
    df = df.filter(~col("event_distance/length").rlike(r"(?i)etappen|tage|days|/|,"))
    return df
