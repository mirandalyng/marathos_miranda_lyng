## Rename columns to snake_case 
import re
from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when, round as spark_round


## RENAME COLUMNS 
def to_snake_case(name):
    snakecase = snakecase =re.sub(r"[\s]+", "_", name.strip().lower())
    return snakecase


## This is not used but can be used for camelCase
def to_camelCase(name): 
    snakecase = to_snake_case(name)
    words = snakecase.split("_")
    camelcase = words[0].lower() + "".join(word.capitalize() for word in words[1:])
    return camelcase


def rename_columns_to_snakecase(df): 
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns) 

#####################
## FILTER FUNCTIONS
##################### 
def filter_null_on_raw(df: DataFrame) -> DataFrame: 
    df = df.filter(
        col("athlete_id").isNotNull() &
        col("year_of_event").isNotNull() 
    )
    return df

def athlete_age(df: DataFrame) -> DataFrame: