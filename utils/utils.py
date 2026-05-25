## Rename columns to snake_case 
import re
from pyspark.sql.functions import col, lit, coalesce, when

#tested to do both functions but will use snake_case for good readibility 

def to_snake_case(name):
    snakecase = snakecase =re.sub(r"[\s]+", "_", name.strip().lower())
    return snakecase

def to_camelCase(name): 
    snakecase = to_snake_case(name)
    words = snakecase.split("_")
    camelcase = words[0].lower() + "".join(word.capitalize() for word in words[1:])
    return camelcase


def rename_columns_to_snakecase(df): 
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns) 


def clean_null_values_string(df):
    for column, col_type in df.dtypes: 
        if col_type == "string": 
            df = df.withColumn(column, coalesce(col(column), lit("unknown")))
    return df


