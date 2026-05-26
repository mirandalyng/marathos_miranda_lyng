from pyspark import pipelines as dp

# Base dir with the data folders
BASE_DIR = "/Volumes/marathos/default/raw"

# Provide a schema to the data

schema = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(f"{BASE_DIR}/country_data/countries.csv")
    .schema
)


# This will create a streaming table
# Supply-cataloge --> bronze schema --> raw supply chain (table)
@dp.table(
    name="marathos.bronze.raw_countries",
    comment="Raw dataset for iso3166 country codes",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def raw_countries_data():
    return (
        spark.readStream.format("csv")
        .options(header=True, encoding="UTF-8")
        .schema(schema)
        .load(f"{BASE_DIR}/country_data")
    )
