#Spark declarity pipeline to ingest the data into our bronze layer

from pyspark import pipelines as dp

# Base dir with the data folders
BASE_DIR = "/Volumes/marathos/default/raw"

# Provide a schema to the data

schema = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(f"{BASE_DIR}/data/TWO_CENTURIES_OF_UM_RACES.csv")
    .schema
)

# This will create a streaming table 
# Supply-cataloge --> bronze schema --> raw supply chain (table)
@dp.table(name="marathos.bronze.raw_marathos", comment ="Raw dataset for marathos (ultra marathon dataset)", table_properties={
    "delta.columnMapping.mode": "name", 
    "delta.minReaderVersion": "2", 
    "delta.minWriterVersion": "5"
})

def raw_marathos_data(): 
    return (
        spark.readStream.format("csv")
        .options(header=True, encoding="latin1")
        .schema(schema).load(f"{BASE_DIR}/data")
    )

