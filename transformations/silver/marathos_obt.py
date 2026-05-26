from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when, round as spark_round, lower, trim, regexp_replace, dense_rank, lit, try_to_date
from pyspark.sql.window import Window


@dp.table(name="supply_chain_live.silver.supply_chain_obt", 
          comment ="Cleaned supply chain data for DataCo", 
          table_properties={
            "delta.columnMapping.mode": "name", 
            "delta.minReaderVersion": "2", 
            "delta.minWriterVersion": "5"
    }
)