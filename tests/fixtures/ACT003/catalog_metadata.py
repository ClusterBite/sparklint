from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()


class TableManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def column_info(self, table: str):
        # Catalog metadata: one row per column / property, not per data row.
        return self.spark.sql(f"DESCRIBE TABLE EXTENDED {table}").toPandas()

    def properties(self, table: str):
        result = self.spark.sql(f"SHOW TBLPROPERTIES {table}")
        return result.toPandas()


catalogs = spark.sql("SHOW CATALOGS").toPandas()
tables = spark.sql("show tables in sales").select("tableName").toPandas()
cols = spark.sql("DESC t").toPandas()
local = spark.createDataFrame(pd.DataFrame({"a": [1, 2]})).toPandas()
