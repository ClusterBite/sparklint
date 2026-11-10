import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Other counts, a stats column list, other properties, non-constant values.
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '32')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '5')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '-2')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'ts,customer_id')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.checkpointInterval' = '0')")
spark.sql("ALTER TABLE t UNSET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols')")
spark.sql("SHOW TBLPROPERTIES t")
spark.sql("SELECT 'delta.dataSkippingNumIndexedCols = 0' AS note")
spark.sql("ANALYZE TABLE t COMPUTE DELTA STATISTICS")
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "40")
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", os.environ["N"])
spark.conf.get("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "0")
spark.conf.set("spark.databricks.delta.stats.collect", "true")
spark.conf.unset("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols")


# Not a SparkSession.
class Db:
    def sql(self, q):
        pass


Db().sql("ALTER TABLE t SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')")
props = {"delta.dataSkippingNumIndexedCols": "0"}

spark.sql("ALTER TABLE blobs SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')")  # noqa: CFG020
