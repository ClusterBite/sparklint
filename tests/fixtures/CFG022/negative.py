import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# 16 MB and above, invalid / non-positive sizes, other properties, non-constant values.
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '16mb')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '128mb')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '1gb')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '268435456')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '0')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = 'auto')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.tuneFileSizesForRewrites' = 'true')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.checkpointInterval' = '5')")
spark.sql("ALTER TABLE t UNSET TBLPROPERTIES ('delta.targetFileSize')")
spark.sql("SHOW TBLPROPERTIES t")
spark.sql("SELECT 'delta.targetFileSize = 1mb' AS note")
spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", "256mb")
spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", os.environ["SIZE"])
spark.conf.get("spark.databricks.delta.properties.defaults.targetFileSize", "1mb")
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "1gb")
spark.conf.unset("spark.databricks.delta.properties.defaults.targetFileSize")
spark.read.option("delta.targetFileSize", "1mb").table("t")


# Not a SparkSession.
class Db:
    def sql(self, q):
        pass


Db().sql("ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '1mb')")
props = {"delta.targetFileSize": "1mb"}

spark.sql("ALTER TABLE hot_merge SET TBLPROPERTIES ('delta.targetFileSize' = '8mb')")  # noqa: CFG022
