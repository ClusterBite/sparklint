import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enabled, other properties, non-constant values.
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')")
spark.sql("ALTER TABLE t UNSET TBLPROPERTIES ('delta.enableDeletionVectors')")
spark.sql("SHOW TBLPROPERTIES t")
spark.sql("SELECT 'delta.enableDeletionVectors = false' AS note")
spark.sql("COMMENT ON TABLE t IS 'TBLPROPERTIES (delta.enableDeletionVectors = false)'")
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')  -- was 'false'")
spark.sql("REORG TABLE t APPLY (PURGE)")
spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", os.environ["DV"])
spark.conf.get("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
spark.conf.set("spark.databricks.delta.deletionVectors.useMetadataRowIndex", "false")
spark.conf.unset("spark.databricks.delta.properties.defaults.enableDeletionVectors")

# A writer option is not a TBLPROPERTIES list (not covered yet).
spark.range(3).write.format("delta").option("compression", "false").save("/tmp/x")


# Not a SparkSession.
class Db:
    def sql(self, q):
        pass


Db().sql("ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")
props = {"delta.enableDeletionVectors": "false"}

spark.sql("ALTER TABLE ext SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")  # noqa: CFG019
