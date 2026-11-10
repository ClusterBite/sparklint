from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("ALTER TABLE sales SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")  # expect: CFG019
spark.sql("ALTER TABLE sales SET TBLPROPERTIES (delta.enableDeletionVectors = false)")  # expect: CFG019
spark.sql("alter table sales set tblproperties ('DELTA.ENABLEDELETIONVECTORS'='FALSE')")  # expect: CFG019
spark.sql(  # expect: CFG019
    """
    CREATE TABLE IF NOT EXISTS events (id BIGINT, ts TIMESTAMP)
    USING DELTA
    TBLPROPERTIES (
      'delta.appendOnly' = 'false',
      -- external reader cannot read DVs
      'delta.enableDeletionVectors' = 'false'
    )
    """
)
DDL = "ALTER TABLE t SET TBLPROPERTIES (\"delta.enableDeletionVectors\" = \"false\")"
spark.sql(DDL)  # expect: CFG019
table = "t"
spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")  # expect: CFG019

# Session defaults for new tables.
spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")  # expect: CFG019
spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", False)  # expect: CFG019
spark.sql("SET spark.databricks.delta.properties.defaults.enableDeletionVectors = false")  # expect: CFG019
KEY = "spark.databricks.delta.properties.defaults.enableDeletionVectors"
spark.conf.set(KEY, "false")  # expect: CFG019
session = (
    SparkSession.builder.appName("job")
    .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")  # expect: CFG019
    .getOrCreate()
)
conf = SparkConf().set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")  # expect: CFG019


def setup(spark: SparkSession, name: str):
    spark.sql(f"CREATE TABLE {name} (a INT) TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')")  # expect: CFG019


# Set back later in the file: still reported (as info).
spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")  # expect: CFG019
spark.sql("CREATE TABLE legacy_out (a INT)")
spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")
