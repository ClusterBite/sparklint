from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")  # expect: CFG009
spark.conf.set("spark.databricks.delta.autoCompact.enabled", False)  # expect: CFG009
spark.conf.set("SPARK.DATABRICKS.DELTA.AUTOCOMPACT.ENABLED", "FALSE")  # expect: CFG009
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "false")  # expect: CFG009
spark.sql("SET spark.databricks.delta.autoCompact.enabled = false")  # expect: CFG009

KEY = "spark.databricks.delta.optimizeWrite.enabled"
spark.conf.set(KEY, "false")  # expect: CFG009

session = (
    SparkSession.builder.appName("job")
    .config("spark.databricks.delta.autoCompact.enabled", "false")  # expect: CFG009
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={"spark.databricks.delta.optimizeWrite.enabled": False}).getOrCreate()  # expect: CFG009
conf = SparkConf().set("spark.databricks.delta.autoCompact.enabled", "false")  # expect: CFG009

# Table properties in SQL.
spark.sql("ALTER TABLE sales SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false')")  # expect: CFG009
spark.sql(  # expect: CFG009
    """
    CREATE TABLE IF NOT EXISTS events (id BIGINT, ts TIMESTAMP)
    USING DELTA
    TBLPROPERTIES (
      delta.autoOptimize.autoCompact = false,
      'delta.appendOnly' = 'true'
    )
    """
)
DDL = "ALTER TABLE t SET TBLPROPERTIES (\"delta.autoOptimize.autoCompact\" = \"false\")"
spark.sql(DDL)  # expect: CFG009


def setup(spark: SparkSession):
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")  # expect: CFG009


# Set back later in the file: still reported (as info).
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")  # expect: CFG009
spark.range(10).write.format("delta").mode("append").saveAsTable("t")
spark.conf.unset("spark.databricks.delta.autoCompact.enabled")
