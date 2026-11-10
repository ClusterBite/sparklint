from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("ALTER TABLE sales SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')")  # expect: CFG020
spark.sql("ALTER TABLE sales SET TBLPROPERTIES (delta.dataSkippingNumIndexedCols = 0)")  # expect: CFG020
spark.sql("ALTER TABLE sales SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '-1')")  # expect: CFG020
spark.sql("ALTER TABLE sales SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '')")  # expect: CFG020
spark.sql("alter table sales set tblproperties ('DELTA.DATASKIPPINGNUMINDEXEDCOLS'=' 0 ')")  # expect: CFG020
spark.sql(  # expect: CFG020
    """
    CREATE TABLE IF NOT EXISTS events (id BIGINT, payload STRING)
    USING DELTA
    TBLPROPERTIES (
      'delta.appendOnly' = 'true',
      'delta.dataSkippingNumIndexedCols' = '0'
    )
    """
)
DDL = "ALTER TABLE t SET TBLPROPERTIES (\"delta.dataSkippingNumIndexedCols\" = \"-1\")"
spark.sql(DDL)  # expect: CFG020

# Session defaults for new tables.
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "0")  # expect: CFG020
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", 0)  # expect: CFG020
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", -1)  # expect: CFG020
spark.sql("SET spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols = 0")  # expect: CFG020
NONE = 0
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", NONE)  # expect: CFG020
session = (
    SparkSession.builder.appName("job")
    .config("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "0")  # expect: CFG020
    .getOrCreate()
)
conf = SparkConf().set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "-1")  # expect: CFG020


def setup(spark: SparkSession, name: str):
    spark.sql(f"CREATE TABLE {name} (a INT) TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')")  # expect: CFG020


# Set back later in the file: still reported (as info).
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "0")  # expect: CFG020
spark.sql("CREATE TABLE raw_dump (a INT)")
spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "32")
