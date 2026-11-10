from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("ALTER TABLE sales SET TBLPROPERTIES ('delta.targetFileSize' = '8mb')")  # expect: CFG022
spark.sql("ALTER TABLE sales SET TBLPROPERTIES (delta.targetFileSize = 1048576)")  # expect: CFG022
spark.sql("alter table sales set tblproperties ('DELTA.TARGETFILESIZE'=' 4MB ')")  # expect: CFG022
spark.sql(  # expect: CFG022
    """
    CREATE TABLE IF NOT EXISTS events (id BIGINT, payload STRING)
    USING DELTA
    TBLPROPERTIES (
      'delta.appendOnly' = 'true',
      'delta.targetFileSize' = '2mb'
    )
    """
)
DDL = "ALTER TABLE t SET TBLPROPERTIES (\"delta.targetFileSize\" = \"512kb\")"
spark.sql(DDL)  # expect: CFG022

# Session default for new tables.
spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", "8mb")  # expect: CFG022
spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", 4194304)  # expect: CFG022
spark.sql("SET spark.databricks.delta.properties.defaults.targetFileSize = 1mb")  # expect: CFG022
SMALL = "10mb"
spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", SMALL)  # expect: CFG022
session = (
    SparkSession.builder.appName("job")
    .config("spark.databricks.delta.properties.defaults.targetFileSize", "8m")  # expect: CFG022
    .getOrCreate()
)
conf = SparkConf().set("spark.databricks.delta.properties.defaults.targetFileSize", "15mb")  # expect: CFG022


def setup(spark: SparkSession, name: str):
    spark.sql(f"CREATE TABLE {name} (a INT) TBLPROPERTIES ('delta.targetFileSize' = '4mb')")  # expect: CFG022


# Set back later in the file: still reported (as info).
spark.conf.set("spark.databricks.delta.properties.defaults.targetFileSize", "4mb")  # expect: CFG022
spark.sql("CREATE TABLE small_files (a INT)")
spark.conf.unset("spark.databricks.delta.properties.defaults.targetFileSize")
