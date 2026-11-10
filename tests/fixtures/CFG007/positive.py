from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "2g")  # expect: CFG007
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1gb")  # expect: CFG007
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "16GB")  # expect: CFG007
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 2 * 1024 * 1024 * 1024)  # expect: CFG007
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(4 * 1024**3))  # expect: CFG007
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1 << 31)  # expect: CFG007
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "1500m")  # expect: CFG007
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "10737418240")  # expect: CFG007
spark.sql("SET spark.sql.autoBroadcastJoinThreshold = 2147483648")  # expect: CFG007

GB = 1024 * 1024 * 1024
THRESHOLD = 3 * GB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", THRESHOLD)  # expect: CFG007

session = (
    SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", "8g")  # expect: CFG007
    .getOrCreate()
)
conf = SparkConf().set("spark.sql.autoBroadcastJoinThreshold", "2g")  # expect: CFG007


def tune(spark: SparkSession):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "2g")  # expect: CFG007
