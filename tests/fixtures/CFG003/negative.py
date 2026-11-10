from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "64")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.sql("SET spark.sql.ansi.enabled=true")
s2 = SparkSession.builder.config("spark.executor.memory", "8g").getOrCreate()  # job code: builder config is fine
settings = {}
settings.set = None
conf = {"spark.executor.memory": "1g"}
