from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Keys in constants, unset, and sqlQuery= go through the shared config-write matcher.
EXEC_MEM = "spark.executor.memory"
spark.conf.set(EXEC_MEM, "8g")  # expect: CFG003
spark.conf.unset("spark.executor.cores")  # expect: CFG003
spark.sql(sqlQuery="SET spark.task.cpus = 2")  # expect: CFG003

# Job code: SparkConf / builder config before the session starts are fine.
conf = SparkConf().set("spark.executor.memory", "8g").setAll([("spark.driver.memory", "4g")])
s2 = SparkSession.builder.config(map={"spark.executor.memory": "8g"}).getOrCreate()
