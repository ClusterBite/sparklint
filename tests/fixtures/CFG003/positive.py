from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.executor.memory", "8g")  # expect: CFG003
spark.conf.set("spark.driver.maxResultSize", "4g")  # expect: CFG003
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # expect: CFG003
spark.sql("SET spark.dynamicAllocation.enabled=false")  # expect: CFG003
