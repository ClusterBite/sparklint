from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# `SET key` reads the value; it is not a write.
spark.sql("SET spark.executor.memory")

# sqlQuery= and a statement bound to a name are read like a positional literal.
spark.sql(sqlQuery="SET spark.executor.memory=8g")  # expect: CFG003
STMT = "SET spark.serializer = org.apache.spark.serializer.KryoSerializer"
spark.sql(STMT)  # expect: CFG003
