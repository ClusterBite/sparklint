from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

KEY = "spark.sql.optimizer.excludedRules"

# Set around one query and cleared afterwards: still reported, as a scoped note.
spark.conf.set(KEY, "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates")  # expect: CFG021
spark.table("t").count()
spark.conf.unset(KEY)
