from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Static keys applied in a loop over a constant dict are each reported.
RUNTIME = {"spark.executor.memory": "8g", "spark.sql.shuffle.partitions": "64"}
for name, val in RUNTIME.items():
    spark.conf.set(name, val)  # expect: CFG003 ~"spark.executor.memory"

for name, val in [("spark.sql.shuffle.partitions", "64")]:
    spark.conf.set(name, val)
