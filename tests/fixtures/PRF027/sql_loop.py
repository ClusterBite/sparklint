from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# The SQL form inside a loop.
for t in ["a", "b"]:
    sample = spark.sql(f"SELECT a FROM {t} ORDER BY rand() LIMIT 10")  # expect: PRF027 ~"SQL ORDER BY rand()" ~"Inside a loop"
