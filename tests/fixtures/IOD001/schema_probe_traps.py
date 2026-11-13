from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The read feeds real work: still reported.
df = spark.read.json("/in")  # expect: IOD001
schema = df.schema
df.write.saveAsTable("t")

# Schema taken after a transformation: the frame is read for data, not only its schema.
cols = spark.read.json("/in").select("a").columns  # expect: IOD001

# A streaming source without a schema fails at runtime, probe or not.
s = spark.readStream.json("/in").schema  # expect: IOD001

# Another attribute of the read frame (`.write`) is real work.
spark.read.option("inferSchema", True).csv("/x.csv").write.parquet("/y")  # expect: IOD001
