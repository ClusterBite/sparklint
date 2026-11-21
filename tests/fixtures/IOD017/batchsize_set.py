from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
# batchsize set: only the numPartitions part (and the loop note) remain.
for t in ["a", "b"]:
    df.write.option("batchsize", 10000).jdbc("jdbc:x", t)  # expect: IOD017[warning] ~"Inside a loop" !~"batchsize is also unset"
df.write.option("batchsize", 10000).jdbc("jdbc:x", "t")  # expect: IOD017[info] !~"Inside a loop" !~"batchsize is also unset"
