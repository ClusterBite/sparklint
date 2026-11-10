from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data/in")

# Writer option.
df.write.option("maxRecordsPerFile", 10000).parquet("/data/out")  # expect: IOD019
df.write.option("maxRecordsPerFile", "5000").mode("overwrite").save("/data/out")  # expect: IOD019
df.write.format("delta").option("MAXRECORDSPERFILE", 50000).saveAsTable("t")  # expect: IOD019
df.write.options(maxRecordsPerFile=1000, compression="snappy").csv("/data/csv")  # expect: IOD019
df.write.options(**{"maxRecordsPerFile": 99999}).json("/data/json")  # expect: IOD019
df.write.save("/data/out", format="parquet", maxRecordsPerFile=20000)  # expect: IOD019
df.writeTo("cat.db.t").option("maxRecordsPerFile", 1000).append()  # expect: IOD019

CAP = 10000
df.write.option("maxRecordsPerFile", CAP).parquet("/data/out")  # expect: IOD019

writer = df.write.format("parquet").option("maxRecordsPerFile", 2000)
writer.save("/data/out")  # expect: IOD019

(
    df.writeStream.format("delta")
    .option("checkpointLocation", "/chk")
    .option("maxRecordsPerFile", 10000)
    .start("/data/stream")  # expect: IOD019
)

# Session config.
spark.conf.set("spark.sql.files.maxRecordsPerFile", 10000)  # expect: IOD019
spark.conf.set("spark.sql.files.maxRecordsPerFile", "50000")  # expect: IOD019
spark.sql("SET spark.sql.files.maxRecordsPerFile = 1000")  # expect: IOD019
session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.files.maxRecordsPerFile", "20000")  # expect: IOD019
    .getOrCreate()
)
conf = SparkConf().set("spark.sql.files.maxRecordsPerFile", "30000")  # expect: IOD019


def save(frame: DataFrame, path):
    frame.write.option("maxRecordsPerFile", 25000).parquet(path)  # expect: IOD019


def scoped(spark: SparkSession, frame, path):
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 1000)  # expect: IOD019
    frame.write.parquet(path)
    spark.conf.set("spark.sql.files.maxRecordsPerFile", 0)
