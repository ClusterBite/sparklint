from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

url = "jdbc:postgresql://db/x"
a = spark.read.jdbc(url, "orders", column="id", lowerBound=1, upperBound=100, numPartitions=8)
b = spark.read.jdbc(url, "orders", predicates=["a < 5", "a >= 5"])
c = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", "orders")
    .option("partitionColumn", "id")
    .option("lowerBound", 1)
    .option("upperBound", 100)
    .option("numPartitions", 8)
    .load()
)

def read_with(opts):
    # Options from an opaque dict: partitioning may be in there.
    return spark.read.format("jdbc").options(**opts).load()


d = spark.read.jdbc(url, "orders", properties={"partitionColumn": "id", "numPartitions": "8"})
e = spark.read.format("delta").load("/p")


def read_sql_table(table_name, con, options):
    reader = spark.read
    reader.option("dbtable", table_name)
    reader.option("url", con)
    reader.options(**options)
    return reader.format("jdbc").load()
