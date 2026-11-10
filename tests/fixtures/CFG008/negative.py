import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enabling (the default) or other keys.
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.filterPushdown", True)
spark.conf.set("spark.sql.parquet.aggregatePushdown", "false")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.get("spark.sql.parquet.filterPushdown", "false")
spark.sql("SET spark.sql.orc.filterPushdown = true")
spark.sql("SELECT 'spark.sql.parquet.filterPushdown=false'")

# Non-constant values.
spark.conf.set("spark.sql.parquet.filterPushdown", os.environ["PUSHDOWN"])

url = "jdbc:postgresql://db/x"

# JDBC with pushdown on / not mentioned.
a = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", "t")
    .option("partitionColumn", "id")
    .option("pushDownPredicate", True)
    .load()
)
b = spark.read.jdbc(url, "t", predicates=["a"], properties={"user": "u"})
c = spark.read.jdbc(url, "t", predicates=["a"], properties={"pushDownAggregate": "false"})

# Not a JDBC source: pushDownPredicate means nothing there.
d = spark.read.format("parquet").option("pushDownPredicate", "false").load("/x")
e = spark.read.option("pushDownPredicate", "false").load("/x")

# Unknown value / opaque options.
flag = os.environ.get("PD")
f = spark.read.format("jdbc").option("url", url).option("dbtable", "t").option("predicates", "x").option("pushDownPredicate", flag).load()


# A user class with the same method names.
class Conf:
    def set(self, k, v):
        pass


Conf().set("spark.sql.parquet.filterPushdown", "false")
settings = {"spark.sql.parquet.filterPushdown": "false"}

spark.conf.set("spark.sql.parquet.filterPushdown", "false")  # noqa: CFG008
