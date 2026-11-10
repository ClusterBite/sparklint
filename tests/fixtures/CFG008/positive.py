from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.parquet.filterPushdown", "false")  # expect: CFG008
spark.conf.set("spark.sql.parquet.filterPushdown", False)  # expect: CFG008
spark.conf.set("spark.sql.orc.filterPushdown", "FALSE")  # expect: CFG008
spark.sql("SET spark.sql.parquet.filterPushdown = false")  # expect: CFG008

KEY = "spark.sql.parquet.filterPushdown"
spark.conf.set(KEY, "false")  # expect: CFG008

session = (
    SparkSession.builder.appName("job")
    .config("spark.sql.orc.filterPushdown", "false")  # expect: CFG008
    .getOrCreate()
)
session2 = SparkSession.builder.config(map={"spark.sql.parquet.filterPushdown": False}).getOrCreate()  # expect: CFG008
conf = SparkConf().set("spark.sql.parquet.filterPushdown", "false")  # expect: CFG008

url = "jdbc:postgresql://db/x"

# JDBC reads with predicate pushdown switched off.
a = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", "orders")
    .option("pushDownPredicate", False)
    .load()  # expect: CFG008, IOD002
)
b = (
    spark.read.format("jdbc")
    .options(url=url, dbtable="orders", pushDownPredicate="false")
    .option("partitionColumn", "id")
    .load()  # expect: CFG008
)
r = spark.read.format("JDBC").option("PUSHDOWNPREDICATE", "false")
c = r.option("url", url).option("dbtable", "t").option("predicates", "x").load()  # expect: CFG008
d = spark.read.option("pushDownPredicate", "false").load(format="jdbc", url=url, dbtable="t", partitionColumn="id")  # expect: CFG008
e = spark.read.jdbc(url, "orders", predicates=["a"], properties={"user": "u", "pushDownPredicate": "false"})  # expect: CFG008
props = {"user": "u", "pushDownPredicate": False}
f = spark.read.jdbc(url, "orders", column="id", lowerBound=0, upperBound=10, numPartitions=4, properties=props)  # expect: CFG008


def load(spark: SparkSession):
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", "t")
        .option("partitionColumn", "id")
        .option("pushDownPredicate", "false")
        .load()  # expect: CFG008
    )


# Set back later in the file: still reported (as info).
spark.conf.set("spark.sql.orc.filterPushdown", "false")  # expect: CFG008
spark.read.orc("/data/x").where("a > 1").count()
spark.conf.unset("spark.sql.orc.filterPushdown")

# Option on the reader chain of a jdbc() read.
spark.read.option("pushDownPredicate", False).jdbc(url, "t", predicates=["a"])  # expect: CFG008
