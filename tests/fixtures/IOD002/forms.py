from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
url = "jdbc:postgresql://db/x"

# Keyword arguments and reader-level options.
a = spark.read.jdbc(url=url, table="orders")  # expect: IOD002
b = spark.read.options(url=url, dbtable="t").format("jdbc").load()  # expect: IOD002
c = spark.read.load(format="jdbc", url=url, dbtable="t")  # expect: IOD002
# numPartitions alone does not split the read.
d = spark.read.format("jdbc").option("url", url).option("dbtable", "t").option("numPartitions", 8).load()  # expect: IOD002
# The `query` option is reported like `dbtable` (Spark rejects partitionColumn with it).
e = spark.read.format("jdbc").option("url", url).option("query", "select * from orders").load()  # expect: IOD002
# Filtered / limited subqueries are reported at info, also with a line break after WHERE.
f = spark.read.format("jdbc").option("url", url).option("dbtable", "(select * from orders where\n x > 1) t").load()  # expect: IOD002
g = spark.read.format("jdbc").option("url", url).option("dbtable", "(SELECT TOP 10 * FROM t) q").load()  # expect: IOD002
# A keyword inside a string literal is not a row limit (warning).
h = spark.read.format("jdbc").option("url", url).option("dbtable", "(select ' limit 1 ' from t) q").load()  # expect: IOD002

# Not reported: partitioned forms.
ok1 = spark.read.jdbc(url, "orders", "id", 1, 100, 8)
ok2 = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", "t")
    .option("PARTITIONCOLUMN", "id")
    .option("lowerBound", 1)
    .option("upperBound", 9)
    .option("numPartitions", 8)
    .load()
)
