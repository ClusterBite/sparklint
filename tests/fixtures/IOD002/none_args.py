from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
url = "jdbc:postgresql://db/x"
props = {"user": "u"}
NO_COLUMN = None

# pyspark ignores a `None` argument: these reads run on one connection.
a = spark.read.jdbc(url, "orders", column=None, properties=props)  # expect: IOD002[warning]
b = spark.read.jdbc(url, "orders", column=NO_COLUMN, properties=props)  # expect: IOD002[warning]
c = spark.read.jdbc(url, "orders", predicates=None, properties=props)  # expect: IOD002[warning]
d = spark.read.jdbc(url, "orders", None, None, None, None, None, props)  # expect: IOD002[warning]
e = spark.read.load(format="jdbc", url=url, dbtable="t", partitionColumn=None)  # expect: IOD002[warning]

# Not reported: a real partition column or predicates (also through a constant).
COLUMN = "id"
PREDICATES = ["id < 10", "id >= 10"]
f = spark.read.jdbc(url, "orders", column=COLUMN, lowerBound=1, upperBound=9, numPartitions=4, properties=props)
g = spark.read.jdbc(url, "orders", predicates=PREDICATES, properties=props)
h = spark.read.jdbc(url, "orders", "id", 1, 9, 4, properties=props)
