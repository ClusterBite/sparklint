from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
INFER = False
MERGE = "true"
CSV = "csv"

# Options and formats held in constants count like literals.
spark.read.option("header", True).option("inferSchema", INFER).csv("/p")  # expect: IOD001[info]
spark.read.format(CSV).option("inferSchema", True).load("/p")  # expect: IOD001[warning]
spark.read.option("mergeSchema", MERGE).parquet("/p")  # expect: IOD001 ~"mergeSchema"
# A format of None is not given: the reader's own format (here none: not judged).
spark.read.load("/p", format=None)
