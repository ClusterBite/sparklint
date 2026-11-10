from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
raw = spark.read.table("bronze.events")

# The Stack Overflow recipe: infer a schema for a JSON string column via the RDD.
schema = spark.read.json(raw.rdd.map(lambda r: r.payload)).schema  # expect: IOD016
parsed = raw.withColumn("j", F.from_json("payload", schema))

# flatMap variant
s2 = spark.read.json(raw.select("payload").rdd.flatMap(lambda r: r)).schema  # expect: IOD016

# RDD bound to a name first
json_rdd = raw.rdd.map(lambda r: r["payload"])
s3 = spark.read.json(json_rdd)  # expect: IOD016

# keyword argument and the plain .rdd of a one-column frame
s4 = spark.read.json(path=raw.select("payload").rdd)  # expect: IOD016

# reader chain with options but no schema
s5 = spark.read.option("multiLine", "true").json(raw.rdd.map(lambda r: r.body))  # expect: IOD016


def infer(df: DataFrame, col: str):
    # typed parameter: still a DataFrame
    return spark.read.json(df.rdd.map(lambda r: r[col])).schema  # expect: IOD016


# sampled inference still converts every row through Python first
s6 = spark.read.json(raw.rdd.map(lambda r: r.payload), samplingRatio=0.1)  # expect: IOD016
