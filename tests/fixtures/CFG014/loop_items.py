from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Settings applied in a loop over a constant dict or list of pairs are read
# like the direct calls (shared config-write matcher).
TUNING = {
    "spark.sql.codegen.wholeStage": "false",
    "spark.sql.shuffle.partitions": "64",
}
for key, value in TUNING.items():
    spark.conf.set(key, value)  # expect: CFG014 ~"wholeStage=false"

for key, value in [("spark.sql.parquet.enableVectorizedReader", False)]:
    SparkConf().set(key, value)  # expect: CFG014 ~"enableVectorizedReader=false"

# Not reported: values that are not constants, a parameter dict.
def apply(settings):
    for key, value in settings.items():
        spark.conf.set(key, value)
