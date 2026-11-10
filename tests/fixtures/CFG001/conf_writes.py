from pyspark import SparkConf
from pyspark.sql import SparkSession

# A builder map bound to a name, and SparkConf.setIfMissing / setAll.
SETTINGS = {"spark.sql.adaptive.enabled": "false"}
s = SparkSession.builder.config(map=SETTINGS).getOrCreate()  # expect: CFG001
conf = SparkConf().setIfMissing("spark.sql.adaptive.enabled", "false")  # expect: CFG001
conf2 = SparkConf().setAll([("spark.sql.adaptive.skewJoin.enabled", "false")])  # expect: CFG001
