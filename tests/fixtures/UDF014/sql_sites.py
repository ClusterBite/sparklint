from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
spark.udf.register("geo_near", lambda a, b, c, d: True, "boolean")

people = spark.table("people")
names = spark.table("names")

# Registered UDF called from F.expr text: the str= keyword, a name.
a = people.join(names, F.expr(str="geo_near(p.lat, p.lon, n.lat, n.lon)"))  # expect: UDF014
NEAR = "geo_near(p.lat, p.lon, n.lat, n.lon)"
b = people.join(names, F.expr(NEAR))  # expect: UDF014
