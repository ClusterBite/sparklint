from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.writeTo("db.t").append()
df.write.saveAsTable("db.t")


class Repo:
    def insertInto(self, t):
        pass


Repo().insertInto("t")
