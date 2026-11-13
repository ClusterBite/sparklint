import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf

spark = SparkSession.builder.getOrCreate()

# Driver-side data: the shuffle is cheap and only sets the number of output files.
pdf = pd.DataFrame({"id": [1, 2, 3]})
users = spark.createDataFrame(pdf)
users.repartition(5).write.mode("overwrite").format("json").save("/out/users")
users = users.withColumn("ts", F.current_timestamp())
users.repartition(5).write.mode("overwrite").format("parquet").save("/out/users_pq")

parts = []
for i in range(10):
    parts.append((i, f"part-{i}"))
spark.createDataFrame(parts, ["id", "name"]).repartition(3).write.saveAsTable("parts")

rows = [(u, 1) for u in ["a", "b"]]
(spark.createDataFrame(rows, ["user", "flag"])
    .repartition(3).write.mode("overwrite").saveAsTable("perms"))

spark.createDataFrame(pdf.dropna()).repartition(2).write.json("/out/clean")

# A one-row frame.
spark.table("t").agg(F.max("a")).repartition(4).write.saveAsTable("m")


# A Python UDF after the repartition: the repartition spreads the UDF work.
@pandas_udf("array<string>")
def split_docs(s: pd.Series) -> pd.Series:
    return s.apply(lambda x: x.split())


(spark.table("raw_docs")
    .filter("text is not null")
    .repartition(30)
    .withColumn("content", F.explode(split_docs("text")))
    .drop("text")
    .write.mode("overwrite").saveAsTable("docs"))

upper = udf(lambda s: s.upper(), "string")
spark.table("t").repartition(8).select(upper("a").alias("a")).write.saveAsTable("u")
