import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

a = {1, 2}
b = a.union({3})
c = "-".join(["x", "y"])
pdf1 = pd.DataFrame({"k": [1]})
pdf2 = pd.DataFrame({"k": [1]})
j = pdf1.join(pdf2)
sdf = spark.table("t")
cross = sdf.join(spark.table("u"))  # expect: PRF003
