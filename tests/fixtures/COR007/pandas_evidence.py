from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from helpers import load, load_frame

spark = SparkSession.builder.getOrCreate()

# A value used with a pandas-only attribute (`.iloc`, `.shape`, `.sort_values()`) is
# not known to be a Spark DataFrame: a DataFrame-only method name on it is no
# evidence (mixed evidence is none), so neither is what it returns.
x = load()
first = x.iloc[0]
x.withColumn("a", F.lit(1)).dropDuplicates()

frame = load_frame()
frame.withColumnRenamed("a", "b").dropDuplicates()
print(frame.shape)


def tidy(p):
    p.sort_values("a")
    p.withColumn("b", F.lit(2)).dropDuplicates()


# Without pandas evidence the DataFrame-only method still marks a DataFrame.
y = load()
y.withColumn("a", F.lit(1)).dropDuplicates()  # expect: COR007
