from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.streaming.state import GroupStateTimeout
import pandas as pd

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")
stream = spark.readStream.table("events")


def score(pdf):
    return pdf


# A constant key is one global group too.
c1 = df.groupBy(F.lit(1)).applyInPandas(score, schema=df.schema)  # expect: PRF035
c2 = df.groupBy(F.lit("all").alias("g")).applyInPandas(score, schema=df.schema)  # expect: PRF035
c3 = df.groupby([lit(0)]).applyInArrow(score, schema=df.schema)  # expect: PRF035
c4 = df.groupBy(F.lit(1), F.lit("x")).applyInPandas(score, schema=df.schema)  # expect: PRF035

# Streaming stateful functions: one state, one task per micro-batch.
s1 = stream.groupBy().applyInPandasWithState(  # expect: PRF035
    score, "a long", "c long", "append", GroupStateTimeout.NoTimeout
)
s2 = stream.groupBy(F.lit(1)).transformWithStateInPandas(score, "a long", "append", "None")  # expect: PRF035

# Traps: a string is a column NAME, a constant next to a real key, a lit of a variable
# of a Column (that is the column), a driver-built frame, keyed stateful grouping.

ok1 = df.groupBy("1").applyInPandas(score, schema=df.schema)
ok2 = df.groupBy(F.lit(1), "id").applyInPandas(score, schema=df.schema)
ok3 = df.groupBy(F.lit(F.col("id"))).applyInPandas(score, schema=df.schema)
ok4 = spark.createDataFrame(pd.DataFrame({"a": [1]})).groupBy().applyInPandas(score, "a long")
ok5 = stream.groupBy("user").applyInPandasWithState(
    score, "a long", "c long", "append", GroupStateTimeout.NoTimeout
)
ok6 = df.groupBy(F.lit(1)).agg(F.sum("v"))
