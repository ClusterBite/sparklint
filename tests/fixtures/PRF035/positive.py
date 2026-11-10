from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")


def score(pdf):
    return pdf


out = df.groupBy().applyInPandas(score, schema="id long, v double")  # expect: PRF035
out2 = df.groupby().applyInPandas(score, "id long, v double")  # expect: PRF035
out3 = df.groupBy([]).applyInPandas(score, schema=df.schema)  # expect: PRF035
out4 = df.groupBy(()).applyInArrow(score, schema=df.schema)  # expect: PRF035

everything = df.groupBy()
out5 = everything.applyInPandas(score, schema=df.schema)  # expect: PRF035

other = spark.read.table("labels")
pairs = df.groupBy().cogroup(other.groupBy())  # expect: PRF035


@pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
def legacy(pdf):
    return pdf


out6 = df.where(F.col("v") > 0).groupBy().apply(legacy)  # expect: PRF035


def build(frame: DataFrame):
    return frame.groupBy().applyInPandas(score, schema=frame.schema)  # expect: PRF035


for day in ["2026-01-01", "2026-01-02"]:
    daily = spark.table("events").where(F.col("day") == day)
    daily.groupBy().applyInPandas(score, schema=daily.schema).write.saveAsTable("t")  # expect: PRF035

multi = (
    df.select("id", "v")
    .groupBy()
    .applyInPandas(  # expect: PRF035
        score,
        schema="id long, v double",
    )
)


def untyped(frame):
    # A parameter named like a DataFrame (heuristic type): applyInPandas exists only on
    # Spark's GroupedData, so the likely receiver is enough.
    return frame.groupBy().applyInPandas(score, schema="x")  # expect: PRF035
