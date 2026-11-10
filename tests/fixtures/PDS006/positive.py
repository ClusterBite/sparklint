import pyspark.pandas as ps
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()

psdf = ps.read_table("sales")
psdf["running"] = psdf["amount"].cumsum()  # expect: PDS006
psdf["prev"] = psdf["amount"].shift(1)  # expect: PDS006
psdf["delta"] = psdf["amount"].diff()  # expect: PDS006
psdf["growth"] = psdf["amount"].pct_change()  # expect: PDS006
ranks = psdf.rank()  # expect: PDS006
best = psdf[["a", "b"]].cummax()  # expect: PDS006
worst = psdf.sort_values("ts")["amount"].cummin()  # expect: PDS006
prod = (psdf["amount"] * 2 + 1).cumprod()  # expect: PDS006
masked = psdf[psdf["amount"] > 0]["amount"].cumsum()  # expect: PDS006
chained = psdf["amount"].shift(1).cumsum()  # expect: PDS006

from_spark = spark.table("events").pandas_api()
lagged = from_spark.shift(2)  # expect: PDS006

ser = ps.Series([1, 2, 3])
csum = ser.cumsum()  # expect: PDS006
fp = ps.from_pandas(load_pandas())  # noqa: F821
fp_rank = fp["score"].rank(ascending=False)  # expect: PDS006


def per_user(frame: ps.DataFrame) -> ps.DataFrame:
    return frame["x"].diff()  # expect: PDS006


def from_df(df: DataFrame):
    return df.pandas_api().cumsum()  # expect: PDS006
