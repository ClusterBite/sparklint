import numpy as np
import pandas as pd
import polars as pl
import pyspark.pandas as ps
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# A pandas frame stays pandas through its methods, whatever the variable is called
# (`*_df` names are DataFrames only when nothing else is known about the value).
pd_df = pd.read_parquet("trips.parquet")
zones_df = pd.read_csv("zones.csv")
pd_df = pd_df.join(zones_df.set_index("LocationID"), on="PULocationID")
pd_df.head()
pd_df.count()

merged_df = pd_df.merge(zones_df, on="LocationID")
merged_df.head()
agg_df = pd_df.groupby("zone").sum()
agg_df.head()
agg_df.first()
top_df = pd_df.sort_values("fare").drop(columns=["a"]).filter(items=["b"])
top_df.take([0, 1])
sub_df = pd_df[pd_df["fare"] > 10]
sub_df.count()
loc_df = pd_df.loc[pd_df.fare > 10, ["fare"]]
loc_df.head()
scaled_df = pd_df * 2
scaled_df.head()
series_df = pd.Series([1, 2]).to_frame()
series_df.head()
norm_df = pd.json_normalize([{"a": 1}])
norm_df.head()

local_df = spark.table("t").toPandas().reset_index()
local_df.head()
converted_df = spark.table("t").pandas_api().to_pandas().dropna()
converted_df.count()

arr_df = np.array([[1, 2]]).reshape(2, 1)
arr_df.take([0])
polars_df = pl.read_parquet("x.parquet").select("a").filter(pl.col("a") > 1)
polars_df.head()
polars_df.collect()

psdf = ps.read_parquet("/data")
grouped_df = psdf.groupby("a").sum()
grouped_df.head()

for chunk_df in np.array_split(pd_df, 4):
    chunk_df.count()

# Spark frames next to them still fire.
sdf = spark.createDataFrame(pd_df)
sdf.head()  # expect: ACT007
back_df = spark.table("t").toPandas()
spark.createDataFrame(back_df).count()  # expect: ACT007
out_df = load_events()
out_df.count()  # expect: ACT007
