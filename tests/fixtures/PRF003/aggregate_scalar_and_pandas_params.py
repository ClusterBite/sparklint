from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder.getOrCreate()

funds = spark.read.table("fund_trans_silver")

# A row-wise function of a global aggregate is still one row: scalar broadcast idiom.
max_date_df = funds.select(F.to_date(F.max("datetime")).alias("max_dt"))
recent = funds.crossJoin(max_date_df)
rounded = funds.select(F.round(F.avg("amount"), 2).alias("avg_amt"))
with_avg = funds.crossJoin(rounded)
bounds = funds.select(F.coalesce(F.max("amount"), F.lit(0)).alias("hi"))
flagged = funds.join(bounds)


def agg_payer(months):
    # the one-row frame is captured from the enclosing scope
    return funds.crossJoin(max_date_df).where(F.col("datetime") >= F.add_months(F.col("max_dt"), -months))


# A parameter used as a pandas frame (`.iloc`, `.shape`) is not a Spark DataFrame.
def predict(df, predictions):
    chunk = df.iloc[0 : df.shape[0] // 2]
    return df.join(predictions), chunk


def merge_parts(df, parts):
    rows = len(df.iloc[:, 0])
    return df.join(pd.concat(parts)), rows
