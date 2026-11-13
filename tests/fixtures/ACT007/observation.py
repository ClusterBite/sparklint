"""Actions that fill a Spark Observation are the point of df.observe(): not reported."""

from pyspark.sql import DataFrame, Observation, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.observation import Observation as Obs

spark = SparkSession.builder.getOrCreate()


def observed_row_count(df: DataFrame) -> int:
    obs = Observation("metrics")
    observed = df.observe(obs, F.count(F.lit(1)).alias("rows"))
    observed.count()
    return obs.get["rows"]


def populate(metrics_df: DataFrame | None, observation: Observation | None) -> None:
    # Force an action so the Observation is filled; otherwise .get blocks.
    if metrics_df is not None and observation is not None:
        metrics_df.count()


def populate_quoted(metrics_df: DataFrame, observation: "Obs") -> None:
    metrics_df.collect()


def from_library(engine, df: DataFrame):
    checked_df, observation = engine.apply_checks(df, [])
    checked_df.count()  # materialise the observation
    assert observation.get == {"input_row_count": 4}


checked, metrics = from_library(None, spark.table("t"))
checked.count()
print(metrics.get)
