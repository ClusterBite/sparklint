"""Discarded actions that do not fill an Observation stay reported."""

from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()


def other_function(df: DataFrame) -> None:
    # An Observation in another function does not make this action useful.
    df.count()  # expect: ACT007


def get_called(engine, df: DataFrame):
    # dict-style .get(..) calls are not an Observation read.
    checked_df, settings = engine.apply_checks(df, [])
    checked_df.count()  # expect: ACT007
    return settings.get("mode")


def read_before(engine, df: DataFrame):
    checked_df, observation = engine.apply_checks(df, [])
    print(observation.get)
    checked_df.count()  # expect: ACT007


def unrelated_pair(engine, df: DataFrame):
    other_df, observation = engine.apply_checks(df, [])
    df.collect()  # expect: ACT007
    return observation.get, other_df
