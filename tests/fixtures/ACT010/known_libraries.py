# Databricks notebook source
import dbldatagen as dg
from databricks.feature_engineering import FeatureEngineeringClient
from tempo import TSDF

# COMMAND ----------

# DataFrames handed out by other libraries: reported.
logs_spec = (
    dg.DataGenerator(spark, name="logs", rows=1000, partitions=4)
    .withIdOutput()
    .withColumn("level", "string", values=["INFO", "WARN"])
)
logs = logs_spec.build()
display(logs)  # expect: ACT010

fe = FeatureEngineeringClient()
features = fe.read_table(name="main.ml.features")
display(features)  # expect: ACT010
training_set = fe.create_training_set(df=logs, feature_lookups=[], label="y")
display(training_set.load_df())  # expect: ACT010

quotes = TSDF(logs, ts_col="event_ts", partition_cols=["symbol"])
display(quotes.df)  # expect: ACT010
display(quotes.resample(freq="min", func="mean").df)  # expect: ACT010

# COMMAND ----------

# Not DataFrames, or bounded: nothing to report.
display(logs_spec)
display(quotes)
display(training_set)
display(logs.limit(10))
display(quotes.df.limit(10))
logs_spec.explain()
quotes.show()
