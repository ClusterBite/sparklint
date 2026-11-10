import pickle
from typing import Iterator

import boto3
import joblib
import mlflow
import pandas as pd
import psycopg2
import torch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import PandasUDFType, pandas_udf
from transformers import AutoModel, pipeline

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")


# Series -> Series: the body runs for every Arrow batch.
@pandas_udf("double")
def score(s: pd.Series) -> pd.Series:
    model = mlflow.pyfunc.load_model("models:/m/1")  # expect: UDF011
    return pd.Series(model.predict(s.to_frame()))


@F.pandas_udf("double")
def score2(s: pd.Series) -> pd.Series:
    m = joblib.load("/dbfs/model.pkl")  # expect: UDF011
    return pd.Series(m.predict(s.to_frame()))


@pandas_udf("string")
def embed(s: pd.Series) -> pd.Series:
    m = AutoModel.from_pretrained("bert-base-uncased")  # expect: UDF011
    nlp = pipeline("sentiment-analysis")  # expect: UDF011
    return s.map(lambda x: str(nlp(x)) + str(m))


@pandas_udf("string")
def tag(s: pd.Series) -> pd.Series:
    with open("/dbfs/m.pkl", "rb") as fh:
        m = pickle.load(fh)  # expect: UDF011
    s3 = boto3.client("s3")  # expect: UDF011
    return s.map(m.get) + str(s3)


# Legacy SCALAR function type is per batch too.
@pandas_udf("double", PandasUDFType.SCALAR)
def legacy(s):
    t = torch.load("/dbfs/m.pt")  # expect: UDF011
    return s * float(t)


# Same-file helper without caching: still per batch.
def load_model():
    return mlflow.sklearn.load_model("models:/m/2")


@pandas_udf("double")
def via_helper(s: pd.Series) -> pd.Series:
    m = load_model()  # expect: UDF011
    return pd.Series(m.predict(s.to_frame()))


# Function form: pandas_udf(fn, type).
def plain(s: pd.Series) -> pd.Series:
    conn = psycopg2.connect("dbname=x")  # expect: UDF011
    return s + len(str(conn))


plain_udf = pandas_udf(plain, "long")


# Iterator UDF: loading INSIDE the batch loop repeats per batch.
@pandas_udf("double")
def iter_bad(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for s in batches:
        m = mlflow.pyfunc.load_model("models:/m/1")  # expect: UDF011
        yield pd.Series(m.predict(s.to_frame()))


# mapInPandas: loading inside the `for pdf in it` loop is per batch.
def predict_batches(it):
    for pdf in it:
        m = mlflow.pyfunc.load_model("models:/m/1")  # expect: UDF011
        yield pdf.assign(p=m.predict(pdf))


out = df.mapInPandas(predict_batches, schema="a long, p double")


# applyInPandas runs per group: any load repeats per group.
def per_group(pdf):
    m = joblib.load("/dbfs/model.pkl")  # expect: UDF011
    return pdf.assign(p=m.predict(pdf))


out2 = df.groupBy("k").applyInPandas(per_group, schema="k long, p double")


# Imports inside the UDF body (common in notebooks) resolve too.
@pandas_udf("double")
def local_import(s: pd.Series) -> pd.Series:
    import mlflow.pyfunc as mp
    from joblib import load as jl

    m = mp.load_model("models:/m/1")  # expect: UDF011
    v = jl("/dbfs/v.pkl")  # expect: UDF011
    return pd.Series(m.predict(s.to_frame())) + v
