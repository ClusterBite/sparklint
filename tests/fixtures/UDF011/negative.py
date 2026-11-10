import functools
from functools import lru_cache
from typing import Iterator, Tuple

import joblib
import mlflow
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import PandasUDFType, pandas_udf, udf

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")


# Iterator form: load once per partition, before the loop.
@pandas_udf("double")
def score_iter(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    model = mlflow.pyfunc.load_model("models:/m/1")
    for s in batches:
        yield pd.Series(model.predict(s.to_frame()))


@pandas_udf("double")
def score_multi(batches: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    model = joblib.load("/dbfs/m.pkl")
    for a, b in batches:
        yield pd.Series(model.predict(pd.concat([a, b], axis=1)))


# Legacy SCALAR_ITER.
@pandas_udf("double", PandasUDFType.SCALAR_ITER)
def legacy_iter(batches):
    model = joblib.load("/dbfs/m.pkl")
    for s in batches:
        yield s * model.coef_


# mapInPandas: load at the top of the function, before the loop (once per partition).
def predict_batches(it):
    model = mlflow.pyfunc.load_model("models:/m/1")
    for pdf in it:
        yield pdf.assign(p=model.predict(pdf))


out = df.mapInPandas(predict_batches, schema="a long, p double")


def predict_gen(it):
    model = joblib.load("/dbfs/m.pkl")
    return (pdf.assign(p=model.predict(pdf)) for pdf in it)


out_g = df.mapInPandas(predict_gen, schema="a long, p double")


# Module-level caching: lru_cache-decorated loader.
@lru_cache(maxsize=1)
def get_model():
    return mlflow.pyfunc.load_model("models:/m/1")


@functools.cache
def get_model2():
    return joblib.load("/dbfs/m.pkl")


@pandas_udf("double")
def cached(s: pd.Series) -> pd.Series:
    return pd.Series(get_model().predict(s.to_frame())) + get_model2().coef_


# Module-level caching: `global _m; if _m is None` pattern.
_MODEL = None
_CACHE = {}


@pandas_udf("double")
def global_cached(s: pd.Series) -> pd.Series:
    global _MODEL
    if _MODEL is None:
        _MODEL = mlflow.pyfunc.load_model("models:/m/1")
    return pd.Series(_MODEL.predict(s.to_frame()))


@pandas_udf("double")
def dict_cached(s: pd.Series) -> pd.Series:
    if "m" not in _CACHE:
        _CACHE["m"] = joblib.load("/dbfs/m.pkl")
    return pd.Series(_CACHE["m"].predict(s.to_frame()))


def get_model3():
    global _MODEL
    if _MODEL is None:
        _MODEL = joblib.load("/dbfs/m.pkl")
    return _MODEL


@pandas_udf("double")
def via_cached_helper(s: pd.Series) -> pd.Series:
    return pd.Series(get_model3().predict(s.to_frame()))


# Model loaded on the driver and captured (broadcast-ish): not a per-batch load.
driver_model = mlflow.pyfunc.load_model("models:/m/1")


@pandas_udf("double")
def captured(s: pd.Series) -> pd.Series:
    return pd.Series(driver_model.predict(s.to_frame()))


# Not a pandas UDF / not Spark: plain functions and pandas look-alikes.
def plain_function(s):
    return joblib.load("/dbfs/m.pkl").predict(s)


pdf = pd.DataFrame({"k": [1]})
pdf.groupby("k").apply(plain_function)


class Thing:
    def mapInPandas(self, f, schema):
        return f


Thing().mapInPandas(plain_function, "x long")


# A user function named like a loader.
def load(path):
    return path


@pandas_udf("string")
def harmless(s: pd.Series) -> pd.Series:
    return s + load("x")


# Plain (row) udf: out of scope for UDF011.
@udf("double")
def row_udf(x):
    return float(joblib.load("/dbfs/m.pkl").predict([[x]])[0])
