import joblib
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession.builder.getOrCreate()


# Grouped-aggregate and legacy grouped pandas UDFs run once per group: own message.
@pandas_udf("double")
def agg_mean(v: pd.Series) -> float:
    m = joblib.load("/dbfs/m.pkl")  # expect: UDF011
    return float(m.predict(v).mean())


@pandas_udf("double")
def agg_np(v: pd.Series) -> np.float64:
    m = joblib.load("/dbfs/m.pkl")  # expect: UDF011
    return m.predict(v).mean()


@pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
def legacy_map(pdf):
    m = joblib.load("/dbfs/m.pkl")  # expect: UDF011
    return pdf


@pandas_udf("double", functionType=PandasUDFType.GROUPED_AGG)
def legacy_agg(v):
    m = joblib.load("/dbfs/m.pkl")  # expect: UDF011
    return 1.0
