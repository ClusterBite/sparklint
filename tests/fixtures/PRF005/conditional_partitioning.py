from typing import Optional

from pyspark.sql import DataFrame, Window, WindowSpec
from pyspark.sql import functions as F


class Series:
    def __init__(self, df: DataFrame, ts_col: str, series_ids: Optional[list]):
        self.df = df
        self.ts_col = ts_col
        self.series_ids = series_ids

    # partitioned when the caller has series ids; one series otherwise
    def _base_window(self) -> WindowSpec:
        w = Window.orderBy(self.ts_col)
        if self.series_ids:
            w = w.partitionBy(*self.series_ids)
        return w

    def lagged(self, col: str) -> DataFrame:
        w = self._base_window()
        return self.df.withColumn("prev", F.lag(col).over(w))

    # the window's origin is unknown here
    def top_rows(self, win: WindowSpec, n: int) -> DataFrame:
        return self.df.withColumn("rn", F.row_number().over(win)).where(F.col("rn") <= n)


def latest(df: DataFrame, group_by: Optional[list]) -> DataFrame:
    spec = (
        Window.partitionBy(*group_by).orderBy(F.col("ts").desc())
        if group_by
        else Window.orderBy(F.col("ts").desc())
    )
    return df.withColumn("rn", F.row_number().over(spec))


def latest_branch(df: DataFrame, keys: list) -> DataFrame:
    if keys:
        spec = Window.partitionBy(*keys).orderBy("ts")
    else:
        spec = Window.orderBy("ts")
    return df.withColumn("rn", F.row_number().over(spec))
