from pyspark.sql import DataFrame


def to_driver(frame: DataFrame):
    return frame.collect()  # expect: ACT002


def to_driver_str(frame: "DataFrame"):
    return frame.collect()  # expect: ACT002
