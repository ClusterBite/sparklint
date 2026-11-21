import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Settings applied in a loop over a constant dict or list of pairs: one write per item.
SETTINGS = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "64",
}
for key, value in SETTINGS.items():
    spark.conf.set(key, value)  # expect: CFG001 ~"spark.sql.adaptive.enabled=false"

for k, v in {"spark.sql.adaptive.skewJoin.enabled": False}.items():
    spark.conf.set(k, v)  # expect: CFG001 ~"skew-join"

PAIRS = [("spark.sql.adaptive.coalescePartitions.enabled", "false")]
for k, v in PAIRS:
    spark.conf.set(k, v)  # expect: CFG001 ~"coalescing"

builder = SparkSession.builder
for k, v in (("spark.sql.adaptive.enabled", "false"),):
    builder = builder.config(k, v)  # expect: CFG001

conf = SparkConf()
for k, v in [("spark.sql.adaptive.enabled", "false")]:
    conf.set(k, v)  # expect: CFG001

# Not followed: items that are not constant, swapped names, other loop shapes.
for k, v in os.environ.items():
    spark.conf.set(k, v)

for k, v in {"spark.sql.adaptive.enabled": "false"}.items():
    spark.conf.set(v, k)

for i, (k, v) in enumerate({"spark.sql.adaptive.enabled": "false"}.items()):
    spark.conf.set(k, v)

for k in {"spark.sql.adaptive.enabled": "false"}:
    spark.conf.set(k, "true")

for k, v in {"spark.sql.adaptive.enabled": "true", **dict(os.environ)}.items():
    spark.conf.set(k, v)


def apply(settings):
    for k, v in settings.items():
        spark.conf.set(k, v)
