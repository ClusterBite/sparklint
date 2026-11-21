import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# A dict bound in an if/else: both branches' settings are written (each on some run).
if os.environ.get("FAST"):
    SETTINGS = {"spark.sql.adaptive.enabled": "false"}
else:
    SETTINGS = {"spark.sql.adaptive.skewJoin.enabled": "false"}
for k, v in SETTINGS.items():
    spark.conf.set(k, v)  # expect: CFG001, CFG001

# One branch unknown: the known branch's settings still count.
if os.environ.get("FROM_FILE"):
    LOADED = dict(os.environ)
else:
    LOADED = {"spark.sql.adaptive.coalescePartitions.enabled": "false"}
for k, v in LOADED.items():
    spark.conf.set(k, v)  # expect: CFG001

# Names followed through other names, list / set literals of pairs.
BASE = {"spark.sql.adaptive.enabled": "false"}
ALIAS = BASE
for k, v in ALIAS.items():
    spark.conf.set(k, v)  # expect: CFG001

for k, v in {("spark.sql.adaptive.enabled", "false")}:
    spark.conf.set(k, v)  # expect: CFG001

# SparkConf.setAll over a name bound to pairs, or a dict's items.
PAIRS = [("spark.sql.adaptive.enabled", "false")]
SparkConf().setAll(PAIRS)  # expect: CFG001
SparkConf().setAll({"spark.sql.adaptive.enabled": "false"}.items())  # expect: CFG001

# Builder config(map=...) with a dict bound in an if/else.
if os.environ.get("LOCAL"):
    OPTS = {"spark.sql.adaptive.enabled": "false"}
else:
    OPTS = {"spark.sql.adaptive.enabled": "true"}
SparkSession.builder.config(map=OPTS).getOrCreate()  # expect: CFG001

# Still not followed: a parameter, a with target.
def apply(settings):
    for k, v in settings.items():
        spark.conf.set(k, v)


with open("conf.json") as settings_file:
    for k, v in settings_file:
        spark.conf.set(k, v)
