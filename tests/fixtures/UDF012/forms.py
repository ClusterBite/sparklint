import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# A lambda bound once to a name, then passed to a row-wise sink.
send = lambda r: requests.post("https://hook", json=r.asDict())  # expect: UDF012
df.foreach(send)

fetch = lambda x: requests.get(f"https://api/{x}").text  # expect: UDF012
fetch_udf = F.udf(fetch, "string")

ping = lambda r: requests.head(r.url)  # expect: UDF012
df.rdd.map(ping).count()

# The function as a keyword argument.
F.udf(f=lambda x: requests.get(x).text, returnType="string")  # expect: UDF012
spark.udf.register("geo", f=lambda ip: requests.get(ip).text)  # expect: UDF012


# A module-level cache filled on first use: one client per Python worker.
_session = None


def cached_get(x):
    global _session
    if _session is None:
        _session = requests.Session()
    return _session.get(x).text


cached_udf = F.udf(cached_get, "string")

# A lambda bound on two branches: each binding is followed.
if spark.conf.get("mode") == "a":
    either = lambda r: requests.post("https://a", json=r.asDict())  # expect: UDF012
else:
    either = lambda r: requests.post("https://b", json=r.asDict())  # expect: UDF012
df.foreach(either)

# A lambda that is only used on the driver.
local = lambda u: requests.get(u).json()
cfg = local("https://config")
