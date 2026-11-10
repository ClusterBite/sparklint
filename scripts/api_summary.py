import collections
import json
import sys

s = json.load(open(sys.argv[1]))
print(s["pyspark_version"], len(s["functions"]))
for c, m in s["classes"].items():
    cnt = collections.Counter(v["returns"] for v in m.values())
    print(c, len(m), dict(cnt.most_common(6)))
d = s["classes"]["DataFrame"]
for k in ["groupBy", "write", "count", "collect", "toPandas", "withColumn", "na", "rdd", "writeTo", "first", "sparkSession", "head", "transform", "mergeInto"]:
    print(k, d.get(k))
print({k: v for k, v in s["functions"].items() if v["returns"] != "Column"})
print("other in DataFrame:", [k for k, v in d.items() if v["returns"] == "other"])
