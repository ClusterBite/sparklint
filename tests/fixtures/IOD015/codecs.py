from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Every Hadoop codec but bzip2 is unsplittable for text-based files.
a = spark.read.csv("/raw/a.csv.zst")  # expect: IOD015
b = spark.read.json("/raw/b.json.lz4")  # expect: IOD015
c = spark.read.text("/raw/c.txt.snappy")  # expect: IOD015
d = spark.read.csv("/raw/d.csv.deflate", header=True)  # expect: IOD015
e = spark.read.csv(path="/raw/e.csv.gz")  # expect: IOD015
f = spark.read.text(paths="/raw/f.txt.gz")  # expect: IOD015
g = spark.read.format("json").load(path="/raw/g.json.gz")  # expect: IOD015
h = spark.read.csv("/raw/h.csv.gz", multiLine=False)  # expect: IOD015

# Traps: splittable or not a codec extension Spark knows.
t1 = spark.read.csv("/raw/t1.csv.bz2")
t2 = spark.read.csv("/raw/t2.zip")
t3 = spark.read.csv("/raw/t3.csv.zstd")
t4 = spark.read.csv("/raw/t4_gz")

# Traps: multiLine / wholetext reads take each file whole anyway.
m1 = spark.read.json("/raw/m1.json.gz", multiLine=True)
m2 = spark.read.option("multiLine", "true").json("/raw/m2.json.gz")
m3 = spark.read.csv("/raw/m3.csv.gz", multiLine=True, header=True)
m4 = spark.read.text("/raw/m4.txt.gz", wholetext=True)
