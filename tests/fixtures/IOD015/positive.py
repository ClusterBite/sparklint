from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

a = spark.read.csv("/raw/orders.csv.gz", header=True)  # expect: IOD015
b = spark.read.json("s3://bucket/events/2026-11-08.json.gz")  # expect: IOD015
c = spark.read.text("/logs/app.log.GZ")  # expect: IOD015
d = spark.read.format("csv").option("header", "true").load("/raw/x.csv.gzip")  # expect: IOD015
e = spark.read.load("/raw/y.json.gz", format="json")  # expect: IOD015
day = "2026-11-08"
f = spark.read.csv(f"/raw/{day}/*.csv.gz")  # expect: IOD015
g = spark.read.csv(["/raw/a.csv", "/raw/b.csv.gz"])  # expect: IOD015

PATH = "/landing/dump.json.gz"
h = spark.read.option("multiLine", "true").json(PATH)  # expect: IOD015

reader = spark.read.format("text")
i = reader.load("/logs/*.gz")  # expect: IOD015

s = spark.readStream.schema("v STRING").json("/stream/in/*.json.gz")  # expect: IOD015
al = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("/mnt/landing/*.csv.gz")  # expect: IOD015
)
