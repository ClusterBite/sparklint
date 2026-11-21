import mlflow
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# A function defined on two branches: each definition is followed.
if spark.conf.get("mode") == "fast":

    def predict(it):
        for pdf in it:
            m = mlflow.pyfunc.load_model("models:/fast/1")  # expect: UDF011
            yield pdf.assign(p=m.predict(pdf))

else:

    def predict(it):
        m = mlflow.pyfunc.load_model("models:/slow/1")
        for pdf in it:
            yield pdf.assign(p=m.predict(pdf))


out = df.mapInPandas(predict, schema="a long, p double")
