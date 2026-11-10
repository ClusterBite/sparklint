import socket
import urllib.request

import boto3
import httpx
import psycopg2
import redis
import requests
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from requests import post
from sqlalchemy import create_engine

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")


# Plain UDF: one HTTP request per row.
@F.udf("string")
def enrich(x):
    return requests.get(f"https://api/x/{x}").text  # expect: UDF012


@F.udf(returnType="string")
def enrich2(x):
    with requests.Session() as s:  # expect: UDF012
        return s.get(f"https://api/{x}").text


def lookup(x):
    r = redis.Redis(host="cache")  # expect: UDF012
    return r.get(x)


lookup_udf = F.udf(lookup, "string")
lam_udf = F.udf(lambda x: httpx.get(f"https://api/{x}").text, "string")  # expect: UDF012
spark.udf.register("geo", lambda ip: urllib.request.urlopen(ip).read(), "string")  # expect: UDF012


# DataFrame.foreach: per row.
def send(row):
    post("https://hook", json=row.asDict())  # expect: UDF012


df.foreach(send)


def save(row):
    conn = psycopg2.connect("dbname=x")  # expect: UDF012
    conn.cursor().execute("insert ...", (row.a,))


df.foreach(save)

# RDD row-wise transformations.
rdd = df.rdd
rdd.map(lambda r: boto3.client("s3").head_object(Bucket="b", Key=r.k))  # expect: UDF012


def fetch(r):
    return MongoClient("mongodb://h").db.c.find_one({"_id": r})  # expect: UDF012


rdd.flatMap(fetch).count()
rdd.foreach(lambda r: socket.socket().connect(("h", 1)))  # expect: UDF012


# Same-file helper (not cached) creates the engine.
def engine():
    return create_engine("postgresql://h/db")


def write_row(r):
    engine().execute("insert ...")  # expect: UDF012


rdd.foreach(write_row)


# Streaming foreach writer: process() runs per row.
class Writer:
    def open(self, partition_id, epoch_id):
        return True

    def process(self, row):
        requests.post("https://hook", json=row.asDict())  # expect: UDF012

    def close(self, error):
        pass


stream = spark.readStream.table("src")
stream.writeStream.foreach(Writer()).start()


def per_row(row):
    requests.put("https://hook", data=str(row))  # expect: UDF012


stream.writeStream.foreach(per_row).start()


# Spark UDF with an import inside the body.
@F.udf("string")
def geo(ip):
    import requests as rq

    return rq.get(f"https://geo/{ip}").text  # expect: UDF012
