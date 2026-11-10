# Databricks notebook source
from pyspark.sql import SparkSession

s2 = SparkSession.builder.config("spark.executor.cores", "4").getOrCreate()  # expect: CFG003
spark.conf.set("spark.sql.shuffle.partitions", "64")
