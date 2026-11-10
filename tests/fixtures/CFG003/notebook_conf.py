# Databricks notebook source
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().set("spark.executor.memory", "8g")  # expect: CFG003
conf.setAll([("spark.driver.memory", "4g")])  # expect: CFG003
SETTINGS = {"spark.executor.cores": "4", "spark.sql.shuffle.partitions": "64"}
s2 = SparkSession.builder.config(map=SETTINGS).getOrCreate()  # expect: CFG003
