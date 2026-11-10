# Databricks notebook source
df = spark.table("t").cache()  # expect: PRF009
df.count()
df.collect()
