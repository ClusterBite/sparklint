# Databricks notebook source
rdd = sc.parallelize([1, 2])  # expect: DBX001
sqlContext.sql("select 1")  # expect: DBX001
