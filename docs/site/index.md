---
title: sparklint
description: A fast, Spark-aware linter for PySpark code and Databricks notebooks, written in Rust and installed with pip.
hide:
  - navigation
---

# sparklint

A fast linter for PySpark code and Databricks notebooks, written in Rust and installed
with pip.

sparklint finds the Spark habits that make jobs slow and expensive: unbounded
`collect()`s, `count()` used as an emptiness check or only for a log line, Python UDFs
where a built-in function exists, actions and writes inside loops, caches used once,
cartesian joins, `coalesce(1)` before a write, risky Spark configs, and more. On
Databricks these patterns turn straight into extra cluster hours.

```bash
pip install sparklint
sparklint check .
```

[Get started](getting-started.md){ .md-button .md-button--primary }
[Browse the rules](rules/index.md){ .md-button }

## Why another linter

- **It knows which values are Spark objects.** sparklint tracks Spark types (SparkSession,
  DataFrame, Column, GroupedData, Window, RDD, readers, writers, streams), so rules fire
  only on real Spark values. A `count()` on a Python list or a pandas DataFrame is not
  reported, and DataFrames passed into functions or returned from helpers in other
  modules are still recognised. [How it works](how-it-works.md).
- **Ruff does not do this.** Ruff declined PySpark rules because they need type
  information ([ruff#7272](https://github.com/astral-sh/ruff/issues/7272)). Name-based
  linters either report look-alikes such as `str.join` or only work when the variable is
  literally called `df`.
- **Notebooks are first-class.** `.py` files, Databricks source notebooks and Jupyter
  `.ipynb` files, with magics, non-Python cells and per-cell syntax errors handled.
  [Notebooks](notebooks.md).
- **Fast, no Spark needed.** A single native binary built on Ruff's Python parser. No
  JVM, no PySpark install, files linted in parallel.
- **Fixes.** Safe fixes with `--fix`, behaviour-changing ones with `--unsafe-fixes`, also
  inside notebooks.

## Example

```python title="etl.py"
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
orders = spark.read.parquet("/data/orders")
print(f"rows: {orders.count()}")
if orders.count() > 0:
    orders.write.mode("overwrite").saveAsTable("analytics.big_orders")
```

```text
$ sparklint check etl.py
etl.py:5:23: ACT007 orders.count() is computed only to be logged — a full job for a log line.
  |
4 | orders = spark.read.parquet("/data/orders")
5 | print(f"rows: {orders.count()}")
  |                       ^^^^^^^ ACT007
6 | if orders.count() > 0:
  |
  = help: Drop it, read numOutputRows from the Delta commit, or use df.observe() to compute the metric in the same job.

etl.py:6:4: ACT006 [*] orders.count() used as an emptiness check scans every row — use orders.isEmpty().
  |
5 | print(f"rows: {orders.count()}")
6 | if orders.count() > 0:
  |    ^^^^^^^^^^^^^^^^^^ ACT006
7 |     orders.write.mode("overwrite").saveAsTable("analytics.big_orders")
  |
  = fix: Replace with `not orders.isEmpty()`

Found 2 findings in 1 file checked (0.00s).
[*] 1 fixable with the `--fix` option.
```

## What sparklint does not tell you

sparklint reads code. It cannot tell you which finding costs real money, or how much:
that depends on how big the data is, how often the job runs and what the cluster costs.
Many Databricks savings are not in the code at all: cluster and warehouse sizing, idle
time, job compute versus all-purpose compute, spot instances, tables nobody reads.

[ClusterBite](https://clusterbite.com/?utm_source=docs&utm_medium=site&utm_campaign=sparklint)
finds those from your workspace's own usage data and measures what each change saved.

!!! note "Status: early (0.x)"
    Rules, codes and output may still change between minor versions. Reports of false
    positives are especially welcome on
    [GitHub](https://github.com/clusterbite/sparklint/issues).
