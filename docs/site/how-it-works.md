---
title: How it works
description: sparklint's Spark-aware type inference - why it reports fewer false positives than name-based PySpark linters.
---

# How it works

## The problem with name-based linting

Many PySpark mistakes are method calls that look harmless: `count()`, `collect()`,
`filter()`, `join()`, `first()`. The same names exist on Python lists, strings, sets and
pandas DataFrames. A linter that only sees names has two bad options: report every
`.count()` (noise) or only the ones on a variable literally called `df` (misses most real
code, where DataFrames are passed into functions and returned from helpers).

Ruff declined PySpark rules for exactly this reason: they "would need type information to
determine if a variable is a DataFrame, RDD or transformation reliably"
([ruff#7272](https://github.com/astral-sh/ruff/issues/7272)).

## A Spark-only type checker

sparklint is not a full Python type checker. It models the Spark world, and only enough
Python to place Spark types:

- **Where Spark values come from**: `SparkSession.builder…getOrCreate()`,
  `SparkSession.getActiveSession()`, `DatabricksSession`, the notebook globals,
  `spark.read.*`, `spark.table`, `spark.sql`, `createDataFrame`, `readStream`, Delta
  tables, pandas-on-Spark conversions.
- **Imports**: `from pyspark.sql import functions as F`, `import pyspark.sql.functions as
  sf`, `from pyspark.sql.functions import col` all resolve to the same qualified names.
  Rules match qualified names, never aliases.
- **Annotations**: parameters and returns annotated `DataFrame`, `"DataFrame"`,
  `Optional[DataFrame]`, `Column`, `SparkSession` and so on.
- **The PySpark API**: a table of every method's return type per receiver, generated
  from PySpark itself (`DataFrame.groupBy → GroupedData`, `GroupedData.agg → DataFrame`,
  `DataFrame.write → DataFrameWriter`).
- **Dataflow**: assignments, branches, loops, comprehensions, `with` blocks and
  attributes on `self`.
- **Across files**: a first pass summarises every function in the project (what it
  returns, which parameters it uses as DataFrames), so a helper in another module that
  returns a DataFrame is understood where it is called.
- **Other libraries**: DataFrames made by common Databricks libraries (dbldatagen,
  Databricks Feature Engineering, tempo) are recognised; pandas, numpy and polars values are
  known *not* to be Spark.

Each type comes with a confidence: *certain* (an annotation or a known origin) or
*likely* (a name heuristic such as `orders_df`, which you can turn off with
`heuristics = false`). Methods whose names are ambiguous (`count`, `filter`, `collect`,
`first`, `join`, `select`, `show`, `limit`, `distinct`) are only reported on a known Spark
receiver.

## Facts about DataFrames

On top of types, sparklint tracks facts that decide whether something is a problem:

- **bounded**: known to have at most *n* rows (`limit(n)`, a global `agg`,
  `createDataFrame` of a literal list). `collect()` on these is fine.
- **reduced**: filtered or aggregated, but not bounded. Still reported, at lower
  severity.
- **cached**, **streaming**, the **source** (format, table, path) and known columns.

## SQL inside Python

`spark.sql("...")` strings, f-strings and `F.expr(...)` text are read too: the leading
keyword tells a query from DDL/DML, and rules look at `LIMIT`, joins, `SET` statements,
table properties and so on.

## What it does not do

- It does not run your code or connect to a cluster.
- It is not a general Python type checker: use pyright, mypy or ty for that, and ruff
  for general Python linting. sparklint is meant to run next to them.
- It cannot know how big your data is, how often a job runs or what it costs. A finding
  is a place to look, not a measured cost.

The full design notes are in
[docs/DESIGN.md](https://github.com/clusterbite/sparklint/blob/main/docs/DESIGN.md).
