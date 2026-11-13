---
title: Notebooks
description: How sparklint lints Databricks source notebooks and Jupyter .ipynb files - magics, non-Python cells, notebook globals and fixes.
---

# Notebooks

sparklint lints three kinds of files:

- plain `.py` files;
- **Databricks source notebooks**: `.py` files that start with
  `# Databricks notebook source`, with cells separated by `# COMMAND ----------` and
  `# MAGIC` lines (what Databricks writes when you export a notebook or keep it in a Git
  folder);
- **Jupyter notebooks** (`.ipynb`).

## What changes in a notebook

- **Notebook globals exist.** `spark`, `sc`, `sqlContext`, `dbutils` and `display` are
  known without being assigned, so `spark.table("t").collect()` is understood.
- **Non-Python cells are skipped**: cells that start with `%sql`, `%scala`, `%r`, `%sh`,
  `%md` or `%fs` in Databricks notebooks. In plain Jupyter notebooks `%sql <query>` and
  `%R <code>` on a cell's first line are line magics (ipython-sql / JupySQL, rpy2), so
  the rest of the cell is still linted as Python.
- **Magics and shell lines are not syntax errors**: `%pip install …`, `!ls`,
  `files = !ls`.
- **The last expression of a cell is displayed.** A bare `df` at the end of a cell counts
  as `display(df)` (see [ACT010](rules/ACT010.md)).
- **A syntax error stays in its cell.** The broken cell is reported as
  [SYN001](rules/SYN001.md) and skipped; the other cells are still linted and fixed.
- **Locations point at the cell.** JSON output for `.ipynb` files carries the cell
  number and the line inside the cell.

## The `notebook` option

```toml
[tool.sparklint]
notebook = "auto"   # default
```

- `auto`: Databricks source notebooks and `.ipynb` files are notebooks; other `.py`
  files are not.
- `on`: every file is a notebook. Use it for plain `.py` files that contain `%pip` or
  `!cmd` lines (for example notebooks exported without the Databricks header), or when
  your jobs rely on the notebook globals.
- `off`: no file is a notebook.

For jobs where the platform provides `spark` (or other names) but the files are not
notebooks, declare them instead:

```toml
[tool.sparklint]
known-spark-globals = ["spark", "sc:SparkContext"]
```

## Fixes

`--fix` and `--unsafe-fixes` rewrite notebooks in place: Databricks source notebooks as
text, `.ipynb` files cell by cell, keeping outputs and metadata. A cell with a syntax
error is left as it is.
