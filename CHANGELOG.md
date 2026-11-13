# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor versions may change rules and
output).

## [Unreleased]

- PRF003 now also reports `df.join(other, on=None)` and `df.join(other, None, ..)`: a `None`
  condition is the same as no condition.
- STY003 no longer offers to remove the `drop(..)` when the following `select` names a dropped
  column inside an expression string (`F.expr("id + 1")`, `F.col("t.id")`).
- PDS005 now also finds `pandas_on_Spark.compute.ops_on_diff_frames` turned on through
  `SparkConf().set(..)`, `SparkSession.builder.config(map={..})`, a SQL `SET` statement or a
  key held in a variable; turned back off later in the file, it is reported at info.
- DBX016 now also reports a `for` loop over the full table listing whose name test is an
  `elif` branch.

- DBX003 now also finds `input_file_name()` in a query passed as `sqlQuery=` or through a
  variable holding the text (`spark.sql(q)`, `df.filter(cond)`, `F.expr(cond)`), and no
  longer reports it inside an `EXPLAIN` query.
- Rules that read `spark.sql(..)` text (PRF022, PRF024, PRF027 and the others) no longer check the
  query inside an `EXPLAIN ...` statement, and its result counts as one row (ACT002 / ACT003):
  `EXPLAIN` only plans the query.
- COR007 now reports a discarded `df.hint(..)`: it returns a new DataFrame and has no effect on its own.
- ACT005 no longer reports polling loops: a `while` loop that sleeps between checks
  (`time.sleep(..)`), or one whose condition re-runs the same action until another job
  has written data (`while spark.table(t).count() == 0: ...`).
- ACT007 no longer reports the action that fills a Spark `Observation` (`df.observe(obs, ..)`
  then `observed.count()` before reading `obs.get`): in a function that holds an
  `Observation` (built there or an annotated parameter), or when the DataFrame was unpacked
  from the same call as a value whose `.get` is read afterwards.
- IOD001 no longer reports a read that is only asked for its schema (`spark.read.json(sample).schema`,
  `.columns`, `.dtypes`, `.printSchema()`): inferring the schema is the point there.
- ACT002, ACT003 and ACT004 no longer report catalog metadata commands
  (`spark.sql("SHOW TBLPROPERTIES t").toPandas()`, `for r in spark.sql("SHOW CATALOGS").collect()`,
  `DESCRIBE TABLE EXTENDED`) or frames built from driver-side data: their size follows the
  catalog or the driver, not the table. `SHOW PARTITIONS` and `DESCRIBE HISTORY` are still reported.
- Readers and writers built on several paths (`if`/`else` statements or an `if` expression,
  each path starting at `spark.read` / `df.write` / `df.writeStream`) are now analysed:
  rules that report a missing option (STM001 checkpointLocation, STM010
  maxOffsetsPerTrigger, IOD001/IOD016 schemas, IOD002/IOD017 JDBC partitioning) fire when
  no path sets it. Writers whose paths differ in
  anything but options, format or schema stay unanalysed. A builder followed through more
  than four names is analysed too.
- A value used with a pandas-only attribute (`x.iloc`, `x.shape`, `x.sort_values(..)`) is
  no longer guessed a Spark DataFrame from a DataFrame-only method on it
  (`x = get(); x.iloc[0]; x.withColumn(..)`), and a `*_df` name whose uses are all
  pandas-only (`result_df = fetch(); result_df.shape`) no longer counts as a DataFrame by
  its name, as for function parameters already.
- PRF008 counts a cached frame's direct uses with the same policy as its derived frames:
  a use inside an `assert` message, a `raise` statement or a `pytest.raises` /
  `assertRaises` block is not a reuse, so a cache read once normally and once in an error
  message is now reported.
- ACT007 no longer reports a discarded action directly inside `with pytest.raises(..)` /
  `pytest.warns(..)` / `self.assertRaises(..)` (and the `Regex` / `Warns` variants): like a
  `try:` probe, the test runs it for the error. PRF006 and PRF008 (reuse by derived frames)
  no longer count actions directly inside `pytest.raises` / `assertRaises` blocks (they are
  expected to fail).
- Rules that report a missing reader/writer option (STM001 checkpointLocation, STM010
  maxOffsetsPerTrigger, missing schemas and JDBC partitioning) no longer fire when the
  reader or writer is a function parameter, a helper's return value or a name bound on
  several paths: the option may be set where it was built.
- PRF008 no longer reports a cache read once by a frame that then runs two or more actions
  (`s = df.agg(..); s.show(); s.collect()`), or a bare `df.persist()` statement on a function
  parameter (it caches the caller's DataFrame).
- PRF006 counts each action once: a name rebound on one branch (`if c: df = df.join(x)`)
  or a frame used twice by one downstream frame (`a.union(a)`) no longer doubles the count,
  and actions inside an `assert` message or a `raise` statement are not counted.
- PRF005 no longer reports a window that is partitioned on some path only (`if keys: w =
  w.partitionBy(*keys)`, or an `if`/`else` expression choosing a partitioned spec), or a
  window passed in as a function parameter or returned by a helper annotated `-> WindowSpec`.
- COR007 no longer reports a discarded transformation directly inside a `try:` body with an
  `except` handler or a `with pytest.raises(..)` / `self.assertRaises(..)` block (a probe
  for the analysis error, e.g. a missing column).
- PRF010 no longer reports `repartition(n)` before a write when the frame is built from
  driver-side data (`spark.createDataFrame(pdf)`, a list of rows) or has one row, or when a
  later step calls a UDF (the repartition spreads the UDF work). IOD003 reports
  `coalesce(1)` on driver-side data at info level.
- PRF003 no longer reports a cross join with a one-row frame built from a function of an
  aggregate (`df.select(F.to_date(F.max("d")))`, `F.round(F.avg(..), 2)`), or a `join` on a
  function parameter used as a pandas frame (`df.iloc[..]`, `df.shape`).
- COR010 no longer reports a union whose column order is the same by construction: a
  frame unioned with itself (`df.union(df)`), or one side re-selected with the other's
  columns (`a.union(b.select(a.columns))`).
- A `withColumn(..)` call that no DataFrame accepts (a literal value such as `"int"`, a third
  argument, or keywords other than `colName` / `col`) no longer marks an unknown object as
  a DataFrame: dbldatagen-style builders reached through helper functions are no longer
  reported by PRF001 and other DataFrame rules.
- Rule docs for ACT001, ACT004 and ACT005 now describe what the rules actually do (ACT001's
  full list of action kinds; ACT005 does not follow actions into helper functions and has
  no `loop-literal-max` option).
- Rule docs for CFG012-CFG017 now quote every message the rules print (CFG012's
  `openCostInBytes` and CFG016's negative-timeout variants were missing) and say when a
  later write of the same key turns a finding into a note.
- COR001 no longer reports `helper(..) is None` / `is not None` when `helper` is your own
  function annotated to return a Column (or `Column | None`): that is the normal check for
  an optional Column. `F.col(..) is None` and other pyspark calls are still reported.
- Documentation site at https://clusterbite.github.io/sparklint/: getting started,
  configuration, notebooks, CI, one page per rule and the cylint mapping.
- `sparklint rules --implemented` help and the developer docs no longer mention "planned"
  rules: every rule in this build is implemented.
- `notebook = "on"` now also parses plain `.py` files as notebooks, so `%pip` / `!cmd` /
  `x = !cmd` lines are no longer reported as syntax errors (SYN001) and the rest of the file
  is linted.
- CFG003 no longer treats IPython's `display(..)` before `SparkSession.builder` in a plain
  Jupyter notebook as a sign that the session already runs; it counts only when the
  notebook also names Databricks storage paths.
- `sparklint check` with a path that does not exist reports it as an error and no longer
  ends with "All checks passed!"; the summary counts the errors (exit code 2 as before).
- `sparklint check -` lints one file read from standard input; `--stdin-filename` names it
  (config, `.ipynb`, output). With `--fix` the fixed source is written to stdout and the
  report to stderr.
- Building from source needs Rust 1.97 or newer (the stated minimum was 1.88, which the
  parser crates do not support); CI now builds with the minimum version.
- DataFrames made by dbldatagen (`DataGenerator(..).build()`), the Databricks Feature
  Engineering / Feature Store APIs (`read_table`, `score_batch`, `load_df`) and tempo
  (`TSDF(..).df`) are now recognised, so rules such as `display(df)` (ACT010) and
  `toPandas()` (ACT003) see them; dbldatagen generators are no longer taken for DataFrames.
- `--fix` / `--unsafe-fixes` now fix Jupyter notebooks (`.ipynb`): the fixed code is written
  back into the cells. A notebook with a syntax error in one cell is still fixed in its
  other cells (Databricks source notebooks too); the broken cell is left as it is.
- In plain Jupyter notebooks, `%sql <query>` and `%R <code>` on a cell's first line are
  treated as line magics (ipython-sql / JupySQL, rpy2): the rest of the cell is linted as
  Python instead of being skipped. Databricks notebooks keep treating them as SQL / R cells.
- `collect()[0]` (ACT009) and other driver-size checks treat more one-row frames as small:
  `select([..aggregates..])`, `selectExpr("min(v) AS lo", "max(v) AS hi")` and
  `groupBy().agg(..)` without keys. A SQL `SELECT max(x) OVER (..)` is no longer taken for
  one row.
- Rule docs (`sparklint rule <CODE>` and the rule pages) now quote the message each rule
  actually prints; COR003's message, written with backticks, was blank there.

## [0.1.0] - 2026-11-10

First public release.

- `sparklint check` for `.py` files, Databricks source notebooks and `.ipynb` notebooks,
  built on Ruff's Python parser.
- Notebook-aware: `%magic` and `!shell` lines and non-Python cells are skipped, the last
  expression of a cell counts as displayed, and a syntax error in one cell does not hide
  findings in the others.
- A Spark type oracle (SparkSession, DataFrame, Column, GroupedData, Window, RDD,
  readers/writers, streaming) with cross-file function summaries, so rules fire only on
  known Spark receivers.
- 91 rules across the ACT, PRF, UDF, IOD, CFG, STM, COR, DBX, PDS and STY families,
  plus SYN001 for files that do not parse.
- Safe and unsafe fixes (`--fix`, `--unsafe-fixes`).
- Output formats: text, json, github, sarif; `--statistics`; exit codes 0/1/2.
- Configuration in `[tool.sparklint]` (`pyproject.toml`) or `sparklint.toml`: selectors,
  tags, presets (default, review, strict, cylint-compat, serverless), per-file ignores,
  severities, rule options.
- Suppressions: `# noqa`, `# sparklint: ignore[...]`, `# sparklint: file-ignore[...]`,
  and cylint's `# cy:ignore` (mapped to sparklint codes).
- `sparklint rules` and `sparklint rule <CODE>` print the rule catalog.
