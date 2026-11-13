# sparklint — Rules Catalog

sparklint is a Rust, pip-installable PySpark linter (ruff-style CLI, `# noqa`-style suppressions, safe/unsafe autofixes). Its differentiator is a small **Spark-aware type inference**: it knows which names hold a `SparkSession`, `DataFrame`, `Column`, `GroupedData`, `WindowSpec`, `RDD`, `DataFrameWriter`, `StreamingQuery` and so on. Ruff declined PySpark rules because they need type information ([ruff#7272](https://github.com/astral-sh/ruff/issues/7272)). Name-based linters (cylint, pyspark-antipattern) either false-positive on `str.join` / `set.union`, or they only work when the variable is literally called `spark` / `df`.

Priorities:
1. **Cost / performance** (DBUs, driver OOMs, shuffles, recomputation, small files, always-on streams).
2. **Correctness** (silent wrong results first, then runtime failures).
3. **Platform compatibility** (Unity Catalog, standard/shared access mode, serverless, Spark Connect, Spark 4). These matter because they decide whether a team can move to cheaper compute.
4. **Style** (all off by default).

Credits: the `CY` codes and the mapping in §4 refer to cylint (Apache-2.0). The other tags in the rule headers name where an idea was seen first: `PA` = pyspark-antipattern, `palantir` = the Palantir PySpark style guide, `mrpowers` = the MrPowers PySpark style guide, `astrumu` = pylint-pyspark, `ruff#7272` = the Ruff issue that asked for PySpark rules. Everything else comes from the Spark and Databricks documentation.

---

## 1. Code scheme

Codes look like ruff's: a **3-letter family prefix plus 3 digits**. Within a family, lower numbers are higher value. Codes are never reused.

| Prefix | Family | Focus | Count |
|---|---|---|---|
| `ACT` | Actions & driver | Spark actions, driver collection, actions in loops or logging | 12 |
| `PRF` | Plan & shuffle performance | Loops that grow plans, joins, windows, caching, partitioning, redundant shuffles | 22 |
| `UDF` | UDFs | Python/pandas UDF cost and UDF correctness | 7 |
| `IOD` | I/O & Delta | Reads, writes, JDBC, MERGE, OPTIMIZE/VACUUM, partitioning, overwrite | 11 |
| `CFG` | Spark config | `spark.conf.set` misuse (AQE, broadcast, static confs, legacy flags) | 19 |
| `STM` | Structured Streaming | Checkpoints, triggers, watermarks, foreachBatch | 4 |
| `COR` | Correctness | Column/Python operator traps, nondeterminism, joins, nulls, regex, dates | 6 |
| `DBX` | Databricks platform & compatibility | UC, standard/shared access mode, serverless, Spark Connect, notebooks | 5 |
| `PDS` | pandas-on-Spark | `pyspark.pandas` traps | 2 |
| `STY` | Style & naming | Conventions (all off by default, apart from cylint parity) | 3 |

Selection works like ruff: `select = ["ACT", "PRF", "COR001"]`, `ignore = ["PRF022"]`, and `extend-select`. Rules also carry **tags**, so concerns that cut across families can be selected with `select = ["tag:serverless"]`:
- `cost`: directly burns DBUs or driver time. This is the main section of a cost review.
- `connect`, `serverless`, `uc-standard`: breaks or misbehaves on that compute.
- `spark4`: deprecated or removed in Spark 4.
- `notebook`: only relevant in notebook sources.

**Presets** (`preset = "..."`):
- `default`: rules marked *on* below.
- `review` (audit): every non-style rule, including the `ACT001` action inventory and all info-level cost rules.
- `serverless` / `uc-migration`: adds every `DBX` rule and promotes their severity to error.
- `strict`: everything.

**Target profile** (`[tool.sparklint] target = "classic" | "dedicated" | "standard" | "serverless" | "connect"`, `spark-version = "3.5"`, `dbr = "15.4"`) gates or changes severity for version- and compute-specific rules, for example `ACT006`'s autofix needs Spark ≥ 3.3 and `DBX001` is an error on `serverless`/`connect`.

**Severity:** `error` = almost certainly a bug or a hard failure on the target. `warning` = real cost or risk; fix unless justified. `info` = worth knowing; shown in review reports.

**Suppressions:** `# noqa: PRF001`, `# sparklint: ignore[PRF001,ACT002]`, and file-level `# sparklint: file-ignore[...]`. The cylint form `# cy:ignore CY001` is honoured through the mapping table in §4, so existing cylint suppressions keep working.

**Autofix classes** (as in ruff):
- `safe`: semantics preserved; applied with `--fix`.
- `unsafe`: likely equivalent but can change behaviour or needs a version; applied with `--unsafe-fixes`.
- `none`: suggestion text only.

---

## 2. Type-inference contract assumed by the rules

**Inferred types:** `SparkSession`, `SparkContext`, `DataFrame` (fact `streaming: bool`), `Column`, `GroupedData`, `WindowSpec`, `DataFrameReader` / `DataStreamReader`, `DataFrameWriter` / `DataFrameWriterV2` / `DataStreamWriter`, `StreamingQuery`, `RDD`, `Row`, `list[Row]`, `pandas.DataFrame`, `ps.DataFrame` (pandas-on-Spark), `DeltaTable` / `DeltaMergeBuilder`, `UserDefinedFunction`, `Broadcast`, `Accumulator`, `str` (with constant folding), and `Unknown`.

**Where types come from:**
- Session origins: `SparkSession.builder…getOrCreate()`, `SparkSession.getActiveSession()`, `DatabricksSession.builder…`, and the notebook globals (`spark`, `sc`, `sqlContext`, `dbutils`, `display`) when the file is a Databricks notebook source (`# Databricks notebook source`) or a `.ipynb` file.
- DataFrame origins: `spark.read.*`, `.table`, `.sql`, `.createDataFrame`, `.range`, `readStream`, `DeltaTable.toDF()`, and `ps.DataFrame.to_spark()`.
- Parameter and return annotations, including string annotations and `TYPE_CHECKING` imports.
- A table of method return types per receiver type, for example `DataFrame.groupBy → GroupedData`, `GroupedData.agg → DataFrame`, `DataFrame.write → DataFrameWriter`, `Column.over → Column`.
- `functions.*` signatures: every `F.x` returns `Column` unless the function table says otherwise.
- Assignment propagation and aliasing.
- **Intra-project function summaries**: return types, plus which parameters are only read.

**Facts attached to DataFrame values:**
- `bounded(n)`: known to have at most `n` rows. Set by `limit(n)`, `agg` with no `groupBy`, `summary`/`describe`, `createDataFrame(<literal list>)`, and `spark.range(<small literal>)`.
- `reduced`: has been through `filter`, `where`, `groupBy().agg`, `distinct`, or `sample`. These are *not* bounded. cylint's `FILTER_METHODS` treats them as bounded, which is a false-negative gap.
- `cached` (with `StorageLevel`), `streaming`.
- `source` (format, path or table name, `delta: bool` when the project or the call proves it).
- `known_columns` (from `select` literals, `createDataFrame` schemas, DDL strings in the project).
- `single_row`.

**Scopes and control flow:** rules see function and lambda scopes, `for`/`while`/comprehension loop bodies, branch-aware def-use (a reassignment kills the earlier value), and notebook cells (`# COMMAND ----------` boundaries are kept for reporting, and `# MAGIC %sql` cells are linted as SQL strings).

**Type-info levels used in the entries:**
- **T0**: syntax plus import resolution only (`F` resolves to `pyspark.sql.functions`).
- **T1**: receiver type of a call.
- **T2**: intra-procedural dataflow (def-use chains, loop and branch context, facts).
- **T3**: cross-file (project function summaries, module constants, table metadata elsewhere in the project).

**cylint gaps this design fixes** (from reading its `tracker.py` and the rules):
- (a) Sessions are recognised only by the names `spark`, `sqlContext` or `sc`. There is no `SparkSession.builder`, `getActiveSession`, parameter annotation, or function-return propagation, so any DataFrame passed into a function is invisible.
- (b) The tracker is module-global (`ast.walk`), so the same name in two functions is conflated and reassignments never kill earlier facts.
- (c) "Use counts" (CY005, CY014) are textual `Name` occurrences after a line number. They are wrong in loops and branches, and they count `del df` and attribute reads.
- (d) `filter`, `agg` and `groupBy` are treated as limiting, which hides CY001/CY006 on huge filtered sets.
- (e) About half the rules don't consult the tracker at all: CY008, CY013, CY016, CY017 (only matches the name `Window`), CY018 (only the receiver `spark.read`), CY002 and CY033. They match method names only.
- (f) CY021 decides a call is logging if the receiver name contains the substring `log` (so `catalog.info()` counts).
- (g) CY003 flags any `.withColumn` in a loop when the result is reassigned, even when the receiver is not a DataFrame.
- (h) No cross-file analysis.

---

## 3. Rule index

Columns: Sev = error/warning/info (E/W/I). On = in the `default` preset. TI = type-info level. Fix = autofix class.

| Code | Name | Sev | On | TI | Fix | Tags |
|---|---|---|---|---|---|---|
| ACT001 | spark-action | I | off | T1 | none | cost |
| ACT002 | collect-unbounded | W | on | T2 | unsafe | cost |
| ACT003 | topandas-unbounded | W | on | T2 | none | cost |
| ACT004 | driver-row-iteration | W | on | T1 | none | cost |
| ACT005 | action-in-loop | W | on | T2 | none | cost |
| ACT006 | count-for-emptiness | W | on | T1 | safe/unsafe | cost |
| ACT007 | action-only-for-logging | W | on | T1 | none | cost |
| ACT008 | collect-to-filter-roundtrip | W | on | T2 | none | cost |
| ACT009 | collect-for-driver-compute | W | on | T2 | unsafe | cost |
| ACT010 | debug-action-left-in | W | on | T1 | unsafe | cost, notebook |
| ACT011 | rdd-collect | W | on | T1 | unsafe | cost, connect |
| ACT015 | approx-quantile-exact | W | on | T1 | none | cost |
| PRF001 | withcolumn-in-loop | W | on | T2 | unsafe | cost |
| PRF002 | column-op-in-loop | W | on | T2 | unsafe | cost |
| PRF003 | cartesian-join | E | on | T1 | none | cost |
| PRF004 | non-equi-join | W | on | T1 | none | cost |
| PRF005 | global-window | W | on | T1 | none | cost |
| PRF006 | recomputed-lineage | W | on | T2 | none | cost |
| PRF008 | cache-single-use | W | on | T2 | unsafe | cost |
| PRF009 | cache-never-unpersisted | I | on | T2 | none | cost |
| PRF010 | repartition-before-write | I | on | T1 | unsafe | cost |
| PRF012 | explode-before-filter | W | on | T1 | none | cost |
| PRF015 | collect-dedup-redundant | W | on | T0 | safe | cost |
| PRF022 | sql-select-star | I | off | T0 | none | cost |
| PRF023 | pivot-without-values | W | on | T1 | none | cost |
| PRF024 | cube-many-columns | W | on | T1 | none | cost |
| PRF025 | not-in-subquery | W | on | T1 | none | cost |
| PRF026 | many-distinct-aggregates | W | on | T1 | none | cost |
| PRF027 | order-by-rand | W | on | T1 | none | cost |
| PRF029 | replicated-storage-level | W | on | T1 | none | cost |
| PRF031 | running-collect-list-window | W | on | T1 | none | cost |
| PRF032 | or-join-condition | W | on | T1 | none | cost |
| PRF035 | keyless-grouped-pandas | W | on | T1 | none | cost |
| PRF036 | forced-nested-loop-join-hint | W | on | T1 | none | cost |
| UDF001 | udf-has-builtin | W | on | T0 | unsafe | cost |
| UDF002 | udf-in-filter | W | on | T1 | none | cost |
| UDF006 | udf-missing-return-type | W | on | T0 | none | correctness |
| UDF011 | pandas-udf-per-batch-init | W | on | T1 | none | cost |
| UDF012 | per-row-external-connection | W | on | T1 | none | cost |
| UDF013 | udf-whole-row-struct | I | on | T1 | none | cost |
| UDF014 | udf-in-join-condition | W | on | T1 | none | cost |
| IOD001 | read-schema-inference | W | on | T1 | none | cost |
| IOD002 | jdbc-read-unpartitioned | W | on | T1 | none | cost |
| IOD003 | coalesce-one-before-write | W | on | T1 | unsafe | cost |
| IOD004 | insert-into-positional | W | on | T1 | unsafe | correctness |
| IOD006 | write-in-loop | W | on | T2 | none | cost |
| IOD015 | unsplittable-compressed-read | I | on | T1 | none | cost |
| IOD016 | json-schema-inference-via-rdd | W | on | T2 | none | cost, connect |
| IOD017 | jdbc-write-unbounded-connections | I | on | T1 | none | cost |
| IOD018 | repair-table-in-job | I | on | T1 | none | cost |
| IOD019 | small-max-records-per-file | I | on | T1 | none | cost |
| IOD020 | zorder-many-columns | I | on | T1 | none | cost |
| CFG001 | aqe-disabled | W | on | T0 | none | cost |
| CFG002 | broadcast-disabled | W | on | T0 | none | cost |
| CFG003 | static-conf-at-runtime | E | on | T0 | none | connect, serverless |
| CFG007 | broadcast-threshold-too-high | W | on | T0 | none | cost |
| CFG008 | predicate-pushdown-disabled | W | on | T1 | none | cost |
| CFG009 | delta-file-compaction-disabled | I | on | T0 | none | cost |
| CFG010 | disk-cache-disabled | I | on | T0 | none | cost |
| CFG011 | arrow-batch-size-extreme | W | on | T0 | none | cost |
| CFG012 | file-partition-size-extreme | W | on | T0 | none | cost |
| CFG013 | aqe-min-partition-num-set | I | on | T0 | none | cost |
| CFG014 | engine-acceleration-disabled | W | on | T0 | none | cost |
| CFG015 | dynamic-pruning-disabled | W | on | T0 | none | cost |
| CFG016 | broadcast-timeout-raised | I | on | T0 | none | cost |
| CFG017 | driver-result-limit-removed | W | on | T0 | none | cost |
| CFG018 | io-compression-disabled | W | on | T0 | none | cost |
| CFG019 | deletion-vectors-disabled | W | on | T0 | none | cost |
| CFG020 | file-statistics-disabled | W | on | T0 | none | cost |
| CFG021 | optimizer-rules-excluded | I | on | T0 | none | cost |
| CFG022 | delta-target-file-size-small | W | on | T0 | none | cost |
| STM001 | stream-missing-checkpoint | E | on | T1 | none | correctness |
| STM002 | trigger-once-deprecated | W | on | T1 | safe | spark4, serverless |
| STM010 | kafka-stream-uncapped-batch | W | on | T1 | none | cost |
| STM011 | heap-state-store-explicit | I | on | T0 | none | cost |
| COR001 | column-eq-none | E | on | T1 | safe | correctness |
| COR002 | python-bool-op-on-column | E | on | T1 | safe | correctness |
| COR003 | bitwise-comparison-precedence | E | on | T1 | safe | correctness |
| COR007 | discarded-transformation | E | on | T1 | none | correctness |
| COR010 | union-by-position | W | on | T1 | unsafe | correctness |
| COR016 | regex-escape | W | on | T0 | safe | correctness |
| DBX001 | spark-context-access | W | on | T1 | unsafe | connect, serverless, uc-standard |
| DBX002 | rdd-access | W | on | T1 | unsafe | connect, serverless, uc-standard |
| DBX003 | input-file-name | W | on | T0 | unsafe | uc-standard, serverless |
| DBX016 | list-tables-existence-check | I | on | T1 | none | cost |
| DBX017 | pipeline-stream-fanout | I | on | T1 | none | cost, pipelines |
| PDS005 | pandas-on-spark-ops-on-diff-frames | W | on | T0 | none | cost |
| PDS006 | pandas-on-spark-single-partition-op | I | on | T1 | none | cost |
| STY001 | functions-alias | I | on* | T0 | safe | style |
| STY002 | join-missing-how | I | on* | T1 | safe | style |
| STY003 | redundant-drop-before-select | I | on* | T1 | safe | style |

`on*`: on only in the `cylint-compat` preset, which reproduces cylint's defaults. Off in `default`.

---

## 4. cylint → sparklint mapping

All 29 rules cylint implements are covered. cylint has no CY019, CY022–024 or CY026–030 (gaps in its numbering).

| cylint | cylint name | sparklint | Mapping | What changes |
|---|---|---|---|---|
| CY001 | collect-unfiltered | ACT002 | 1:1 | `reduced` (filter/agg) no longer silences it; it is reported as info with the size unknown. `bounded` silences it. |
| CY002 | udf builtin exists | UDF001 | 1:1 | Adds lambdas, `def` bodies, `@udf`, `spark.udf.register`, and more builtins (regex, date, coalesce). |
| CY003 | withcolumn-loop | PRF001 | 1:1 | Receiver must be a DataFrame (T1). Also catches comprehensions and `functools.reduce`. |
| CY004 | select-star | PRF022 | 1:1 | Off by default. Fires only when the `*` result flows to a write, cache or collect. |
| CY005 | cache single use | PRF008 | 1:1 | Real def-use count across branches and loops (use inside a loop = many). |
| CY006 | topandas-unfiltered | ACT003 | 1:1 | Same as ACT002, plus pandas-on-Spark `to_pandas()`. |
| CY007 | cross-join | PRF003 | 1:1 | Skips crossJoin with a `bounded(1)` side (scalar broadcast idiom). |
| CY008 | repartition before write | PRF010 | 1:1 | Kept: the column-subset-of-`partitionBy` exemption. New: exemption when count ≥ the configured file-count target. |
| CY009 | udf in filter | UDF002 | 1:1 | Resolves UDF objects across files (T3). |
| CY010 | join without how | STY002 | 1:1 | Moved to style, off in `default`. |
| CY011 | loop column transform | PRF002 | 1:1 | Adds `select` / `withColumnsRenamed` / `toDF` in loops. |
| CY012 | debug methods | ACT010 | 1:1 | Plus `display()` global, console/memory sinks. Notebook-aware. |
| CY013 | coalesce(1) write | IOD003 | 1:1 | Typed writer chain, includes `writeTo`. |
| CY014 | repeated actions | PRF006 | 1:1 | Path-sensitive across loops and functions. Counts writes as actions. Exempts when the source is `bounded`. |
| CY015 | non-equi join | PRF004 | 1:1 | Recognises range joins and suggests a bin/range-join hint. |
| CY016 | invalid escape | COR016 | 1:1, widened | Also catches *valid but wrong* escapes in regex args (`"\b"` = backspace). |
| CY017 | window orderBy w/o partitionBy | PRF005 | 1:1 | Also `Window.partitionBy()` with no args, and imported aliases (`W`). |
| CY018 | schema inference | IOD001 | 1:1 | Plus `inferSchema=True`, `mergeSchema`, `samplingRatio`, JSON multiLine. |
| CY020 | count emptiness | ACT006 | 1:1 | Plus truthiness `if df.count():` (PA D009), with a safe fix to `isEmpty()`. |
| CY021 | count-log | ACT007 | 1:1, widened | Any action (count/collect/first/show), not just `count`. Typed logger detection. |
| CY025 | missing unpersist | PRF009 | 1:1 | Info. Also suggests `with df.cache():`-style context helpers. |
| CY031 | collect iteration | ACT004 | 1:1 | Plus `toLocalIterator`, `toPandas().iterrows/itertuples`, `.rdd.collect()`. |
| CY032 | drop before select | STY003 | 1:1 | Moved to style (Catalyst prunes anyway). |
| CY033 | array_distinct(collect_list) | PRF015 | 1:1, widened | Plus `array_distinct(collect_set)`. |
| CY034 | rdd collect | ACT011 | 1:1 | — |
| CY035 | union by name | COR010 | 1:1 | Uses known schemas to downgrade when column lists provably match. |
| CY036 | insertInto | IOD004 | 1:1 | Separate message for `overwrite=True` (full-table vs dynamic overwrite). |
| CY037 | functions alias | STY001 | 1:1 | Accepts `F` or `sf` (Spark docs use `sf`); configurable. |
| CY038 | explode before filter | PRF012 | 1:1 | Only when the filter does not reference the exploded output. |

---

## 5. Rule details

Entry format:
- Header: **category · severity · default · type-info · autofix · provenance**.
- *Msg* = the one-line diagnostic. *Why* = the impact. *Detect* = the exact conditions. *FP* = false-positive risks and mitigations. *Fix* = the autofix. *Ex* = bad → good.

### ACT — Actions & driver

#### ACT001 `spark-action`
perf · info · off (on in `review`) · T1 · none · ruff#7272, new
- **Msg:** `Spark action: df.count() triggers a job (kind: job).`
- **Why:** In a cost review the first question is where the jobs are. This rule inventories every call that triggers a job or eager work; the message names its `kind`: `job` (DataFrame count/first/isEmpty/foreach/foreachPartition; RDD count/reduce/fold/aggregate/sum/max/min/mean/stats/countByKey/countByValue/treeReduce/treeAggregate), `driver-collect` (collect/toPandas/toArrow/take/head/tail/toLocalIterator; RDD top/takeOrdered/takeSample/collectAsMap; pandas-on-Spark to_pandas/to_numpy), `write` (DataFrameWriter save/saveAsTable/insertInto/parquet/csv/json/orc/text/jdbc/xml, `writeTo(..)` create/replace/createOrReplace/append/overwrite/overwritePartitions, `mergeInto(..).merge()`, RDD saveAs*File), `eager-sql` (`spark.sql` whose text starts with a DDL/DML keyword such as CREATE, INSERT, MERGE, DELETE, UPDATE, OPTIMIZE, VACUUM, CACHE), `stream-start` (`writeStream` start/toTable), `checkpoint`, and `display` (`df.show()`, and Databricks `display(df)`).
- **Detect:** A call whose resolved receiver type plus method is in the action table above. `spark.sql(<const/f-string>)` is classified by its leading SQL keyword. Not in the table (not reported): Delta Lake `DeltaTable` builder calls (`merge(..).execute()`, `optimize()`, `vacuum()`), schema-inferring reads, and schema lookups such as `df.columns`.
- **FP:** None by construction (it is an inventory). An action inside a function passed to `foreachBatch` is reported like any other.
- **Ex:** `n = df.count()` → (reported, no change)

#### ACT002 `collect-unbounded`
perf · warning · on · T2 · unsafe · CY001, PA D001
- **Msg:** `.collect() on a DataFrame with no known row bound pulls everything to the driver.`
- **Why:** The most common cause of driver OOM. It also serialises the job's whole output through the driver, so the cluster sits idle.
- **Detect:** `x.collect()` where `type(x)=DataFrame` and not `bounded`. If `reduced`, downgrade to info with the message suffix "(filtered/aggregated — size unknown)".
- **FP:**
  - Lookup tables that are small but not provably bounded: add an explicit `.limit()`, or suppress the line with `# noqa: ACT002`.
  - Driver-sized frames are not reported: catalog metadata commands (`spark.sql("SHOW TABLES")`, `SHOW TBLPROPERTIES`, `SHOW COLUMNS`, `DESCRIBE [TABLE|EXTENDED] t`; not `SHOW PARTITIONS` or `DESCRIBE HISTORY`, which grow with the data) and frames built from driver data (`spark.createDataFrame(pandas_df)`), also after row-wise steps. Joined with a table, they are reported again.
  - Already inside a `bounded` function summary (T3).
- **Fix (unsafe):** `.collect()` → `.limit(N).collect()`, with `N` from config. Changes semantics, so unsafe.
- **Ex:** `rows = spark.table("sales").collect()` → `rows = spark.table("sales").limit(1000).collect()` or keep the work in Spark.

#### ACT003 `topandas-unbounded`
perf · warning · on · T2 · none · CY006, PA P001
- **Msg:** `.toPandas() on an unbounded DataFrame materialises it in driver memory (x2–3 for pandas).`
- **Why:** Same as ACT002, plus pandas memory overhead. Also covers pandas-on-Spark `psdf.to_pandas()` / `.to_numpy()`.
- **Detect:** `toPandas()` on a DataFrame, or `to_pandas()`/`to_numpy()` on `ps.DataFrame`/`ps.Series`, when not `bounded`. `reduced` → info.
- **FP:** Same as ACT002 (driver-sized frames such as `spark.sql("DESCRIBE TABLE EXTENDED t").toPandas()` are not reported). ML feature-matrix code is a common legitimate use; exempt it by path glob (`ml/**`).
- **Ex:** `pdf = df.toPandas()` → `pdf = df.select(cols).limit(n).toPandas()`, or `df.write…` and read the result elsewhere.

#### ACT004 `driver-row-iteration`
perf · warning · on · T1 · none · CY031
- **Msg:** `Iterating rows on the driver (for row in df.collect()) — express this as a DataFrame transformation.`
- **Why:** Single-threaded Python over the whole dataset while the executors are idle and billed.
- **Detect:** The iterable of a `for` statement or comprehension is `collect()`, `toLocalIterator()`, `toPandas().iterrows()/itertuples()/items()`, `.rdd.collect()`, or a name bound to one of those (T2). The receiver must be a DataFrame or RDD. When the loop body also runs a Spark action or write (the N+1 pattern, see ACT005), the message says so.
- **FP:**
  - Iterating a `bounded` frame (e.g. a config table with `limit`) → no finding.
  - Iterating a driver-sized frame (catalog metadata such as `for r in spark.sql("SHOW CATALOGS").collect()`, or a frame built from driver data) → no finding.
  - Iterating `df.columns` or `df.dtypes` is not row iteration and is not matched.
- **Ex:** `for r in df.collect(): out.append(r.a*2)` → `df.select((F.col("a")*2).alias("a2"))`

#### ACT005 `action-in-loop`
perf · warning · on · T2 · none · new
- **Msg:** `Spark action df.count() inside a loop runs one job per iteration (N jobs, N scans).` (`write df.write.saveAsTable() ...` for writes)
- **Why:** The classic N+1 pattern, e.g. `for d in dates: df.filter(...).count()`. Each iteration rescans the source. It usually collapses into one `groupBy`.
- **Detect:** An ACT001-table action of kind `job`, `driver-collect` or `write` (not `display`, `stream-start` or `checkpoint`) lexically inside a `for`/`while`/comprehension body, reported once per action call. Only the loop body itself counts: an action inside a helper function that the loop calls is not followed.
- **FP:**
  - Loops over a small constant list of tables (`for t in ["a","b"]: spark.table(t).write…`) are legitimate orchestration. The finding is info instead of warning when every enclosing loop iterates a literal list/tuple of at most 10 items, OR each iteration reads a different source (the loop variable reaches `spark.table(..)` / `spark.read…(..)` / a helper call), OR the frame is provably bounded (e.g. after `limit(n)`).
  - The `foreachBatch` body is not a loop.
  - Polling loops are not reported: a `while` loop whose body calls `time.sleep(..)` (not counting nested functions), or an action in the `while` condition whose frame reads nothing rebound inside the loop (`while spark.table(t).count() == 0: ...`), waits for data written elsewhere rather than iterating over data. A condition whose frame changes each iteration (`while df.filter(F.col("v") > i).count() > 0: i += 1`) is still reported, and so is a `for` loop that sleeps for pacing.
- **Ex:** `for c in countries: print(df.filter(F.col("c")==c).count())` → `df.groupBy("c").count().show()`

#### ACT006 `count-for-emptiness`
perf · warning · on · T1 · safe (Spark ≥3.3) / unsafe · CY020, PA D006/D007/D009, PA D005
- **Msg:** `df.count() used as an emptiness check scans every row — use df.isEmpty().`
- **Why:** `count()` cannot stop early. `isEmpty()` is `limit(1)`.
- **Detect:**
  - `Compare(df.count(), [==|!=|>|<=], 0)`, or against 1 with `>=`/`<`, either side.
  - `not df.count()`, or `df.count()` as a test: `if` / `elif` / `while` / `assert` / a conditional expression, also through `and` / `or` / `not`.
  - `df.rdd.isEmpty()` → `df.isEmpty()` (D005).
  - `n = df.count()` where every later use of `n` is one of the emptiness checks above (reported on the `count()` call).
- **FP:**
  - If `n` is also logged, returned or used arithmetically, it is not reported (the count is needed).
  - A row-bounded frame (`df.limit(1).count()`, aggregate-only or `LIMIT n` SQL) is cheap to count: not reported.
  - `len(df.head(1))` / `len(df.take(1))` are fine.
- **Fix:** For the compare / `not` / test forms: safe when `spark-version ≥ 3.3` (`df.isEmpty()` / `not df.isEmpty()`), otherwise unsafe (`len(df.take(1)) == 0`). `df.rdd.isEmpty()` gets a safe fix only with `spark-version ≥ 3.3`. The `n = df.count()` form has no fix.
- **Ex:** `if df.count() > 0:` → `if not df.isEmpty():`

#### ACT007 `action-only-for-logging`
perf · warning · on · T1 · none · CY021 (widened), ruff#7272
- **Msg:** `df.count() is computed only to be logged — a full job for a log line.`
- **Why:** Very common in production pipelines: `logger.info(f"rows: {df.count()}")` after every step doubles or triples job count. Each call recomputes the lineage unless cached.
- **Detect:**
  - A DataFrame action (`count()` / `collect()` / `first()` / `head()` / `take()` / `isEmpty()` / `toPandas()`, also inside `len(..)` or an f-string) or `rdd.getNumPartitions()` appears inside the arguments of a logging sink: `print`, `display`, `logging.*` functions, a logger typed via `logging.getLogger` / `structlog.get_logger` / `loguru.logger`, an attribute named like a logger (`self.logger.info(..)`), `dbutils.notebook.exit`, or `mlflow.log_metric(s)` / `log_param(s)`.
  - A bare expression statement running one of those actions (`df.count()`, result discarded).
  - `n = df.count()` where every use of `n` is inside a logging sink (reported on the `count()` call).
- **FP:**
  - A discarded action directly inside a `try:` body with an `except` handler, or directly inside `with pytest.raises(..)` / `pytest.warns(..)` / `self.assertRaises(..)` (and the `Regex` / `Warns` variants), is a probe (does the table exist, is there a grant, does a bad argument fail on evaluation): not reported. Statements nested deeper (an `if` inside the block) or in `except` / `else` / `finally` still are.
  - The last top-level expression statement of a notebook cell (`.ipynb` and Databricks source format): the notebook displays its value, so `df.head()` / `df.count()` there is used, not discarded. Not reported (the "computed only to be logged" form still is).
  - pandas / numpy / polars values, and everything derived from them by a method, attribute, index or operator (`pd_df = pd_df.join(..)`; `pd_df.head()`), are never Spark, whatever the variable is called.
  - Counts that are deliberately logged as data-quality metrics: suppress them with `# sparklint: ignore[ACT007]`.
  - A row-bounded source (`df.limit(5).collect()`) is cheap: not reported.
  - A discarded action that fills a Spark `Observation` (the `df.observe()` pattern this rule recommends): not reported when the same function (or module, for top-level code) holds a `pyspark.sql.Observation` value (constructed there or an annotated parameter), or when the DataFrame was unpacked from the same call as a value whose `.get` is read after the action (`checked_df, observation = engine.apply_checks(df, ..)`; `checked_df.count()`; `observation.get`). `.get(..)` calls and reads before the action do not count.
  - Unlike cylint, this rule does not use the `log` substring heuristic.
- **Ex:** `log.info("n=%d", df.count())` → remove; or read `numOutputRows` from the write's Delta history; or `df.observe("m", F.count(F.lit(1)))` (Spark 3.3+, metric computed in the same job).

#### ACT008 `collect-to-filter-roundtrip`
perf · warning · on · T2 · none · new
- **Msg:** `Values collected to the driver are fed back into isin()/a filter — use a semi join.`
- **Why:** Two jobs plus a huge literal `IN` list in the plan (slow analysis, codegen limits, driver memory). A `left_semi` join (broadcast if small) is one job.
- **Detect:**
  - A list collected from an unbounded DataFrame: `df.collect()` / `toLocalIterator()`, a list / set / generator comprehension over one, `list()` / `set()` / `tuple()` / `sorted()` of one, or `.tolist()` / `.to_list()` / `.unique()` after `toPandas()`; directly or through names bound to it.
  - Reported where it is passed to `Column.isin(..)` (also as `*ids`), or interpolated into an f-string given to `spark.sql`.
- **FP:**
  - Row-bounded sources (`df.limit(10).collect()`) are not matched.
  - One scalar (`first()[0]`) used as a threshold is a scalar, not a list: not matched.
  - Other sinks (a list in `F.expr` text, a Python `in` test) are not reported.
- **Ex:** `ids=[r.id for r in bad.select("id").collect()]; df.filter(F.col("id").isin(ids))` → `df.join(bad.select("id"), "id", "left_semi")`

#### ACT009 `collect-for-driver-compute`
perf · warning · on · T2 · unsafe · new
- **Msg:** `Aggregate computed on the driver from collect() — compute it in Spark.`
- **Why:**
  - `len(df.collect())`, `sum(r.x for r in df.collect())`, `max(...)`, `sorted(df.collect())[0]`, and `df.collect()[0][0]` on unbounded frames transfer all rows to compute one scalar.
  - `collect()[0]` on an unbounded frame pulls everything to read one row.
- **Detect:**
  - The builtins `len` / `sum` / `min` / `max` / `sorted` / `any` / `all` / `set` or `collections.Counter`, called with one argument that is an unbounded `df.collect()`, a comprehension whose first source is one, or a name bound once to one and used only there.
  - `df.collect()[0]` where the receiver is not row-bounded (`agg` without `groupBy` is bounded, so `df.agg(F.max("x")).collect()[0][0]` passes).
- **Not reported (one-row frames):** `select` of only aggregates (also as one list: `df.select([F.count("a"), F.max("b")])`), `selectExpr` whose every expression is an aggregate (`df.selectExpr("min(v) AS lo", "max(v) AS hi")`; a window `OVER (..)` is not), a grouping without keys (`df.groupBy().agg(..)`, `df.groupBy().sum("x")`, also through `pivot`), and SQL `SELECT` of only aggregates without `GROUP BY` or `OVER`.
- **Fix (unsafe):** `df.collect()[0]` → `df.first()`; `len(df.collect())` → `df.count()` (only when the `collect()` is the direct argument). Other forms have no fix.
- **Ex:** `mx = max(r.ts for r in df.collect())` → `mx = df.agg(F.max("ts")).first()[0]`

#### ACT010 `debug-action-left-in`
perf · warning · on · T1 · unsafe · CY012, PA D003/D008, astrumu module_show
- **Msg:** `df.show() left in pipeline code triggers extra jobs (nobody reads the output in a scheduled run).` (names the call: `df.show()`, `display()`, `writeStream.format("console")`, ...)
- **Why:** Each `show`/`display` is a job (display may collect up to 10k rows). In scheduled jobs nobody reads the output. Console or memory streaming sinks in production are the streaming equivalent.
- **Detect:**
  - DataFrame `.show/.display/.printSchema/.explain/.describe().show/.summary().show`, on a receiver the oracle types as a Spark DataFrame (Certain or Likely).
  - Global `display(x)` when `x` is typed as a Spark DataFrame (Certain or Likely) and is not row-bounded.
  - `writeStream.format("console"|"memory")`.
  - Plain `.py` files → warning. Notebook sources (Databricks source notebooks and `.ipynb`) → info, because interactive exploration is legitimate. `printSchema`/`explain` are always info (no job on Classic).
- **FP:**
  - Notebooks that also run as scheduled jobs: raise the rule with `[tool.sparklint.severity] ACT010 = "warning"` (the override applies to every ACT010 finding, `printSchema`/`explain` included).
  - Not Spark actions, never reported: `displayHTML(..)` (renders a string), `display(dbutils.fs.ls(..))`, `display(<pandas frame / figure / image / list>)`, `display(df.head(5))` / `display(df.take(n))` (already local rows), and a row-bounded frame (`display(df.limit(10))`, aggregate-only or `LIMIT n` SQL: a deliberate small preview).
  - `.explain()` / `.show()` on objects of other libraries that mirror the DataFrame API (dbldatagen's `DataGenerator(..).withColumn(..)` builder, plotly figures): an instance of a class imported from a non-Spark library, and the builder-style chain on it, is not a DataFrame.
  - `display(x)` where the oracle cannot type `x` (a result of a third-party call bound to a name that does not look like a DataFrame, e.g. `logs = spec.build()`) is not reported: a known gap, preferred over reporting every `display`.
- **Fix (unsafe):** Delete the statement when it is a bare expression statement and the DataFrame is Certain (never on a heuristic, name-based type); when it is the only statement of a block, replace it with `pass`. No fix for console/memory streaming sinks or a call used inside a larger expression.
- **Ex:** `df.show()` → (remove)

#### ACT011 `rdd-collect`
perf · warning · on · T1 · unsafe · CY034, PA PERF001
- **Msg:** `df.rdd.collect() bypasses Arrow and converts every row through Python pickling.`
- **Why:** The `.rdd` conversion deserialises every Row through Py4J/pickle, which is far slower than `collect()` or Arrow `toPandas()`. It is also not available on Connect, serverless or standard access mode (see DBX002).
- **Detect:** `.collect()` or `.take(n)` on `x.rdd`, directly or after any chain of RDD methods (`x.rdd.map(...).filter(...).collect()`), where the oracle types `x` as a DataFrame (Certain or Likely). Other RDD actions (`count`, `first`, `foreach`) and RDDs not built from a DataFrame (`sc.parallelize(..)`) are not reported.
- **Fix (unsafe):** only for the direct forms: `df.rdd.collect()` → `df.collect()`, `df.rdd.take(n)` → `df.take(n)`. A chain with RDD transformations has no automatic fix.
- **Ex:** `df.rdd.map(lambda r: r.id).collect()` → `[r.id for r in df.select("id").collect()]` or `df.select("id").toPandas()["id"]`.

#### ACT015 `approx-quantile-exact`
perf · warning · on · T1 · none · new
- **Msg:** `approxQuantile(..., relativeError=0) computes exact quantiles: a full pass over the data that the Spark docs call "very expensive".` Inside a loop the message adds that it runs on every iteration.
- **Why:** `relativeError=0` turns the Greenwald-Khanna sketch into an exact computation: every value is kept in the summaries, which are merged and collected to the driver. The docs warn it "could be very expensive". It is an action, often used for outlier bounds inside pipelines, so it can also run once per loop iteration.
- **Detect:** `approxQuantile` on a receiver the oracle types as a `DataFrame` with Certain confidence (or `df.stat` of one) whose `relativeError` (third positional or keyword) constant-folds to `0` / `0.0` (literal, or a name bound once to such a literal). Calls with `**kw` are skipped.
- **FP:** Low. pandas `quantile`, numpy and user classes are never Spark; untyped parameters and Py4J handles are not Certain DataFrames. Genuinely needing exact values on a small frame is the legitimate case (`# noqa: ACT015`).
- **Ex:** `df.approxQuantile("x", [0.25, 0.75], 0.0)` → `df.approxQuantile("x", [0.25, 0.75], 0.001)`

### PRF — Plan & shuffle performance

#### PRF001 `withcolumn-in-loop`
perf · warning · on · T2 · unsafe · CY003, PA L003, ruff#7272
- **Msg:** `.withColumn() in a loop adds one projection per iteration — use withColumns()/select().` For the reduce form: `functools.reduce(...withColumn...) adds one projection per element — use withColumns()/select().` Help: `Build all expressions first, then apply them once: df.withColumns({name: expr, ...}) (Spark >= 3.3) or df.select("*", *exprs).`
- **Why:** Every call creates a new Project node and re-runs analysis, so cost is O(n²) in the number of columns. With hundreds of columns, driver planning time reaches minutes and can hit StackOverflowError (Spark docs warn about this explicitly).
- **Detect:**
  - `df = df.withColumn(...)` (also inside a longer chain rooted at `df`) in a `for` / `while` body, where the new binding of `df` reaches the receiver on the next iteration (a loop-carried DataFrame). One finding per loop, on the first such call.
  - `functools.reduce(lambda d, c: d.withColumn(...), cols, df)` where the initial value is a DataFrame.
  - The receiver must be typed DataFrame (avoids cylint's untyped match).
- **Not reported:** a comprehension that builds one new frame per element (`[df.withColumn(c, ..) for c in cols]`: nothing is carried between iterations), and a loop that assigns the result to a different name.
- **FP:** Loops with ≤ 3 iterations over a literal list or tuple → info (the reduce form is always a warning).
- **Fix (unsafe):** Only when the `for` loop body is the single statement `df = df.withColumn(<name>, <expr>)` with two positional arguments, no `else` and everything on one line: rewrite to `df = df.withColumns({<name>: <expr> for c in <iter>})` (Spark ≥ 3.3). Other loops, `while` loops and the reduce form have no fix.
- **Ex:** `for c in cols: df = df.withColumn(c, F.trim(c))` → `df = df.withColumns({c: F.trim(c) for c in cols})`

#### PRF002 `column-op-in-loop`
perf · warning · on · T2 · unsafe · CY011
- **Msg:** `.<method>() in a loop adds one plan node per iteration — batch it into one call.` Help: `Build the full list first: df.withColumnsRenamed({old: new, ...}) (Spark >= 3.4), df.drop(*cols), or one df.select(...).`
- **Why:** Same plan growth as PRF001.
- **Detect:** A loop-carried DataFrame (as in PRF001: `df = df.<method>(...)` in a `for` / `while` body whose new binding reaches the next iteration) calling `withColumnRenamed`, `withColumnsRenamed`, `withColumns`, `drop`, `select`, `selectExpr`, `toDF`, or `na.fill` / `na.replace`. One finding per loop. A `withColumn` in the loop (a cast, say) is PRF001's.
- **FP:** Loops with ≤ 3 iterations over a literal list or tuple → info.
- **Fix (unsafe):** Only when the `for` loop body is that single statement with positional arguments: `df.withColumnRenamed(old, new)` → `df = df.withColumnsRenamed({old: new for c in <iter>})` (Spark ≥ 3.4; one-line arguments), and `df.drop(c)` where `c` is the loop variable → `df = df.drop(*<iter>)`. Other methods have no fix.
- **Ex:** `for c in cols: df = df.withColumnRenamed(c, c.lower())` → `df = df.withColumnsRenamed({c: c.lower() for c in cols})`

#### PRF003 `cartesian-join`
perf · error · on · T1 · none · CY007, PA S010/S011
- **Msg:** `Cartesian product: crossJoin()/join() without a condition produces |A|×|B| rows.`
- **Why:** Output explodes and runs as a broadcast nested loop or CartesianProduct. This is the single most expensive accident in a cost review.
- **Detect:**
  - `DataFrame.crossJoin(x)`.
  - `DataFrame.join(other)` with no `on`, `on=None`, or a positional `None` condition (whatever `how` says, `"cross"` included).
  - `join(other, on=True)` or `join(other, on=F.lit(True))`.
  - Help: `Join on a key (df.join(other, "key", "inner")); for a scalar, aggregate it first so one side has one row.`
- **Not reported:** SQL text (`CROSS JOIN`, comma joins in `spark.sql`) is not checked.
- **FP:** One side is `bounded(1)` (scalar from `agg()` without groupBy, a `select` whose every item is an aggregate or a row-wise function of aggregates and literals such as `F.to_date(F.max("d"))`, or `createDataFrame([literal])`). This is the scalar-broadcast idiom → no finding. Either side with any other known row bound (`spark.range(10)`, `limit(n)`, a frame built from driver data) → info.
- **Ex:** `a.crossJoin(b)` → `a.join(b, "key", "inner")`; for a scalar use `a.join(F.broadcast(stats), how="cross")` with `stats` bounded.

#### PRF004 `non-equi-join`
perf · warning · on · T1 · none · CY015
- **Msg:** `Join condition has no equality predicate — executes as a nested-loop join.`
- **Why:** Without an equi-key, Spark cannot use a sort-merge or hash join, so it compares O(n·m) rows.
- **Detect:** The join condition (a `Column` expression tree, possibly a list or `&` chain) contains no `==`/`eqNullSafe` between columns from different sides. Range-only conditions (`a.ts.between(b.start, b.end)`) are the typical case. Also `F.expr` strings using only `<`, `>`, `BETWEEN` or `LIKE`.
- **FP:** One side is `bounded`/broadcast-hinted and small → info.
- **Fix:** none. The message suggests adding a bucketing equi-key (`floor(ts/3600)`), or a Databricks range-join hint `df.hint("range_join", <bin>)`.
- **Ex:** `ev.join(win, (ev.ts>=win.s)&(ev.ts<win.e))` → add `ev.bucket==win.bucket &` …

#### PRF005 `global-window`
perf · warning · on · T1 · none · CY017, palantir "Empty partitionBy()"
- **Msg:** `Window without partitionBy() moves all rows into a single partition.`
- **Why:** Spark logs "No Partition Defined for Window operation! Moving all data to a single partition". One task does all the work, it spills, and the cluster is idle.
- **Detect:** `Column.over(w)` where the oracle types `w` (through names, `if`/`else` expressions and branches) as a `WindowSpec` built from `Window` / an alias of `pyspark.sql.window.Window` that has no `partitionBy` with ≥ 1 argument on any path: `Window.orderBy(..)`, `Window.rowsBetween(..)`, and `Window.partitionBy()` with no arguments (or an empty list). SQL `OVER (...)` clauses are not checked.
- **Not reported:** a spec that is partitioned on some path only (`if keys: w = w.partitionBy(*keys)`, `Window.partitionBy(*k).orderBy(t) if k else Window.orderBy(t)`): without keys there is one series by design. A spec whose origin is unknown: a function parameter, an attribute set from one, or the result of a function annotated `-> WindowSpec` (the annotation is used, not the body).
- **Ex:** `F.row_number().over(Window.orderBy("ts"))` → `Window.partitionBy("account").orderBy("ts")`, or `df.agg(...)` for global totals.

#### PRF006 `recomputed-lineage`
perf · warning · on · T2 · none · CY014, PA PERF007
- **Msg:** `df feeds 3 actions/writes without cache — its full lineage is recomputed each time.`
- **Why:** Each action re-executes the plan from source, including joins and shuffles. On Databricks the disk cache softens re-reads but not recomputed shuffles or UDFs.
- **Detect:**
  - Per DataFrame value (def-use, path-sensitive): count the actions and writes reachable on one execution path. Uses inside a loop count as ≥ 2.
  - Also count *branching consumers*: two different downstream DataFrames that both end in an action.
  - Fire when the count is ≥ 2, the value is not `cached`, and the plan contains at least one "expensive" node (join, groupBy, window, distinct, UDF, or a non-Delta source such as a file read with schema inference or JDBC).
- **FP:**
  - A cheap scan-filter-project of a Delta table: recomputation is cheaper than caching → no finding.
  - Exclude `printSchema`/`explain`, and `.columns` (not actions).
  - Each action counts once, however many paths lead to it: a name rebound on one branch (`if c: df = df.join(x)`) followed by `df.count()`, or a frame used twice by one downstream frame (`a.union(a)`, a self-join) feeding a single action.
  - Actions inside an `assert` message or a `raise` statement do not count: they run only when the check already failed. Neither do statements directly inside `with pytest.raises(..)` / `self.assertRaises(..)` (the test expects them to fail); a `try:` probe and a `pytest.warns(..)` block run to the end and do count.
- **Ex:** `agg = big.join(d,"k").groupBy("x").sum(); agg.write…; agg.count()` → `agg = (...).persist(); …; agg.unpersist()`

#### PRF008 `cache-single-use`
perf · warning · on · T2 · unsafe · CY005
- **Msg:** `df is cached but consumed only once — cache costs a materialisation and memory with no reuse.`
- **Why:** Cache adds serialisation and storage-memory pressure (and evicts useful blocks) without any reuse.
- **Detect:** A `cached` DataFrame value whose number of *consuming* uses on every path (actions, writes, or derivations that end in actions) is ≤ 1. A use inside a loop counts as many. A single derivation counts the actions (including `show()` / `display()`) that it and the frames derived from it run on one path: `s = df.agg(..); s.show(); s.collect()` reuses the cache and is not reported; a derived frame that is cached itself does not count as reuse of this cache. Uses inside an `assert` message, a `raise` statement or directly inside `with pytest.raises(..)` / `self.assertRaises(..)` do not count (they never run on the passing path; same counting policy as PRF006), so a cache read once normally and once in an error message is reported.
- **Not reported:** a cache whose frame, or a frame derived from it, is returned, yielded, stored or captured by a closure (its uses are out of sight), and a bare `df.cache()` / `df.persist()` statement on a function parameter (it caches the caller's DataFrame for the caller). `c = param.cache()` used once is still reported.
- **FP:** A cache intended to break a nondeterministic lineage (e.g. `rand`/`uuid` columns that must stay stable across two writes). If the plan contains a nondeterministic expression and a later join, say "cache for determinism — use a checkpoint or a write instead".
- **Fix (unsafe):** Remove `.cache()`/`.persist()` when it is a pure suffix of the expression.
- **Ex:** `df = spark.table("t").cache(); df.write.saveAsTable("u")` → drop the `.cache()`.

#### PRF009 `cache-never-unpersisted`
perf · info · on · T2 · none · CY025, PA PERF005
- **Msg:** `df is cached but never unpersisted in this scope.`
- **Why:** In long-running notebooks or shared all-purpose clusters, cached blocks hold executor memory until the session ends. That causes spills and evictions for other workloads.
- **Detect:** A `cached` value with no `unpersist()` on it (or its alias) on every path to scope exit, and that does not escape (is not returned, stored on `self`, or passed to an unknown function). Module scope in a job file → skip (the session ends with the job).
- **Ex:** `df.cache(); ...` → `try: … finally: df.unpersist()`

#### PRF010 `repartition-before-write`
perf · info · on · T1 · unsafe · CY008
- **Msg:** `repartition() right before write adds a full shuffle — rely on optimized writes / auto compaction, or partition by the write's partition columns.`
- **Why:** On Delta in Databricks, optimized writes and auto compaction size files without a manual shuffle. A numeric `repartition(n)` costs a full shuffle and often produces worse file sizes.
- **Detect:** `repartition(...)` in the writer chain, with only cheap ops (`select`, `withColumn`, `sortWithinPartitions`) between it and `.write`/`.writeTo`.
- **Exemptions** (from cylint, extended):
  - All repartition columns ⊆ `partitionBy` columns. This is intentional, to reduce the number of files per partition.
  - A literal count ≥ 100 (raising parallelism, not reducing files).
  - The repartitioned frame is built from driver-side data (`spark.createDataFrame` of a pandas frame, list or tuple, kept through row-wise steps, joins and unions with other such frames) or has a known small row count (`agg`, a literal `limit`): the shuffle is cheap and only sets the number of output files.
  - A step after the repartition calls a user-defined function (`withColumn("c", my_udf("x"))`): the repartition spreads the UDF work over more tasks.
  - `repartition(1)` is reported by IOD003 instead when that rule is enabled.
- **Fix (unsafe):** Remove the `.repartition(...)`.
- **Ex:** `df.repartition(10).write.format("delta").saveAsTable("t")` → `df.write.format("delta").saveAsTable("t")`

#### PRF012 `explode-before-filter`
perf · warning · on · T1 · none · CY038
- **Msg:** `explode() runs before a filter that does not depend on the exploded values — filter first.`
- **Why:** Explode multiplies rows. Filtering afterwards does row×array-size work. Catalyst can push some filters below Generate, but not those mixed with other projections or UDFs.
- **Detect:** In a DataFrame chain or dataflow: a `select`/`withColumn` containing `F.explode/explode_outer/posexplode/inline`, followed by `filter`/`where` whose referenced columns do not include the explode output alias or any column derived from it. The filter must reference only pre-existing columns.
- **Ex:** `df.withColumn("i", F.explode("items")).filter(F.col("country")=="ZA")` → `df.filter(F.col("country")=="ZA").withColumn("i", F.explode("items"))`

#### PRF015 `collect-dedup-redundant`
perf · warning · on · T0 · safe · CY033, PA ARR001/ARR003
- **Msg:** `array_distinct(collect_list(x)) → collect_set(x)` / `array_distinct(collect_set(x)) is redundant.`
- **Why:** The first form materialises duplicates and then deduplicates them. The second is a no-op pass.
- **Detect:** `F.array_distinct(F.collect_list(e))` or `F.array_distinct(F.collect_set(e))`, including `.over(w)` forms.
- **FP:** `collect_list` preserves order and `collect_set` does not. If the result is fed to `F.array_join`/`element_at`/`[0]` and order matters, the user should not apply the fix, so the fix is offered as safe only when the result is not consumed by order-sensitive functions; otherwise it is unsafe.
- **Ex:** `F.array_distinct(F.collect_list("x"))` → `F.collect_set("x")`

#### PRF022 `sql-select-star`
perf · info · off · T0 · none · CY004, PA F020
- **Msg:** `SELECT * in spark.sql() — list the columns you need.`
- **Why:** Column pruning is usually automatic through later projections. The real cost appears when `*` flows directly into a write, cache, collect or toPandas, or when the source is a wide row-based format (CSV/JSON/JDBC).
- **Detect:** `spark.sql(<const/f-string>)` whose text matches `SELECT\s+\*\s+FROM` (also `select("*")`). When enabled, it fires only if the result flows (T2) to a write/cache/collect/toPandas or the source format is row-based. Otherwise it is silent.
- **Not reported:** `spark.sql("EXPLAIN … SELECT * FROM t")`: an `EXPLAIN` only plans its query and returns one row of plan text (no rule that reads `spark.sql` text checks the query inside an `EXPLAIN`).
- **Ex:** `spark.sql("SELECT * FROM t").write…` → `spark.sql("SELECT a,b FROM t").write…`

#### PRF023 `pivot-without-values`
perf · warning · on · T1 · none · new
- **Msg:** `pivot(col) without a values list runs an extra Spark job to find the distinct values.`
- **Why:** Without `values`, Spark eagerly runs `distinct(col)` (a full scan plus shuffle, collected to the driver) just to work out the output schema. It does this every time the plan is built, so inside a loop it repeats per iteration. The docs call this "less efficient".
- **Detect:** A `pivot` call on a receiver the oracle types as `GroupedData` with a pivot column and no `values` (missing, or the literal `None`). Calls that pass `*args`/`**kw` are skipped. Inside a loop the message says the job repeats.
- **FP:** Low. pandas `DataFrame.pivot` / pandas-on-Spark are not `GroupedData` and are never reported. A tiny or genuinely unknown domain is the legitimate case (suppress with `# noqa: PRF023`).
- **Ex:** `df.groupBy("year").pivot("course").sum("earnings")` → `df.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings")`

#### PRF024 `cube-many-columns`
perf · warning · on · T1 · none · new
- **Msg:** `CUBE over 6 columns makes 64 grouping sets: every input row is copied 64 times before the shuffle.`
- **Why:** `cube(c1..cN)` is `GROUPING SETS` over all 2^N subsets. The Expand operator emits one copy of every input row per set before the shuffle, so N=6 is 64× the shuffle data. `rollup` makes N+1 sets (milder, reported from 8 columns).
- **Detect:** `cube(...)` / `rollup(...)` on a receiver the oracle types as `DataFrame`, counting positional columns (a single list/tuple literal is expanded); `*cols` and keywords are skipped. Also `spark.sql(<const/f-string>)` with `GROUP BY CUBE(..)` / `ROLLUP(..)` or `GROUP BY a, b, ... WITH CUBE|ROLLUP` (top-level expressions counted, nested parentheses and quotes respected). Thresholds: `cube-min-columns` (default 5) and `rollup-min-columns` (default 8); 0 disables.
- **FP:** Low-med (threshold rule, but the growth is exponential). Spark `cube` exists only on DataFrames; pandas/numpy/user classes are not reported.
- **Ex:** `df.cube("a", "b", "c", "d", "e").count()` → `spark.sql("... GROUP BY GROUPING SETS ((a, b), (a), ())")` listing only the sets you need (`df.groupingSets(...)` on Spark ≥ 4.0).

#### PRF025 `not-in-subquery`
perf · warning · on · T1 · none · new
- **Msg:** `SQL NOT IN (SELECT ...) is planned as a null-aware anti join, usually a broadcast nested-loop join: very slow, or a broadcast failure when the subquery is large.`
- **Why:** To keep SQL's NULL semantics, Spark rewrites `x NOT IN (subquery)` as a left anti join on `x = y OR isnull(x = y)`. The `OR` rules out sort-merge and shuffled hash joins. Catalyst's own comment says it "will almost certainly be planned as a Broadcast Nested Loop join". Since Spark 3.1, a single-column subquery that fits under the broadcast threshold gets a null-aware broadcast hash join instead. Multi-column or large subqueries stay O(n·m), and Spark still tries to broadcast them even with `autoBroadcastJoinThreshold=-1`.
- **Detect:** `spark.sql(<text>)` on a SparkSession receiver, where the text is a literal, an f-string or a single-assignment variable holding one. It must contain `NOT IN (` followed by `SELECT` or `WITH` (case-insensitive, extra parentheses allowed), outside string literals and `--`/`/* */` comments. Literal lists (`NOT IN (1, 2)`), `IN (SELECT ...)` and `NOT EXISTS` are not reported. Inside a loop the message says it repeats.
- **FP:** Low-med. A tiny single-column subquery gets the fast null-aware broadcast hash join. If the column can hold NULLs, `NOT EXISTS` returns different rows (`NOT IN` returns nothing when the subquery yields a NULL), so the help text states that precondition. `df.where("...")` / `F.expr` strings and `%sql` cells are not scanned yet.
- **Ex:** `spark.sql("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)")` → `spark.sql("SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id)")`

#### PRF026 `many-distinct-aggregates`
perf · warning · on · T1 · none · new
- **Msg:** `3 exact distinct aggregates over different columns in one agg(): Spark copies every input row about 3 times (one copy per distinct column set) before the shuffle.`
- **Why:** With more than one distinct column set in an aggregation, `RewriteDistinctAggregates` adds an Expand that emits one copy of every input row per distinct set (plus one when regular aggregates are present) before the first shuffle. N distinct counts mean roughly N× the shuffle data and memory; one reported job went from 65 M to 4.5 B internal rows with 68 `COUNT(DISTINCT)`s, and upstream benchmarks show 3-10× slowdowns at 2-6 distincts.
- **Detect:** `agg(...)` on a `GroupedData` (including after `pivot`) or `DataFrame`, `select(...)` and `selectExpr(...)` on a `DataFrame`, and `spark.sql(<const/f-string>)`. Counts distinct aggregates by their argument set: `F.countDistinct`/`count_distinct`/`sumDistinct`/`sum_distinct` anywhere inside the arguments (column names, `F.col`, `df.x`, `df["x"]` normalised; argument order ignored), `F.expr`/`selectExpr` strings and SQL `<fn>(DISTINCT ...)` (via `SqlText::distinct_aggregates`, per `SELECT`, outside strings and comments). Reports at `distinct-aggregates-min` (default 3; 0 disables) different sets. Starred argument lists are not counted. Inside a loop the message says it repeats.
- **FP:** Low-medium: on small inputs the copies are cheap, and exact counts are sometimes required (reporting). The same column counted several ways is one set and not reported.
- **Ex:** `df.groupBy("day").agg(F.countDistinct("user"), F.countDistinct("session"), F.countDistinct("device"))` → `F.approx_count_distinct(...)` for each, or one aggregation per distinct column over `select(k, c).distinct()` joined on `k`.

#### PRF027 `order-by-rand`
perf · warning · on · T1 · none · new
- **Msg:** `orderBy(rand()) sorts the whole dataset (a sampling job plus a full shuffle) just to randomise the row order.`
- **Why:** A global sort is range-partitioned: Spark first runs a sampling job to find the range bounds, then shuffles every row. Sorting by a random key buys nothing but row order, and the usual `limit(n)` afterwards still funnels into one partition. `sample(fraction)` or `where(rand() < p)` is a narrow filter with no shuffle.
- **Detect:** `orderBy`/`sort` on a receiver the oracle types as `DataFrame` whose leading sort key (first positional argument, or the first element of a list/tuple) is `rand()`/`randn()` from `pyspark.sql.functions`, optionally wrapped in `.asc()/.desc()` (+ `_nulls_*`), `F.asc/F.desc(...)`, or `F.expr("rand()")`. Also `spark.sql(<const/f-string>)` with `ORDER BY rand()|randn()|random()` as the leading key outside strings, comments and window specs (parentheses holding no `SELECT`), including inside subqueries and CTEs. Inside a loop the message says it repeats.
- **FP:** Low. Not reported: a random tie-breaker after another key, `Window.orderBy(F.rand())` (per-group sampling with `row_number`), `sortWithinPartitions`, SQL `SORT BY`/`DISTRIBUTE BY rand()`, a materialised random column sorted by name (`withColumn("r", F.rand()).orderBy("r")`, not tracked yet).
- **Ex:** `df.orderBy(F.rand()).limit(100)` → `df.sample(fraction=0.01).limit(100)`

#### PRF029 `replicated-storage-level`
perf · warning · on · T1 · none · new
- **Msg:** `Storage level MEMORY_AND_DISK_2 keeps every cached partition twice (extra executor memory/disk and a network copy of each block); recomputation already makes cached data fault-tolerant.`
- **Why:** The `_2`/`_3` levels replicate every cached block to another executor: double (or triple) the cache footprint, which evicts other cached data or spills, plus a network transfer of each block while the cache is built. All levels are already fault-tolerant through lineage recomputation; the guide reserves replication for fast recovery when serving requests. In batch ETL it is nearly always copy-paste.
- **Detect:** `persist(level)` on a DataFrame or RDD, `spark.catalog.cacheTable(t, level)` (Spark ≥ 3.5), where `level` resolves (through single-assignment variables) to a `pyspark.StorageLevel` attribute ending in `_2`/`_3` or a `StorageLevel(...)` constructor with a literal `replication` > 1; and `spark.sql` text starting with `CACHE` whose `OPTIONS ('storageLevel' '<level>_2|_3')`. Loops add a note.
- **FP:** Low. Not reported: unreplicated levels, levels from variables with several bindings or parameters, `persist` on user classes, `StorageLevel` constructors with a non-literal replication.
- **Ex:** `df.persist(StorageLevel.MEMORY_AND_DISK_2)` → `df.persist(StorageLevel.MEMORY_AND_DISK)`

#### PRF031 `running-collect-list-window`
perf · warning · on · T1 · none · new
- **Msg:** `collect_list() over a running window (ordered, frame from the partition start to the current row) gives every row its own growing array: O(N²) elements per partition.`
- **Why:** An ordered window's default frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. With `collect_list`/`collect_set`/`array_agg` each output row carries its own copy of the array so far, so a partition of N rows materialises N·(N+1)/2 elements: executor OOM or huge spill and shuffle on large or skewed keys. The intent is usually "all values per key, in order" (one `groupBy().agg()` row per key) or "the last n values" (a bounded frame).
- **Detect:** `.over(w)` on a `pyspark.sql.functions.collect_list|collect_set|array_agg` call, where the oracle types `w` (through variables) as a `WindowSpec` with `orderBy` and either no frame or a last `rowsBetween`/`rangeBetween` whose start is unbounded (`Window.unboundedPreceding`, `-sys.maxsize`, a huge literal) and whose end is finite (`Window.currentRow` or an int literal). Without `partitionBy` the message adds that all rows land in one partition, and PRF005 is dropped at that site (precedence pair).
- **FP:** Low-med. Fine on tiny partitions. Not reported: unordered windows and explicit whole-partition frames, bounded frames, frames whose bounds are variables, SQL text (`collect_list(..) OVER (..)` in `spark.sql` is not scanned yet).
- **Ex:** `F.collect_list("v").over(Window.partitionBy("k").orderBy("ts"))` → `df.groupBy("k").agg(F.sort_array(F.collect_list(F.struct("ts", "v"))))`, or `.rowsBetween(-10, Window.currentRow)` for the last 10 values

### UDF — User-defined functions

#### PRF032 `or-join-condition`
perf · warning · on · T1 · none · new
- **Msg:** `Join condition ORs equalities together — Spark can't use it as a join key, so it runs a nested-loop join (O(n·m) comparisons).`
- **Why:** Spark extracts hash / sort-merge join keys only from equalities that are ANDed at the top of the condition. `(a.id == b.id) | (a.alt_id == b.id)` has equalities, so PRF004 stays quiet, but none of them is a key: the join is planned as a BroadcastNestedLoopJoin, or a Cartesian product when neither side fits a broadcast.
- **Detect:** `join` on a receiver the oracle types as `DataFrame`. The `on` argument is split into ANDed conjuncts (`&` chains, list/tuple elements, single-assignment Column variables up to 3 deep, `F.expr` SQL text split on top-level `AND` outside quotes and parentheses). Fires when some conjunct is an OR (`|`, SQL `OR`) containing an equality between non-literal operands (`==`, `eqNullSafe`, `=`, `<=>`) and no conjunct is a plain equality. One side bounded or `broadcast(...)` → info.
- **FP:** Low. Not reported: an equality conjunct next to the OR (Spark keys on it), ORs without equalities (PRF004's), an opaque conjunct NEXT TO the OR (user helper call, unresolved Column variable, negated variable: it may be the key), literal-only SQL equalities count as filters, unknown receivers, `or` inside SQL string literals or backtick names. SQL `JOIN … ON … OR` in `spark.sql` text is not checked yet.
- **Fix:** none. The help suggests one equi-join per OR branch plus `unionByName` (and `dropDuplicates` when a row can match both), or exploding the candidate keys into one key column.
- **Ex:** `a.join(b, (a.id == b.id) | (a.alt_id == b.id))` → `a.join(b, a.id == b.id).unionByName(a.join(b, a.alt_id == b.id))`

#### PRF035 `keyless-grouped-pandas`
perf · warning · on · T1 · none · new
- **Msg:** `groupBy() with no columns before applyInPandas(): the whole DataFrame is one group, so a single task loads every row into one pandas DataFrame (no parallelism, OOM once it outgrows one executor).`
- **Why:** An empty grouping is one global group. For plain aggregates Spark still aggregates partially in parallel, but a grouped-map pandas/Arrow function receives each group as ONE frame: the whole DataFrame is shuffled into a single task and materialised in one Python worker, so one core works while the cluster idles, and the job OOMs once the data outgrows one executor.
- **Detect:** `applyInPandas`, `applyInArrow`, `apply` (grouped-map `pandas_udf`) or `cogroup` on a receiver the oracle types as `GroupedData`, whose grouping call (chained, or the single binding of a name) is `DataFrame.groupBy()` / `groupby()` with no columns (also `groupBy([])` / `groupBy(())`). Inside a loop the message says so.
- **FP:** Low. Not reported: any grouping column, `*cols` / list variables / keywords (columns unknown), aggregates on a keyless `groupBy()` (`agg`, `count`, `avg`), a bounded input (`limit(n)` before `groupBy()`), names bound in several branches, unknown receivers, pandas `groupby`.
- **Fix:** none. The help suggests a real key, or `mapInPandas` (once per partition, in parallel).
- **Ex:** `df.groupBy().applyInPandas(score, schema)` → `df.mapInPandas(score_batches, schema)`

#### PRF036 `forced-nested-loop-join-hint`
perf · warning · on · T1 · none · new
- **Msg:** `hint("shuffle_replicate_nl") forces a shuffle-and-replicate nested-loop join: every row of one side is compared with every row of the other (a cartesian product filtered afterwards), even when the join has an equality key a hash or sort-merge join could use.`
- **Why:** The `SHUFFLE_REPLICATE_NL` join strategy hint makes Spark plan a `CartesianProduct` and filter it, |A|×|B| comparisons on every run, even when the condition has an equi-key that a hash or sort-merge join would use. It is usually left over from an experiment or copied from a theta-join example.
- **Detect:** `DataFrame.hint(name)` whose name (string literal or a name bound once to one, any case, surrounding spaces ignored) is `shuffle_replicate_nl`; `spark.sql` text with a `/*+ ... SHUFFLE_REPLICATE_NL(..) ... */` hint comment (new `SqlText::hints()`: hint names at the top level of `/*+ */` comments; ordinary comments and string literals masked). Inside a loop the message says so.
- **FP:** Low. Not reported: other strategy / partitioning hints, `F.broadcast`, names that only contain the text, non-constant hint names, ordinary `/* */` / `--` comments, string literals and identifiers in SQL, unknown receivers. A genuine theta join of two small inputs is a legitimate use (the help says to keep it with a comment / `# noqa`).
- **Fix:** none. The help suggests dropping the hint (or BROADCAST / MERGE / SHUFFLE_HASH for an equi-join).
- **Ex:** `orders.join(customers.hint("shuffle_replicate_nl"), "customer_id")` → `orders.join(customers, "customer_id")`

#### UDF001 `udf-has-builtin`
perf · warning · on · T0 · unsafe · CY002, PA U001/U002/U003, ruff#7272
- **Msg:** `UDF body is equivalent to built-in F.lower() — drop the UDF.`
- **Why:** Python UDFs serialise each row JVM→Python→JVM. They are opaque to Catalyst (no pushdown or constant folding) and run outside Photon. Replacing one with a builtin is often 2–10× on that stage.
- **Detect:**
  - A UDF definition (`F.udf(fn|lambda)`, `@udf`/`@F.udf(...)`, `spark.udf.register(name, fn)`, and `pandas_udf` with a trivial body) whose body is a single return expression matching the builtin table:
    - String methods (`lower`, `upper`, `strip`, `startswith`, `replace`, `split`, `zfill`, `len`, slicing → `substring`).
    - `re.sub/match/search/findall` → `regexp_replace/rlike/regexp_extract/regexp_extract_all`.
    - Arithmetic, `round/abs/math.*`, casts `int/float/str/bool`.
    - `x if x is not None else d` → `coalesce`, `a if c else b` → `when`.
    - `datetime.strptime/strftime` → `to_timestamp/date_format`.
    - `json.loads(x)[k]` → `get_json_object`/`from_json`.
    - `hashlib.md5/sha256` → `md5/sha2`.
    - List comprehension over an array arg → `transform`/`filter`.
  - A UDF returning `StringType`/`ArrayType` whose body only does string or list ops → also flagged (PA U001/U002).
- **FP:** Bodies with library calls outside the table (e.g. `phonenumbers.parse`) → not flagged. Null handling differs: `x.lower()` raises on None, `F.lower` returns null. That is why the fix is unsafe (the result is "more" correct, but different).
- **Fix (unsafe):** Rewrite call sites `my_udf(F.col("a"))` → `F.lower(F.col("a"))` when the UDF is single-expression and used only in this file.
- **Ex:** `up = F.udf(lambda s: s.upper())` → `F.upper("s")`

#### UDF002 `udf-in-filter`
perf · warning · on · T1 · none · CY009
- **Msg:** `UDF used as a filter predicate — prevents predicate pushdown/data skipping and evaluates the UDF on every row.`
- **Why:** The filter can't be pushed to Parquet/Delta, so there is no file or row-group skipping. Every row goes through Python before rows are removed.
- **Detect:** The `filter`/`where` argument contains a call to a value of type `UserDefinedFunction` (resolved cross-file, T3 when imported), or SQL text in `filter("myudf(x)")` naming a registered UDF.
- **Fix:** none. Suggest a native expression (UDF001), or split the predicate so native conjuncts come first: `df.filter(native).filter(udf(...))`.
- **Ex:** `df.filter(is_valid(F.col("email")))` → `df.filter(F.col("email").rlike(r"^[^@]+@[^@]+$"))`

#### UDF006 `udf-missing-return-type`
correctness · warning · on · T0 · none · new
- **Msg:** `udf() without returnType defaults to StringType — numeric results become strings.`
- **Why:** A silent type change. Downstream comparisons become string comparisons (`"10" < "9"`), and writes fail or change the table schema.
- **Detect:** `F.udf(fn)` / `@udf` with no returnType arg, where the function body or annotation returns a non-str (literal returns, a `-> int` annotation, arithmetic). With an unknown return type, info.
- **Ex:** `@udf def f(x): return x*2` → `@udf("long") def f(x): return x*2`

#### UDF011 `pandas-udf-per-batch-init`
perf · warning · on · T1 · none · new
- **Msg:** `heavy initialiser inside a pandas UDF runs for every Arrow batch — load it once per partition.`
- **Why:** A Series→Series `pandas_udf` body runs once per Arrow batch (~10k rows) on every task; `applyInPandas` runs once per group. Loading a model, a tokenizer, a cloud client or a DB connection there repeats that cost thousands of times. The iterator forms (`Iterator[pd.Series] -> Iterator[pd.Series]`, `mapInPandas`) let you initialise once per partition, before the batch loop.
- **Detect:** The body of a function decorated with or passed to `pandas_udf` (non-`Iterator` type hints, not `SCALAR_ITER`), or passed to `GroupedData/cogroup.applyInPandas`/`applyInArrow`, calls a heavy initialiser: `mlflow.*.load_model`, `joblib.load`, `pickle.load`, `cloudpickle/dill.load`, `torch.load`, `*.from_pretrained`, `transformers.pipeline`, keras `load_model`, `boto3.client/resource`, or a DB connect (`psycopg2/pyodbc/pymysql/oracledb/snowflake/databricks.sql.connect`, `sqlalchemy.create_engine`, `pymongo.MongoClient`, `redis.Redis`). For iterator pandas UDFs and `DataFrame.mapInPandas`/`mapInArrow` only calls inside a `for`/comprehension over the iterator parameter are reported; loading before the loop is the recommended pattern. Names are matched as resolved qualified names. One level of same-file helper calls is followed.
- **FP:** Low-med. Not reported behind module-level caching: a `functools.lru_cache`/`cache`-decorated loader, or an assignment to a module-level cache (`global _m; if _m is None: _m = load()`, `_CACHE[k] = load()`). A cache written some other way (e.g. a class attribute) may still be reported; suppress with `# noqa: UDF011`.
- **Ex:** `@pandas_udf("double")\ndef f(s: pd.Series) -> pd.Series: m = mlflow.pyfunc.load_model(uri); ...` → `def f(it: Iterator[pd.Series]) -> Iterator[pd.Series]: m = mlflow.pyfunc.load_model(uri); for s in it: yield ...`

#### UDF012 `per-row-external-connection`
perf · warning · on · T1 · none · new
- **Msg:** `row-wise function opens an external connection for every record — open it once per partition.`
- **Why:** A plain UDF, `foreach` or RDD row function runs once per record. Creating an HTTP session, a cloud client or a DB/socket connection there costs a TCP/TLS handshake (and often auth) per row and throttles throughput. Spark's guide calls this a common mistake and prescribes `foreachPartition` (or a pooled client).
- **Detect:** The body of a function or lambda decorated with/passed to `udf` / `spark.udf.register`, `DataFrame.foreach`, `RDD.map/flatMap/filter/foreach/mapValues/flatMapValues`, `writeStream.foreach(fn)`, or the `process` method of a streaming foreach writer (a class passed to `writeStream.foreach`, or any class defining `open`, `process` and `close`) calls `requests.get/post/put/patch/delete/head/request/Session`, `httpx.*`, `urllib.request.urlopen`, `urllib3.PoolManager`, `boto3.client/resource`, `socket.socket/create_connection`, or a DB connect (same table as UDF011). One level of same-file helper calls is followed.
- **FP:** Low. Not reported for `foreachPartition`, `mapPartitions`, `foreachBatch`, `ForeachWriter.open`, pandas UDFs, a module-level/pooled client (`SESSION.get`), or an `lru_cache`d client factory. Receivers must be known Spark DataFrames/RDDs/stream writers, so pandas `.map`, builtin `map` and user classes are ignored.
- **Ex:** `df.foreach(lambda r: requests.post(url, json=r.asDict()))` → `df.foreachPartition(send_partition)` with one `requests.Session()` per partition

#### UDF013 `udf-whole-row-struct`
perf · info · on · T1 · none · new
- **Msg:** `UDF f() gets the whole row as a struct — all columns are shipped to Python and back, and Spark can't prune the unread ones from the scan.`
- **Why:** A UDF called with `struct("*")` or every column of the frame serializes all columns (wide strings, arrays, blobs included) to the Python worker and back for each row, pickle or Arrow. Because the UDF references every column, column pruning below it stops working, so the scan reads every column of a columnar table too.
- **Detect:** A call of a value the oracle types as `UserDefinedFunction` (`F.udf`, `@udf`, `pandas_udf`, an inline `F.udf(...)(...)`) with an argument that is `F.struct("*")` / `struct(F.col("*"))` / `struct(df["*"])`, `F.struct(*df.columns)` / `F.struct(df.columns)`, an unfiltered comprehension over `df.columns` (inside `struct` or starred), or a starred `*df.columns` (`df` typed `DataFrame`).
- **FP:** Low-med (a narrow frame costs little). Not reported: explicit column lists, a starred variable of names, comprehensions with an `if` (a chosen subset), built-ins over the whole row (`to_json(struct("*"))`, `xxhash64(*df.columns)`), a `struct("*")` nested inside another built-in before reaching the UDF, plain Python functions, pandas frames' `.columns`, `pandas_udf(..., functionType=X)` with X not `SCALAR`/`SCALAR_ITER` (grouped-map / grouped-agg UDFs take the whole frame by design; pyspark's own `applyInPandas` builds these).
- **Fix:** none. The help says to pass only the columns the function reads, or use built-ins for whole-row hashes / JSON.
- **Ex:** `df.withColumn("k", row_key(F.struct("*")))` → `df.withColumn("k", row_key(F.col("id"), F.col("name")))`

#### UDF014 `udf-in-join-condition`
perf · warning · on · T1 · none · new
- **Msg:** `UDF f() in a join condition — Spark can't derive a join key from it, so it runs a nested-loop / cartesian join and calls Python once per row PAIR.`
- **Why:** Spark extracts join keys only from equalities. An opaque Python function of both sides gives none, so the join becomes a Cartesian product / BroadcastNestedLoopJoin and the UDF is evaluated for every pair of rows (130k x 20m = 2.6 trillion Python calls in the cited question). PRF004 stays quiet because opaque calls are unknown to it.
- **Detect:** `join` on a receiver the oracle types as `DataFrame`: the `on` argument is split into ANDed conjuncts (`&` chains, list/tuple elements, single-assignment Column variables up to 3 deep, `F.expr` SQL text). Fires when some conjunct calls a UDF (a value typed `UserDefinedFunction`: `F.udf`, `@udf`, `pandas_udf`; or SQL text calling a name registered with `spark.udf.register` in the file) and no conjunct is a usable equality or opaque. Also `a.crossJoin(b).where/filter(...)` and condition-less `a.join(b).filter(...)` when the UDF's arguments read columns of two frames (`a.x`, `b["y"]`). One side bounded or `broadcast(...)` → info.
- **FP:** Low. Not reported: an equality conjunct next to the UDF (Spark keys on it, then applies the UDF per candidate pair), an equality with a one-frame UDF on one side (`a.k == norm(b.k)` is still a key), an opaque helper or unresolved Column variable next to it, unknown receivers, filters over a cross join whose UDF reads one frame (Spark pushes those below the join; UDF002 covers them). Overlap: UDF014 wins over PRF032 (UDF ORed with an equality) and UDF002 (UDF filter over a cross join) at the same call.
- **Fix:** none. The help suggests blocking on a cheap equality key (postcode, token, `floor(x / bin)`) and filtering the candidate pairs with the UDF, or built-ins (`levenshtein`, `soundex`).
- **Ex:** `a.join(b, fuzzy(a.name, b.name))` → `a.join(b, F.soundex(a.name) == F.soundex(b.name)).where(fuzzy(a.name, b.name))`

### IOD — I/O & Delta

#### IOD001 `read-schema-inference`
perf · warning · on · T1 · none · CY018, PA F019
- **Msg:** `CSV/JSON read without an explicit schema — inference scans the data an extra time and the schema drifts.`
- **Why:**
  - `inferSchema` (CSV) and JSON inference trigger an eager job over the input (or `samplingRatio` of it) before the real read.
  - Schemas change silently when files change.
  - `mergeSchema=true` on Parquet reads every footer.
- **Detect:**
  - A `DataFrameReader`/`DataStreamReader` chain with format csv/json (method or `format()`/`load`) and no `.schema(...)` / `schema=`, or with `inferSchema=True` / `.option("inferSchema","true")`.
  - `.option("mergeSchema","true")` on parquet reads.
  - Streaming file sources without a schema → error, because they fail at runtime unless `spark.sql.streaming.schemaInference` is set.
  - Not Auto Loader with `cloudFiles.schemaLocation`.
- **FP:** CSV with `header` and no inferSchema reads all-string columns: no extra scan, but types are wrong. Report it as info: "all columns StringType".
- **Not reported:** a batch read only asked for its schema (`spark.read.json(sample).schema`, `.columns`, `.dtypes`, `.printSchema()`): inferring the schema is the point there (often to pass it to a streaming reader). A schema taken after a transformation, or from a name that is also used for data, is still reported.
- **Ex:** `spark.read.option("header",True).option("inferSchema",True).csv(p)` → `spark.read.schema(SCHEMA).option("header",True).csv(p)`

#### IOD002 `jdbc-read-unpartitioned`
perf · warning · on · T1 · none · new
- **Msg:** `JDBC read without partitionColumn/lowerBound/upperBound/numPartitions (or predicates) runs on a single connection/task.`
- **Why:** The whole table is pulled through one executor core while the rest of the cluster idles. A major cause of slow, expensive ingestion jobs.
- **Detect:** `spark.read.jdbc(url, table)` without `column/lowerBound/upperBound/numPartitions` or `predicates=`. Also `format("jdbc")` chains missing the `partitionColumn` option (and the companion options). Also flags `fetchsize` unset as info (driver defaults are often 10 rows for Oracle).
- **FP:** A `query`/`dbtable` subquery that is known to be small (e.g. contains `WHERE id = ?` or `LIMIT`) → info. Lakehouse Federation or `remote_query` are alternatives.
- **Ex:** `spark.read.jdbc(url,"orders",properties=p)` → `spark.read.jdbc(url,"orders",column="id",lowerBound=1,upperBound=n,numPartitions=32,properties=p)`

#### IOD003 `coalesce-one-before-write`
perf · warning · on · T1 · unsafe · CY013, PA S007
- **Msg:** `coalesce(1)/repartition(1) before write funnels the whole output through one task.`
- **Why:** A single-core write of the full output. With `coalesce(1)` the upstream stage collapses to one task too (no shuffle boundary).
- **Detect:** `coalesce(1)` / `repartition(1)` in a writer chain (`.write…save/saveAsTable/parquet/csv/json/insertInto` or `.writeTo`).
- **FP:** A small export for a single-file consumer (CSV for a business user): the source is `bounded`/`reduced` or built from driver-side data (`spark.createDataFrame(pdf)`) → info. Suggest `repartition(1)` over `coalesce(1)` so the upstream stays parallel.
- **Fix (unsafe):** For Delta targets, remove it.
- **Ex:** `df.coalesce(1).write.format("delta").save(p)` → `df.write.format("delta").save(p)`

#### IOD004 `insert-into-positional`
correctness · warning · on · T1 · unsafe · CY036, ruff#7272
- **Msg:** `insertInto() matches columns by position` / `insertInto(overwrite=True) may overwrite the whole table — use writeTo().append()/overwritePartitions().`
- **Why:** Column order mismatches silently write data into the wrong columns. `insertInto(..., overwrite=True)` with static `partitionOverwriteMode` truncates every partition.
- **Detect:** A `DataFrameWriter.insertInto(t[, overwrite])` call. Also `.mode("overwrite").insertInto`.
- **Fix (unsafe):** `df.write.insertInto(t)` → `df.writeTo(t).append()`; overwrite → `df.writeTo(t).overwritePartitions()`. The semantics differ (by-name matching), hence unsafe.
- **Ex:** `df.write.insertInto("db.t", overwrite=True)` → `df.writeTo("db.t").overwritePartitions()`

#### IOD006 `write-in-loop`
perf · warning · on · T2 · none · new
- **Msg:** `Write/append inside a loop — N small commits/files; build one DataFrame and write once.`
- **Why:**
  - Per-iteration writes create many small files and many Delta commits (log growth, OPTIMIZE debt).
  - Each iteration re-plans and re-scans.
  - Concurrent-append conflicts.
- **Detect:** Writer terminal (save/saveAsTable/insertInto/writeTo.*/merge.execute) inside a for/while body. The target is the same name or table across iterations (constant-folded), or the iterable is data-derived (e.g. `for k in keys_from_collect`).
- **FP:** Loops over distinct target tables (orchestration) → ACT005 info only, not this rule.
- **Scope:** append-only writes (`mode("append")`, `mode="append"`, `insertInto` without overwrite, `writeTo(..).append()`) whose target (table/path argument, `option("path")`, `writeTo` table) does not depend on the loop variable or on names rebound in the loop body. Overwrites (including chunked `replaceWhere` backfills), unknown modes and per-iteration targets stay ACT005 only; at the same site IOD006 wins over ACT005. Info when every enclosing loop is a literal of ≤ 10 items. Not covered yet: Delta `merge(..).execute()` in loops.
- **Ex:** `for d in days: compute(d).write.mode("append").saveAsTable("t")` → `reduce(DataFrame.unionByName, [compute(d) for d in days]).write.mode("append").saveAsTable("t")` (or one set-based compute).

#### IOD015 `unsplittable-compressed-read`
perf · info · on · T1 · none · new
- **Msg:** `Reading gzip-compressed csv (/raw/x.csv.gz): gzip is not splittable, so each file is read by a single task whatever its size.`
- **Why:** gzip streams cannot be split, so a single large `.gz` becomes a one-core stage and a skewed partition downstream.
- **Detect:** `csv` / `json` / `text` / `load` on a `DataFrameReader` or `DataStreamReader` (format from the method, `format(..)`, `format=` or Auto Loader `cloudFiles.format`) whose path argument (literal, f-string, one-binding name, or a list of them) ends in `.gz` / `.gzip` (globs such as `*.csv.gz` included).
- **FP:** Medium: many small gz files parallelise fine and file sizes are unknown statically, hence info. pandas `read_csv("x.gz")` and RDD `textFile` are not reported.
- **Ex:** `spark.read.csv("/raw/big.csv.gz")` → re-compress as bzip2 / split upstream / land once as Delta; otherwise `spark.read.csv(p).repartition(64)`.

#### IOD016 `json-schema-inference-via-rdd`
perf · warning · on · T2 · none · new
- **Msg:** `spark.read.json over an RDD without a schema: an extra full job ships every row through Python and infers the schema over all of it before the real read (and df.rdd is unsupported on Spark Connect / serverless).`
- **Why:** The popular recipe for a JSON string column's schema (`spark.read.json(df.rdd.map(lambda r: r.payload)).schema`) converts every row to a Python object and back, then infers types over the whole column, before `from_json` reads the data again.
- **Detect:** `json(...)` on a `DataFrameReader` (oracle Certain) whose first argument (`path=` too) is typed RDD (e.g. `df.rdd.map(..)`, `.rdd.flatMap(..)`, a name bound to one), with no schema in the call (`schema=` / 2nd positional) or the reader chain (`.schema(..)`, `option("schema", ..)`). `sc.parallelize([literal list])` (tests, examples) is exempt. At the same call it wins over IOD001.
- **FP:** Low: the pattern is explicit; tiny RDDs from `sc.parallelize` over a literal list (directly or through one name) are exempt. `samplingRatio` still reports (every row still goes through Python).
- **Ex:** `spark.read.json(df.rdd.map(lambda r: r.payload)).schema` → an explicit DDL schema, `F.schema_of_json` on one sample value, or `parse_json` into VARIANT on Databricks.

#### IOD017 `jdbc-write-unbounded-connections`
perf · info · on · T1 · none · new
- **Msg:** `JDBC write without numPartitions opens one database connection per partition at once (200 after a shuffle), which can exhaust the database's connection limit and stall or fail the write.`
- **Why:** Each partition of the written frame opens its own JDBC connection, all concurrently. After a shuffle that is `spark.sql.shuffle.partitions` (200) connections, beyond most databases' limits (MySQL's default is 151), so the write throttles, retries or fails while the cluster idles. `numPartitions` caps the connections (Spark coalesces to it). The default `batchsize` of 1000 rows per round trip is the second common slow-write cause; the message mentions it when unset.
- **Detect:** a JDBC write on a `DataFrameWriter` (`df.write.jdbc(...)`, `df.write.format("jdbc")...save()`, `save(format="jdbc")`, through a stored writer) whose settings (via `IoSettings::write`: options, `options(**dict)`, the `properties` dict) are complete and lack `numPartitions`. Not reported when the frame was given a small or unknown explicit partition count (`coalesce(n)` / `repartition(n)` with n <= 64 or a variable), followed back through narrow transformations and names bound once; `repartition("col")`, `repartition(400)` or a shuffle after the coalesce still report. Opaque option dicts / `properties` values, or a stored writer configured by a separate statement, are skipped. Warning inside a loop.
- **FP:** Low-medium: a small frame with few partitions is harmless (the partition count is unknown statically), hence info.
- **Ex:** `df.write.jdbc(url, "events", mode="append")` → `df.write.option("numPartitions", 8).option("batchsize", 10000).jdbc(url, "events", mode="append")`.

#### IOD018 `repair-table-in-job`
perf · info · on · T1 · none · new
- **Msg:** `Partition repair (REPAIR TABLE / RECOVER PARTITIONS) lists every partition directory of the table on each run, so the job slows down as the table grows even when it wrote one new partition.`
- **Why:** Partition recovery walks the whole table directory tree on every run (one storage listing per partition), so a daily job that adds one partition pays for all of them, and the cost grows with the table. Delta tables never need it.
- **Detect:** `spark.sql(...)` on a session (literal, f-string, `sqlQuery=`, a name bound once) whose statement is `MSCK REPAIR TABLE t`, `REPAIR TABLE t [ADD|DROP|SYNC PARTITIONS]` or `ALTER TABLE t RECOVER PARTITIONS` (keywords outside strings and comments, via `SqlText::repairs_partitions`), and `spark.catalog.recoverPartitions(t)`. Warning inside a loop. `REPAIR TABLE t SYNC METADATA` (Unity Catalog metadata sync) is not reported.
- **FP:** Low-medium: a one-off backfill or migration script legitimately repairs once, hence info.
- **Ex:** `spark.sql("MSCK REPAIR TABLE lake.events")` → `spark.sql("ALTER TABLE lake.events ADD IF NOT EXISTS PARTITION (dt='2024-01-01')")`, or convert the table to Delta.

#### IOD019 `small-max-records-per-file`
perf · info · on · T1 · none · new
- **Msg:** `maxRecordsPerFile=N caps each output file at N rows: every write task splits its output into many small files (slower listing and opening for every reader, weaker compression and statistics, more compaction work).`
- **Why:** A row cap splits each task's output into ceil(rows / cap) files. Small caps (tens of thousands of rows) produce many small files: every later reader pays listing and per-file open cost, Parquet compression and min/max statistics get worse, and Delta compaction has more to rewrite. Databricks recommends against `maxRecordsPerFile` unless necessary.
- **Detect:** a writer terminal (`DataFrameWriter`, `writeTo(..)` V2, `DataStreamWriter.start/toTable`) whose settings (via `IoSettings::of`: `option`, `options(k=v)`, `options(**dict)`, terminal keywords, a stored writer; case-insensitive key) set `maxRecordsPerFile` to a constant 1..99,999 (int or string literal, or a name bound once to one); and the session key `spark.sql.files.maxRecordsPerFile` written with such a value through the config-write matcher (`conf.set`, builder `config`, `SparkConf`, SQL `SET`; a later write of the key makes it a note). 0 / negative (unlimited), >= 100,000 and unknown values are quiet.
- **FP:** Low-medium: very wide rows or a downstream reader with a hard row limit can justify a cap, hence info and the 100,000 threshold.
- **Ex:** `df.write.option("maxRecordsPerFile", 10000).parquet(p)` → `df.write.parquet(p)` (Delta: optimized writes / `delta.targetFileSize`).

#### IOD020 `zorder-many-columns`
perf · info · on · T1 · none · new
- **Msg:** `SQL ZORDER BY over N columns: Z-ordering interleaves every column, so each extra one widens the per-file value range of all the others; data skipping weakens for every column while OPTIMIZE still rewrites the data.`
- **Why:** Z-order maps all keys onto one space-filling curve, so locality per key falls as keys are added: with four or more keys each file spans a wide range of every key and min/max file skipping prunes little, while each OPTIMIZE still rewrites the data it touches. Databricks recommends at most three Z-order columns (DBSQL rejects more than nine).
- **Detect:** `spark.sql(...)` on a session (literal, f-string, `sqlQuery=`, a name bound once) whose `ZORDER BY (..)` list (parentheses optional; top-level items counted, quotes / nested calls respected, via `SqlText::zorder_columns`) has more than `zorder-max-columns` (default 3; 0 disables) columns; and `<DeltaTable>.optimize()[.where(..)].executeZOrderBy(..)` counting string literals, names typed `str` and literal lists/tuples. `*cols`, list variables and an `optimize()` builder stored in a name are skipped.
- **FP:** Low-medium: four filter columns can still pay off on very large tables, hence info and the configurable threshold.
- **Ex:** `OPTIMIZE t ZORDER BY (user_id, event_type, country, device)` → `OPTIMIZE t ZORDER BY (user_id, event_type)`, or `ALTER TABLE t CLUSTER BY (...)` (liquid clustering).

### CFG — Spark config

#### CFG001 `aqe-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `Adaptive Query Execution disabled (spark.sql.adaptive.enabled=false): no partition coalescing, skew-join splitting or runtime broadcast conversion for the rest of the session.` (one message per key: `AQE partition coalescing disabled: …` for `…coalescePartitions.enabled`, `AQE skew-join handling disabled: …` for `…skewJoin.enabled`)
- **Why:** Loses partition coalescing, skew-join splitting and dynamic broadcast conversion. Usually means more tasks, worse skew, and bigger clusters. Also flags `…adaptive.coalescePartitions.enabled=false` and `…skewJoin.enabled=false`.
- **Detect:** `spark.conf.set(k, v)` / `SparkSession.builder.config(k, v)` (also `config(map={...})`) / `SparkConf().set(k, v)` (`setIfMissing`, `setAll`) / `spark.sql("SET k=v")` with those keys (any casing) and a false value (`False`, `"false"` in any casing, or a name bound once to one). Shared with all config-value rules (CFG002, CFG007-CFG022): when a later write in the same file sets the key to a value the rule does not report (or one it cannot read), or calls `spark.conf.unset(k)`, the earlier write is a scoped workaround: it stays reported, as info, with "(Set back later in this file, …)" added to the message.
- **Ex:** remove the line.

#### CFG002 `broadcast-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.autoBroadcastJoinThreshold=-1 disables automatic broadcast joins for the whole session: every small-table join becomes a sort-merge join that shuffles the large side.` (for the two `adaptive` keys: `… disables runtime (AQE) broadcast conversion …`)
- **Why:** Every small-dimension join becomes a sort-merge join with a full shuffle of the large side. Usually it was added to work around one bad join.
- **Detect:** Key `spark.sql.autoBroadcastJoinThreshold` (or `spark.sql.adaptive.autoBroadcastJoinThreshold` / `spark.databricks.adaptive.autoBroadcastJoinThreshold`) set to a size ≤ 0 (`-1`, `0`, `"0"`, `"-1b"`; strings read like Spark's byte sizes), through the same config-write matcher and "set back later" info downgrade as CFG001.
- **Ex:** Remove it; for the one problem join use `hint("merge")` or `hint("shuffle_hash")`.

#### CFG003 `static-conf-at-runtime`
correctness · error · on · T0 · none · new (Databricks Spark config limitations)
- **Msg:** `spark.executor.memory cannot be changed at runtime — this has no effect (and raises CANNOT_MODIFY_CONFIG on Spark Connect/serverless).` In a notebook with a running session: `spark.executor.memory is a static setting: SparkSession.builder.config() on the already-running notebook session ignores it.` (or `… a SparkConf built in a notebook cannot change the already-running session …`).
- **Why:** The user believes they tuned the job. In fact nothing changed (Classic), or it fails (Connect).
- **Detect:** `spark.conf.set` / `spark.conf.unset` / SQL `SET` (any casing of the key) with a key in the static/cluster-level table: `spark.executor.*`, `spark.driver.*`, `spark.dynamicAllocation.*`, `spark.serializer`, `spark.jars*`, `spark.submit.*`, `spark.master`, `spark.task.cpus`, `spark.cores.max`, `spark.memory.*`, `spark.sql.extensions`, `spark.sql.catalogImplementation`, `spark.sql.warehouse.dir`, `spark.default.parallelism`, `spark.local.dir`, `spark.yarn.*`, `spark.kubernetes.*` and `spark.databricks.cluster.*` → error. `SparkSession.builder.config(...)` / `SparkConf().set(...)` with such a key in a notebook whose session already runs → warning (ignored); in a plain `.py` module they are never reported (there the builder normally creates the session). `notebook = "on"` makes a module count as a running notebook. The session already runs in a Databricks notebook (source format, or `.ipynb` with Databricks metadata); in any other notebook only with evidence in the file: `spark` / `sc` / `sqlContext` / `dbutils` / `displayHTML` used without being defined, an earlier `getOrCreate()`, or `display` used before the builder call in a notebook that also names Databricks storage (`dbfs:/`, `/dbfs/`, `/Volumes/`, `/Workspace/` paths); IPython's own `display` alone is not evidence.
- **FP:** A plain Jupyter notebook whose first Spark statement is the builder: there `SparkSession.builder.config(...).getOrCreate()` creates the session and the static settings apply. Not reported.
- **Ex:** move it to the cluster or job compute config.

#### CFG007 `broadcast-threshold-too-high`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.autoBroadcastJoinThreshold raised to 2 GB: every table estimated under that size is collected to the driver and copied to every executor (driver OOM / broadcast timeouts; stale statistics let big tables through).` At ≥ 8 GB: `… set to 8 GB: at or above Spark's 8 GB broadcast limit, so any join that qualifies fails …` (error).
- **Why:** Each automatic broadcast is collected to the driver, hashed there and shipped to every executor. A threshold in the GB range turns ordinary joins into driver OOMs and broadcast timeouts, and missing or stale statistics let large tables under the estimate. Above Spark's hard 8 GB limit a qualifying join fails outright ("Cannot broadcast the table that is larger than 8GB").
- **Detect:** The CFG002 keys set to a size ≥ `broadcast-threshold-max` (`[tool.sparklint]` option, default `"1g"`, `"0"` = off). Values are parsed like Spark's byte strings (`"512m"`, `"2g"`, `"16gb"`, plain bytes) and folded from int arithmetic (`2 * 1024 ** 3`, `str(...)`, `1 << 31`). Error at ≥ 8 GB.
- **FP:** Low: the value is a literal. Clusters with very large drivers can raise `broadcast-threshold-max`; a write that is set back later in the file is info.
- **Ex:** `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "2g")` → keep the default; `big.join(F.broadcast(small_filtered), "k")`.

#### CFG008 `predicate-pushdown-disabled`
perf · warning · on · T1 · none · new
- **Msg:** `spark.sql.parquet.filterPushdown=false: Parquet scans stop using row-group min/max statistics to skip data, so every filtered read decodes all of it.` (`ORC scans` for the ORC key) / `JDBC read with pushDownPredicate=false: filters are not sent to the database, so the whole table is copied over the network and filtered in Spark.`
- **Why:** With pushdown off, Parquet/ORC readers cannot skip row groups by their statistics, and a JDBC source ships the whole table to Spark to be filtered there: a full scan (for JDBC a full table transfer) for every filtered query. Usually a leftover workaround for one old reader bug.
- **Detect:** `spark.sql.parquet.filterPushdown` / `spark.sql.orc.filterPushdown` set false through the config-write matcher described under CFG001 (`False` or `"false"` in any casing; a write set back later in the file is info). JDBC: a reader chain with `format("jdbc")` (or `load(format="jdbc", ...)`) whose `option` / `options` / `load` keywords set `pushDownPredicate` (key in any casing) to `False` or `"false"` in any casing, or `spark.read.jdbc(..., properties={...})` with that key (a literal dict or a name bound to one). Options set by a separate `reader.option(...)` statement are not followed, so that form is not reported.
- **FP:** Low: exact keys and literal values. A source with a buggy pushdown deserves a noqa with the reason.
- **Ex:** `spark.read.format("jdbc").option("pushDownPredicate", False)...load()` → drop the option, or move the filter into the `query` subquery.

#### CFG009 `delta-file-compaction-disabled`
perf · info · on · T0 · none · new
- **Msg:** `spark.databricks.delta.optimizeWrite.enabled=false turns off optimized writes for every Delta table this session writes: appends, MERGEs and streaming writes leave many small files (slower reads, a bigger Delta log, OPTIMIZE debt).` (`auto compaction` for the autoCompact keys; `every Delta table this session creates` for the `properties.defaults` keys) / `Table property delta.autoOptimize.autoCompact = false: writes to this table stop sizing their files, so appends and streaming writes leave many small files (slower reads, OPTIMIZE debt).` (one finding per statement, naming every property set false)
- **Why:** The session switches override every table the job writes, so appends, MERGEs and streaming micro-batches produce many small files: slower reads, a bigger Delta log and OPTIMIZE debt. Databricks recommends optimized writes plus auto compaction.
- **Detect:** `spark.databricks.delta.optimizeWrite.enabled`, `spark.databricks.delta.autoCompact.enabled` and their `spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite` / `.autoCompact` defaults set false through the config-write matcher described under CFG001. SQL: `spark.sql` text whose `TBLPROPERTIES (...)` list sets `delta.autoOptimize.optimizeWrite` / `delta.autoOptimize.autoCompact` (quoted or not) to `false` or `'false'` (ALTER and CREATE).
- **FP:** Low: exact keys and literal values. Latency-bound streams sometimes turn it off on purpose (hence info); a noqa with the reason documents that.
- **Ex:** `spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")` → remove it; if one table needs it off, set the table property there and schedule OPTIMIZE.

#### CFG010 `disk-cache-disabled`
perf · info · on · T0 · none · new
- **Msg:** `spark.databricks.io.cache.enabled=false turns off the Databricks disk cache: every later read of the same Parquet/Delta files goes back to cloud storage instead of the workers' local SSDs.`
- **Why:** The disk cache keeps local SSD copies of remote Parquet/Delta files, so repeated reads (joins against the same dimension, iterative jobs, several queries over one table) skip cloud storage. Databricks recommends it for most workloads and enables it on SSD worker types. Switching it off in code is usually a leftover from a cold-read benchmark.
- **Detect:** `spark.databricks.io.cache.enabled` set false through the config-write matcher described under CFG001 (conf.set, builder config / `map=`, `SparkConf().set`, SQL `SET`; `False` or `"false"` in any casing). A write set back later in the file gets the "(Set back later in this file, ...)" note.
- **FP:** Low: exact key and literal value. Benchmarks that want cold reads noqa it with the reason.
- **Ex:** `spark.conf.set("spark.databricks.io.cache.enabled", "false")` → remove it.

#### CFG011 `arrow-batch-size-extreme`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.execution.arrow.maxRecordsPerBatch=1000000 (default 10000): Arrow batches live in executor memory overhead, so big batches of wide rows get Python workers killed (exit 137 / container exceeded memory) on every pandas UDF in the job.` / `…=0 removes the Arrow batch limit: each pandas UDF / applyInPandas / mapInPandas / toPandas batch holds a whole partition outside the JVM heap (…)` / `…=100 (default 10000): batches this small spend more time crossing the JVM/Python boundary than computing, losing most of the vectorised speed-up.` (info)
- **Why:** Arrow batches are allocated outside the JVM heap, so the batch size multiplies straight into memory overhead on every pandas UDF, `applyInPandas`, `mapInPandas` and Arrow `toPandas` in the job. Too large shows up as exit 137 / "container killed" with no JVM error; 0 or negative removes the limit (a whole partition per batch). Too small spends the time crossing the JVM/Python boundary.
- **Detect:** `spark.sql.execution.arrow.maxRecordsPerBatch` written through the config-write matcher described under CFG001 with an integer value (a literal, a string such as `"100"`, or constant arithmetic such as `10 ** 6`): >= 100000 or <= 0 = warning, 1-499 = info (bands are fixed; no option). Other values (`"1e6"`, non-constant values) are skipped. A write set back later in the file to a value in the 500-99999 range is info with a note.
- **FP:** Low-med: narrow rows tolerate bigger batches, so a deliberate value can be noqa'd with the reason.
- **Ex:** `spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 1000000)` → keep the default (or lower it for wide rows) and size `spark.executor.memoryOverhead`.

#### CFG012 `file-partition-size-extreme`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.files.maxPartitionBytes set to 4 GB (default 128 MB): scan tasks this big spill to disk or run executors out of memory, and too few of them leave cores idle.` / `… set to 1 MB (default 128 MB): scan partitions this small turn every stage into thousands of tiny tasks (scheduling overhead, many small output files).` (info; "post-shuffle" and default 64 MB for the advisory size) / `spark.sql.files.openCostInBytes=0 (default 4 MB): small files count as free to open, so one scan task gets packed with thousands of them (slow listing-bound straggler tasks).` (info)
- **Why:** `spark.sql.files.maxPartitionBytes` sizes scan tasks and `spark.sql.adaptive.advisoryPartitionSizeInBytes` sizes post-shuffle partitions after AQE coalescing. Far above the default, each task holds gigabytes (spill, OOM, idle cores); far below, the job drowns in scheduling overhead and small output files. `spark.sql.files.openCostInBytes` <= 0 makes small files look free, so one task gets packed with thousands of them.
- **Detect:** the config-write matcher with Spark's byte-string parser (`"16m"`, `"1g"`, `134217728`, `"128MB"`): maxPartitionBytes > 1 GB or advisory size (also the Spark 3.0 name `spark.sql.adaptive.shuffle.targetPostShuffleInputSize`) > 2 GB = warning; either < 8 MB = info; openCostInBytes <= 0 = info. A size of 0 or below for the two partition-size keys, non-constant and unparseable values are skipped; a hit followed later in the file by a write of the same key that is not reported (set back) becomes info with a note.
- **FP:** Low-med: a deliberately small scan size for a CPU-heavy UDF stage is a known trick (hence info, noqa with the reason).
- **Ex:** `spark.conf.set("spark.sql.files.maxPartitionBytes", "4g")` → keep the default or a few hundred MB.

#### CFG013 `aqe-min-partition-num-set`
perf · info · on · T0 · none · new
- **Msg:** `spark.sql.adaptive.coalescePartitions.minPartitionNum=400 overrides AQE's minPartitionSize: AQE can no longer merge small post-shuffle partitions below this count, so small shuffles keep many tiny tasks (and small output files).`
- **Why:** Databricks' AQE page says "Don't set this property": it overrides `coalescePartitions.minPartitionSize`, so a value copied from an old cluster keeps every small shuffle at hundreds or thousands of tiny tasks and small output files.
- **Detect:** `spark.sql.adaptive.coalescePartitions.minPartitionNum` (or the Spark 3.0 name `spark.sql.adaptive.minNumPostShufflePartitions`) set to any constant value through the config-write matcher; `unset` is quiet, and a set followed later in the file by `unset` gets a "set back later" note.
- **FP:** Low: exact key, rarely right.
- **Ex:** `spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", 400)` → remove it.

#### CFG014 `engine-acceleration-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.codegen.wholeStage=false turns off whole-stage code generation: every operator of every query runs row by row through virtual iterator calls instead of one compiled loop, multiplying CPU time for the whole session.` / `spark.sql.parquet.enableVectorizedReader=false makes every Parquet/Delta scan decode values row by row instead of in columnar batches, multiplying CPU time for the whole session.` (same wording for ORC)
- **Why:** Both switches are workarounds for one old error (the 64 KB generated-method limit, a reader type bug) that stay on for every later query of the session and multiply CPU time of scans and operators.
- **Detect:** `spark.sql.codegen.wholeStage`, `spark.sql.parquet.enableVectorizedReader` or `spark.sql.orc.enableVectorizedReader` set false (`"false"` in any casing or Python `False`) through the config-write matcher (conf.set, builder config / `map=`, SparkConf, SQL `SET`, constant names); a write set back later in the file is info. Other codegen keys (`codegen.fallback`, `maxFields`) and the in-memory columnar reader key are not reported.
- **FP:** Low: exact keys and literal values; a genuine workaround deserves a set-back after the query or a noqa with the reason.
- **Ex:** `spark.conf.set("spark.sql.codegen.wholeStage", "false")` → remove it.

#### CFG015 `dynamic-pruning-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `spark.databricks.optimizer.dynamicFilePruning=false turns off dynamic file pruning: joins and MERGEs stop skipping the Delta files of the big table that cannot match the small side, so they read every file.` / `spark.sql.optimizer.dynamicPartitionPruning.enabled=false turns off dynamic partition pruning: a join on a partition column reads every partition of the big table instead of only the ones the small side selects.`
- **Why:** Dynamic file/partition pruning pushes the small side's join keys into the scan of the big table at run time; without it a star-schema join or a MERGE scans the whole fact table. Databricks advises against disabling it except to compare plans.
- **Detect:** `spark.databricks.optimizer.dynamicFilePruning` or `spark.sql.optimizer.dynamicPartitionPruning.enabled` set false through the config-write matcher; a write set back (or unset) later in the file is info. The DFP size/file thresholds are not reported.
- **FP:** Low: exact keys and literal values.
- **Ex:** `spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")` → remove it.

#### CFG016 `broadcast-timeout-raised`
perf · info · on · T0 · none · new
- **Msg:** `spark.sql.broadcastTimeout raised to 1 h (default 300 s): a broadcast that needs longer is usually far bigger than estimated or built by a slow query, and the longer wait keeps the cluster busy on it (AQE broadcast stages ignore this setting since Spark 3.2).` / `spark.sql.broadcastTimeout=-1 (default 300 s): a negative value means a broadcast never times out, so an oversized or stuck broadcast holds the whole cluster until someone kills the job.`
- **Why:** Raising the timeout is the common answer to `Futures timed out after [300 seconds]`, but the timeout is a symptom: an oversized broadcast side (stale statistics, a forced hint) or a slow build query. A longer wait (or a negative value, which never times out) lets that broadcast hold the cluster longer instead of failing fast.
- **Detect:** `spark.sql.broadcastTimeout` written through the config-write matcher (conf.set, builder config / `map=`, SparkConf, SQL `SET`, constant names) with a constant duration above 300 seconds (plain seconds or Spark time strings `600s`, `10min`, `1h`) or a negative value; a write set back later in the file is a note. Non-constant and unparseable values are skipped.
- **FP:** Low-med: an intentionally small but slow-to-build relation on a non-AQE path is a legitimate reason (noqa with the reason).
- **Ex:** `spark.conf.set("spark.sql.broadcastTimeout", "3600")` → shrink/filter the broadcast side, drop the forced hint or `ANALYZE TABLE`; keep the default.

#### CFG017 `driver-result-limit-removed`
perf · warning · on · T0 · none · new
- **Msg:** `spark.driver.maxResultSize=0 removes the driver's result limit (default 1g): an oversized collect(), toPandas() or broadcast then crashes the driver with an out-of-memory error, losing the session while the cluster sits idle, instead of failing that one job cleanly.` / `spark.driver.maxResultSize raised to 64 GB (default 1g): results this large are serialised and held on the driver, which usually runs out of memory or stalls in garbage collection before the limit is reached.` (info)
- **Why:** The limit exists to protect the driver from out-of-memory on large collects and broadcasts. Removing it (0 or negative = unlimited) turns a clean "job aborted" error into a driver crash: the session and its cached state are lost, the job restarts, and all workers wait meanwhile. A limit of 16 GB or more is effectively the same on typical drivers.
- **Detect:** `spark.driver.maxResultSize` written through a builder (`config`, `map=`, a dict bound to a name) or SparkConf (`set`, `setIfMissing`, `setAll`), constant keys/values through names, with a constant size <= 0 (warning) or >= 16 GB (info); sizes through Spark's byte-string parser, unparseable or non-constant values skipped. Runtime `spark.conf.set` / SQL `SET` of the key are not reported (a launch-time key, no effect there). A later write of the key makes a hit a note.
- **FP:** Low: exact key and value; a driver deliberately sized for huge results deserves a noqa with the reason.
- **Ex:** `SparkSession.builder.config("spark.driver.maxResultSize", "0")` → keep a limit and aggregate / limit / write the result instead of collecting it.

#### CFG018 `io-compression-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `spark.shuffle.compress=false: shuffle map output is written and sent uncompressed: several times more shuffle bytes over the network and on local disk.` / `spark.sql.inMemoryColumnarStorage.compressed=false: cached tables and DataFrames are stored uncompressed: …`
- **Why:** All four switches default to true ("Generally a good idea" in the Spark docs). Off, they multiply shuffle network and disk bytes, spill volume, broadcast transfer size or cached-table memory (more evictions and recomputes).
- **Detect:** `spark.shuffle.compress`, `spark.shuffle.spill.compress`, `spark.broadcast.compress` set false through a builder (`config`, `map=`) or SparkConf, and `spark.sql.inMemoryColumnarStorage.compressed` set false through any config write (also runtime conf.set / SQL `SET`); a write set back later in the file is a note. The three core keys written on a running session are not reported (Spark rejects or ignores them there). `spark.rdd.compress` (default false) and file-format codecs are not reported.
- **FP:** Low: exact keys and literal values.
- **Ex:** `SparkSession.builder.config("spark.shuffle.compress", "false")` → remove it; to trade CPU for size pick `spark.io.compression.codec`.

#### CFG019 `deletion-vectors-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `Table property delta.enableDeletionVectors = false: every DELETE, UPDATE and MERGE rewrites each whole data file that holds a changed row instead of writing a small row bitmap (write amplification on every change).` / `spark.databricks.delta.properties.defaults.enableDeletionVectors=false creates every new Delta table of this session without deletion vectors: …`
- **Why:** With deletion vectors a change marks the deleted/updated rows in a small bitmap file; without them Delta copies every untouched row of each affected Parquet file. MERGE/UPDATE-heavy jobs then rewrite far more bytes than they change.
- **Detect:** `spark.sql` text whose `TBLPROPERTIES (..)` (CREATE / ALTER .. SET) sets `delta.enableDeletionVectors` false (via `sparklint_semantic::sql` properties), and the session default `spark.databricks.delta.properties.defaults.enableDeletionVectors` set false through any config write (conf.set, builder, SparkConf, SQL `SET`); a session write set back later in the file is a note. Not yet: writer V2 `tableProperty(..)` and `DeltaTable` builder `.property(..)`.
- **FP:** Low: exact keys and literal values; the help names the legitimate case (old readers / external engines without deletion-vector support).
- **Ex:** `ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')` → remove it, or noqa it with the reader that needs it.

#### CFG020 `file-statistics-disabled`
perf · warning (info for -1) · on · T0 · none · new
- **Msg:** `Table property delta.dataSkippingNumIndexedCols = 0 writes no min/max file statistics: every filtered query reads every file (no data skipping), and ZORDER or clustering on the table prunes nothing.` / `… = -1 collects statistics on every column: on wide tables each write pays for min/max on hundreds of columns that no filter uses.` (info)
- **Why:** Data skipping, ZORDER and liquid clustering all rely on per-file min/max statistics; 0 indexed columns (or an empty stats-column list) turns every query into a full scan. -1 is the opposite extreme: write cost on wide tables.
- **Detect:** `spark.sql` `TBLPROPERTIES (..)` setting `delta.dataSkippingNumIndexedCols` to 0 / -1 or `delta.dataSkippingStatsColumns` to an empty string, and the session default `spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols` = 0 / -1 through any config write (a later write of the key makes it a note). Other values are not reported.
- **FP:** Low for 0, medium for -1 (narrow tables are fine; hence info).
- **Ex:** `TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')` → remove it, or set `delta.dataSkippingStatsColumns = 'ts,customer_id'`.

#### CFG021 `optimizer-rules-excluded`
perf · info · on · T0 · none · new
- **Msg:** `spark.sql.optimizer.excludedRules turns off Catalyst optimizer rules (PushDownPredicates) for every query of this session: lost optimizations such as filter pushdown, column pruning or join reordering make queries read and shuffle more. Unknown rule names are silently ignored, so a name copied from another Spark version does nothing.` (the adaptive key says `for every adaptive re-optimisation`; more than three names show as `A, B, C and N more`)
- **Why:** The excluded-rules lists are diagnostic knobs. Left in after a one-off workaround they remove Catalyst (or AQE re-optimisation) rules for every query of the session; and because unknown names are ignored, a list carried across Spark versions may do nothing at all.
- **Detect:** shared config-write matcher (`conf.set`, builder `config` / `map=`, `SparkConf` set/setAll/setIfMissing, SQL `SET`, constant names) on `spark.sql.optimizer.excludedRules` or `spark.sql.adaptive.optimizer.excludedRules` with a constant, non-empty rule list (up to three short rule names shown). An empty value, reads, unsets and non-constant values are not reported; a later write of the key in the file adds a scoped note.
- **FP:** Low-med: genuine workarounds for optimizer bugs exist; the help asks to scope them and say why (noqa).
- **Ex:** `spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates")` → remove it, or set it back after the affected query.

#### CFG022 `delta-target-file-size-small`
perf · warning · on · T0 · none · new
- **Msg:** `Table property delta.targetFileSize = 8 MB is below 16 MB: OPTIMIZE, auto compaction and optimized writes then aim for files that small instead of autotuning (256 MB to 1 GB by table size), so every read opens many more files and the Delta log grows with every write.` / `spark.databricks.delta.properties.defaults.targetFileSize = 8 MB gives every new Delta table of this session a fixed target file size below 16 MB: …`
- **Why:** A fixed `delta.targetFileSize` turns off Databricks' size-based file autotuning for the table. Below ~16 MB every maintenance and write path produces small files: more files to list and open per query, more Delta log entries, worse data skipping per file.
- **Detect:** `spark.sql` `TBLPROPERTIES (..)` (CREATE / ALTER) setting `delta.targetFileSize`, and the session default `spark.databricks.delta.properties.defaults.targetFileSize` through any config write (`conf.set`, SQL `SET`, builder, `SparkConf`, constant names), to a constant size (bytes or `k`/`m`/`g` units, Spark's byte-string parser) between 1 byte and 16 MB. Larger, zero, negative and unparsable values are not reported; a later write of the key in the file makes it a scoped note.
- **FP:** Low-med: deliberately small files for very MERGE-heavy tables; the help names `delta.tuneFileSizesForRewrites` as the supported route.
- **Ex:** `ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '8mb')` → remove it, or `'delta.tuneFileSizesForRewrites' = 'true'`.

### STM — Structured Streaming

#### STM001 `stream-missing-checkpoint`
correctness · error · on · T1 · none · new
- **Msg:** `writeStream started without checkpointLocation — no exactly-once, progress lost on restart.`
- **Why:**
  - Without a checkpoint the query uses a temp checkpoint, or fails on some sinks.
  - A restart reprocesses from scratch or skips data, with duplicates.
  - On Databricks the temp location may be deleted.
- **Detect:** A `DataStreamWriter` chain reaching `start()`/`toTable()` with no `.option("checkpointLocation", …)` and no `checkpointLocation=` kwarg, and the session-level `spark.sql.streaming.checkpointLocation` not set in the project (T3).
- **FP:** `format("console"|"memory")` debug sinks → ACT010 instead. Not reported when the writer chain does not start in view at `df.writeStream`: a writer passed in as a function parameter or returned by a helper may carry a checkpoint set elsewhere. A writer bound on several paths (`if`/`else`) is reported only when every path starts at `df.writeStream`, no path sets the checkpoint and the paths differ in options/format only.
- **Ex:** `.option("checkpointLocation", f"{CKPT}/orders")`

#### STM002 `trigger-once-deprecated`
perf · warning · on · T1 · safe · new (Spark 3.4 deprecation; serverless)
- **Msg:** `trigger(once=True) is deprecated — use trigger(availableNow=True).`
- **Why:**
  - `once` processes everything in one micro-batch (memory blow-ups on backlogs) and ignores rate limits.
  - `availableNow` splits the backlog into batches that respect `maxFilesPerTrigger`/`maxBytesPerTrigger`.
  - Serverless documents Once as deprecated.
- **Detect:** `DataStreamWriter.trigger(once=True)`.
- **Fix (safe):** Replace the kwarg.
- **Ex:** `.trigger(once=True)` → `.trigger(availableNow=True)`

#### STM010 `kafka-stream-uncapped-batch`
perf · warning · on · T1 · none · new
- **Msg:** `Kafka stream from the earliest offsets without maxOffsetsPerTrigger: the first micro-batch reads the whole retained topic at once.` / `Kafka stream without maxOffsetsPerTrigger: after downtime or a lagging restart, one micro-batch reads the whole backlog at once.` (info)
- **Why:** Without a cap, Spark plans each micro-batch up to the latest offset. A new query starting at `earliest` (or a restart after an outage) reads the whole retained backlog in ONE batch: hours of work in one batch, executor OOMs, a huge state-store commit, and a cluster sized for that spike. `trigger(availableNow=True)` honours the cap and drains the backlog in several batches.
- **Detect:** `DataStreamReader.load()` (oracle Certain) whose reader chain has format `kafka` (`format("kafka")` or `load(format="kafka")`, case-insensitive) and no `maxOffsetsPerTrigger` / `maxBytesPerTrigger` in `option`/`options`/`load` keywords (shared reader option walker). Warning when `startingOffsets` is `earliest` or a JSON offsets map, info otherwise. Skipped when options come from an opaque dict or `**kwargs`, the reader is configured by separate statements, or the chain does not start in view at `spark.readStream` (a reader passed in as a parameter or returned by a helper).
- **FP:** Low-med: low-volume topics never build a large backlog (the message says so). Batch `spark.read` Kafka reads are not flagged.
- **Ex:** `.option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 500_000)`

#### STM011 `heap-state-store-explicit`
perf · info · on · T0 · none · new
- **Msg:** `Streaming state store set to HDFSBackedStateStoreProvider: all state lives in the executor JVM heap, so as the number of keys grows micro-batches slow down in garbage collection and executors run out of memory. A query's provider cannot change for an existing checkpoint, so this choice is locked in.`
- **Why:** The heap-backed provider keeps every state key in executor memory; with millions of keys GC pauses make micro-batches slow and erratic and executors OOM. RocksDB keeps state off-heap (the Databricks default from DBR 17.3). The provider is fixed per checkpoint, so the choice is hard to undo.
- **Detect:** shared config-write matcher (`conf.set`, builder `config` / `map=`, `SparkConf` set/setAll/setIfMissing, SQL `SET`, constant names) on `spark.sql.streaming.stateStore.providerClass` whose value's last dotted segment is `HDFSBackedStateStoreProvider` (any case). Absence of a provider setting is not reported (the runtime default may already be RocksDB). A later write of the key in the file adds a scoped note.
- **FP:** Low: queries with small, bounded state and tests can keep the heap store (noqa).
- **Ex:** `spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")`

### COR — Correctness

#### COR001 `column-eq-none`
correctness · error · on · T1 · safe · new, mrpowers nulls
- **Msg:** `Column == None is always NULL (a filter drops every row) — use .isNull().` (`!=` → `.isNotNull()`) / `` `Column is None` is always False in Python — use .isNull(). `` (`is not` → always True, `.isNotNull()`)
- **Why:** SQL three-valued logic. `col == None` becomes `col = NULL`, which is never true. A silent empty result.
- **Detect:** A single comparison (`==`, `!=`, either operand order) between an operand that is certainly a Column (a name that only looks like a column is not enough) and the constant `None`. `is None`/`is not None` on an expression that is always a Column (a pyspark call such as `F.col("a")`, a method on a Spark object, `df.a`, `df["a"]`) is always False/True in Python, so it is flagged as well; on a variable or a call to your own helper it is the normal check for an optional Column and is not flagged. Also `F.col("x") == F.lit(None)`.
- **Fix (safe):** `==None` → `.isNull()`, `!=None` → `.isNotNull()` (the Column is parenthesised when needed). The `is` / `is not` forms get the same rewrite as an unsafe fix: the result turns from a Python bool into a Column, which is only right inside a DataFrame expression (in an `if` test it raises).
- **Ex:** `df.filter(F.col("a") == None)` → `df.filter(F.col("a").isNull())`

#### COR002 `python-bool-op-on-column`
correctness · error · on · T1 · safe · new
- **Msg:** `'and' on Columns — use & (with parentheses); Python cannot convert a Column to bool.` (`or` → `|`) / `'not' on a Column — use ~ (with parentheses); …`
- **Why:** It raises `ValueError: Cannot convert column into bool` at runtime. For mixed operands such as `flag and F.col(...)`, Python short-circuits on the plain value and silently drops one side.
- **Detect:** `and` / `or` with at least one operand that is certainly a Column (one finding for the whole chain), and `not` on a Column. Values only guessed to be Columns, pandas Series and other look-alikes are not reported.
- **Fix (safe):** Rewrite to `a & b` / `a | b` / `~a`, parenthesising every operand that is not a plain name, attribute, call or subscript.
- **Ex:** `F.col("a")>1 and F.col("b")<2` → `(F.col("a")>1) & (F.col("b")<2)`

#### COR003 `bitwise-comparison-precedence`
correctness · error · on · T1 · safe · new
- **Msg:** ``& / | bind tighter than comparisons: `a > 1 & b < 2` parses as `a > (1 & b) < 2` — parenthesise each comparison.``
- **Why:** Python parses this as a chained comparison `a > (1 & b) and (1 & b) < 2`. On Columns that either raises "Cannot convert column into bool" or computes a bitwise AND of literals. Both are silent nonsense or runtime errors.
- **Detect:** A comparison chain with an operand that is an unparenthesised `x & y` / `x | y`, where a comparison operand or a side of that `&` / `|` is certainly a Column, i.e. the AST shape of `cmp & cmp`. A bitwise expression written in its own parentheses (`a > (1 & b)`) is deliberate and not reported; plain integer/bool arithmetic is not reported.
- **Fix (safe):** Re-parenthesise from the source text: `(F.col("a") > 1) & (F.col("b") < 2)`; no fix when the chain cannot be regrouped unambiguously.

#### COR007 `discarded-transformation`
correctness · error · on · T1 · none · new
- **Msg:** `Result of df.withColumn() is discarded — DataFrames are immutable; assign it.`
- **Why:** `df.withColumn(...)`, `df.dropDuplicates()` or `df.na.fill(0)` as a bare statement does nothing. The code "looks" right and silently drops the logic.
- **Detect:** An expression statement whose value is a transformation called on a DataFrame, GroupedData or `df.na` receiver and returning a `DataFrame` or `GroupedData` (`df.withColumn(..)`, `df.groupBy("a")`, `df.groupBy("a").count()`, `df.na.fill(0)`, `df.hint("broadcast")`). Actions and calls with an effect are skipped: `cache`, `persist`, `unpersist`, `checkpoint`, `localCheckpoint`, the temp-view registrations, `printSchema`, `explain`, `show`, `display`, `foreach(Partition)`, `writeTo`, `observe`, and any call with an `inplace=` keyword. A receiver known only by its name (`orders_df`) needs a DataFrame-only method (pandas has `drop` / `fillna` / `rename` too). Error in `.py` files, warning in notebooks.
- **Not reported:** a statement directly inside a `try:` body that has an `except` handler, or directly inside `with pytest.raises(..)` / `pytest.warns(..)` / `self.assertRaises(..)` (and the `Regex` / `Warns` variants): Classic Spark analyses a transformation eagerly, so `df.select("c")` there is a deliberate probe for the error (missing column, bad argument). Statements in `except` / `else` / `finally`, in other `with` blocks (`contextlib.suppress`), or nested deeper (an `if` inside the `with`) are still reported.
- **Ex:** `df.dropDuplicates(["id"])` → `df = df.dropDuplicates(["id"])`

#### COR010 `union-by-position`
correctness · warning · on · T1 · unsafe · CY035, ruff#7272
- **Msg:** `.union() matches columns by position — a different column order silently puts values in the wrong columns; use unionByName().` (`.unionAll()` likewise); for the reduce form `reduce(DataFrame.union, …) matches columns by position — use DataFrame.unionByName.`
- **Why:** When the schemas are in a different order, values silently land in the wrong columns (and type-coerce).
- **Detect:** `DataFrame.union/unionAll(other)` where `other` is a DataFrame or of unknown type, and `functools.reduce(DataFrame.union, ...)` / `reduce(DataFrame.unionAll, ...)`. Not reported when the column order is the same by construction: a frame unioned with itself (`df.union(df)`, `self.df.union(self.df)`), or one side re-selected with the other's columns (`a.union(b.select(a.columns))`, `select(*a.columns)`, also further down an `a.union(..).union(..)` chain). Both sides selecting the same literal column names (`a.select("x", "y").union(b.select("x", "y"))`, string arguments only) → info.
- **Fix (unsafe):** → `unionByName(other)` (it raises on mismatched names, unlike `union`, so it is a behaviour change). The `reduce(..)` form has no fix.

#### COR016 `regex-escape`
correctness · warning · on · T0 · safe · CY016 (widened)
- **Msg:** `Invalid escape sequence "\d" in a non-raw string — a SyntaxWarning today, an error in future Python; use r"...".` and, for a control escape in a regex, `"\b" in a regex passed as a non-raw string is a control character, not a regex escape — use r"...".`
- **Why:** `"\bword\b"` silently becomes a backspace character, so the regex never matches (a silent wrong result). Invalid escapes like `"\d"` are a SyntaxWarning in Python 3.12+ and will become errors.
- **Detect:** Any non-raw string, bytes or f-string literal with an invalid escape (all strings, as in cylint; `\N`, `\u`, `\U` are valid in strings, invalid in bytes). Plus valid-but-suspicious escapes (`\b`, `\a`, `\f`, `\v`, `\0`) in a regex argument: the pattern of `F.regexp_replace`, `regexp_extract(_all)`, `split`, `regexp_like`, `regexp`, `rlike`, `regexp_count`, `regexp_substr`, `regexp_instr`, of `re.compile` / `match` / `search` / `fullmatch` / `sub` / `subn` / `split` / `findall` / `finditer`, and of `Column.rlike` / `Column.regexp`. SQL text (`spark.sql("... RLIKE '\bx'")`) is not checked.
- **Fix:** add the `r` prefix. Safe for an invalid escape when the literal holds no valid escape (`"\n\d"` gets no fix: the `\n` would change), has no `u` prefix and does not end in a backslash; no fix inside f-strings. Unsafe for a control escape in a regex, offered when the literal has no `\\` and no `u` prefix.

### DBX — Databricks platform & compatibility

#### DBX001 `spark-context-access`
compat · warning · on · T1 · unsafe · Databricks access-mode & serverless limitations
- **Msg:** `<expr>.sparkContext is unavailable on Spark Connect and serverless (and blocked on standard access mode before DBR 14).` The same text starts with `SparkContext` / `SQLContext` / `HiveContext` for a constructor call, and with ``The `sc` global`` / ``The `sqlContext` global`` for the notebook globals.
- **Why:**
  - It blocks moving the workload to serverless or shared/standard clusters, which is often the cheaper option.
  - On standard access mode many `sc` functions stay blocked: `parallelize`, `textFile`, `setJobGroup`, `setLocalProperty`, `getConf`, `runJob`, `emptyRDD`, `range`, `union`, `stop`, …
- **Detect:**
  - `.sparkContext` on a value the type oracle knows is a SparkSession (`spark`, `SparkSession.builder...getOrCreate()`, an annotated parameter, `df.sparkSession`). One finding per access, so `sc = spark.sparkContext` is reported once, not at every later use of `sc`.
  - The names `sc` and `sqlContext` when nothing in the file defines them (the notebook globals).
  - The constructors `SparkContext(...)`, `SparkContext.getOrCreate()`, `SQLContext(...)`, `SQLContext.getOrCreate()`, `HiveContext(...)` (matched by qualified name).
- **Fix (unsafe):**
  - `<session>.sparkContext.setLogLevel(x)` as a whole statement → remove the statement.
  - `<session>.sparkContext.applicationId` → `<session>.conf.get("spark.app.id")`.
  - For the `sc` global the same two fixes are offered in notebooks only (using `spark`).
  - Everything else has no fix; the help text names the replacements: `spark.createDataFrame(rows)` for `parallelize`, `spark.conf.get(k)` for `getConf`, `F.broadcast(df)` for broadcast joins.

#### DBX002 `rdd-access`
compat · warning · on · T1 · unsafe · PA D002/D005, Databricks limitations
- **Msg:** `<expr>.rdd is not supported on Spark Connect, serverless or standard access mode.`
- **Why:** The RDD API is unsupported there. It is also a performance smell (ACT011).
- **Detect:** reading `.rdd` on a value the type oracle knows (or, from its name and use, believes) is a DataFrame. RDDs that come from `sc.parallelize(...)` / `sc.textFile(...)` are not reported here: the `sc` access is DBX001's finding.
- **Fix (unsafe):**
  - `df.rdd.isEmpty()` → `df.isEmpty()` (when the configured Spark version is 3.3 or later).
  - `df.rdd.map(lambda r: <one-line expression>).collect()` → `[<expression> for r in df.collect()]`.
  - Other uses (`getNumPartitions()`, `mapPartitions`, ...) get no fix; the help text points to `df.isEmpty()`, `df.collect()` and `mapInPandas` / `applyInPandas` for row-wise Python.

#### DBX003 `input-file-name`
compat · warning · on · T0 · unsafe · Databricks (deprecated on UC; DBR 13.3+)
- **Msg:** `input_file_name() is not supported on Unity Catalog standard/serverless compute (and is deprecated) — use the _metadata.file_path column.`
- **Why:** It errors on UC shared/standard compute and serverless, and is deprecated.
- **Detect:**
  - `F.input_file_name()` and `input_file_name()` imported from `pyspark.sql.functions` (matched by qualified name, so a local function of that name is not reported).
  - A call to `input_file_name()` in SQL text: the query `spark.sql(...)` runs (positional or `sqlQuery=`, an f-string, or a name bound once to the text; an `EXPLAIN` only plans, so it is not reported), the string passed to `F.expr(...)`, and string arguments of `df.selectExpr(...)` / `df.filter(...)` / `df.where(...)` (also through a name bound once). The name inside a string literal or comment of that SQL is not a call.
- **Fix (unsafe):** `F.input_file_name()` → `F.col("_metadata.file_path")` (attribute form only; a bare imported `input_file_name()` and SQL text get no fix). Only valid for file-source DataFrames, hence unsafe.

#### DBX016 `list-tables-existence-check`
perf · info (warning in loops) · on · T1 · none · new
- **Msg:** `Existence check for one table by listing every table: the driver fetches the whole listing (one metastore round trip per page on Unity Catalog) to test a single name.` (`schema` instead of `table` for `listDatabases()` / `SHOW SCHEMAS`; inside a loop it adds `Inside a loop, the full listing is fetched on every iteration.`). Help: use `spark.catalog.tableExists("catalog.schema.table")` (or `databaseExists("catalog.schema")`), or a pattern argument / `SHOW TABLES LIKE` to keep the listing small.
- **Why:** `spark.catalog.listTables(db)` / `SHOW TABLES IN db` returns metadata for every table in the schema; on Unity Catalog that is paged metastore calls, slow on schemas with thousands of tables. Code that only wants to know whether ONE table exists (often inside a loop over table names) keeps the cluster idle while the driver waits. `spark.catalog.tableExists("cat.db.tbl")` / `databaseExists` (Spark >= 3.3, Databricks) is one lookup.
- **Detect:** a full listing — `Catalog.listTables(db)` / `listDatabases()` without a `pattern` argument (oracle: `Catalog` receiver), or `spark.sql("SHOW TABLES|SCHEMAS|DATABASES [IN|FROM x]")` with no pattern, optionally `.select(..)`, then `.collect()`/`.toLocalIterator()` — used inline as (1) the source of a comprehension/generator whose condition or element compares the row's name field (`.name`, `.tableName`, `.databaseName`, `.namespace`, `["tableName"]`, optionally `.lower()`) with `==`; (2) the right side of `in`/`not in` (the listing itself or a comprehension projecting the name); (3) a `for` loop over it whose body has a top-level `if` / `elif` testing `<name> == x` (a test nested in another loop or block is not followed); (4) `spark.sql("SHOW TABLES ...").filter/where(...)` comparing `tableName` / `databaseName` / `namespace` with `==` (Column expression) or SQL `=` (string condition). Reported on the listing call ((4): on the `filter` / `where` call); warning when that call sits inside a loop (a comprehension's own iteration does not count), info otherwise. No fix (the table's qualified name is not known statically). A listing bound to a name first (listed once, then many lookups) is not reported.
- **FP:** Low. Filtering the listing by other fields (`tableType == "VIEW"`, `isTemporary`), `!=`/`in <collection>` filters, pattern arguments and `SHOW TABLES LIKE` stay quiet.
- **Ex:** `if name in [t.name for t in spark.catalog.listTables("db")]:` → `if spark.catalog.tableExists(f"db.{name}"):`

#### DBX017 `pipeline-stream-fanout`
perf · info · on · T1 · none · 2026-11-09 (Lakeflow fan-out docs, ForEachBatch sink Public Preview)
- **Msg:** `<source> is streamed separately by N pipeline datasets/flows: <names and lines>. Each one is its own streaming query that re-reads every micro-batch, so the cluster is sized for all of them running at once.` Each dataset is labelled with its `name=` (as written) or function name and line, `(N from a loop, ...)` for a def in a literal-list loop; when a loop's length is unknown the count reads `pipeline datasets/flows generated in a loop` and the def is labelled `(one per loop item, ...)`. Help: one `@dp.append_flow` into a `@dp.foreach_batch_sink` whose handler persists the batch, writes each target with `txnAppId`/`txnVersion`, then unpersists, with the trade-offs (no expectations, lineage or full-refresh cleanup; writes not atomic across targets).
- **Why:** In a Lakeflow (DLT) pipeline every streaming table / append flow is a separate streaming query with its own checkpoint. When one ingestion table fans out to many outputs (often a `for` loop generating one `@dp.table` per region/type), each output reads every micro-batch of the source on its own, so N small queries run concurrently and the pipeline needs a cluster sized for N streams for little work. Databricks documents this for loops ("each generated flow or table processes the entire source dataset independently") and offers the ForEachBatch sink (`@dp.foreach_batch_sink`, Public Preview on the PREVIEW channel): one `append_flow` reads the source once and the handler writes every target per micro-batch (with `persist()` so the batch is not recomputed per write).
- **Detect:** functions decorated with `dlt`/`pyspark.pipelines` `table`, `streaming_table`, `materialized_view`, `view`, `temporary_view` or `append_flow` (qualified names, any alias) whose body streams a fixed source: `spark.readStream….table("x")` (oracle: `DataStreamReader`), `….load("path")`, Kafka-style `….option("subscribe", "t").load()`, or `dlt.read_stream("x")` / `dp.read_stream("x")` (`live.` prefix and case ignored). Readers are grouped by source; a def inside a `for` over a literal list counts once per element (nested loops multiply), a loop over anything else counts as reaching the threshold; decorated defs inside helper functions count once each. Report on the first read when ≥ `pipeline-fanout-min-flows` (default 3; 0 disables) datasets stream the same source, one finding per source. Sources that change per iteration (f-strings) and reads inside an undecorated def nested in the dataset function (a handler) are ignored; batch reads (`spark.read.table`) are not counted (the ForEachBatch sink is for streaming flows).
- **FP:** Low-med. Some fan-outs are deliberate (targets need expectations, lineage or full-refresh handling, which a ForEachBatch sink gives up; writes across targets are not atomic) — suppress with `# noqa: DBX017`. SQL pipeline files are not linted.
- **Ex:** `for r in REGIONS: @dp.table(name=f"orders_{r}") def t(): return spark.readStream.table("raw_orders").filter(...)` → one `@dp.foreach_batch_sink(name="orders_by_region")` whose handler persists `batch_df` and writes `batch_df.filter(...)` to each region table (`txnAppId`/`txnVersion` options for idempotent retries), fed by a single `@dp.append_flow(target="orders_by_region")` reading `raw_orders`.

### PDS — pandas-on-Spark

#### PDS005 `pandas-on-spark-ops-on-diff-frames`
perf · warning · on · T0 · none · new
- **Msg:** `compute.ops_on_diff_frames=True: every operation between two different pandas-on-Spark frames silently becomes a join (a shuffle per expression).`
- **Why:** By default pandas-on-Spark refuses arithmetic/assignment between two different frames because it needs a join on the index. Turning the option on removes that guard, so `psdf1["a"] + psdf2["b"]` or `psdf1["x"] = psdf2["y"]` each become a hidden join with a shuffle. The best-practices guide says to avoid it.
- **Detect:** `ps.set_option("compute.ops_on_diff_frames", True)` (also `pyspark.pandas.config.set_option`, `key=` / `value=`), `ps.option_context(..., "compute.ops_on_diff_frames", True, ...)`, `ps.options.compute.ops_on_diff_frames = True`, and the Spark conf key `pandas_on_Spark.compute.ops_on_diff_frames` (the prefix pandas-on-Spark stores its options under) set to `True` / `"true"` (any casing) every way Spark config is written: `spark.conf.set`, `SparkSession.builder.config(k, v)` or `config(map={...})`, `SparkConf().set(..)`, and SQL `SET k = true`; a key or value bound to a constant name is followed. A conf write that a later write of the same key in the file turns back off (or `spark.conf.unset`) is still reported, at info. Names are matched by qualified name, so plain `pd.set_option` is never reported; non-literal values are skipped. No fix.
- **Help:** `Keep the default (False) and combine the data explicitly once (merge/join, or ps.concat), so the cost is visible.`
- **FP:** Low (exact option name). A deliberate, one-off combine on small frames can be suppressed with `# noqa: PDS005`.
- **Ex:** `ps.set_option("compute.ops_on_diff_frames", True); psdf1["a"] + psdf2["b"]` → `psdf1.merge(psdf2, on="id")` then operate within one frame.

#### PDS006 `pandas-on-spark-single-partition-op`
perf · info · on · T1 · none · new
- **Msg:** `<op>() on an un-grouped pandas-on-Spark frame runs over a window with no partition: all rows move to a single partition.`
- **Why:** On an un-grouped `ps.DataFrame`/`ps.Series`, `rank`, `shift`, `diff`, `pct_change` and `cumsum/cumprod/cummax/cummin` are built on a Spark Window with no partition spec, which "moves all data into a single partition in a single machine" (an OOM or one-core stage). This is the pandas-on-Spark twin of PRF005.
- **Detect:** One of those methods called on a receiver the oracle types as pandas-on-Spark with Certain confidence: `ps.DataFrame/Series(...)`, `ps.read_*`, `ps.from_pandas`, `ps.concat`, `ps.sql`, `df.pandas_api()`/`to_pandas_on_spark()`, `ps.DataFrame`/`ps.Series` annotations, and values derived from them by column selection (`psdf["x"]`, `psdf[["a","b"]]`, boolean masks), element-wise arithmetic/comparison with numbers or other ps frames, and frame-preserving methods (`sort_values`, `fillna`, `assign`, ...). `groupby(..)`, `rolling`/`expanding`, reductions and integer/label lookups are not frames, so grouped ops are never reported. In a chain (`s.shift(1).cumsum()`) only the first op is reported. Not followed (not reported): `.loc` / `.iloc` selections, `head(n)`, and aggregates of a `groupby` (`psdf.groupby(k).sum().cumsum()`). No fix.
- **Help:** `Group first (psdf.groupby(key).<op>()) or use a Spark Window.partitionBy(...); fine on small frames (# noqa: PDS006).`
- **FP:** Medium: harmless on small frames (suppress with `# noqa: PDS006`). Plain pandas and Spark DataFrames are never reported.
- **Ex:** `psdf["amount"].cumsum()` → `psdf.groupby("user")["amount"].cumsum()`, or a Spark `Window.partitionBy("user").orderBy("ts")`.

### STY — Style & naming (all off in `default`; STY001–003 on in `cylint-compat`)

#### STY001 `functions-alias`
style · info · T0 · safe · CY037, astrumu C4003/C4004, ruff#7272
- **Msg:** `Import pyspark.sql.functions as F or sf (found fn).` (the found alias is shown in backticks; the accepted aliases come from the settings; for `types`: `Import pyspark.sql.types as T (found t).`)
- **Detect:** `from pyspark.sql import functions` / `types` without an alias or with an alias not in `functions-aliases` (default `["F", "sf"]`; the Spark docs use `sf`, per the ruff thread) / `types-aliases` (default `["T"]`), and `import pyspark.sql.functions as f` / `import pyspark.sql.types as t` with an alias not in the list. Not reported: a plain `import pyspark.sql.functions` (used fully qualified), relative imports, imports of single functions (`from pyspark.sql.functions import col`).
- **Fix (safe):** Rename the alias (or add `as F`) and every reference to it, using the first accepted alias. No fix when that name is already bound anywhere in the file (module or nested scope), or when a reference can also resolve to another binding.

#### STY002 `join-missing-how`
style · info · T1 · safe · CY010, palantir
- **Msg:** `join() without an explicit how= — state the join type (defaults to inner).`
- **Detect:** A `join` call on a DataFrame with at least one argument, no third positional argument, no `how=` keyword and no `*args` / `**kwargs`.
- **Fix (safe):** Append `how="inner"` after the last argument (the join type Spark uses when none is given).

#### STY003 `redundant-drop-before-select`
style · info · T1 · safe · CY032, ruff#7272
- **Msg:** `drop() before select() of explicit columns is redundant — select() already picks the columns.`
- **Detect:** `.drop(...)` on a DataFrame directly followed by `.select(...)` in the same chain (a frame bound to a name between the two is not followed), where every select argument is an explicit column: not `"*"` / `"t.*"` / `F.col("*")`, `df.columns`, `*cols`, or a list / comprehension / name that is not known to be a Column; selects with keyword arguments are skipped. Reported on the `drop`.
- **Fix (safe):** Remove the `.drop(...)`. No fix when the select names a dropped column (a string, an identifier inside an expression string such as `F.expr("id + 1")`, or an attribute like `b.id`): after a join the drop can be what resolves an ambiguous name.
