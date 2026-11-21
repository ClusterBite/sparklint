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
| UDF001 | udf-has-builtin | W | on | T0 | none | cost |
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
- *Msg* = the one-line diagnostic (planned rules only: an implemented rule's messages and help texts are declared with its code, `Msg` consts in `crates/sparklint_linter/src/rules/`, and generated into spec/rules.json; its *Wording* note says how the `{placeholders}` are filled). *Why* = the impact. *Detect* = the exact conditions. *FP* = false-positive risks and mitigations. *Fix* = the autofix. *Ex* = bad → good.

### ACT — Actions & driver

#### ACT001 `spark-action`
perf · info · off (on in `review`) · T1 · none · ruff#7272, new
- **Why:** In a cost review the first question is where the jobs are. This rule inventories every call that triggers a job or eager work; the message names its `kind`: `job` (DataFrame count/isEmpty/foreach/foreachPartition; RDD count/reduce/fold/aggregate/sum/max/min/mean/stats/countByKey/countByValue/treeReduce/treeAggregate), `driver-collect` (collect/toPandas/toArrow/first/take/head/tail/toLocalIterator; RDD first/take/top/takeOrdered/takeSample/collectAsMap/collect/toLocalIterator; pandas-on-Spark to_pandas/to_numpy), `write` (DataFrameWriter save/saveAsTable/insertInto/parquet/csv/json/orc/text/jdbc/xml, `writeTo(..)` create/replace/createOrReplace/append/overwrite/overwritePartitions, `mergeInto(..)` + `whenMatched()` / `whenNotMatched()` / `whenNotMatchedBySource()` clauses + `.merge()`, RDD saveAs*File; Delta Lake `DeltaTable` operations: `delete` / `update` / `updateExpr` / `vacuum` / `restoreToVersion` / `restoreToTimestamp` / `generate` / `upgradeTableProtocol` / `clone*`, `merge(..)` + `whenMatched…` / `whenNotMatched…` clauses + `execute()`, `optimize()[.where(..)]` + `executeCompaction()` / `executeZOrderBy(..)`, `DeltaTable.create…(spark)…execute()` and `DeltaTable.convertToDelta(..)`), `eager-sql` (`spark.sql` whose text starts with a DDL/DML keyword such as CREATE, INSERT, MERGE, DELETE, UPDATE, OPTIMIZE, VACUUM, CACHE), `stream-start` (`writeStream` start/toTable), `checkpoint`, and `display` (`df.show()`, and Databricks `display(df)`).
- **Wording:** `{call}` is the call as written (`orders.count()`, `orders.filter(…).collect()`; a long chain is shortened), `{kind}` one of the kinds above.
- **Detect:** A call the shared call classifier (`sparklint_semantic::call_run`: one table of receiver type + method rows) says runs something; a Delta Lake table operation is worded as `write`. `spark.sql(<const/f-string>)` is classified by its leading SQL keyword. Delta builder chains are followed from `execute()` / `executeCompaction()` / `executeZOrderBy()` back to `merge(..)` / `optimize()` on a `DeltaTable` (`forName` / `forPath`, `alias(..)` allowed), also through a name bound once to part of the chain (`m = dt.merge(..)`, then `m.whenMatchedUpdateAll().execute()`). `DeltaTable` may come from `from delta.tables import DeltaTable`, `from delta import DeltaTable` or a star import of either.
- **Not reported:** a Delta merge or optimize builder that is never executed, `DeltaTable.history()` / `detail()` / `toDF()` (lazy frames), other objects' `execute()` / `update()` / `delete()` (database cursors, dicts, user classes), schema-inferring reads, schema lookups such as `df.columns`, and `localCheckpoint()` (eager by default, not in the table yet). `checkpoint(eager=False)` is reported although it runs nothing until the next action.
- **FP:** None by construction (it is an inventory). An action inside a function passed to `foreachBatch` is reported like any other.
- **Ex:** `n = df.count()` → (reported, no change)

#### ACT002 `collect-unbounded`
perf · warning · on · T2 · unsafe · CY001, PA D001
- **Wording:** The `reduced` variant (info) is for a frame filtered, aggregated, deduplicated or sampled on the way.
- **Why:** The most common cause of driver OOM. It also serialises the job's whole output through the driver, so the cluster sits idle.
- **Detect:** `x.collect()` where `x` is a Spark DataFrame (also returned from a local helper function or passed through one) with no row bound and not driver-sized.
- **Not reported:**
  - Row-bounded frames: `limit(n)` (any `n`, also a variable or parameter), `spark.sql("... LIMIT n")` and an f-string count `spark.sql(f"... LIMIT {n}")` at the end of the query, one-row aggregates (`agg(..)`, `select(F.max(..))`, `selectExpr("max(x)")`, `SELECT count(*) FROM t`), `describe()` / `summary()`, `spark.range(n)` with a small literal `n`, and a local helper that returns such a frame.
  - Driver-sized frames: catalog metadata commands (`spark.sql("SHOW TABLES")`, `SHOW TBLPROPERTIES`, `SHOW COLUMNS`, `DESCRIBE [TABLE|EXTENDED] t`, `DESCRIBE DETAIL`; not `SHOW PARTITIONS` or `DESCRIBE HISTORY`, which grow with the data) and frames built from driver data (`spark.createDataFrame(pandas_df)` or a list), also after row-wise steps. Joined with a table, they are reported again.
  - `df.rdd.collect()` (ACT011), `take(n)` / `head(n)` / `tail(n)` (bounded by their argument).
  - Lookup tables that are small but not provably bounded are still reported: add an explicit `.limit()`, or suppress the line with `# noqa: ACT002`.
- **Overlap:** ACT004 wins when the collected rows are iterated, ACT009 when they only feed a builtin (`len`, `sum`, `[0]`).
- **Fix (unsafe):** `.collect()` → `.limit(N).collect()`, with `N` from the `collect-limit` setting (default 1000); offered on both variants. Changes semantics, so unsafe.
- **Ex:** `rows = spark.table("sales").collect()` → `rows = spark.table("sales").limit(1000).collect()` or keep the work in Spark.

#### ACT003 `topandas-unbounded`
perf · warning · on · T2 · none · CY006, PA P001
- **Wording:** `{method}` is the method as called (`toPandas`, `to_pandas`, `to_numpy`); the `reduced` variants (info) are for frames filtered, aggregated, deduplicated or sampled on the way.
- **Why:** Same as ACT002: every row is shipped to the driver, which runs out of memory or sits in GC while the executors idle. A pandas copy adds two to three times the Arrow size on top.
- **Detect:** a call with no arguments to `toPandas()` or `toArrow()` (Spark 4) on a DataFrame, or to `to_pandas()` / `to_numpy()` on a pandas-on-Spark DataFrame or Series, when the frame has no known row bound. Filtered, distinct or grouped frames whose row count is unknown are info; everything else is a warning.
- **FP:**
  - Row-bounded frames are not reported: `limit(n)` (also with a name or parameter for `n`), aggregates without keys (`df.agg(F.max("x"))`, `groupBy().count()`, `spark.sql("SELECT count(*) ...")`), `describe()` / `summary()`, a `LIMIT n` query, unions of bounded frames, `spark.range(n)` with a literal, pandas-on-Spark `head(n)`.
  - Driver-sized frames are not reported: catalog metadata commands (`SHOW TABLES`, `SHOW COLUMNS`, `DESCRIBE [TABLE|EXTENDED|DETAIL] t`) and frames built from driver data (`spark.createDataFrame(pandas_df)`). `SHOW PARTITIONS` and `DESCRIBE HISTORY` grow with the data and are reported.
  - Calls on objects that are not Spark (a pyarrow table, a pandas frame, a user class with a `toPandas` method) are not reported.
  - When the conversion feeds row iteration (`for _, r in df.toPandas().iterrows()`), ACT004 reports it instead.
  - ML feature-matrix code converting whole frames on purpose is a common legitimate use: turn the rule off for those paths with `per-file-ignores` (e.g. `"ml/**" = ["ACT003"]`).
- **Fix:** none.
- **Ex:** `pdf = df.toPandas()` → `pdf = df.select(cols).limit(n).toPandas()`, or `df.write…` and read the result elsewhere.

#### ACT004 `driver-row-iteration`
perf · warning · on · T1 · none · CY031
- **Wording:** `{source}` names the source as written in the code (receiver, method and short arguments; a long receiver is shortened, `spark.sql(…).collect()`): `for row in orders.collect()`, `for row in orders.toLocalIterator(True)`, `for row in orders.rdd.collect()`, `orders.toPandas().iterrows()` (also `itertuples()` / `items()`, or `pdf.iterrows()` for a pandas frame pulled from Spark), `orders.toPandas().to_dict("records")`, `orders.toArrow().to_pylist()`, `rows from orders.collect()` for a name bound to the call (`rows collected from a DataFrame` when the name is bound in several ways). `rows-n-plus-one` is for a loop body that also runs a Spark action or write (the N+1 pattern, see ACT005).
- **Why:** Single-threaded Python over the whole dataset while the executors are idle and billed.
- **Detect:** The iterable of a `for` statement or comprehension (also inside `enumerate` / `zip` / `list` / `sorted` / `set` / `tuple` / `reversed`, or an open-ended slice such as `rows[1:]`) holds rows pulled to the driver (the shared answer, `sparklint_semantic::pulled`): `collect()`, `toLocalIterator()` (with or without `prefetchPartitions`), `toPandas().iterrows()/itertuples()/items()`, `toPandas().to_dict("records")` (positional or `orient=`), `toArrow().to_pylist()`, pandas-on-Spark `to_pandas().iterrows()`, `.rdd.collect()`, directly or through every binding of a name (`t = df.toArrow(); t.to_pylist()`), or a value the type oracle knows is a list of rows / a pandas frame from Spark. The pull must be on a DataFrame, RDD or pandas-on-Spark frame.
- **Overlap:** wins over ACT002 / ACT003 at the `collect()` / `toPandas()` / `toArrow()` call it iterates; ACT009 wins over it when the rows only feed a builtin such as `sum()`.
- **Fix:** none.
- **FP:**
  - Iterating a `bounded` frame (e.g. a config table with `limit`, a one-row aggregate) → no finding.
  - Iterating a driver-sized frame (catalog metadata such as `for r in spark.sql("SHOW CATALOGS").collect()`, or a frame built from driver data) → no finding.
  - Iterating `df.columns` or `df.dtypes` is not row iteration and is not matched; nor are `to_dict()` / `to_dict("list")` (one item per column) or Arrow `to_batches()`.
  - A slice with an end (`for r in rows[:3]`) walks a few rows (ACT002 still reports the collect); values derived per row (`ids = [r.id for r in df.collect()]`, then `for i in ids`) are reported once, at the comprehension that iterates the rows; `rdd.collectAsMap()` is a dict.
- **Ex:** `for r in df.collect(): out.append(r.a*2)` → `df.select((F.col("a")*2).alias("a2"))`

#### ACT005 `action-in-loop`
perf · warning · on · T2 · none · new
- **Wording:** `{call}` is the call as written; long arguments are elided (`saveAsTable(…)`) and a long receiver is shortened (`df.filter(…).write.mode(…)`, else `spark….collect()`).
- **Why:** The classic N+1 pattern, e.g. `for d in dates: df.filter(...).count()`. Each iteration rescans the source. It usually collapses into one `groupBy`.
- **Detect:** An ACT001-table action of kind `job`, `driver-collect` or `write`, or a Delta Lake table operation (not `display`, `stream-start` or `checkpoint`), lexically inside a `for`/`while`/comprehension body, reported once per action call. That includes `toArrow()`, `toLocalIterator(..)`, RDD actions, pandas-on-Spark `to_pandas()`, `writeTo(..).append()`, Spark 4 `mergeInto(..).whenMatched()...merge()` and `DeltaTable` `vacuum()` / `delete(..)` / `update(..)` / `merge(..)…execute()` (also through a name) / `optimize()…executeZOrderBy(..)`.
- **FP:**
  - Only the loop the action sits in counts (the innermost loop that repeats it; the shared loop verdict, `sparklint_semantic::loop_character`).
  - Loops over a small constant list of tables (`for t in ["a","b"]: spark.table(t).write…`) are legitimate orchestration. The finding is info instead of warning when that loop iterates a literal or constant iterable (a list / tuple / set / dict literal, a name bound only to one and never filled in place by `append` / `extend` / `insert` / `update` / `add` / item assignment, starred literal parts, `zip` / `enumerate` / `sorted` of them) of at most `driver-loop-small-max` items (default 10), OR each iteration reads a different source (the loop variable, or a name computed from it in the body, reaches `spark.table(..)` / `spark.read…(..)` / `DeltaTable.forName(spark, ..)` / `forPath(..)` / a table name in a `spark.sql(..)` f-string / a helper call / a frame joined or unioned in; a `spark.sql` f-string whose holes are only in predicates reads the same tables every time), OR the frame fits on the driver: a row bound (`limit(n)`, `spark.sql("... LIMIT n")` including an f-string `LIMIT {n}`, a one-row aggregate), a frame built from driver data (`spark.createDataFrame` of a list, tuple or pandas frame) or catalog metadata (`SHOW TABLES`, `DESCRIBE ..`).
  - The `foreachBatch` body is not a loop.
  - Retry and first-match loops are not reported: an action followed in the same block by an unconditional `break` or `return` (including `return spark.table(t).count()`) completes once. A `continue` in an `except` handler still counts as a retry; a conditional `break` keeps the finding, and a `break` only leaves the innermost loop.
  - Polling loops are not reported: a `while` loop whose body calls `time.sleep(..)` (not counting nested functions), or an action in the `while` condition whose frame reads nothing rebound inside the loop (`while spark.table(t).count() == 0: ...`), waits for data written elsewhere rather than iterating over data. A condition whose frame changes each iteration (`while df.filter(F.col("v") > i).count() > 0: i += 1`) is still reported, and so is a `for` loop that sleeps for pacing.
- **Not reported:** an action inside a helper function the loop calls (helpers are not followed); `show()` / `display()`, `writeStream...start()` and `checkpoint()`; `spark.sql("INSERT ...")` / `MERGE` / `DELETE` statements (not in the action table here); actions on a receiver the oracle cannot type as Spark (a loop variable over an untyped list of frames).
- **Overlap:** IOD006 (write in loop) wins at the same write.
- **Fix:** none.
- **Ex:** `for c in countries: print(df.filter(F.col("c")==c).count())` → `df.groupBy("c").count().show()`

#### ACT006 `count-for-emptiness`
perf · warning · on · T1 · safe (Spark ≥3.3) / unsafe · CY020, PA D006/D007/D009, PA D005
- **Wording:** `{count}`, `{call}` and `{receiver}` are named as written (`orders.filter(orders.amount > 0).count()`); `held-count` is a count held in a name, `rdd-is-empty` the `df.rdd.isEmpty()` form.
- **Why:** `count()` cannot stop early: it runs the whole lineage over every row. `isEmpty()` is a `limit(1)` job.
- **Detect:**
  - `Compare(df.count(), [==|!=|>|<=], 0)`, or against 1 with `>=`/`<`, either side, anywhere (`if`, an assignment, a `return`).
  - `not df.count()`, or `df.count()` as a test: `if` / `elif` / `while` / `assert` / a conditional expression, also through `and` / `or` / `not`.
  - `df.rdd.isEmpty()` → `df.isEmpty()` (D005). A transformed RDD (`df.rdd.map(f).isEmpty()`) is not reported.
  - `n = df.count()` where every later use of `n` is one of the emptiness checks above (reported on the `count()` call).
  - The receiver must be a Spark DataFrame: typed, or named like one (`*_df`), including function parameters.
- **Not reported:**
  - `n = df.count()` when `n` is also logged, returned, compared with another number or used arithmetically (the count is needed).
  - A frame that fits on the driver is cheap to count: a row bound (`df.limit(1).count()`, an aggregate-only `agg` / `select` / SQL, a SQL `LIMIT n`, also an f-string `LIMIT {n}`), driver-built data (`spark.createDataFrame(pdf)`) or catalog metadata (`spark.sql("SHOW TABLES")`).
  - Comparisons that are not emptiness tests (`df.count() > 1`).
  - `len(df.head(1))` / `len(df.take(1))` (already cheap); pandas-on-Spark `psdf.count()` (a per-column Series).
  - A count wrapped in a call (`bool(df.count())`) and untyped receivers (a parameter `x` with no annotation or DataFrame-like name).
- **FP:** Low; test files assert emptiness this way often (`assert df.count() == 0`), set `per-file-ignores` for them.
- **Fix:** For the compare / `not` / test forms: safe when `spark-version ≥ 3.3` (the default is 3.5; `df.isEmpty()` / `not df.isEmpty()`, parenthesised when an operator needs it), otherwise unsafe (`len(df.take(1)) == 0` / `> 0`). `df.rdd.isEmpty()` gets a safe fix only with `spark-version ≥ 3.3`. The `n = df.count()` form has no fix.
- **Ex:** `if df.count() > 0:` → `if not df.isEmpty():`

#### ACT007 `action-only-for-logging`
perf · warning · on · T1 · none · CY021 (widened), ruff#7272
- **Wording:** `{call}` is the call as written (`orders.filter("amount > 1").collect()`, `df.rdd.getNumPartitions()`); `logged` covers the action inside a logging call and `n = df.count()` whose every use is logged.
- **Why:** Very common in production pipelines: `logger.info(f"rows: {df.count()}")` after every step doubles or triples job count. Each call recomputes the lineage unless cached.
- **Detect:**
  - A DataFrame action that returns a result (`count()` with no arguments / `isEmpty()` / `collect()` / `first()` / `head()` / `take()` / `tail()` / `toPandas()` / `toArrow()` / `toLocalIterator()`; not `foreach`, the shared call classifier's job and pull rows), also inside `len(..)`, an f-string or any other expression in the arguments) or an RDD `getNumPartitions()` appears inside the arguments of a logging sink: `print`, Databricks `display`, `logging.*` functions, a logger typed via `logging.getLogger` / `structlog.get_logger` / `loguru.logger` (its `debug` / `info` / `warning` / `error` / ... methods), an attribute named like a logger on an untyped object (`self.logger.info(..)`), `dbutils.notebook.exit`, or `mlflow.log_metric(s)` / `log_param(s)`. Actions inside a `lambda` in the arguments are not counted.
  - A bare expression statement running one of those actions (`df.count()`, result discarded).
  - `n = df.count()` where every use of `n` is inside a logging sink (reported on the `count()` call). A use in an emptiness test makes it ACT006's case instead; any other use (returned, compared with a number, passed on) means the count is needed.
  - Receivers: typed Spark DataFrames, including name-heuristic ones (`*_df` names and parameters); an untyped parameter is not reported even when it is used like a DataFrame elsewhere. pandas-on-Spark and plain pandas objects are not.
- **FP:**
  - A discarded action directly inside a `try:` body with an `except` handler, or directly inside `with pytest.raises(..)` / `pytest.warns(..)` / `self.assertRaises(..)` (and the `Regex` / `Warns` variants), is a probe (does the table exist, is there a grant, does a bad argument fail on evaluation): not reported. Statements nested deeper (an `if` inside the block) or in `except` / `else` / `finally` still are.
  - The last top-level expression statement of a notebook cell (`.ipynb` and Databricks source format): the notebook displays its value, so `df.head()` / `df.count()` there is used, not discarded. Not reported (the "computed only to be logged" form still is).
  - Eager caching: a discarded `count()` on the cached frame itself (`df.cache().count()`, `df.persist(level).count()`, `checkpoint()` / `localCheckpoint()` results, or `df.count()` after a `df.cache()` statement) fills the cache that later actions read: not reported. A frame derived from the cache (`df.cache()` then `df.filter(..).count()`) and a discarded `collect()` of a cached frame still are.
  - Frames that fit on the driver are cheap to count or pull: a row bound (`df.limit(5).collect()`, an aggregate-only `select` / SQL query, `LIMIT {n}` f-strings), catalog metadata SQL (`spark.sql("SHOW TABLES")`, `DESCRIBE ..`) and frames built from driver data (`spark.createDataFrame(rows)` / `createDataFrame(pandas_df)`). Not reported in any variant.
  - pandas / numpy / polars values, and everything derived from them by a method, attribute, index or operator (`pd_df = pd_df.join(..)`; `pd_df.head()`), are never Spark, whatever the variable is called.
  - A discarded action that fills a Spark `Observation` (the `df.observe()` pattern this rule recommends): not reported when the same function (or module, for top-level code) holds a `pyspark.sql.Observation` value (constructed there or an annotated parameter), or when the DataFrame was unpacked from the same call as a value whose `.get` is read after the action (`checked_df, observation = engine.apply_checks(df, ..)`; `checked_df.count()`; `observation.get`). `.get(..)` calls and reads before the action do not count.
  - Counts that are deliberately logged as data-quality metrics: suppress them with `# sparklint: ignore[ACT007]`.
  - Unlike cylint, this rule does not use the `log` substring heuristic.
- **Not reported:** `warnings.warn(..)` and other non-logging calls; actions inside a helper function that does the logging (helpers are not followed); a count held in a name that is also used for anything but logging; `bool(df.count())`-style wrappers outside a logging call.
- **Overlap:** none. `print(len(df.collect()))` gets both ACT009 (on `len(..)`: count in Spark instead) and ACT007 (on `collect()`: drop the log line).
- **Fix:** none.
- **Ex:** `log.info("n=%d", df.count())` → remove; or read `numOutputRows` from the write's Delta history; or `df.observe("m", F.count(F.lit(1)))` (Spark 3.3+, metric computed in the same job).

#### ACT008 `collect-to-filter-roundtrip`
perf · warning · on · T2 · none · new
- **Wording:** `{sink}` is `filter()` / `where()`, or the function as written for `F.expr()` / `expr()`.
- **Why:** Two jobs plus a huge literal `IN` list in the plan (slow analysis, codegen limits, driver memory). A `left_semi` join (broadcast if small) is one job.
- **Detect:**
  - Values pulled from a DataFrame, RDD or pandas-on-Spark frame that may not fit on the driver (the shared answer, `sparklint_semantic::pulled`): `collect()` / `toLocalIterator()`, a comprehension whose first source is one, `list()` / `set()` / `frozenset()` / `tuple()` / `sorted()` / `reversed()` of one, an open slice of one (`ids[1:]`), the values of a `toPandas()` / `to_pandas()` column (`.tolist()` / `.to_list()` / `.unique()` anywhere on the chain, `list(pdf["id"])`), the rows of `toArrow().to_pylist()`; directly or through every binding of a name (up to four names deep), or a value the type oracle knows is a list from Spark.
  - Reported where it is passed to `Column.isin(..)` (also as `*ids`, also under `~`), or interpolated into an f-string given to `spark.sql` (positional or `sqlQuery=`), to `DataFrame.filter` / `where` (positional or `condition=`) or to `F.expr`.
- **Not reported:**
  - Values that fit on the driver: a row-bounded source (`df.limit(10).collect()`, `spark.sql("... LIMIT 5")`, a one-row aggregate such as `agg(F.collect_set("id")).collect()[0][0]`), catalog metadata (`SHOW TABLES`, `DESCRIBE`) and frames built from local data (`spark.createDataFrame(rows)`).
  - One scalar (`first()[0]`) or one collected row (`collect()[0]`) used as a threshold; column names and frame metadata (`list(pdf)`, `list(pdf.columns)`, `list(pdf.dtypes)`).
  - Other sinks: a Python `in` test, `str.format()` / `%` query strings, `selectExpr` text, dict comprehensions (`list(d.keys())`).
- **Fix:** none.
- **Ex:** `ids=[r.id for r in bad.select("id").collect()]; df.filter(F.col("id").isin(ids))` → `df.join(bad.select("id"), "id", "left_semi")`

#### ACT009 `collect-for-driver-compute`
perf · warning · on · T2 · unsafe · new
- **Why:**
  - `len(df.collect())`, `sum(r.x for r in df.collect())`, `max(...)`, `sorted(df.collect())[0]`, and `df.collect()[0][0]` on frames that may not fit on the driver transfer all rows to compute one scalar.
  - `collect()[0]` on a frame that may not fit on the driver pulls everything to read one row.
- **Detect:**
  - The builtins `len` / `sum` / `min` / `max` / `sorted` / `any` / `all` / `set` or `collections.Counter`, called with one argument holding rows pulled from a DataFrame that may not fit on the driver (the shared answer, `sparklint_semantic::pulled`): a pull of every row (`collect()`, `toLocalIterator()`, `toPandas()`, `toArrow()`), a comprehension whose first source is one, `list()` / `sorted()` / `set()` of one, a pandas column (`sum(pdf["amount"])`), directly or through names: every binding that reaches the name counts (`if ..: rows = a.collect() else: rows = b.collect()`), and each name on the way must be read only there.
  - `len()` of a local frame counts its rows (`len(df.toPandas())`); its other aggregates walk the column labels and an iterator has no `len()`, so those are not reported. A nest of aggregates reports once, at the innermost (`len(set(r.id for r in df.collect()))` → `set()`).
  - `df.collect()[0]` where the receiver may not fit on the driver (`agg` without `groupBy` is bounded, so `df.agg(F.max("x")).collect()[0][0]` passes; so do driver-built frames and `SHOW` / `DESCRIBE` results).
- **Not reported (one-row frames):** `select` of only aggregates (also as one list: `df.select([F.count("a"), F.max("b")])`), `selectExpr` whose every expression is an aggregate (`df.selectExpr("min(v) AS lo", "max(v) AS hi")`; a window `OVER (..)` is not), a grouping without keys (`df.groupBy().agg(..)`, `df.groupBy().sum("x")`, also through `pivot`), and SQL `SELECT` of only aggregates without `GROUP BY` or `OVER`.
- **Also reported:** `df.collect()[-1]` (use `df.tail(1)[0]`, Spark 3.0+) and `df.collect()[:n]` / `[0:n]` with a literal `n` (use `df.take(n)`); each index / slice form also through names whose bindings all hold the collect and are used only there (`rows = df.collect()` then `rows[0]`). Other indexes (`[1]`, `[-2]`), open-ended or stepped slices (`[1:]`, `[:10:2]`) and a bound held in a name (`[:n]`) are not reported.
- **Wording:** `{function}() computed on the driver from {collect} — compute it in Spark.` (help: `df.count()` / `df.agg(..).first()`); `{call}[0] pulls every row to the driver to read one — use .first().`; `{call}[-1] … read the last one — use .tail(1).` and `{call}[:{n}] … keep {n} — use .take({n}).`, each with a help line. The collect is named as written (`orders.where("amount > 0").collect()`), also when the finding sits on the name that holds it.
- **Not reported:** `df.toPandas().shape[0]` and pandas methods (`pdf["x"].sum()`; ACT003 reports the `toPandas()`); `df.rdd.collect()` (ACT011); index forms on an iterator or a list built from one (`list(df.toLocalIterator())[0]`); a name holding the rows that is used more than once (`rows = df.collect()` then `len(rows)` and a loop: ACT002 / ACT004 report the collect); builtins with more than one positional argument (`max(a, b)`); receivers the type oracle cannot place (an untyped parameter; a `*_df` name is enough).
- **Overlap:** one finding per pull: ACT009 wins over ACT002 (`collect-unbounded`), ACT003 (`topandas-unbounded`) and ACT004 (`driver-row-iteration`) at the same `.collect()` / `.toPandas()` call, also when the pull is assigned to a name first (with several bindings, every pull call is a site).
- **Fix (unsafe):** `df.collect()[0]` → `df.first()`, `df.collect()[-1]` → `df.tail(1)[0]`, `df.collect()[:n]` → `df.take(n)`, `len(df.collect())` / `len(df.toPandas())` / `len(df.toArrow())` → `df.count()` (only when the pull is written in place, not through a name). Other forms have no fix.
- **Ex:** `mx = max(r.ts for r in df.collect())` → `mx = df.agg(F.max("ts")).first()[0]`

#### ACT010 `debug-action-left-in`
perf · warning · on · T1 · unsafe · CY012, PA D003/D008, astrumu module_show
- **Why:** Each `show`/`display` is a job (display may collect up to 10k rows). In scheduled jobs nobody reads the output. Console or memory streaming sinks in production are the streaming equivalent.
- **Detect:**
  - DataFrame `.show/.display/.printSchema/.explain/.describe().show/.summary().show`, on a receiver the oracle types as a Spark DataFrame (Certain or Likely); `.show` / `.display` only when a preview of the receiver is not cheap (see below).
  - Global `display(x)` when `x` is typed as a Spark DataFrame (Certain or Likely) and a preview of it is not cheap.
  - A preview is cheap for driver-built data, catalog metadata and a row bound from a limit-like step (`limit(n)`, SQL `LIMIT n`, also before an aggregate: `df.limit(100).agg(..)`). A bound that comes only from aggregating the frame is not cheap: `df.agg(..).show()`, `df.groupBy().count().show()`, `df.describe().show()`, `df.summary().show()`, `display(df.agg(..))` and `spark.sql("SELECT count(*) FROM t").show()` read every row and are reported.
  - `writeStream.format("console"|"memory")`.
  - Plain `.py` files → warning. Notebook sources (Databricks source notebooks and `.ipynb`) → info, because interactive exploration is legitimate. `printSchema`/`explain` are always info (no job on Classic).
- **FP:**
  - Notebooks that also run as scheduled jobs: raise the rule with `[tool.sparklint.severity] ACT010 = "warning"` (the override applies to every ACT010 finding, `printSchema`/`explain` included).
  - Not Spark actions, never reported: `displayHTML(..)` (renders a string), `display(dbutils.fs.ls(..))`, `display(<pandas frame / figure / image / list>)`, `display(df.head(5))` / `display(df.take(n))` (already local rows), and a cheap preview, through `show()` / `.display()` as much as through `display(x)` (`df.limit(10).show()`, `display(df.limit(10))`, `LIMIT n` SQL, driver-built data, `SHOW` / `DESCRIBE` / `DESCRIBE DETAIL` results, a table-less `SELECT current_date()`, an `EXPLAIN`: a deliberate small preview).
  - `.explain()` / `.show()` on objects of other libraries that mirror the DataFrame API (dbldatagen's `DataGenerator(..).withColumn(..)` builder, plotly figures): an instance of a class imported from a non-Spark library, and the builder-style chain on it, is not a DataFrame.
  - `display(x)` where the oracle cannot type `x` (a result of a third-party call bound to a name that does not look like a DataFrame, e.g. `logs = spec.build()`) is not reported: a known gap, preferred over reporting every `display`.
- **Fix (unsafe):** Delete the statement when it is a bare expression statement and the DataFrame is Certain (never on a heuristic, name-based type); when it is the only statement of a block, replace it with `pass`. No fix for console/memory streaming sinks or a call used inside a larger expression.
- **Wording:** three variants, `{call}` being the call as written (a `display(..)` whose argument is long names the frame hop by hop: `display(spark.sql(…))`). `show` / `display`: "left in pipeline code triggers extra jobs"; `printSchema` / `explain`: "prints to the driver log: it runs no job, but it is noise in a scheduled run (and a server round trip on Spark Connect)"; both with the help "Delete it before the code runs as a job; where someone reads the output, keep it with `# noqa: ACT010`". Console / memory sinks: "is a debug sink: the stream's output is printed to the driver log (console) or kept in driver memory (memory), not stored", help: write to a durable sink such as a Delta table, or the `noop` format for a benchmark.
- **Also:** `describe()` / `summary()` without `.show()` are lazy and not reported, nor is the `noop` streaming format.
- **Ex:** `df.show()` → (remove)

#### ACT011 `rdd-collect`
perf · warning · on · T1 · unsafe · CY034, PA PERF001
- **Wording:** `{call}` is the call as written, RDD steps included: `df.rdd.map(lambda r: r.id).collect()`, `df.rdd.take(5)`. Two message variants: the direct form (`df.rdd.collect()`) names the DataFrame call that returns the same rows (`{fixed}`, e.g. `df.collect()`); a chain with RDD steps says the steps run in Python workers. Both helps add that `.rdd` is unavailable on Spark Connect, serverless and standard access mode.
- **Why:** RDD steps on `df.rdd` (`map`, `flatMap`, `mapPartitions`, `keys`, `zipWithIndex`, `reduceByKey`, ...) run in Python workers: every row is pickled out of the JVM, handed to Python one at a time and pickled back, with none of the Arrow batching of `mapInPandas` / `mapInArrow` or the native speed of DataFrame expressions. The direct form (`df.rdd.collect()`, no steps) costs about the same as `df.collect()` (both pickle rows to the driver) but takes the query out of the SQL plan for nothing, and `.rdd` is not available on Connect, serverless or standard access mode (see DBX002).
- **Detect:** an RDD action on `x.rdd`, directly or after any chain of RDD methods, where the oracle types `x` as a DataFrame (Certain or Likely):
  - `collect()`, `take(n)`, `first()`, `toLocalIterator()`: direct or after RDD steps.
  - `collectAsMap()`, `top(n)`, `takeOrdered(n)`, `takeSample(..)`: only after RDD steps (`df.rdd.map(lambda r: (r.k, r.v)).collectAsMap()`); directly on `df.rdd` they run no Python worker.
- **Not reported:** frames that fit on the driver (a row bound such as `limit(10)`, catalog metadata SQL such as `SHOW TABLES`, `createDataFrame` from local data); other RDD actions (`count`, `foreach`, `reduce`, saves); RDDs not built from a DataFrame (`sc.parallelize(..)`, `sc.textFile(..)`); an RDD held in a name (`r = df.rdd; r.collect()`). DBX002 reports the `.rdd` access itself in all of these.
- **Fix (unsafe):** only for the direct forms: `df.rdd.collect()` → `df.collect()`, `df.rdd.take(n)` → `df.take(n)`, `df.rdd.first()` → `df.first()` (returns `None` on an empty frame where the RDD raises), `df.rdd.toLocalIterator()` → `df.toLocalIterator()`. A chain with RDD steps has no automatic fix.
- **Ex:** `df.rdd.map(lambda r: r.id).collect()` → `[r.id for r in df.select("id").collect()]` or `df.select("id").toPandas()["id"]`; `dict(df.rdd.map(lambda r: (r.k, r.v)).collect())` → `{r.k: r.v for r in df.select("k", "v").collect()}`.

#### ACT015 `approx-quantile-exact`
perf · warning · on · T1 · none · new
- **Wording:** `{error}` is the relative error as written. A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** `relativeError=0` turns the Greenwald-Khanna sketch into an exact computation: every value is kept in the summaries, which are merged and collected to the driver. The docs warn it "could be very expensive". It is an action, often used for outlier bounds inside pipelines, so it can also run once per loop iteration.
- **Detect:** `approxQuantile` on a receiver the oracle types as a `DataFrame` with Certain confidence (or `df.stat` of one) whose `relativeError` (third positional or keyword) constant-folds to `0` / `0.0` (literal, or a name bound once to such a literal). Calls with `**kw` are skipped.
- **FP:** Low. pandas `quantile`, numpy and user classes are never Spark; untyped parameters and Py4J handles are not Certain DataFrames. Genuinely needing exact values on a small frame is the legitimate case (`# noqa: ACT015`).
- **Ex:** `df.approxQuantile("x", [0.25, 0.75], 0.0)` → `df.approxQuantile("x", [0.25, 0.75], 0.001)`

### PRF — Plan & shuffle performance

#### PRF001 `withcolumn-in-loop`
perf · warning · on · T2 · unsafe · CY003, PA L003, ruff#7272
- **Why:** Every call creates a new Project node and re-runs analysis, so cost is O(n²) in the number of columns. With hundreds of columns, driver planning time reaches minutes and can hit StackOverflowError (Spark docs warn about this explicitly).
- **Detect:**
  - `df = df.withColumn(...)` (also inside a longer chain rooted at `df`) in a `for` / `while` body, where the new binding of `df` reaches the receiver on the next iteration (a loop-carried DataFrame). One finding per loop, on the first such call.
  - `functools.reduce(lambda d, c: d.withColumn(...), cols, df)` where the initial value is a DataFrame.
  - The receiver must be typed DataFrame (avoids cylint's untyped match).
- **Not reported:** a comprehension that builds one new frame per element (`[df.withColumn(c, ..) for c in cols]`: nothing is carried between iterations), a loop that assigns the result to a different name, and a loop that leaves right after the chain (an unconditional `break` / `return`: it runs once). The loop judged is the innermost loop that repeats the statement, as for ACT005 / IOD006.
- **FP:** Loops with ≤ 3 iterations over a literal list or tuple → info (the reduce form is always a warning).
- **Fix (unsafe):** Only when the `for` loop body is the single statement `df = df.withColumn(<name>, <expr>)` with two positional arguments, no `else` and everything on one line: rewrite to `df = df.withColumns({<name>: <expr> for c in <iter>})` (Spark ≥ 3.3). Other loops, `while` loops and the reduce form have no fix.
- **Ex:** `for c in cols: df = df.withColumn(c, F.trim(c))` → `df = df.withColumns({c: F.trim(c) for c in cols})`

#### PRF002 `column-op-in-loop`
perf · warning · on · T2 · unsafe · CY011
- **Why:** Same plan growth as PRF001.
- **Detect:** A loop-carried DataFrame (as in PRF001: `df = df.<method>(...)` in a `for` / `while` body whose new binding reaches the next iteration) calling `withColumnRenamed`, `withColumnsRenamed`, `withColumns`, `drop`, `select`, `selectExpr`, `toDF`, or `na.fill` / `na.replace`. One finding per loop. A `withColumn` in the loop (a cast, say) is PRF001's.
- **FP:** Loops with ≤ 3 iterations over a literal list or tuple → info.
- **Fix (unsafe):** Only when the `for` loop body is that single statement with positional arguments: `df.withColumnRenamed(old, new)` → `df = df.withColumnsRenamed({old: new for c in <iter>})` (Spark ≥ 3.4; one-line arguments), and `df.drop(c)` where `c` is the loop variable → `df = df.drop(*<iter>)`. Other methods have no fix.
- **Ex:** `for c in cols: df = df.withColumnRenamed(c, c.lower())` → `df = df.withColumnsRenamed({c: c.lower() for c in cols})`

#### PRF003 `cartesian-join`
perf · error · on · T1 · none · CY007, PA S010/S011
- **Why:** Output explodes and runs as a broadcast nested loop or CartesianProduct. This is the single most expensive accident in a cost review.
- **Detect:**
  - `DataFrame.crossJoin(x)`.
  - `DataFrame.join(other)` with no `on`, `on=None`, or a positional `None` condition (whatever `how` says, `"cross"` included).
  - `join(other, on=True)` or `join(other, on=F.lit(True))`.
  - Help: `Join on a key (df.join(other, "key", "inner")); for a scalar, aggregate it first so one side has one row.`
- **Not reported:** SQL text (`CROSS JOIN`, comma joins in `spark.sql`) is not checked.
- **FP:** One side is `bounded(1)` (scalar from `agg()` without groupBy, a `select` whose every item is an aggregate or a row-wise function of aggregates and literals such as `F.to_date(F.max("d"))`, or `createDataFrame([literal])`). This is the scalar-broadcast idiom → no finding. Either side that otherwise fits on the driver (a row bound such as `spark.range(10)` or `limit(n)`, a frame built from driver data, catalog metadata) → info.
- **Ex:** `a.crossJoin(b)` → `a.join(b, "key", "inner")`; for a scalar use `a.join(F.broadcast(stats), how="cross")` with `stats` bounded.

#### PRF004 `non-equi-join`
perf · warning · on · T1 · none · CY015
- **Why:** Without an equi-key, Spark cannot use a sort-merge or hash join, so it compares O(n·m) rows.
- **Detect:** `DataFrame.join(other, on, ...)` whose condition (second argument or `on=`) is known and has no equality between two non-literal sides:
  - Column expressions: no `==` / `eqNullSafe` whose operands are both non-literal, anywhere in the condition (`&` chains, a list of conditions, `between`, `<`, `>`, `!=`, `like`). An equality against a literal (`ev.kind == "x"`) filters rows; it is not a key.
  - `F.expr` strings: no `=` / `<=>` between two non-literal sides, and some `<`, `>`, `!=`, `<>`, `BETWEEN` or `LIKE` comparison. `kind = 'x'` (single or double quotes, numbers, `TRUE` / `FALSE` / `NULL`) is a filter, not a key.
  - A condition held in a name is followed through up to three single assignments.
- **Not reported:** column-name joins (`"id"`, `["id", "day"]`, a name holding a string or list of strings); a condition that is a parameter, a user helper call (`_match(a, b)`), a Column of unknown origin or a literal (`F.lit(True)`); no condition at all (PRF003). An `OR` of equalities is PRF032's case.
- **Severity:** info when one side is small: it fits on the driver (a row bound such as `limit` or a one-row aggregate, driver-built data, catalog metadata) or is marked for broadcast (`F.broadcast(x)`, `x.hint("broadcast")` / `"broadcastjoin"` / `"mapjoin"`, on either side).
- **Fix:** none. The help suggests adding an equi-key (a time bucket such as `floor(ts/3600)` on both sides) next to the range predicate, or a range-join hint on Databricks.
- **Ex:** `ev.join(win, (ev.ts>=win.s)&(ev.ts<win.e))` → add `ev.bucket==win.bucket &` …

#### PRF005 `global-window`
perf · warning · on · T1 · none · CY017, palantir "Empty partitionBy()"
- **Why:** Spark logs "No Partition Defined for Window operation! Moving all data to a single partition". One task does all the work, it spills, and the cluster is idle.
- **Detect:** `Column.over(w)` where the oracle types `w` (through names, `if`/`else` expressions and branches) as a `WindowSpec` built from `Window` / an alias of `pyspark.sql.window.Window` that has no `partitionBy` with ≥ 1 argument on any path: `Window.orderBy(..)`, `Window.rowsBetween(..)`, and `Window.partitionBy()` with no arguments (or an empty list). SQL `OVER (...)` clauses are not checked.
- **Not reported:** a spec that is partitioned on some path only (`if keys: w = w.partitionBy(*keys)`, `Window.partitionBy(*k).orderBy(t) if k else Window.orderBy(t)`): without keys there is one series by design. A spec whose origin is unknown: a function parameter, an attribute set from one, or the result of a function annotated `-> WindowSpec` (the annotation is used, not the body).
- **Fix:** none. Help: `Add Window.partitionBy(<key>) — or use df.agg(...) for global totals.`
- **Ex:** `F.row_number().over(Window.orderBy("ts"))` → `Window.partitionBy("account").orderBy("ts")`, or `df.agg(...)` for global totals.

#### PRF006 `recomputed-lineage`
perf · warning · on · T2 · none · CY014, PA PERF007
- **Wording:** `{name}` is the DataFrame's name; reported on its assignment.
- **Why:** Each action re-executes the plan from source, including joins and shuffles. On Databricks the disk cache softens re-reads but not recomputed shuffles or UDFs.
- **Detect:**
  - Per DataFrame assigned to a name (def-use, path-sensitive): count the jobs that compute it: actions that run a job or bring rows to the driver, and writes, reached directly or through DataFrames derived from it. Uses inside a loop count as 2. Caching a derived DataFrame counts once and ends that path.
  - Fire when the count is ≥ 2, the DataFrame is not cached, not streaming, has no known row bound, and its plan contains an expensive step: `join` / `crossJoin` / `lateralJoin`, `groupBy` / `rollup` / `cube` / `agg` / `pivot`, `distinct` / `dropDuplicates`, `orderBy` / `sort`, `repartition` / `repartitionByRange`, `intersect` / `subtract` / `exceptAll`, `mapInPandas` / `mapInArrow` / `applyInPandas` / `applyInArrow`, a UDF call or a window function in an argument (a UDF value, a Column variable holding one, or SQL text calling a `spark.udf.register` name: `F.expr("my_udf(x)")`, `selectExpr`), a `spark.sql` query with a `JOIN`, `GROUP BY`, `DISTINCT`, window, `UNION` or `ORDER BY`, or a CSV / JSON / XML / JDBC read.
  - When one derived DataFrame carries all the recomputation, that one is reported instead of its parent.
- **Not reported:**
  - A cheap scan-filter-project (e.g. of a Delta table): recomputing it is cheaper than caching.
  - `show()` / `display()`, `printSchema` / `explain` and `.columns` do not count.
  - Each action counts once, however many paths lead to it: a name rebound on one branch (`if c: df = df.join(x)`) followed by `df.count()`, or a frame used twice by one downstream frame (`a.union(a)`, a self-join) feeding a single action.
  - Actions inside an `assert` message or a `raise` statement do not count: they run only when the check already failed. Neither do statements directly inside `with pytest.raises(..)` / `self.assertRaises(..)` (the test expects them to fail); a `try:` probe and a `pytest.warns(..)` block run to the end and do count.
- **Fix:** none. Help: ``Persist it once (`x = x.persist()` … `x.unpersist()`), or restructure so one job produces all outputs.``
- **Ex:** `agg = big.join(d,"k").groupBy("x").sum(); agg.write…; agg.count()` → `agg = (...).persist(); …; agg.unpersist()`

#### PRF008 `cache-single-use`
perf · warning · on · T2 · unsafe · CY005
- **Wording:** `{method}` is the method used, `cache` or `persist`; `never-consumed` when nothing reads it.
- **Why:** Cache adds serialisation and storage-memory pressure (and evicts useful blocks) without any reuse.
- **Detect:** A `cached` DataFrame value whose number of *consuming* uses on every path (actions, writes, or derivations that end in actions) is ≤ 1. A use inside a loop counts as many. A single derivation counts the actions (including `show()` / `display()`) that it and the frames derived from it run on one path: `s = df.agg(..); s.show(); s.collect()` reuses the cache and is not reported; a derived frame that is cached itself does not count as reuse of this cache. Uses inside an `assert` message, a `raise` statement or directly inside `with pytest.raises(..)` / `self.assertRaises(..)` do not count (they never run on the passing path; same counting policy as PRF006), so a cache read once normally and once in an error message is reported.
- **Not reported:** a cache whose frame, or a frame derived from it, is returned, yielded, stored or captured by a closure (its uses are out of sight), and a bare `df.cache()` / `df.persist()` statement on a function parameter (it caches the caller's DataFrame for the caller). `c = param.cache()` used once is still reported.
- **FP:** A cache meant to keep nondeterministic columns (`rand`, `uuid`) stable is still reported when it is read once; a cache does not guarantee that anyway (evicted blocks are recomputed), so write the frame or `checkpoint()` it, or suppress the line.
- **Fix (unsafe):** Remove `.cache()` / `.persist()` from an assignment (`x = df.cache()`). A bare `df.cache()` statement gets no fix.
- **Ex:** `df = spark.table("t").cache(); df.write.saveAsTable("u")` → drop the `.cache()`.

#### PRF009 `cache-never-unpersisted`
perf · info · on · T2 · none · CY025, PA PERF005
- **Why:** In long-running notebooks or shared all-purpose clusters, cached blocks hold executor memory until the session ends. That causes spills and evictions for other workloads.
- **Detect:** A DataFrame `x = df.cache()` / `persist()` (or a bare `df.cache()` statement) with no `unpersist()` on it or an alias anywhere after the cache (one `unpersist()` on any branch is enough), whose frame and derived frames do not escape (returned, yielded, stored, passed to an unknown call or captured by a closure). Module-level caches are reported only in notebooks (in a job file the session ends with the job). A `spark.catalog.clearCache()` anywhere in the file turns the rule off for that file. A cache PRF008 reports (read at most once) is not reported again here.
- **Ex:** `df.cache(); ...` → `try: … finally: df.unpersist()`

#### PRF010 `repartition-before-write`
perf · info · on · T1 · unsafe · CY008
- **Why:** On Delta in Databricks, optimized writes and auto compaction size files without a manual shuffle. A numeric `repartition(n)` costs a full shuffle and often produces worse file sizes.
- **Detect:** `repartition(...)` in the frame a `.write` / `.writeTo` writes, with only column-level steps between them (`select`, `selectExpr`, `withColumn(s)`, `withColumn(s)Renamed`, `drop`, `sortWithinPartitions`, `alias`, `toDF`, `withMetadata`), in the same expression or through up to three names bound once (`r = df.repartition(10)` then `r.write...`). Not reported: `repartitionByRange`, `coalesce`, a repartition followed by a filter, join, aggregate or other row-changing step.
- **Exemptions** (from cylint, extended):
  - Every repartition expression is a column name (`"c"`, `F.col("c")`) that is also a `partitionBy` / `partitionedBy` column, with or without a count (`repartition(8, "day")`). This is intentional: it bounds the number of files per output partition.
  - A literal count ≥ 100 (raising parallelism, not reducing files).
  - The repartitioned frame is built from driver-side data (`spark.createDataFrame` of a pandas frame, list or tuple, kept through row-wise steps, joins and unions with other such frames) or has a row bound (`agg`, `limit(n)`): the shuffle is cheap and only sets the number of output files.
  - A step after the repartition calls a user-defined function (`withColumn("c", my_udf("x"))`, a Column variable holding one, or SQL text calling a `spark.udf.register` name: `F.expr("my_udf(x)")`, `selectExpr("my_udf(x) AS c")`): the repartition spreads the UDF work over more tasks.
  - `repartition(1)` is reported by IOD003 instead when that rule is enabled.
- **Fix (unsafe):** Remove the `.repartition(...)`, only when it is in the same expression as the write (no fix through a name).
- **Ex:** `df.repartition(10).write.format("delta").saveAsTable("t")` → `df.write.format("delta").saveAsTable("t")`

#### PRF012 `explode-before-filter`
perf · warning · on · T1 · none · CY038
- **Why:** Explode multiplies rows. Filtering afterwards does row×array-size work. Catalyst can push some filters below Generate, but not those mixed with other projections or UDFs.
- **Detect:**
  - A `filter` / `where` on a DataFrame whose receiver chain (followed through up to three single-assignment names) reaches a `withColumn("name", <explode>)` or a `select(..)` with an explode in it (`explode`, `explode_outer`, `posexplode(_outer)`, `inline(_outer)`), and the condition reads none of the explode's output columns (the `withColumn` name, the select's `alias(..)` names, or `col` / `pos` / `key` / `value` without an alias) nor a column a later literal-named `withColumn` built from them.
  - Steps allowed between the explode and the filter: `withColumn` with a literal name, `drop`, `alias`, `cache`, `persist`, `distinct`, `dropDuplicates()` without a subset, `orderBy` / `sort` / `sortWithinPartitions`, `repartition` / `repartitionByRange` / `coalesce`, `hint`.
  - Reported on the explode call when the chain is one expression, otherwise on the filter.
- **Not reported:** a condition that names a Column variable (unknown columns); any other step in between (`limit`, `sample`, a union, `dropDuplicates(subset)`, `select`, `selectExpr`, `withColumns`, `withColumnRenamed`, `toDF`, a join, an aggregation, another filter); a later column whose name is not a literal, or whose value is a Column variable, a DataFrame attribute or an expression string naming an exploded column.
- **Fix:** none.
- **Ex:** `df.withColumn("i", F.explode("items")).filter(F.col("country")=="ZA")` → `df.filter(F.col("country")=="ZA").withColumn("i", F.explode("items"))`

#### PRF015 `collect-dedup-redundant`
perf · warning · on · T0 · safe · CY033, PA ARR001/ARR003
- **Why:** The first form materialises duplicates and then deduplicates them. The second is a no-op pass.
- **Detect:** `F.array_distinct(F.collect_list(e))` or `F.array_distinct(F.collect_set(e))` with one argument, including the `.over(w)` window forms; matched by qualified name, so local functions of the same name are not reported.
- **FP:** `collect_list` keeps encounter order and `collect_set` does not, so the collect_list fix is unsafe when order can matter: inside a window (`.over(w)`; an ordered window keeps collect_list order) or when the array is passed straight to `array_join`, `element_at`, `slice`, `get`, `array_position`, `concat_ws`, a subscript or `getItem`. Consumers further away (through an alias or a later column) are not followed.
- **Fix:** replace the whole expression with `<module>.collect_set(<args>)` (keeping `.over(w)`); safe for the collect_set form and for an order-free collect_list, unsafe in the cases above. No fix when `collect_list` / `collect_set` is imported as a bare name.
- **Ex:** `F.array_distinct(F.collect_list("x"))` → `F.collect_set("x")`

#### PRF022 `sql-select-star`
perf · info · off · T0 · none · CY004, PA F020
- **Why:** Column pruning is usually automatic through later projections. The real cost appears when `*` flows directly into a write, cache, collect or toPandas: every column is read, shuffled, stored or shipped to the driver.
- **Detect:** `spark.sql(..)` whose query text (a literal, an f-string, `sqlQuery=`, or a name bound once to one) has a select list starting with `*` or `alias.*` (also `*, more`, `DISTINCT *`, either side of a `UNION`). It fires only when the result reaches a write, a driver collect (`collect`, `toPandas`, `toLocalIterator`, ...) or a `cache` / `persist` / `checkpoint`, directly in the chain or through assigned names, with no `select` / `selectExpr` / `groupBy` / `agg` on the way.
- **Not reported:** `SELECT * FROM (subquery)` and `WITH … SELECT *` (already projected); `* EXCEPT (..)`; a star that is not the first select item (`SELECT a, b.*`); a query or chain with a known row bound (`LIMIT n`, `limit(n)`, `take` / `head` / `tail`): a few rows cost little whatever the column count; `show()` / `display()`, `count()`; SQL that writes by itself (`INSERT … SELECT *`, `CREATE TABLE … AS SELECT *`); `df.select("*")` (it projects nothing); the source format is not considered. `spark.sql("EXPLAIN … SELECT * FROM t")`: an `EXPLAIN` only plans its query and returns one row of plan text (no rule that reads `spark.sql` text checks the query inside an `EXPLAIN`).
- **Ex:** `spark.sql("SELECT * FROM t").write…` → `spark.sql("SELECT a,b FROM t").write…`

#### PRF023 `pivot-without-values`
perf · warning · on · T1 · none · new
- **Wording:** `{column}` is the pivot column as written. A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** Without `values`, Spark eagerly runs `distinct(col)` (a full scan plus shuffle, collected to the driver) just to work out the output schema. It does this every time the plan is built, so inside a loop it repeats per iteration. The docs call this "less efficient".
- **Detect:** A `pivot` call on a receiver the oracle types as `GroupedData` (also through a name or a typed `DataFrame` parameter) with a pivot column (positional or `pivot_col=`, a name or a Column) and no `values` (missing, or the literal `None`, positional or `values=`). Any other values argument, including a name, counts as given. Calls that pass `*args`/`**kw` are skipped, and so is a receiver whose type is unknown (an untyped parameter's `groupby`). Inside a loop or a comprehension the message says the job repeats. No fix: the values are data only the author knows.
- **FP:** Low. pandas `DataFrame.pivot` / pandas-on-Spark are not `GroupedData` and are never reported. A tiny or genuinely unknown domain is the legitimate case (suppress with `# noqa: PRF023`).
- **Ex:** `df.groupBy("year").pivot("course").sum("earnings")` → `df.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings")`

#### PRF024 `cube-many-columns`
perf · warning · on · T1 · none · new
- **Wording:** `{op}` is `CUBE` or `ROLLUP` (`ROLLUP over 8 columns makes 9 grouping sets`); the `sql-` variants are for `spark.sql` queries. A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** `cube(c1..cN)` is `GROUPING SETS` over all 2^N subsets. The Expand operator emits one copy of every input row per set before the shuffle, so N=6 is 64× the shuffle data. `rollup` makes N+1 sets (milder, reported from 8 columns). Several CUBE / ROLLUP / GROUPING SETS items in one SQL `GROUP BY` multiply: `GROUP BY CUBE(a, b, c), CUBE(d, e, f)` is 8 × 8 = 64 sets although each CUBE alone is small.
- **Detect:** `cube(...)` / `rollup(...)` on a receiver the oracle types as `DataFrame`, counting positional columns (a single list/tuple literal is expanded); `*cols` and keywords are skipped. Also `spark.sql(<text>)` (a literal, an f-string, `sqlQuery=`, or a name bound once to one) with `GROUP BY CUBE(..)` / `ROLLUP(..)` or `GROUP BY a, b, ... WITH CUBE|ROLLUP` (top-level expressions counted, nested parentheses and quotes respected). Thresholds: `cube-min-columns` (default 5) and `rollup-min-columns` (default 8); 0 disables. A SQL `GROUP BY` with several CUBE / ROLLUP / GROUPING SETS items is reported when one CUBE / ROLLUP reaches its threshold or the product of all items' sets reaches the sets of a `cube-min-columns` CUBE (32 by default); each `GROUP BY` clause (a subquery's included) is judged on its own, and the one with the most sets is reported.
- **FP:** Low-med (threshold rule, but the growth is exponential). Not reported: `cube` on pandas, numpy, user classes, untyped parameters and Py4J handles; `GROUPING SETS` alone (the explicit form); plain grouping columns next to a CUBE (they add no sets); `cube(..)` / `rollup(..)` used as a function outside `GROUP BY`; text inside string literals and comments; `EXPLAIN` queries.
- **Fix:** none.
- **Ex:** `df.cube("a", "b", "c", "d", "e").count()` → `spark.sql("... GROUP BY GROUPING SETS ((a, b), (a), ())")` listing only the sets you need (`df.groupingSets(...)` on Spark ≥ 4.0).

#### PRF025 `not-in-subquery`
perf · warning · on · T1 · none · new
- **Wording:** A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** To keep SQL's NULL semantics, Spark rewrites `x NOT IN (subquery)` as a left anti join on `x = y OR isnull(x = y)`. The `OR` rules out sort-merge and shuffled hash joins. Catalyst's own comment says it "will almost certainly be planned as a Broadcast Nested Loop join". Since Spark 3.1, a single-column subquery that fits under the broadcast threshold gets a null-aware broadcast hash join instead. Multi-column or large subqueries stay O(n·m), and Spark still tries to broadcast them even with `autoBroadcastJoinThreshold=-1`.
- **Detect:** `spark.sql(<text>)` on a SparkSession receiver, where the text is a literal, an f-string, `sqlQuery=`, or a name bound once to one. It must contain `NOT IN (` followed by `SELECT` or `WITH` (case-insensitive, extra parentheses allowed), or `NOT (<expr> IN (SELECT|WITH ...))` where the parentheses hold exactly that one IN test (the same plan), outside string literals and `--`/`/* */` comments. Inside a loop the message says it repeats.
- **FP:** Low-med. A tiny single-column subquery gets the fast null-aware broadcast hash join. If the column can hold NULLs, `NOT EXISTS` returns different rows (`NOT IN` returns nothing when the subquery yields a NULL), so the help text states that precondition. Not reported: literal lists (`NOT IN (1, 2)`), `IN (SELECT ...)`, `NOT EXISTS`, a `NOT (..)` around a larger condition (`NOT (id IN (SELECT ..) AND x > 1)`), `EXPLAIN` queries, unknown receivers and other `.sql` methods (sqlite, user classes), query text that is not constant. `df.where("...")` / `F.expr` strings and `%sql` cells are not scanned.
- **Fix:** none.
- **Ex:** `spark.sql("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)")` → `spark.sql("SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id)")`

#### PRF026 `many-distinct-aggregates`
perf · warning · on · T1 · none · new
- **Wording:** `{what}` names the call: `agg()`, `select()`, `selectExpr()` or `SQL SELECT`. A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** With more than one distinct column set in an aggregation, `RewriteDistinctAggregates` adds an Expand that emits one copy of every input row per distinct set (plus one when regular aggregates are present) before the first shuffle. N distinct counts mean roughly N× the shuffle data and memory; one reported job went from 65 M to 4.5 B internal rows with 68 `COUNT(DISTINCT)`s, and upstream benchmarks show 3-10× slowdowns at 2-6 distincts.
- **Detect:** `agg(...)` on a `GroupedData` (including after `pivot`) or `DataFrame`, `select(...)` and `selectExpr(...)` on a `DataFrame`, and `spark.sql(<const/f-string>)`. Counts distinct aggregates by their argument set: `F.countDistinct`/`count_distinct`/`sumDistinct`/`sum_distinct` anywhere inside the arguments (column names, `F.col`, `df.x`, `df["x"]` normalised; argument order ignored), `F.expr`/`selectExpr` strings and SQL `<fn>(DISTINCT ...)` (via `SqlText::distinct_aggregates`, per `SELECT`, outside strings and comments). Reports at `distinct-aggregates-min` (default 3; 0 disables) different sets. A starred comprehension (`*[F.countDistinct(c) for c in ["a", "b", "c"]]`, a generator, or a name bound once to one) counts one set per literal value when the aggregate reads the loop variable; the column list may be a name bound once to a literal list or tuple. Other starred lists are not counted. Inside a loop the message says it repeats.
- **FP:** Low-medium: on small inputs the copies are cheap, and exact counts are sometimes required (reporting). The same column counted several ways is one set and not reported. Not reported: comprehensions over a non-literal iterable (`df.columns`, a function result), with an `if` filter or several `for` clauses, a name bound on more than one branch, `approx_count_distinct`, `collect_set`, `select(...).distinct()`, unknown receivers (pandas `nunique`, user classes).
- **Fix:** none.
- **Ex:** `df.groupBy("day").agg(F.countDistinct("user"), F.countDistinct("session"), F.countDistinct("device"))` → `F.approx_count_distinct(...)` for each, or one aggregation per distinct column over `select(k, c).distinct()` joined on `k`.

#### PRF027 `order-by-rand`
perf · warning · on · T1 · none · new
- **Wording:** `{method}` is `orderBy` or `sort`. A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** A global sort is range-partitioned: Spark first runs a sampling job to find the range bounds, then shuffles every row. Sorting by a random key buys nothing but row order, and the usual `limit(n)` afterwards still funnels into one partition. `sample(fraction)` or `where(rand() < p)` is a narrow filter with no shuffle.
- **Detect:** `orderBy`/`sort` on a receiver the oracle types as `DataFrame` whose leading sort key (first positional argument, or the first element of a list/tuple) is `rand()`/`randn()` from `pyspark.sql.functions`, optionally wrapped in `.asc()/.desc()` (+ `_nulls_*`), `F.asc/F.desc(...)`, or `F.expr("rand()")` (the string may be a name bound once). Also `spark.sql(<const/f-string>)` with `ORDER BY rand()|randn()|random()` as the leading key outside strings, comments and window specs (parentheses holding no `SELECT`), including inside subqueries and CTEs. Inside a loop the message says it repeats.
- **FP:** Low. Not reported: a random tie-breaker after another key, `Window.orderBy(F.rand())` (per-group sampling with `row_number`), `sortWithinPartitions`, SQL `SORT BY`/`DISTRIBUTE BY rand()`, a materialised random column sorted by name (`withColumn("r", F.rand()).orderBy("r")`, not tracked yet), a frame with a known row bound or built on the driver (`df.limit(100).orderBy(F.rand())`, `spark.createDataFrame(rows)`), unknown receivers and `sort` methods of other objects.
- **Fix:** none.
- **Ex:** `df.orderBy(F.rand()).limit(100)` → `df.sample(fraction=0.01).limit(100)`

#### PRF029 `replicated-storage-level`
perf · warning · on · T1 · none · new
- **Wording:** `{level}` is the level as written (`DISK_ONLY_3 ... 3 times`, `StorageLevel(..., replication=2)` for a constructor); `{base}` in the help is the level without its `_2`/`_3` suffix (`MEMORY_AND_DISK` for a constructor). A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** The `_2`/`_3` levels replicate every cached block to another executor: double (or triple) the cache footprint, which evicts other cached data or spills, plus a network transfer of each block while the cache is built. All levels are already fault-tolerant through lineage recomputation; the guide reserves replication for fast recovery when serving requests. In batch ETL it is nearly always copy-paste.
- **Detect:** `persist(level)` / `persist(storageLevel=level)` on a DataFrame or RDD, `spark.catalog.cacheTable(t, level)` (Spark ≥ 3.5, positional or `storageLevel=`), where `level` resolves (through names bound once) to a `pyspark.StorageLevel` attribute ending in `_2`/`_3` or a `StorageLevel(...)` constructor whose fifth argument / `replication=` is an int literal > 1; and `spark.sql` text (a string, f-string or a name bound once) starting with `CACHE` whose `OPTIONS ('storageLevel' '<level>_2|_3')` (any case, `=` optional; text in comments or string values is ignored). Loops add a note.
- **FP:** Low. Not reported: unreplicated levels (`OFF_HEAP` included), levels from variables with several bindings, parameters or conditional expressions (`L2 if x else L1`), `persist` on user classes or pandas-on-Spark accessors (`psdf.spark.persist`), `StorageLevel` constructors with a non-literal replication.
- **Fix:** none.
- **Ex:** `df.persist(StorageLevel.MEMORY_AND_DISK_2)` → `df.persist(StorageLevel.MEMORY_AND_DISK)`

#### PRF031 `running-collect-list-window`
perf · warning · on · T1 · none · new
- **Wording:** `{name}` is the aggregate as called (`collect_list`, `collect_set`, `array_agg`); the `-unpartitioned` variants are for a window without `partitionBy()` / `PARTITION BY`.
- **Why:** An ordered window's default frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. With `collect_list`/`collect_set`/`array_agg` each output row carries its own copy of the array so far, so a partition of N rows materialises N·(N+1)/2 elements: executor OOM or huge spill and shuffle on large or skewed keys. The intent is usually "all values per key, in order" (one `groupBy().agg()` row per key) or "the last n values" (a bounded frame).
- **Detect:** `.over(w)` on a `pyspark.sql.functions.collect_list|collect_set|array_agg` call, where the oracle types `w` (through variables) as a `WindowSpec` with `orderBy` and either no frame or a last `rowsBetween`/`rangeBetween` whose start is unbounded (`Window.unboundedPreceding`, `-sys.maxsize`, or a constant int expression reaching back 1,000,000 rows or more: `-10**9`, `-(2**63)`, `-1_000_000`) and whose end is finite (`Window.currentRow` or a constant int below that size, e.g. `-1`). The same check reads SQL text in `F.expr(..)`, `df.selectExpr(..)` (each string, or a list of them) and `spark.sql(..)` (strings, f-strings or a name bound once; `EXPLAIN` skipped): `collect_list|collect_set|array_agg(..)` (an optional `FILTER (WHERE ..)` between) `OVER (..)` with `ORDER BY` / `SORT BY` and no frame, `ROWS|RANGE BETWEEN UNBOUNDED PRECEDING AND <CURRENT ROW | n PRECEDING | n FOLLOWING>`, or the short form `ROWS UNBOUNDED PRECEDING` (n PRECEDING >= 1,000,000 counts as unbounded); one finding per call. Without `partitionBy` / `PARTITION BY` / `DISTRIBUTE BY` the message adds that all rows land in one partition, and PRF005 is dropped at that site (precedence pair).
- **FP:** Low-med. Fine on tiny partitions. Not reported: unordered windows and explicit whole-partition frames, bounded frames (below 1,000,000 rows back), frames from the current row to the end (a different cost), frames whose bounds are variables, SQL `INTERVAL` range frames, named SQL windows (`OVER w` with a `WINDOW` clause), text inside string literals or comments.
- **Fix:** none.
- **Ex:** `F.collect_list("v").over(Window.partitionBy("k").orderBy("ts"))` → `df.groupBy("k").agg(F.sort_array(F.collect_list(F.struct("ts", "v"))))`, or `.rowsBetween(-10, Window.currentRow)` for the last 10 values

### UDF — User-defined functions

#### PRF032 `or-join-condition`
perf · warning · on · T1 · none · new
- **Why:** Spark extracts hash / sort-merge join keys only from equalities that are ANDed at the top of the condition. `(a.id == b.id) | (a.alt_id == b.id)` has equalities, so PRF004 stays quiet, but none of them is a key: the join is planned as a BroadcastNestedLoopJoin, or a Cartesian product when neither side fits a broadcast.
- **Detect:** `join` on a receiver the oracle types as `DataFrame` (a certain Column condition). The `on` argument (positional or `on=`) is split into ANDed conjuncts (`&` chains, list/tuple elements, Column variables bound once, up to 3 deep, `F.expr` SQL text split on top-level `AND` outside quotes and parentheses; OR binds looser than AND). Fires when some conjunct is an OR (`|`, SQL `OR`) containing an equality between non-literal operands (`==`, `eqNullSafe`, `=`, `<=>`) and no conjunct is a plain equality. When one side fits on the driver (row bound, driver-built data, catalog metadata) or is marked for broadcast (`F.broadcast(..)`, `hint("broadcast" | "broadcastjoin" | "mapjoin")`) the finding is info.
- **FP:** Low. Not reported: an equality conjunct next to the OR (Spark keys on it); the same key equality ANDed into every OR branch (`(k & a) | (k & b)`, sides in either order; Spark factors it out to `k & (a | b)`); ORs without equalities (PRF004's); an opaque conjunct next to the OR (user helper call, unresolved Column variable, negated variable: it may be the key); literal equalities (`c.region = 'eu'`, `orders.flag == 1`) count as filters, not keys; unknown receivers; `or` inside SQL string literals or backtick names. Not followed: a shared key held in a Column variable inside the branches, `df["k"]` vs `df.k` spellings of one column. SQL `JOIN … ON … OR` in `spark.sql` text is not checked yet.
- **Fix:** none.
- **Ex:** `a.join(b, (a.id == b.id) | (a.alt_id == b.id))` → `a.join(b, a.id == b.id).unionByName(a.join(b, a.alt_id == b.id))`

#### PRF035 `keyless-grouped-pandas`
perf · warning · on · T1 · none · new
- **Wording:** `{grouping}` and `{method}` are the ones called; a constant key reads `groupBy() on only a constant F.lit() key`; the `stateful` variants are for the streaming stateful functions. A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** An empty or constant grouping is one global group. For plain aggregates Spark still aggregates partially in parallel, but a grouped-map pandas/Arrow function receives each group as ONE frame: the whole DataFrame is shuffled into a single task and materialised in one Python worker, so one core works while the cluster idles, and the job OOMs once the data outgrows one executor. A streaming stateful function on one group keeps a single state and runs every micro-batch in one task.
- **Detect:** `applyInPandas`, `applyInArrow`, `apply` (grouped-map `pandas_udf`), `cogroup`, `applyInPandasWithState`, `transformWithStateInPandas` or `transformWithState` on a receiver the oracle types as `GroupedData`, whose grouping call (chained, or the single binding of a name) is `DataFrame.groupBy(..)` / `groupby(..)` with no columns (also `groupBy([])` / `groupBy(())`) or with only constant keys: `F.lit(<string / number / bool literal>)`, also aliased (`.alias(..)` / `.name(..)`), positional or in one list / tuple.
- **FP:** Low. Not reported: any real grouping column next to a constant, a plain string (a column name, even `"1"`), `F.lit` of a variable or a Column, `*cols` / list variables / keywords (columns unknown), aggregates on a keyless or constant grouping (`agg`, `count`, `avg`), an input that fits on the driver (a row bound such as `limit(n)` before `groupBy()`, or a frame built from driver data with `createDataFrame`), names bound in several branches, unknown receivers, pandas `groupby`. A constant column added earlier (`withColumn("g", F.lit(1)).groupBy("g")`) is not followed.
- **Fix:** none. Help: `Group by a real key, or use df.mapInPandas(fn, schema) to run the function once per partition in parallel.`
- **Ex:** `df.groupBy().applyInPandas(score, schema)` → `df.mapInPandas(score_batches, schema)`

#### PRF036 `forced-nested-loop-join-hint`
perf · warning · on · T1 · none · new
- **Wording:** `{hint}` is the hint name quoted as written (trimmed), or `SQL /*+ SHUFFLE_REPLICATE_NL */ hint` for the SQL form. A retry or first-match loop that leaves right after the call (an unconditional `break` / `return`) does not count as a loop.
- **Why:** The `SHUFFLE_REPLICATE_NL` join strategy hint makes Spark plan a `CartesianProduct` and filter it, |A|×|B| comparisons on every run, even when the condition has an equi-key that a hash or sort-merge join would use. It is usually left over from an experiment or copied from a theta-join example.
- **Detect:** `DataFrame.hint(name, ..)` whose name (first argument or `name=`, a string literal or a name bound once to one, any case, surrounding spaces ignored, extra hint parameters allowed) is `shuffle_replicate_nl`; `spark.sql` text (positional or `sqlQuery=`, f-strings, a name bound once) with a `/*+ ... SHUFFLE_REPLICATE_NL ... */` hint comment, with or without a table list, among other hints, spaces optional (`SqlText::hints()`: hint names at the top level of `/*+ */` comments; ordinary comments and string literals masked).
- **FP:** Low. Not reported: other strategy / partitioning hints, `F.broadcast`, names that only contain the text, non-constant hint names, ordinary `/* */` / `--` comments, string literals and identifiers in SQL, `EXPLAIN` queries, unknown receivers. A hint on a frame that is never joined is inert but still reported. A genuine theta join of two small inputs is a legitimate use (keep it with a comment / `# noqa`).
- **Fix:** none. Help: `Drop the hint and let Spark pick the join (or use BROADCAST / MERGE / SHUFFLE_HASH for an equi-join); keep it only for a theta join of two small inputs, with a comment saying so.`
- **Ex:** `orders.join(customers.hint("shuffle_replicate_nl"), "customer_id")` → `orders.join(customers, "customer_id")`

#### UDF001 `udf-has-builtin`
perf · warning · on · T0 · none · CY002, PA U001/U002/U003, ruff#7272
- **Wording:** `{builtin}` is the built-in equivalent to the body (`F.lower()`, `Column.rlike()`, ...).
- **Why:** Python UDFs serialise each row JVM→Python→JVM. They are opaque to Catalyst (no pushdown or constant folding) and run outside Photon. Replacing one with a builtin is often 2–10× on that stage.
- **Detect:** A UDF definition — `F.udf(fn | lambda)` (also `f=`), `@udf` / `@udf("type")` / `@F.udf(returnType=...)` on a def, `spark.udf.register("name", fn | lambda)` (also `f=`); a `def` or lambda passed by name, followed through every binding that reaches the name (each must be such a function with the same built-in) — whose body is a single return expression (a docstring is allowed) built only from the UDF's parameters and literals, and matching one of these shapes:
  - String methods on a parameter: `lower` / `casefold`, `upper`, `strip` / `lstrip` / `rstrip`, `title` / `capitalize`, `startswith`, `endswith`, `replace`, `split`, `zfill`, `find` / `index`, and `strftime` on a date parameter.
  - Slicing a parameter → `F.substring()`; `x[::-1]` → `F.reverse()`.
  - `len` → `F.length()` / `F.size()`, `abs`, `round`, casts `str` / `int` / `float` / `bool`, `math.sqrt` / `floor` / `ceil` / `log` / `log10` / `log2` / `exp` / `pow` / `fabs`.
  - `re.sub` → `F.regexp_replace()`, `re.findall` → `F.regexp_extract_all()`; `re.match` / `re.search` / `re.fullmatch` alone, wrapped in `bool(..)` or compared `is (not) None` → `Column.rlike()`.
  - `datetime.strptime` → `F.to_timestamp()`; `hashlib.md5 / sha1 / sha2xx(...).hexdigest()` → `F.md5()` / `F.sha1()` / `F.sha2()`; `json.loads(x)[<literal>]` → `F.get_json_object()`.
  - `x is None` / `x is not None`, `x in [literals]`, comparisons and arithmetic over parameters and numbers, `a + b` on parameters / literals (`F.concat()`).
  - `x if x is not None else d` → `F.coalesce()`; a null guard (`f(x) if x else None`, `f(x) if x is not None else None`) is looked through; `a if <simple test> else b` → `F.when().otherwise()`.
- **FP:** Bodies with library calls outside the table (e.g. `phonenumbers.parse`), helper calls, module-level names (a compiled `pattern.search(x)`), several statements, slices with another step (`x[::2]`) and match groups (`re.match(..).group(1)`) are not reported. Not followed: `pandas_udf` bodies, `def`s bound more than once, UDFs imported from another module. The named built-in is the closest match, not always a drop-in: Python raises on None where the built-in returns NULL, `str.split` takes a literal separator where `F.split` takes a regex, `str.find` counts from 0 where `F.instr` counts from 1.
- **Fix:** none. Help: `Python UDFs serialise every row JVM→Python→JVM and block Catalyst/Photon; the built-in runs natively (note: built-ins return NULL for NULL input).`
- **Ex:** `up = F.udf(lambda s: s.upper())` → `F.upper("s")`

#### UDF002 `udf-in-filter`
perf · warning · on · T1 · none · CY009
- **Why:** The UDF condition can't be pushed to Parquet/Delta, so there is no file or row-group skipping on it, and every row that reaches the filter goes through Python before rows are removed.
- **Detect:** The condition of `filter` / `where` (positional or `condition=`) on a DataFrame contains a call of a UDF value defined in this file (`F.udf`, `@udf`, `pandas_udf`), anywhere in the expression (inside `&`, `F.when`, comparisons); or SQL text naming a UDF registered in this file with `spark.udf.register("name", ...)` (case-insensitive), either as the filter string itself or inside `F.expr("...")`. A Column variable in the condition bound once (up to three hops) is followed to its value.
- **FP:** Low. Not reported: UDF names inside SQL string literals or comments, UDFs imported from another module or registered in another file, names bound on several branches, unknown receivers. A filter over a cross join whose UDF reads both sides is reported by UDF014 instead.
- **Fix:** none. Help: `Rewrite the predicate with built-in functions. Splitting it does not help: Spark already applies the native conditions of a filter before the UDF.`
- **Ex:** `df.filter(is_valid(F.col("email")))` → `df.filter(F.col("email").rlike(r"^[^@]+@[^@]+$"))`

#### UDF006 `udf-missing-return-type`
correctness · warning · on · T0 · none · new
- **Wording:** `non-string-return` is a warning; `unknown-return` (the return value's type is not known) is info.
- **Why:** A silent type change. Spark turns a non-string result into its Java string form (`10` -> `"10"`, a list -> `"[1, 2]"`), so comparisons become string comparisons (`"10" < "9"`) and writes fail or change the table schema.
- **Detect:** `pyspark.sql.functions.udf` (also an undefined `udf` name in notebooks/snippets) used as a bare `@udf`, `@udf()` / `@udf(useArrow=True)`, or called with the function (`F.udf(fn)`, `F.udf(lambda ..)`, `F.udf(f=..)`), and `spark.udf.register("n", fn)` / `register("n", f=fn)` with no third argument, when no `returnType` is given (positional second argument, `returnType=` keyword or `**kwargs` all count as given). The function is a lambda, or a name whose every reaching binding is a lambda or a same-file `def` (the return kinds of all bindings are joined; a binding the oracle cannot follow means not reported). Its return kind comes from the return annotation first (`int`, `float`, `bool`, `list`, `dict`, `tuple`, `set`, `List[..]`, `Dict[..]` = non-string; `str` = string; `Optional[X]`, `Union[X, None]` and `X | None` take X), otherwise from every `return` value: number / bool literals, containers and comprehensions, comparisons, `not`, `- * / // ** %` arithmetic (string formatting with `%` stays a string), `int() float() bool() len() round() abs() sum() sorted() json.loads() math.*`, `.split() .count() .find() .index() .items()` are non-strings; string literals, f-strings, `str() repr() format() json.dumps()` and string methods (`.upper()`, `.strip()`, `.format()`, `.join()`, `.isoformat()`, `.strftime()`, ...) are strings. Any non-string return makes it a warning; returns that are all strings or `None` are not reported; anything else (a variable, an unknown call, `x or ""`) is info.
- **Not reported:** a function that is already a UDF (`spark.udf.register("n", my_udf)`), imported functions, attributes (`F.udf(mod.fn)`), names bound on several branches, `pandas_udf` (Spark requires its return type). A name that ends in `Type` or is ALL_CAPS as the first argument is read as a return type, not as the function.
- **Fix:** none (the right type is a judgement call).
- **Ex:** `@udf def f(x): return x*2` → `@udf("long") def f(x): return x*2`

#### UDF011 `pandas-udf-per-batch-init`
perf · warning · on · T1 · none · new
- **Wording:** `{call}` is the call as written; a call through a helper reads `<helper>() (which calls <call>())`. `series-udf`: a Series -> Series pandas UDF; `grouped-udf`: a Series -> scalar grouped aggregate, legacy `GROUPED_AGG` / `GROUPED_MAP`; `batch-loop`: `{api}` is `iterator pandas_udf`, `mapInPandas` or `mapInArrow`; `per-group-function`: `{api}` is `applyInPandas` or `applyInArrow`.
- **Why:** A Series→Series `pandas_udf` body runs once per Arrow batch (~10k rows) on every task; a grouped-aggregate pandas UDF and `applyInPandas` run once per group. Loading a model, a tokenizer, a cloud client or a DB connection there repeats that cost thousands of times. The iterator forms (`Iterator[pd.Series] -> Iterator[pd.Series]`, `mapInPandas`) let you initialise once per partition, before the batch loop.
- **Detect:** The body of a function decorated with or passed to `pandas_udf` (a `def` or lambda; non-`Iterator` type hints, not `SCALAR_ITER`), or passed to `GroupedData/cogroup.applyInPandas`/`applyInArrow` (a name is followed through every binding that reaches it: each lambda or same-file `def` is checked), calls a heavy initialiser: `mlflow.*.load_model`, `joblib.load`, `pickle.load`, `cloudpickle/dill.load`, `torch.load`, `*.from_pretrained`, `transformers.pipeline`, keras `load_model`, `boto3.client/resource`, or a DB connect (`psycopg/psycopg2/pyodbc/pymysql/mysql.connector/cx_Oracle/oracledb/snowflake.connector/databricks.sql.connect`, `sqlalchemy.create_engine`, `pymongo.MongoClient`, `redis.Redis/StrictRedis/from_url`). A pandas UDF whose return annotation is a scalar (`int`, `float`, `bool`, `str`, a numpy scalar) or whose `functionType` is `GROUPED_AGG` / `GROUPED_MAP` gets the per-group message. For iterator pandas UDFs (`Iterator` / `Iterable` / `Generator` annotation on the first parameter or the return) and `DataFrame.mapInPandas`/`mapInArrow` only calls inside a `for` or comprehension over the first parameter are reported; loading before the loop is the recommended pattern. Names are matched as resolved qualified names. One level of same-file helper calls is followed (a name bound once to a `def`).
- **FP:** Low-med. Not reported behind module-level caching: a `functools.lru_cache`/`cache`/`cached_property`-decorated loader, or an assignment to a module-level cache (`global _m; if _m is None: _m = load()`, `_CACHE[k] = load()`). A cache written some other way (e.g. a class attribute, `cachetools`) may still be reported; suppress with `# noqa: UDF011`.
- **Not reported:** loads in an iterator UDF reached through `map(lambda s: ..., it)` or a `while` loop (only `for` / comprehensions over the iterator count), nested `def`s and classes in the body, helpers from other files, the legacy `groupby().apply(pandas_udf)` call site (its `GROUPED_MAP` def is still reported), `pickle.loads` of a broadcast value.
- **Fix:** none.
- **Ex:** `@pandas_udf("double")\ndef f(s: pd.Series) -> pd.Series: m = mlflow.pyfunc.load_model(uri); ...` → `def f(it: Iterator[pd.Series]) -> Iterator[pd.Series]: m = mlflow.pyfunc.load_model(uri); for s in it: yield ...`

#### UDF012 `per-row-external-connection`
perf · warning · on · T1 · none · new
- **Wording:** `{sink}` is `udf`, `DataFrame.foreach`, `RDD.<method>`, `writeStream.foreach` or `ForeachWriter`; `{name}` is the def, the name a lambda is bound to, `<lambda>` for an inline lambda or `<Class>.process`. A call through a helper reads `<helper>() (which calls <call>())`.
- **Why:** A plain UDF, `foreach` or RDD row function runs once per record. Creating an HTTP session, a cloud client or a DB/socket connection there costs a TCP/TLS handshake (and often auth) per row and throttles throughput. Spark's guide calls this a common mistake and prescribes `foreachPartition` (or a pooled client).
- **Detect:** The body of a function or lambda (inline, or a name followed through every reaching binding to a `def` or a lambda) decorated with/passed to `udf` (positionally or as `f=`) / `spark.udf.register` (second argument or `f=`), `DataFrame.foreach`, `RDD.map/flatMap/filter/foreach/mapValues/flatMapValues`, `writeStream.foreach(fn)`, or the `process` method of a streaming foreach writer (a class passed to `writeStream.foreach`, or any class defining `open`, `process` and `close`) calls `requests.get/post/put/patch/delete/head/request/Session`, `httpx.*`, `urllib.request.urlopen`, `urllib3.PoolManager`, `boto3.client/resource`, `socket.socket/create_connection`, or a DB connect (same table as UDF011). Names are matched as resolved qualified names (`from requests import post`, an `import` inside the body). One level of same-file helper calls is followed (a name bound once to a `def`).
- **FP:** Low. Not reported for `foreachPartition`, `mapPartitions`, `foreachBatch`, `ForeachWriter.open`, pandas UDFs, a module-level/pooled client (`SESSION.get`), an `lru_cache`d client factory, or a network client stored in a module-level cache on first use (`global _s; if _s is None: _s = requests.Session()`). Receivers must be known Spark DataFrames/RDDs/stream writers, so pandas `.map`, builtin `map` and user classes are ignored.
- **Not reported:** a function or lambda name bound on more than one branch, helpers from other files, nested `def`s and classes in the body, a network client created through a non-listed wrapper library, a ForeachWriter class defined outside this file.
- **Fix:** none.
- **Ex:** `df.foreach(lambda r: requests.post(url, json=r.asDict()))` → `df.foreachPartition(send_partition)` with one `requests.Session()` per partition

#### UDF013 `udf-whole-row-struct`
perf · info · on · T1 · none · new
- **Wording:** `{udf}` is `UDF <name>` (`UDF` for an inline `F.udf(..)(..)`); `{how}` is `the whole row as a struct`, or `every column as an argument` for a starred column list.
- **Why:** A UDF called with `struct("*")` or every column of the frame serializes all columns (wide strings, arrays, blobs included) to the Python worker and back for each row, pickle or Arrow. Because the UDF references every column, column pruning below it stops working, so the scan reads every column of a columnar table too.
- **Detect:** A call of a value the oracle types as `UserDefinedFunction` (`F.udf`, `@udf`, `pandas_udf`, an inline `F.udf(...)(...)`) with an argument that is `F.struct("*")` / `struct(F.col("*"))` / `struct(df["*"])`, `F.struct(*df.columns)` / `F.struct(df.columns)`, an unfiltered comprehension over `df.columns` (inside `struct` or starred), or a starred `*df.columns` (`df` typed `DataFrame`). Positional and keyword arguments count; a name bound once to one of these struct forms is followed one hop (`row = F.struct("*"); f(row)`).
- **FP:** Low-med (a narrow frame costs little). Not reported: explicit column lists, a starred variable of names, comprehensions with an `if` (a chosen subset), built-ins over the whole row (`to_json(struct("*"))`, `xxhash64(*df.columns)`), a `struct("*")` nested inside another built-in before reaching the UDF, plain Python functions, pandas frames' `.columns`, `pandas_udf(..., functionType=X)` with X not `SCALAR`/`SCALAR_ITER` (grouped-map / grouped-agg UDFs take the whole frame by design; pyspark's own `applyInPandas` builds these).
- **Not reported:** a name bound on more than one branch, `df.columns` copied into another variable first (`cols = df.columns; f(*cols)`), structs built in other files.
- **Fix:** none.
- **Ex:** `df.withColumn("k", row_key(F.struct("*")))` → `df.withColumn("k", row_key(F.col("id"), F.col("name")))`

#### UDF014 `udf-in-join-condition`
perf · warning · on · T1 · none · new
- **Wording:** `{place}` is `a join condition`, or `a filter over a cross join` for a filter after `crossJoin` / a condition-less `join`.
- **Why:** Spark extracts join keys only from equalities. An opaque Python function of both sides gives none, so the join becomes a Cartesian product / BroadcastNestedLoopJoin and the UDF is evaluated for every pair of rows (130k x 20m = 2.6 trillion Python calls in the cited question). PRF004 stays quiet because opaque calls are unknown to it.
- **Detect:** `join` on a receiver the oracle types as `DataFrame`: the `on` argument is split into ANDed conjuncts (`&` chains, list/tuple elements, single-assignment Column variables up to 3 deep, `F.expr` SQL text). Fires when some conjunct calls a UDF (a value typed `UserDefinedFunction`: `F.udf`, `@udf`, `pandas_udf`; or SQL text calling a name registered with `spark.udf.register` in the file) and no conjunct is a usable equality or opaque. Also `a.crossJoin(b).where/filter(...)` and condition-less `a.join(b).filter(...)` when the UDF's arguments read columns of two frames (`a.x`, `b["y"]`). Either side small → info: it fits on the driver (row bound such as `limit(n)`, driver-built data, catalog metadata), `F.broadcast(..)` or a `hint("broadcast" | "broadcastjoin" | "mapjoin")` (same test as PRF004 / PRF032).
- **FP:** Low. Not reported: an equality conjunct next to the UDF (Spark keys on it, then applies the UDF per candidate pair), an equality with a one-frame UDF on one side (`a.k == norm(b.k)` is still a key), an opaque helper or unresolved Column variable next to it, unknown receivers, filters over a cross join whose UDF reads one frame (Spark pushes those below the join; UDF002 covers them). Overlap: UDF014 wins over PRF032 (UDF ORed with an equality) and UDF002 (UDF filter over a cross join) at the same call.
- **Not reported:** SQL `JOIN ... ON udf(..)` inside `spark.sql` text, UDFs registered in other files, frames referenced through columns that are not `df.col` / `df["col"]` (e.g. `F.col("a.x")` after an alias) in the cross-join filter form.
- **Fix:** none.
- **Ex:** `a.join(b, fuzzy(a.name, b.name))` → `a.join(b, F.soundex(a.name) == F.soundex(b.name)).where(fuzzy(a.name, b.name))`

### IOD — I/O & Delta

#### IOD001 `read-schema-inference`
perf · warning (info for a CSV read without inferSchema, error for a streaming file source) · on · T1 · none · CY018, PA F019
- **Wording:** `{format}` is the source format as written (`JSON` / `XML` upper-cased in `inferred`); `csv-strings` is info, `streaming-no-schema` an error, `streaming-schema-inference` a warning (the file sets spark.sql.streaming.schemaInference).
- **Why:**
  - `inferSchema` (CSV) and JSON / XML inference run an extra job over the input (or `samplingRatio` of it) before the real read.
  - Schemas change silently when files change.
  - `mergeSchema=true` on Parquet / ORC reads every file footer.
  - A streaming file source without a schema fails when the query is defined; with `spark.sql.streaming.schemaInference` it infers once, from the files present at start.
- **Detect:** a `DataFrameReader` / `DataStreamReader` call (`csv`, `json`, `xml`, `parquet`, `orc`, `text`, `load`) whose format comes from the method, `format(..)` or `load(format=..)` / positional `load(path, "json")`, with options read from `.option()` / `.options()` (also a literal `**{..}`) and keyword arguments (a keyword argument that is `None`, also a name bound to `None`, is not set: pyspark drops it, so `csv(p, inferSchema=None)` is the all-strings info case), and no schema (`.schema(..)`, `schema=`, positional schema argument; a schema argument that is `None`, also a name bound to `None` on every path, counts as no schema, since pyspark then infers):
  - CSV: warning with `inferSchema` true (keyword or option, any casing), info otherwise (all columns StringType).
  - JSON, XML: warning.
  - Parquet, ORC: warning only with `mergeSchema` true.
  - Streaming csv / json / xml / parquet / orc / avro sources: error, or a warning when the file sets `spark.sql.streaming.schemaInference` (conf.set, builder config, SparkConf, SQL `SET`). Streaming `text` has a fixed schema and is not reported.
- **Not reported:**
  - A batch read only asked for its schema (`spark.read.json(sample).schema`, `.columns`, `.dtypes`, `.printSchema()`): inferring the schema is the point there (often to pass it to a streaming reader). A schema taken after a transformation, or from a name that is also used for data, is still reported.
  - Auto Loader (`format("cloudFiles")`), which has its own schema handling.
  - A read over an RDD that fits the driver (the type checker's size verdict): `sc.parallelize([literal strings])` (also `c=`, or a name holding a literal list on every path and never filled later), kept through RDD steps, and the `.rdd` of a frame that fits the driver (`spark.createDataFrame([..]).rdd.map(..)`, `df.limit(100).rdd`): inferring from a few driver-side rows costs nothing (tests, examples).
  - Batch avro, text and Delta reads, and an unknown format (a variable).
  - Readers whose settings are not all visible: an opaque `**opts`, or a reader configured by separate `r.option(..)` / `r.schema(..)` statements.
  - The session-wide `spark.sql.parquet.mergeSchema` setting.
- **FP:** A CSV read meant to be all strings (raw landing data) is still reported as info; suppress it per line.
- **Fix:** none.
- **Ex:** `spark.read.option("header",True).option("inferSchema",True).csv(p)` → `spark.read.schema(SCHEMA).option("header",True).csv(p)`

#### IOD002 `jdbc-read-unpartitioned`
perf · warning (info for a filtered or limited subquery) · on · T1 · none · new
- **Wording:** `unpartitioned-query` is a read through the `query` option.
- **Why:** The whole table is pulled through one executor core while the rest of the cluster idles. A major cause of slow, expensive ingestion jobs. `numPartitions` alone does not split a read: without `partitionColumn` and its bounds Spark still issues one query.
- **Detect:** A batch `DataFrameReader` call whose format is `jdbc`: `spark.read.jdbc(url, table, ...)` (positional or keyword arguments) and `format("jdbc")` / `load(format="jdbc", ...)` chains, with the options read from `.option()` / `.options()` / keyword arguments / a `properties=` dict (also through names bound once). Reported when neither `partitionColumn` (`column=` in `jdbc()`, any casing) nor `predicates=` is set; an argument that is `None` (also a name bound to `None`, or positional `None`s) is not set, since pyspark then reads on one connection. Info instead of warning when the `dbtable` or `query` text restricts its rows (`WHERE`, `LIMIT`, `SELECT TOP n`, `FETCH FIRST|NEXT`, read outside quoted strings and comments): a deliberate extract that is likely small.
- **FP:** A small lookup table read in full is still reported (no row count is known statically); suppress it per line. A `WHERE` is only a hint that the extract is small, so filtered subqueries stay visible as info.
- **Not reported:** options that cannot all be seen (`.options(**opts)` with an unknown dict or a dict name used more than once, a reader configured by separate `reader.option()` statements), a format that differs between `if` / `else` paths, Databricks named connectors (`format("postgresql")`, `"sqlserver"`, `"mysql"`, ...), and `fetchsize` (no check exists).
- **Fix:** none.
- **Ex:** `spark.read.jdbc(url,"orders",properties=p)` → `spark.read.jdbc(url,"orders",column="id",lowerBound=1,upperBound=n,numPartitions=32,properties=p)`

#### IOD003 `coalesce-one-before-write`
perf · warning · on · T1 · unsafe · CY013, PA S007
- **Wording:** `{call}` is the call or hint as written; `coalesce` is for `coalesce(1)` and the coalesce hint, `repartition` for `repartition(1)` and the repartition hint.
- **Why:** A single-core write of the full output. With `coalesce(1)` the upstream stage collapses to one task too (no shuffle boundary).
- **Detect:** a batch DataFrame write (`.write` ... `save` / `saveAsTable` / `insertInto` / `parquet` / `csv` / `json` / `orc` / `text` / `jdbc` ..., or `.writeTo(..)`) whose frame comes from a one-partition step: `coalesce(1)`, `repartition(1)` or `repartition(1, cols)` (also `numPartitions=1` and a name bound once to a constant 1), or a `hint("coalesce" | "repartition", 1)` (any casing). Steps between that do not repartition (`select`, `filter`, `withColumn`, other hints such as `broadcast`, ...) are skipped; any shuffle (`groupBy`, `join`, `orderBy`, `distinct`, `union`, another `repartition` / `coalesce` / partitioning hint) after the one-partition step ends the search. Names are followed through every binding that reaches the write, up to four name hops (`one = df.coalesce(1)` then `one.write.parquet(p)`); a name bound on several paths (`if`/`else`) is reported when EVERY path ends in a one-partition step, even through different steps (`coalesce(1)` on one branch, `repartition(1)` or `coalesce(1).select(..)` on the other), at the first path's step.
- **Info, not warning:** (on every path) the frame before the step has a row bound (`limit(n)`, an aggregate to one row), is a grouped aggregate, or is built from driver-side data (`spark.createDataFrame(pdf)` / a list): a small single-file export.
- **Not reported:** another or unknown partition count, a name with one path that keeps its partitions or shuffles after the step (`else: out = df`), an if-expression (`df.coalesce(1) if x else df.repartition(1)`), a frame passed to a function that writes it, `rebalance` / `repartition_by_range` hints, streaming writes (`writeStream`), `rdd.coalesce(1).saveAsTextFile(..)`, SQL `/*+ COALESCE(1) */` hints in `spark.sql` text, frames that are not written (`toPandas()`, `collect()`).
- **Fix (unsafe):** only for Delta targets (`format("delta")` or `writeTo(..)`) and only when the step is in the same expression as the write (so every path shares it): remove the step (`.coalesce(1)`, `.repartition(1)`, `.hint(..)`).
- **Ex:** `df.coalesce(1).write.format("delta").save(p)` → `df.write.format("delta").save(p)`

#### IOD004 `insert-into-positional`
correctness · warning · on · T1 · unsafe · CY036, ruff#7272
- **Why:** Column order mismatches silently write data into the wrong columns. `insertInto(..., overwrite=True)` with static `partitionOverwriteMode` truncates every partition.
- **Wording:** three message variants: append (`insertInto() matches columns by position …; use writeTo(t).append()`), overwrite (`insertInto(overwrite) … may overwrite every partition — use writeTo(t).overwritePartitions()`), and layout (`insertInto() cannot be combined with partitionBy()` / `bucketBy()`: Spark rejects the write because the target table's definition sets its layout), the last one an error with the help `Drop the partitionBy() call (the table's own layout is used), and write by name with writeTo(t).append().`
- **Detect:** a `DataFrameWriter.insertInto(..)` call (any format, Delta too: `insertInto` is positional there as well), also through a writer held in a variable (`w = df.write.mode(..)` then `w.insertInto(t)`) or typed by its annotation (`wr: DataFrameWriter`). The overwrite message is used for `overwrite=True` (keyword or second positional argument, also a name bound to `True`) or for a `.mode("overwrite")` / `.mode(saveMode="overwrite")` when the `overwrite` argument is missing or `None` (pyspark only applies the flag when it is not `None`); `overwrite=False` appends, whatever the mode. A writer chain with `partitionBy(..)` or `bucketBy(..)` gets the layout message instead.
- **Not reported:** SQL `INSERT INTO t SELECT ..` text (also positional), user classes with an `insertInto` method. A mode set by a separate statement on a held writer (`w.mode("overwrite")` on its own line) is not read: such calls get the append message.
- **Fix (unsafe):** `df.write.insertInto(t)` → `df.writeTo(t).append()`; overwrite → `df.writeTo(t).overwritePartitions()`. Offered only when the writer chain is in the same expression and has no step other than `mode("append" | "overwrite")` (no `format`, `option`, `partitionBy`, ...) and `insertInto` gets only the table name and an `overwrite` flag whose value is known (`True`, `False`, `None` or a name bound to one). No fix for the layout message. The semantics differ (by-name matching, dynamic partition overwrite), hence unsafe.
- **Ex:** `df.write.insertInto("db.t", overwrite=True)` → `df.writeTo("db.t").overwritePartitions()`

#### IOD006 `write-in-loop`
perf · warning · on · T2 · none · new
- **Wording:** `{target}` is the table name or path as written.
- **Why:**
  - Per-iteration writes create many small files and many Delta commits (log growth, OPTIMIZE debt).
  - Each iteration re-plans and re-scans.
  - Concurrent-append conflicts.
- **Detect:** a DataFrame write inside a `for` / `while` body (or a comprehension) that appends: `mode("append")`, `mode="append"` or a positional `"append"` mode in `save` / `saveAsTable` / `parquet` / ..., `insertInto(t)` without `overwrite=True` (and no other mode), or `writeTo(t).append()`. The target (table / path argument, `option("path", ..)`, the `writeTo` table) must not depend on the loop variable or on a name rebound in the loop body. Reported at the write method, once per write call.
- **Severity:** info when the loop the write sits in iterates a literal or constant iterable (as ACT005) of at most `driver-loop-small-max` items (default 10); warning otherwise (including `while` loops and loops over collected keys).
- **FP:**
  - Loops over distinct targets (`saveAsTable(f"bronze.{t}")`, a target bound inside the loop) are orchestration: not reported (ACT005 covers them).
  - Overwrites (including chunked `replaceWhere` backfills), an unknown mode (`mode(m)`) and streaming writes are not reported.
  - Retry and first-match loops are not reported: a write followed in the same block by an unconditional `break` or `return` runs once (`for attempt in range(3): try: ...save(..); break` / `except: sleep`). A `continue` in an `except` handler still counts as a retry; any other `continue` between the write and the `break`, or a conditional `break` (`if done(): break`), keeps the finding. A `break` only leaves the innermost loop: a retry loop nested in a loop over parts is still reported for the outer loop.
  - Writes in a function defined inside the loop (a `foreachBatch` sink) are not in the loop.
  - pandas `to_csv(mode="a")` and other non-Spark writers are not reported.
- **Overlap:** wins over ACT005 at the same write.
- **Not reported:** Delta `merge(..).execute()` in a loop, SQL `INSERT INTO` text, and writes made by a helper function the loop calls.
- **Fix:** none.
- **Ex:** `for d in days: compute(d).write.mode("append").saveAsTable("t")` → `reduce(DataFrame.unionByName, [compute(d) for d in days]).write.mode("append").saveAsTable("t")` (or one set-based compute).

#### IOD015 `unsplittable-compressed-read`
perf · info · on · T1 · none · new
- **Wording:** E.g. `Reading gzip-compressed csv (/raw/x.csv.gz): ...`; paths longer than 48 characters are shown by their last 40.
- **Why:** Spark picks the codec of a text-based file (CSV, JSON, text) from its extension, and only bzip2 streams can be split. A single large gzip / zstd / LZ4 / Snappy / deflate file becomes a one-core stage and a skewed partition downstream.
- **Detect:** `csv` / `json` / `text` / `load` on a `DataFrameReader` or `DataStreamReader` (format from the method, `format(..)`, `format=` or Auto Loader `cloudFiles.format`) whose path (positional, `path=`, `paths=` for a batch `text` read, or a `path` option in the chain; a literal, an f-string, a name bound once, or a list / tuple of them) ends in `.gz` / `.gzip` (gzip), `.zst` (zstd), `.lz4`, `.snappy` or `.deflate`, case-insensitive, globs such as `*.csv.gz` included. One finding per call, for the first matching path.
- **Not reported:** `multiLine=True` CSV / JSON reads and `wholetext=True` text reads (each file is read whole by one task anyway, so re-compressing would not split it); `.bz2` (splittable); extensions Spark does not decode (`.zip`, `.zstd`); Parquet / ORC / Avro / Delta / XML reads; paths built by calls (`os.path.join`, `pathlib`) or names bound more than once; separate `reader.option(..)` statements; pandas `read_csv` and RDD `textFile`.
- **FP:** Medium: many small compressed files parallelise fine and file sizes are unknown statically, hence info.
- **Fix:** none.
- **Ex:** `spark.read.csv("/raw/big.csv.gz")` → re-compress as bzip2 / split upstream / land once as Delta; otherwise `spark.read.csv(p).repartition(64)`.

#### IOD016 `json-schema-inference-via-rdd`
perf · warning · on · T2 · none · new
- **Wording:** `rdd-of-text-files` is an RDD of file text (a type-checker fact): lines (`sc.textFile(..)`) or whole-file contents (`sc.wholeTextFiles(..).values()` / `.map(..)` picking the second item: `lambda kv: kv[1]` or `kv[-1]` under any parameter name, a name bound to such a lambda, `operator.itemgetter(1)`), kept through rows-only RDD steps (`filter`, `distinct`, `sample`, `coalesce`, `repartition`, `cache`, a union of two such RDDs) and through names; `map` / `flatMap` over the text drops it. Its help names `multiLine=True` for one-document-per-file input.
- **Why:** The popular recipe for a JSON string column's schema (`spark.read.json(df.rdd.map(lambda r: r.payload)).schema`) converts every row to a Python object and back, then infers types over the whole column, before `from_json` reads the data again.
- **Detect:** `json(...)` on a `DataFrameReader` (oracle Certain) whose first argument (`path=` too) is typed RDD (`df.rdd`, `.rdd.map(..)` / `.flatMap(..)`, `sc.textFile(..)`, a name bound to one), with no schema in the call (`schema=` / 2nd positional; `None`, or a name bound to `None` on every path, counts as no schema) or the reader chain (`.schema(..)`, `option("schema", ..)`). At the same call it wins over IOD001.
- **Not reported:** an RDD that fits the driver (tests, examples): `sc.parallelize([literal list])` (also `c=`, or a name holding a literal list on every path and never filled later by `append` / item stores), through RDD steps and names, and the `.rdd` of a frame that fits the driver (`spark.createDataFrame([..]).rdd`, `df.limit(n).rdd`); arguments of unknown type (untyped parameters); readers configured by separate `reader.option(..)` statements or `**opts` (the schema may be set there); file-path reads (IOD001's case).
- **FP:** Low: the pattern is explicit. `samplingRatio` still reports (every row still goes through Python).
- **Fix:** none.
- **Ex:** `spark.read.json(df.rdd.map(lambda r: r.payload)).schema` → an explicit DDL schema, `F.schema_of_json` on one sample value, or `parse_json` into VARIANT on Databricks.

#### IOD017 `jdbc-write-unbounded-connections`
perf · info · on · T1 · none · new
- **Wording:** The `-no-batchsize` variants: no `batchsize` is set; the `-in-loop` variants (warning): the write repeats in a loop.
- **Why:** Each partition of the written frame opens its own JDBC connection, all concurrently. After a shuffle that is `spark.sql.shuffle.partitions` (200) connections, beyond most databases' limits (MySQL's default is 151), so the write throttles, retries or fails while the cluster idles. `numPartitions` caps the connections (Spark coalesces to it). The default `batchsize` of 1000 rows per round trip is the second common slow-write cause; the message mentions it when unset.
- **Detect:** a JDBC write on a `DataFrameWriter` (`df.write.jdbc(...)`, `df.write.format("jdbc")...save()`, `save(format="jdbc")`, through a stored writer) whose settings (via `IoSettings::write`: options, `options(**dict)`, the `properties` dict) are complete and lack `numPartitions`. `numPartitions` is matched case-insensitively. The partition count is read from integer literals and names bound once to a constant (arithmetic folded: `MANY // 2`).
- **Not reported:** frames given a small or unknown explicit partition count (`coalesce(n)` / `repartition(n)` / `numPartitions=`, n <= 64 or a parameter), followed back through narrow transformations (select, withColumn, filter, drop, cache, hint, ...) and names bound once; opaque option dicts / `properties` values; a stored writer configured by a separate statement. Still reported: `repartition("col")` (also a column name held in a constant), `repartition(400)`, or a shuffle after the coalesce. Warning only when the write repeats in a loop: a retry or first-match loop that leaves right after the write (an unconditional `break` / `return`) stays info.
- **Fix:** none.
- **FP:** Low-medium: a small frame with few partitions is harmless (the partition count is unknown statically), hence info.
- **Ex:** `df.write.jdbc(url, "events", mode="append")` → `df.write.option("numPartitions", 8).option("batchsize", 10000).jdbc(url, "events", mode="append")`.

#### IOD018 `repair-table-in-job`
perf · info · on · T1 · none · new
- **Wording:** `{how}` is `Partition repair (REPAIR TABLE / RECOVER PARTITIONS)` or `spark.catalog.recoverPartitions`; `repair-in-loop` (a repeating loop) is a warning.
- **Why:** Partition recovery walks the whole table directory tree on every run (one storage listing per partition), so a daily job that adds one partition pays for all of them, and the cost grows with the table. Delta tables never need it.
- **Detect:** `spark.sql(...)` on a session (literal, f-string, `sqlQuery=`, a name bound once) whose statement is `MSCK REPAIR TABLE t`, `REPAIR TABLE t [ADD|DROP|SYNC PARTITIONS]` or `ALTER TABLE t RECOVER PARTITIONS` (keywords outside strings and comments, via `SqlText::repairs_partitions`), and `spark.catalog.recoverPartitions(t)` on a typed catalog.
- **Not reported:** `REPAIR TABLE t SYNC METADATA` (Unity Catalog metadata sync); the statement text inside a string literal or a comment of another query; queries built by calls or names bound more than once. A retry or first-match loop that leaves right after the repair (unconditional `break` / `return`) stays info.
- **Fix:** none.
- **FP:** Low-medium: a one-off backfill or migration script legitimately repairs once, hence info.
- **Ex:** `spark.sql("MSCK REPAIR TABLE lake.events")` → `spark.sql("ALTER TABLE lake.events ADD IF NOT EXISTS PARTITION (dt='2024-01-01')")`, or convert the table to Delta.

#### IOD019 `small-max-records-per-file`
perf · info · on · T1 · none · new
- **Why:** A row cap splits each task's output into ceil(rows / cap) files. Small caps (tens of thousands of rows) produce many small files: every later reader pays listing and per-file open cost, Parquet compression and min/max statistics get worse, and Delta compaction has more to rewrite. Databricks recommends against `maxRecordsPerFile` unless necessary.
- **Detect:** a writer terminal (`DataFrameWriter`, `writeTo(..)` V2, `DataStreamWriter.start/toTable`) whose settings (via `IoSettings::of`: `option`, `options(k=v)`, `options(**dict)`, terminal keywords, a stored writer; case-insensitive key) set `maxRecordsPerFile` to a constant 1..99,999 (int or string literal, or a name bound once to one); and the session key `spark.sql.files.maxRecordsPerFile` written with such a value through the config-write matcher (`conf.set`, builder `config`, `SparkConf`, SQL `SET`; a later write of the key adds a note that it is set back). Underscores in string values are ignored (`"1_000"`).
- **Not reported:** 0 / negative (unlimited), >= 100,000 and unknown values (parameters, names bound more than once, computed expressions); readers.
- **Fix:** none.
- **FP:** Low-medium: very wide rows or a downstream reader with a hard row limit can justify a cap, hence info and the 100,000 threshold.
- **Ex:** `df.write.option("maxRecordsPerFile", 10000).parquet(p)` → `df.write.parquet(p)` (Delta: optimized writes / `delta.targetFileSize`).

#### IOD020 `zorder-many-columns`
perf · info · on · T1 · none · new
- **Wording:** `{what}` is `SQL ZORDER BY`, or `executeZOrderBy` for the Delta API form.
- **Why:** Z-order maps all keys onto one space-filling curve, so locality per key falls as keys are added: with four or more keys each file spans a wide range of every key and min/max file skipping prunes little, while each OPTIMIZE still rewrites the data it touches. Databricks recommends at most three Z-order columns (DBSQL rejects more than nine).
- **Detect:** `spark.sql(...)` on a session (literal, f-string, `sqlQuery=`, a name bound once) whose `ZORDER BY (..)` list (parentheses optional; top-level items counted, quotes / nested calls respected, via `SqlText::zorder_columns`) has more than `zorder-max-columns` (default 3; 0 disables) columns; and `<DeltaTable>.optimize()[.where(..)].executeZOrderBy(..)` counting string literals, names typed `str` and literal lists/tuples. An expression such as `coalesce(a, b)` counts as one column.
- **Not reported:** `*cols`, list variables, keyword arguments and an `optimize()` builder stored in a name (the count is unknown); `CLUSTER BY` (liquid clustering).
- **Fix:** none.
- **FP:** Low-medium: four filter columns can still pay off on very large tables, hence info and the configurable threshold.
- **Ex:** `OPTIMIZE t ZORDER BY (user_id, event_type, country, device)` → `OPTIMIZE t ZORDER BY (user_id, event_type)`, or `ALTER TABLE t CLUSTER BY (...)` (liquid clustering).

### CFG — Spark config

#### CFG001 `aqe-disabled`
perf · warning · on · T0 · none · new
- **Why:** Loses partition coalescing, skew-join splitting and dynamic broadcast conversion. Usually means more tasks, worse skew, and bigger clusters. Also flags `…adaptive.coalescePartitions.enabled=false` and `…skewJoin.enabled=false`.
- **Detect:** `spark.conf.set(k, v)` / `SparkSession.builder.config(k, v)` (also `config(map={...})`) / `SparkConf().set(k, v)` (`setIfMissing`, `setAll`) / `spark.sql("SET k=v")` with those keys (any casing) and a false value (`False`, `"false"` in any casing, or a name whose every binding is one). A `set(k, v)` / `config(k, v)` call inside `for k, v in SETTINGS.items():` or `for k, v in [(key, value), ...]:` over a constant dict or list of pairs (written there or bound once to a name) counts as one write per item; items from a function parameter, `os.environ` or a `**` spread are not read. `spark.sql("RESET k")` is not read (it does not count as setting the key back). Shared with all config-value rules (CFG002, CFG007-CFG022): when a later write in the same file sets the key to a value the rule does not report (or one it cannot read), or calls `spark.conf.unset(k)`, the earlier write is a scoped workaround: it stays reported, as info, with "(Set back later in this file, …)" added to the message.
- **Ex:** remove the line.

#### CFG002 `broadcast-disabled`
perf · warning · on · T0 · none · new
- **Why:** Every small-dimension join becomes a sort-merge join with a full shuffle of the large side. Usually it was added to work around one bad join.
- **Detect:** Key `spark.sql.autoBroadcastJoinThreshold` (or `spark.sql.adaptive.autoBroadcastJoinThreshold` / `spark.databricks.adaptive.autoBroadcastJoinThreshold`) set to a size ≤ 0 (`-1`, `0`, `"0"`, `"-1b"`; strings read like Spark's byte sizes), through the same config-write matcher and "set back later" info downgrade as CFG001.
- **Ex:** Remove it; for the one problem join use `hint("merge")` or `hint("shuffle_hash")`.

#### CFG003 `static-conf-at-runtime`
correctness · error · on · T0 · none · new (Databricks Spark config limitations)
- **Why:** The user believes they tuned the job. In fact nothing changed (Classic), or it fails (Connect).
- **Detect:** `spark.conf.set` / `spark.conf.unset` / SQL `SET` (any casing of the key; also a `set(k, v)` in a loop over a constant dict or list of pairs, as under CFG001) with a key in the static/cluster-level table: `spark.executor.*`, `spark.driver.*`, `spark.dynamicAllocation.*`, `spark.serializer`, `spark.jars*`, `spark.submit.*`, `spark.master`, `spark.task.cpus`, `spark.cores.max`, `spark.memory.*`, `spark.sql.extensions`, `spark.sql.catalogImplementation`, `spark.sql.warehouse.dir`, `spark.default.parallelism`, `spark.local.dir`, `spark.yarn.*`, `spark.kubernetes.*` and `spark.databricks.cluster.*` → error. `SparkSession.builder.config(...)` / `SparkConf().set(...)` with such a key in a notebook whose session already runs → warning (ignored); in a plain `.py` module they are never reported (there the builder normally creates the session). `notebook = "on"` makes a module count as a running notebook. The session already runs in a Databricks notebook (source format, or `.ipynb` with Databricks metadata); in any other notebook only with evidence in the file: `spark` / `sc` / `sqlContext` / `dbutils` / `displayHTML` used without being defined, an earlier `getOrCreate()`, or `display` used before the builder call in a notebook that also names Databricks storage (`dbfs:/`, `/dbfs/`, `/Volumes/`, `/Workspace/` paths); IPython's own `display` alone is not evidence.
- **FP:** A plain Jupyter notebook whose first Spark statement is the builder: there `SparkSession.builder.config(...).getOrCreate()` creates the session and the static settings apply. Not reported.
- **Ex:** move it to the cluster or job compute config.

#### CFG007 `broadcast-threshold-too-high`
perf · warning · on · T0 · none · new
- **Why:** Each automatic broadcast is collected to the driver, hashed there and shipped to every executor. A threshold in the GB range turns ordinary joins into driver OOMs and broadcast timeouts, and missing or stale statistics let large tables under the estimate. Above Spark's hard 8 GB limit a qualifying join fails outright ("Cannot broadcast the table that is larger than 8GB").
- **Detect:** The CFG002 keys set to a size ≥ `broadcast-threshold-max` (`[tool.sparklint]` option, default `"1g"`, `"0"` = off). Values are parsed like Spark's byte strings (`"512m"`, `"2g"`, `"16gb"`, plain bytes) and folded from int arithmetic (`2 * 1024 ** 3`, `str(...)`, `1 << 31`). Error at ≥ 8 GB.
- **FP:** Low: the value is a literal. Clusters with very large drivers can raise `broadcast-threshold-max`; a write that is set back later in the file is info.
- **Ex:** `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "2g")` → keep the default; `big.join(F.broadcast(small_filtered), "k")`.

#### CFG008 `predicate-pushdown-disabled`
perf · warning · on · T1 · none · new
- **Why:** With pushdown off, Parquet/ORC readers cannot skip row groups by their statistics, and a JDBC source ships the whole table to Spark to be filtered there: a full scan (for JDBC a full table transfer) for every filtered query. Usually a leftover workaround for one old reader bug.
- **Detect:** `spark.sql.parquet.filterPushdown` / `spark.sql.orc.filterPushdown` set false through the config-write matcher described under CFG001 (`False` or `"false"` in any casing; a write set back later in the file is info). JDBC: a reader chain with `format("jdbc")` (or `load(format="jdbc", ...)`) whose `option` / `options` / `load` keywords set `pushDownPredicate` (key in any casing) to `False` or `"false"` in any casing, or `spark.read.jdbc(..., properties={...})` with that key (a literal dict or a name bound to one). Options set by a separate `reader.option(...)` statement are not followed, so that form is not reported.
- **FP:** Low: exact keys and literal values. A source with a buggy pushdown deserves a noqa with the reason.
- **Ex:** `spark.read.format("jdbc").option("pushDownPredicate", False)...load()` → drop the option, or move the filter into the `query` subquery.

#### CFG009 `delta-file-compaction-disabled`
perf · info · on · T0 · none · new
- **Wording:** `{what}` is `optimized writes` or `auto compaction`; `{scope}` is `every Delta table this session creates` for the `properties.defaults` keys, else `every Delta table this session writes`; `table-property` is one finding per statement, naming every property set false.
- **Why:** The session switches override every table the job writes, so appends, MERGEs and streaming micro-batches produce many small files: slower reads, a bigger Delta log and OPTIMIZE debt. Databricks recommends optimized writes plus auto compaction.
- **Detect:** `spark.databricks.delta.optimizeWrite.enabled`, `spark.databricks.delta.autoCompact.enabled` and their `spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite` / `.autoCompact` defaults set false through the config-write matcher described under CFG001. SQL: `spark.sql` text whose `TBLPROPERTIES (...)` list sets `delta.autoOptimize.optimizeWrite` / `delta.autoOptimize.autoCompact` (quoted or not) to `false` or `'false'` (ALTER and CREATE).
- **FP:** Low: exact keys and literal values. Latency-bound streams sometimes turn it off on purpose (hence info); a noqa with the reason documents that.
- **Ex:** `spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")` → remove it; if one table needs it off, set the table property there and schedule OPTIMIZE.

#### CFG010 `disk-cache-disabled`
perf · info · on · T0 · none · new
- **Why:** The disk cache keeps local SSD copies of remote Parquet/Delta files, so repeated reads (joins against the same dimension, iterative jobs, several queries over one table) skip cloud storage. Databricks recommends it for most workloads and enables it on SSD worker types. Switching it off in code is usually a leftover from a cold-read benchmark.
- **Detect:** `spark.databricks.io.cache.enabled` set false through the config-write matcher described under CFG001 (conf.set, builder config / `map=`, `SparkConf().set`, SQL `SET`; `False` or `"false"` in any casing). A write set back later in the file gets the "(Set back later in this file, ...)" note.
- **FP:** Low: exact key and literal value. Benchmarks that want cold reads noqa it with the reason.
- **Ex:** `spark.conf.set("spark.databricks.io.cache.enabled", "false")` → remove it.

#### CFG011 `arrow-batch-size-extreme`
perf · warning · on · T0 · none · new
- **Why:** Arrow batches are allocated outside the JVM heap, so the batch size multiplies straight into memory overhead on every pandas UDF, `applyInPandas`, `mapInPandas` and Arrow `toPandas` in the job. Too large shows up as exit 137 / "container killed" with no JVM error; 0 or negative removes the limit (a whole partition per batch). Too small spends the time crossing the JVM/Python boundary.
- **Detect:** `spark.sql.execution.arrow.maxRecordsPerBatch` written through the config-write matcher described under CFG001 with an integer value (a literal, a string such as `"100"`, or constant arithmetic such as `10 ** 6`): >= 100000 or <= 0 = warning, 1-499 = info (bands are fixed; no option). Other values (`"1e6"`, non-constant values) are skipped. A write set back later in the file to a value in the 500-99999 range is info with a note.
- **FP:** Low-med: narrow rows tolerate bigger batches, so a deliberate value can be noqa'd with the reason.
- **Ex:** `spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 1000000)` → keep the default (or lower it for wide rows) and size `spark.executor.memoryOverhead`.

#### CFG012 `file-partition-size-extreme`
perf · warning · on · T0 · none · new
- **Wording:** `{what}` is `scan` (default 128 MB) or `post-shuffle` (default 64 MB, the advisory size); `too-small` and `open-cost-free` are info.
- **Why:** `spark.sql.files.maxPartitionBytes` sizes scan tasks and `spark.sql.adaptive.advisoryPartitionSizeInBytes` sizes post-shuffle partitions after AQE coalescing. Far above the default, each task holds gigabytes (spill, OOM, idle cores); far below, the job drowns in scheduling overhead and small output files. `spark.sql.files.openCostInBytes` <= 0 makes small files look free, so one task gets packed with thousands of them.
- **Detect:** the config-write matcher with Spark's byte-string parser (`"16m"`, `"1g"`, `134217728`, `"128MB"`): maxPartitionBytes > 1 GB or advisory size (also the Spark 3.0 name `spark.sql.adaptive.shuffle.targetPostShuffleInputSize`) > 2 GB = warning; either < 8 MB = info; openCostInBytes <= 0 = info. A size of 0 or below for the two partition-size keys, non-constant and unparseable values are skipped; a hit followed later in the file by a write of the same key that is not reported (set back) becomes info with a note. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low-med: a deliberately small scan size for a CPU-heavy UDF stage is a known trick (hence info, noqa with the reason).
- **Ex:** `spark.conf.set("spark.sql.files.maxPartitionBytes", "4g")` → keep the default or a few hundred MB.

#### CFG013 `aqe-min-partition-num-set`
perf · info · on · T0 · none · new
- **Why:** Databricks' AQE page says "Don't set this property": it overrides `coalescePartitions.minPartitionSize`, so a value copied from an old cluster keeps every small shuffle at hundreds or thousands of tiny tasks and small output files.
- **Detect:** `spark.sql.adaptive.coalescePartitions.minPartitionNum` (or the Spark 3.0 name `spark.sql.adaptive.minNumPostShufflePartitions`) set to any constant value through the config-write matcher; `unset` is quiet, and a set followed later in the file by `unset` gets a "set back later" note. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low: exact key, rarely right.
- **Ex:** `spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", 400)` → remove it.

#### CFG014 `engine-acceleration-disabled`
perf · warning · on · T0 · none · new
- **Why:** Both switches are workarounds for one old error (the 64 KB generated-method limit, a reader type bug) that stay on for every later query of the session and multiply CPU time of scans and operators.
- **Detect:** `spark.sql.codegen.wholeStage`, `spark.sql.parquet.enableVectorizedReader` or `spark.sql.orc.enableVectorizedReader` set false (`"false"` in any casing or Python `False`) through the config-write matcher (conf.set, builder config / `map=`, SparkConf, SQL `SET`, constant names); a write set back later in the file is info. Other codegen keys (`codegen.fallback`, `maxFields`) and the in-memory columnar reader key are not reported. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low: exact keys and literal values; a genuine workaround deserves a set-back after the query or a noqa with the reason.
- **Ex:** `spark.conf.set("spark.sql.codegen.wholeStage", "false")` → remove it.

#### CFG015 `dynamic-pruning-disabled`
perf · warning · on · T0 · none · new
- **Why:** Dynamic file/partition pruning pushes the small side's join keys into the scan of the big table at run time; without it a star-schema join or a MERGE scans the whole fact table. Databricks advises against disabling it except to compare plans.
- **Detect:** `spark.databricks.optimizer.dynamicFilePruning` or `spark.sql.optimizer.dynamicPartitionPruning.enabled` set false through the config-write matcher; a write set back (or unset) later in the file is info. The DFP size/file thresholds are not reported. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low: exact keys and literal values.
- **Ex:** `spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")` → remove it.

#### CFG016 `broadcast-timeout-raised`
perf · info · on · T0 · none · new
- **Why:** Raising the timeout is the common answer to `Futures timed out after [300 seconds]`, but the timeout is a symptom: an oversized broadcast side (stale statistics, a forced hint) or a slow build query. A longer wait (or a negative value, which never times out) lets that broadcast hold the cluster longer instead of failing fast.
- **Detect:** `spark.sql.broadcastTimeout` written through the config-write matcher (conf.set, builder config / `map=`, SparkConf, SQL `SET`, constant names) with a constant duration above 300 seconds (plain seconds or Spark time strings `600s`, `10min`, `1h`) or a negative value; a write set back later in the file is a note. Non-constant and unparseable values are skipped. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low-med: an intentionally small but slow-to-build relation on a non-AQE path is a legitimate reason (noqa with the reason).
- **Ex:** `spark.conf.set("spark.sql.broadcastTimeout", "3600")` → shrink/filter the broadcast side, drop the forced hint or `ANALYZE TABLE`; keep the default.

#### CFG017 `driver-result-limit-removed`
perf · warning · on · T0 · none · new
- **Why:** The limit exists to protect the driver from out-of-memory on large collects and broadcasts. Removing it (0 or negative = unlimited) turns a clean "job aborted" error into a driver crash: the session and its cached state are lost, the job restarts, and all workers wait meanwhile. A limit of 16 GB or more is effectively the same on typical drivers.
- **Detect:** `spark.driver.maxResultSize` written through a builder (`config`, `map=`, a dict bound to a name) or SparkConf (`set`, `setIfMissing`, `setAll`), constant keys/values through names, with a constant size <= 0 (warning) or >= 16 GB (info); sizes through Spark's byte-string parser, unparseable or non-constant values skipped. Runtime `spark.conf.set` / SQL `SET` of the key are not reported (a launch-time key, no effect there). A later write of the key makes a hit a note. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low: exact key and value; a driver deliberately sized for huge results deserves a noqa with the reason.
- **Ex:** `SparkSession.builder.config("spark.driver.maxResultSize", "0")` → keep a limit and aggregate / limit / write the result instead of collecting it.

#### CFG018 `io-compression-disabled`
perf · warning · on · T0 · none · new
- **Why:** All four switches default to true ("Generally a good idea" in the Spark docs). Off, they multiply shuffle network and disk bytes, spill volume, broadcast transfer size or cached-table memory (more evictions and recomputes).
- **Detect:** `spark.shuffle.compress`, `spark.shuffle.spill.compress`, `spark.broadcast.compress` set false through a builder (`config`, `map=`) or SparkConf, and `spark.sql.inMemoryColumnarStorage.compressed` set false through any config write (also runtime conf.set / SQL `SET`); a write set back later in the file is a note. The three core keys written on a running session are not reported (Spark rejects or ignores them there). `spark.rdd.compress` (default false) and file-format codecs are not reported. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low: exact keys and literal values.
- **Ex:** `SparkSession.builder.config("spark.shuffle.compress", "false")` → remove it; to trade CPU for size pick `spark.io.compression.codec`.

#### CFG019 `deletion-vectors-disabled`
perf · warning · on · T0 · none · new
- **Why:** With deletion vectors a change marks the deleted/updated rows in a small bitmap file; without them Delta copies every untouched row of each affected Parquet file. MERGE/UPDATE-heavy jobs then rewrite far more bytes than they change.
- **Detect:** `spark.sql` text whose `TBLPROPERTIES (..)` (CREATE / ALTER .. SET) sets `delta.enableDeletionVectors` false (via `sparklint_semantic::sql` properties), and the session default `spark.databricks.delta.properties.defaults.enableDeletionVectors` set false through any config write (conf.set, builder, SparkConf, SQL `SET`); a session write set back later in the file is a note. Not yet: writer V2 `tableProperty(..)` and `DeltaTable` builder `.property(..)`. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low: exact keys and literal values; the help names the legitimate case (old readers / external engines without deletion-vector support).
- **Ex:** `ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')` → remove it, or noqa it with the reader that needs it.

#### CFG020 `file-statistics-disabled`
perf · warning (info for -1) · on · T0 · none · new
- **Why:** Data skipping, ZORDER and liquid clustering all rely on per-file min/max statistics; 0 indexed columns (or an empty stats-column list) turns every query into a full scan. -1 is the opposite extreme: write cost on wide tables.
- **Wording:** the message names the setting and its value as written: `Table property delta.dataSkippingNumIndexedCols = 0`, `Table property delta.dataSkippingStatsColumns = ''`, or the session default key with `= 0` / `= -1`.
- **Detect:** `spark.sql` `TBLPROPERTIES (..)` setting `delta.dataSkippingNumIndexedCols` to 0 / -1 or `delta.dataSkippingStatsColumns` to an empty string, and the session default `spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols` = 0 / -1 through any config write (a later write of the key makes it a note). Other values are not reported. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low for 0, medium for -1 (narrow tables are fine; hence info).
- **Ex:** `TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')` → remove it, or set `delta.dataSkippingStatsColumns = 'ts,customer_id'`.

#### CFG021 `optimizer-rules-excluded`
perf · info · on · T0 · none · new
- **Wording:** `{scope}` is `every adaptive re-optimisation` for the adaptive key; more than three names show as `A, B, C and N more`.
- **Why:** The excluded-rules lists are diagnostic knobs. Left in after a one-off workaround they remove Catalyst (or AQE re-optimisation) rules for every query of the session; and because unknown names are ignored, a list carried across Spark versions may do nothing at all.
- **Detect:** shared config-write matcher (`conf.set`, builder `config` / `map=`, `SparkConf` set/setAll/setIfMissing, SQL `SET`, constant names) on `spark.sql.optimizer.excludedRules` or `spark.sql.adaptive.optimizer.excludedRules` with a constant, non-empty rule list (up to three short rule names shown). An empty value, reads, unsets and non-constant values are not reported; a later write of the key in the file adds a scoped note. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low-med: genuine workarounds for optimizer bugs exist; the help asks to scope them and say why (noqa).
- **Ex:** `spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates")` → remove it, or set it back after the affected query.

#### CFG022 `delta-target-file-size-small`
perf · warning · on · T0 · none · new
- **Why:** A fixed `delta.targetFileSize` turns off Databricks' size-based file autotuning for the table. Below ~16 MB every maintenance and write path produces small files: more files to list and open per query, more Delta log entries, worse data skipping per file.
- **Detect:** `spark.sql` `TBLPROPERTIES (..)` (CREATE / ALTER) setting `delta.targetFileSize`, and the session default `spark.databricks.delta.properties.defaults.targetFileSize` through any config write (`conf.set`, SQL `SET`, builder, `SparkConf`, constant names), to a constant size (bytes or `k`/`m`/`g` units, Spark's byte-string parser) between 1 byte and 16 MB. Larger, zero, negative and unparsable values are not reported; a later write of the key in the file makes it a scoped note. Session settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`) count like direct calls (shared matcher, see CFG001).
- **FP:** Low-med: deliberately small files for very MERGE-heavy tables; the help names `delta.tuneFileSizesForRewrites` as the supported route.
- **Ex:** `ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize' = '8mb')` → remove it, or `'delta.tuneFileSizesForRewrites' = 'true'`.

### STM — Structured Streaming

#### STM001 `stream-missing-checkpoint`
correctness · error · on · T1 · none · new
- **Why:**
  - Without a checkpoint the query uses a temp checkpoint, or fails on some sinks.
  - A restart reprocesses from scratch or skips data, with duplicates.
  - On Databricks the temp location may be deleted.
- **Detect:** A `DataStreamWriter` chain reaching `start()`/`toTable()` (positional path / format / output mode included, `foreachBatch` sinks too) with no `checkpointLocation` option (any key case) in `option` / `options` / `start` / `toTable` keywords, and the session-level `spark.sql.streaming.checkpointLocation` not set in the same file (read through the shared config writes: `conf.set`, builder `config`, `SparkConf`, SQL `SET`). A checkpoint whose value is a parameter or variable counts as set.
- **FP:** Debug sinks `format("console" | "memory" | "noop")` (also `start(format="console")`) are not reported; `console` / `memory` are ACT010's case. Not reported when options come from an opaque dict or `**kwargs`, nor when the session default is set in another file. Not reported when the writer chain does not start in view at `df.writeStream`: a writer passed in as a function parameter or returned by a helper may carry a checkpoint set elsewhere. A writer bound on several paths (`if`/`else`) is reported only when every path starts at `df.writeStream`, no path sets the checkpoint and the paths differ in options/format only.
- **Fix:** none (the location is project-specific).
- **Ex:** `.option("checkpointLocation", f"{CKPT}/orders")`

#### STM002 `trigger-once-deprecated`
perf · warning · on · T1 · safe · new (Spark 3.4 deprecation; serverless)
- **Why:**
  - `once` processes everything in one micro-batch (memory blow-ups on backlogs) and ignores rate limits.
  - `availableNow` splits the backlog into batches that respect `maxFilesPerTrigger`/`maxBytesPerTrigger`.
  - Serverless documents Once as deprecated.
- **Detect:** `DataStreamWriter.trigger(once=True)` with the literal `True` (`once=False` and non-literal flags are not reported). Scala's `Trigger.Once()` and SQL are not read.
- **Fix (safe):** Replace the keyword with `availableNow=True`. Offered only when the configured `spark-version` is 3.3 or later (`availableNow` does not exist before) and the call sets no other trigger (`processingTime`, `continuous`, `availableNow` or a positional interval): such a call already raises "Multiple triggers not allowed", which the swap would not fix.
- **Ex:** `.trigger(once=True)` → `.trigger(availableNow=True)`

#### STM010 `kafka-stream-uncapped-batch`
perf · warning · on · T1 · none · new
- **Why:** Without a cap, Spark plans each micro-batch up to the latest offset. A new query starting at `earliest` (or a restart after an outage) reads the whole retained backlog in ONE batch: hours of work in one batch, executor OOMs, a huge state-store commit, and a cluster sized for that spike. `trigger(availableNow=True)` honours the cap and drains the backlog in several batches.
- **Detect:** `DataStreamReader.load()` (oracle Certain) whose reader chain has format `kafka` (`format("kafka")` or `load(format="kafka")`, case-insensitive) and no `maxOffsetsPerTrigger` / `maxBytesPerTrigger` (any key case) in `option`/`options`/`load` keywords (shared reader option walker). Warning when `startingOffsets` is `earliest` or a JSON offsets map, or when `startingTimestamp` / `startingOffsetsByTimestamp` is set (they win over `startingOffsets`); info otherwise. `minOffsetsPerTrigger` alone does not count as a cap (it only delays small batches). Skipped when options come from an opaque dict or `**kwargs`, the reader is configured by separate statements, or the chain does not start in view at `spark.readStream` (a reader passed in as a parameter or returned by a helper).
- **FP:** Low-med: low-volume topics never build a large backlog (the message says so). Batch `spark.read` Kafka reads are not flagged.
- **Fix:** none (the cap depends on the topic's volume).
- **Ex:** `.option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 500_000)`

#### STM011 `heap-state-store-explicit`
perf · info · on · T0 · none · new
- **Why:** The heap-backed provider keeps every state key in executor memory; with millions of keys GC pauses make micro-batches slow and erratic and executors OOM. RocksDB keeps state off-heap (the Databricks default from DBR 17.3). The provider is fixed per checkpoint, so the choice is hard to undo.
- **Detect:** shared config-write matcher (`conf.set`, builder `config` / `map=`, `SparkConf` set/setAll/setIfMissing, SQL `SET`, constant names) on `spark.sql.streaming.stateStore.providerClass` whose value's last dotted segment is `HDFSBackedStateStoreProvider` (any case). Absence of a provider setting is not reported (the runtime default may already be RocksDB). A later write of the key in the file adds a scoped note.
- **FP:** Low: queries with small, bounded state and tests can keep the heap store (noqa). Not reported: other provider classes (including wrappers whose name only contains `HDFSBackedStateStoreProvider`), values held in a variable that is not a constant, cluster configuration outside the code.
- **Fix:** none.
- **Ex:** `spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")`

### COR — Correctness

#### COR001 `column-eq-none`
correctness · error · on · T1 · safe · new, mrpowers nulls
- **Why:** SQL three-valued logic. `col == None` becomes `col = NULL`, which is never true. A silent empty result.
- **Wording:** six variants: `Column == None` / `Column != None` (always NULL), `Column is None` / `Column is not None` (always False / True in Python), and for SQL text `` `{op} NULL` in SQL text is always NULL `` with `{op}` the operator as written (`=`, `==`, `!=`, `<>`), ending in `use IS NULL.` or `use IS NOT NULL.`.
- **Detect:** A single comparison (`==`, `!=`, either operand order) between an operand that is certainly a Column (a name that only looks like a column is not enough) and the constant `None` (also `F.lit(None)` and `F.lit(None).cast(..)`). `is None`/`is not None` on an expression that is always a Column (a pyspark call such as `F.col("a")`, a method on a Spark object, `df.a`, `df["a"]`) is always False/True in Python, so it is flagged as well; on a variable or a call to your own helper it is the normal check for an optional Column and is not flagged. Also `F.col("x") == F.lit(None)`.
- **Detect (SQL text):** `= NULL`, `== NULL`, `!= NULL`, `<> NULL` (either operand order, any case) in the text of `spark.sql(..)`, `df.filter("..")` / `where("..")`, `F.expr("..")` and each `selectExpr(..)` item (a literal, an f-string, or a name bound to one): in `WHERE` / `ON` / `HAVING`, `CASE WHEN`, a MERGE `WHEN MATCHED AND ..` condition or a select item. One finding per variant and call, on the text argument.
- **Not reported:** `IS NULL` / `IS NOT NULL`, the null-safe `<=>` and `eqNullSafe(..)`; `>= NULL` / `<= NULL` and the like; assignments: `UPDATE .. SET a = NULL`, MERGE `UPDATE SET t.x = NULL`, a `SET key = NULL` statement, `TBLPROPERTIES (..)` / `OPTIONS (..)` pairs, named arguments (`=>`); text inside string literals and comments of the SQL; `NULLIF(..)`.
- **Fix (safe):** `==None` → `.isNull()`, `!=None` → `.isNotNull()` (the Column is parenthesised when needed). The `is` / `is not` forms get the same rewrite as an unsafe fix: the result turns from a Python bool into a Column, which is only right inside a DataFrame expression (in an `if` test it raises). SQL-text findings have no fix.
- **Ex:** `df.filter(F.col("a") == None)` → `df.filter(F.col("a").isNull())`

#### COR002 `python-bool-op-on-column`
correctness · error · on · T1 · safe · new
- **Wording:** `'and' on Columns — use & (with parentheses); …` (`or` / `|` alike), `'not' on a Column — use ~ (with parentheses); …`, and for a test whose `and` / `or` chain ends in a Column: `This test gets a Column from 'and' (its last operand) — Python cannot convert a Column to bool.` Help: combine Column conditions with `&` / `|` / `~`, each comparison in parentheses; for the test form, test Python values there and pass Column conditions to `filter()` / `when()`.
- **Why:** Python converts every operand of `and` / `or` except the last to bool, and the operand of `not`; on a Column that raises `ValueError: Cannot convert column into bool`. `F.col("a") > 1 and F.col("b") < 2` is the most common form.
- **Detect:**
  - `and` / `or` with a Column in any position but the last (one finding per chain; a nested chain that can give a Column counts as one), and `not` on a Column.
  - A chain whose only Column is its last operand is reported only when the chain itself is a boolean test: an `if` / `elif` / `while` / `assert` / ternary test, a comprehension `if`, or the operand of `not` (`if c is not None and c == 1:`). This form has no fix.
  - Operands must be Columns with certain type information (values only guessed to be Columns, pandas Series and other look-alikes are not reported).
- **Not reported:** a chain whose only Column is its last operand outside a test (`msg or default_col`, `opt and F.col("x") == opt`): Python never converts that Column, the value is the Column or the earlier falsy value (the usual optional-condition idiom); `not (a and b)` is reported once, as the chain's finding.
- **Fix:** replace each keyword with `&` / `|` (or `not` with `~`) and parenthesise every operand that is not a plain name, attribute, call or subscript and not already in parentheses; comments and line breaks are kept. Safe when every operand is a Column; unsafe when a Python value sits next to them (under `&` / `|` it becomes a literal, and a string is not read as SQL).
- **Ex:** `F.col("a")>1 and F.col("b")<2` → `(F.col("a")>1) & (F.col("b")<2)`

#### COR003 `bitwise-comparison-precedence`
correctness · error · on · T1 · safe · new
- **Why:** Python parses `a > 1 & b < 2` as the chained comparison `a > (1 & b) and (1 & b) < 2`. On Columns that either raises "Cannot convert column into bool" or computes a bitwise AND of a literal and a Column. Both are silent nonsense or runtime errors.
- **Detect:** A comparison with an operand that is an unparenthesised `x & y` / `x | y`, reported when a side of that `&` / `|` is a Column (`F.col("a") > 1 & F.col("b")` computes `1 & F.col("b")`), or when the comparison is a chain (three or more operands) over a Column (`F.col("a") > 1 & 2 < F.col("b")`, which calls `bool()` on a Column). Columns typed from usage count too (`frame.a` on a parameter used as a DataFrame).
- **Not reported:** a bitwise expression in its own parentheses (`a > (1 & b)`, deliberate); a single comparison of a Column against `x | y` of plain values (`F.col("mode") == READ | WRITE`, the integer-flag idiom, which compares with the combined flags as written); plain integer / bool arithmetic (`flags & 1 == 1`); pandas and pandas-on-Spark Series (same precedence trap, but not a Spark Column).
- **Fix (safe):** Parenthesise each comparison: `(F.col("a") > 1) & (F.col("b") < 2)`. Only parentheses are inserted, so comments, line breaks and operands already in parentheses stay as written; no fix when a side of the `&` / `|` is itself an unparenthesised `&` / `|`.

#### COR007 `discarded-transformation`
correctness · error · on · T1 · none · new
- **Wording:** `{call}` is the call as written; long arguments are elided (`df.dropDuplicates(…)`). A chain that never runs gets its own message: `{call} sets up {what} that never runs — nothing happens until {finish} is called.`, with `{what}` = a write / a streaming query / a merge / an OPTIMIZE / a table creation and `{finish}` the calls that would run it (`save() / saveAsTable() / insertInto()`, `start() / toTable()`, `merge()`, `execute()`, `executeCompaction() / executeZOrderBy()`).
- **Why:** `df.withColumn(...)`, `df.dropDuplicates()` or `df.na.fill(0)` as a bare statement does nothing. A writer chain with no `save()` / `start()` is worse: the write the code was written for never happens, with no error. The code "looks" right and silently drops the logic.
- **Detect:** An expression statement whose value is a transformation called on a DataFrame, GroupedData, `df.na` or `df.stat` receiver and returning a `DataFrame` or `GroupedData` (`df.withColumn(..)`, `df.groupBy("a")`, `df.groupBy("a").count()`, `df.na.fill(0)`, `df.stat.sampleBy(..)`, `df.hint("broadcast")`, `df.observe(..)`: its metrics are only filled in by an action on the frame it returns). Actions and calls with an effect are skipped: `cache`, `persist`, `unpersist`, `checkpoint`, `localCheckpoint` (unless `eager=False` / `False`, which only marks the returned frame), the temp-view registrations, `printSchema`, `explain`, `show`, `display`, `foreach(Partition)`, `writeTo`, and any call with an `inplace=` keyword. Also an expression statement that starts a writer or builder and stops before the call that runs it: `df.write…` (no `save` / `saveAsTable` / `insertInto` / format shortcut), `df.writeStream…` (no `start` / `toTable`), `df.writeTo(t)…` (no `append` / `overwrite` / `create` / ...), Spark 4 `df.mergeInto(t, cond)…` (no `merge()`), Delta Lake `dt.merge(src, cond)…` (no `execute()`), `dt.optimize()[.where(..)]` (no `executeCompaction` / `executeZOrderBy`) and `DeltaTable.create(spark)…` (no `execute()`). The chain must start the writer in the same statement: Spark's writers (and Delta's optimize / create builders) change in place, so `w.format("delta")` on a writer held in a name is a real configuration step and is not reported. A receiver known only by its name (`orders_df`) needs a DataFrame-only method (pandas has `drop` / `fillna` / `rename` too). Error in `.py` files, warning in notebooks.
- **Not reported:** a statement directly inside a `try:` body that has an `except` handler, or directly inside `with pytest.raises(..)` / `pytest.warns(..)` / `self.assertRaises(..)` (and the `Regex` / `Warns` variants): Classic Spark analyses a transformation eagerly, so `df.select("c")` there is a deliberate probe for the error (missing column, bad argument). Statements in `except` / `else` / `finally`, in other `with` blocks (`contextlib.suppress`), or nested deeper (an `if` inside the `with`) are still reported.
- **Ex:** `df.dropDuplicates(["id"])` → `df = df.dropDuplicates(["id"])`

#### COR010 `union-by-position`
correctness · warning · on · T1 · unsafe · CY035, ruff#7272
- **Wording:** `{method}` is `union` or `unionAll`.
- **Why:** When the schemas are in a different order, values silently land in the wrong columns (and type-coerce).
- **Detect:** `DataFrame.union/unionAll(other)` where `other` is a DataFrame or of unknown type, and `functools.reduce(DataFrame.union, ...)` / `reduce(DataFrame.unionAll, ...)`.
- **Not reported** (the column order is the same by construction; each side is followed through row-only steps such as `filter` / `where` / `limit` / `distinct` / `dropDuplicates` / `orderBy` / `repartition` / `cache` / `dropna` / `fillna` / set operations, a semi or anti join (it keeps the left side's columns), the receiver of an earlier `union` (or of a `unionByName` without `allowMissingColumns=True`) in the chain, and names bound with a plain `name = value` or `name: T = value`; a name bound on several paths (if / else, a loop) counts when every path agrees):
  - the same frame on both sides (`df.union(df)`, `self.df.union(self.df)`, `df.filter(c1).union(df.filter(c2))`);
  - both sides selecting the same column list: the same list name (`a.select(cols).union(b.select(cols))`, also `*cols`, as long as `cols` is not rebound in between) or the same string literals (`select("x", "y")`, `select(["x", "y"])`);
  - one side re-selected with the other's columns (`a.union(b.select(a.columns))`, `select(*a.columns)`, also through a name and further down an `a.union(..).union(..)` chain).
  Tuple unpacking (`u, v = a.select(cols), b.select(cols)`) is not followed, and paths that disagree (one branch adds a column) are reported.
- **Fix (unsafe):** → `unionByName(other)` (it raises on mismatched names, unlike `union`, so it is a behaviour change). The `reduce(..)` form has no fix.

#### COR016 `regex-escape`
correctness · warning · on · T0 · safe · CY016 (widened)
- **Why:** `"\bword\b"` silently becomes a backspace character and `re.sub("(a)", "\1", s)` puts the character `\x01` where group 1 was meant, so the regex never matches or the replacement is garbage (a silent wrong result). Invalid escapes like `"\d"` are a SyntaxWarning in Python 3.12+ and will become errors.
- **Wording:** three messages: an invalid escape (`Invalid escape sequence "\d" in a non-raw string ...`), a regex control character (`"\b" in a regex passed as a non-raw string is a control character, not a regex escape ...`) and a group reference (`"\1" in a regex passed as a non-raw string is the control character \x01, not a group reference ...`), each with a help line.
- **Detect:** Any non-raw string, bytes or f-string literal with an invalid escape (all strings, as in cylint; `\N`, `\u`, `\U` are valid in strings, invalid in bytes). Plus valid escapes that mean something else to the regex engine, in a regex argument (positional or keyword; plain literals, implicitly concatenated literals and the literal text of f-strings):
  - `\b` (word boundary, but a backspace in a plain string) in a pattern;
  - `\1` to `\77` (a group reference, but an octal character in a plain string; three octal digits such as `\101` are an octal escape to the regex engine too and are not reported) in a pattern and in the replacement of `re.sub` / `re.subn`;
  - `\v` in a Spark pattern only (Java reads `\v` as any vertical whitespace, a plain string holds one vertical tab).
  - Patterns read: `F.regexp_replace` / `regexp_extract` / `split` (`pattern`), `F.regexp_extract_all` / `regexp_like` / `regexp` / `rlike` / `regexp_count` / `regexp_substr` / `regexp_instr` (`regexp`), `Column.rlike` / `Column.regexp` (`other`), `re.compile` / `match` / `search` / `fullmatch` / `sub` / `subn` / `split` / `findall` / `finditer` (`pattern`), and `re.sub` / `re.subn` `repl`.
- **Not reported:** `\a`, `\f`, `\0`, `\t`, `\n` and other escapes both regex engines read as the same character (and `\v` for `re`); `\b` in an `re.sub` replacement (a backspace there either way); a pattern held in a name or built by a helper; Spark replacement strings (they use `$1`, not `\1`); SQL text (`spark.sql("... RLIKE '\bx'")`); raw strings and doubled backslashes (`"\\1"`).
- **Fix:** add the `r` prefix. Safe for an invalid escape when the literal holds no valid escape (`"\n\d"` gets no fix: the `\n` would change), has no `u` prefix and does not end in a backslash; no fix inside f-strings. Unsafe for a regex control character or group reference, offered when the literal has no `\\` and no `u` prefix.

### DBX — Databricks platform & compatibility

#### DBX001 `spark-context-access`
compat · warning · on · T1 · unsafe · Databricks access-mode & serverless limitations
- **Wording:** `{what}` is `<expr>.sparkContext` as written, or `SparkContext` / `SQLContext` / `HiveContext` for a constructor call; `context-global` is for the notebook `sc` / `sqlContext` globals.
- **Why:**
  - It blocks moving the workload to serverless or shared/standard clusters, which is often the cheaper option.
  - On standard access mode many `sc` functions stay blocked: `parallelize`, `textFile`, `setJobGroup`, `setLocalProperty`, `getConf`, `runJob`, `emptyRDD`, `range`, `union`, `stop`, …
- **Detect:**
  - `.sparkContext` on a value the type oracle knows is a SparkSession (`spark`, `SparkSession.builder...getOrCreate()`, an annotated parameter, `df.sparkSession`). One finding per access, so `sc = spark.sparkContext` is reported once, not at every later use of `sc`.
  - The names `sc` and `sqlContext` when nothing in the file defines them (the notebook globals).
  - The constructors `SparkContext(...)`, `SparkContext.getOrCreate()`, `SQLContext(...)`, `SQLContext.getOrCreate()`, `HiveContext(...)` (matched by qualified name).
- **Not reported:**
  - An access with a fallback: anywhere inside a `try:` body (any depth, not inside a function defined there) whose handler is bare or catches `Exception`, `BaseException`, `RuntimeError`, `NotImplementedError`, `PySparkException`, `PySparkNotImplementedError`, `PySparkRuntimeError`, `Py4JError` or `Py4JJavaError` (plus `NameError` for the `sc` / `sqlContext` globals) and does not raise again. Spark Connect and serverless raise `PySparkNotImplementedError` here, so such code already runs everywhere. A narrow handler (`except KeyError`), a handler that re-raises, the handler / `else` / `finally` arms and a `try:` with only `finally` are still reported.
  - An access directly in a `with pytest.raises(..)` / `self.assertRaises(..)` block (a test expecting the context to be unavailable).
  - `.sparkContext` on a receiver the type oracle does not know as a SparkSession, and `sc` / `sqlContext` when the file defines them (a parameter, an assignment).
- **Fix (unsafe):**
  - `<session>.sparkContext.setLogLevel(x)` as a whole statement → remove the statement.
  - `<session>.sparkContext.applicationId` → `<session>.conf.get("spark.app.id")`.
  - For the `sc` global the same two fixes are offered in notebooks only (using `spark`).
  - Everything else has no fix; the help text names the replacements: `spark.createDataFrame(rows)` for `parallelize`, `spark.conf.get(k)` for `getConf`, `F.broadcast(df)` for broadcast joins.

#### DBX002 `rdd-access`
compat · warning · on · T1 · unsafe · PA D002/D005, Databricks limitations
- **Why:** The RDD API is unsupported on Spark Connect, serverless compute and standard access mode (every runtime). It is also a performance smell: RDD steps run row by row in Python workers (ACT011).
- **Wording:** `<receiver>.rdd is not supported on Spark Connect, serverless or standard access mode.` with the receiver as written (a long chain hop by hop, `spark.read.format(…).load(…).rdd`). Help: `Use DataFrame APIs: df.isEmpty(), df.collect(), mapInPandas/applyInPandas for row-wise Python.`
- **Detect:** reading `.rdd` on a value the type oracle knows (or, from its name and use, believes) is a DataFrame: a typed or `*_df` parameter, a frame from a reader, `spark.sql`, `createDataFrame`, a pandas-on-Spark `to_spark()`, a cached or limited frame, a streaming frame. One finding per `.rdd`, at the attribute, whatever follows it (`print(df.rdd)`, `rdd = df.rdd`, `getNumPartitions()`, `zipWithIndex()`, `toDF()`).
  - Frames that fit on the driver (`df.limit(10).rdd.collect()`, `createDataFrame` from local data, `SHOW TABLES`) are still reported: the API is unsupported whatever the size. ACT011 skips them.
  - Overlap: on `df.rdd.map(..).collect()` ACT011 also reports the whole call for its Python-worker cost; the two findings sit on different ranges (this one on `df.rdd`) and say different things, so both stay.
- **Not reported:**
  - RDDs that do not come from a DataFrame: `sc.parallelize(...)`, `sc.textFile(...)`, `spark.sparkContext...` (the context access is DBX001's finding), and later uses of an RDD held in a name (`rdd.count()` after `rdd = df.rdd` reports only the `.rdd`).
  - `.rdd` on objects that are not DataFrames: user classes, pandas frames.
  - `.rdd` on a value the oracle cannot type: an attribute of `self` set elsewhere (`self.y_pred.rdd`), an element of a list returned by an imported helper (`splits[0].select(..).rdd`), a frame built in another file. These are known gaps of the type oracle, not of the rule.
- **Fix (unsafe):**
  - `df.rdd.isEmpty()` → `df.isEmpty()` (when the configured Spark version is 3.3 or later; not when `isEmpty` gets arguments).
  - `df.rdd.map(lambda r: <one-line expression>).collect()` → `[<expression> for r in df.collect()]`.
  - `df.rdd.flatMap(lambda r: <one-line expression>).collect()` → `[v for r in df.collect() for v in <expression>]` (the column-to-list idiom `df.select("c").rdd.flatMap(lambda x: x).collect()` becomes `[v for x in df.select("c").collect() for v in x]`); the flattened value is named `v`, `value`, `item` or `_v`, the first that no word of the lambda uses, and a conditional / lambda / walrus expression is parenthesised.
  - Unsafe because the comprehension runs on the driver over `Row` objects pulled with `collect()`, where the RDD steps ran in Python workers; results are the same list.
  - Other uses (`getNumPartitions()`, `mapPartitions`, `take(n)` after a step, a lambda with several parameters or a multi-line body, ...) get no fix; the help text points to `df.isEmpty()`, `df.collect()` and `mapInPandas` / `applyInPandas` for row-wise Python.

#### DBX003 `input-file-name`
compat · warning · on · T0 · unsafe · Databricks (unavailable on UC; deprecated since DBR 13.3 LTS, `input_file_name` unsupported from DBR 17.3 LTS)
- **Why:** `input_file_name()`, `input_file_block_start()` and `input_file_block_length()` are not available on Unity Catalog and are deprecated since Databricks Runtime 13.3 LTS; `input_file_name()` is no longer supported at all in Databricks SQL and from Databricks Runtime 17.3 LTS. The file-metadata column `_metadata` (`file_path`, `file_name`, `file_block_start`, `file_block_length`, ...) replaces them and is reliable, where the functions are non-deterministic.
- **Wording:** two message variants: one for `input_file_name()` (names the 17.3 LTS removal and `_metadata.file_path`), one for the block functions (names the function and its `_metadata` field). The help names the replacement column for DataFrame code and SQL.
- **Detect:**
  - `F.input_file_name()`, `F.input_file_block_start()`, `F.input_file_block_length()`, and the same functions imported from `pyspark.sql.functions` (matched by qualified name, so a local function of that name is not reported).
  - `F.call_function("input_file_name")` (Spark 3.5+, any case, the name may be a constant bound once).
  - A call to any of the three in SQL text: the query `spark.sql(...)` runs (positional or `sqlQuery=`, an f-string, or a name bound once to the text; an `EXPLAIN` only plans, so it is not reported), the string passed to `F.expr(...)`, and string arguments of `df.selectExpr(...)` (also list items) / `df.filter(...)` / `df.where(...)` (also `condition=`, through a name bound once). The name inside a string literal or comment of that SQL is not a call. One finding per function named in a text.
- **Not reported:** `F.call_function(<name not known as a constant>)`, the `_metadata` column itself, local functions with the same names.
- **Fix (unsafe):** `F.input_file_name()` → `F.col("_metadata.file_path")`, `F.input_file_block_start()` → `F.col("_metadata.file_block_start")`, `F.input_file_block_length()` → `F.col("_metadata.file_block_length")` (attribute form only; bare imported names, `call_function` and SQL text get no fix). Only valid for file-source DataFrames, hence unsafe. The full path (`file_path`) is what `input_file_name()` returned; `file_name` is only the last path part.

#### DBX016 `list-tables-existence-check`
perf · info (warning in loops) · on · T1 · none · new
- **Wording:** `{what}` is `table`, or `schema` for `listDatabases()` / `SHOW SCHEMAS`; `{api}` in the help is `spark.catalog.tableExists("catalog.schema.table")` or `databaseExists("catalog.schema")`.
- **Why:** `spark.catalog.listTables(db)` / `SHOW TABLES IN db` returns metadata for every table in the schema; on Unity Catalog that is paged metastore calls, slow on schemas with thousands of tables. Code that only wants to know whether ONE table exists (often inside a loop over table names) keeps the cluster idle while the driver waits. `spark.catalog.tableExists("cat.db.tbl")` / `databaseExists` (Spark >= 3.3, Databricks) is one lookup.
- **Detect:** a full listing — `Catalog.listTables(db)` / `listDatabases()` without a `pattern` argument (oracle: `Catalog` receiver), or `spark.sql("SHOW TABLES|SCHEMAS|DATABASES [IN|FROM x]")` with no pattern, optionally `.select(..)`, then `.collect()`/`.toLocalIterator()` — used inline as (1) the source of a comprehension/generator whose condition or element compares the row's name field (`.name`, `.tableName`, `.databaseName`, `.namespace`, `["tableName"]`, optionally `.lower()`) with `==`; (2) the right side of `in`/`not in` (the listing itself or a comprehension projecting the name); (3) a `for` loop over it whose body has a top-level `if` / `elif` testing `<name> == x` (a test nested in another loop or block is not followed); (4) `spark.sql("SHOW TABLES ...").filter/where(...)` comparing `tableName` / `databaseName` / `namespace` with `==` (Column expression) or SQL `=` (string condition). Reported on the listing call ((4): on the `filter` / `where` call); warning when that call sits inside a loop (a comprehension's own iteration does not count, nor a retry or first-match loop that leaves right after the call with an unconditional `break` / `return`), info otherwise. No fix (the table's qualified name is not known statically). A listing bound to a name first (listed once, then many lookups) is not reported.
- **FP:** Low. Filtering the listing by other fields (`tableType == "VIEW"`, `isTemporary`), `!=`/`in <collection>` filters, pattern arguments and `SHOW TABLES LIKE` stay quiet.
- **Ex:** `if name in [t.name for t in spark.catalog.listTables("db")]:` → `if spark.catalog.tableExists(f"db.{name}"):`

#### DBX017 `pipeline-stream-fanout`
perf · info · on · T1 · none · 2026-11-09 (Lakeflow fan-out docs, ForEachBatch sink Public Preview)
- **Wording:** Each dataset in `{datasets}` is labelled with its `name=` (as written) or function name and line, `(N from a loop, ...)` for a def in a literal-list loop; when a loop's length is unknown `{by}` reads `pipeline datasets/flows generated in a loop` and the def is labelled `(one per loop item, ...)`.
- **Why:** In a Lakeflow (DLT) pipeline every streaming table / append flow is a separate streaming query with its own checkpoint. When one ingestion table fans out to many outputs (often a `for` loop generating one `@dp.table` per region/type), each output reads every micro-batch of the source on its own, so N small queries run concurrently and the pipeline needs a cluster sized for N streams for little work. Databricks documents this for loops ("each generated flow or table processes the entire source dataset independently") and offers the ForEachBatch sink (`@dp.foreach_batch_sink`, Public Preview on the PREVIEW channel): one `append_flow` reads the source once and the handler writes every target per micro-batch (with `persist()` so the batch is not recomputed per write).
- **Detect:** functions decorated with `dlt`/`pyspark.pipelines` `table`, `streaming_table`, `materialized_view`, `view`, `temporary_view` or `append_flow` (qualified names, any alias) whose body streams a fixed source: `spark.readStream….table("x")` (oracle: `DataStreamReader`), `….load("path")`, Kafka-style `….option("subscribe", "t").load()`, or `dlt.read_stream("x")` / `dp.read_stream("x")` (`live.` prefix and case ignored). Readers are grouped by source; a def inside a `for` over a literal list counts once per element (nested loops multiply), a loop over anything else counts as reaching the threshold; decorated defs inside helper functions count once each. Report on the first read when ≥ `pipeline-fanout-min-flows` (default 3; 0 disables) datasets stream the same source, one finding per source. Sources that change per iteration (f-strings) and reads inside an undecorated def nested in the dataset function (a handler) are ignored; batch reads (`spark.read.table`) are not counted (the ForEachBatch sink is for streaming flows).
- **FP:** Low-med. Some fan-outs are deliberate (targets need expectations, lineage or full-refresh handling, which a ForEachBatch sink gives up; writes across targets are not atomic) — suppress with `# noqa: DBX017`. SQL pipeline files are not linted.
- **Ex:** `for r in REGIONS: @dp.table(name=f"orders_{r}") def t(): return spark.readStream.table("raw_orders").filter(...)` → one `@dp.foreach_batch_sink(name="orders_by_region")` whose handler persists `batch_df` and writes `batch_df.filter(...)` to each region table (`txnAppId`/`txnVersion` options for idempotent retries), fed by a single `@dp.append_flow(target="orders_by_region")` reading `raw_orders`.

### PDS — pandas-on-Spark

#### PDS005 `pandas-on-spark-ops-on-diff-frames`
perf · warning · on · T0 · none · new
- **Why:** By default pandas-on-Spark refuses arithmetic/assignment between two different frames because it needs a join on the index. Turning the option on removes that guard, so `psdf1["a"] + psdf2["b"]` or `psdf1["x"] = psdf2["y"]` each become a hidden join with a shuffle. The best-practices guide says to avoid it.
- **Detect:** `ps.set_option("compute.ops_on_diff_frames", True)` (also `pyspark.pandas.config.set_option`, `key=` / `value=`), `ps.option_context(..., "compute.ops_on_diff_frames", True, ...)`, `ps.options.compute.ops_on_diff_frames = True`, and the Spark conf key `pandas_on_Spark.compute.ops_on_diff_frames` (the prefix pandas-on-Spark stores its options under) set to `True` / `"true"` (any casing) every way Spark config is written: `spark.conf.set`, `SparkSession.builder.config(k, v)` or `config(map={...})`, `SparkConf().set(..)`, and SQL `SET k = true`; a key or value bound to a constant name is followed. A conf write that a later write of the same key in the file turns back off (or `spark.conf.unset`) is still reported, at info. Names are matched by qualified name, so plain `pd.set_option` is never reported; non-literal values are skipped. No fix.
- **FP:** Low (exact option name). A deliberate, one-off combine on small frames can be suppressed with `# noqa: PDS005`.
- **Ex:** `ps.set_option("compute.ops_on_diff_frames", True); psdf1["a"] + psdf2["b"]` → `psdf1.merge(psdf2, on="id")` then operate within one frame.

#### PDS006 `pandas-on-spark-single-partition-op`
perf · info · on · T1 · none · new
- **Why:** On an un-grouped `ps.DataFrame`/`ps.Series`, `rank`, `shift`, `diff`, `pct_change` and `cumsum/cumprod/cummax/cummin` are built on a Spark Window with no partition spec, which "moves all data into a single partition in a single machine" (an OOM or one-core stage). This is the pandas-on-Spark twin of PRF005.
- **Detect:** One of those methods called on a receiver the oracle types as pandas-on-Spark with Certain confidence: `ps.DataFrame/Series(...)`, `ps.read_*`, `ps.from_pandas`, `ps.concat`, `ps.sql`, `df.pandas_api()`/`to_pandas_on_spark()`, `ps.DataFrame`/`ps.Series` annotations, and values derived from them by column selection (`psdf["x"]`, `psdf[["a","b"]]`, boolean masks), element-wise arithmetic/comparison with numbers or other ps frames, and frame-preserving methods (`sort_values`, `fillna`, `assign`, ...). `groupby(..)`, `rolling`/`expanding`, reductions and integer/label lookups are not frames, so grouped ops are never reported. In a chain (`s.shift(1).cumsum()`) only the first op is reported. Not followed (not reported): `.loc` / `.iloc` selections, `head(n)`, and aggregates of a `groupby` (`psdf.groupby(k).sum().cumsum()`). No fix.
- **FP:** Medium: harmless on small frames (suppress with `# noqa: PDS006`). Plain pandas and Spark DataFrames are never reported.
- **Ex:** `psdf["amount"].cumsum()` → `psdf.groupby("user")["amount"].cumsum()`, or a Spark `Window.partitionBy("user").orderBy("ts")`.

### STY — Style & naming (all off in `default`; STY001–003 on in `cylint-compat`)

#### STY001 `functions-alias`
style · info · T0 · safe · CY037, astrumu C4003/C4004, ruff#7272
- **Wording:** `{module}` is `functions` or `types`; the accepted aliases (`{allowed}`) come from the settings.
- **Detect:** `from pyspark.sql import functions` / `types` without an alias or with an alias not in `functions-aliases` (default `["F", "sf"]`; the Spark docs use `sf`, per the ruff thread) / `types-aliases` (default `["T"]`), and `import pyspark.sql.functions as f` / `import pyspark.sql.types as t` with an alias not in the list. Not reported: a plain `import pyspark.sql.functions` (used fully qualified), relative imports, imports of single functions (`from pyspark.sql.functions import col`).
- **Fix (safe):** Rename the alias (or add `as F`) and every reference to it, using the first accepted alias. No fix when that name is already bound anywhere in the file (module or nested scope), or when a reference can also resolve to another binding.

#### STY002 `join-missing-how`
style · info · T1 · safe · CY010, palantir
- **Detect:** A `join` call on a DataFrame with at least one argument, no third positional argument, no `how=` keyword and no `*args` / `**kwargs`.
- **Fix (safe):** Append `how="inner"` after the last argument (the join type Spark uses when none is given).

#### STY003 `redundant-drop-before-select`
style · info · T1 · safe · CY032, ruff#7272
- **Detect:** `.drop(...)` on a DataFrame directly followed by `.select(...)` in the same chain (a frame bound to a name between the two is not followed), where every select argument is an explicit column: not `"*"` / `"t.*"` / `F.col("*")`, `df.columns`, `*cols`, or a list / comprehension / name that is not known to be a Column; selects with keyword arguments are skipped. Reported on the `drop`.
- **Fix (safe):** Remove the `.drop(...)`. No fix when the select names a dropped column (a string, an identifier inside an expression string such as `F.expr("id + 1")`, or an attribute like `b.id`): after a join the drop can be what resolves an ambiguous name.
