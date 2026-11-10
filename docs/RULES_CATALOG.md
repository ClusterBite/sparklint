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
- **Msg:** `Spark action: df.count() triggers a job (kind: driver-collect).`
- **Why:** In a cost review the first question is where the jobs are. This rule inventories every point that triggers a job or eager work, with a `kind` field: `job` (count/first/isEmpty/foreach), `driver-collect` (collect/toPandas/take/head/tail/toLocalIterator), `write` (save/saveAsTable/insertInto/writeTo.*/DeltaMergeBuilder.execute/DeltaTable.optimize/vacuum), `eager-sql` (`spark.sql` with DDL/DML text), `eager-read` (CSV/JSON read with schema inference), `stream-start`, `checkpoint` (eager), `schema-rpc` (`.columns`/`.schema`/`.dtypes`, Connect only), and `display`.
- **Detect:** A call whose resolved receiver type plus method is in the action table. `spark.sql(<const>)` is classified by its leading SQL keyword. The `--output actions` reporter emits CSV/JSON grouped by file and function, with loop nesting depth.
- **FP:** None by construction (it is an inventory). Nested lambdas inside `foreachBatch` are labelled `per-microbatch`.
- **Ex:** `n = df.count()` → (reported, no change)

#### ACT002 `collect-unbounded`
perf · warning · on · T2 · unsafe · CY001, PA D001
- **Msg:** `.collect() on a DataFrame with no known row bound pulls everything to the driver.`
- **Why:** The most common cause of driver OOM. It also serialises the job's whole output through the driver, so the cluster sits idle.
- **Detect:** `x.collect()` where `type(x)=DataFrame` and not `bounded`. If `reduced`, downgrade to info with the message suffix "(filtered/aggregated — size unknown)".
- **FP:**
  - Lookup tables that are small but not provably bounded. Configure `small-tables = ["dim_*", "*.config"]` (matched against the `source` fact), or an explicit `.limit()`.
  - Already inside a `bounded` function summary (T3).
- **Fix (unsafe):** `.collect()` → `.limit(N).collect()`, with `N` from config. Changes semantics, so unsafe.
- **Ex:** `rows = spark.table("sales").collect()` → `rows = spark.table("sales").limit(1000).collect()` or keep the work in Spark.

#### ACT003 `topandas-unbounded`
perf · warning · on · T2 · none · CY006, PA P001
- **Msg:** `.toPandas() on an unbounded DataFrame materialises it in driver memory (x2–3 for pandas).`
- **Why:** Same as ACT002, plus pandas memory overhead. Also covers pandas-on-Spark `psdf.to_pandas()` / `.to_numpy()`.
- **Detect:** `toPandas()` on a DataFrame, or `to_pandas()`/`to_numpy()` on `ps.DataFrame`/`ps.Series`, when not `bounded`. `reduced` → info.
- **FP:** Same as ACT002. ML feature-matrix code is a common legitimate use; exempt it by path glob (`ml/**`).
- **Ex:** `pdf = df.toPandas()` → `pdf = df.select(cols).limit(n).toPandas()`, or `df.write…` and read the result elsewhere.

#### ACT004 `driver-row-iteration`
perf · warning · on · T1 · none · CY031
- **Msg:** `Iterating rows on the driver (for row in df.collect()) — express this as a DataFrame transformation.`
- **Why:** Single-threaded Python over the whole dataset while the executors are idle and billed.
- **Detect:** The iterable of a `for` statement or comprehension is `collect()`, `toLocalIterator()`, `toPandas().iterrows()/itertuples()/items()`, `.rdd.collect()`, or a name bound to one of those (T2). The receiver must be a DataFrame or RDD. Higher severity when the loop body calls a Spark action or write (the N+1 pattern, see ACT005).
- **FP:**
  - Iterating a `bounded` frame (e.g. a config table with `limit`) → no finding.
  - Iterating `df.columns` or `df.dtypes` is not row iteration and is not matched.
- **Ex:** `for r in df.collect(): out.append(r.a*2)` → `df.select((F.col("a")*2).alias("a2"))`

#### ACT005 `action-in-loop`
perf · warning · on · T2 · none · new
- **Msg:** `Spark action inside a loop runs one job per iteration (N jobs, N scans).`
- **Why:** The classic N+1 pattern, e.g. `for d in dates: df.filter(...).count()`. Each iteration rescans the source. It usually collapses into one `groupBy`.
- **Detect:** An ACT001-table action, or a write, lexically inside a `for`/`while`/comprehension body (also inside a function called from the loop body, via a T3 summary flag `performs_action`). The receiver must be a DataFrame or writer. Reported once per loop and action-site.
- **FP:**
  - Loops over a small constant list of tables (`for t in ["a","b"]: spark.table(t).write…`) are legitimate orchestration. Downgrade to info when the iterable is a literal list/tuple of length ≤ `loop-literal-max` (default 10) and each iteration uses a different source.
  - The `foreachBatch` body is not a loop.
- **Ex:** `for c in countries: print(df.filter(F.col("c")==c).count())` → `df.groupBy("c").count().show()`

#### ACT006 `count-for-emptiness`
perf · warning · on · T1 · safe (Spark ≥3.3) / unsafe · CY020, PA D006/D007/D009, PA D005
- **Msg:** `df.count() used as an emptiness check scans every row — use df.isEmpty().`
- **Why:** `count()` cannot stop early. `isEmpty()` is `limit(1)`.
- **Detect:**
  - `Compare(df.count(), [==|!=|>|<=], 0)`, or against 1 with `>=`/`<`, either side.
  - `not df.count()`, or `df.count()` in a boolean context (if/while/assert/BoolOp).
  - `len(df.head(1))` is fine.
  - `df.rdd.isEmpty()` → `df.isEmpty()` (D005).
  - T2: `n = df.count()` where every later use of `n` is an emptiness compare.
- **FP:** If `n` is also logged or used arithmetically, skip (the count is needed).
- **Fix:** Safe when `spark-version ≥ 3.3`: `df.isEmpty()` / `not df.isEmpty()`. Otherwise unsafe: `len(df.take(1)) == 0`.
- **Ex:** `if df.count() > 0:` → `if not df.isEmpty():`

#### ACT007 `action-only-for-logging`
perf · warning · on · T1 · none · CY021 (widened), ruff#7272
- **Msg:** `df.count() is computed only to be logged — a full job for a log line.`
- **Why:** Very common in real jobs: `logger.info(f"rows: {df.count()}")` after every step doubles or triples job count. Each call recomputes the lineage unless cached.
- **Detect:**
  - An action (count/collect/first/head/take/isEmpty/`len(df.collect())`/`df.rdd.getNumPartitions()`) appears only inside the arguments of a logging sink: `print`, `display`, `logging.*`, a `logging.Logger` instance (typed via `logging.getLogger`), `structlog`/`loguru` loggers, `dbutils.notebook.exit`, or `mlflow.log_metric`.
  - Also bare expression statements `df.count()` (result discarded).
  - T2: `n = df.count()` where every use of `n` is a logging sink.
- **FP:**
  - The last top-level expression statement of a notebook cell (`.ipynb` and Databricks source format): the notebook displays its value, so `df.head()` / `df.count()` there is used, not discarded. Not reported (the "computed only to be logged" form still is).
  - pandas / numpy / polars values, and everything derived from them by a method, attribute, index or operator (`pd_df = pd_df.join(..)`; `pd_df.head()`), are never Spark, whatever the variable is called.
  - Counts that are deliberately logged as data-quality metrics. Allow `# sparklint: ignore[ACT007]` or config `log-metrics-allowed = ["dq_*"]` (function-name globs).
  - Cheap when the source is `bounded`.
  - Unlike cylint, this rule does not use the `log` substring heuristic.
- **Ex:** `log.info("n=%d", df.count())` → remove; or read `numOutputRows` from the write's Delta history; or `df.observe("m", F.count(F.lit(1)))` (Spark 3.3+, metric computed in the same job).

#### ACT008 `collect-to-filter-roundtrip`
perf · warning · on · T2 · none · new
- **Msg:** `Values collected to the driver are fed back into isin()/a filter — use a semi join.`
- **Why:** Two jobs plus a huge literal `IN` list in the plan (slow analysis, codegen limits, driver memory). A `left_semi` join (broadcast if small) is one job.
- **Detect:** A name bound to a list comprehension, `collect()`, or `toPandas()[col].tolist()` over a DataFrame flows (T2) into `Column.isin(...)`, a `filter`/`where` expression, or an f-string inside `spark.sql`.
- **FP:** Bounded sources (e.g. `limit(10)`), or one scalar (`first()[0]`) used as a threshold. That is a scalar, not a list, so it is not matched.
- **Ex:** `ids=[r.id for r in bad.select("id").collect()]; df.filter(F.col("id").isin(ids))` → `df.join(bad.select("id"), "id", "left_semi")`

#### ACT009 `collect-for-driver-compute`
perf · warning · on · T2 · unsafe · new
- **Msg:** `Aggregate computed on the driver from collect() — compute it in Spark.`
- **Why:**
  - `len(df.collect())`, `sum(r.x for r in df.collect())`, `max(...)`, `sorted(df.collect())[0]`, and `df.collect()[0][0]` on unbounded frames transfer all rows to compute one scalar.
  - `collect()[0]` on an unbounded frame pulls everything to read one row.
- **Detect:**
  - Python builtin `len/sum/min/max/sorted/any/all/set/Counter` applied to a collect-derived iterable.
  - `Subscript(collect(), 0)` where the receiver is not `bounded(1)` (`agg` without `groupBy` is `bounded(1)`, so `df.agg(F.max("x")).collect()[0][0]` passes).
- **Fix (unsafe):** `df.collect()[0]` → `df.first()`; `len(df.collect())` → `df.count()`.
- **Ex:** `mx = max(r.ts for r in df.collect())` → `mx = df.agg(F.max("ts")).first()[0]`

#### ACT010 `debug-action-left-in`
perf · warning · on · T1 · unsafe · CY012, PA D003/D008, astrumu module_show
- **Msg:** `.show()/display()/printSchema()/explain() left in pipeline code triggers extra jobs.`
- **Why:** Each `show`/`display` is a job (display may collect up to 10k rows). In scheduled jobs nobody reads the output. Console or memory streaming sinks in production are the streaming equivalent.
- **Detect:**
  - DataFrame `.show/.display/.printSchema/.explain/.describe().show/.summary().show`, on a receiver the oracle types as a Spark DataFrame (Certain or Likely).
  - Global `display(x)` when `x` is typed as a Spark DataFrame (Certain or Likely) and is not row-bounded.
  - `writeStream.format("console"|"memory")`.
  - `.py` modules imported as libraries, and job entry points (configured via `job-paths`) → warning. Notebook sources → info, because interactive exploration is legitimate. `printSchema`/`explain` are always info (no job on Classic).
- **FP:**
  - Notebooks used both interactively and as jobs. Use `notebook-severity = "info"|"warning"`.
  - Not Spark actions, never reported: `displayHTML(..)` (renders a string), `display(dbutils.fs.ls(..))`, `display(<pandas frame / figure / image / list>)`, `display(df.head(5))` / `display(df.take(n))` (already local rows), and a row-bounded frame (`display(df.limit(10))`, aggregate-only or `LIMIT n` SQL: a deliberate small preview).
  - `.explain()` / `.show()` on objects of other libraries that mirror the DataFrame API (dbldatagen's `DataGenerator(..).withColumn(..)` builder, plotly figures): an instance of a class imported from a non-Spark library, and the builder-style chain on it, is not a DataFrame.
  - `display(x)` where the oracle cannot type `x` (a result of a third-party call bound to a name that does not look like a DataFrame, e.g. `logs = spec.build()`) is not reported: a known gap, preferred over reporting every `display`.
- **Fix (unsafe):** Delete the statement when it is a bare expression statement and the DataFrame is Certain (never on a heuristic, name-based type).
- **Ex:** `df.show()` → (remove)

#### ACT011 `rdd-collect`
perf · warning · on · T1 · unsafe · CY034, PA PERF001
- **Msg:** `df.rdd.collect() bypasses Arrow and converts row-by-row in Python.`
- **Why:** The `.rdd` conversion deserialises every Row through Py4J/pickle, which is far slower than `collect()` or Arrow `toPandas()`. It is also not available on Connect, serverless or standard access mode (see DBX002).
- **Detect:** `x.rdd.collect()` / `x.rdd.map(...).collect()` / `x.rdd.take(n)` where `x: DataFrame`.
- **Fix (unsafe):** `df.rdd.collect()` → `df.collect()`.
- **Ex:** `df.rdd.map(lambda r: r.id).collect()` → `[r.id for r in df.select("id").collect()]` or `df.select("id").toPandas()["id"]`.

#### ACT015 `approx-quantile-exact`
perf · warning · on · T1 · none · new
- **Msg:** `approxQuantile(..., relativeError=0) computes exact quantiles: a full pass over the data that the Spark docs call "very expensive".`
- **Why:** `relativeError=0` turns the Greenwald-Khanna sketch into an exact computation: every value is kept in the summaries, which are merged and collected to the driver. The docs warn it "could be very expensive". It is an action, often used for outlier bounds inside pipelines, so it can also run once per loop iteration.
- **Detect:** `approxQuantile` on a receiver the oracle types as a `DataFrame` with Certain confidence (or `df.stat` of one) whose `relativeError` (third positional or keyword) constant-folds to `0` / `0.0` (literal, or a name bound once to such a literal). Calls with `**kw` are skipped.
- **FP:** Low. pandas `quantile`, numpy and user classes are never Spark; untyped parameters and Py4J handles are not Certain DataFrames. Genuinely needing exact values on a small frame is the legitimate case (`# noqa: ACT015`).
- **Ex:** `df.approxQuantile("x", [0.25, 0.75], 0.0)` → `df.approxQuantile("x", [0.25, 0.75], 0.001)`

### PRF — Plan & shuffle performance

#### PRF001 `withcolumn-in-loop`
perf · warning · on · T2 · unsafe · CY003, PA L003, ruff#7272
- **Msg:** `.withColumn() in a loop adds one projection per iteration — use withColumns()/select().`
- **Why:** Every call creates a new Project node and re-runs analysis, so cost is O(n²) in the number of columns. With hundreds of columns, driver planning time reaches minutes and can hit StackOverflowError (Spark docs warn about this explicitly).
- **Detect:**
  - `DataFrame.withColumn` call inside a `for`/`while` body or comprehension, where the result is reassigned to a name that flows back into the next iteration's receiver (a loop-carried DataFrame).
  - Also `functools.reduce(lambda d, c: d.withColumn(...), cols, df)`.
  - The receiver must be typed DataFrame (avoids cylint's untyped match).
- **FP:** Loops with ≤ 3 iterations over a literal → info.
- **Fix (unsafe):** When the loop body is a single `df = df.withColumn(<expr_c>, <expr>)` with no other statements, rewrite to `df = df.withColumns({<expr_c>: <expr> for c in <iter>})` (Spark ≥ 3.3). Otherwise use `select("*", ...)`.
- **Ex:** `for c in cols: df = df.withColumn(c, F.trim(c))` → `df = df.withColumns({c: F.trim(c) for c in cols})`

#### PRF002 `column-op-in-loop`
perf · warning · on · T2 · unsafe · CY011
- **Msg:** `.withColumnRenamed()/.drop()/.select() in a loop — batch it into one call.`
- **Why:** Same plan growth as PRF001.
- **Detect:** A loop-carried DataFrame receiving `withColumnRenamed`, `drop`, `select`, `selectExpr`, `toDF`, `alias`, `cast`-via-withColumn, or `na.fill(subset=[c])` inside a loop.
- **Fix (unsafe):** Renames → `df.withColumnsRenamed({old: new for …})` (Spark ≥ 3.4); drops → `df.drop(*cols)`.
- **Ex:** `for c in cols: df = df.withColumnRenamed(c, c.lower())` → `df = df.withColumnsRenamed({c: c.lower() for c in cols})`

#### PRF003 `cartesian-join`
perf · error · on · T1 · none · CY007, PA S010/S011
- **Msg:** `Cartesian product: crossJoin()/join() without a condition produces |A|×|B| rows.`
- **Why:** Output explodes and runs as a broadcast nested loop or CartesianProduct. This is the single most expensive accident in a cost review.
- **Detect:**
  - `DataFrame.crossJoin(x)`.
  - `DataFrame.join(other)` with no `on` and no positional condition.
  - `join(other, on=F.lit(True))`.
  - Also SQL strings: `CROSS JOIN`, or comma joins `FROM a, b` without a WHERE that equates them (best-effort SQL parse).
- **FP:** One side is `bounded(1)` (scalar from `agg()` without groupBy, or `createDataFrame([literal])`). This is the scalar-broadcast idiom → no finding. Calendar or dimension spines from `spark.range`/`sequence` with a small literal → info.
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
- **Detect:** A `WindowSpec` value (built from `Window`/an alias of `pyspark.sql.window.Window`, tracked through names and function returns) with `orderBy`/`rowsBetween`/`rangeBetween` and no `partitionBy` with ≥ 1 argument, used in `Column.over(...)`. Also `Window.partitionBy()` with no args, and SQL `OVER (ORDER BY …)` without `PARTITION BY`.
- **FP:** The DataFrame is `bounded` → info. Global `row_number` for surrogate keys: the message suggests `monotonically_increasing_id`, `zipWithIndex`, or identity columns.
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
- **Ex:** `agg = big.join(d,"k").groupBy("x").sum(); agg.write…; agg.count()` → `agg = (...).persist(); …; agg.unpersist()`

#### PRF008 `cache-single-use`
perf · warning · on · T2 · unsafe · CY005
- **Msg:** `df is cached but consumed only once — cache costs a materialisation and memory with no reuse.`
- **Why:** Cache adds serialisation and storage-memory pressure (and evicts useful blocks) without any reuse.
- **Detect:** A `cached` DataFrame value whose number of *consuming* uses on every path (actions, writes, or derivations that end in actions) is ≤ 1. A use inside a loop counts as many.
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
  - Count ≥ `write-repartition-min` (default 100).
  - The format is not Delta and the sink is a single-file export → handled by IOD003 instead.
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
- **FP:** A small export for a single-file consumer (CSV for a business user): the source is `bounded`/`reduced` → info. Suggest `repartition(1)` over `coalesce(1)` so the upstream stays parallel.
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
- **Msg:** `Adaptive Query Execution disabled (spark.sql.adaptive.enabled=false).`
- **Why:** Loses partition coalescing, skew-join splitting and dynamic broadcast conversion. Usually means more tasks, worse skew, and bigger clusters. Also flags `…adaptive.coalescePartitions.enabled=false` and `…skewJoin.enabled=false`.
- **Detect:** `spark.conf.set(k, v)` / `SparkSession.builder.config(k, v)` (also `config(map={...})`) / `SparkConf().set(k, v)` / `spark.sql("SET k=v")` with those keys and a false value (literal, single-assignment variable). Shared with CFG002/CFG007: when the same key is written again later in the file (another value, or `spark.conf.unset(k)`), the earlier write is a scoped workaround and is reported as info.
- **Ex:** remove the line.

#### CFG002 `broadcast-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `autoBroadcastJoinThreshold=-1 disables broadcast joins for the whole session.`
- **Why:** Every small-dimension join becomes a sort-merge join with a full shuffle of the large side. Usually it was added to work around one bad join.
- **Detect:** Key `spark.sql.autoBroadcastJoinThreshold` (or `spark.sql.adaptive.autoBroadcastJoinThreshold` / `spark.databricks.adaptive.autoBroadcastJoinThreshold`) set to a size ≤ 0 (`-1`, `"0"`, `"-1b"`), through the same config-write matcher as CFG001.
- **Ex:** Remove it; for the one problem join use `hint("merge")` or `hint("shuffle_hash")`.

#### CFG003 `static-conf-at-runtime`
correctness · error · on · T0 · none · new (Databricks Spark config limitations)
- **Msg:** `spark.executor.memory cannot be changed at runtime — this has no effect (and raises CANNOT_MODIFY_CONFIG on Spark Connect/serverless).`
- **Why:** The user believes they tuned the job. In fact nothing changed (Classic), or it fails (Connect).
- **Detect:** `spark.conf.set` / SQL `SET` with a key in the static/cluster-level table: `spark.executor.*`, `spark.driver.*`, `spark.dynamicAllocation.*`, `spark.serializer`, `spark.jars*`, `spark.submit.*`, `spark.master`, `spark.task.cpus`, `spark.memory.*`, `spark.sql.extensions`, `spark.sql.catalogImplementation`, `spark.default.parallelism`, `spark.local.dir`, and `spark.databricks.cluster.*`. `SparkSession.builder.config(...)` / `SparkConf().set(...)` in a notebook whose session already runs → warning (ignored). The session already runs in a Databricks notebook (source format, or `.ipynb` with Databricks metadata); in any other notebook only with evidence in the file: `spark` / `sc` / `sqlContext` / `dbutils` / `displayHTML` used without being defined, `display` used before the builder call, or an earlier `getOrCreate()`.
- **FP:** A plain Jupyter notebook whose first Spark statement is the builder: there `SparkSession.builder.config(...).getOrCreate()` creates the session and the static settings apply. Not reported.
- **Ex:** move it to the cluster or job compute config.

#### CFG007 `broadcast-threshold-too-high`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.autoBroadcastJoinThreshold raised to 2 GB: every table estimated under that size is collected to the driver and copied to every executor.`
- **Why:** Each automatic broadcast is collected to the driver, hashed there and shipped to every executor. A threshold in the GB range turns ordinary joins into driver OOMs and broadcast timeouts, and missing or stale statistics let large tables under the estimate. Above Spark's hard 8 GB limit a qualifying join fails outright ("Cannot broadcast the table that is larger than 8GB").
- **Detect:** The CFG002 keys set to a size ≥ `broadcast-threshold-max` (default `"1g"`, `"0"` = off). Values are parsed like Spark's byte strings (`"512m"`, `"2g"`, `"16gb"`, plain bytes) and folded from int arithmetic (`2 * 1024 ** 3`, `str(...)`, `1 << 31`). Error at ≥ 8 GB.
- **FP:** Low: the value is a literal. Clusters with very large drivers can raise `broadcast-threshold-max`; a write that is set back later in the file is info.
- **Ex:** `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "2g")` → keep the default; `big.join(F.broadcast(small_filtered), "k")`.

#### CFG008 `predicate-pushdown-disabled`
perf · warning · on · T1 · none · new
- **Msg:** `spark.sql.parquet.filterPushdown=false: Parquet scans stop using row-group min/max statistics to skip data.` / `JDBC read with pushDownPredicate=false: filters are not sent to the database, so the whole table is copied over the network.`
- **Why:** With pushdown off, Parquet/ORC readers cannot skip row groups by their statistics, and a JDBC source ships the whole table to Spark to be filtered there: a full scan (for JDBC a full table transfer) for every filtered query. Usually a leftover workaround for one old reader bug.
- **Detect:** `spark.sql.parquet.filterPushdown` / `spark.sql.orc.filterPushdown` set false through the CFG001 config-write matcher (a write set back later in the file is info). JDBC: a `DataFrameReader.load()` chain with `format("jdbc")` (or `load(format="jdbc")`) whose `option`/`options` set `pushDownPredicate` to `False`/`"false"` (case-insensitive key), or `spark.read.jdbc(..., properties={...})` with that key (literal dict or a name bound once to one).
- **FP:** Low: exact keys and literal values. A source with a buggy pushdown deserves a noqa with the reason.
- **Ex:** `spark.read.format("jdbc").option("pushDownPredicate", False)...load()` → drop the option, or move the filter into the `query` subquery.

#### CFG009 `delta-file-compaction-disabled`
perf · info · on · T0 · none · new
- **Msg:** `spark.databricks.delta.optimizeWrite.enabled=false turns off optimized writes for every Delta table this session writes: appends, MERGEs and streaming writes leave many small files.` / `Table property delta.autoOptimize.autoCompact = false: writes to this table stop sizing their files.`
- **Why:** The session switches override every table the job writes, so appends, MERGEs and streaming micro-batches produce many small files: slower reads, a bigger Delta log and OPTIMIZE debt. Databricks recommends optimized writes plus auto compaction.
- **Detect:** `spark.databricks.delta.optimizeWrite.enabled`, `spark.databricks.delta.autoCompact.enabled` and their `spark.databricks.delta.properties.defaults.autoOptimize.*` defaults set false through the CFG001 config-write matcher (a write set back later in the file is info). SQL: `spark.sql` text whose `TBLPROPERTIES (...)` list sets `delta.autoOptimize.optimizeWrite` / `delta.autoOptimize.autoCompact` to false (ALTER and CREATE).
- **FP:** Low: exact keys and literal values. Latency-bound streams sometimes turn it off on purpose (hence info); a noqa with the reason documents that.
- **Ex:** `spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")` → remove it; if one table needs it off, set the table property there and schedule OPTIMIZE.

#### CFG010 `disk-cache-disabled`
perf · info · on · T0 · none · new
- **Msg:** `spark.databricks.io.cache.enabled=false turns off the Databricks disk cache: every later read of the same Parquet/Delta files goes back to cloud storage.`
- **Why:** The disk cache keeps local SSD copies of remote Parquet/Delta files, so repeated reads (joins against the same dimension, iterative jobs, several queries over one table) skip cloud storage. Databricks recommends it for most workloads and enables it on SSD worker types. Switching it off in code is usually a leftover from a cold-read benchmark.
- **Detect:** `spark.databricks.io.cache.enabled` set false through the CFG001 config-write matcher (conf.set, builder config / `map=`, `SparkConf().set`, SQL `SET`; a write set back later in the file is still reported, as a note).
- **FP:** Low: exact key and literal value. Benchmarks that want cold reads noqa it with the reason.
- **Ex:** `spark.conf.set("spark.databricks.io.cache.enabled", "false")` → remove it.

#### CFG011 `arrow-batch-size-extreme`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.execution.arrow.maxRecordsPerBatch=1000000 (default 10000): Arrow batches live in executor memory overhead, so big batches of wide rows get Python workers killed.` / `…=100: batches this small lose most of the vectorised speed-up.` (info)
- **Why:** Arrow batches are allocated outside the JVM heap, so the batch size multiplies straight into memory overhead on every pandas UDF, `applyInPandas`, `mapInPandas` and Arrow `toPandas` in the job. Too large shows up as exit 137 / "container killed" with no JVM error; 0 or negative removes the limit (a whole partition per batch). Too small spends the time crossing the JVM/Python boundary.
- **Detect:** `spark.sql.execution.arrow.maxRecordsPerBatch` written through the config-write matcher with a constant integer value: >= 100000 or <= 0 = warning, 1-499 = info (bands fixed for now). Non-integer or unknown values are skipped.
- **FP:** Low-med: narrow rows tolerate bigger batches, so a deliberate value can be noqa'd with the reason.
- **Ex:** `spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 1000000)` → keep the default (or lower it for wide rows) and size `spark.executor.memoryOverhead`.

#### CFG012 `file-partition-size-extreme`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.files.maxPartitionBytes set to 4 GB (default 128 MB): scan tasks this big spill to disk or run executors out of memory, and too few of them leave cores idle.` / `… set to 1 MB: partitions this small turn every stage into thousands of tiny tasks.` (info)
- **Why:** `spark.sql.files.maxPartitionBytes` sizes scan tasks and `spark.sql.adaptive.advisoryPartitionSizeInBytes` sizes post-shuffle partitions after AQE coalescing. Far above the default, each task holds gigabytes (spill, OOM, idle cores); far below, the job drowns in scheduling overhead and small output files. `spark.sql.files.openCostInBytes` <= 0 makes small files look free, so one task gets packed with thousands of them.
- **Detect:** the config-write matcher with Spark's byte-string parser (`"16m"`, `"1g"`, `134217728`, `"128MB"`): maxPartitionBytes > 1 GB or advisory size (also the Spark 3.0 name `spark.sql.adaptive.shuffle.targetPostShuffleInputSize`) > 2 GB = warning; either < 8 MB = info; openCostInBytes <= 0 = info. Non-constant or unparseable values are skipped; a write set back later in the file is a note.
- **FP:** Low-med: a deliberately small scan size for a CPU-heavy UDF stage is a known trick (hence info, noqa with the reason).
- **Ex:** `spark.conf.set("spark.sql.files.maxPartitionBytes", "4g")` → keep the default or a few hundred MB.

#### CFG013 `aqe-min-partition-num-set`
perf · info · on · T0 · none · new
- **Msg:** `spark.sql.adaptive.coalescePartitions.minPartitionNum=400 overrides AQE's minPartitionSize: AQE can no longer merge small post-shuffle partitions below this count.`
- **Why:** Databricks' AQE page says "Don't set this property": it overrides `coalescePartitions.minPartitionSize`, so a value copied from an old cluster keeps every small shuffle at hundreds or thousands of tiny tasks and small output files.
- **Detect:** `spark.sql.adaptive.coalescePartitions.minPartitionNum` (or the Spark 3.0 name `spark.sql.adaptive.minNumPostShufflePartitions`) set to any value through the config-write matcher; `unset` is quiet.
- **FP:** Low: exact key, rarely right.
- **Ex:** `spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", 400)` → remove it.

#### CFG014 `engine-acceleration-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `spark.sql.codegen.wholeStage=false turns off whole-stage code generation: every operator of every query runs row by row through virtual iterator calls instead of one compiled loop, multiplying CPU time for the whole session.` / `spark.sql.parquet.enableVectorizedReader=false makes every Parquet/Delta scan decode values row by row instead of in columnar batches, …`
- **Why:** Both switches are workarounds for one old error (the 64 KB generated-method limit, a reader type bug) that stay on for every later query of the session and multiply CPU time of scans and operators.
- **Detect:** `spark.sql.codegen.wholeStage`, `spark.sql.parquet.enableVectorizedReader` or `spark.sql.orc.enableVectorizedReader` set false through the config-write matcher (conf.set, builder config / `map=`, SparkConf, SQL `SET`, constant names); a write set back later in the file is info. Other codegen keys (`codegen.fallback`, `maxFields`) and the in-memory columnar reader key are not reported.
- **FP:** Low: exact keys and literal values; a genuine workaround deserves a set-back after the query or a noqa with the reason.
- **Ex:** `spark.conf.set("spark.sql.codegen.wholeStage", "false")` → remove it.

#### CFG015 `dynamic-pruning-disabled`
perf · warning · on · T0 · none · new
- **Msg:** `spark.databricks.optimizer.dynamicFilePruning=false turns off dynamic file pruning: joins and MERGEs stop skipping the Delta files of the big table that cannot match the small side, so they read every file.` / `spark.sql.optimizer.dynamicPartitionPruning.enabled=false turns off dynamic partition pruning: …`
- **Why:** Dynamic file/partition pruning pushes the small side's join keys into the scan of the big table at run time; without it a star-schema join or a MERGE scans the whole fact table. Databricks advises against disabling it except to compare plans.
- **Detect:** `spark.databricks.optimizer.dynamicFilePruning` or `spark.sql.optimizer.dynamicPartitionPruning.enabled` set false through the config-write matcher; a write set back (or unset) later in the file is info. The DFP size/file thresholds are not reported.
- **FP:** Low: exact keys and literal values.
- **Ex:** `spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", "false")` → remove it.

#### CFG016 `broadcast-timeout-raised`
perf · info · on · T0 · none · new
- **Msg:** `spark.sql.broadcastTimeout raised to 1 h (default 300 s): a broadcast that needs longer is usually far bigger than estimated or built by a slow query, and the longer wait keeps the cluster busy on it (AQE broadcast stages ignore this setting since Spark 3.2).`
- **Why:** Raising the timeout is the common answer to `Futures timed out after [300 seconds]`, but the timeout is a symptom: an oversized broadcast side (stale statistics, a forced hint) or a slow build query. A longer wait (or a negative value, which never times out) lets that broadcast hold the cluster longer instead of failing fast.
- **Detect:** `spark.sql.broadcastTimeout` written through the config-write matcher (conf.set, builder config / `map=`, SparkConf, SQL `SET`, constant names) with a constant duration above 300 seconds (plain seconds or Spark time strings `600s`, `10min`, `1h`) or a negative value; a write set back later in the file is a note. Non-constant and unparseable values are skipped.
- **FP:** Low-med: an intentionally small but slow-to-build relation on a non-AQE path is a legitimate reason (noqa with the reason).
- **Ex:** `spark.conf.set("spark.sql.broadcastTimeout", "3600")` → shrink/filter the broadcast side, drop the forced hint or `ANALYZE TABLE`; keep the default.

#### CFG017 `driver-result-limit-removed`
perf · warning · on · T0 · none · new
- **Msg:** `spark.driver.maxResultSize=0 removes the driver's result limit (default 1g): an oversized collect(), toPandas() or broadcast then crashes the driver with an out-of-memory error, losing the session while the cluster sits idle, instead of failing that one job cleanly.` / `spark.driver.maxResultSize raised to 64 GB (default 1g): …` (info)
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
- **Msg:** `spark.sql.optimizer.excludedRules turns off Catalyst optimizer rules (PushDownPredicates) for every query of this session: lost optimizations such as filter pushdown, column pruning or join reordering make queries read and shuffle more. Unknown rule names are silently ignored, so a name copied from another Spark version does nothing.`
- **Why:** The excluded-rules lists are diagnostic knobs. Left in after a one-off workaround they remove Catalyst (or AQE re-optimisation) rules for every query of the session; and because unknown names are ignored, a list carried across Spark versions may do nothing at all.
- **Detect:** shared config-write matcher (`conf.set`, builder `config` / `map=`, `SparkConf` set/setAll/setIfMissing, SQL `SET`, constant names) on `spark.sql.optimizer.excludedRules` or `spark.sql.adaptive.optimizer.excludedRules` with a constant, non-empty rule list (up to three short rule names shown). An empty value, reads, unsets and non-constant values are not reported; a later write of the key in the file adds a scoped note.
- **FP:** Low-med: genuine workarounds for optimizer bugs exist; the help asks to scope them and say why (noqa).
- **Ex:** `spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates")` → remove it, or set it back after the affected query.

#### CFG022 `delta-target-file-size-small`
perf · warning · on · T0 · none · new
- **Msg:** `Table property delta.targetFileSize = 8 MB is below 16 MB: OPTIMIZE, auto compaction and optimized writes then aim for files that small instead of autotuning (256 MB to 1 GB by table size), so every read opens many more files and the Delta log grows with every write.`
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
- **FP:** `format("console"|"memory")` debug sinks → ACT010 instead.
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
- **Detect:** `DataStreamReader.load()` (oracle Certain) whose reader chain has format `kafka` (`format("kafka")` or `load(format="kafka")`, case-insensitive) and no `maxOffsetsPerTrigger` / `maxBytesPerTrigger` in `option`/`options`/`load` keywords (shared reader option walker). Warning when `startingOffsets` is `earliest` or a JSON offsets map, info otherwise. Skipped when options come from an opaque dict or `**kwargs`, or the reader is configured by separate statements.
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
- **Msg:** `Column == None is always NULL (filter drops every row) — use .isNull() / .isNotNull().`
- **Why:** SQL three-valued logic. `col == None` becomes `col = NULL`, which is never true. A silent empty result.
- **Detect:** `Compare` with a `Column`-typed operand and the constant `None` (`==`, `!=`). `is None`/`is not None` on a Column is always False/True in Python, so it is flagged as well. Also `F.col("x") == F.lit(None)`.
- **Fix (safe):** `==None` → `.isNull()`, `!=None` → `.isNotNull()`.
- **Ex:** `df.filter(F.col("a") == None)` → `df.filter(F.col("a").isNull())`

#### COR002 `python-bool-op-on-column`
correctness · error · on · T1 · safe · new
- **Msg:** `'and'/'or'/'not' on Columns — use &, |, ~ (with parentheses).`
- **Why:** It raises `ValueError: Cannot convert column into bool` at runtime. For mixed operands such as `flag and F.col(...)`, Python short-circuits on the plain value and silently drops one side.
- **Detect:** `BoolOp(And|Or)` with ≥ 1 Column operand. `UnaryOp(Not)` on a Column.
- **Fix (safe):** Rewrite to `(a) & (b)` / `(a) | (b)` / `~(a)`, parenthesising each operand.
- **Ex:** `F.col("a")>1 and F.col("b")<2` → `(F.col("a")>1) & (F.col("b")<2)`

#### COR003 `bitwise-comparison-precedence`
correctness · error · on · T1 · safe · new
- **Msg:** `& / | bind tighter than comparisons: F.col('a') > 1 & F.col('b') < 2 parses as a > (1 & b) < 2.`
- **Why:** Python parses this as a chained comparison `a > (1 & b) and (1 & b) < 2`. On Columns that either raises "Cannot convert column into bool" or computes a bitwise AND of literals. Both are silent nonsense or runtime errors.
- **Detect:** A `Compare` node whose `comparators`/`left` include a `BinOp(BitAnd|BitOr)` and whose overall operands include a Column, i.e. the AST shape of an unparenthesised `cmp & cmp`.
- **Fix (safe):** Re-parenthesise from the source tokens: `(F.col("a") > 1) & (F.col("b") < 2)`.

#### COR007 `discarded-transformation`
correctness · error · on · T1 · none · new
- **Msg:** `Result of df.withColumn() is discarded — DataFrames are immutable; assign it.`
- **Why:** `df.withColumn(...)`, `df.dropDuplicates()` or `df.na.fill(0)` as a bare statement does nothing. The code "looks" right and silently drops the logic.
- **Detect:** An expression statement whose value is a call returning a `DataFrame`/`Column`/`GroupedData` from a transformation method (not an action, not `cache`/`persist`/`createOrReplaceTempView`/`unpersist`, which have effects).
- **Ex:** `df.dropDuplicates(["id"])` → `df = df.dropDuplicates(["id"])`

#### COR010 `union-by-position`
correctness · warning · on · T1 · unsafe · CY035, ruff#7272
- **Msg:** `union()/unionAll() matches columns by position — use unionByName().`
- **Why:** When the schemas are in a different order, values silently land in the wrong columns (and type-coerce).
- **Detect:** `DataFrame.union/unionAll(other)`. When both sides' `known_columns` are identical *and* in the same order → info.
- **Fix (unsafe):** → `unionByName(other)` (it raises on mismatched names, unlike `union`, so it is a behaviour change).

#### COR016 `regex-escape`
correctness · warning · on · T0 · safe · CY016 (widened)
- **Msg:** `Regex passed as a non-raw string: "\b" is a backspace, "\d" is an invalid escape — use r"...".`
- **Why:** `"\bword\b"` silently becomes a backspace character, so the regex never matches (a silent wrong result). Invalid escapes like `"\d"` are a SyntaxWarning in Python 3.12+ and will become errors.
- **Detect:** Any non-raw string literal with an invalid escape (all strings, as in cylint). Plus valid-but-suspicious escapes (`\b`, `\a`, `\f`, `\v`, `\0`) in arguments of `rlike`, `regexp_*`, `split`, `regexp`, or the `LIKE`/`RLIKE` text of `spark.sql`.
- **Fix (safe):** Convert to a raw string when the literal contains no intentional control chars.

### DBX — Databricks platform & compatibility

#### DBX001 `spark-context-access`
compat · warning (error on serverless/connect) · on · T1 · unsafe · Databricks access-mode & serverless limitations
- **Msg:** `spark.sparkContext / sc / sqlContext is unavailable on Spark Connect and serverless (and on standard access mode before DBR 14).`
- **Why:**
  - It blocks moving the workload to serverless or shared/standard clusters, which is often the cheaper option.
  - On standard access mode many `sc` functions stay blocked: `parallelize`, `textFile`, `setJobGroup`, `setLocalProperty`, `getConf`, `runJob`, `emptyRDD`, `range`, `union`, `stop`, …
- **Detect:** Attribute access `.sparkContext` on a SparkSession. Use of `SparkContext`-typed values (`sc`, `SparkContext.getOrCreate()`). `SQLContext(...)` / `sqlContext`.
- **Fix (unsafe) for common calls:**
  - `sc.setLogLevel(x)` → remove.
  - `sc.parallelize(rows)` → `spark.createDataFrame(rows)`.
  - `sc.broadcast(x)` → join with `F.broadcast(df)`.
  - `sc.getConf().get(k)` → `spark.conf.get(k)`.
  - `sc.applicationId` → `spark.conf.get("spark.app.id")`.
  - `sc.defaultParallelism` → no Connect equivalent (message only).

#### DBX002 `rdd-access`
compat · warning (error on serverless/connect) · on · T1 · unsafe · PA D002/D005, Databricks limitations
- **Msg:** `DataFrame.rdd is not supported on Spark Connect / serverless / standard access mode.`
- **Why:** The RDD API is unsupported there. It is also a performance smell (ACT011).
- **Detect:** `.rdd` on a DataFrame. `RDD`-typed values from any origin. Common replacements are given in the message.
- **Fix (unsafe):**
  - `df.rdd.isEmpty()` → `df.isEmpty()`.
  - `df.rdd.getNumPartitions()` → no direct replacement on Connect (suggest removing, or `F.spark_partition_id()` counting).
  - `df.rdd.map(lambda r: r[0]).collect()` → `[r[0] for r in df.collect()]`.

#### DBX003 `input-file-name`
compat · warning · on · T0 · unsafe · Databricks (deprecated on UC; DBR 13.3+)
- **Msg:** `input_file_name() is not supported on Unity Catalog standard/serverless — use the _metadata.file_path column.`
- **Why:** It errors on UC shared/standard compute and serverless, and is deprecated.
- **Detect:** `F.input_file_name()`, and the SQL `input_file_name()` in `spark.sql`/`expr` strings.
- **Fix (unsafe):** `F.input_file_name()` → `F.col("_metadata.file_path")`. Only valid for file-source DataFrames, hence unsafe.

#### DBX016 `list-tables-existence-check`
perf · info (warning in loops) · on · T1 · none · new
- **Msg:** `Existence check for one table by listing every table: the driver fetches the whole listing (one metastore round trip per page on Unity Catalog) to test a single name.`
- **Why:** `spark.catalog.listTables(db)` / `SHOW TABLES IN db` returns metadata for every table in the schema; on Unity Catalog that is paged metastore calls, slow on schemas with thousands of tables. Code that only wants to know whether ONE table exists (often inside a loop over table names) keeps the cluster idle while the driver waits. `spark.catalog.tableExists("cat.db.tbl")` / `databaseExists` (Spark >= 3.3, Databricks) is one lookup.
- **Detect:** a full listing — `Catalog.listTables(db)` / `listDatabases()` without a `pattern` argument (oracle: `Catalog` receiver), or `spark.sql("SHOW TABLES|SCHEMAS|DATABASES [IN|FROM x]")` with no pattern, optionally `.select(..)`, then `.collect()`/`.toLocalIterator()` — used inline as (1) the source of a comprehension/generator whose condition or element compares the row's name field (`.name`, `.tableName`, `.databaseName`, `.namespace`, `["tableName"]`, optionally `.lower()`) with `==`; (2) the right side of `in`/`not in` (the listing itself or a comprehension projecting the name); (3) a `for` loop over it whose body has `if <name> == x`; (4) `spark.sql("SHOW TABLES ...").filter/where(...)` comparing `tableName` with `==` / SQL `=`. Reported on the listing call; warning inside a loop, info otherwise. A listing bound to a name first (listed once, then many lookups) is not reported.
- **FP:** Low. Filtering the listing by other fields (`tableType == "VIEW"`, `isTemporary`), `!=`/`in <collection>` filters, pattern arguments and `SHOW TABLES LIKE` stay quiet.
- **Ex:** `if name in [t.name for t in spark.catalog.listTables("db")]:` → `if spark.catalog.tableExists(f"db.{name}"):`

#### DBX017 `pipeline-stream-fanout`
perf · info · on · T1 · none · new
- **Msg:** `<source> is streamed separately by N pipeline datasets/flows: <names and lines>. Each one is its own streaming query that re-reads every micro-batch, so the cluster is sized for all of them running at once.`
- **Why:** In a Lakeflow (DLT) pipeline every streaming table / append flow is a separate streaming query with its own checkpoint. When one ingestion table fans out to many outputs (often a `for` loop generating one `@dp.table` per region/type), each output reads every micro-batch of the source on its own, so N small queries run concurrently and the pipeline needs a cluster sized for N streams for little work. Databricks documents this for loops ("each generated flow or table processes the entire source dataset independently") and offers the ForEachBatch sink (`@dp.foreach_batch_sink`, Public Preview on the PREVIEW channel): one `append_flow` reads the source once and the handler writes every target per micro-batch (with `persist()` so the batch is not recomputed per write).
- **Detect:** functions decorated with `dlt`/`pyspark.pipelines` `table`, `streaming_table`, `materialized_view`, `view`, `temporary_view` or `append_flow` (qualified names, any alias) whose body streams a fixed source: `spark.readStream….table("x")` (oracle: `DataStreamReader`), `….load("path")`, Kafka-style `….option("subscribe", "t").load()`, or `dlt.read_stream("x")` / `dp.read_stream("x")` (`live.` prefix and case ignored). Readers are grouped by source; a def inside a `for` over a literal list counts once per element, a loop over anything else counts as reaching the threshold. Report on the first read when ≥ `pipeline-fanout-min-flows` (default 3; 0 disables) datasets stream the same source. Sources that change per iteration (f-strings) and nested handler defs are ignored; batch reads (`spark.read.table`) are not counted (the ForEachBatch sink is for streaming flows).
- **FP:** Low-med. Some fan-outs are deliberate (targets need expectations, lineage or full-refresh handling, which a ForEachBatch sink gives up; writes across targets are not atomic) — suppress with `# noqa: DBX017`. SQL pipeline files are not linted.
- **Ex:** `for r in REGIONS: @dp.table(name=f"orders_{r}") def t(): return spark.readStream.table("raw_orders").filter(...)` → one `@dp.foreach_batch_sink(name="orders_by_region")` whose handler persists `batch_df` and writes `batch_df.filter(...)` to each region table (`txnAppId`/`txnVersion` options for idempotent retries), fed by a single `@dp.append_flow(target="orders_by_region")` reading `raw_orders`.

### PDS — pandas-on-Spark

#### PDS005 `pandas-on-spark-ops-on-diff-frames`
perf · warning · on · T0 · none · new
- **Msg:** `compute.ops_on_diff_frames=True: every operation between two different pandas-on-Spark frames silently becomes a join (a shuffle per expression).`
- **Why:** By default pandas-on-Spark refuses arithmetic/assignment between two different frames because it needs a join on the index. Turning the option on removes that guard, so `psdf1["a"] + psdf2["b"]` or `psdf1["x"] = psdf2["y"]` each become a hidden join with a shuffle. The best-practices guide says to avoid it.
- **Detect:** `ps.set_option("compute.ops_on_diff_frames", True)` (also `pyspark.pandas.config.set_option`, `value=`), `ps.option_context(..., "compute.ops_on_diff_frames", True, ...)`, `ps.options.compute.ops_on_diff_frames = True`, and `spark.conf.set` / `SparkSession.builder.config` with the conf key `pandas_on_Spark.compute.ops_on_diff_frames` (the prefix pandas-on-Spark uses to store options) set to `True`/`"true"`. Names are matched by qualified name, so plain `pd.set_option` is never reported; non-literal values are skipped.
- **FP:** Low (exact option name). A deliberate, one-off combine on small frames can be suppressed with `# noqa: PDS005`.
- **Ex:** `ps.set_option("compute.ops_on_diff_frames", True); psdf1["a"] + psdf2["b"]` → `psdf1.merge(psdf2, on="id")` then operate within one frame.

#### PDS006 `pandas-on-spark-single-partition-op`
perf · info · on · T1 · none · new
- **Msg:** `<op>() on an un-grouped pandas-on-Spark frame runs over a window with no partition: all rows move to a single partition.`
- **Why:** On an un-grouped `ps.DataFrame`/`ps.Series`, `rank`, `shift`, `diff`, `pct_change` and `cumsum/cumprod/cummax/cummin` are built on a Spark Window with no partition spec, which "moves all data into a single partition in a single machine" (an OOM or one-core stage). This is the pandas-on-Spark twin of PRF005.
- **Detect:** One of those methods called on a receiver the oracle types as pandas-on-Spark with Certain confidence: `ps.DataFrame/Series(...)`, `ps.read_*`, `ps.from_pandas`, `ps.concat`, `ps.sql`, `df.pandas_api()`/`to_pandas_on_spark()`, `ps.DataFrame`/`ps.Series` annotations, and values derived from them by column selection (`psdf["x"]`, `psdf[["a","b"]]`, boolean masks), element-wise arithmetic/comparison with numbers or other ps frames, and frame-preserving methods (`sort_values`, `fillna`, `assign`, ...). `groupby(..)`, `rolling`/`expanding`, reductions and integer/label lookups are not frames, so grouped ops are never reported. In a chain (`s.shift(1).cumsum()`) only the first op is reported.
- **FP:** Medium: harmless on small frames (suppress with `# noqa: PDS006`). Plain pandas and Spark DataFrames are never reported.
- **Ex:** `psdf["amount"].cumsum()` → `psdf.groupby("user")["amount"].cumsum()`, or a Spark `Window.partitionBy("user").orderBy("ts")`.

### STY — Style & naming (all off in `default`; STY001–003 on in `cylint-compat`)

#### STY001 `functions-alias`
style · info · T0 · safe · CY037, astrumu C4003/C4004, ruff#7272
- **Msg:** `Import pyspark.sql.functions as F (configurable: F | sf).`
- **Detect:** `from pyspark.sql import functions` without an alias, an alias not in `functions-aliases` (default `["F","sf"]`; the Spark docs use `sf`, per the ruff thread), or `import pyspark.sql.functions as f`. Same for `types` → `T`.
- **Fix (safe):** Rename the alias and its references.

#### STY002 `join-missing-how`
style · info · T1 · safe · CY010, palantir
- **Msg:** `join() without explicit how= — state the join type.`
- **Detect:** A DataFrame `join` with no 3rd positional arg or `how=`.
- **Fix (safe):** Add `how="inner"`.

#### STY003 `redundant-drop-before-select`
style · info · T1 · safe · CY032, ruff#7272
- **Msg:** `drop() before select() of explicit columns is redundant.`
- **Detect:** `.drop(...)` immediately followed by `.select(<explicit cols>)` in a chain or linear dataflow (not `select("*")` / `select(df.columns)`).
- **Fix (safe):** Remove the `drop`. If `select` references a dropped column it would fail anyway, so skip the fix in that case.
