# sparklint — design

A fast, pip-installable, Rust-based linter for PySpark code, with Spark-aware type
inference and Databricks notebook support.

`docs/DEVELOPMENT.md` covers building, testing and packaging; `docs/RULES_CATALOG.md`
describes every rule.

## 1. Why this exists

- Ruff will not ship PySpark rules: they "would need type information to determine if a
  variable is a DataFrame, RDD or transformation reliably" (astral-sh/ruff#7272, open
  since 2023). ty may one day expose that, but not as a plugin API for third-party rules.
- cylint is pure-Python and name-heuristic: a DataFrame is
  "anything assigned from `spark.read/sql/table`" in the same scope. It misses DataFrames
  passed as parameters, returned from helpers, built in other modules, chained through
  `transform()`, and it flags look-alike calls on non-Spark objects (e.g. `list.count()`,
  pandas `.collect`). Rules are file-local.
- Code reviews of Spark jobs are mostly manual. A linter that finds the expensive
  patterns across a whole `/Workspace` export or repo in seconds turns that review into a
  worklist.

## 2. Goals / non-goals

Goals
1. `pip install sparklint` → a single native binary (maturin `bin` wheel, like ruff/ty).
   No Spark, no JVM, no Python deps needed to lint.
2. Spark-aware types without running code: know which expressions are SparkSession,
   DataFrame, Column, GroupedData, Window/WindowSpec, RDD, DataFrameReader/Writer(V2),
   DataStreamReader/Writer, StreamingQuery, pandas-on-Spark DataFrame, and plain pandas.
3. Inputs: `.py`, Databricks source-format notebooks (`# Databricks notebook source`,
   `# COMMAND ----------`, `# MAGIC %sql` …), `.ipynb`; `spark.sql("...")` strings and
   `%sql` cells get light SQL checks.
4. ruff-like UX: `sparklint check [paths]`, `--select/--ignore/--extend-select`, rule
   codes, `# noqa: ACT002` and `# sparklint: ignore[...]`, `[tool.sparklint]` in
   `pyproject.toml` / `sparklint.toml`, `--fix` for safe fixes, `--output-format
   text|json|github|sarif`, `sparklint rule ACT002` explains a rule, exit codes.
5. Speed: parallel per-file (rayon); a 10k-file workspace in a few seconds.
6. Cross-file: a project index of function summaries ("returns DataFrame", "parameter 0
   is used as DataFrame") so helpers defined in another module are understood.

Non-goals (for now)
- Full Python type checking (that's ty/pyright). We only model the Spark universe and
  enough Python (assignments, calls, returns, loops, comprehensions, attributes on self)
  to place Spark types.
- An LSP server (designed for: diagnostics are position-based and the engine is
  file-pure).

## 3. Architecture

Cargo workspace:

```
crates/
  sparklint_source     source loading: .py, Databricks notebook source, .ipynb (ruff_notebook),
                       cell map (cell index <-> line ranges), %magic handling, SQL cells
  sparklint_semantic   scopes/bindings + Spark type inference (the "oracle")
  sparklint_linter     rule registry, Checker (AST visitor), diagnostics, fixes, noqa,
                       config, rules/*.rs
  sparklint            CLI binary (clap): check, rule, rules, version
python/sparklint/      thin Python package (`python -m sparklint`, find_sparklint_bin())
spec/pyspark_api.json  generated PySpark API model (see 3.3), compiled in via include_str!
scripts/gen_api.py     generator (runs against an installed pyspark)
```

Parser/AST: `ruff_python_parser` + `ruff_python_ast` + `ruff_text_size` +
`ruff_source_file` + `ruff_notebook`, pinned with `=` to one release (0.0.21 at time of
writing). Astral publishes these crates to crates.io. It is the same parser ruff and ty
use: it recovers from errors, it is fast, and it handles the full Python 3.14 grammar. The
API is unstable, so we pin exactly and bump on purpose.

### 3.1 Types (the lattice)

```
SparkType = SparkSession | SparkContext | SQLContext
          | DataFrame{streaming: bool} | Column | GroupedData | PandasGroupedOps
          | Window (class) | WindowSpec | RDD | Row | StructType/DataType
          | DataFrameReader | DataFrameWriter | DataFrameWriterV2 | MergeIntoWriter
          | DataStreamReader | DataStreamWriter | StreamingQuery
          | DataFrameNaFunctions | DataFrameStatFunctions | Catalog | RuntimeConfig
          | FunctionsModule (pyspark.sql.functions) | TypesModule | UdfFunction
          | PandasOnSparkFrame | PandasOnSparkSeries | PandasFrame | DeltaTable
          | Py(builtin: int/str/list/dict/bool/None) | Unknown
```
Types are a small `enum` + flags; a value can be a *union* (bit set) for joins of
branches (`if x: df = a else: df = b`). Every inferred type carries a *confidence*
(`Certain` from annotation/known origin, `Likely` from name heuristics) so rules can
require certainty or accept heuristics (`--heuristics` / per-rule).

### 3.2 Inference (sparklint_semantic)

Per module, flow-sensitive-lite, single forward pass per scope with loop re-visit:
1. **Roots**: `SparkSession.builder...getOrCreate()`, `SparkSession.active()`, the
   Databricks notebook globals `spark`, `sc`, `sqlContext`, `dbutils`, `display`
   (enabled automatically for notebooks; `known-globals` config for jobs), `DeltaTable.forName/
   forPath`, `pyspark.pandas` constructors, `pd.*`.
2. **Imports**: resolve `from pyspark.sql import functions as F` / `import
   pyspark.sql.functions as sf` / `from pyspark.sql.functions import col, udf` /
   `from pyspark.sql import DataFrame, SparkSession, Window` to canonical qualified names
   (`pyspark.sql.functions.col`). Rules match on qualified names, never on the alias.
3. **Annotations**: parameters/returns/variables annotated `DataFrame`, `"DataFrame"`,
   `Optional[DataFrame]`, `list[DataFrame]`, `Column`, `SparkSession` … are `Certain`.
4. **Method model**: receiver type + method name → return type, from the generated
   `spec/pyspark_api.json` (e.g. `DataFrame.groupBy -> GroupedData`, `GroupedData.agg ->
   DataFrame`, `DataFrame.write -> DataFrameWriter` (property), `DataFrame.count -> int`
   + `action`). Attribute access `df.colname` / `df["x"]` → Column.
5. **Assignments**: `x = <expr>` binds `type(expr)`; tuple unpacking of
   `randomSplit`; `with`-targets; walrus; augmented assignment keeps type; loops bind
   loop targets (`for row in df.collect()` → Row; `for c in df.columns` → str).
6. **Functions**: a summary per function: return type (join of `return` exprs), and
   *parameter usage* evidence (`p.select(...)`, `p.groupBy` ⇒ parameter is a DataFrame,
   `Likely`). Calls to a summarised function propagate the return type. `df.transform(f)`
   → f's return type (DataFrame by contract). `functools.reduce(DataFrame.unionByName,
   dfs)` → DataFrame.
7. **Project index (cross-file)**: phase A parses all files in parallel and builds
   summaries keyed by qualified name (`pkg.mod.func`, `pkg.mod.Class.method`); phase B lints
   with the index available, resolving `from pkg.mod import func`. One level of fixpoint is
   enough in practice (summaries computed in dependency order where possible, else 2 rounds).
8. **Heuristic fallback** (confidence `Likely`): names matching `df`, `*_df`, `df_*`,
   `sdf`; receivers of unmistakable DataFrame-only methods (`withColumn`, `groupBy`,
   `withColumnRenamed`, `unionByName`, `toPandas`, `createOrReplaceTempView`) are
   DataFrames. Ambiguous methods (`count`, `filter`, `collect`, `first`, `join`, `select`,
   `show`, `limit`, `distinct`) are only treated as Spark when the receiver type is known —
   this is what kills the cylint false positives on pandas/lists/strings.

The oracle exposes `ty_of(expr) -> (SparkType, Confidence)`, `qualified_name(expr)`,
`spark_call(expr)`, `binding_uses(name)` (def-use for cache/unpersist/multiple-action
rules) and `in_loop(node)`.

`spark_call(expr)` / `spark_call_named(expr, &["count", ...])` answer "which Spark
method is this call": `Some(SparkCall { call, receiver, method, receiver_ty })` only when
the receiver is in the Spark universe (`Ty::is_spark`), with `qualname()`
(`pyspark.sql.DataFrame.count`), `is(Ty::DataFrame, "count")`, `confidence()` /
`is_certain()`, `bounded()` and `has_no_args()`. The invariant of point 8 (ambiguous
methods only on a known Spark receiver; never on lists, pandas, Py4J handles, unknown
values) lives in that one function (`sparklint_semantic/src/spark_call.rs`, with unit
tests); rules match `sc.is(Ty::X, "m")` instead of string-matching the attribute and
probing the receiver. Confidence stays visible: rules that need proof check
`is_certain()`, others accept `Likely` receivers. Chain walkers that inspect non-Spark
values (pandas frames from `toPandas`, collected lists) or purely syntactic builder
chains still use `rules::helpers::method_call`.

Reader/writer settings live in ONE place, `rules::io_settings::IoSettings`: given a
read/write terminal call (`load`, `csv`, `jdbc`, `save`, `saveAsTable`, `start`,
`toTable` …) it answers `format()`, `option(key)` / `has` / `text` / `flag`,
`has_schema()`, `target()`, `mode()`, `partition_by()` and `complete()`. It walks the
builder chain (through stored builders), reads `options(**{..})` and single-use dict
literals, the terminal's keyword and positional arguments (`load(path, format,
schema)`, jdbc arguments and its `properties` dict) with Spark's precedence (terminal
first, then the closest `.option`), and marks the settings incomplete for opaque
`**kwargs`, non-literal keys or a stored builder configured by a separate statement.
IO rules (IOD001/002/006/015/016, CFG008 jdbc, STM001, STM010, DBX017, the write rules)
ask it instead of walking chains; rules that need an option to be *absent* check
`complete()` first.

Pluggable later: a `TypeOracle` trait so a ty-backed oracle (real inference against
installed pyspark stubs) can be swapped in once ty's semantic crate is stable enough.

### 3.3 PySpark API model (spec/pyspark_api.json)

Generated by `scripts/gen_api.py` from an installed pyspark (4.x, ships `py.typed`):
introspects classes (`DataFrame`, `Column`, `GroupedData`, `SparkSession`,
`DataFrameReader`, `DataFrameWriter`, `DataFrameWriterV2`, `DataStreamReader/Writer`,
`StreamingQuery`, `Window`, `WindowSpec`, `RDD`, `SparkContext`, `Catalog`,
`DataFrameNaFunctions/StatFunctions`, `pyspark.pandas.DataFrame/Series`) and the
`pyspark.sql.functions` module; records method → return annotation, properties,
parameters and deprecation notes (docstring `.. deprecated::`). A hand-written
overlay (in code: `sparklint_semantic::api::apply_overlay`) adds semantics the annotations don't carry: *action* vs
*transformation*, *wide* (shuffle) vs *narrow*, driver-collecting, Spark-Connect-unsupported,
UC-shared-mode-unsupported, deprecated-since. The JSON is checked in, so building needs no
pyspark.

### 3.4 Linter engine

- `Checker` walks the AST once per file with the oracle; rules subscribe to node kinds
  (`Call`, `Assign`, `For`, `Compare`, `BoolOp`, `FunctionDef`, `Module` end…) — ruff's
  model. Metadata (code, name, category, default severity, default-enabled, fix
  availability, docs) comes from `spec/rules.json`.
- The rule table (`rules/mod.rs`): each rule module exports `pub const RULES: &[Rule]`,
  one entry per rule — its code plus the hooks it uses (`init` / `stmt` / `call` / `expr`
  / `finish`, plain `fn` pointers); `rules::TABLE` lists the modules. Everything else is
  derived: the registry's `implemented` flag, the enabled check (done once per file when
  the checker collects the enabled rules' hooks) and the dispatch. Shared def-use
  analyses (count variables, cache candidates, frame flow) are built in `rules::init` for
  the rules that read them. Rules stay file-pure; per-file helper state lives in
  `rules::State`.
- **Adding a rule**: (1) catalog row in RULES_CATALOG.md + `python3 scripts/gen_rules.py >
  spec/rules.json`; (2) hook function(s) in a rule module and one
  `Rule::new("CODE").call(hook)` line in its `RULES` (a new module also needs `pub mod`
  and a `TABLE` line); (3) `tests/fixtures/<CODE>/` with a positive and a pure-negative
  file. Tests check that every table entry is in the catalog, appears once and has a
  fixture dir; rule counts in tests are derived from `spec/rules.json`, not literals.
- Rule overlap (one finding per site, under the most specific rule): rules never ask
  whether another rule is enabled. A finding may carry a `site` (the `.collect()` /
  `.toPandas()` call, the loop, the `.cache()` call; default: its range) and
  `overlap::PRECEDENCE` lists `(higher, lower)` pairs (ACT009 > ACT002/ACT004, ACT004 >
  ACT002/ACT003, PRF001 > PRF002, PRF008 > PRF009, IOD003 > PRF010). After all rules ran
  for a file, `overlap::resolve` drops a lower finding whose site carries a higher one
  (before `noqa`, so a suppressed higher finding still wins). With the higher rule
  disabled the lower one fires. The fixture harness's preset pass lints every fixture
  with `ALL` and the default preset and checks exactly this; `<CODE>/overlap.py`
  fixtures hold the cases where `<CODE>` must lose.
- Rule codes: a 3-letter family prefix + 3 digits (`ACT002`, `PRF001`, `COR001` …;
  families, tags, presets and the cylint mapping are in RULES_CATALOG.md §1 and §4). Selection
  supports prefixes (`PRF` = whole family, `PRF0` …) plus `tag:<name>` and presets, like ruff.
- Diagnostics: code, message, range (file:line:col, mapped back to notebook cell),
  severity, optional `Fix { edits, applicability: Safe|Unsafe }`, optional `help`.
- Suppression: `# noqa`, `# noqa: ACT002,PRF001`, `# sparklint: ignore[ACT002]` per line,
  `# sparklint: file-ignore[...]`, cylint-compatible `# cy:ignore CY001` (mapped via
  RULES_CATALOG §4).
- Config: `[tool.sparklint]` in `pyproject.toml` or `sparklint.toml` (closest wins):
  `select`, `ignore`, `extend-select`, `per-file-ignores`, `exclude`, `severity` overrides,
  `known-spark-globals`, `notebook = auto|on|off`, `heuristics = true`, `spark-version =
  "3.5"` (gates version-specific fixes) and per-rule thresholds (DEVELOPMENT.md lists
  them all).
- Output formats: text (ruff-style with code frame), json, github annotations, sarif
  (GitHub code scanning / Azure DevOps). Exit code: 0 clean, 1 findings, 2 error.
- Fixes: text edits; `--fix` applies Safe ones, `--unsafe-fixes` the rest; idempotent,
  re-parse after applying, abort a file's fixes on parse error.

### 3.5 SQL inside Python

`spark.sql(<literal or f-string>)` and `%sql` cells: tokenize (not a full SQL parser)
for a few high-value checks (`SELECT *` from a table, `ORDER BY` without `LIMIT`
in a CTAS/INSERT, `CROSS JOIN`, missing `WHERE` on `DELETE/UPDATE`, f-string SQL injection
of untrusted variables).

All SQL text goes through one module, `sparklint_semantic::sql` (the oracle needs it
too). `model.spark_sql(expr)` / `model.sql_query(call)` take a `sql()` call's query once:
the argument (positional or `sqlQuery=`), a literal or f-string (holes become `{}`) or a
name bound once to one (`model.resolve_text`). `SqlText::new(text)` (also for `F.expr` /
filter strings) tokenizes it once, dropping comments and masking string literals and
quoted identifiers, and answers the questions rules ask: `keyword()` (statement kind),
`set_pair()` (`SET k = v`), `properties("OPTIONS" | "TBLPROPERTIES")`, `has_keyword` /
`calls(fn)` outside strings, `trailing_limit()` / `single_row()` (oracle `bounded`), and
the plan probes (`not_in_subquery`, `cube_width`, `order_by_rand`, `select_star`,
`is_expensive`, join-condition `cond()` / `conjuncts()`). Rules never scan raw SQL text
themselves; a new SQL question becomes a `SqlText` method with table tests.

Spark config writes likewise have one matcher, `rules/conf.rs`: `writes(call)` (conf
set/unset, builder `config` incl. maps, `SparkConf().set/setIfMissing/setAll`, SQL
`SET`; constant keys followed through names) and `file_sets(key)`. CFG003 and STM001's
session-checkpoint check use it directly. Config-*value* rules
(CFG001/002/007-021, STM011, IOD019) are rows, not hooks: `Rule::new("CODE").conf(&Conf::new(
KEYS, verdict))`, optionally `.table(PROPS, table_verdict)` for `spark.sql`
`TBLPROPERTIES (..)`. Keys are listed with their usual casing (matched
case-insensitively; the verdict gets that display name in `Setting::key`), and a verdict
is a plain `fn(&Setting) -> Option<Finding>` (message, help, severity). The module runs
`writes()` once per call for every enabled row, dispatches per key, records each write,
downgrades a flagged write that a later write of the same key undoes to info ("set back
later"), judges table properties and reports. A rule with another shape keeps a normal
hook on the same `Rule` (CFG008's JDBC reader option, IOD019's writer option).

## 4. Distribution

- maturin `bindings = "bin"` wheel per platform (manylinux x86_64/aarch64, macOS, Windows),
  `pip install sparklint` / `uvx sparklint`; `python -m sparklint` runs the same binary.
- pre-commit hook, GitHub Action snippet; on Databricks, run it as a job task against a
  `/Workspace` export (`databricks workspace export-dir`).

## 5. Implementation notes

- Rule metadata for every catalog rule is generated into `spec/rules.json`
  (`scripts/gen_rules.py`) and compiled in. A catalog rule without an implementation is
  registered as `planned`: `sparklint rules` lists it and it can be selected, but it never
  runs. SYN001 is reported for files that do not parse (no rules run on them).
- Every cylint rule (CY001-CY038) has its mapped rule running.
- Dataflow for the def-use rules: the semantic model records `if`/`match` arms
  (`branch_path`, `mutually_exclusive`, `max_path_weight`) so counts are per execution
  path; the linter classifies every load of a DataFrame binding by the chain it heads
  (action / derived binding / cache / unpersist / escape, `rules::flow`). PRF006, PRF009
  and PRF022 are computed from those records at the end of the file.
- Oracle facts: aggregate-only SQL without `GROUP BY`, `DESCRIBE DETAIL` and
  aggregate-only `select()` are `bounded(1)`; Py4J handles (`JavaObject` annotations,
  `x._j…` receivers) are never Spark values.
- Fixture harness: `<name>.py.fixed` snapshots the safe fixes, `<name>.py.unsafe-fixed`
  the safe + unsafe fixes.
- Closures: a deferred nested function body resolves an enclosing-scope name to *every*
  binding of that name in the enclosing scope when no binding reaches the end of that
  scope (for example an import inside an `except` handler that returns).
- `spark.sql(<literal, f-string or a name bound to one>)` whose query ends in a
  top-level `LIMIT n` (outside comments/strings) is `bounded(n)`.
- Not yet: junit output, SQL-cell checks, `small-tables` / `log-metrics-allowed` options,
  fixes written back into `.ipynb`, a language server.
