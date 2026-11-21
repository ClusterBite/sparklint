# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor versions may change rules and
output).

## [Unreleased]

- IOD001 / IOD002: a reader argument passed as `None` (`jdbc(url, t, column=None)`, `csv(p, inferSchema=None)`) now counts as not given, as in pyspark: the one-connection JDBC read is reported and the CSV read gets the all-strings note.
- IOD003 now reports a write whose frame ends in one partition on every `if`/`else` path, also when the branches use different steps (`coalesce(1)` / `repartition(1)`, or a `select` after the step on one branch).
- RDDs now carry the same origin facts as DataFrames in the type checker: `sc.parallelize([...])` over a literal list (also `c=`, or a name holding a literal list on every path) is driver-built data with a row bound, kept through RDD steps, by `collect()` rows and by `toDF()`; `df.rdd` keeps the frame's facts. Visible changes: IOD001 / IOD016 no longer report reads over the `.rdd` of a frame that fits the driver (`spark.createDataFrame([..]).rdd.map(..)`, `df.limit(100).rdd`) or over a literal `parallelize` followed by RDD steps, and now report `sc.parallelize(rows)` when `rows` is a literal filled later (`rows = []` + `rows.append(..)`); IOD016 gives the read-the-files help for file text kept through rows-only steps (`sc.textFile(p).filter(..)`, a union of two `textFile`s) and for `wholeTextFiles(..).map(..)` picking the contents in other spellings (`lambda p: p[-1]`, `operator.itemgetter(1)`, a name bound to the lambda); ACT004 / ACT010 no longer report iterating the `collect()` of, or `show()` on, a literal `parallelize` (as for `createDataFrame([..])`).

- IOD001 and IOD016 treat `schema=None` (also a positional `None` or a name bound to `None`) as no schema, as pyspark does, so such reads are now reported. IOD001 no longer reports reads over a few literal strings (`spark.read.json(sc.parallelize(['{"a": 1}']))`). IOD016 gives the read-the-files help (with `multiLine=True`) for `sc.wholeTextFiles(..).values()`.
- IOD015 now reads a path given as a `path` option (`format("csv").option("path", "x.csv.gz").load()`) and the streaming `readStream.text(path=..)` keyword (it was looked up as `paths=`, the batch reader's spelling, so such reads were missed).
- IOD004 now reports `insertInto()` after `partitionBy()` / `bucketBy()` as an error (Spark rejects that write), reads `mode(saveMode=..)` and an `overwrite` flag held in a constant, and treats `overwrite=None` like a missing flag. IOD006 reads the same forms.
- Per-iteration questions share one answer to "what does this expression depend on?" (loop variables, names rebound in the loop, parameters). Visible changes: PRF001 / PRF002 judge the loop that repeats the statement, so a loop that returns or breaks right after its `withColumn` chain (it runs once) is no longer reported; ACT005 / IOD006 no longer treat a list, dict or set literal that is filled in place later (`rows = []` + `rows.append(..)`, `d[k] = v`) as a small literal loop (warning instead of info); ACT005 counts a helper argument computed from the loop variable in the body (`name = f"db.{t}"; load(name).count()`) as a different source per item (info), and IOD006 reports an append whose target is rebound to the same constant on every iteration.
- Constants are read one way everywhere: reader/writer options, formats, option keys and save modes held in a name (`MODE = "append"; df.write.mode(MODE)`, `option("inferSchema", INFER)`) now count like literals (IOD001, IOD002, IOD004, IOD006, IOD015, IOD016 and the other reader/writer rules), a `None` mode or format is treated as not given as pyspark does (`.mode(None)` and `saveAsTable(t, mode=None)` keep the mode set before), PDS005 reads `ps.set_option(.., FLAG)` through names, and `take(N)` / `head(N)` / `limit(N)` with a constant `N` bound the rows.
- PRF010 and IOD003 follow a frame bound on several paths (`if ..: out = df.coalesce(1) else: out = df.coalesce(1)`) when every path has the same steps below the write, and read the partition count alike: PRF010 now treats `repartition(N)` / `repartition(numPartitions=N)` with a constant `N` of 100 or more as raising parallelism (not reported), and IOD003 reads a `hint(NAME, 1)` name held in a constant.
- IOD004 and IOD006 ask one answer to "how does this write land?": the `insertInto` overwrite flag is read by Python truthiness as pyspark does (`insertInto(t, "false")` overwrites), `insertInto` after `.mode("error")` / `.mode("ignore")` appends (Spark appends for every mode but overwrite), and a mode set on a writer passed in as a parameter is read.
- Names are followed back to their values one way everywhere (every binding that reaches the name, up to four names in a row, assignments / annotated assignments / walrus / literal unpacking only). Visible changes: Spark settings applied in a loop over a dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`), `builder.config(map=SETTINGS)` and `SparkConf().setAll(PAIRS)` now see a dict bound in an if/else (both branches' settings), a dict bound through another name, a set of pairs and `setAll` over a name or `d.items()` (CFG rules); ACT005 / IOD006 treat a loop over a short constant list as a small driver loop (info) also when the list is annotated with a generic type (`TABLES: dict[str, int] = {..}`) or bound in a chained assignment (`A = B = [..]`); name chains in other rules (explode / write-chain walkers, SparkConf receivers, join conditions) follow up to four names instead of three.
- PRF006, PRF010 and UDF014 share one answer to "does this column expression call a Python UDF?": PRF006 and PRF010 now also see UDFs registered with `spark.udf.register` and called from SQL text (`F.expr("my_udf(x)")`, `selectExpr("my_udf(x) AS y")`, a filter string, `spark.sql(..)`) and UDFs held in a Column variable (`c = my_udf(F.col("x"))` … `withColumn("y", c)`): PRF006 counts such lineage as expensive, and PRF010 no longer reports a `repartition()` that spreads such a UDF step over more tasks. UDF014 follows a Column variable inside a join condition (`same = fuzzy(a.x, b.y)`; `a.join(b, same == F.lit(True))`).
- SQL conditions and query shapes are read from one parse of the SQL text (AND / OR / NOT over comparisons). Visible changes: PRF006 no longer counts a `spark.sql(..)` query as expensive because of look-alike text (`INSERT OVERWRITE`, a column named `window` or `distinct_n`, `ZORDER BY`, `IS NOT DISTINCT FROM`), and does count `GROUP BY` / `ORDER BY` split over lines; for PRF004 / PRF032 / UDF014 join conditions in SQL text, `x BETWEEN a AND b` is one condition, `RLIKE` / `ILIKE` are non-equi comparisons, and an `=` inside a function call (`coalesce(a.k = b.k, false)`) is no longer read as a join key.
- **Changes suppression semantics (minor-version change):** suppression comments and the configuration now name rules with one grammar (codes, prefixes, rule names, `tag:x`, presets, `ALL`, cylint codes). A token in a comment that names no rule no longer silences every rule: `# noqa: collect-without-limit` (a mistyped name), `# noqa: CY999` or `# sparklint: file-ignore[tag:nope]` now suppress nothing, and `sparklint check` prints a `warning:` with the file and line. Rule names, tags and presets now work in comments (`# noqa: collect-unbounded`, `# sparklint: ignore[tag:cost]`), `# noqa: ALL` silences every rule (it silenced none), and a bare `# noqa` still silences the whole line. Other linters' codes in `# noqa` (`E501`) are skipped quietly. In the configuration, cylint codes are accepted (`per-file-ignores = ["CY001"]`) and `tag:<name>` with an unknown tag is now an error.
- COR016: the unsafe "make it a raw string" fix for a regex escape is no longer offered when the pattern continues over a line with a backslash (the raw string would keep the backslash and the line break); both COR016 fixes now share one answer to "can this literal take an `r` prefix?".
- ACT006 now also reports `df.count()` tested in a comprehension `if` (`[r for r in rows if df.count()]`), a `match` guard or through a walrus (`if (n := df.count()):`, reported without a fix since the name keeps the count), and a held count read only in a comprehension `if` or as a non-last `and` / `or` operand; COR002 reports an `and` / `or` chain ending in a Column when it is a `match` guard or tested through a walrus. Both rules now share one answer to "does Python test this value as a bool?".
- COR003 no longer reports an `&` / `|` operand the author put in parentheses when a comment or line break sits inside them (`a > (x & y  # note` with `)` on the next line); such groupings were treated as unparenthesised.
- Fixes: one fix whose result would not parse no longer cancels every other fix of the same `--fix` round (only that fix is skipped); a receiver that spans lines inside its own parentheses (`(df\n    .filter(..)).count() == 0`) keeps them in the ACT006 / ACT009 / ACT011 / COR001 / DBX002 fix text instead of producing code that does not parse; the COR002 fix no longer wraps a plain literal operand in parentheses.
- COR010 reads column order from a new oracle fact: a union is no longer reported when one side was derived from the other by set operations (`intersect`, `subtract`, `exceptAll`), `dropna`, `fillna`, `replace` or `sampleBy`, through a name bound on several paths that all agree (if / else, a loop of filters) or an annotated assignment; `a.unionByName(b, allowMissingColumns=True).union(a)` is now reported (the first union adds `b`'s columns).
- IOD003 no longer reports a `coalesce(1)` / `repartition(1)` that a later set operation (`intersect`, `subtract`, `exceptAll`), `drop_duplicates` or `lateralJoin` re-partitions before the write; every shuffle or combine of the new transformation table now ends the search, not a list of its own.
- DBX001: a `sparkContext` / `sc` / `SparkContext(..)` access inside a `try:` whose handler catches the unavailable-context error and falls back (or in a `pytest.raises` block) is no longer reported.
- COR016 now reports `\1`-style group references in a non-raw regex or `re.sub` replacement (`re.sub("(a)", "\1", s)` inserts the character `\x01`), reads keyword arguments (`re.compile(pattern="\bx")`, `F.regexp_replace(s, pattern=..)`) and f-string patterns, and no longer reports `\a`, `\f`, `\0` (or `\v` in Python `re`), which the regex engine reads the same way raw or not. Messages have help lines.
- COR010 (`union` by position) no longer reports unions whose sides have the same column order by construction: both sides selecting the same column list (`a.select(cols).union(b.select(cols))`), the same frame after filters / limits / sorts (`df.filter(c1).union(df.filter(c2))`), a semi / anti join's left side; names bound once are followed. Same literal `select(..)` lists are no longer reported at info. Help lines added.
- COR003 no longer reports one comparison of a Column against `x | y` of plain values (`F.col("mode") == READ | WRITE`); its fix only inserts parentheses (comments, line breaks and existing parentheses stay) and no longer drops parentheses an operand already had; help line added.
- COR002 no longer reports an `and` / `or` chain whose only Column is its last operand (`msg or default_col`, the optional-condition idiom Python runs fine) unless the chain is itself an `if` / `while` / `assert` / ternary test, where it gets its own message. The fix keeps comments and line breaks, and is unsafe when a Python value sits next to the Columns; messages have a help line.
- DBX003 now also reports `input_file_block_start()` and `input_file_block_length()` (unavailable on Unity Catalog, unsafe fix to `F.col("_metadata.file_block_start")` / `file_block_length`) and `F.call_function("input_file_name")`; the message says `input_file_name()` is no longer supported from Databricks Runtime 17.3 LTS, and a help line names the replacement column.
- COR001 now also reports `= NULL`, `!= NULL` and `<> NULL` comparisons in SQL text (`spark.sql(..)`, `df.filter("a = NULL")`, `F.expr(..)`, `selectExpr(..)`), which are NULL for every row; assignments such as `UPDATE .. SET a = NULL` are not reported. A typed NULL literal (`F.lit(None).cast("string")`) now counts as `None`.
- COR007 now reports a write, stream, merge or Delta OPTIMIZE / create chain that never runs (`df.write.mode("overwrite").format("delta")` with no `save()`, `writeStream…` with no `start()`, `dt.merge(..)…` with no `execute()`), plus a discarded `observe(..)`, `df.stat.sampleBy(..)` and lazy `checkpoint(eager=False)`.
- CFG020's message for an empty `delta.dataSkippingStatsColumns` no longer prints the value twice (`= '' = 0`).
- Spark config rules (CFG001-CFG022) now read settings applied in a loop over a constant dict or list of pairs (`for k, v in SETTINGS.items(): spark.conf.set(k, v)`).
- DBX002 gets an unsafe fix for `df.rdd.flatMap(lambda r: ..).collect()` (the
  column-to-list idiom `df.select("c").rdd.flatMap(lambda x: x).collect()` becomes
  `[v for x in df.select("c").collect() for v in x]`), like the existing `map` fix.
- One shared answer to "does this hold rows pulled to the driver, and how did they get
  there?" for ACT004, ACT008 and
  ACT009. Visible changes: ACT009 also
  reports driver compute over `toLocalIterator()` / `toPandas()` / `toArrow()` pulls
  (`len(df.toPandas())` → `df.count()`, `sum(pdf["amount"])`, `max(r.ts for r in
  df.toLocalIterator())`), through names bound in several ways and through values held
  in a name (`ids = [r.id for r in rows]; set(ids)`), and wins over ACT003 at the same
  `toPandas()`; ACT004 also follows open slices (`rows[1:]`), `reversed()` and frame rows
  through a name (`t = df.toArrow(); t.to_pylist()`); ACT008 also follows slices,
  `reversed()`, pandas-on-Spark `to_pandas()` columns and Arrow rows.
- ACT010 reports `show()` / `display()` of a one-row or few-row aggregate again
  (`df.agg(..).show()`, `df.describe().show()`, `df.summary().show()`,
  `display(df.agg(..))`, `spark.sql("SELECT count(*) FROM t").show()`): the aggregate
  reads the whole frame. A limit before it (`df.limit(100).agg(..)`), driver-built data
  and catalog metadata are still a cheap preview.
- One shared answer to "what does this call run?" (job, driver pull, write, display,
  stream start, checkpoint, Delta table operation). Visible changes: ACT005 now reports
  Delta Lake table operations in a loop (`for t in tables: DeltaTable.forName(spark,
  t).vacuum()`, `delete` / `update` / `merge(..)…execute()` / `optimize()…`; a table named
  per item is a different source, info); ACT007 also reports discarded or logged `tail()`,
  `toArrow()` and `toLocalIterator()`; ACT001 lists `first()` as `driver-collect` (was
  `job`); ACT004's N+1 note also counts a Delta operation or `display(df)` in the loop body.
- ACT005 and IOD006 now judge only the loop the action / write sits in (not every
  enclosing loop), count a constant list bound to a name (`REGIONS = [..]`, starred parts,
  `zip` / `enumerate` of literals, dict literals) as a short literal loop, and take the
  limit from the new `driver-loop-small-max` option (default 10, as before). ACT005 also
  treats a frame joined or unioned in per item (`base.join(spark.table(t), ..)`) and a
  helper given a per-item frame as a different source each iteration (info), and a
  `spark.sql` f-string whose holes sit only in predicates as the same source (warning).
- ACT001 now lists Delta Lake table operations: `DeltaTable` `delete` / `update` / `vacuum` /
  `restoreToVersion` / `generate` / `clone`, `merge(..)…execute()`,
  `optimize()…executeCompaction()` / `executeZOrderBy()`, `DeltaTable.create…(spark)…execute()`
  and `convertToDelta`. `DeltaTable` from `from delta.tables import *` is now recognised.
- ACT011 messages no longer claim `df.rdd.collect()` bypasses Arrow (`df.collect()` does not
  use Arrow either): the direct form now names the DataFrame call that returns the same rows,
  and a chain with RDD steps says the steps run in Python workers. ACT011 now also reports
  `first()` / `toLocalIterator()` (with the same unsafe fix) and `collectAsMap()` / `top()` /
  `takeOrdered()` / `takeSample()` after RDD steps; frames that fit on the driver
  (`limit(n)`, `SHOW TABLES`, local `createDataFrame`) are no longer reported.
- DBX001 messages no longer say `sparkContext` is blocked on standard access mode only
  before DBR 14: access itself works there from 14.0, but most `sc` functions
  (`parallelize`, `textFile`, `getConf`, …) stay blocked on every runtime.
- ACT010 `printSchema()` / `explain()` findings no longer claim they trigger jobs (they
  run none; the message now says they are log noise), console / memory streaming sinks
  get their own message and a help line pointing to a durable sink, and every ACT010
  finding now has a help line. Messages for a function call with a long argument name it
  (`display(spark.sql(…))` instead of `display(…)`).
- ACT009 now also reports `df.collect()[-1]` (use `df.tail(1)`) and `df.collect()[:n]` (use
  `df.take(n)`), with unsafe fixes, and the index forms through a name bound once to the
  collect (`rows = df.collect()` then `rows[0]`). When the rows go through such a name,
  only ACT009 is reported, no longer ACT002 at the `collect()` as well.
- One answer to "is this frame small?": frames that fit on the driver (a row bound
  including `limit(n)` with a variable `n`, frames built with `createDataFrame` from local
  data, `SHOW` / `DESCRIBE` results) are now treated alike by every rule. ACT006 no longer
  reports `count()` emptiness checks on them, ACT009 no longer reports `collect()[0]` /
  `len(df.collect())` on them, ACT010 no longer reports `display()` of them, PRF010 no
  longer reports `repartition(n)` before writing them, and ACT005, PRF003, PRF004, PRF032
  and UDF014 lower findings with such a frame (on either join side) to info.
- ACT010 now gives `df.show()` / `df.display()` the same answer as `display(df)`: a preview of
  a frame that fits on the driver (`df.limit(10).show()`, an aggregate-only or `LIMIT n`
  query, a `describe()` / `summary()` result, a frame built with `createDataFrame` from local
  data, `SHOW` / `DESCRIBE` results) is no longer reported, also in plain `.py` files.
- Rules that ask which columns an expression reads (PRF010, PRF012, PRF024, PRF026, STY003)
  now share one reading: only pyspark's own `col` / `column` name a column (a local `col()`
  helper no longer does), `df.a` / `df["a"]` count everywhere, column lists may be a list, a
  tuple or a starred list (`cube(*["a", "b"])`, `partitionBy(("day", "hour"))`), string
  values (`F.lit("x")`, `'x'` in SQL text) are not column names, a dotted name counts each
  of its parts, and a Column variable or f-string makes the reading incomplete. PRF012 now
  reports filters that only compare against a value spelled like the exploded column; STY003
  no longer reports `select(df["*"])` and offers its fix only when no selected column can be
  a dropped one.
- SQL text passed to `F.expr`, `df.filter` / `where`, `selectExpr` and `spark.sql` is read
  the same way by every rule: through a name whose every assignment holds the same text,
  as the `str=` / `condition=` keyword, from f-strings, and from `selectExpr` list or
  `*[...]` arguments. ACT008, DBX003, DBX016, PRF004, PRF012, PRF031, PRF032, UDF002 and
  UDF014 now see these forms (e.g. `cond = f"id IN ({ids})"; df.filter(cond)`,
  `F.expr(str="input_file_name()")`); ACT008 also covers `selectExpr` text and no longer
  reports an `EXPLAIN` query, which only plans.
- `sparklint rule ACT007` / `ACT008` / `IOD001` / `UDF011` (and their docs pages) showed an
  empty Message section. Every rule's message and help text now has one source, the rule
  itself: `sparklint rule CODE`, `sparklint rules --format json` (`messages`), the docs
  site and SARIF rule descriptors list each message variant the rule can report (with
  `{placeholders}` for the parts taken from the code) and its help line, so they can no
  longer drift from what `check` prints.
- ACT008 now also reports collected values interpolated into `df.filter(f"...")` / `where` /
  `F.expr` condition strings and the values of a `toPandas()` column held in a name
  (`list(pdf["id"])`); values that fit on the driver (`SHOW TABLES` results, frames built
  from local data) are no longer reported.
- ACT007 no longer reports the eager-cache idiom (`df.cache().count()`, `df.persist().count()`,
  or `df.count()` right after `df.cache()`), nor counts of frames that fit on the driver
  (`SHOW TABLES` / `DESCRIBE` results, frames built with `createDataFrame` from local data);
  a logged count held in a name now carries the same help line as the other logged forms.
- ACT006 findings now carry a help line (`isEmpty()` needs Spark 3.3+, `len(df.take(1)) == 0`
  before; keep `count()` when the number is needed).
- Messages name the call the way it is written in the code, one wording for every rule:
  receiver, method and short arguments (`orders.show(20, truncate=False)`,
  `orders.write.mode("append").saveAsTable("t")`, `display(orders)`); long arguments are
  elided (`(…)`) and a long receiver is shown hop by hop (`spark.sql(…).collect()`) before
  falling back to `spark….collect()`. ACT004 no longer shows a stand-in `df` / `rdd`
  (`for row in df.collect()`, `df.toArrow().to_pylist()`) for another receiver; ACT001,
  ACT005, ACT007, ACT010, ACT011 and COR007 now show the call's arguments (`df.take(5)`, not
  `df.take()`); ACT006, ACT009 and DBX002 show a long receiver hop by hop instead of `df….`.
  Message text only: no finding is added or removed.
- Constants held in a name now count when every binding that reaches the name agrees
  (`if a: N = 8` / `else: N = 2 * 4`), not only for a name bound once: config values (CFG
  rules), `repartition(n)` / `coalesce(n)` counts (IOD017 and the write rules), ACT015's
  `relativeError`, and string arguments read through a Spark call (ACT010 format, UDF002 names).
- Arguments passed by keyword are now read like positional ones by ACT008 (`spark.sql(sqlQuery=..)`),
  ACT010 (`writeStream.format(source=..)`), COR010 (`df.union(other=..)`), DBX016
  (`filter(condition=..)` on SHOW TABLES), DBX017 (`readStream.table(tableName=..)` /
  `load(path=..)`) and UDF002 / UDF014 (`spark.udf.register(name=..)`; the name may also be a
  constant). A starred argument before the position now makes the argument unknown.
- UDF001, UDF006, UDF011 and UDF012 share one answer to "which function does this UDF wrap?":
  UDF001 now also follows a lambda bound to a name (`f = lambda s: s[::-1]; F.udf(f)`) and
  `spark.udf.register("n", f=..)`, UDF006 reports `register("n", f=..)` without a return type,
  and all four follow a name bound on several branches (if/else) to every lambda or def it may
  be (UDF001/UDF006 need every branch to be one; before, such names were skipped).
- Spark 4 `df.mergeInto(..).whenMatched()...merge()` chains are now typed through their
  when-clauses: ACT001 lists the merge as a write and ACT005 reports one inside a loop (before,
  the writer was lost at `whenMatched()` and no rule saw the merge).
- A `spark.sql(f"... LIMIT {n}")` query whose row count is an f-string value now counts as
  row-bounded, like `df.limit(n)`: ACT002 / ACT003 / ACT004 and the other driver-pull rules no
  longer report it.
- ACT004 also reports `df.toLocalIterator(prefetchPartitions)`, `df.toPandas().to_dict("records")`
  and `df.toArrow().to_pylist()` loops, and its message names the real source for rows held in
  a variable (it said `.collect()` for a `toLocalIterator()` iterator).
- README and the configuration page give a fuller `per-file-ignores` recipe for test
  files: the nine driver-pull / debug-output rules instead of the whole ACT family, so
  compatibility and correctness findings stay on in tests.
- The "inside a loop" note (and DBX016's loop warning) on ACT015, DBX016, PRF023-PRF027,
  PRF029, PRF035 and PRF036 no longer fires for retry or first-match loops that leave right
  after the call (an unconditional `break` / `return`).
- `sparklint rule SYN001` and its rule page describe the real behaviour: the actual message,
  notebook cells still linted around a broken cell, and that `noqa` comments cannot suppress
  it (use `per-file-ignores`).
- ACT003 also reports `toArrow()` (Spark 4) on an unbounded DataFrame, and names
  pandas-on-Spark data (DataFrame or Series) as such in its message.
- STM010 treats a Kafka stream that starts from a timestamp (`startingTimestamp` /
  `startingOffsetsByTimestamp`) without `maxOffsetsPerTrigger` as a backlog read (warning, own
  message) instead of the info for the default start.
- STM002 no longer offers the `availableNow=True` fix when the same `trigger(..)` call sets
  another trigger (Spark already rejects that call).
- IOD017 reads partition counts and key columns held in a constant (`MANY = 400;
  df.coalesce(MANY)` and `df.repartition(KEY)` are now reported); IOD017 and IOD018 stay info
  in retry and first-match loops (the write or repair is followed by an unconditional `break` /
  `return`) instead of the per-iteration warning.
- IOD015 also reports zstd (`.zst`), LZ4, Snappy and deflate compressed CSV / JSON / text
  reads (only bzip2 is splittable) and names the codec; `multiLine` / `wholetext` reads, which
  take each file whole anyway, are no longer reported.
- IOD016: an RDD of raw file lines (`sc.textFile(..)`) gets help to read the files directly;
  the message now says RDDs (not only `df.rdd`) are unsupported on Spark Connect.
- ACT005 and IOD006 no longer report retry and first-match loops: an action or append followed
  in the same block by an unconditional `break` or `return` (`try: df.write...; break` /
  `except: sleep`, `if found: return spark.table(t).count()`) runs once.
- IOD003 now also reports `hint("coalesce", 1)` / `hint("repartition", 1)` before a write and
  a partition count held in a constant (`ONE = 1; df.coalesce(ONE)`); the message names the
  step it found.
- IOD001: a streaming file source in a file that sets `spark.sql.streaming.schemaInference` is a
  warning (the schema is inferred once, at query start) instead of an error, and streaming
  findings now carry a `readStream.schema(..)` help.
- IOD002 reads the `dbtable` / `query` text as SQL: a `WHERE` after a line break counts,
  keywords inside quoted strings do not, and `SELECT TOP n` / `FETCH FIRST` count as limits
  (info). An unfiltered read through the `query` option is now a warning like `dbtable`, with
  help explaining that Spark rejects `partitionColumn` together with `query`.
- UDF012 now follows a lambda bound to a name (`send = lambda r: ...; df.foreach(send)`) and
  `spark.udf.register(name, f=...)`, and names the lambda by its variable in the message.
  UDF013 follows a whole-row struct held in a variable (`row = F.struct("*"); f(row)`) and
  keyword arguments. UDF014 is info when a broadcast `hint(..)` or `F.broadcast` marks either
  join side small, like PRF004.
- UDF006 now follows a lambda bound to a name and `F.udf(f=...)`, and reads `Optional[X]` /
  `X | None` return annotations as X (`Optional[str]` is no longer reported, `Optional[int]` is
  a warning). UDF011 gives grouped pandas UDFs (Series -> scalar, legacy `GROUPED_AGG` /
  `GROUPED_MAP`) a per-group message instead of the per-batch one.
- UDF001 names `F.reverse()` for `x[::-1]` (no longer `F.substring()`), skips other slice
  steps, and now reports `bool(re.match(..))` / `re.search(..) is not None` bodies
  (`Column.rlike()`). UDF002 follows a Column variable in the filter condition to the UDF
  call it holds; its help no longer suggests splitting the filter, which Spark already does.
- PRF032 no longer reports an OR join condition whose branches all share the same key
  equality (`(k & a) | (k & b)`, also in `F.expr` text): Spark factors it out and keeps the
  hash key.
- PRF031 also reads running `collect_list` / `collect_set` / `array_agg` windows written as SQL
  text (`F.expr`, `selectExpr`, `spark.sql`), and treats frames reaching back a million rows or
  more (`rowsBetween(-10**9, 0)`) as running frames.
- PRF026 counts distinct aggregates built by a starred comprehension over a literal column
  list (`agg(*[F.countDistinct(c) for c in ["a", "b", "c"]])`).
- PRF027 no longer reports random sorts of a row-bounded or driver-built frame
  (`df.limit(100).orderBy(F.rand())`), and follows an `F.expr` string bound to a name.
- PRF024 now reports a SQL `GROUP BY` whose CUBE / ROLLUP / GROUPING SETS items multiply into
  many grouping sets (`GROUP BY CUBE(a, b, c), CUBE(d, e, f)` = 64 sets), and says when the
  query runs inside a loop.
- PRF025 also reports `NOT (x IN (SELECT ...))`, which Spark plans like `x NOT IN (SELECT ...)`.
- PRF004 now reports `F.expr` join conditions whose only `=` compares a column with a literal
  (`F.expr("ts BETWEEN start AND end AND kind = 'x'")`) or that compare with `!=`: a literal
  equality filters rows, it is not a join key.
- PRF004 and PRF032 report at info when either side carries a broadcast hint
  (`df.hint("broadcast")`, `"broadcastjoin"`, `"mapjoin"`), as they already did for
  `F.broadcast(df)`.
- PRF010 no longer reports `repartition(n, "col")` before a write partitioned by the same
  columns (`.partitionBy("col")`): like `repartition("col")`, it bounds the files per output
  partition on purpose.
- PRF012 no longer reports when a step that changes which rows the filter sees sits between the
  explode and the filter (`limit`, `sample`, a union, `dropDuplicates(subset)`), or when a later
  column reads the exploded value through an expression string (`F.expr("upper(i)")`), a
  Column variable or a DataFrame attribute.
- PRF015's fix for `array_distinct(collect_list(x).over(w))` is now unsafe: an ordered window
  keeps the collect_list order, collect_set does not.
- PRF022 also reads `SELECT *, extra`, `SELECT t.*` and `SELECT DISTINCT *` as star selects,
  and no longer reports results with a known row bound (`LIMIT n`, `take(n)`, `head(n)`,
  `limit(n).collect()`).
- PRF035 also reports a grouping on only constant `F.lit(..)` keys and the streaming stateful
  functions (`applyInPandasWithState`, `transformWithStateInPandas`, `transformWithState`) on
  one global group, and no longer reports frames built from driver data.

## [0.2.0] - 2026-11-13

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
