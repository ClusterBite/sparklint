//! IOD001 read-schema-inference, IOD015 unsplittable-compressed-read,
//! IOD016 json-schema-inference-via-rdd, IOD017 jdbc-write-unbounded-connections,
//! IOD018 repair-table-in-job, IOD019 small-max-records-per-file,
//! IOD020 zorder-many-columns.

use ruff_python_ast::{self as ast, Expr};
use sparklint_semantic::call_run::{RunKind, run_of_ty};
use sparklint_semantic::transform;
use sparklint_semantic::{Hops, Ty, call_arg, int_literal, str_literal};

use super::Rule;
use super::conf::{Conf, Finding, Setting};
use super::helpers::*;
use super::io_settings::IoSettings;
use super::msg::Msg;
use crate::checker::Checker;
use crate::diagnostic::Severity;
use crate::{finding, msg};

/// IOD001/015/016/017/018/019/020 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("IOD001").call(read_schema_inference).msgs(&[
        IOD001_CSV_INFER,
        IOD001_CSV_STRINGS,
        IOD001_INFERRED,
        IOD001_MERGE_SCHEMA,
        IOD001_STREAM,
        IOD001_STREAM_INFERENCE,
    ]),
    Rule::new("IOD015")
        .call(unsplittable_compressed_read)
        .msgs(&[IOD015]),
    Rule::new("IOD016")
        .call(json_schema_inference_via_rdd)
        .msgs(&[IOD016_FILES, IOD016]),
    Rule::new("IOD017")
        .call(jdbc_write_unbounded_connections)
        .msgs(&[IOD017, IOD017_BATCHSIZE, IOD017_LOOP, IOD017_BATCHSIZE_LOOP]),
    Rule::new("IOD018")
        .call(repair_table_in_job)
        .msgs(&[IOD018, IOD018_LOOP]),
    Rule::new("IOD019")
        .conf(&Conf::new(
            &["spark.sql.files.maxRecordsPerFile"],
            small_max_records_conf,
        ))
        .call(small_max_records_per_file)
        .msgs(&[IOD019_SESSION, IOD019_WRITER]),
    Rule::new("IOD020")
        .call(zorder_many_columns)
        .msgs(&[IOD020]),
];

const IOD001_READ_HELP: &str =
    "Pass an explicit schema: spark.read.schema(SCHEMA)... (a StructType or DDL string).";
const IOD001_STREAM_HELP: &str =
    "Pass an explicit schema: spark.readStream.schema(SCHEMA)... (a StructType or DDL string).";
const IOD001_CSV_INFER: Msg = Msg::new(
    "IOD001",
    "csv-infer-schema",
    "CSV read with inferSchema and no explicit schema — inference scans the data an extra time and the schema drifts.",
)
.help(IOD001_READ_HELP);
const IOD001_CSV_STRINGS: Msg = Msg::new(
    "IOD001",
    "csv-strings",
    "CSV read without an explicit schema — every column is read as StringType.",
)
.help(IOD001_READ_HELP);
const IOD001_INFERRED: Msg = Msg::new(
    "IOD001",
    "inferred",
    "{format} read without an explicit schema — inference scans the data an extra time and the schema drifts.",
)
.help(IOD001_READ_HELP);
const IOD001_MERGE_SCHEMA: Msg = Msg::new(
    "IOD001",
    "merge-schema",
    "{format} read with mergeSchema=true reads every file footer to merge schemas.",
)
.help("Drop mergeSchema, or read with an explicit schema.");
const IOD001_STREAM: Msg = Msg::new(
    "IOD001",
    "streaming-no-schema",
    "Streaming {format} source without an explicit schema — fails at runtime unless spark.sql.streaming.schemaInference is set, and the schema drifts.",
)
.help(IOD001_STREAM_HELP);
const IOD001_STREAM_INFERENCE: Msg = Msg::new(
    "IOD001",
    "streaming-schema-inference",
    "Streaming {format} source infers its schema (spark.sql.streaming.schemaInference) — only from the files present when the query starts, so later schema changes are not picked up.",
)
.help(IOD001_STREAM_HELP);
const IOD015: Msg = Msg::new(
    "IOD015",
    "unsplittable",
    "Reading {codec}-compressed {format} ({path}): {codec} is not splittable, so each file is read by a single task whatever its size.",
)
.help("Re-compress as bzip2 or split the files upstream, or land the data once as Delta/Parquet. If the source cannot change, .repartition(n) right after the read spreads the downstream work.");
const IOD016_TEXT: &str = "spark.read.json over an RDD without a schema: an extra full job ships every row through Python and infers the schema over all of it before the real read (and RDDs are unsupported on Spark Connect / serverless).";
const IOD016_FILES: Msg = Msg::new("IOD016", "rdd-of-text-files", IOD016_TEXT)
    .help("Read the files directly with spark.read.json(path) (one JVM scan, no Python round trip; add multiLine=True when each file is one JSON document), and pass an explicit schema to skip inference.");
const IOD016: Msg = Msg::new("IOD016", "rdd", IOD016_TEXT)
    .help("Pass an explicit schema (DDL string), infer it from a small sample (F.schema_of_json on one value, or df.limit(1000)), or on Databricks parse into VARIANT with parse_json.");
const IOD017_HELP: &str = "Set .option(\"numPartitions\", N) (or properties={{\"numPartitions\": \"N\"}}) to the number of connections the database can take; Spark coalesces to it.";
const IOD017: Msg = Msg::new(
    "IOD017",
    "no-num-partitions",
    "JDBC write without numPartitions opens one database connection per partition at once (200 after a shuffle), which can exhaust the database's connection limit and stall or fail the write.",
)
.help(IOD017_HELP);
const IOD017_BATCHSIZE: Msg = Msg::new(
    "IOD017",
    "no-num-partitions-no-batchsize",
    "JDBC write without numPartitions opens one database connection per partition at once (200 after a shuffle), which can exhaust the database's connection limit and stall or fail the write. batchsize is also unset (1000 rows per round trip); raising it usually speeds up bulk writes.",
)
.help(IOD017_HELP);
const IOD017_LOOP: Msg = Msg::new(
    "IOD017",
    "no-num-partitions-in-loop",
    "JDBC write without numPartitions opens one database connection per partition at once (200 after a shuffle), which can exhaust the database's connection limit and stall or fail the write. Inside a loop, every iteration opens the connections again.",
)
.help(IOD017_HELP);
const IOD017_BATCHSIZE_LOOP: Msg = Msg::new(
    "IOD017",
    "no-num-partitions-no-batchsize-in-loop",
    "JDBC write without numPartitions opens one database connection per partition at once (200 after a shuffle), which can exhaust the database's connection limit and stall or fail the write. batchsize is also unset (1000 rows per round trip); raising it usually speeds up bulk writes. Inside a loop, every iteration opens the connections again.",
)
.help(IOD017_HELP);
const IOD018_HELP: &str = "Register only the partitions just written (ALTER TABLE t ADD IF NOT EXISTS PARTITION (...)), or use a Delta table, which needs no partition repair.";
const IOD018: Msg = Msg::new(
    "IOD018",
    "repair",
    "{how} lists every partition directory of the table on each run, so the job slows down as the table grows even when it wrote one new partition.",
)
.help(IOD018_HELP);
const IOD018_LOOP: Msg = Msg::new(
    "IOD018",
    "repair-in-loop",
    "{how} lists every partition directory of the table on each run, so the job slows down as the table grows even when it wrote one new partition. Inside a loop, the full listing runs on every iteration.",
)
.help(IOD018_HELP);
const IOD019_SESSION: Msg = Msg::new(
    "IOD019",
    "session",
    "spark.sql.files.maxRecordsPerFile={n} caps every output file of the session at {n} rows: each write task splits its output into many small files (slower listing and opening for every reader, weaker compression and statistics, more compaction work).",
)
.help(MAX_RECORDS_HELP);
const IOD019_WRITER: Msg = Msg::new(
    "IOD019",
    "writer-option",
    "maxRecordsPerFile={n} caps each output file at {n} rows: every write task splits its output into many small files (slower listing and opening for every reader, weaker compression and statistics, more compaction work).",
)
.help(MAX_RECORDS_HELP);
const IOD020: Msg = Msg::new(
    "IOD020",
    "zorder-many-columns",
    "{what} over {n} columns: Z-ordering interleaves every column, so each extra one widens the per-file value range of all the others; data skipping weakens for every column while OPTIMIZE still rewrites the data.",
)
.help("Z-order by the 1-3 high-cardinality columns most used in filters (partition columns never help), or switch the table to liquid clustering (CLUSTER BY) on Databricks.");

const FILE_FORMATS: &[&str] = &["csv", "json", "parquet", "orc", "xml", "text", "avro"];

pub fn read_schema_inference(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(
        expr,
        &["csv", "json", "load", "parquet", "orc", "xml", "text"],
    ) else {
        return;
    };
    let streaming = match sc.class() {
        Ty::DataFrameReader => false,
        Ty::DataStreamReader => true,
        _ => return,
    };
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    // An unseen setting (`**opts`, a separate `r.schema(s)` statement) may be the schema.
    if io.has_schema() || !io.complete() {
        return;
    }
    let Some(format) = io.format().map(str::to_string) else {
        return;
    };
    let range = method_range(expr);
    if format == "cloudfiles" {
        return; // Auto Loader: STM008 territory
    }
    if !streaming && schema_probe(ch) {
        return;
    }
    // `spark.read.json(sc.parallelize(['{"a": 1}']))`: inference over an RDD that fits the
    // driver (a few literal strings, a literal frame's `.rdd`) costs nothing (tests,
    // examples).
    if io.target().is_some_and(|t| small_rdd(ch, t)) {
        return;
    }
    if streaming && FILE_FORMATS.contains(&format.as_str()) && format != "text" {
        // With the session flag set the query starts, but the schema is inferred from the
        // files present at start and fixed from then on.
        let d = if super::conf::file_sets(ch, "spark.sql.streaming.schemainference") {
            msg!(IOD001_STREAM_INFERENCE, range, format)
        } else {
            msg!(IOD001_STREAM, range, format).with_severity(Severity::Error)
        };
        ch.report(d);
        return;
    }
    match format.as_str() {
        "csv" => match io
            .has("inferschema")
            .then(|| io.flag("inferschema").unwrap_or(true))
        {
            Some(true) => ch.report(msg!(IOD001_CSV_INFER, range)),
            _ => ch.report(msg!(IOD001_CSV_STRINGS, range).with_severity(Severity::Info)),
        },
        "json" | "xml" => ch.report(msg!(
            IOD001_INFERRED,
            range,
            format = format.to_ascii_uppercase()
        )),
        "parquet" | "orc" if io.flag("mergeschema") == Some(true) => {
            ch.report(msg!(IOD001_MERGE_SCHEMA, range, format))
        }
        _ => {}
    }
}

/// The read is only asked for its schema (`spark.read.json(p).schema`, `.columns`,
/// `.dtypes`, `.printSchema()`): inference is the point (typically to hand the schema
/// to a streaming reader), so there is no extra scan to save.
fn schema_probe(ch: &Checker) -> bool {
    use crate::checker::Node;
    matches!(
        ch.parent(),
        Some(Node::Expr(Expr::Attribute(a)))
            if matches!(a.attr.as_str(), "schema" | "columns" | "dtypes" | "printSchema")
    )
}

// ------------------------------------------------------------------ IOD015

/// The non-splittable compression codec a (constant-folded) path names by its
/// extension, also through a glob such as `*.csv.gz`: Hadoop picks the codec from the
/// extension and only bzip2 is splittable, so gzip (`.gz` / `.gzip`), zstd (`.zst`),
/// LZ4 (`.lz4`), Snappy (`.snappy`) and deflate (`.deflate`) text files are read by
/// one task each.
fn unsplittable_codec(p: &str) -> Option<&'static str> {
    let p = p.trim().trim_end_matches('/').to_ascii_lowercase();
    [
        (".gz", "gzip"),
        (".gzip", "gzip"),
        (".zst", "zstd"),
        (".lz4", "lz4"),
        (".snappy", "snappy"),
        (".deflate", "deflate"),
    ]
    .into_iter()
    .find(|(ext, _)| p.ends_with(ext))
    .map(|(_, codec)| codec)
}

/// `spark.read.csv("x.csv.gz")` / `format("json").load("x.json.gz")`: gzip is not
/// splittable, so each file is read by one task whatever its size.
pub fn unsplittable_compressed_read(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch
        .model
        .spark_call_named(expr, &["csv", "json", "text", "load"])
    else {
        return;
    };
    if !matches!(sc.class(), Ty::DataFrameReader | Ty::DataStreamReader) {
        return;
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    let format = match io.format() {
        Some("cloudfiles") => io
            .option("cloudfiles.format")
            .and_then(str_literal)
            .map(str::to_ascii_lowercase),
        f => f.map(str::to_string),
    };
    let Some(format) = format.filter(|f| matches!(f.as_str(), "csv" | "json" | "text")) else {
        return;
    };
    // multiLine csv/json and wholetext reads take each file whole in one task whatever
    // the compression: re-compressing would not split them.
    if io.flag("multiline") == Some(true) || io.flag("wholetext") == Some(true) {
        return;
    }
    // The path argument (`paths=` for a batch `text` read) or a `path` option.
    let Some(arg) = io.target() else {
        return;
    };
    let paths: Vec<&Expr> = match arg {
        Expr::List(l) => l.elts.iter().collect(),
        Expr::Tuple(t) => t.elts.iter().collect(),
        e => vec![e],
    };
    let Some((path, codec)) = paths
        .iter()
        .filter_map(|p| ch.model.resolve_text(p))
        .find_map(|t| unsplittable_codec(&t).map(|c| (t, c)))
    else {
        return;
    };
    let chars: Vec<char> = path.chars().collect();
    let shown = if chars.len() > 48 {
        format!("…{}", chars[chars.len() - 40..].iter().collect::<String>())
    } else {
        path.clone()
    };
    ch.report(msg!(
        IOD015,
        method_range(expr),
        codec,
        format,
        path = shown
    ));
}

// ------------------------------------------------------------------ IOD016

/// An RDD that fits the driver (`sc.parallelize([...literal...])`, `df.limit(5).rdd`).
fn small_rdd(ch: &Checker, e: &Expr) -> bool {
    let t = ch.model.ty_of(e);
    t.ty == Ty::Rdd && t.size().fits_driver()
}

/// `spark.read.json(df.rdd.map(lambda r: r.payload))` without a schema: the Stack
/// Overflow recipe for a JSON column's schema runs an extra full job through Python.
pub fn json_schema_inference_via_rdd(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["json"]) else {
        return;
    };
    if sc.class() != Ty::DataFrameReader || !sc.is_certain() {
        return;
    }
    let Some(arg) = sc.arg(0, "path") else {
        return;
    };
    let at = ch.model.ty_of(arg);
    // `sc.parallelize([...literal...])`, a literal frame's `.rdd`: a tiny test RDD.
    if at.ty != Ty::Rdd || small_rdd(ch, arg) {
        return;
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    if io.has_schema() || !io.complete() {
        return;
    }
    // `sc.textFile(p)`, `sc.wholeTextFiles(p).values()`: the raw text of files that
    // `spark.read.json(p)` could read directly.
    let m = if at.facts.raw_file_text() {
        &IOD016_FILES
    } else {
        &IOD016
    };
    ch.report(msg!(m, method_range(expr)));
}

/// `spark.sql("MSCK REPAIR TABLE t")` / `REPAIR TABLE` / `ALTER TABLE t RECOVER
/// PARTITIONS` / `spark.catalog.recoverPartitions(t)`: every run lists every
/// partition directory of the table.
pub fn repair_table_in_job(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let how = if ch
        .model
        .spark_sql(expr)
        .is_some_and(|q| q.repairs_partitions())
    {
        "Partition repair (REPAIR TABLE / RECOVER PARTITIONS)"
    } else if ch
        .model
        .spark_call_named(expr, &["recoverPartitions"])
        .is_some_and(|sc| sc.class() == Ty::Catalog)
    {
        "spark.catalog.recoverPartitions"
    } else {
        return;
    };
    let range = method_range(expr);
    let d = if ch.model.in_repeating_loop(&range) {
        msg!(IOD018_LOOP, range, how).with_severity(Severity::Warning)
    } else {
        msg!(IOD018, range, how).with_severity(Severity::Info)
    };
    ch.report(d);
}

// ------------------------------------------------------------------ IOD017

/// Coalesce/repartition values small enough to cap the connection count.
const SMALL_PARTITIONS: i64 = 64;

/// Whether `df` (followed through narrow transformations and names bound once) was
/// given an explicit partition count that is small or unknown: `coalesce(8)`,
/// `repartition(n)`. `repartition("col")` and `repartition(200)` keep the
/// connection count high.
fn partitions_capped<'e>(ch: &Checker<'e>, mut e: &'e Expr, mut hops: Hops) -> bool {
    loop {
        if let Expr::Name(_) = e {
            let Some((v, h)) = ch.model.hop_one(e, hops) else {
                return false;
            };
            hops = h;
            e = v;
            continue;
        }
        let Some((c, recv, m)) = method_call(e) else {
            return false;
        };
        if !ch.model.is_dataframe(recv) {
            return false;
        }
        if matches!(m, "coalesce" | "repartition") {
            let Some(n) = call_arg(c, 0, "numPartitions") else {
                return false; // repartition(cols) only: shuffle-partition count
            };
            return match ch.model.const_int(n) {
                Some(v) => v <= SMALL_PARTITIONS as i128,
                // a column name / Column: partitioned by key, count unchanged
                None => !(ch.model.const_str(n).is_some() || ch.model.is_column(n)),
            };
        }
        if !transform::frame_row(m).is_some_and(|r| r.keeps_partitions()) {
            return false;
        }
        e = recv;
    }
}

/// `df.write.jdbc(...)` / `df.write.format("jdbc")...save()` without `numPartitions`:
/// one concurrent database connection per partition of the frame.
pub fn jdbc_write_unbounded_connections(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["jdbc", "save"]) else {
        return;
    };
    if sc.class() != Ty::DataFrameWriter {
        return;
    }
    let Some(io) = IoSettings::write(ch, expr) else {
        return;
    };
    if io.format() != Some("jdbc") || !io.complete() || io.has("numpartitions") {
        return;
    }
    if io
        .df
        .is_some_and(|df| partitions_capped(ch, df, Hops::DEFAULT))
    {
        return;
    }
    let range = method_range(expr);
    let in_loop = ch.model.in_repeating_loop(&range);
    let m = match (!io.has("batchsize"), in_loop) {
        (false, false) => &IOD017,
        (true, false) => &IOD017_BATCHSIZE,
        (false, true) => &IOD017_LOOP,
        (true, true) => &IOD017_BATCHSIZE_LOOP,
    };
    let sev = if in_loop {
        Severity::Warning
    } else {
        Severity::Info
    };
    ch.report(msg!(m, range).with_severity(sev));
}

// ------------------------------------------------------------------ IOD019

/// Row caps below this split ordinary task output into many small files.
const MAX_RECORDS_LOW: i64 = 100_000;

/// A positive row cap below [`MAX_RECORDS_LOW`] in option/config value text.
fn small_record_cap(v: &str) -> Option<i64> {
    let v = v.trim().replace('_', "");
    let n: i64 = v.parse().ok()?;
    (n > 0 && n < MAX_RECORDS_LOW).then_some(n)
}

/// Text of an option value: a literal (int or string) or a name bound once to one.
fn option_value_text(ch: &Checker, e: &Expr) -> Option<String> {
    let lit = |e: &Expr| {
        int_literal(e)
            .map(|n| n.to_string())
            .or_else(|| str_literal(e).map(str::to_string))
    };
    if let Some(t) = lit(e) {
        return Some(t);
    }
    lit(ch.model.value_of(e, Hops::DEFAULT))
}

const MAX_RECORDS_HELP: &str = "Remove the cap and let file size follow the data (Delta: optimized writes / OPTIMIZE, or delta.targetFileSize). Keep a cap only for very wide rows or a downstream reader with a hard row limit, sized in the millions.";

/// `spark.sql.files.maxRecordsPerFile` set to a small row count: every task's output
/// split into many small files.
fn small_max_records_conf(s: &Setting) -> Option<Finding> {
    let n = small_record_cap(s.value)?;
    Some(finding!(IOD019_SESSION, n).info())
}

/// `df.write.option("maxRecordsPerFile", 10000)`: the writer half of IOD019 (the
/// session config half is its [`Conf`] row).
pub fn small_max_records_per_file(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call(expr) else {
        return;
    };
    let class = sc.class();
    if !matches!(
        class,
        Ty::DataFrameWriter | Ty::DataFrameWriterV2 | Ty::DataStreamWriter
    ) || !matches!(
        run_of_ty(class, sc.method).map(|r| r.kind),
        Some(RunKind::Write | RunKind::StreamStart)
    ) {
        return;
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    let Some(n) = io
        .option("maxrecordsperfile")
        .and_then(|e| option_value_text(ch, e))
        .and_then(|v| small_record_cap(&v))
    else {
        return;
    };
    ch.report(msg!(IOD019_WRITER, method_range(expr), n).with_severity(Severity::Info));
}

// ------------------------------------------------------------------ IOD020

/// `OPTIMIZE t ZORDER BY (a, b, c, d)` in `spark.sql` or
/// `DeltaTable...optimize()[.where(..)].executeZOrderBy(..)` with more columns than
/// `zorder-max-columns`: every extra key dilutes the clustering of all the others.
pub fn zorder_many_columns(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let max = ch.settings.zorder_max_columns as usize;
    if max == 0 {
        return;
    }
    let (n, what) = if let Some(q) = ch.model.spark_sql(expr) {
        match q.zorder_columns() {
            Some(n) => (n, "SQL ZORDER BY"),
            None => return,
        }
    } else {
        match delta_zorder_columns(ch, c) {
            Some(n) => (n, "executeZOrderBy"),
            None => return,
        }
    };
    if n <= max {
        return;
    }
    ch.report(msg!(IOD020, method_range(expr), what, n).with_severity(Severity::Info));
}

/// Column count of `<DeltaTable>.optimize()[.where(..)].executeZOrderBy(cols...)`;
/// `None` when it is another call or the count is unknown (`*cols`, a variable list).
fn delta_zorder_columns(ch: &Checker, c: &ast::ExprCall) -> Option<usize> {
    let Expr::Attribute(a) = &*c.func else {
        return None;
    };
    if a.attr.as_str() != "executeZOrderBy" {
        return None;
    }
    let mut recv = &*a.value;
    loop {
        let Expr::Call(rc) = recv else {
            return None;
        };
        let Expr::Attribute(ra) = &*rc.func else {
            return None;
        };
        match ra.attr.as_str() {
            "where" => recv = &ra.value,
            "optimize" if ch.model.ty_of(&ra.value).ty == Ty::DeltaTable => break,
            _ => return None,
        }
    }
    if !c.arguments.keywords.is_empty() {
        return None;
    }
    let mut n = 0;
    for arg in c.arguments.args.iter() {
        n += match arg {
            Expr::List(l) if l.elts.iter().all(|e| !e.is_starred_expr()) => l.elts.len(),
            Expr::Tuple(t) if t.elts.iter().all(|e| !e.is_starred_expr()) => t.elts.len(),
            Expr::List(_) | Expr::Tuple(_) | Expr::Starred(_) => return None,
            e if str_literal(e).is_some() => 1,
            // A column name in a variable: one column unless it is a list.
            e => match ch.model.ty_of(e).ty {
                Ty::Str => 1,
                _ => return None,
            },
        };
    }
    Some(n)
}

#[cfg(test)]
mod iod019_tests {
    use super::small_record_cap;

    #[test]
    fn record_caps() {
        assert_eq!(small_record_cap("10000"), Some(10000));
        assert_eq!(small_record_cap(" 99999 "), Some(99999));
        assert_eq!(small_record_cap("1_000"), Some(1000));
        assert_eq!(small_record_cap("1"), Some(1));
        assert_eq!(small_record_cap("100000"), None);
        assert_eq!(small_record_cap("5000000"), None);
        assert_eq!(small_record_cap("0"), None);
        assert_eq!(small_record_cap("-1"), None);
        assert_eq!(small_record_cap("10k"), None);
        assert_eq!(small_record_cap(""), None);
    }
}
