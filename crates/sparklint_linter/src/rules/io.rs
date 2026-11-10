//! IOD001 read-schema-inference, IOD015 unsplittable-compressed-read,
//! IOD016 json-schema-inference-via-rdd, IOD017 jdbc-write-unbounded-connections,
//! IOD018 repair-table-in-job, IOD019 small-max-records-per-file,
//! IOD020 zorder-many-columns.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::Ranged;
use sparklint_semantic::api::{ActionKind, action_kind};
use sparklint_semantic::{Ty, call_arg, int_literal, str_literal};

use super::Rule;
use super::actions::method_range;
use super::conf::{Conf, Finding, Setting};
use super::helpers::*;
use super::io_settings::IoSettings;
use crate::checker::Checker;
use crate::diagnostic::{Diagnostic, Severity};

/// IOD001/015/016/017/018/019/020 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("IOD001").call(read_schema_inference),
    Rule::new("IOD015").call(unsplittable_compressed_read),
    Rule::new("IOD016").call(json_schema_inference_via_rdd),
    Rule::new("IOD017").call(jdbc_write_unbounded_connections),
    Rule::new("IOD018").call(repair_table_in_job),
    Rule::new("IOD019")
        .conf(&Conf::new(
            &["spark.sql.files.maxRecordsPerFile"],
            small_max_records_conf,
        ))
        .call(small_max_records_per_file),
    Rule::new("IOD020").call(zorder_many_columns),
];

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
    if streaming && FILE_FORMATS.contains(&format.as_str()) && format != "text" {
        ch.report(
            Diagnostic::new(
                "IOD001",
                format!("Streaming {format} source without an explicit schema — fails at runtime unless spark.sql.streaming.schemaInference is set, and the schema drifts."),
                range,
            )
            .with_severity(Severity::Error),
        );
        return;
    }
    let help =
        "Pass an explicit schema: spark.read.schema(SCHEMA)... (a StructType or DDL string).";
    match format.as_str() {
        "csv" => match io.has("inferschema").then(|| io.flag("inferschema").unwrap_or(true)) {
            Some(true) => ch.report(
                Diagnostic::new(
                    "IOD001",
                    "CSV read with inferSchema and no explicit schema — inference scans the data an extra time and the schema drifts.",
                    range,
                )
                .with_help(help),
            ),
            _ => ch.report(
                Diagnostic::new(
                    "IOD001",
                    "CSV read without an explicit schema — every column is read as StringType.",
                    range,
                )
                .with_severity(Severity::Info)
                .with_help(help),
            ),
        },
        "json" | "xml" => ch.report(
            Diagnostic::new(
                "IOD001",
                format!("{} read without an explicit schema — inference scans the data an extra time and the schema drifts.", format.to_ascii_uppercase()),
                range,
            )
            .with_help(help),
        ),
        "parquet" | "orc" if io.flag("mergeschema") == Some(true) => ch.report(
            Diagnostic::new(
                "IOD001",
                format!("{format} read with mergeSchema=true reads every file footer to merge schemas."),
                range,
            )
            .with_help("Drop mergeSchema, or read with an explicit schema."),
        ),
        _ => {}
    }
}

// ------------------------------------------------------------------ IOD015

/// Whether a (constant-folded) path names a gzip file: `.gz` / `.gzip`, also through a
/// glob such as `*.csv.gz`.
fn gzip_path(p: &str) -> bool {
    let p = p.trim().trim_end_matches('/').to_ascii_lowercase();
    p.ends_with(".gz") || p.ends_with(".gzip")
}

/// `spark.read.csv("x.csv.gz")` / `format("json").load("x.json.gz")`: gzip is not
/// splittable, so each file is read by one task whatever its size.
pub fn unsplittable_compressed_read(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch
        .model
        .spark_call_named(expr, &["csv", "json", "text", "load"])
    else {
        return;
    };
    let m = sc.method;
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
    let kw = if m == "text" { "paths" } else { "path" };
    let Some(arg) = call_arg(c, 0, kw) else {
        return;
    };
    let paths: Vec<&Expr> = match arg {
        Expr::List(l) => l.elts.iter().collect(),
        Expr::Tuple(t) => t.elts.iter().collect(),
        e => vec![e],
    };
    let Some(path) = paths
        .iter()
        .filter_map(|p| ch.model.resolve_text(p))
        .find(|t| gzip_path(t))
    else {
        return;
    };
    let chars: Vec<char> = path.chars().collect();
    let shown = if chars.len() > 48 {
        format!("…{}", chars[chars.len() - 40..].iter().collect::<String>())
    } else {
        path.clone()
    };
    ch.report(
        Diagnostic::new(
            "IOD015",
            format!(
                "Reading gzip-compressed {format} ({shown}): gzip is not splittable, so each file is read by a single task whatever its size."
            ),
            method_range(expr),
        )
        .with_help(
            "Re-compress as bzip2 or split the files upstream, or land the data once as Delta/Parquet. If the source cannot change, .repartition(n) right after the read spreads the downstream work.",
        ),
    );
}

// ------------------------------------------------------------------ IOD016

/// `sc.parallelize([...literal...])` (also through one binding): a tiny test RDD.
fn literal_parallelize(ch: &Checker, e: &Expr, depth: u32) -> bool {
    if let Expr::Name(n) = e
        && depth < 4
        && let Some(b) = ch.model.single_binding(n)
        && let Some(v) = ch.model.binding(b).value
    {
        return literal_parallelize(ch, v, depth + 1);
    }
    let Some((c, recv, "parallelize")) = method_call(e) else {
        return false;
    };
    let literal = |a: &Expr| matches!(a, Expr::List(_) | Expr::Tuple(_));
    ch.model.ty_of(recv).ty == Ty::SparkContext
        && match c.arguments.args.first() {
            Some(Expr::Name(n)) => ch
                .model
                .single_binding(n)
                .and_then(|b| ch.model.binding(b).value)
                .is_some_and(literal),
            Some(a) => literal(a),
            None => false,
        }
}

/// `spark.read.json(df.rdd.map(lambda r: r.payload))` without a schema: the Stack
/// Overflow recipe for a JSON column's schema runs an extra full job through Python.
pub fn json_schema_inference_via_rdd(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["json"]) else {
        return;
    };
    if sc.class() != Ty::DataFrameReader || !sc.is_certain() {
        return;
    }
    let Some(arg) = call_arg(c, 0, "path") else {
        return;
    };
    let at = ch.model.ty_of(arg);
    if at.ty != Ty::Rdd || literal_parallelize(ch, arg, 0) {
        return;
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    if io.has_schema() || !io.complete() {
        return;
    }
    ch.report(
        Diagnostic::new(
            "IOD016",
            "spark.read.json over an RDD without a schema: an extra full job ships every row through Python and infers the schema over all of it before the real read (and df.rdd is unsupported on Spark Connect / serverless).",
            method_range(expr),
        )
        .with_help(
            "Pass an explicit schema (DDL string), infer it from a small sample (F.schema_of_json on one value, or df.limit(1000)), or on Databricks parse into VARIANT with parse_json.",
        ),
    );
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
    let mut d = Diagnostic::new(
        "IOD018",
        format!(
            "{how} lists every partition directory of the table on each run, so the job slows down as the table grows even when it wrote one new partition."
        ),
        range,
    )
    .with_severity(Severity::Info)
    .with_help(
        "Register only the partitions just written (ALTER TABLE t ADD IF NOT EXISTS PARTITION (...)), or use a Delta table, which needs no partition repair.",
    );
    if ch.model.in_loop(&range) {
        d.message
            .push_str(" Inside a loop, the full listing runs on every iteration.");
        d = d.with_severity(Severity::Warning);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ IOD017

/// Transformations that keep the partition count of their input.
const NARROW: &[&str] = &[
    "select",
    "selectExpr",
    "withColumn",
    "withColumns",
    "withColumnRenamed",
    "withColumnsRenamed",
    "drop",
    "filter",
    "where",
    "alias",
    "toDF",
    "fillna",
    "dropna",
    "replace",
    "withMetadata",
    "cache",
    "persist",
    "hint",
];

/// Coalesce/repartition values small enough to cap the connection count.
const SMALL_PARTITIONS: i64 = 64;

/// Whether `df` (followed through narrow transformations and names bound once) was
/// given an explicit partition count that is small or unknown: `coalesce(8)`,
/// `repartition(n)`. `repartition("col")` and `repartition(200)` keep the
/// connection count high.
fn partitions_capped<'e>(ch: &Checker<'e>, mut e: &'e Expr, mut hops: u32) -> bool {
    loop {
        if let Expr::Name(n) = e {
            if hops == 0 {
                return false;
            }
            let Some(v) = ch
                .model
                .single_binding(n)
                .and_then(|b| ch.model.binding(b).value)
            else {
                return false;
            };
            if v.range() == e.range() {
                return false;
            }
            hops -= 1;
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
            return match int_literal(n) {
                Some(v) => v <= SMALL_PARTITIONS,
                // a column name / Column: partitioned by key, count unchanged
                None => !(str_literal(n).is_some() || ch.model.is_column(n)),
            };
        }
        if !NARROW.contains(&m) {
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
    if io.df.is_some_and(|df| partitions_capped(ch, df, 4)) {
        return;
    }
    let range = method_range(expr);
    let mut d = Diagnostic::new(
        "IOD017",
        "JDBC write without numPartitions opens one database connection per partition at once (200 after a shuffle), which can exhaust the database's connection limit and stall or fail the write.",
        range,
    )
    .with_severity(Severity::Info)
    .with_help(
        "Set .option(\"numPartitions\", N) (or properties={\"numPartitions\": \"N\"}) to the number of connections the database can take; Spark coalesces to it.",
    );
    if !io.has("batchsize") {
        d.message.push_str(
            " batchsize is also unset (1000 rows per round trip); raising it usually speeds up bulk writes.",
        );
    }
    if ch.model.in_loop(&range) {
        d.message
            .push_str(" Inside a loop, every iteration opens the connections again.");
        d = d.with_severity(Severity::Warning);
    }
    ch.report(d);
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
    let Expr::Name(n) = e else {
        return None;
    };
    let v = ch
        .model
        .single_binding(n)
        .and_then(|b| ch.model.binding(b).value)?;
    lit(v)
}

const MAX_RECORDS_HELP: &str = "Remove the cap and let file size follow the data (Delta: optimized writes / OPTIMIZE, or delta.targetFileSize). Keep a cap only for very wide rows or a downstream reader with a hard row limit, sized in the millions.";

/// `spark.sql.files.maxRecordsPerFile` set to a small row count: every task's output
/// split into many small files.
fn small_max_records_conf(s: &Setting) -> Option<Finding> {
    let n = small_record_cap(s.value)?;
    Some(
        Finding::new(
            format!(
                "spark.sql.files.maxRecordsPerFile={n} caps every output file of the session at {n} rows: each write task splits its output into many small files (slower listing and opening for every reader, weaker compression and statistics, more compaction work)."
            ),
            MAX_RECORDS_HELP,
        )
        .info(),
    )
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
        action_kind(class, sc.method),
        Some(ActionKind::Write | ActionKind::StreamStart)
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
    ch.report(
        Diagnostic::new(
            "IOD019",
            format!(
                "maxRecordsPerFile={n} caps each output file at {n} rows: every write task splits its output into many small files (slower listing and opening for every reader, weaker compression and statistics, more compaction work)."
            ),
            method_range(expr),
        )
        .with_severity(Severity::Info)
        .with_help(MAX_RECORDS_HELP),
    );
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
    ch.report(
        Diagnostic::new(
            "IOD020",
            format!(
                "{what} over {n} columns: Z-ordering interleaves every column, so each extra one widens the per-file value range of all the others; data skipping weakens for every column while OPTIMIZE still rewrites the data."
            ),
            method_range(expr),
        )
        .with_severity(Severity::Info)
        .with_help(
            "Z-order by the 1-3 high-cardinality columns most used in filters (partition columns never help), or switch the table to liquid clustering (CLUSTER BY) on Databricks.",
        ),
    );
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
