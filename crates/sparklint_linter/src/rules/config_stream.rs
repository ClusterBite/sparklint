//! IOD002 jdbc-read-unpartitioned, CFG003 static-conf-at-runtime,
//! STM001 stream-missing-checkpoint, STM002 trigger-once-deprecated,
//! STM010 kafka-stream-uncapped-batch, STM011 heap-state-store-explicit.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange, TextSize};
use sparklint_semantic::Ty;

use super::Rule;
use super::actions::method_range;
use super::conf::{Conf, Finding, Setting, Via};
use super::io_settings::IoSettings;
use crate::checker::Checker;
use crate::diagnostic::{Diagnostic, Edit, Fix, Severity};

/// IOD002, CFG003, STM001/002/010/011 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("IOD002").call(jdbc_read_unpartitioned),
    Rule::new("CFG003").call(static_conf_at_runtime),
    Rule::new("STM001").call(stream_missing_checkpoint),
    Rule::new("STM002").call(trigger_once_deprecated),
    Rule::new("STM010").call(kafka_stream_uncapped_batch),
    Rule::new("STM011").conf(&Conf::new(
        &["spark.sql.streaming.stateStore.providerClass"],
        heap_state_store_explicit,
    )),
];

// ------------------------------------------------------------------ STM011

/// True for a provider class value that names Spark's JVM-heap state store.
fn is_heap_provider(v: &str) -> bool {
    let v = v.trim();
    let last = v.rsplit('.').next().unwrap_or(v);
    last.eq_ignore_ascii_case("HDFSBackedStateStoreProvider")
}

fn heap_state_store_explicit(s: &Setting) -> Option<Finding> {
    is_heap_provider(s.value).then(|| {
        Finding::new(
            "Streaming state store set to HDFSBackedStateStoreProvider: all state lives in the executor JVM heap, so as the number of keys grows micro-batches slow down in garbage collection and executors run out of memory. A query's provider cannot change for an existing checkpoint, so this choice is locked in.",
            "Use the RocksDB provider (org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider; the Databricks default from DBR 17.3), or remove the line. Keep the heap store only for queries with small, bounded state.",
        )
        .info()
    })
}

// ------------------------------------------------------------------ IOD002

fn small_query(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    t.contains(" where ") || t.contains(" limit ") || t.contains(" top ")
}

pub fn jdbc_read_unpartitioned(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call(expr) else {
        return;
    };
    if sc.class() != Ty::DataFrameReader {
        return;
    }
    if !matches!(sc.method, "jdbc" | "load") {
        return;
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    if io.format() != Some("jdbc") || !io.complete() {
        return;
    }
    if io.has("partitioncolumn") || io.has("predicates") {
        return;
    }
    let small = io.has("query")
        || ["dbtable", "query"]
            .iter()
            .any(|k| io.text(k).is_some_and(|t| small_query(&t)));
    let mut d = Diagnostic::new(
        "IOD002",
        "JDBC read without partitionColumn/lowerBound/upperBound/numPartitions (or predicates) runs on a single connection and task.",
        method_range(expr),
    )
    .with_help("Add column/lowerBound/upperBound/numPartitions (or predicates=[...]) so the read is split across executors.");
    if small {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ CFG008 (JDBC half)

/// CFG008 for JDBC reads: `pushDownPredicate` set false on a `format("jdbc")`
/// reader chain, or in the `properties` dict of `spark.read.jdbc(...)`.
pub(crate) fn jdbc_pushdown_disabled(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["load", "jdbc"]) else {
        return;
    };
    if sc.class() != Ty::DataFrameReader {
        return;
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    let disabled = io.format() == Some("jdbc")
        && io
            .text("pushdownpredicate")
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("false"));
    if disabled {
        ch.report(
            Diagnostic::new(
                "CFG008",
                "JDBC read with pushDownPredicate=false: filters are not sent to the database, so the whole table is copied over the network and filtered in Spark.",
                method_range(expr),
            )
            .with_help(
                "Drop the option (the default pushes filters down), or put the filter into the `query`/`dbtable` subquery yourself.",
            ),
        );
    }
}

// ------------------------------------------------------------------ CFG003

const STATIC_PREFIXES: &[&str] = &[
    "spark.executor.",
    "spark.driver.",
    "spark.dynamicallocation.",
    "spark.jars",
    "spark.submit.",
    "spark.memory.",
    "spark.databricks.cluster.",
    "spark.yarn.",
    "spark.kubernetes.",
];

const STATIC_KEYS: &[&str] = &[
    "spark.serializer",
    "spark.master",
    "spark.task.cpus",
    "spark.sql.extensions",
    "spark.sql.catalogimplementation",
    "spark.default.parallelism",
    "spark.local.dir",
    "spark.sql.warehouse.dir",
    "spark.cores.max",
];

fn is_static_conf(key: &str) -> bool {
    let k = key.trim().to_ascii_lowercase();
    STATIC_KEYS.contains(&k.as_str()) || STATIC_PREFIXES.iter().any(|p| k.starts_with(p))
}

/// What a notebook file says about its Spark session already running.
#[derive(Debug, Clone, Copy, Default)]
pub struct SessionEvidence {
    /// `spark` / `sc` / `sqlContext` / `dbutils` / `displayHTML` used without the file
    /// ever defining them: the platform provides them, with a live session.
    implicit_session: bool,
    /// First use of the undefined `display` global (IPython has one too, so it only
    /// counts before the session is built).
    first_display: Option<TextSize>,
    /// End of the first `….getOrCreate()` call.
    first_get_or_create: Option<TextSize>,
}

fn session_evidence(ch: &Checker) -> SessionEvidence {
    struct V<'c, 'a> {
        ch: &'c Checker<'a>,
        ev: SessionEvidence,
    }
    impl<'b> Visitor<'b> for V<'_, '_> {
        fn visit_expr(&mut self, e: &'b Expr) {
            match e {
                // A load of a name no binding reaches (assignment targets have no use site).
                Expr::Name(n)
                    if self
                        .ch
                        .model
                        .use_site(n)
                        .is_some_and(|u| u.reaching.is_empty()) =>
                {
                    match n.id.as_str() {
                        "spark" | "sc" | "sqlContext" | "dbutils" | "displayHTML" => {
                            self.ev.implicit_session = true;
                        }
                        "display" => {
                            self.ev.first_display.get_or_insert(n.start());
                        }
                        _ => {}
                    }
                }
                Expr::Call(c) => {
                    if let Expr::Attribute(a) = &*c.func
                        && a.attr.as_str() == "getOrCreate"
                    {
                        let end = self
                            .ev
                            .first_get_or_create
                            .map_or(c.end(), |p| p.min(c.end()));
                        self.ev.first_get_or_create = Some(end);
                    }
                }
                _ => {}
            }
            visitor::walk_expr(self, e);
        }
    }
    let mut v = V {
        ch,
        ev: SessionEvidence::default(),
    };
    v.visit_body(&ch.module.body);
    v.ev
}

/// Whether the session already runs when the builder / `SparkConf` code at `at`
/// executes, so a static setting made there is ignored. True in a Databricks notebook
/// (and when the user forces notebook mode on a module); in any other notebook (plain
/// Jupyter, where `SparkSession.builder` normally *creates* the session) only with
/// evidence in the file: the platform globals used without a definition, or an
/// earlier `getOrCreate()`.
fn session_already_running(ch: &mut Checker, at: TextRange) -> bool {
    if ch.source.is_databricks() || !ch.source.is_notebook() {
        return true;
    }
    let ev = match ch.state.session_evidence {
        Some(ev) => ev,
        None => {
            let ev = session_evidence(ch);
            ch.state.session_evidence = Some(ev);
            ev
        }
    };
    ev.implicit_session
        || ev.first_display.is_some_and(|d| d < at.start())
        || ev.first_get_or_create.is_some_and(|g| g <= at.start())
}

pub fn static_conf_at_runtime(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    for w in super::conf::writes(ch, c, expr) {
        if !is_static_conf(&w.key) {
            continue;
        }
        let key = &w.name;
        if !w.via.is_runtime() && !(ch.is_notebook() && session_already_running(ch, w.at)) {
            continue;
        }
        let d = match w.via {
            v if v.is_runtime() => Diagnostic::new(
                "CFG003",
                format!(
                    "{key} cannot be changed at runtime — this has no effect (and raises CANNOT_MODIFY_CONFIG on Spark Connect/serverless)."
                ),
                w.at,
            ),
            Via::Builder if ch.is_notebook() => Diagnostic::new(
                "CFG003",
                format!("{key} is a static setting: SparkSession.builder.config() on the already-running notebook session ignores it."),
                w.at,
            )
            .with_severity(Severity::Warning),
            Via::SparkConf if ch.is_notebook() => Diagnostic::new(
                "CFG003",
                format!("{key} is a static setting: a SparkConf built in a notebook cannot change the already-running session, which ignores it."),
                w.at,
            )
            .with_severity(Severity::Warning),
            _ => continue,
        };
        ch.report(d.with_help("Set it in the cluster / job compute Spark config instead."));
    }
}

// ------------------------------------------------------------------ STM001

pub fn stream_missing_checkpoint(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["start", "toTable"]) else {
        return;
    };
    if sc.class() != Ty::DataStreamWriter {
        return;
    }
    let m = sc.method;
    if super::conf::file_sets(ch, "spark.sql.streaming.checkpointlocation") {
        return; // a session-level default is configured in this file
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    if !io.complete() || io.has("checkpointlocation") {
        return;
    }
    if matches!(io.format(), Some("console" | "memory" | "noop")) {
        return; // debug sinks (ACT010)
    }
    ch.report(
        Diagnostic::new(
            "STM001",
            format!("writeStream.{m}() without checkpointLocation — no exactly-once, progress is lost on restart."),
            method_range(expr),
        )
        .with_help(".option(\"checkpointLocation\", f\"{CHECKPOINT_ROOT}/<query>\") — one location per query."),
    );
}

// ------------------------------------------------------------------ STM010

/// Options that cap how much a Kafka micro-batch reads.
const KAFKA_CAPS: &[&str] = &["maxoffsetspertrigger", "maxbytespertrigger"];

pub fn kafka_stream_uncapped_batch(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["load"]) else {
        return;
    };
    if sc.class() != Ty::DataStreamReader || !sc.is_certain() {
        return;
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    if io.format() != Some("kafka") || !io.complete() {
        return;
    }
    if KAFKA_CAPS.iter().any(|k| io.has(k)) {
        return;
    }
    let backlog = io
        .text("startingoffsets")
        .map(|v| v.trim().to_ascii_lowercase())
        .is_some_and(|v| v == "earliest" || v.starts_with('{'));
    let mut d = Diagnostic::new(
        "STM010",
        if backlog {
            "Kafka stream from the earliest offsets without maxOffsetsPerTrigger: the first micro-batch reads the whole retained topic at once (long batch, executor OOM risk, cluster sized for the spike)."
        } else {
            "Kafka stream without maxOffsetsPerTrigger: after downtime or a lagging restart, one micro-batch reads the whole backlog at once."
        },
        method_range(expr),
    )
    .with_help(
        "Add .option(\"maxOffsetsPerTrigger\", <records per batch>); trigger(availableNow=True) honours it and drains a backlog in several batches. Low-volume topics can ignore this.",
    );
    if !backlog {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ STM002

pub fn trigger_once_deprecated(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["trigger"]) else {
        return;
    };
    if sc.class() != Ty::DataStreamWriter {
        return;
    }
    let Some(kw) = c.arguments.keywords.iter().find(|k| {
        k.arg.as_ref().is_some_and(|a| a.as_str() == "once")
            && matches!(&k.value, Expr::BooleanLiteral(b) if b.value)
    }) else {
        return;
    };
    let mut d = Diagnostic::new(
        "STM002",
        "trigger(once=True) is deprecated — use trigger(availableNow=True) (respects rate limits, processes the backlog in batches).",
        kw.range,
    );
    if ch.settings.spark_version >= (3, 3) {
        d = d.with_fix(Fix::safe(
            "Replace with `availableNow=True`",
            vec![Edit::replace(kw.range, "availableNow=True")],
        ));
    }
    ch.report(d);
}

#[cfg(test)]
mod tests {
    use super::is_heap_provider;

    #[test]
    fn heap_provider_names() {
        for v in [
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
            " HDFSBackedStateStoreProvider ",
            "org.apache.spark.sql.execution.streaming.state.hdfsbackedstatestoreprovider",
        ] {
            assert!(is_heap_provider(v), "{v}");
        }
        for v in [
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
            "com.databricks.sql.streaming.state.RocksDBStateStoreProvider",
            "com.example.HDFSBackedStateStoreProviderWrapper",
            "",
        ] {
            assert!(!is_heap_provider(v), "{v}");
        }
    }
}
