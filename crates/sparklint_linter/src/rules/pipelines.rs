//! Lakeflow / DLT pipeline rules: DBX017 pipeline-stream-fanout.
//!
//! A pipeline where one streaming source feeds many datasets (`@dp.table` /
//! `@dlt.table` / `@dp.append_flow` functions, often generated in a `for` loop) runs one
//! streaming query per dataset, and every query reads every micro-batch of the source
//! on its own. The cluster has to be sized for N concurrent queries even when each does
//! little work. The ForEachBatch sink (`@dp.foreach_batch_sink`, Public Preview) reads
//! the source once per micro-batch and writes all targets from one handler.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, Expr, Stmt};
use ruff_text_size::{Ranged, TextRange};
use rustc_hash::FxHashMap;
use sparklint_semantic::{Ty, static_text};

use super::Rule;
use super::io_settings::IoSettings;
use crate::checker::Checker;
use crate::diagnostic::Diagnostic;

/// DBX017 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[Rule::new("DBX017").init(stream_fanout)];

/// Pipeline modules: `import dlt` and `from pyspark import pipelines as dp`.
const PIPELINE_MODULES: &[&str] = &["dlt", "pyspark.pipelines"];
/// Decorators that define a dataset or flow whose function returns its query.
const DATASET_DECORATORS: &[&str] = &[
    "table",
    "streaming_table",
    "materialized_view",
    "view",
    "temporary_view",
    "append_flow",
];

/// A loop count we can't see (`for t in config["targets"]`).
const UNKNOWN: u32 = u32::MAX;

/// One pipeline function that streams a source.
struct Reader {
    /// The streaming read call (reported on the first reader).
    read: TextRange,
    /// The dataset or flow name (`name=` / the function name).
    dataset: String,
    /// Datasets this function defines: 1, the literal loop length, or [`UNKNOWN`].
    copies: u32,
    /// The source as written (`raw_events`, `kafka topic orders`).
    shown: String,
}

fn stream_fanout(ch: &mut Checker) {
    let found = {
        let module = ch.module;
        let mut defs = Defs {
            ch,
            loops: Vec::new(),
            found: Vec::new(),
        };
        defs.visit_body(&module.body);
        defs.found
    };

    let mut groups: FxHashMap<String, Vec<Reader>> = FxHashMap::default();
    let mut order: Vec<String> = Vec::new();
    for (key, r) in found {
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups.entry(key).or_default().push(r);
    }
    let min = ch.settings.pipeline_fanout_min_flows;
    if min == 0 {
        return;
    }
    for key in order {
        let readers = &groups[&key];
        let unknown = readers.iter().any(|r| r.copies == UNKNOWN);
        let total: u64 = readers
            .iter()
            .map(|r| {
                if r.copies == UNKNOWN {
                    1
                } else {
                    r.copies as u64
                }
            })
            .sum();
        // A loop of unknown length generates a dataset per config entry: that is the
        // fan-out pattern itself, so it counts as reaching the threshold.
        if !unknown && total < min as u64 {
            continue;
        }
        let first = &readers[0];
        let by = if unknown {
            "pipeline datasets/flows generated in a loop".to_string()
        } else {
            format!("{total} pipeline datasets/flows")
        };
        let mut names: Vec<String> = Vec::new();
        for r in readers {
            let line = ch.source.line_index.line_of(r.read.start());
            let label = if r.copies == UNKNOWN {
                format!("`{}` (one per loop item, line {line})", r.dataset)
            } else if r.copies > 1 {
                format!("`{}` ({} from a loop, line {line})", r.dataset, r.copies)
            } else {
                format!("`{}` (line {line})", r.dataset)
            };
            names.push(label);
        }
        let msg = format!(
            "{} is streamed separately by {by}: {}. Each one is its own \
             streaming query that re-reads every micro-batch, so the cluster is sized for all of \
             them running at once.",
            first.shown,
            names.join(", ")
        );
        let d = Diagnostic::new("DBX017", msg, first.read).with_help(
            "Read the source once: point one @dp.append_flow at a @dp.foreach_batch_sink \
             (Public Preview, pipeline channel PREVIEW) whose handler does batch_df.persist(), \
             writes each target (with txnAppId/txnVersion options so retried batches don't \
             duplicate rows), then unpersist(). Trade-offs: the targets stop being \
             pipeline-managed tables (no expectations, lineage or full-refresh cleanup) and \
             writes to different targets are not atomic.",
        );
        ch.report(d);
    }
}

/// Collects pipeline functions and the streaming sources they read.
struct Defs<'c, 'a> {
    ch: &'c Checker<'a>,
    /// Literal lengths (or [`UNKNOWN`]) of the enclosing `for` loops.
    loops: Vec<u32>,
    found: Vec<(String, Reader)>,
}

impl<'a> Visitor<'a> for Defs<'_, 'a> {
    fn visit_stmt(&mut self, stmt: &'a Stmt) {
        match stmt {
            Stmt::For(f) => {
                self.loops.push(loop_len(self.ch, &f.iter));
                visitor::walk_stmt(self, stmt);
                self.loops.pop();
            }
            Stmt::FunctionDef(f) => {
                if let Some(name) = self.dataset_name(f) {
                    self.record(f, name);
                }
                // Nested defs inside a pipeline function are not datasets; still walk
                // so dataset defs nested in helper functions (generator helpers) count.
                visitor::walk_stmt(self, stmt);
            }
            _ => visitor::walk_stmt(self, stmt),
        }
    }
}

impl<'a> Defs<'_, 'a> {
    /// The dataset name if `f` is decorated with a pipeline dataset/flow decorator.
    fn dataset_name(&self, f: &ast::StmtFunctionDef) -> Option<String> {
        for dec in &f.decorator_list {
            let (callee, call) = match &dec.expression {
                Expr::Call(c) => (&*c.func, Some(c)),
                e => (e, None),
            };
            let Some(q) = self.ch.model.qualified_name(callee) else {
                continue;
            };
            let Some((module, attr)) = q.rsplit_once('.') else {
                continue;
            };
            if !PIPELINE_MODULES.contains(&module) || !DATASET_DECORATORS.contains(&attr) {
                continue;
            }
            let named = call.and_then(|c| {
                c.arguments
                    .keywords
                    .iter()
                    .find(|k| k.arg.as_ref().is_some_and(|a| a.as_str() == "name"))
                    .map(|k| match &k.value {
                        Expr::StringLiteral(s) => s.value.to_str().to_string(),
                        v => self.ch.slice(v.range()).to_string(),
                    })
            });
            return Some(named.unwrap_or_else(|| f.name.as_str().to_string()));
        }
        None
    }

    fn record(&mut self, f: &'a ast::StmtFunctionDef, dataset: String) {
        let copies = self.loops.iter().fold(1u32, |acc, &n| {
            if acc == UNKNOWN || n == UNKNOWN {
                UNKNOWN
            } else {
                acc.saturating_mul(n)
            }
        });
        let mut reads = Reads {
            ch: self.ch,
            out: Vec::new(),
        };
        reads.visit_body(&f.body);
        // One reader per function and source (a self-join reads it twice in one query).
        let mut seen: Vec<String> = Vec::new();
        for (key, shown, read) in reads.out {
            if seen.contains(&key) {
                continue;
            }
            seen.push(key.clone());
            self.found.push((
                key,
                Reader {
                    read,
                    dataset: dataset.clone(),
                    copies,
                    shown,
                },
            ));
        }
    }
}

/// Literal length of a `for` loop's iterable, or [`UNKNOWN`]. A name bound once at
/// module level to a literal (`REGIONS = ["us", "eu"]`) is followed.
fn loop_len(ch: &Checker, iter: &Expr) -> u32 {
    if let Expr::Name(n) = iter {
        let mut found = None;
        for st in &ch.module.body {
            let (targets, value) = match st {
                Stmt::Assign(a) => (a.targets.iter().collect::<Vec<_>>(), &*a.value),
                Stmt::AnnAssign(a) => match &a.value {
                    Some(v) => (vec![&*a.target], &**v),
                    None => continue,
                },
                Stmt::AugAssign(a) => (vec![&*a.target], &*a.value),
                _ => continue,
            };
            if targets
                .iter()
                .any(|t| matches!(t, Expr::Name(t) if t.id == n.id))
            {
                if found.is_some() || matches!(st, Stmt::AugAssign(_)) {
                    return UNKNOWN;
                }
                found = Some(value);
            }
        }
        return found.map_or(UNKNOWN, literal_len);
    }
    literal_len(iter)
}

fn literal_len(iter: &Expr) -> u32 {
    match iter {
        Expr::List(l) => l.elts.len() as u32,
        Expr::Tuple(t) => t.elts.len() as u32,
        Expr::Set(s) => s.elts.len() as u32,
        Expr::Dict(d) => d.items.len() as u32,
        _ => UNKNOWN,
    }
}

/// Streaming reads inside one pipeline function.
struct Reads<'c, 'a> {
    ch: &'c Checker<'a>,
    /// (grouping key, shown source, read call range)
    out: Vec<(String, String, TextRange)>,
}

impl<'a> Visitor<'a> for Reads<'_, 'a> {
    fn visit_stmt(&mut self, stmt: &'a Stmt) {
        // A nested def is a separate function (often a foreach_batch handler).
        if matches!(stmt, Stmt::FunctionDef(_) | Stmt::ClassDef(_)) {
            return;
        }
        visitor::walk_stmt(self, stmt);
    }

    fn visit_expr(&mut self, expr: &'a Expr) {
        if let Some((key, shown)) = self.stream_source(expr) {
            self.out.push((key, shown, expr.range()));
        }
        visitor::walk_expr(self, expr);
    }
}

impl Reads<'_, '_> {
    /// `(key, shown)` for a streaming read of a fixed source.
    fn stream_source(&self, e: &Expr) -> Option<(String, String)> {
        let Expr::Call(c) = e else { return None };
        // dlt.read_stream("x") / dp.read_stream("x")
        if let Some(q) = self.ch.model.qualified_name(&c.func)
            && let Some((module, "read_stream")) = q.rsplit_once('.')
            && PIPELINE_MODULES.contains(&module)
        {
            let arg = c.arguments.args.first()?;
            return table_key(arg);
        }
        let sc = self.ch.model.spark_call_named(e, &["table", "load"])?;
        if sc.class() != Ty::DataStreamReader {
            return None;
        }
        let Expr::Attribute(a) = &*c.func else {
            return None;
        };
        if a.attr.as_str() == "table" {
            return table_key(c.arguments.args.first()?);
        }
        // load(path) or a Kafka/Event Hubs-style load() keyed by its topic option.
        if let Some(path) = c.arguments.args.first() {
            let p = static_text(path)?;
            if p.contains("{}") {
                return None;
            }
            return Some((format!("path:{p}"), format!("`{p}`")));
        }
        let io = IoSettings::of(self.ch, &sc)?;
        topic_key(&io)
    }
}

/// Grouping key for a table name (`live.` prefix and case ignored).
fn table_key(arg: &Expr) -> Option<(String, String)> {
    let t = static_text(arg)?;
    if t.contains("{}") {
        // Differs per loop iteration (f-string): not a shared source.
        return None;
    }
    let lower = t.to_ascii_lowercase();
    let key = lower.strip_prefix("live.").unwrap_or(&lower).to_string();
    Some((format!("table:{key}"), format!("`{t}`")))
}

/// `…option("subscribe", "orders")…load()`: key on the topic option.
fn topic_key(io: &IoSettings) -> Option<(String, String)> {
    const TOPIC_OPTS: &[&str] = &[
        "subscribe",
        "subscribepattern",
        "assign",
        "topic",
        "eventhubs.name",
    ];
    let (k, v) = TOPIC_OPTS
        .iter()
        .find_map(|k| io.option(k).map(|v| (k, v)))?;
    let v = static_text(v)?;
    if v.contains("{}") {
        return None;
    }
    Some((format!("topic:{k}={v}"), format!("Topic `{v}`")))
}
