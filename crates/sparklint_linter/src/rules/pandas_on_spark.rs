//! pandas-on-Spark rules: PDS005 ops-on-diff-frames, PDS006 single-partition-op.

use ruff_python_ast::{self as ast, Expr, Stmt};
use ruff_text_size::Ranged;
use sparklint_semantic::{Ty, call_arg, static_text};

use super::Rule;
use super::actions::method_range;
use super::helpers::*;
use crate::checker::Checker;
use crate::diagnostic::Diagnostic;

/// PDS005/006 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PDS005")
        .stmt(pds005_stmt)
        .call(ops_on_diff_frames_call),
    Rule::new("PDS006").call(single_partition_op),
];

fn pds005_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::Assign(a) = stmt {
        ops_on_diff_frames_assign(ch, a);
    }
}

// ------------------------------------------------------------------ PDS005

const OPS_ON_DIFF: &str = "compute.ops_on_diff_frames";
/// pandas-on-Spark stores its options in the Spark conf under this prefix
/// (`pyspark/pandas/config.py`: `_key_format = "pandas_on_Spark.{}".format`).
const OPS_ON_DIFF_CONF: &str = "pandas_on_Spark.compute.ops_on_diff_frames";
const OPS_ON_DIFF_ATTR: &str = "pyspark.pandas.options.compute.ops_on_diff_frames";

fn pds005(ch: &mut Checker, range: ruff_text_size::TextRange) {
    ch.report(
        Diagnostic::new(
            "PDS005",
            "compute.ops_on_diff_frames=True: every operation between two different pandas-on-Spark frames silently becomes a join (a shuffle per expression).",
            range,
        )
        .with_help(
            "Keep the default (False) and combine the data explicitly once (merge/join, or ps.concat), so the cost is visible.",
        ),
    );
}

fn is_true(e: &Expr) -> bool {
    bool_literal(e) == Some(true)
}

/// `ps.set_option("compute.ops_on_diff_frames", True)`,
/// `ps.option_context("compute.ops_on_diff_frames", True, ...)`,
/// `spark.conf.set("pandas_on_Spark.compute.ops_on_diff_frames", "true")` and the
/// `SparkSession.builder.config(...)` form.
pub fn ops_on_diff_frames_call(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let q = ch.model.qualified_name(&c.func);
    let hit = match q.as_deref() {
        Some("pyspark.pandas.set_option" | "pyspark.pandas.config.set_option") => {
            call_arg(c, 0, "key").and_then(static_text).as_deref() == Some(OPS_ON_DIFF)
                && call_arg(c, 1, "value").is_some_and(is_true)
        }
        Some("pyspark.pandas.option_context" | "pyspark.pandas.config.option_context") => {
            let args = &c.arguments.args;
            args.iter().all(|a| !a.is_starred_expr())
                && args.chunks(2).any(|p| {
                    p.len() == 2
                        && static_text(&p[0]).as_deref() == Some(OPS_ON_DIFF)
                        && is_true(&p[1])
                })
        }
        _ => {
            let Some(sc) = ch.model.spark_call_named(expr, &["set", "config"]) else {
                return;
            };
            let key_kw = match (sc.class(), sc.method) {
                (Ty::RuntimeConfig, "set") => "key",
                (Ty::SessionBuilder, "config") => "key",
                _ => return,
            };
            call_arg(c, 0, key_kw)
                .and_then(static_text)
                .is_some_and(|k| k.trim() == OPS_ON_DIFF_CONF)
                && call_arg(c, 1, "value").is_some_and(is_true)
        }
    };
    if hit {
        pds005(ch, method_range(expr));
    }
}

/// `ps.options.compute.ops_on_diff_frames = True`.
pub fn ops_on_diff_frames_assign(ch: &mut Checker, a: &ast::StmtAssign) {
    if !is_true(&a.value) {
        return;
    }
    for t in &a.targets {
        if ch.model.qualified_name(t).as_deref() == Some(OPS_ON_DIFF_ATTR) {
            pds005(ch, t.range());
        }
    }
}

// ------------------------------------------------------------------ PDS006

const SINGLE_PARTITION_OPS: &[&str] = &[
    "rank",
    "shift",
    "diff",
    "pct_change",
    "cumsum",
    "cumprod",
    "cummax",
    "cummin",
];

/// `psdf.cumsum()` / `psdf["x"].shift(1)` on an un-grouped pandas-on-Spark frame: a
/// Spark Window without a partition spec, so every row goes through one task.
/// `psdf.groupby(k)...` results are not frames to the oracle and are never reported.
pub fn single_partition_op(ch: &mut Checker, _call: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, SINGLE_PARTITION_OPS) else {
        return;
    };
    if sc.class() != Ty::PandasOnSparkFrame || !sc.is_certain() {
        return;
    }
    let (recv, m) = (sc.receiver, sc.method);
    // `psdf.shift(1).cumsum()`: report the innermost op only (one shuffle to one task).
    if ch
        .model
        .spark_call_named(recv, SINGLE_PARTITION_OPS)
        .is_some_and(|inner| inner.class() == Ty::PandasOnSparkFrame)
    {
        return;
    }
    ch.report(
        Diagnostic::new(
            "PDS006",
            format!(
                "{m}() on an un-grouped pandas-on-Spark frame runs over a window with no partition: all rows move to a single partition."
            ),
            method_range(expr),
        )
        .with_help(format!(
            "Group first (psdf.groupby(key).{m}()) or use a Spark Window.partitionBy(...); fine on small frames (# noqa: PDS006)."
        )),
    );
}
