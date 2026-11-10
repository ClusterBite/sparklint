//! Writer-chain rules: PRF010 repartition-before-write, IOD003
//! coalesce-one-before-write, IOD004 insert-into-positional, IOD006 write-in-loop.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::{Ty, call_arg, int_literal};

use super::Rule;
use super::actions::method_range;
use super::helpers::*;
use super::io_settings::{IoSettings, column_name};
use crate::checker::Checker;
use crate::diagnostic::{Diagnostic, Edit, Fix, Severity};

/// PRF010, IOD003/004/006 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PRF010").call(repartition_before_write),
    Rule::new("IOD003").call(coalesce_one_before_write),
    Rule::new("IOD004").call(insert_into_positional),
    Rule::new("IOD006").call(write_in_loop),
];

/// One DataFrame method call below the writer: (method, call, call expr, receiver,
/// whether it is in the same expression as the write).
pub struct DfStep<'e> {
    pub method: &'e str,
    pub call: &'e ast::ExprCall,
    pub expr: &'e Expr,
    pub recv: &'e Expr,
    pub same_expr: bool,
}

/// DataFrame method calls from `df` downwards (closest to the write first),
/// following variables through single bindings.
pub fn df_steps<'e>(ch: &Checker<'e>, df: &'e Expr) -> Vec<DfStep<'e>> {
    let mut out = Vec::new();
    let mut e = df;
    let mut same = true;
    let mut hops = 0;
    loop {
        if let Some(sc) = ch.model.spark_call(e)
            && sc.is_dataframe()
        {
            let (c, r, m) = (sc.call, sc.receiver, sc.method);
            out.push(DfStep {
                method: m,
                call: c,
                expr: e,
                recv: r,
                same_expr: same,
            });
            e = r;
            continue;
        }
        if let Expr::Name(n) = e
            && hops < 3
            && let Some(b) = ch.model.single_binding(n)
            && let Some(v) = ch.model.binding(b).value
            && v.range() != e.range()
        {
            hops += 1;
            same = false;
            e = v;
            continue;
        }
        return out;
    }
}

const CHEAP: &[&str] = &[
    "select",
    "selectExpr",
    "withColumn",
    "withColumns",
    "withColumnRenamed",
    "withColumnsRenamed",
    "drop",
    "sortWithinPartitions",
    "alias",
    "toDF",
    "withMetadata",
];

/// `repartition(n)`/`coalesce(n)` with a single integer-literal argument.
fn single_int_arg(c: &ast::ExprCall) -> Option<i64> {
    if c.arguments.args.len() == 1 && c.arguments.keywords.is_empty() {
        int_literal(&c.arguments.args[0])
    } else {
        call_arg(c, 0, "numPartitions").and_then(int_literal)
    }
}

fn remove_step_fix(step: &DfStep, what: &str) -> Option<Fix> {
    step.same_expr.then(|| {
        Fix::unsafe_(
            format!("Remove .{what}()"),
            vec![Edit::delete(TextRange::new(
                step.recv.range().end(),
                step.expr.range().end(),
            ))],
        )
    })
}

// ------------------------------------------------------------------ PRF010

pub fn repartition_before_write(ch: &mut Checker, _call: &ast::ExprCall, expr: &Expr) {
    let Some(w) = IoSettings::write(ch, expr) else {
        return;
    };
    let Some(df) = w.df else { return };
    let steps = df_steps(ch, df);
    let Some(step) = steps
        .iter()
        .take_while(|s| CHEAP.contains(&s.method) || s.method == "repartition")
        .find(|s| s.method == "repartition")
    else {
        return;
    };
    let c = step.call;
    let n = c.arguments.args.first().and_then(int_literal);
    if n.is_some_and(|n| n >= 100) {
        return; // raising parallelism, not reducing files
    }
    let cols: Vec<String> = c.arguments.args.iter().filter_map(column_name).collect();
    if n.is_none() && !cols.is_empty() {
        let pb = w.partition_by();
        if !pb.is_empty() && cols.iter().all(|c| pb.contains(c)) {
            return; // one file per output partition: intentional
        }
    }
    let mut d = Diagnostic::new(
        "PRF010",
        "repartition() right before a write adds a full shuffle — rely on optimized writes/auto compaction, or repartition by the partitionBy columns.",
        method_range(step.expr),
    );
    if let Some(f) = remove_step_fix(step, "repartition") {
        d = d.with_fix(f);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ IOD003

const SHUFFLES: &[&str] = &[
    "repartition",
    "repartitionByRange",
    "groupBy",
    "groupby",
    "agg",
    "join",
    "crossJoin",
    "distinct",
    "dropDuplicates",
    "orderBy",
    "sort",
    "union",
    "unionByName",
    "unionAll",
];

pub fn coalesce_one_before_write(ch: &mut Checker, _call: &ast::ExprCall, expr: &Expr) {
    let Some(w) = IoSettings::write(ch, expr) else {
        return;
    };
    let Some(df) = w.df else { return };
    let steps = df_steps(ch, df);
    let mut hit = None;
    for s in &steps {
        if matches!(s.method, "coalesce" | "repartition") && single_int_arg(s.call) == Some(1) {
            hit = Some(s);
            break;
        }
        if SHUFFLES.contains(&s.method) || s.method == "coalesce" {
            break;
        }
    }
    let Some(step) = hit else { return };
    let src = ch.model.ty_of(step.recv);
    let small = src.bounded().is_some() || src.facts.reduced;
    let delta = w.v2 || w.format() == Some("delta");
    let mut d = Diagnostic::new(
        "IOD003",
        format!(
            "{}(1) before a write funnels the whole output through one task.",
            step.method
        ),
        method_range(step.expr),
    )
    .with_help(if step.method == "coalesce" {
        "Drop it (Delta optimized writes size files), or use repartition(1) so the upstream stage stays parallel."
    } else {
        "Drop it (Delta optimized writes size files) unless a single-file export is required."
    });
    if small {
        d = d.with_severity(Severity::Info);
    }
    if delta && let Some(f) = remove_step_fix(step, step.method) {
        d = d.with_fix(f);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ IOD004

pub fn insert_into_positional(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["insertInto"]) else {
        return;
    };
    if sc.class() != Ty::DataFrameWriter {
        return;
    }
    let w = IoSettings::write(ch, expr);
    let overwrite_arg = call_arg(c, 1, "overwrite").and_then(bool_literal);
    let mode = w.as_ref().and_then(IoSettings::mode);
    let overwrite = overwrite_arg == Some(true)
        || (overwrite_arg.is_none() && mode.as_deref() == Some("overwrite"));
    let msg = if overwrite {
        "insertInto(overwrite) matches columns by position and may overwrite every partition — use writeTo(t).overwritePartitions()."
    } else {
        "insertInto() matches columns by position — a column-order mismatch silently writes data into the wrong columns; use writeTo(t).append()."
    };
    let mut d = Diagnostic::new("IOD004", msg, method_range(expr));
    if let Some(w) = &w
        && let Some(df) = w.df
        && !w.indirect
        && !w.v2
        && w.calls.iter().all(|(m, _)| *m == "mode")
        && (w.calls.is_empty() || mode.is_some())
        && matches!(mode.as_deref(), None | Some("append") | Some("overwrite"))
        && let Some(table) = call_arg(c, 0, "tableName")
        && c.arguments.args.len() + c.arguments.keywords.len()
            == 1 + usize::from(overwrite_arg.is_some())
    {
        let terminal = if overwrite {
            "overwritePartitions()"
        } else {
            "append()"
        };
        let text = format!(
            "{}.writeTo({}).{terminal}",
            paren_src(ch, df),
            ch.slice(table.range())
        );
        d = d.with_fix(Fix::unsafe_(
            format!("Replace with `{text}`"),
            vec![Edit::replace(expr.range(), text)],
        ));
    }
    ch.report(d);
}

// ------------------------------------------------------------------ IOD006

/// Whether the write appends (`mode("append")`, `mode="append"`, `insertInto`
/// without overwrite, `writeTo(..).append()`).
fn appends(w: &IoSettings, c: &ast::ExprCall) -> bool {
    if w.v2 {
        return w.method == "append";
    }
    let mode = w.mode();
    if w.method == "insertInto" {
        let overwrite = call_arg(c, 1, "overwrite").and_then(bool_literal);
        return match overwrite {
            Some(o) => !o,
            None => matches!(mode.as_deref(), None | Some("append")),
        };
    }
    mode.as_deref() == Some("append")
}

pub fn write_in_loop(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(w) = IoSettings::write(ch, expr) else {
        return;
    };
    let loops = ch.model.loops_at(expr.start());
    let Some(inner) = loops.last().copied() else {
        return;
    };
    if !appends(&w, c) {
        return;
    }
    let Some(target) = w.target() else {
        return;
    };
    // Only one target per iteration: a target derived from the loop variable is
    // orchestration over many tables (ACT005 covers it).
    let targets = super::driver::loop_targets(ch, inner);
    if super::driver::varies_per_iteration(ch, target, inner, &targets) {
        return;
    }
    let small_literal = loops.iter().all(|l| l.literal_len.is_some_and(|n| n <= 10));
    let mut d = Diagnostic::new(
        "IOD006",
        format!(
            "Append to the same target {} inside a loop: one commit and a new set of small files per iteration.",
            ch.slice(target.range())
        ),
        method_range(expr),
    )
    .with_site(expr.range())
    .with_help("Build the per-iteration frames, union them (reduce(DataFrame.unionByName, frames)) and write once, or compute all iterations in one set-based query.");
    if small_literal {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}
