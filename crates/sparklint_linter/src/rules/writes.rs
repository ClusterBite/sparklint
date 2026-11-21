//! Writer-chain rules: PRF010 repartition-before-write, IOD003
//! coalesce-one-before-write, IOD004 insert-into-positional, IOD006 write-in-loop.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::columns;
use sparklint_semantic::transform::{self, Transform};
use sparklint_semantic::udf_refs::UdfRefs;
use sparklint_semantic::{Hops, Ty, call_arg};

use super::Rule;
use super::helpers::*;
use super::io_settings::{IoSettings, SaveMode};
use super::msg::Msg;
use crate::checker::Checker;
use crate::diagnostic::{Edit, Fix, Severity};
use crate::msg;

/// PRF010, IOD003/004/006 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PRF010")
        .call(repartition_before_write)
        .msgs(&[PRF010]),
    Rule::new("IOD003")
        .call(coalesce_one_before_write)
        .msgs(&[IOD003_COALESCE, IOD003_REPARTITION]),
    Rule::new("IOD004").call(insert_into_positional).msgs(&[
        IOD004_APPEND,
        IOD004_OVERWRITE,
        IOD004_LAYOUT,
    ]),
    Rule::new("IOD006").call(write_in_loop).msgs(&[IOD006]),
];

const PRF010: Msg = Msg::new(
    "PRF010",
    "repartition-before-write",
    "repartition() right before a write adds a full shuffle — rely on optimized writes/auto compaction, or repartition by the partitionBy columns.",
);
const IOD003_COALESCE: Msg = Msg::new(
    "IOD003",
    "coalesce",
    "{call} before a write funnels the whole output through one task.",
)
.help("Drop it (Delta optimized writes size files), or use repartition(1) so the upstream stage stays parallel.");
const IOD003_REPARTITION: Msg = Msg::new(
    "IOD003",
    "repartition",
    "{call} before a write funnels the whole output through one task.",
)
.help("Drop it (Delta optimized writes size files) unless a single-file export is required.");
const IOD004_OVERWRITE: Msg = Msg::new(
    "IOD004",
    "overwrite",
    "insertInto(overwrite) matches columns by position and may overwrite every partition — use writeTo(t).overwritePartitions().",
);
const IOD004_APPEND: Msg = Msg::new(
    "IOD004",
    "append",
    "insertInto() matches columns by position — a column-order mismatch silently writes data into the wrong columns; use writeTo(t).append().",
);
const IOD004_LAYOUT: Msg = Msg::new(
    "IOD004",
    "layout",
    "insertInto() cannot be combined with {step}(): Spark rejects the write because the target table's definition sets its layout.",
)
.help("Drop the {step}() call (the table's own layout is used), and write by name with writeTo(t).append().");
const IOD006: Msg = Msg::new(
    "IOD006",
    "append-in-loop",
    "Append to the same target {target} inside a loop: one commit and a new set of small files per iteration.",
)
.help("Build the per-iteration frames, union them (reduce(DataFrame.unionByName, frames)) and write once, or compute all iterations in one set-based query.");

/// One DataFrame method call below the writer: (method, its transformation row, call,
/// call expr, receiver, whether it is in the same expression as the write).
#[derive(Clone)]
struct DfStep<'e> {
    method: &'e str,
    row: Option<&'static Transform>,
    call: &'e ast::ExprCall,
    expr: &'e Expr,
    recv: &'e Expr,
    same_expr: bool,
}

impl DfStep<'_> {
    /// The step as written (method + argument text): what two paths must share.
    fn key<'s>(&self, ch: &Checker<'s>) -> (&str, &'s str) {
        (self.method, ch.slice(self.call.arguments.range))
    }
}

/// At most this many paths are followed below one write; past it a branch point ends
/// every path there (fewer steps known, never a guessed one).
const MAX_PATHS: usize = 32;

/// DataFrame method calls from `df` downwards (closest to the write first), following
/// names through every reaching binding (the values module's [`Hops`]): a name bound on
/// several paths continues with the steps every path agrees on (same method and
/// arguments, from the write side) and stops where they differ or a path holds no
/// known value.
fn df_steps<'e>(ch: &Checker<'e>, df: &'e Expr) -> Vec<DfStep<'e>> {
    let mut paths = df_step_paths(ch, df);
    let first = paths.swap_remove(0);
    let agreed = first
        .iter()
        .enumerate()
        .take_while(|(i, s)| {
            paths
                .iter()
                .all(|p| p.get(*i).is_some_and(|o| o.key(ch) == s.key(ch)))
        })
        .count();
    first.into_iter().take(agreed).collect()
}

/// Every way `df` can be built: one step list per path through the reaching bindings
/// (closest to the write first, never empty: a path that ends at an unknown value is
/// the steps up to it). Rules that ask "does EVERY path do X" read these; rules that
/// need one shared step read [`df_steps`].
fn df_step_paths<'e>(ch: &Checker<'e>, df: &'e Expr) -> Vec<Vec<DfStep<'e>>> {
    steps_from(ch, df, Hops::DEFAULT, true)
}

fn steps_from<'e>(
    ch: &Checker<'e>,
    mut e: &'e Expr,
    mut hops: Hops,
    mut same: bool,
) -> Vec<Vec<DfStep<'e>>> {
    let mut out = Vec::new();
    loop {
        if let Some(sc) = ch.model.spark_call(e)
            && sc.is_dataframe()
        {
            out.push(DfStep {
                method: sc.method,
                row: transform::of_call(&sc),
                call: sc.call,
                expr: e,
                recv: sc.receiver,
                same_expr: same,
            });
            e = sc.receiver;
            continue;
        }
        let Some((reached, h)) = ch.model.hop(e, hops) else {
            return vec![out];
        };
        let Some(values) = reached.iter().map(|r| r.value).collect::<Option<Vec<_>>>() else {
            return vec![out];
        };
        same = false;
        hops = h;
        if let [v] = values[..] {
            e = v;
            continue;
        }
        let mut paths = Vec::new();
        for v in values {
            for tail in steps_from(ch, v, h, false) {
                if paths.len() == MAX_PATHS {
                    return vec![out];
                }
                let mut p = out.clone();
                p.extend(tail);
                paths.push(p);
            }
        }
        return paths;
    }
}

/// The partition count of `repartition(n[, cols])` / `coalesce(n)` (`numPartitions=` too),
/// as a constant (`1`, a name whose every binding is `1`).
fn partition_count(ch: &Checker, c: &ast::ExprCall) -> Option<i128> {
    call_arg(c, 0, "numPartitions").and_then(|a| ch.model.const_int(a))
}

/// A `hint("coalesce" | "repartition", n, ..)` step: (lower-case hint name, count), the
/// name and count read as constants.
fn partition_hint<'e>(ch: &Checker, s: &DfStep<'e>) -> Option<(String, Option<i128>)> {
    if s.method != "hint" {
        return None;
    }
    let name = call_arg(s.call, 0, "name")
        .and_then(|a| ch.model.const_str(a))?
        .trim()
        .to_ascii_lowercase();
    if !matches!(
        name.as_str(),
        "coalesce" | "repartition" | "repartition_by_range" | "rebalance"
    ) {
        return None;
    }
    // `hint(name, *parameters)`: the count is the first parameter.
    let n = call_arg(s.call, 1, "").and_then(|a| ch.model.const_int(a));
    Some((name, n))
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
    let Some(pos) = steps
        .iter()
        .take_while(|s| s.row.is_some_and(|r| r.cheap()) || s.method == "repartition")
        .position(|s| s.method == "repartition")
    else {
        return;
    };
    let step = &steps[pos];
    // Driver-side data (`createDataFrame(pdf)`): the shuffle is cheap and only sets
    // the number of output files.
    let src = ch.model.ty_of(step.recv);
    if src.size().cheap_shuffle() {
        return;
    }
    // A Python UDF after the repartition (a UDF value, a registered UDF in SQL text, a
    // Column variable holding one): it spreads the UDF work over more tasks.
    if steps[..pos]
        .iter()
        .any(|s| UdfRefs::of_call(ch.model, s.expr).any())
    {
        return;
    }
    let c = step.call;
    // The count as IOD003 reads it (`repartition(N)`, `numPartitions=N`, constants).
    if partition_count(ch, c).is_some_and(|n| n >= 100) {
        return; // raising parallelism, not reducing files
    }
    // Partitioning expressions after an optional count (`repartition(8, "date")`):
    // every one must be a plain column name for the subset test.
    let counted = c
        .arguments
        .args
        .first()
        .is_some_and(|a| ch.model.const_int(a).is_some());
    let exprs = &c.arguments.args[usize::from(counted)..];
    let cols: Option<Vec<String>> = exprs
        .iter()
        .map(|e| columns::column_name(ch.model, e))
        .collect();
    if let Some(cols) = cols.filter(|c| !c.is_empty()) {
        let pb = &w.layout().partition_by;
        if !pb.is_empty() && cols.iter().all(|c| pb.contains(c)) {
            return; // bounded files per output partition: intentional
        }
    }
    let mut d = msg!(PRF010, method_range(step.expr));
    if let Some(f) = remove_step_fix(step, "repartition") {
        d = d.with_fix(f);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ IOD003

/// A step after which a `coalesce(1)` / `repartition(1)` below it no longer sets the
/// output partition count: a shuffle or a combine.
fn repartitions(s: &DfStep) -> bool {
    s.row
        .is_some_and(|r| r.shuffle.is_some() || r.combine.is_some())
}

pub fn coalesce_one_before_write(ch: &mut Checker, _call: &ast::ExprCall, expr: &Expr) {
    let Some(w) = IoSettings::write(ch, expr) else {
        return;
    };
    let Some(df) = w.df else { return };
    // Every way the frame is built must end in one partition (`if ..: out =
    // df.coalesce(1) else: out = df.coalesce(1).select(..)`); a path without the step
    // (`else: out = df`) or with an unknown count makes the write unknown.
    let Some(hits) = df_step_paths(ch, df)
        .iter()
        .map(|p| one_partition_step(ch, p))
        .collect::<Option<Vec<_>>>()
    else {
        return;
    };
    let Some((step, what, narrow)) = hits.first().cloned() else {
        return;
    };
    // Small when every path's frame before the step is.
    let small = hits
        .iter()
        .all(|(s, ..)| ch.model.ty_of(s.recv).size().small_output());
    let one_step = hits
        .iter()
        .all(|(s, ..)| s.expr.range() == step.expr.range());
    let delta = w.v2 || w.format() == Some("delta");
    let v = if narrow {
        &IOD003_COALESCE
    } else {
        &IOD003_REPARTITION
    };
    let mut d = msg!(v, method_range(step.expr), call = what);
    if small {
        d = d.with_severity(Severity::Info);
    }
    if delta
        && one_step
        && let Some(f) = remove_step_fix(&step, step.method)
    {
        d = d.with_fix(f);
    }
    ch.report(d);
}

/// The step on one path that leaves the frame in one partition before the write:
/// (step, the call as worded, whether it is the narrow `coalesce` form). `None` when a
/// shuffle, a combine or another partition count comes first.
fn one_partition_step<'e>(
    ch: &Checker<'e>,
    steps: &[DfStep<'e>],
) -> Option<(DfStep<'e>, String, bool)> {
    for s in steps {
        if let Some((name, n)) = partition_hint(ch, s) {
            // Every partitioning hint ends the search like the method forms do.
            if n == Some(1) && matches!(name.as_str(), "coalesce" | "repartition") {
                let what = format!("hint(\"{name}\", 1)");
                return Some((s.clone(), what, name == "coalesce"));
            }
            return None;
        }
        if matches!(s.method, "coalesce" | "repartition") && partition_count(ch, s.call) == Some(1)
        {
            return Some((
                s.clone(),
                format!("{}(1)", s.method),
                s.method == "coalesce",
            ));
        }
        if repartitions(s) || s.method == "coalesce" {
            return None;
        }
    }
    None
}

// ------------------------------------------------------------------ IOD004

pub fn insert_into_positional(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["insertInto"]) else {
        return;
    };
    if sc.class() != Ty::DataFrameWriter {
        return;
    }
    let Some(io) = IoSettings::of(ch, &sc) else {
        return;
    };
    // The chain leads back to its DataFrame (`IoSettings::write`).
    let w = io.df.is_some().then_some(&io);
    // `layout` steps make Spark raise before anything is written (error severity, no fix).
    if let Some(w) = w
        && let Some(step) = w.layout().step(&["partitionBy", "bucketBy"])
    {
        ch.report(msg!(IOD004_LAYOUT, method_range(expr), step).with_severity(Severity::Error));
        return;
    }
    let mode = io.write_mode();
    let overwrite = mode == SaveMode::Overwrite;
    let v = if overwrite {
        &IOD004_OVERWRITE
    } else {
        &IOD004_APPEND
    };
    let mut d = msg!(v, method_range(expr));
    if let Some(w) = w
        && let Some(df) = w.df
        && !w.indirect
        && !w.v2
        && w.bare()
        && matches!(mode, SaveMode::Append | SaveMode::Overwrite)
        && let Some(table) = sc.arg(0, "tableName")
        && c.arguments.args.len() + c.arguments.keywords.len()
            == 1 + usize::from(sc.arg(1, "overwrite").is_some())
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

pub fn write_in_loop(ch: &mut Checker, _call: &ast::ExprCall, expr: &Expr) {
    let Some(w) = IoSettings::write(ch, expr) else {
        return;
    };
    let Some(inner) = ch.model.repeating_loop_at(expr.start()) else {
        return;
    };
    // `mode("append")`, `mode="append"`, `insertInto` without overwrite,
    // `writeTo(..).append()`.
    if w.write_mode() != SaveMode::Append {
        return;
    }
    let Some(target) = w.target() else {
        return;
    };
    // Only one target per iteration: a target derived from the loop variable is
    // orchestration over many tables (ACT005 covers it).
    if inner.varies(target) {
        return;
    }
    let small_literal = inner
        .iterable(&ch.source.text)
        .small(ch.settings.driver_loop_small_max as usize);
    let target = ch.slice(target.range());
    let mut d = msg!(IOD006, method_range(expr), target).with_site(expr.range());
    if small_literal {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}
