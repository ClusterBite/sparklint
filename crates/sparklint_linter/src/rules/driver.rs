//! ACT — more actions & driver rules: ACT003, ACT005, ACT008, ACT009, ACT011.

use ruff_python_ast::{self as ast, Expr, Stmt};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::api::{ActionKind, action_kind};
use sparklint_semantic::{LoopInfo, LoopKind, Ty, int_literal};

use super::Rule;
use super::actions::method_range;
use super::helpers::*;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Diagnostic, Edit, Fix, Severity};

/// ACT003/005/008/009/011 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("ACT003").call(topandas_unbounded),
    Rule::new("ACT005").call(action_in_loop),
    Rule::new("ACT008").call(collect_to_filter_roundtrip),
    Rule::new("ACT009")
        .call(collect_driver_compute)
        .expr(act009_expr),
    Rule::new("ACT011").call(rdd_collect),
];

fn act009_expr(ch: &mut Checker, expr: &Expr) {
    if let Expr::Subscript(s) = expr {
        collect_index_zero(ch, s, expr);
    }
}

// ------------------------------------------------------------------ ACT003

pub fn topandas_unbounded(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch
        .model
        .spark_call_named(expr, &["toPandas", "to_pandas", "to_numpy"])
    else {
        return;
    };
    let hit = sc.is(Ty::DataFrame, "toPandas")
        || sc.is_any(Ty::PandasOnSparkFrame, &["to_pandas", "to_numpy"]);
    if !hit || !sc.has_no_args() || sc.driver_sized() {
        return;
    }
    let (m, t) = (sc.method, sc.receiver_ty);
    let mut msg = format!(
        ".{m}() on an unbounded DataFrame materialises it in driver memory (x2-3 for pandas)."
    );
    let mut sev = Severity::Warning;
    if t.facts.reduced {
        msg = format!(
            ".{m}() on a DataFrame with no known row bound materialises it in driver memory (filtered/aggregated — size unknown)."
        );
        sev = Severity::Info;
    }
    ch.report(
        Diagnostic::new("ACT003", msg, method_range(expr))
            .with_site(expr.range())
            .with_severity(sev)
            .with_help("Select only the needed columns and bound the rows (.limit(n)) before converting, or write the result and read it elsewhere."),
    );
}

// ------------------------------------------------------------------ ACT005

/// Names bound by the target(s) of the loop `l` (found on the parent stack).
pub(super) fn loop_targets(ch: &Checker, l: &LoopInfo) -> Vec<String> {
    let mut out = Vec::new();
    let mut add = |t: &Expr| {
        walk_exprs(t, &mut |e| {
            if let Expr::Name(n) = e {
                out.push(n.id.as_str().to_string());
            }
        })
    };
    for n in ch.parents().iter().rev() {
        match n {
            Node::Stmt(Stmt::For(f)) if f.range == l.range => {
                add(&f.target);
                break;
            }
            Node::Expr(e) if e.range() == l.range => {
                let gens = match e {
                    Expr::ListComp(c) => &c.generators[..],
                    Expr::SetComp(c) => &c.generators[..],
                    Expr::Generator(c) => &c.generators[..],
                    Expr::DictComp(c) => &c.generators[..],
                    _ => break,
                };
                for g in gens {
                    add(&g.target);
                }
                break;
            }
            _ => {}
        }
    }
    out
}

fn mentions_any(e: &Expr, names: &[String]) -> bool {
    let mut found = false;
    walk_exprs(e, &mut |x| {
        if let Expr::Name(n) = x
            && names.iter().any(|t| t == n.id.as_str())
        {
            found = true;
        }
    });
    found
}

/// Whether `e` depends on a loop target or on a name (re)bound inside the loop body.
pub(super) fn varies_per_iteration(
    ch: &Checker,
    e: &Expr,
    l: &LoopInfo,
    targets: &[String],
) -> bool {
    let mut found = false;
    walk_exprs(e, &mut |x| {
        if found {
            return;
        }
        if let Expr::Name(n) = x {
            found = targets.iter().any(|t| t == n.id.as_str())
                || ch
                    .model
                    .reaching(n)
                    .iter()
                    .any(|b| l.body.contains_range(ch.model.binding(*b).stmt_range));
        }
    });
    found
}

/// Whether each iteration reads a different source (`spark.table(t)` with `t` the
/// loop variable, or a loop variable that is itself a DataFrame): orchestration
/// rather than the N+1 pattern.
fn source_varies(ch: &Checker, mut e: &Expr, l: &LoopInfo, targets: &[String], depth: u32) -> bool {
    loop {
        if let Some((c, recv, _)) = method_call(e) {
            let rt = ch.model.ty_of(recv).ty;
            if matches!(
                rt,
                Ty::SparkSession
                    | Ty::SqlContext
                    | Ty::DataFrameReader
                    | Ty::DataStreamReader
                    | Ty::Catalog
                    | Ty::DeltaTable
            ) && (c
                .arguments
                .args
                .iter()
                .any(|a| varies_per_iteration(ch, a, l, targets))
                || c.arguments
                    .keywords
                    .iter()
                    .any(|k| varies_per_iteration(ch, &k.value, l, targets)))
            {
                return true;
            }
            e = recv;
            continue;
        }
        match e {
            Expr::Attribute(a) => e = &a.value,
            Expr::Subscript(s) => e = &s.value,
            Expr::Call(c) => {
                // helper(t) returning a DataFrame
                if c.arguments.args.iter().any(|a| mentions_any(a, targets)) {
                    return true;
                }
                e = &c.func;
            }
            Expr::Name(n) => {
                if targets.iter().any(|t| t == n.id.as_str()) {
                    return true;
                }
                if depth >= 3 {
                    return false;
                }
                return ch.model.reaching(n).iter().any(|b| {
                    let b = ch.model.binding(*b);
                    l.body.contains_range(b.stmt_range)
                        && b.value
                            .is_some_and(|v| source_varies(ch, v, l, targets, depth + 1))
                });
            }
            _ => return false,
        }
    }
}

/// Whether `l` is a polling `while` loop: it waits for something outside the program
/// (a table filled by a stream, a file landing) instead of iterating over data. Either
/// the body sleeps (`time.sleep(..)`, not counting nested functions), or the action
/// sits in the loop condition and its frame is the same on every iteration (nothing
/// it reads is rebound in the loop), so only an outside change can end the loop.
fn polling_loop(ch: &Checker, l: &LoopInfo, action: &Expr, recv: &Expr) -> bool {
    use ruff_python_ast::visitor::{self, Visitor};
    if l.kind != LoopKind::While {
        return false;
    }
    let Some(wh) = ch.parents().iter().rev().find_map(|n| match n {
        Node::Stmt(Stmt::While(w)) if w.range == l.range => Some(w),
        _ => None,
    }) else {
        return false;
    };
    if wh.test.range().contains_range(action.range()) && !varies_per_iteration(ch, recv, l, &[]) {
        return true;
    }
    struct V<'c, 'a> {
        ch: &'c Checker<'a>,
        found: bool,
    }
    impl<'b> Visitor<'b> for V<'_, '_> {
        fn visit_stmt(&mut self, s: &'b Stmt) {
            if !matches!(s, Stmt::FunctionDef(_) | Stmt::ClassDef(_)) && !self.found {
                visitor::walk_stmt(self, s);
            }
        }
        fn visit_expr(&mut self, e: &'b Expr) {
            if self.found {
                return;
            }
            if let Expr::Call(c) = e
                && self
                    .ch
                    .model
                    .qualified_name(&c.func)
                    .is_some_and(|q| q == "time.sleep")
            {
                self.found = true;
                return;
            }
            if !matches!(e, Expr::Lambda(_)) {
                visitor::walk_expr(self, e);
            }
        }
    }
    let mut v = V { ch, found: false };
    for s in &wh.body {
        v.visit_stmt(s);
    }
    v.found
}

pub fn action_in_loop(ch: &mut Checker, _call: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call(expr) else {
        return;
    };
    let (recv, m, rt) = (sc.receiver, sc.method, sc.receiver_ty);
    let Some(kind) = action_kind(rt.ty, m) else {
        return;
    };
    if matches!(
        kind,
        ActionKind::Display | ActionKind::StreamStart | ActionKind::Checkpoint
    ) {
        return;
    }
    let loops = ch.model.loops_at(expr.start());
    let Some(inner) = loops.last().copied() else {
        return;
    };
    if polling_loop(ch, inner, expr, recv) {
        return;
    }
    let targets = loop_targets(ch, inner);
    let small_literal = loops.iter().all(|l| l.literal_len.is_some_and(|n| n <= 10));
    let varies = !targets.is_empty() && source_varies(ch, recv, inner, &targets, 0);
    let bounded = rt.bounded().is_some();
    let what = if kind == ActionKind::Write {
        "write"
    } else {
        "Spark action"
    };
    let mut d = Diagnostic::new(
        "ACT005",
        format!(
            "{what} {}.{m}() inside a loop runs one job per iteration (N jobs, N scans).",
            short(ch, recv)
        ),
        method_range(expr),
    )
    .with_site(expr.range())
    .with_help("Compute all iterations in one job: groupBy the loop key (df.groupBy(\"k\").count()), or union the per-iteration frames and write once.");
    if small_literal || varies || bounded {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ ACT008

/// Whether `e` is a Python list/set of values collected from an unbounded DataFrame.
fn collected_list(ch: &Checker, e: &Expr, depth: u32) -> bool {
    if depth > 4 {
        return false;
    }
    let from_unbounded_spark = |x: &Expr| {
        let t = ch.model.ty_of(x);
        matches!(t.ty, Ty::List(_)) && t.facts.from_spark && t.bounded().is_none()
    };
    match e {
        Expr::Name(n) => ch.model.reaching(n).iter().any(|b| {
            ch.model
                .binding(*b)
                .value
                .is_some_and(|v| v.range() != e.range() && collected_list(ch, v, depth + 1))
        }),
        Expr::ListComp(c) => c.generators.first().is_some_and(|g| {
            from_unbounded_spark(&g.iter) || collected_list(ch, &g.iter, depth + 1)
        }),
        Expr::SetComp(c) => c.generators.first().is_some_and(|g| {
            from_unbounded_spark(&g.iter) || collected_list(ch, &g.iter, depth + 1)
        }),
        Expr::Generator(c) => c.generators.first().is_some_and(|g| {
            from_unbounded_spark(&g.iter) || collected_list(ch, &g.iter, depth + 1)
        }),
        Expr::Call(c) => {
            if let Some(q) = ch.model.qualified_name(&c.func)
                && matches!(
                    q.as_str(),
                    "builtins.list" | "builtins.set" | "builtins.sorted" | "builtins.tuple"
                )
            {
                return c
                    .arguments
                    .args
                    .first()
                    .is_some_and(|a| collected_list(ch, a, depth + 1));
            }
            if let Some((_, recv, m)) = method_call(e) {
                match m {
                    "collect" | "toLocalIterator" => return from_unbounded_spark(e),
                    "tolist" | "to_list" | "unique" => {
                        return chain_has_unbounded_topandas(ch, recv);
                    }
                    _ => {}
                }
            }
            false
        }
        _ => false,
    }
}

fn chain_has_unbounded_topandas(ch: &Checker, mut e: &Expr) -> bool {
    loop {
        if let Some((_, recv, m)) = method_call(e) {
            if m == "toPandas" {
                return ch
                    .model
                    .spark_call(e)
                    .is_some_and(|sc| sc.is_dataframe() && sc.bounded().is_none());
            }
            e = recv;
            continue;
        }
        match e {
            Expr::Subscript(s) => e = &s.value,
            Expr::Attribute(a) => e = &a.value,
            _ => return false,
        }
    }
}

pub fn collect_to_filter_roundtrip(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["isin", "sql"]) else {
        return;
    };
    if sc.is(Ty::Column, "isin") {
        let hit = c.arguments.args.iter().any(|a| {
            let a = match a {
                Expr::Starred(s) => &*s.value,
                other => other,
            };
            collected_list(ch, a, 0)
        });
        if hit {
            ch.report(
                Diagnostic::new(
                    "ACT008",
                    "Values collected to the driver are fed back into isin() — use a semi join.",
                    method_range(expr),
                )
                .with_help("df.join(other.select(\"k\"), \"k\", \"left_semi\") runs as one job (broadcast it if small) instead of collect + a huge IN list."),
            );
        }
        return;
    }
    // spark.sql(f"... IN ({','.join(ids)})")
    if sc.method == "sql"
        && sc.class().is_session()
        && let Some(arg @ Expr::FString(_)) = c.arguments.args.first()
    {
        let mut hit = false;
        walk_exprs(arg, &mut |e| {
            if !hit && matches!(e, Expr::Name(_)) && collected_list(ch, e, 0) {
                hit = true;
            }
        });
        if hit {
            ch.report(
                Diagnostic::new(
                    "ACT008",
                    "Values collected to the driver are interpolated back into spark.sql() — use a semi join.",
                    method_range(expr),
                )
                .with_help("Register the source as a view and use `WHERE k IN (SELECT k FROM v)` / a LEFT SEMI JOIN instead of a literal list."),
            );
        }
    }
}

// ------------------------------------------------------------------ ACT009

const DRIVER_AGG: &[&str] = &[
    "builtins.len",
    "builtins.sum",
    "builtins.min",
    "builtins.max",
    "builtins.sorted",
    "builtins.any",
    "builtins.all",
    "builtins.set",
    "collections.Counter",
];

/// `df.collect()` (no args) on an unbounded DataFrame: returns the receiver.
fn unbounded_collect<'e>(ch: &Checker, e: &'e Expr) -> Option<&'e Expr> {
    let sc = ch.model.spark_call_named(e, &["collect"])?;
    (sc.has_no_args() && sc.is_dataframe() && sc.bounded().is_none()).then_some(sc.receiver)
}

/// Whether `call` is a driver-side aggregate builtin.
fn is_driver_agg(ch: &Checker, c: &ast::ExprCall) -> bool {
    ch.model
        .qualified_name(&c.func)
        .is_some_and(|q| DRIVER_AGG.contains(&q.as_str()))
        && c.arguments.args.len() == 1
}

/// The receiver of an unbounded `.collect()` that is the first source of the
/// comprehension `e`, with the range of that collect call.
fn comp_collect_source<'e>(ch: &Checker, e: &'e Expr) -> Option<(&'e Expr, TextRange)> {
    let gens = match e {
        Expr::ListComp(c) => &c.generators[..],
        Expr::SetComp(c) => &c.generators[..],
        Expr::Generator(c) => &c.generators[..],
        _ => return None,
    };
    let g = gens.first()?;
    unbounded_collect(ch, &g.iter).map(|r| (r, g.iter.range()))
}

pub fn collect_driver_compute(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if !is_driver_agg(ch, c) {
        return;
    }
    let arg = &c.arguments.args[0];
    let fname = ch.slice(c.func.range()).to_string();
    let mut fix = None;
    // Overlap site: the `.collect()` call when it is the argument or the first
    // comprehension source (one finding per collect, see crate::overlap).
    let mut site = None;
    let recv: &Expr = if let Some(r) = unbounded_collect(ch, arg) {
        site = Some(arg.range());
        if fname == "len" {
            let text = format!("{}.count()", paren_src(ch, r));
            fix = Some(Fix::unsafe_(
                format!("Replace with `{text}`"),
                vec![Edit::replace(expr.range(), text)],
            ));
        }
        r
    } else if let Some((r, s)) = comp_collect_source(ch, arg) {
        site = Some(s);
        r
    } else if let Expr::Name(n) = arg
        && let Some(b) = ch.model.single_binding(n)
        && ch.model.binding_uses(b).count() == 1
        && let Some(v) = ch.model.binding(b).value
        && let Some(r) = unbounded_collect(ch, v)
    {
        r
    } else {
        return;
    };
    let mut d = Diagnostic::new(
        "ACT009",
        format!(
            "{fname}() computed on the driver from {}.collect() — compute it in Spark.",
            short(ch, recv)
        ),
        expr.range(),
    )
    .with_help("Use df.count() / df.agg(F.sum(..), F.max(..)).first() so only the scalar leaves the cluster.");
    if let Some(s) = site {
        d = d.with_site(s);
    }
    if let Some(f) = fix {
        d = d.with_fix(f);
    }
    ch.report(d);
}

pub fn collect_index_zero(ch: &mut Checker, s: &ast::ExprSubscript, expr: &Expr) {
    if int_literal(&s.slice) != Some(0) {
        return;
    }
    let Some(recv) = unbounded_collect(ch, &s.value) else {
        return;
    };
    let text = format!("{}.first()", paren_src(ch, recv));
    ch.report(
        Diagnostic::new(
            "ACT009",
            format!(
                "{}.collect()[0] pulls every row to the driver to read one — use .first().",
                short(ch, recv)
            ),
            expr.range(),
        )
        .with_site(s.value.range())
        .with_fix(Fix::unsafe_(
            format!("Replace with `{text}`"),
            vec![Edit::replace(expr.range(), text)],
        )),
    );
}

// ------------------------------------------------------------------ ACT011

pub fn rdd_collect(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["collect", "take"]) else {
        return;
    };
    if sc.class() != Ty::Rdd {
        return;
    }
    let (recv, m) = (sc.receiver, sc.method);
    let mut e = recv;
    let base = loop {
        if let Expr::Attribute(a) = e
            && a.attr.as_str() == "rdd"
            && ch.model.is_dataframe(&a.value)
        {
            break &*a.value;
        }
        match ch.model.spark_call(e) {
            Some(sc) if sc.class() == Ty::Rdd => e = sc.receiver,
            _ => return,
        }
    };
    let mut d = Diagnostic::new(
        "ACT011",
        format!(
            "{}.rdd.{m}() bypasses Arrow and converts every row through Python pickling.",
            short(ch, base)
        ),
        method_range(expr),
    )
    .with_help("Use df.collect() / df.select(...).toPandas() (Arrow) instead of going through .rdd; .rdd is also unavailable on Spark Connect/serverless.");
    if matches!(recv, Expr::Attribute(a) if a.value.range() == base.range()) {
        let args = ch.slice(c.arguments.range);
        let text = format!("{}.{m}{args}", paren_src(ch, base));
        d = d.with_fix(Fix::unsafe_(
            format!("Replace with `{text}`"),
            vec![Edit::replace(expr.range(), text)],
        ));
    }
    ch.report(d);
}
