//! ACT — more actions & driver rules: ACT003, ACT005, ACT008, ACT009, ACT011.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::call_run::{Returns, RunKind, run_of_ty};
use sparklint_semantic::pulled::{Holds, PathStep, Pull, Pulled};
use sparklint_semantic::{SqlSiteKind, Ty, int_literal};

use super::Rule;
use super::describe;
use super::helpers::*;
use super::msg::Msg;
use crate::checker::Checker;
use crate::diagnostic::{Edit, Fix, Severity};
use crate::msg;

/// ACT003/005/008/009/011 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("ACT003").call(topandas_unbounded).msgs(&[
        ACT003,
        ACT003_ARROW,
        ACT003_PS,
        ACT003_REDUCED,
        ACT003_PS_REDUCED,
    ]),
    Rule::new("ACT005")
        .call(action_in_loop)
        .msgs(&[ACT005_ACTION, ACT005_WRITE]),
    Rule::new("ACT008")
        .call(collect_to_filter_roundtrip)
        .msgs(&[ACT008_ISIN, ACT008_SQL, ACT008_CONDITION]),
    Rule::new("ACT009")
        .call(collect_driver_compute)
        .expr(act009_expr)
        .msgs(&[ACT009_AGGREGATE, ACT009_INDEX, ACT009_LAST, ACT009_SLICE]),
    Rule::new("ACT011")
        .call(rdd_collect)
        .msgs(&[ACT011, ACT011_STEPS]),
];

fn act009_expr(ch: &mut Checker, expr: &Expr) {
    if let Expr::Subscript(s) = expr {
        collect_subscript(ch, s, expr);
    }
}

// ------------------------------------------------------------------ ACT003

const ACT003_HELP: &str = "Select only the needed columns and bound the rows (.limit(n)) before converting, or write the result and read it elsewhere.";
const ACT003: Msg = Msg::new(
    "ACT003",
    "unbounded",
    ".{method}() on an unbounded DataFrame materialises it in driver memory (x2-3 for pandas).",
)
.help(ACT003_HELP);
const ACT003_ARROW: Msg = Msg::new(
    "ACT003",
    "unbounded-arrow",
    ".toArrow() on an unbounded DataFrame materialises it in driver memory as one Arrow table.",
)
.help(ACT003_HELP);
const ACT003_PS: Msg = Msg::new(
    "ACT003",
    "unbounded-pandas-on-spark",
    ".{method}() on unbounded pandas-on-Spark data materialises it in driver memory (x2-3 for pandas).",
)
.help(ACT003_HELP);
const ACT003_REDUCED: Msg = Msg::new(
    "ACT003",
    "reduced",
    ".{method}() on a DataFrame with no known row bound materialises it in driver memory (filtered/aggregated — size unknown).",
)
.help(ACT003_HELP);
const ACT003_PS_REDUCED: Msg = Msg::new(
    "ACT003",
    "reduced-pandas-on-spark",
    ".{method}() on pandas-on-Spark data with no known row bound materialises it in driver memory (filtered/aggregated — size unknown).",
)
.help(ACT003_HELP);

pub fn topandas_unbounded(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call(expr) else {
        return;
    };
    // A pull of a driver-side frame (`toPandas` / `toArrow` / `to_pandas` / `to_numpy`).
    let hit = run_of_ty(sc.class(), sc.method).is_some_and(|r| r.returns == Returns::LocalFrame);
    if !hit || !sc.has_no_args() || sc.driver_sized() {
        return;
    }
    let (m, sev) = topandas_message(sc.method, sc.receiver_ty.ty, sc.receiver_ty.facts.reduced);
    ch.report(
        msg!(m, method_range(expr), method = sc.method)
            .with_site(expr.range())
            .with_severity(sev),
    );
}

/// ACT003 message variant and severity for method `m` on a receiver of type `ty`;
/// `reduced` = filtered / aggregated with no known row bound (info).
fn topandas_message(m: &str, ty: Ty, reduced: bool) -> (&'static Msg, Severity) {
    let ps = ty == Ty::PandasOnSparkFrame;
    match (reduced, ps) {
        (true, true) => (&ACT003_PS_REDUCED, Severity::Info),
        (true, false) => (&ACT003_REDUCED, Severity::Info),
        (false, true) => (&ACT003_PS, Severity::Warning),
        (false, false) if m == "toArrow" => (&ACT003_ARROW, Severity::Warning),
        (false, false) => (&ACT003, Severity::Warning),
    }
}

// ------------------------------------------------------------------ ACT005

const ACT005_HELP: &str = "Compute all iterations in one job: groupBy the loop key (df.groupBy(\"k\").count()), or union the per-iteration frames and write once.";
const ACT005_ACTION: Msg = Msg::new(
    "ACT005",
    "action",
    "Spark action {call} inside a loop runs one job per iteration (N jobs, N scans).",
)
.help(ACT005_HELP);
const ACT005_WRITE: Msg = Msg::new(
    "ACT005",
    "write",
    "write {call} inside a loop runs one job per iteration (N jobs, N scans).",
)
.help(ACT005_HELP);

pub fn action_in_loop(ch: &mut Checker, _call: &ast::ExprCall, expr: &Expr) {
    // A job, a pull, a write or a Delta table operation (`dt.vacuum()`,
    // `dt.merge(..)…execute()`); not a display, a stream start or a checkpoint.
    let Some(run) = ch.model.call_run(expr).filter(|r| r.kind().runs_job()) else {
        return;
    };
    // `DeltaTable.convertToDelta(spark, ..)` runs on nothing but its arguments.
    let recv = run.on.unwrap_or(expr);
    let kind = run.kind();
    // Judged on the loop the action sits in (the shared loop verdict).
    let Some(inner) = ch.model.repeating_loop_at(expr.start()) else {
        return;
    };
    if inner.polling(expr, recv) {
        return;
    }
    let small_literal = inner
        .iterable(&ch.source.text)
        .small(ch.settings.driver_loop_small_max as usize);
    let varies = inner.source_varies(recv);
    let bounded = ch.model.ty_of(recv).size().cheap_action();
    let m = if kind == RunKind::Write {
        &ACT005_WRITE
    } else {
        &ACT005_ACTION
    };
    let call = describe::call(ch, expr);
    let mut d = msg!(m, method_range(expr), call).with_site(expr.range());
    if small_literal || varies || bounded {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ ACT008

/// Whether `e` holds values collected from a frame that may not fit the driver (a
/// list / set of rows or of values derived from them, a pandas column's values).
fn collected_list(ch: &Checker, e: &Expr) -> bool {
    ch.model
        .pulled_rows(e)
        .iter()
        .any(|p| p.holds.values() && !p.fits_driver())
}

/// Whether a name interpolated into SQL text (an f-string hole) holds collected values.
fn holes_have_collected(ch: &Checker, holes: &[&Expr]) -> bool {
    let mut hit = false;
    for h in holes {
        walk_exprs(h, &mut |e| {
            if !hit && matches!(e, Expr::Name(_)) && collected_list(ch, e) {
                hit = true;
            }
        });
    }
    hit
}

pub fn collect_to_filter_roundtrip(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    // F.expr(f"id IN ({..})"), df.filter(f"id IN ({..})") / df.where(..),
    // df.selectExpr(f"..."), spark.sql(f"... IN ({','.join(ids)})")
    let sites = ch.model.sql_sites(expr);
    if !sites.is_empty() {
        for s in &sites {
            if !holes_have_collected(ch, &s.holes) {
                continue;
            }
            match s.kind {
                SqlSiteKind::Query => ch.report(msg!(ACT008_SQL, method_range(expr))),
                SqlSiteKind::Expression => {
                    let sink = format!("{}()", ch.slice(c.func.range()));
                    report_sql_string(ch, expr, &sink);
                }
                _ => report_sql_string(ch, expr, &format!("{}()", s.method)),
            }
        }
        return;
    }
    let Some(sc) = ch.model.spark_call_named(expr, &["isin"]) else {
        return;
    };
    if sc.is(Ty::Column, "isin") {
        let hit = c.arguments.args.iter().any(|a| {
            let a = match a {
                Expr::Starred(s) => &*s.value,
                other => other,
            };
            collected_list(ch, a)
        });
        if hit {
            ch.report(msg!(ACT008_ISIN, method_range(expr)));
        }
    }
}

const ACT008_ISIN: Msg = Msg::new(
    "ACT008",
    "isin",
    "Values collected to the driver are fed back into isin() — use a semi join.",
)
.help("df.join(other.select(\"k\"), \"k\", \"left_semi\") runs as one job (broadcast it if small) instead of collect + a huge IN list.");
const ACT008_SQL: Msg = Msg::new(
    "ACT008",
    "spark-sql",
    "Values collected to the driver are interpolated back into spark.sql() — use a semi join.",
)
.help("Register the source as a view and use `WHERE k IN (SELECT k FROM v)` / a LEFT SEMI JOIN instead of a literal list.");
const ACT008_CONDITION: Msg = Msg::new(
    "ACT008",
    "condition-string",
    "Values collected to the driver are interpolated back into {sink} SQL text — use a semi join.",
)
.help("df.join(other.select(\"k\"), \"k\", \"left_semi\") runs as one job (broadcast it if small) instead of collect + a literal IN list.");

fn report_sql_string(ch: &mut Checker, expr: &Expr, sink: &str) {
    ch.report(msg!(ACT008_CONDITION, method_range(expr), sink));
}

// ------------------------------------------------------------------ ACT009

const ACT009_AGGREGATE: Msg = Msg::new(
    "ACT009",
    "driver-aggregate",
    "{function}() computed on the driver from {collect} — compute it in Spark.",
)
.help(
    "Use df.count() / df.agg(F.sum(..), F.max(..)).first() so only the scalar leaves the cluster.",
);
const ACT009_INDEX: Msg = Msg::new(
    "ACT009",
    "index-zero",
    "{call}[0] pulls every row to the driver to read one — use .first().",
);
const ACT009_LAST: Msg = Msg::new(
    "ACT009",
    "index-last",
    "{call}[-1] pulls every row to the driver to read the last one — use .tail(1).",
)
.help("df.tail(1)[0] returns the same row (Spark 3.0+); only the last partitions are read.");
const ACT009_SLICE: Msg = Msg::new(
    "ACT009",
    "slice",
    "{call}[:{n}] pulls every row to the driver to keep {n} — use .take({n}).",
)
.help("df.take(n) returns the same rows and stops after the first partitions that hold them.");

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

/// The ways rows pulled from a DataFrame that may not fit the driver reach `e` and
/// serve only it: every name on the way is read once (rows also used elsewhere are
/// on the driver anyway, so computing from them there is free).
fn unbounded_pulls<'e>(ch: &Checker<'e>, e: &'e Expr) -> Vec<(Pulled<'e>, Pull<'e>)> {
    ch.model
        .pulled_rows(e)
        .into_iter()
        .filter_map(|p| {
            let pull = p.pull?;
            let only_here = p.bindings().all(|b| ch.model.binding_uses(b).count() == 1);
            (pull.recv == Ty::DataFrame && !p.fits_driver() && only_here).then_some((p, pull))
        })
        .collect()
}

/// Whether the builtin aggregate (`len` when `len`) computes over what `holds` holds
/// row by row: `len` of a local frame counts its rows, its other aggregates walk the
/// column labels; an iterator has no `len`; one row is not the rows.
fn aggregates_rows(holds: Holds, len: bool) -> bool {
    match holds {
        Holds::RowList | Holds::Values | Holds::Series => true,
        Holds::RowIter => !len,
        Holds::PandasFrame | Holds::OtherFrame => len,
        Holds::Row => false,
    }
}

/// The diagnostic's overlap sites: every distinct pull call (the first is the site).
fn with_pull_sites(mut d: crate::Diagnostic, pulls: &[Pull]) -> crate::Diagnostic {
    let mut sites: Vec<TextRange> = Vec::new();
    for p in pulls {
        if !sites.contains(&p.call.range()) {
            sites.push(p.call.range());
        }
    }
    if let Some((first, rest)) = sites.split_first() {
        d = d.with_site(*first);
        d.also_sites = rest.to_vec();
    }
    d
}

/// Whether `call` is a driver-side aggregate builtin.
fn is_driver_agg(ch: &Checker, c: &ast::ExprCall) -> bool {
    ch.model
        .qualified_name(&c.func)
        .is_some_and(|q| DRIVER_AGG.contains(&q.as_str()))
        && c.arguments.args.len() == 1
}

pub fn collect_driver_compute(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if !is_driver_agg(ch, c) {
        return;
    }
    let arg = &c.arguments.args[0];
    let len = ch.model.qualified_name(&c.func).as_deref() == Some("builtins.len");
    // One finding per pull: an aggregate over another one (`sorted(set(..))`, `len(s)`
    // with `s = set(..)`) leaves it to the inner one.
    let hits: Vec<_> = unbounded_pulls(ch, arg)
        .into_iter()
        .filter(|(p, _)| {
            aggregates_rows(p.holds, len)
                && !p.path.iter().any(|(st, w)| {
                    *st == PathStep::Wrap && matches!(w, Expr::Call(wc) if is_driver_agg(ch, wc))
                })
        })
        .collect();
    let Some((first, pull)) = hits.first() else {
        return;
    };
    let fname = ch.slice(c.func.range()).to_string();
    let mut fix = None;
    // `len(df.collect())` / `len(df.toPandas())` → `df.count()`.
    if hits.len() == 1 && first.direct() && len {
        let text = format!("{}.count()", paren_src(ch, pull.frame));
        fix = Some(Fix::unsafe_(
            format!("Replace with `{text}`"),
            vec![Edit::replace(expr.range(), text)],
        ));
    }
    let collect = describe::call(ch, pull.call);
    let pulls: Vec<Pull> = hits.iter().map(|(_, p)| *p).collect();
    // Overlap sites: the pull calls (one finding per collect, see crate::overlap).
    let mut d = with_pull_sites(
        msg!(ACT009_AGGREGATE, expr.range(), function = fname, collect),
        &pulls,
    );
    if let Some(f) = fix {
        d = d.with_fix(f);
    }
    ch.report(d);
}

/// `df.collect()[0]`, `[-1]` and `[:n]` (also through a name bound once to the
/// collect and used only there; no fix then).
pub fn collect_subscript(ch: &mut Checker, s: &ast::ExprSubscript, expr: &Expr) {
    enum Pick {
        First,
        Last,
        Take(i64),
    }
    let pick = match &*s.slice {
        Expr::Slice(sl) => {
            let lower_zero = sl
                .lower
                .as_deref()
                .is_none_or(|l| int_literal(l) == Some(0));
            match sl.upper.as_deref().and_then(int_literal) {
                Some(n) if n > 0 && lower_zero && sl.step.is_none() => Pick::Take(n),
                _ => return,
            }
        }
        e => match int_literal(e) {
            Some(0) => Pick::First,
            Some(-1) => Pick::Last,
            _ => return,
        },
    };
    // The collected list itself, directly or through names only.
    let hits: Vec<_> = unbounded_pulls(ch, &s.value)
        .into_iter()
        .filter(|(p, _)| {
            p.holds == Holds::RowList
                && p.path.iter().all(|(st, _)| matches!(st, PathStep::Name(_)))
        })
        .collect();
    let Some((first, pull)) = hits.first() else {
        return;
    };
    let (recv, direct) = (pull.frame, first.direct());
    let pulls: Vec<Pull> = hits.iter().map(|(_, p)| *p).collect();
    let call = describe::call(ch, pull.call);
    let base = paren_src(ch, recv);
    let (d, text) = match pick {
        Pick::First => (
            msg!(ACT009_INDEX, expr.range(), call),
            format!("{base}.first()"),
        ),
        Pick::Last => (
            msg!(ACT009_LAST, expr.range(), call),
            format!("{base}.tail(1)[0]"),
        ),
        Pick::Take(n) => (
            msg!(ACT009_SLICE, expr.range(), call, n = n.to_string()),
            format!("{base}.take({n})"),
        ),
    };
    let mut d = with_pull_sites(d, &pulls);
    if direct {
        d = d.with_fix(Fix::unsafe_(
            format!("Replace with `{text}`"),
            vec![Edit::replace(expr.range(), text)],
        ));
    }
    ch.report(d);
}

// ------------------------------------------------------------------ ACT011

const ACT011: Msg = Msg::new(
    "ACT011",
    "rdd-collect",
    "{call} converts the DataFrame to an RDD only to read rows back; `{fixed}` returns the same rows without the detour.",
)
.help(".rdd is unavailable on Spark Connect, serverless and standard access mode; the DataFrame method also keeps the query in the SQL plan.");

const ACT011_STEPS: Msg = Msg::new(
    "ACT011",
    "rdd-steps",
    "{call} runs the RDD steps in Python workers: every row is pickled out of the JVM and back, one row at a time.",
)
.help("Write the steps as DataFrame operations (select, pyspark.sql.functions, groupBy().agg()) or mapInPandas / mapInArrow (Arrow batches), then collect; .rdd is also unavailable on Spark Connect, serverless and standard access mode.");

pub fn rdd_collect(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call(expr) else {
        return;
    };
    let m = sc.method;
    // An RDD pull of rows to the driver; a `row_action` has a DataFrame pull of the same
    // name and signature (the direct `df.rdd.<m>(..)` form gets a fix). The others
    // (`collectAsMap`, `top`, `takeOrdered`, `takeSample`) are reported only after RDD
    // steps (`df.rdd.map(..).collectAsMap()`): directly on `df.rdd` they run no Python
    // worker.
    let pull = |t: Ty| run_of_ty(t, m).is_some_and(|r| r.kind == RunKind::DriverPull);
    if sc.class() != Ty::Rdd || !pull(Ty::Rdd) {
        return;
    }
    let row_action = pull(Ty::DataFrame);
    let recv = sc.receiver;
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
    // A frame that fits on the driver (row bound, catalog metadata, local data) costs
    // nothing either way; DBX002 still reports the `.rdd` access.
    if ch.model.ty_of(base).driver_sized() {
        return;
    }
    let direct = matches!(recv, Expr::Attribute(a) if a.value.range() == base.range());
    if direct && !row_action {
        return;
    }
    let call = describe::call(ch, expr);
    let d = if direct {
        let args = ch.slice(c.arguments.range);
        let text = format!("{}.{m}{args}", paren_src(ch, base));
        let fixed = describe::method(ch, base, m, Some(&c.arguments));
        msg!(ACT011, method_range(expr), call, fixed).with_fix(Fix::unsafe_(
            format!("Replace with `{text}`"),
            vec![Edit::replace(expr.range(), text)],
        ))
    } else {
        msg!(ACT011_STEPS, method_range(expr), call)
    };
    ch.report(d);
}

#[cfg(test)]
mod act003_tests {
    use super::*;

    #[test]
    fn topandas_message_table() {
        let cases: &[(&str, Ty, bool, &str, Severity)] = &[
            (
                "toPandas",
                Ty::DataFrame,
                false,
                ".toPandas() on an unbounded DataFrame materialises it in driver memory (x2-3 for pandas).",
                Severity::Warning,
            ),
            (
                "toArrow",
                Ty::DataFrame,
                false,
                ".toArrow() on an unbounded DataFrame materialises it in driver memory as one Arrow table.",
                Severity::Warning,
            ),
            (
                "to_pandas",
                Ty::PandasOnSparkFrame,
                false,
                ".to_pandas() on unbounded pandas-on-Spark data materialises it in driver memory (x2-3 for pandas).",
                Severity::Warning,
            ),
            (
                "toPandas",
                Ty::DataFrame,
                true,
                ".toPandas() on a DataFrame with no known row bound materialises it in driver memory (filtered/aggregated — size unknown).",
                Severity::Info,
            ),
            (
                "to_numpy",
                Ty::PandasOnSparkFrame,
                true,
                ".to_numpy() on pandas-on-Spark data with no known row bound materialises it in driver memory (filtered/aggregated — size unknown).",
                Severity::Info,
            ),
        ];
        for (m, ty, reduced, msg, sev) in cases {
            let (v, got_sev) = topandas_message(m, *ty, *reduced);
            assert_eq!(
                (crate::msg_text!(v, method = m), got_sev),
                (msg.to_string(), *sev),
                "{m}"
            );
        }
    }
}
