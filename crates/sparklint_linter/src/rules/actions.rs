//! ACT — actions & driver rules: ACT001, ACT002, ACT004, ACT006, ACT007, ACT010.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, CmpOp, Expr, ExprContext, Stmt, UnaryOp};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::call_run::{Recv, Returns, RunKind, run_of_ty};
use sparklint_semantic::pulled::{PathStep, Pulled};
use sparklint_semantic::{BindingKind, Const, SparkCall, Ty, int_literal};

use super::CountVar;
use super::Rule;
use super::describe::{self, short};
use super::helpers::*;
use super::msg::Msg;
use super::truth_use::TruthUse;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Edit, Fix, Severity};
use crate::msg;

/// ACT001/002/004/006/007/010/015 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("ACT001").call(spark_action).msgs(&[ACT001]),
    Rule::new("ACT002")
        .call(collect_unbounded)
        .msgs(&[ACT002, ACT002_REDUCED]),
    Rule::new("ACT004")
        .stmt(act004_stmt)
        .expr(act004_expr)
        .msgs(&[ACT004, ACT004_N_PLUS_ONE]),
    Rule::new("ACT006")
        .call(act006_call)
        .expr(act006_expr)
        .msgs(&[ACT006, ACT006_HELD, ACT006_RDD]),
    Rule::new("ACT007")
        .stmt(act007_stmt)
        .call(act007_call)
        .msgs(&[ACT007_LOGGED, ACT007_DISCARDED]),
    Rule::new("ACT010")
        .call(debug_action_expr)
        .msgs(&[ACT010, ACT010_PLAN, ACT010_SINK]),
    Rule::new("ACT015")
        .call(approx_quantile_exact)
        .msgs(&[ACT015, ACT015_LOOP]),
];

fn act004_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::For(f) = stmt {
        driver_row_iteration_for(ch, f);
    }
}

fn act004_expr(ch: &mut Checker, expr: &Expr) {
    if matches!(
        expr,
        Expr::ListComp(_) | Expr::SetComp(_) | Expr::DictComp(_) | Expr::Generator(_)
    ) {
        driver_row_iteration_comp(ch, expr);
    }
}

fn act006_call(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    rdd_is_empty(ch, c, expr);
    count_in_bool_context(ch, expr);
}

fn act006_expr(ch: &mut Checker, expr: &Expr) {
    match expr {
        Expr::Compare(c) => count_compare(ch, c, expr),
        Expr::UnaryOp(u) => not_count(ch, u, expr),
        _ => {}
    }
}

fn act007_stmt(ch: &mut Checker, stmt: &Stmt) {
    // `df.head()` as the last statement of a notebook cell is displayed, not discarded.
    if let Stmt::Expr(e) = stmt
        && !ch.is_cell_final(stmt)
    {
        discarded_action(ch, &e.value);
    }
}

fn act007_call(ch: &mut Checker, c: &ast::ExprCall, _expr: &Expr) {
    action_in_logging(ch, c);
}

// ------------------------------------------------------------------ ACT001

const ACT001: Msg = Msg::new(
    "ACT001",
    "action",
    "Spark action: {call} triggers a job (kind: {kind}).",
);

const DDL_KEYWORDS: &[&str] = &[
    "CREATE", "INSERT", "MERGE", "DELETE", "UPDATE", "DROP", "ALTER", "OPTIMIZE", "VACUUM",
    "TRUNCATE", "COPY", "REFRESH", "CACHE", "ANALYZE", "REPLACE", "MSCK",
];

pub fn spark_action(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if let Some(sc) = ch.model.spark_call(expr)
        && sc.class().is_session()
        && sc.method == "sql"
    {
        if ch
            .model
            .sql_query(c)
            .is_some_and(|q| q.keyword().is_some_and(|w| DDL_KEYWORDS.contains(&w)))
        {
            let call = describe::call(ch, expr);
            ch.report(msg!(ACT001, method_range(expr), call, kind = "eager-sql"));
        }
        return;
    }
    let Some(run) = ch.model.call_run(expr) else {
        return;
    };
    // A Delta table operation changes (or maintains) the table: worded as a write.
    let kind = match run.kind() {
        RunKind::DeltaOp => "write",
        k => k.as_str(),
    };
    let range = if run.recv == Recv::Function && run.kind() == RunKind::Display {
        expr.range()
    } else {
        method_range(expr)
    };
    let call = describe::call(ch, expr);
    ch.report(msg!(ACT001, range, call, kind));
}

// ------------------------------------------------------------------ ACT002

const ACT002_HELP: &str = "Keep the work in Spark (aggregate/write), or bound the result with .limit(n) before collecting.";
const ACT002: Msg = Msg::new(
    "ACT002",
    "unbounded",
    ".collect() on a DataFrame with no known row bound pulls everything to the driver.",
)
.help(ACT002_HELP);
const ACT002_REDUCED: Msg = Msg::new(
    "ACT002",
    "reduced",
    ".collect() on a DataFrame with no known row bound pulls everything to the driver (filtered/aggregated — size unknown).",
)
.help(ACT002_HELP);

pub fn collect_unbounded(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["collect"]) else {
        return;
    };
    if !sc.has_no_args() || !sc.is_dataframe() || sc.driver_sized() {
        return;
    }
    let (recv, t) = (sc.receiver, sc.receiver_ty);
    let (m, sev) = if t.facts.reduced {
        (&ACT002_REDUCED, Severity::Info)
    } else {
        (&ACT002, Severity::Warning)
    };
    let limit = ch.settings.collect_limit;
    let fix = Fix::unsafe_(
        format!("Insert .limit({limit}) before .collect()"),
        vec![Edit::insert(recv.range().end(), format!(".limit({limit})"))],
    );
    ch.report(
        msg!(m, method_range(expr))
            .with_site(expr.range())
            .with_severity(sev)
            .with_fix(fix),
    );
}

// ------------------------------------------------------------------ ACT004

const ACT004_HELP: &str = "Use select/withColumn/groupBy (or mapInPandas/applyInPandas for arbitrary Python) so the work runs on the executors.";
const ACT004: Msg = Msg::new(
    "ACT004",
    "rows",
    "Iterating rows on the driver ({source}) — express this as a DataFrame transformation.",
)
.help(ACT004_HELP);
const ACT004_N_PLUS_ONE: Msg = Msg::new(
    "ACT004",
    "rows-n-plus-one",
    "Iterating rows on the driver ({source}) — express this as a DataFrame transformation. The loop body also runs a Spark action per row (N+1 jobs).",
)
.help(ACT004_HELP);

fn body_has_action(ch: &Checker, body: &[Stmt]) -> bool {
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
            if self
                .ch
                .model
                .call_run(e)
                .is_some_and(|r| r.recv != Recv::Ty(Ty::PandasOnSparkFrame))
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
    for s in body {
        v.visit_stmt(s);
    }
    v.found
}

/// Describe a driver-side row iteration source, if `iter` is one, with its overlap
/// site: the name holding the rows when they come through one (the rows may serve
/// other uses too), the `.collect()` / `.toPandas()` call that pulls them otherwise.
fn row_iteration_source(ch: &Checker, iter: &Expr) -> Option<(String, TextRange)> {
    let hit = ch
        .model
        .pulled_rows(iter)
        .into_iter()
        .find(|p| p.holds.rows() && !p.fits_driver() && !first_rows_only(p))?;
    // `enumerate(x)` / `zip(x, ..)` / `list(x)` / `sorted(x)` iterate `x`.
    let i = hit.path.iter().position(|(s, _)| *s != PathStep::Wrap);
    let iterated = i.map_or(hit.at, |i| hit.path[i].1);
    let name = hit
        .path
        .iter()
        .find(|(s, _)| matches!(s, PathStep::Name(_)))
        .map(|(_, e)| *e);
    let site = name.unwrap_or(hit.pull.map_or(hit.at, |p| p.call)).range();
    let desc = match i.map(|i| hit.path[i].0) {
        // `pdf.iterrows()` / `.to_dict("records")` / `table.to_pylist()`.
        Some(PathStep::FrameRows) => describe::call(ch, iterated),
        Some(PathStep::Name(_)) => name_source(ch, iterated),
        _ if hit.pull.is_some() => format!("for row in {}", describe::call(ch, iterated)),
        _ => name_source(ch, iterated),
    };
    Some((desc, site))
}

/// `rows[:3]`: a few of the pulled rows, whatever the pull cost.
fn first_rows_only(p: &Pulled) -> bool {
    p.path.iter().any(|(s, e)| {
        *s == PathStep::Slice
            && matches!(e, Expr::Subscript(sub) if matches!(&*sub.slice, Expr::Slice(sl) if sl.upper.is_some()))
    })
}

/// How the rows behind a name were pulled: `rows from df.collect()` when every binding
/// reaching the use is the same call shape, the generic wording otherwise.
fn name_source(ch: &Checker, name: &Expr) -> String {
    let Expr::Name(n) = name else {
        return "rows collected from a DataFrame".into();
    };
    let mut found: Option<String> = None;
    for b in ch.model.reaching(n) {
        let desc = ch
            .model
            .binding(*b)
            .value
            .filter(|v| {
                ch.model
                    .pull_call(v)
                    .is_some_and(|p| matches!(p.run.returns, Returns::Rows | Returns::RowIter))
            })
            .map(|v| format!("rows from {}", describe::call(ch, v)));
        match (desc, &found) {
            (Some(d), None) => found = Some(d),
            (Some(d), Some(f)) if &d == f => {}
            _ => return "rows collected from a DataFrame".into(),
        }
    }
    found.unwrap_or_else(|| "rows collected from a DataFrame".into())
}

fn report_row_iteration(ch: &mut Checker, iter: &Expr, body: Option<&[Stmt]>) {
    let Some((desc, site)) = row_iteration_source(ch, iter) else {
        return;
    };
    let m = if body.is_some_and(|b| body_has_action(ch, b)) {
        &ACT004_N_PLUS_ONE
    } else {
        &ACT004
    };
    ch.report(msg!(m, iter.range(), source = desc).with_site(site));
}

pub fn driver_row_iteration_for(ch: &mut Checker, f: &ast::StmtFor) {
    report_row_iteration(ch, &f.iter, Some(&f.body));
}

pub fn driver_row_iteration_comp(ch: &mut Checker, expr: &Expr) {
    let gens = match expr {
        Expr::ListComp(c) => &c.generators[..],
        Expr::SetComp(c) => &c.generators[..],
        Expr::Generator(c) => &c.generators[..],
        Expr::DictComp(c) => &c.generators[..],
        _ => return,
    };
    for g in gens {
        report_row_iteration(ch, &g.iter, None);
    }
}

// ------------------------------------------------------------------ ACT006

const ACT006_HELP: &str = "isEmpty() (Spark 3.3+) runs a job that stops at the first row; on older versions use len(df.take(1)) == 0. Keep count() only when the number itself is needed.";
const ACT006: Msg = Msg::new(
    "ACT006",
    "count-check",
    "{count} used as an emptiness check scans every row — use {receiver}.isEmpty().",
)
.help(ACT006_HELP);
const ACT006_HELD: Msg = Msg::new(
    "ACT006",
    "held-count",
    "{call} is only used as an emptiness check — use {receiver}.isEmpty() (stops at the first row).",
)
.help(ACT006_HELP);
const ACT006_RDD: Msg = Msg::new(
    "ACT006",
    "rdd-is-empty",
    "{call} converts the DataFrame to an RDD first — use {receiver}.isEmpty().",
)
.help(ACT006_HELP);

/// Normalised comparison `count OP k` → Some(true) = "is empty", Some(false) =
/// "is not empty", None = not an emptiness check.
fn emptiness_of(op: CmpOp, k: i64) -> Option<bool> {
    match (op, k) {
        (CmpOp::Gt, 0) | (CmpOp::NotEq, 0) | (CmpOp::GtE, 1) => Some(false),
        (CmpOp::Eq, 0) | (CmpOp::LtE, 0) | (CmpOp::Lt, 1) => Some(true),
        _ => None,
    }
}

fn flip(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::LtE => CmpOp::GtE,
        CmpOp::GtE => CmpOp::LtE,
        other => other,
    }
}

/// If `cmp` compares `subject` (by range) against an integer literal as an
/// emptiness check, return whether it tests "is empty".
fn compare_emptiness(cmp: &ast::ExprCompare, subject: TextRange) -> Option<bool> {
    if cmp.ops.len() != 1 || cmp.operands.len() != 2 {
        return None;
    }
    let (l, r) = (&cmp.operands[0], &cmp.operands[1]);
    let op = cmp.ops[0];
    if l.range() == subject {
        emptiness_of(op, int_literal(r)?)
    } else if r.range() == subject {
        emptiness_of(flip(op), int_literal(l)?)
    } else {
        None
    }
}

fn is_empty_fix(ch: &Checker, recv: &Expr, empty: bool, range: TextRange) -> Fix {
    let r = paren_src(ch, recv);
    let modern = ch.settings.spark_version >= (3, 3);
    if modern {
        let mut text = if empty {
            format!("{r}.isEmpty()")
        } else {
            format!("not {r}.isEmpty()")
        };
        if !empty && crate::fix_text::not_needs_parens(ch.parent()) {
            text = format!("({text})");
        }
        Fix::safe(
            format!("Replace with `{text}`"),
            vec![Edit::replace(range, text)],
        )
    } else {
        let text = if empty {
            format!("len({r}.take(1)) == 0")
        } else {
            format!("len({r}.take(1)) > 0")
        };
        Fix::unsafe_(
            format!("Replace with `{text}`"),
            vec![Edit::replace(range, text)],
        )
    }
}

fn emptiness_finding(ch: &Checker, recv: &Expr, range: TextRange) -> crate::Diagnostic {
    let (count, receiver) = (
        describe::method(ch, recv, "count", None),
        describe::receiver(ch, recv),
    );
    msg!(ACT006, range, count, receiver)
}

pub fn count_compare(ch: &mut Checker, c: &ast::ExprCompare, expr: &Expr) {
    for operand in c.operands.iter() {
        let Some(recv) = df_count_call(ch, operand) else {
            continue;
        };
        let Some(empty) = compare_emptiness(c, operand.range()) else {
            continue;
        };
        let fix = is_empty_fix(ch, recv, empty, expr.range());
        let d = emptiness_finding(ch, recv, expr.range());
        ch.report(d.with_fix(fix));
        return;
    }
}

pub fn not_count(ch: &mut Checker, u: &ast::ExprUnaryOp, expr: &Expr) {
    if u.op != UnaryOp::Not {
        return;
    }
    let Some(recv) = df_count_call(ch, &u.operand) else {
        return;
    };
    let fix = is_empty_fix(ch, recv, true, expr.range());
    let d = emptiness_finding(ch, recv, expr.range());
    ch.report(d.with_fix(fix));
}

pub fn count_in_bool_context(ch: &mut Checker, expr: &Expr) {
    let Some(recv) = df_count_call(ch, expr) else {
        return;
    };
    // `not df.count()` is reported (and fixed as a whole) by `not_count`.
    if matches!(ch.parent(), Some(Node::Expr(Expr::UnaryOp(u))) if u.op == UnaryOp::Not) {
        return;
    }
    let Some(TruthUse::Test { named }) = ch.truth_use(expr) else {
        return;
    };
    let d = emptiness_finding(ch, recv, expr.range());
    // Through a walrus the count is also kept in a name: no rewrite of the value.
    if named {
        ch.report(d);
        return;
    }
    let fix = is_empty_fix(ch, recv, false, expr.range());
    ch.report(d.with_fix(fix));
}

pub fn rdd_is_empty(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some((_, recv, m)) = method_call(expr) else {
        return;
    };
    if m != "isEmpty" || !has_no_args(c) {
        return;
    }
    let Expr::Attribute(a) = recv else { return };
    if a.attr.as_str() != "rdd" || !ch.model.is_dataframe(&a.value) {
        return;
    }
    let base = paren_src(ch, &a.value);
    let (call, receiver) = (describe::call(ch, expr), describe::receiver(ch, &a.value));
    let text = format!("{base}.isEmpty()");
    let mut d = msg!(ACT006_RDD, expr.range(), call, receiver);
    if ch.settings.spark_version >= (3, 3) {
        d = d.with_fix(Fix::safe(
            format!("Replace with `{text}`"),
            vec![Edit::replace(expr.range(), text)],
        ));
    }
    ch.report(d);
}

// ------------------------------------------------------------------ ACT006/ACT007 T2

pub fn init_count_vars(ch: &mut Checker) {
    let model = ch.model;
    for (i, b) in model.bindings().iter().enumerate() {
        if !matches!(b.kind, BindingKind::Assign | BindingKind::AnnAssign) {
            continue;
        }
        let Some(v) = b.value else { continue };
        let Some(recv) = df_count_call(ch, v) else {
            continue;
        };
        let cv = CountVar {
            call_range: method_range(v),
            recv: describe::receiver(ch, recv),
            call: describe::method(ch, recv, "count", None),
            uses: 0,
            emptiness_uses: 0,
            logging_uses: 0,
        };
        ch.state.count_vars.insert(i as u32, cv);
    }
}

fn inside_logging_sink(ch: &Checker, e: &Expr) -> bool {
    let mut i = 0;
    while let Some(Node::Expr(anc)) = ch.ancestor(i) {
        if let Expr::Call(c) = anc
            && c.arguments.range.contains_range(e.range())
            && is_logging_sink(ch, c)
        {
            return true;
        }
        if matches!(anc, Expr::Lambda(_)) {
            return false;
        }
        i += 1;
    }
    false
}

pub fn count_var_use(ch: &mut Checker, n: &ast::ExprName, expr: &Expr) {
    if n.ctx != ExprContext::Load {
        return;
    }
    let reaching: Vec<u32> = ch
        .model
        .reaching(n)
        .iter()
        .copied()
        .filter(|b| ch.state.count_vars.contains_key(b))
        .collect();
    if reaching.is_empty() {
        return;
    }
    // Compared with 0 / 1, tested or converted to bool: an emptiness check.
    let emptiness = match ch.parent() {
        Some(Node::Expr(Expr::Compare(c))) => compare_emptiness(c, expr.range()).is_some(),
        _ => ch.truth_use(expr).is_some(),
    };
    let logging = !emptiness && inside_logging_sink(ch, expr);
    for b in reaching {
        if let Some(cv) = ch.state.count_vars.get_mut(&b) {
            cv.uses += 1;
            if emptiness {
                cv.emptiness_uses += 1;
            }
            if logging {
                cv.logging_uses += 1;
            }
        }
    }
}

pub fn finish_count_vars(ch: &mut Checker) {
    let mut vars: Vec<CountVar> = ch.state.count_vars.values().cloned().collect();
    vars.sort_by_key(|v| v.call_range.start());
    for v in vars {
        if v.uses == 0 {
            continue;
        }
        if v.emptiness_uses == v.uses {
            ch.report(msg!(
                ACT006_HELD,
                v.call_range,
                call = v.call,
                receiver = v.recv
            ));
        } else if v.logging_uses == v.uses {
            ch.report(msg!(ACT007_LOGGED, v.call_range, call = v.call));
        }
    }
}

// ------------------------------------------------------------------ ACT007

const ACT007_LOGGED: Msg = Msg::new(
    "ACT007",
    "logged",
    "{call} is computed only to be logged — a full job for a log line.",
)
.help("Drop it, read numOutputRows from the Delta commit, or use df.observe() to compute the metric in the same job.");
const ACT007_DISCARDED: Msg = Msg::new(
    "ACT007",
    "discarded",
    "{call} result is discarded — a full Spark job whose result is never used.",
)
.help("Remove the call; use df.observe() or the write's Delta history for row counts.");

/// An action whose result is the point of the call: a job or a driver pull returning a
/// value or rows (`count`, `isEmpty`, `collect`, `first`, …; not `foreach`).
fn result_action(sc: &SparkCall) -> bool {
    run_of_ty(sc.class(), sc.method).is_some_and(|r| {
        matches!(r.kind, RunKind::Job | RunKind::DriverPull) && r.returns != Returns::Nothing
    })
}

pub fn discarded_action(ch: &mut Checker, value: &Expr) {
    let Some(sc) = ch.model.spark_call(value).filter(result_action) else {
        return;
    };
    let (recv, m) = (sc.receiver, sc.method);
    if (m == "count" && !sc.has_no_args()) || !sc.is_dataframe() || sc.size().cheap_action() {
        return;
    }
    // `df.cache().count()` / `df.persist(..).count()` (or `count()` after a `df.cache()`
    // statement) is the eager-cache idiom: the job fills the cache that later actions read,
    // so its work is not lost with the discarded number.
    if m == "count" && sc.receiver_ty.facts.cached {
        return;
    }
    // `try: df.count() except ...:` / `with pytest.raises(..):` forces evaluation to
    // probe for errors (missing table, no grant, bad argument): the action is run for
    // its exception (or warning), not its result.
    if ch.run_at(value.range()).is_probe() {
        return;
    }
    // The action that fills a `df.observe(obs, ..)` Observation is the point of the
    // observe pattern (the rule's own advice), not discarded work.
    if fills_observation(ch, value, recv) {
        return;
    }
    let call = describe::call(ch, value);
    ch.report(msg!(ACT007_DISCARDED, method_range(value), call));
}

/// A discarded action runs to fill a Spark `Observation` when its function (or the
/// module, for top-level code) holds a value typed `pyspark.sql.Observation`
/// (constructed there or an annotated parameter), or when the action's DataFrame was
/// unpacked from the same call as a value whose `.get` property is read afterwards
/// (`checked_df, observation = engine.apply_checks(df, ..)`; `checked_df.count()`;
/// `observation.get`).
fn fills_observation(ch: &Checker, action: &Expr, recv: &Expr) -> bool {
    let model = ch.model;
    let at = action.range().start();
    let scope = model.function_scope_at(at);
    let in_scope = |r: TextRange| model.function_scope_at(r.start()) == scope;
    if model
        .bindings()
        .iter()
        // A parameter sits in the def header, outside its function's range: use its scope.
        .any(|b| b.ty.ty == Ty::Observation && (b.scope == scope || in_scope(b.range)))
    {
        return true;
    }
    let Some(name) = root_name(recv) else {
        return false;
    };
    let unpacked: Vec<TextRange> = model
        .reaching(name)
        .iter()
        .map(|&b| model.binding(b))
        .filter(|b| b.kind == BindingKind::Assign)
        .map(|b| b.stmt_range)
        .collect();
    model.bindings().iter().enumerate().any(|(i, b)| {
        b.kind == BindingKind::Assign
            && b.name != name.id.as_str()
            && unpacked.contains(&b.stmt_range)
            && model.binding_uses(i as u32).any(|u| {
                u.range.start() > at
                    && in_scope(u.range)
                    && u.attr.as_deref() == Some("get")
                    && !u.called
            })
    })
}

pub fn action_in_logging(ch: &mut Checker, c: &ast::ExprCall) {
    if !is_logging_sink(ch, c) {
        return;
    }
    let mut found: Vec<(TextRange, String)> = Vec::new();
    let args: Vec<&Expr> = c
        .arguments
        .args
        .iter()
        .chain(c.arguments.keywords.iter().map(|k| &k.value))
        .collect();
    for a in args {
        walk_exprs(a, &mut |e| {
            let Some(sc) = ch.model.spark_call(e) else {
                return;
            };
            let m = sc.method;
            let hit = if m == "getNumPartitions" {
                sc.class() == Ty::Rdd
            } else {
                result_action(&sc)
                    && !(m == "count" && !sc.has_no_args())
                    && sc.is_dataframe()
                    && !sc.size().cheap_action()
            };
            if hit {
                found.push((method_range(e), describe::call(ch, e)));
            }
        });
    }
    found.sort_by_key(|(r, _)| r.start());
    found.dedup_by_key(|(r, _)| *r);
    for (range, what) in found {
        ch.report(msg!(ACT007_LOGGED, range, call = what));
    }
}

// ------------------------------------------------------------------ ACT010

const ACT010_HELP: &str = "Delete it before the code runs as a job; where someone reads the output, keep it with `# noqa: ACT010`.";
const ACT010: Msg = Msg::new(
    "ACT010",
    "debug-action",
    "{call} left in pipeline code triggers extra jobs (nobody reads the output in a scheduled run).",
)
.help(ACT010_HELP);
const ACT010_PLAN: Msg = Msg::new(
    "ACT010",
    "debug-plan",
    "{call} left in pipeline code prints to the driver log: it runs no job, but it is noise in a scheduled run (and a server round trip on Spark Connect).",
)
.help(ACT010_HELP);
const ACT010_SINK: Msg = Msg::new(
    "ACT010",
    "debug-sink",
    "{call} is a debug sink: the stream's output is printed to the driver log (console) or kept in driver memory (memory), not stored.",
)
.help("Write the stream to a durable sink such as a Delta table; for a benchmark use the `noop` format.");

pub fn debug_action_expr(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let mut always_info = false;
    let mut variant = &ACT010;
    let what: String;
    // The unsafe fix deletes the statement: only offered when the DataFrame is proven.
    let certain;
    if matches!(&*c.func, Expr::Attribute(_)) {
        let Some(sc) = ch.model.spark_call(expr) else {
            return;
        };
        let (m, rt) = (sc.method, sc.receiver_ty);
        certain = rt.is_certain();
        match m {
            // A cheap preview (`df.limit(10).show()`, driver-built data; not a one-row
            // aggregate that reads the whole frame) is a deliberate small look, the
            // same answer as the global `display(x)` below.
            "show" | "display" if rt.is_dataframe() => {
                if rt.size().cheap_preview() {
                    return;
                }
                what = describe::call(ch, expr);
            }
            "printSchema" | "explain" if rt.is_dataframe() => {
                always_info = true;
                variant = &ACT010_PLAN;
                what = describe::call(ch, expr);
            }
            "format" if rt.ty == Ty::DataStreamWriter => {
                let Some(fmt) = sc.str_arg(ch.model, 0, "source") else {
                    return;
                };
                if !matches!(fmt, "console" | "memory") {
                    return;
                }
                variant = &ACT010_SINK;
                what = describe::call(ch, expr);
            }
            _ => return,
        }
    } else {
        let Some(q) = ch.model.qualified_name(&c.func) else {
            return;
        };
        // `display(x)` runs a Spark job only for a Spark DataFrame: not for
        // `display(dbutils.fs.ls(..))`, a pandas frame, a figure or `display(df.head(5))`,
        // and a cheap preview (`df.limit(10)`, driver-built data, catalog metadata; not
        // `df.agg(..)`, which reads every row) is a deliberate small look.
        // `displayHTML(..)` renders a string and never runs a job.
        if q != "databricks.display" {
            return;
        }
        let Some(arg) = c.arguments.args.first() else {
            return;
        };
        let t = ch.model.ty_of(arg);
        if !t.is_dataframe() || t.size().cheap_preview() {
            return;
        }
        certain = t.is_certain();
        what = describe::call(ch, expr);
    }
    let severity = if always_info || ch.is_notebook() {
        Severity::Info
    } else {
        Severity::Warning
    };
    let mut d = msg!(variant, method_range(expr), call = what).with_severity(severity);
    if certain
        && let Some(Node::Stmt(stmt @ Stmt::Expr(se))) = ch.parent()
        && se.value.range() == expr.range()
    {
        d = d.with_fix(delete_stmt_fix(ch, stmt));
    }
    ch.report(d);
}

// ------------------------------------------------------------------ ACT015

/// `df.approxQuantile(col, probs, 0)` / `df.stat.approxQuantile(..., relativeError=0.0)`:
/// a relative error of 0 computes exact quantiles, which the docs warn "could be very
/// expensive".
pub fn approx_quantile_exact(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["approxQuantile"]) else {
        return;
    };
    let (recv, rt) = (sc.receiver, sc.receiver_ty);
    let spark = match rt.ty {
        Ty::DataFrame => rt.is_certain(),
        // `df.stat` on a Certain DataFrame (or a Certain stat handle).
        Ty::StatFunctions => {
            rt.is_certain()
                || matches!(recv, Expr::Attribute(a) if {
                    let t = ch.model.ty_of(&a.value);
                    t.ty == Ty::DataFrame && t.is_certain()
                })
        }
        _ => false,
    };
    if !spark || c.arguments.keywords.iter().any(|k| k.arg.is_none()) {
        return;
    }
    let Some(err) = sc.arg(2, "relativeError") else {
        return;
    };
    if ch.model.constant(err).and_then(Const::as_num) != Some(0.0) {
        return;
    }
    let m = if ch.model.in_repeating_loop(expr) {
        &ACT015_LOOP
    } else {
        &ACT015
    };
    let error = short(ch, err);
    ch.report(msg!(m, method_range(expr), error));
}

// ------------------------------------------------------------------ ACT015

const ACT015_HELP: &str =
    "Use a small relative error (0.001-0.01 is plenty for thresholds and outlier bounds).";
const ACT015: Msg = Msg::new(
    "ACT015",
    "exact",
    "approxQuantile(..., relativeError={error}) computes exact quantiles: a full pass over the data that the Spark docs call \"very expensive\".",
)
.help(ACT015_HELP);
const ACT015_LOOP: Msg = Msg::new(
    "ACT015",
    "exact-in-loop",
    "approxQuantile(..., relativeError={error}) computes exact quantiles: a full pass over the data that the Spark docs call \"very expensive\". Inside a loop it runs on every iteration.",
)
.help(ACT015_HELP);
