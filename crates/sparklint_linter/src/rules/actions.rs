//! ACT — actions & driver rules: ACT001, ACT002, ACT004, ACT006, ACT007, ACT010.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, CmpOp, Expr, ExprContext, Stmt, UnaryOp};
use ruff_text_size::{Ranged, TextRange, TextSize};
use sparklint_semantic::api::action_kind;
use sparklint_semantic::{BindingKind, Elem, Ty, int_literal, str_literal};

use super::CountVar;
use super::Rule;
use super::helpers::*;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Diagnostic, Edit, Fix, Severity};

/// ACT001/002/004/006/007/010/015 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("ACT001").call(spark_action),
    Rule::new("ACT002").call(collect_unbounded),
    Rule::new("ACT004").stmt(act004_stmt).expr(act004_expr),
    Rule::new("ACT006").call(act006_call).expr(act006_expr),
    Rule::new("ACT007").stmt(act007_stmt).call(act007_call),
    Rule::new("ACT010").call(debug_action_expr),
    Rule::new("ACT015").call(approx_quantile_exact),
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

/// Range from the method name to the end of the call (`.collect()` part of a chain).
pub fn method_range(expr: &Expr) -> TextRange {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = &*c.func
    {
        return TextRange::new(a.attr.range.start(), c.range().end());
    }
    expr.range()
}

// ------------------------------------------------------------------ ACT001

const DDL_KEYWORDS: &[&str] = &[
    "CREATE", "INSERT", "MERGE", "DELETE", "UPDATE", "DROP", "ALTER", "OPTIMIZE", "VACUUM",
    "TRUNCATE", "COPY", "REFRESH", "CACHE", "ANALYZE", "REPLACE", "MSCK",
];

pub fn spark_action(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if matches!(&*c.func, Expr::Attribute(_)) {
        let Some(sc) = ch.model.spark_call(expr) else {
            return;
        };
        let (recv, m, rt) = (sc.receiver, sc.method, sc.receiver_ty);
        let kind = if rt.ty.is_session() && m == "sql" {
            ch.model
                .sql_query(c)
                .filter(|q| q.keyword().is_some_and(|w| DDL_KEYWORDS.contains(&w)))
                .map(|_| "eager-sql")
        } else {
            action_kind(rt.ty, m).map(|k| k.as_str())
        };
        if let Some(kind) = kind {
            let msg = format!(
                "Spark action: {}.{m}() triggers a job (kind: {kind}).",
                short(ch, recv)
            );
            ch.report(Diagnostic::new("ACT001", msg, method_range(expr)));
        }
        return;
    }
    if let Some(q) = ch.model.qualified_name(&c.func)
        && q == "databricks.display"
        && c.arguments
            .args
            .first()
            .is_some_and(|a| ch.model.is_dataframe(a))
    {
        ch.report(Diagnostic::new(
            "ACT001",
            "Spark action: display() triggers a job (kind: display).",
            expr.range(),
        ));
    }
}

// ------------------------------------------------------------------ ACT002

pub fn collect_unbounded(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["collect"]) else {
        return;
    };
    if !sc.has_no_args() || !sc.is_dataframe() || sc.bounded().is_some() {
        return;
    }
    let (recv, t) = (sc.receiver, sc.receiver_ty);
    let mut msg =
        ".collect() on a DataFrame with no known row bound pulls everything to the driver."
            .to_string();
    let mut sev = Severity::Warning;
    if t.facts.reduced {
        msg = ".collect() on a DataFrame with no known row bound pulls everything to the driver (filtered/aggregated — size unknown).".to_string();
        sev = Severity::Info;
    }
    let limit = ch.settings.collect_limit;
    let fix = Fix::unsafe_(
        format!("Insert .limit({limit}) before .collect()"),
        vec![Edit::insert(recv.range().end(), format!(".limit({limit})"))],
    );
    ch.report(
        Diagnostic::new("ACT002", msg, method_range(expr))
            .with_site(expr.range())
            .with_severity(sev)
            .with_fix(fix)
            .with_help("Keep the work in Spark (aggregate/write), or bound the result with .limit(n) before collecting."),
    );
}

// ------------------------------------------------------------------ ACT004

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
            if let Some(sc) = self.ch.model.spark_call(e) {
                let t = sc.class();
                if action_kind(t, sc.method).is_some() && t != Ty::PandasOnSparkFrame {
                    self.found = true;
                    return;
                }
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
/// site (the `.collect()` / `.toPandas()` call that pulls the rows).
fn row_iteration_source(ch: &Checker, iter: &Expr) -> Option<(String, TextRange)> {
    // enumerate(x) / zip(x, ...)
    if let Expr::Call(c) = iter
        && let Some(q) = ch.model.qualified_name(&c.func)
        && matches!(
            q.as_str(),
            "builtins.enumerate" | "builtins.zip" | "builtins.list" | "builtins.sorted"
        )
    {
        return c
            .arguments
            .args
            .iter()
            .find_map(|a| row_iteration_source(ch, a));
    }
    if let Some((_, recv, m)) = method_call(iter) {
        match m {
            "collect" | "toLocalIterator" => {
                if let Some(sc) = ch.model.spark_call(iter)
                    && sc.has_no_args()
                    && (sc.is_dataframe() || sc.class() == Ty::Rdd)
                    && sc.bounded().is_none()
                {
                    let what = if sc.class() == Ty::Rdd { "rdd" } else { "df" };
                    return Some((format!("for row in {what}.{m}()"), iter.range()));
                }
            }
            "iterrows" | "itertuples" | "items" => {
                if let Some(inner) = ch.model.spark_call_named(recv, &["toPandas"])
                    && inner.is_dataframe()
                    && inner.bounded().is_none()
                {
                    return Some((format!("df.toPandas().{m}()"), recv.range()));
                }
                // A pandas frame (not Spark) produced on the driver from a Spark value.
                let rt = ch.model.ty_of(recv);
                if rt.ty == Ty::PandasFrame && rt.facts.from_spark && rt.bounded().is_none() {
                    return Some((format!("df.toPandas().{m}()"), recv.range()));
                }
            }
            _ => {}
        }
        return None;
    }
    if let Expr::Name(_) = iter {
        let t = ch.model.ty_of(iter);
        if let Ty::List(e) = t.ty
            && t.facts.from_spark
            && t.bounded().is_none()
            && matches!(e, Elem::Row | Elem::Unknown)
        {
            return Some((
                format!("rows from {}.collect()", short(ch, iter)),
                iter.range(),
            ));
        }
    }
    None
}

fn report_row_iteration(ch: &mut Checker, iter: &Expr, body: Option<&[Stmt]>) {
    let Some((desc, site)) = row_iteration_source(ch, iter) else {
        return;
    };
    let mut msg = format!(
        "Iterating rows on the driver ({desc}) — express this as a DataFrame transformation."
    );
    if body.is_some_and(|b| body_has_action(ch, b)) {
        msg.push_str(" The loop body also runs a Spark action per row (N+1 jobs).");
    }
    ch.report(Diagnostic::new("ACT004", msg, iter.range()).with_site(site).with_help(
        "Use select/withColumn/groupBy (or mapInPandas/applyInPandas for arbitrary Python) so the work runs on the executors.",
    ));
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

/// Is the current node (`e`) used directly as a boolean test?
fn is_test_context(ch: &Checker, e: &Expr) -> bool {
    let r = e.range();
    let mut i = 0;
    let mut cur = r;
    loop {
        match ch.ancestor(i) {
            Some(Node::Stmt(Stmt::If(s))) => {
                return s.test.range() == cur
                    || s.elif_else_clauses
                        .iter()
                        .any(|c| c.test.as_ref().is_some_and(|t| t.range() == cur));
            }
            Some(Node::Stmt(Stmt::While(s))) => return s.test.range() == cur,
            Some(Node::Stmt(Stmt::Assert(s))) => return s.test.range() == cur,
            Some(Node::Expr(Expr::If(x))) => return x.test.range() == cur,
            Some(Node::Expr(Expr::BoolOp(b))) => {
                cur = b.range;
                i += 1;
            }
            Some(Node::Expr(Expr::UnaryOp(u))) if u.op == UnaryOp::Not => {
                cur = u.range;
                i += 1;
            }
            _ => return false,
        }
    }
}

fn needs_parens_for_not(ch: &Checker) -> bool {
    matches!(
        ch.parent(),
        Some(Node::Expr(
            Expr::BinOp(_)
                | Expr::UnaryOp(_)
                | Expr::Compare(_)
                | Expr::Attribute(_)
                | Expr::Subscript(_)
                | Expr::Await(_)
                | Expr::Starred(_)
        ))
    )
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
        if !empty && needs_parens_for_not(ch) {
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

fn emptiness_message(ch: &Checker, recv: &Expr) -> String {
    let s = short(ch, recv);
    format!("{s}.count() used as an emptiness check scans every row — use {s}.isEmpty().")
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
        let msg = emptiness_message(ch, recv);
        ch.report(Diagnostic::new("ACT006", msg, expr.range()).with_fix(fix));
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
    let msg = emptiness_message(ch, recv);
    ch.report(Diagnostic::new("ACT006", msg, expr.range()).with_fix(fix));
}

pub fn count_in_bool_context(ch: &mut Checker, expr: &Expr) {
    let Some(recv) = df_count_call(ch, expr) else {
        return;
    };
    // `not df.count()` is reported by `not_count`.
    if matches!(ch.parent(), Some(Node::Expr(Expr::UnaryOp(u))) if u.op == UnaryOp::Not) {
        return;
    }
    if !is_test_context(ch, expr) {
        return;
    }
    let fix = is_empty_fix(ch, recv, false, expr.range());
    let msg = emptiness_message(ch, recv);
    ch.report(Diagnostic::new("ACT006", msg, expr.range()).with_fix(fix));
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
    let s = short(ch, &a.value);
    let text = format!("{base}.isEmpty()");
    let mut d = Diagnostic::new(
        "ACT006",
        format!("{s}.rdd.isEmpty() converts the DataFrame to an RDD first — use {s}.isEmpty()."),
        expr.range(),
    );
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
        if ch.model.ty_of(recv).bounded().is_some() {
            continue;
        }
        let cv = CountVar {
            call_range: method_range(v),
            recv: short(ch, recv),
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
    let emptiness = match ch.parent() {
        Some(Node::Expr(Expr::Compare(c))) => compare_emptiness(c, expr.range()).is_some(),
        Some(Node::Expr(Expr::UnaryOp(u))) if u.op == UnaryOp::Not => true,
        _ => is_test_context(ch, expr),
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
            ch.report(Diagnostic::new(
                "ACT006",
                format!(
                    "{r}.count() is only used as an emptiness check — use {r}.isEmpty() (stops at the first row).",
                    r = v.recv
                ),
                v.call_range,
            ));
        } else if v.logging_uses == v.uses {
            ch.report(Diagnostic::new(
                "ACT007",
                format!(
                    "{}.count() is computed only to be logged — a full job for a log line.",
                    v.recv
                ),
                v.call_range,
            ));
        }
    }
}

// ------------------------------------------------------------------ ACT007

const LOGGED_ACTIONS: &[&str] = &[
    "count", "collect", "first", "head", "take", "isEmpty", "toPandas",
];

pub fn discarded_action(ch: &mut Checker, value: &Expr) {
    let Some(sc) = ch.model.spark_call_named(value, LOGGED_ACTIONS) else {
        return;
    };
    let (recv, m) = (sc.receiver, sc.method);
    if (m == "count" && !sc.has_no_args()) || !sc.is_dataframe() || sc.bounded().is_some() {
        return;
    }
    // `try: df.count() except ...:` forces evaluation to probe for errors (missing
    // table, no grant): the action is run for its exception, not its result.
    if let Some(Stmt::Try(t)) = ch.parent_stmt_of_current()
        && !t.handlers.is_empty()
        && t.body
            .iter()
            .any(|s| s.range().contains_range(value.range()))
    {
        return;
    }
    ch.report(
        Diagnostic::new(
            "ACT007",
            format!(
                "{}.{m}() result is discarded — a full Spark job whose result is never used.",
                short(ch, recv)
            ),
            method_range(value),
        )
        .with_help(
            "Remove the call; use df.observe() or the write's Delta history for row counts.",
        ),
    );
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
            let (recv, m) = (sc.receiver, sc.method);
            let hit = if m == "getNumPartitions" {
                sc.class() == Ty::Rdd
            } else {
                LOGGED_ACTIONS.contains(&m)
                    && !(m == "count" && !sc.has_no_args())
                    && sc.is_dataframe()
                    && sc.bounded().is_none()
            };
            if hit {
                found.push((method_range(e), format!("{}.{m}()", short(ch, recv))));
            }
        });
    }
    found.sort_by_key(|(r, _)| r.start());
    found.dedup_by_key(|(r, _)| *r);
    for (range, what) in found {
        ch.report(
            Diagnostic::new(
                "ACT007",
                format!("{what} is computed only to be logged — a full job for a log line."),
                range,
            )
            .with_help("Drop it, read numOutputRows from the Delta commit, or use df.observe() to compute the metric in the same job."),
        );
    }
}

// ------------------------------------------------------------------ ACT010

pub(crate) fn delete_stmt_fix(ch: &Checker, stmt: &Stmt) -> Fix {
    let r = stmt.range();
    if block_len(ch, stmt) <= 1 {
        return Fix::unsafe_("Replace with `pass`", vec![Edit::replace(r, "pass")]);
    }
    let text = &ch.source.text;
    let start = usize::from(r.start());
    let end = usize::from(r.end());
    let line_start = text[..start].rfind('\n').map_or(0, |i| i + 1);
    let line_end = text[end..].find('\n').map_or(text.len(), |i| end + i + 1);
    let before = &text[line_start..start];
    let after = text[end..line_end].trim();
    if before.trim().is_empty() && (after.is_empty() || after.starts_with('#')) {
        Fix::unsafe_(
            "Remove the statement",
            vec![Edit::delete(TextRange::new(
                TextSize::from(line_start as u32),
                TextSize::from(line_end as u32),
            ))],
        )
    } else {
        Fix::unsafe_("Replace with `pass`", vec![Edit::replace(r, "pass")])
    }
}

pub fn debug_action_expr(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let mut always_info = false;
    let what: String;
    // The unsafe fix deletes the statement: only offered when the DataFrame is proven.
    let certain;
    if matches!(&*c.func, Expr::Attribute(_)) {
        let Some(sc) = ch.model.spark_call(expr) else {
            return;
        };
        let (recv, m, rt) = (sc.receiver, sc.method, sc.receiver_ty);
        certain = rt.is_certain();
        match m {
            "show" | "display" if rt.is_dataframe() => what = format!("{}.{m}()", short(ch, recv)),
            "printSchema" | "explain" if rt.is_dataframe() => {
                always_info = true;
                what = format!("{}.{m}()", short(ch, recv));
            }
            "format" if rt.ty == Ty::DataStreamWriter => {
                let Some(fmt) = c.arguments.args.first().and_then(str_literal) else {
                    return;
                };
                if !matches!(fmt, "console" | "memory") {
                    return;
                }
                what = format!("writeStream.format(\"{fmt}\")");
            }
            _ => return,
        }
    } else {
        let Some(q) = ch.model.qualified_name(&c.func) else {
            return;
        };
        // `display(x)` runs a Spark job only for a Spark DataFrame: not for
        // `display(dbutils.fs.ls(..))`, a pandas frame, a figure or `display(df.head(5))`,
        // and a row-bounded frame (`df.limit(10)`) is a deliberate small preview.
        // `displayHTML(..)` renders a string and never runs a job.
        if q != "databricks.display" {
            return;
        }
        let Some(arg) = c.arguments.args.first() else {
            return;
        };
        let t = ch.model.ty_of(arg);
        if !t.is_dataframe() || t.bounded().is_some() {
            return;
        }
        certain = t.is_certain();
        what = "display()".to_string();
    }
    let severity = if always_info || ch.is_notebook() {
        Severity::Info
    } else {
        Severity::Warning
    };
    let mut d = Diagnostic::new(
        "ACT010",
        format!("{what} left in pipeline code triggers extra jobs (nobody reads the output in a scheduled run)."),
        method_range(expr),
    )
    .with_severity(severity);
    if certain
        && let Some(Node::Stmt(stmt @ Stmt::Expr(se))) = ch.parent()
        && se.value.range() == expr.range()
    {
        d = d.with_fix(delete_stmt_fix(ch, stmt));
    }
    ch.report(d);
}

// ------------------------------------------------------------------ ACT015

/// Constant value of a numeric literal (`0`, `0.0`, `-0.0`), following one binding.
fn const_number(ch: &Checker, e: &Expr, depth: u32) -> Option<f64> {
    match e {
        Expr::NumberLiteral(n) => match &n.value {
            ast::Number::Int(i) => i.as_i64().map(|v| v as f64),
            ast::Number::Float(f) => Some(*f),
            ast::Number::Complex { .. } => None,
        },
        Expr::UnaryOp(u) if matches!(u.op, UnaryOp::USub | UnaryOp::UAdd) => {
            const_number(ch, &u.operand, depth).map(|v| if u.op == UnaryOp::USub { -v } else { v })
        }
        Expr::Name(n) if depth < 3 => {
            let b = ch.model.single_binding(n)?;
            let v = ch.model.binding(b).value?;
            const_number(ch, v, depth + 1)
        }
        _ => None,
    }
}

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
    let Some(err) = sparklint_semantic::call_arg(c, 2, "relativeError") else {
        return;
    };
    if const_number(ch, err, 0) != Some(0.0) {
        return;
    }
    let mut msg = format!(
        "approxQuantile(..., relativeError={}) computes exact quantiles: a full pass over the data that the Spark docs call \"very expensive\".",
        short(ch, err)
    );
    if ch.model.in_loop(expr) {
        msg.push_str(" Inside a loop it runs on every iteration.");
    }
    ch.report(
        Diagnostic::new("ACT015", msg, method_range(expr)).with_help(
            "Use a small relative error (0.001-0.01 is plenty for thresholds and outlier bounds).",
        ),
    );
}
