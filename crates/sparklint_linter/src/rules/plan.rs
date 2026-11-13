//! Plan-shape rules: PRF004 non-equi-join, PRF012 explode-before-filter,
//! PRF015 collect-dedup-redundant, PRF023 pivot-without-values, PRF024 cube-many-columns,
//! PRF025 not-in-subquery, PRF026 many-distinct-aggregates, PRF027 order-by-rand,
//! PRF032 or-join-condition, PRF035 keyless-grouped-pandas,
//! PRF036 forced-nested-loop-join-hint, PRF022 sql-select-star (`SELECT *` flowing into
//! a write / collect / cache, followed through [`super::flow`]).

use ruff_python_ast::{self as ast, CmpOp, Expr};
use ruff_text_size::{Ranged, TextRange};
use rustc_hash::FxHashSet;
use sparklint_semantic::api::ActionKind;
use sparklint_semantic::{
    BindingId, SqlCond as JoinCond, SqlConjunct, SqlText, Ty, call_arg, str_literal,
};

use super::Rule;
use super::actions::method_range;
use super::flow::{self, Sink};
use super::helpers::*;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Diagnostic, Edit, Fix, Severity};

/// PRF004/012/015/022/023/024/025/026/027/032/035/036 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PRF004").call(non_equi_join),
    Rule::new("PRF012").call(explode_before_filter),
    Rule::new("PRF015").call(collect_dedup_redundant),
    Rule::new("PRF023").call(pivot_without_values),
    Rule::new("PRF024").call(cube_many_columns),
    Rule::new("PRF025").call(not_in_subquery),
    Rule::new("PRF026").call(many_distinct_aggregates),
    Rule::new("PRF027").call(order_by_rand),
    Rule::new("PRF032").call(or_join_condition),
    Rule::new("PRF035").call(keyless_grouped_pandas),
    Rule::new("PRF036").call(forced_nested_loop_join_hint),
    Rule::new("PRF022")
        .call(sql_select_star)
        .finish(finish_select_star),
];

// ------------------------------------------------------------------ PRF004

fn is_literalish(ch: &Checker, e: &Expr) -> bool {
    matches!(
        e,
        Expr::StringLiteral(_)
            | Expr::NumberLiteral(_)
            | Expr::BooleanLiteral(_)
            | Expr::NoneLiteral(_)
    ) || matches!(e, Expr::Call(c) if ch.model.is_spark_function(&c.func, "lit"))
        || matches!(e, Expr::UnaryOp(u) if matches!(&*u.operand, Expr::NumberLiteral(_)))
}

/// Equality between two non-literal operands somewhere in `e`.
fn has_equality(ch: &Checker, e: &Expr) -> bool {
    let mut found = false;
    walk_exprs(e, &mut |x| {
        if found {
            return;
        }
        match x {
            Expr::Compare(c) => {
                let ops = c.operands.iter().collect::<Vec<_>>();
                for (i, op) in c.ops.iter().enumerate() {
                    if *op == CmpOp::Eq
                        && !is_literalish(ch, ops[i])
                        && !is_literalish(ch, ops[i + 1])
                    {
                        found = true;
                    }
                }
            }
            Expr::Call(_) => {
                if let Some((c, _, m)) = method_call(x)
                    && m == "eqNullSafe"
                    && c.arguments
                        .args
                        .first()
                        .is_some_and(|a| !is_literalish(ch, a))
                {
                    found = true;
                }
                if let Expr::Call(c) = x
                    && ch.model.is_spark_function(&c.func, "expr")
                    && let Some(s) = c.arguments.args.first().and_then(str_literal)
                    && SqlText::new(s).cond() == JoinCond::Equi
                {
                    found = true;
                }
            }
            _ => {}
        }
    });
    found
}

fn join_cond(ch: &Checker, on: &Expr, depth: u32) -> JoinCond {
    match on {
        Expr::StringLiteral(_) => JoinCond::Equi,
        Expr::List(ast::ExprList { elts, .. }) | Expr::Tuple(ast::ExprTuple { elts, .. }) => {
            let kinds: Vec<JoinCond> = elts.iter().map(|e| join_cond(ch, e, depth)).collect();
            if kinds.contains(&JoinCond::Equi) {
                JoinCond::Equi
            } else if !kinds.is_empty() && kinds.iter().all(|k| *k == JoinCond::NonEqui) {
                JoinCond::NonEqui
            } else {
                JoinCond::Unknown
            }
        }
        Expr::BooleanLiteral(_) => JoinCond::Unknown,
        Expr::Call(c) if ch.model.is_spark_function(&c.func, "lit") => JoinCond::Unknown,
        Expr::Call(c) if ch.model.is_spark_function(&c.func, "expr") => c
            .arguments
            .args
            .first()
            .and_then(str_literal)
            .map_or(JoinCond::Unknown, |s| SqlText::new(s).cond()),
        Expr::Name(n) => {
            let t = ch.model.ty_of(on);
            match t.ty {
                Ty::Str | Ty::List(sparklint_semantic::Elem::Str) => return JoinCond::Equi,
                Ty::Column => {}
                _ => return JoinCond::Unknown,
            }
            if depth >= 3 {
                return JoinCond::Unknown;
            }
            match ch
                .model
                .single_binding(n)
                .and_then(|b| ch.model.binding(b).value)
            {
                Some(v) if v.range() != on.range() => join_cond(ch, v, depth + 1),
                _ => JoinCond::Unknown,
            }
        }
        other => {
            let t = ch.model.ty_of(other);
            if !(t.is_column() && t.is_certain()) {
                return JoinCond::Unknown;
            }
            if has_equality(ch, other) {
                return JoinCond::Equi;
            }
            // Calls of user helpers (`_match(a, b)`) and Column variables hide the
            // condition: resolve variables, give up on opaque calls.
            let mut nested_equi = false;
            let mut opaque = false;
            walk_exprs(other, &mut |x| match x {
                Expr::Name(_) if x.range() != other.range() && ch.model.is_column(x) => {
                    match join_cond(ch, x, depth + 1) {
                        JoinCond::Equi => nested_equi = true,
                        JoinCond::Unknown => opaque = true,
                        JoinCond::NonEqui => {}
                    }
                }
                Expr::Call(xc) => {
                    let spark_fn = ch
                        .model
                        .qualified_name(&xc.func)
                        .is_some_and(|q| q.starts_with("pyspark.sql.functions."));
                    let spark_method = matches!(&*xc.func, Expr::Attribute(a)
                        if ch.model.ty_of(&a.value).ty.is_spark());
                    if !spark_fn && !spark_method {
                        opaque = true;
                    }
                }
                _ => {}
            });
            if nested_equi {
                JoinCond::Equi
            } else if opaque {
                JoinCond::Unknown
            } else {
                JoinCond::NonEqui
            }
        }
    }
}

pub fn non_equi_join(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["join"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let rt = sc.receiver_ty;
    let Some(on) = call_arg(c, 1, "on") else {
        return;
    };
    if join_cond(ch, on, 0) != JoinCond::NonEqui {
        return;
    }
    let small = rt.bounded().is_some()
        || call_arg(c, 0, "other").is_some_and(|o| {
            ch.model.ty_of(o).bounded().is_some()
                || matches!(o, Expr::Call(oc) if ch.model.is_spark_function(&oc.func, "broadcast"))
        });
    let mut d = Diagnostic::new(
        "PRF004",
        "Join condition has no equality predicate — it executes as a nested-loop join (O(n·m) comparisons).",
        method_range(expr),
    )
    .with_help("Add an equi-key (e.g. a time bucket: floor(ts/3600) on both sides) next to the range predicate, or use a range-join hint on Databricks.");
    if small {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ PRF032

/// How one conjunct of a join condition constrains the join keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Conj {
    /// A plain equality Spark can use as a hash / sort-merge key.
    Equi,
    /// A disjunction that contains an equality: no usable key from it.
    OrWithEq,
    /// Anything else (range predicate, literal filter...).
    Other,
    /// Opaque (helper call, unresolved variable): we can't tell.
    Unknown,
}

fn has_bitor(e: &Expr) -> bool {
    let mut found = false;
    walk_exprs(e, &mut |x| {
        if let Expr::BinOp(b) = x
            && b.op == ast::Operator::BitOr
        {
            found = true;
        }
    });
    found
}

fn opaque_call(ch: &Checker, e: &Expr) -> bool {
    let mut opaque = false;
    walk_exprs(e, &mut |x| {
        if let Expr::Call(xc) = x {
            let spark_fn = ch
                .model
                .qualified_name(&xc.func)
                .is_some_and(|q| q.starts_with("pyspark.sql.functions."));
            let spark_method = matches!(&*xc.func, Expr::Attribute(a)
                if ch.model.ty_of(&a.value).ty.is_spark());
            if !spark_fn && !spark_method {
                opaque = true;
            }
        }
    });
    opaque
}

pub(crate) fn conjuncts(ch: &Checker, e: &Expr, depth: u32, out: &mut Vec<Conj>) {
    match e {
        Expr::List(ast::ExprList { elts, .. }) | Expr::Tuple(ast::ExprTuple { elts, .. }) => {
            for x in elts {
                conjuncts(ch, x, depth, out);
            }
        }
        Expr::StringLiteral(_) => out.push(Conj::Equi),
        Expr::BinOp(b) if b.op == ast::Operator::BitAnd => {
            conjuncts(ch, &b.left, depth, out);
            conjuncts(ch, &b.right, depth, out);
        }
        Expr::Name(n) => {
            let t = ch.model.ty_of(e);
            match t.ty {
                Ty::Str | Ty::List(sparklint_semantic::Elem::Str) => return out.push(Conj::Equi),
                Ty::Column => {}
                _ => return out.push(Conj::Unknown),
            }
            match ch
                .model
                .single_binding(n)
                .and_then(|b| ch.model.binding(b).value)
            {
                Some(v) if depth < 3 && v.range() != e.range() => conjuncts(ch, v, depth + 1, out),
                _ => out.push(Conj::Unknown),
            }
        }
        Expr::Call(c) if ch.model.is_spark_function(&c.func, "expr") => {
            match c.arguments.args.first().and_then(str_literal) {
                Some(s) => out.extend(SqlText::new(s).conjuncts().into_iter().map(|k| match k {
                    SqlConjunct::Equi => Conj::Equi,
                    SqlConjunct::OrWithEq => Conj::OrWithEq,
                    SqlConjunct::Other => Conj::Other,
                })),
                None => out.push(Conj::Unknown),
            }
        }
        Expr::BinOp(b) if b.op == ast::Operator::BitOr => {
            let t = ch.model.ty_of(e);
            if !(t.is_column() && t.is_certain()) {
                return out.push(Conj::Unknown);
            }
            out.push(if has_equality(ch, e) {
                Conj::OrWithEq
            } else {
                Conj::Other
            });
        }
        other => {
            let t = ch.model.ty_of(other);
            if !(t.is_column() && t.is_certain()) {
                return out.push(Conj::Unknown);
            }
            if has_equality(ch, other) && !has_bitor(other) {
                out.push(Conj::Equi);
            } else if opaque_call(ch, other) {
                out.push(Conj::Unknown);
            } else {
                // Column variables inside (e.g. `~cond`) stay unresolved: treat as unknown.
                let mut names = false;
                walk_exprs(other, &mut |x| {
                    if matches!(x, Expr::Name(_))
                        && x.range() != other.range()
                        && ch.model.is_column(x)
                    {
                        names = true;
                    }
                });
                out.push(if names { Conj::Unknown } else { Conj::Other });
            }
        }
    }
}

pub fn or_join_condition(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["join"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let rt = sc.receiver_ty;
    let Some(on) = call_arg(c, 1, "on") else {
        return;
    };
    let mut cs = Vec::new();
    conjuncts(ch, on, 0, &mut cs);
    if !cs.contains(&Conj::OrWithEq) || cs.iter().any(|k| matches!(k, Conj::Equi | Conj::Unknown)) {
        return;
    }
    let small = rt.bounded().is_some()
        || call_arg(c, 0, "other").is_some_and(|o| {
            ch.model.ty_of(o).bounded().is_some()
                || matches!(o, Expr::Call(oc) if ch.model.is_spark_function(&oc.func, "broadcast"))
        });
    let mut d = Diagnostic::new(
        "PRF032",
        "Join condition ORs equalities together — Spark can't use it as a join key, so it runs a nested-loop join (O(n·m) comparisons).",
        method_range(expr),
    )
    .with_help("Join once per OR branch on its equality and unionByName the results (dropDuplicates if a row can match both), or explode the candidate keys into one key column and equi-join on it.");
    if small {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ PRF012

const EXPLODE_FNS: &[&str] = &[
    "explode",
    "explode_outer",
    "posexplode",
    "posexplode_outer",
    "inline",
    "inline_outer",
];

fn is_explode_call(ch: &Checker, e: &Expr) -> bool {
    let mut found = false;
    walk_exprs(e, &mut |x| {
        if let Expr::Call(c) = x
            && EXPLODE_FNS
                .iter()
                .any(|f| ch.model.is_spark_function(&c.func, f))
        {
            found = true;
        }
    });
    found
}

/// Output column names of an explode inside `select(...)` args.
fn select_explode_outputs(ch: &Checker, c: &ast::ExprCall) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut any = false;
    for a in &c.arguments.args {
        if !is_explode_call(ch, a) {
            continue;
        }
        any = true;
        // `F.explode(x).alias("n")` / `.alias("p", "c")`
        if let Some((ac, _, "alias" | "name" | "as_")) = method_call(a) {
            for n in &ac.arguments.args {
                if let Some(s) = str_literal(n) {
                    out.push(s.to_string());
                }
            }
        } else {
            out.extend(["col", "pos", "key", "value"].map(String::from));
        }
    }
    any.then_some(out)
}

/// Column names referenced by a filter predicate; `None` when it references
/// something we cannot name (a Column variable).
fn predicate_refs(ch: &Checker, e: &Expr) -> Option<FxHashSet<String>> {
    let mut refs = FxHashSet::default();
    let mut unknown = false;
    if let Some(s) = str_literal(e) {
        for tok in s.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.')) {
            if !tok.is_empty() {
                refs.insert(tok.split('.').next().unwrap_or(tok).to_string());
            }
        }
        return Some(refs);
    }
    walk_exprs(e, &mut |x| match x {
        Expr::StringLiteral(s) => {
            let v = s.value.to_str();
            refs.insert(v.split('.').next().unwrap_or(v).to_string());
        }
        Expr::Attribute(a) if ch.model.is_dataframe(&a.value) => {
            refs.insert(a.attr.as_str().to_string());
        }
        Expr::Name(_)
            // `F`, `df` are fine; a Column/str variable could name anything.
            if !is_module_like(ch, x) => {
                unknown = true;
            }
        _ => {}
    });
    (!unknown).then_some(refs)
}

fn is_module_like(ch: &Checker, e: &Expr) -> bool {
    let t = ch.model.ty_of(e);
    ch.model.qualified_name(e).is_some() || (t.ty.is_spark() && !t.is_column())
}

pub fn explode_before_filter(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["filter", "where"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let recv = sc.receiver;
    let Some(pred) = call_arg(c, 0, "condition") else {
        return;
    };
    let Some(refs) = predicate_refs(ch, pred) else {
        return;
    };
    // Walk down the receiver chain; remember columns created above the explode.
    let mut later: Vec<(String, &Expr)> = Vec::new();
    let mut e = recv;
    let mut same_expr = true;
    let mut hops = 0;
    let (explode_range, outputs) = loop {
        if let Some(sc) = ch.model.spark_call(e)
            && sc.is_dataframe()
        {
            let (cc, r, mm) = (sc.call, sc.receiver, sc.method);
            match mm {
                "withColumn" => {
                    let name = cc.arguments.args.first().and_then(str_literal);
                    let val = cc.arguments.args.get(1);
                    if let (Some(n), Some(v)) = (name, val) {
                        if is_explode_call(ch, v) {
                            break (method_range(e), vec![n.to_string()]);
                        }
                        later.push((n.to_string(), v));
                    }
                }
                "select" => {
                    if let Some(outs) = select_explode_outputs(ch, cc) {
                        break (method_range(e), outs);
                    }
                    // A select drops/renames columns: stop (refs may be new names).
                    return;
                }
                "filter" | "where" | "withColumns" | "selectExpr" | "groupBy" | "agg" | "join"
                | "toDF" | "withColumnRenamed" => return,
                _ => {}
            }
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
            same_expr = false;
            e = v;
            continue;
        }
        return;
    };
    // Columns derived from the explode output after it.
    let mut derived: FxHashSet<String> = outputs.into_iter().collect();
    for (name, v) in later.iter().rev() {
        let mut uses = false;
        walk_exprs(v, &mut |x| {
            if let Some(s) = str_literal(x)
                && derived.contains(s.split('.').next().unwrap_or(s))
            {
                uses = true;
            }
        });
        if uses {
            derived.insert(name.clone());
        }
    }
    if refs.iter().any(|r| derived.contains(r)) {
        return;
    }
    let range = if same_expr {
        explode_range
    } else {
        method_range(expr)
    };
    if !ch.state.prf012_seen.insert(range) {
        return;
    }
    ch.report(
        Diagnostic::new(
            "PRF012",
            "explode() runs before a filter that does not use the exploded values — filter first.",
            range,
        )
        .with_help(
            "Move the filter below the explode: df.filter(cond).withColumn(\"x\", F.explode(...)).",
        ),
    );
}

// ------------------------------------------------------------------ PRF015

pub fn collect_dedup_redundant(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if !ch.model.is_spark_function(&c.func, "array_distinct") || c.arguments.args.len() != 1 {
        return;
    }
    let arg = &c.arguments.args[0];
    // peel `.over(w)`
    let (agg, over): (&Expr, Option<TextRange>) = match method_call(arg) {
        Some((oc, r, "over")) => (r, Some(TextRange::new(r.range().end(), oc.range().end()))),
        _ => (arg, None),
    };
    let Expr::Call(inner) = agg else { return };
    let kind = if ch.model.is_spark_function(&inner.func, "collect_list") {
        "collect_list"
    } else if ch.model.is_spark_function(&inner.func, "collect_set") {
        "collect_set"
    } else {
        return;
    };
    let msg = if kind == "collect_list" {
        "array_distinct(collect_list(x)) materialises duplicates and then drops them — use collect_set(x)."
    } else {
        "array_distinct(collect_set(x)) is redundant — collect_set already deduplicates."
    };
    let mut d = Diagnostic::new("PRF015", msg, expr.range());
    // Fix: only for `<mod>.collect_list(...)` so the replacement name resolves.
    if let Expr::Attribute(fa) = &*inner.func {
        let module = ch.slice(fa.value.range());
        let args = ch.slice(inner.arguments.range);
        let over_src = over.map(|r| ch.slice(r)).unwrap_or("");
        let text = format!("{module}.collect_set{args}{over_src}");
        let order_sensitive = kind == "collect_list" && order_sensitive_parent(ch, expr);
        let fix_msg = format!("Replace with `{text}`");
        let edits = vec![Edit::replace(expr.range(), text)];
        d = d.with_fix(if order_sensitive {
            Fix::unsafe_(fix_msg, edits)
        } else {
            Fix::safe(fix_msg, edits)
        });
    }
    ch.report(d);
}

/// The array is consumed by something that depends on element order.
fn order_sensitive_parent(ch: &Checker, expr: &Expr) -> bool {
    match ch.parent() {
        Some(Node::Expr(Expr::Subscript(_))) => true,
        Some(Node::Expr(Expr::Attribute(a))) => {
            matches!(a.attr.as_str(), "getItem" | "__getitem__")
        }
        Some(Node::Expr(Expr::Call(c))) => {
            c.arguments.args.iter().any(|a| a.range() == expr.range())
                && [
                    "array_join",
                    "element_at",
                    "slice",
                    "get",
                    "array_position",
                    "concat_ws",
                ]
                .iter()
                .any(|f| ch.model.is_spark_function(&c.func, f))
        }
        _ => false,
    }
}

// ------------------------------------------------------------------ PRF023

/// `df.groupBy(..).pivot("col")` without a `values` list: Spark runs an extra job
/// (distinct values of the pivot column, collected to the driver) just to build the
/// output schema, every time the plan is built.
pub fn pivot_without_values(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["pivot"]) else {
        return;
    };
    if sc.class() != Ty::GroupedData {
        return;
    }
    let args = &c.arguments;
    if args.args.iter().any(|a| a.is_starred_expr())
        || args.keywords.iter().any(|k| k.arg.is_none())
    {
        return; // `*args` / `**kw`: the values may be passed through them
    }
    let Some(col) = call_arg(c, 0, "pivot_col") else {
        return;
    };
    match call_arg(c, 1, "values") {
        None => {}
        Some(v) if v.is_none_literal_expr() => {}
        Some(_) => return,
    }
    let mut msg = format!(
        "pivot({}) without a values list runs an extra Spark job to find the distinct values.",
        short(ch, col)
    );
    if ch.model.in_loop(expr) {
        msg.push_str(" Inside a loop that job repeats on every iteration.");
    }
    ch.report(Diagnostic::new("PRF023", msg, method_range(expr)).with_help(
        "Pass the values: .pivot(\"col\", [\"a\", \"b\", ...]) (this also fixes the column order).",
    ));
}

// ------------------------------------------------------------------ PRF024

/// `df.cube(c1..cN)` (and `GROUP BY CUBE(..)` / `WITH CUBE` in `spark.sql`) makes 2^N
/// grouping sets: the Expand operator copies every input row 2^N times before the
/// shuffle. `rollup` makes N+1 sets and is reported from a higher column count.
pub fn cube_many_columns(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["cube", "rollup", "sql"]) else {
        return;
    };
    let m = sc.method;
    match m {
        "cube" | "rollup" if sc.is_dataframe() => {
            let args = &c.arguments;
            if args.args.iter().any(|a| a.is_starred_expr()) || !args.keywords.is_empty() {
                return; // `*cols` / keywords: the column count is unknown
            }
            // `df.cube(["a", "b"])` passes the columns as one list.
            let n = match args.args.as_ref() {
                [Expr::List(l)] => l.elts.len(),
                [Expr::Tuple(t)] => t.elts.len(),
                a => a.len(),
            };
            report_cube(ch, m == "cube", n, method_range(expr), "");
        }
        "sql" if sc.class().is_session() => {
            if let Some((is_cube, n)) = ch.model.sql_query(c).and_then(|q| q.cube_width()) {
                report_cube(ch, is_cube, n, method_range(expr), "SQL ");
            }
        }
        _ => {}
    }
}

fn report_cube(ch: &mut Checker, is_cube: bool, n: usize, range: TextRange, what: &str) {
    let (min, sets) = if is_cube {
        (ch.settings.cube_min_columns, 1u128.checked_shl(n as u32))
    } else {
        (ch.settings.rollup_min_columns, Some(n as u128 + 1))
    };
    if min == 0 || n < min as usize {
        return;
    }
    let op = if is_cube { "CUBE" } else { "ROLLUP" };
    let sets = sets.map_or_else(|| "2^N".to_string(), |s| s.to_string());
    ch.report(
        Diagnostic::new(
            "PRF024",
            format!(
                "{what}{op} over {n} columns makes {sets} grouping sets: every input row is copied {sets} times before the shuffle."
            ),
            range,
        )
        .with_help(
            "List only the grouping sets you need: GROUP BY GROUPING SETS ((a, b), (a), ()) in SQL, or df.groupingSets(...) on Spark >= 4.0.",
        ),
    );
}

// ------------------------------------------------------------------ PRF025

/// `spark.sql("... x NOT IN (SELECT ...)")`: the subquery becomes a null-aware anti
/// join, which Spark plans as a broadcast nested-loop join unless a single-column
/// subquery is small enough to broadcast.
pub fn not_in_subquery(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    if !ch
        .model
        .spark_sql(expr)
        .is_some_and(|q| q.not_in_subquery())
    {
        return;
    }
    let mut msg = "SQL `NOT IN (SELECT ...)` is planned as a null-aware anti join, usually a broadcast nested-loop join: very slow, or a broadcast failure when the subquery is large.".to_string();
    if ch.model.in_loop(expr) {
        msg.push_str(" Inside a loop it runs on every iteration.");
    }
    ch.report(Diagnostic::new("PRF025", msg, method_range(expr)).with_help(
        "If the subquery column has no NULLs, write NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k) or use a DataFrame left_anti join: both plan as ordinary hash or sort-merge anti joins.",
    ));
}

// ------------------------------------------------------------------ PRF026

const DISTINCT_FNS: &[&str] = &[
    "countDistinct",
    "count_distinct",
    "sumDistinct",
    "sum_distinct",
];

/// Several exact distinct aggregates over different column sets in one aggregation
/// (`agg(F.countDistinct("a"), F.countDistinct("b"), ...)`, `selectExpr` / `F.expr`
/// strings, `spark.sql` SELECT lists): `RewriteDistinctAggregates` adds an Expand that
/// copies every input row once per distinct column set before the shuffle.
pub fn many_distinct_aggregates(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let min = ch.settings.distinct_aggregates_min as usize;
    if min == 0 {
        return;
    }
    let Some(sc) = ch
        .model
        .spark_call_named(expr, &["agg", "select", "selectExpr", "sql"])
    else {
        return;
    };
    let (n, what) = match sc.method {
        "agg" if sc.class() == Ty::GroupedData || sc.is_dataframe() => {
            (distinct_sets(ch, c, false), "agg()")
        }
        "select" if sc.is_dataframe() => (distinct_sets(ch, c, false), "select()"),
        "selectExpr" if sc.is_dataframe() => (distinct_sets(ch, c, true), "selectExpr()"),
        "sql" if sc.class().is_session() => {
            let Some(q) = ch.model.sql_query(c) else {
                return;
            };
            let n = q
                .distinct_aggregates()
                .into_iter()
                .map(|keys| keys.into_iter().collect::<FxHashSet<_>>().len())
                .max()
                .unwrap_or(0);
            (n, "SQL SELECT")
        }
        _ => return,
    };
    if n < min {
        return;
    }
    let mut msg = format!(
        "{n} exact distinct aggregates over different columns in one {what}: Spark copies every input row about {n} times (one copy per distinct column set) before the shuffle."
    );
    if ch.model.in_loop(expr) {
        msg.push_str(" Inside a loop that repeats on every iteration.");
    }
    ch.report(Diagnostic::new("PRF026", msg, method_range(expr)).with_help(
        "Use F.approx_count_distinct where an estimate is enough, or compute each distinct count in its own aggregation over pre-deduplicated rows (df.select(k, c).distinct().groupBy(k).count()) and join the results.",
    ));
}

/// Number of different argument sets among the distinct aggregates in the call's
/// arguments (`sql_strings`: the arguments are SQL expression strings, `selectExpr`).
fn distinct_sets(ch: &Checker, c: &ast::ExprCall, sql_strings: bool) -> usize {
    let mut keys: FxHashSet<String> = FxHashSet::default();
    let add_sql = |text: Option<String>, keys: &mut FxHashSet<String>| {
        if let Some(t) = text {
            keys.extend(SqlText::new(&t).distinct_aggregates().into_iter().flatten());
        }
    };
    for a in c.arguments.args.iter() {
        if sql_strings {
            let a = match a {
                Expr::Starred(s) => &*s.value,
                a => a,
            };
            match a {
                Expr::List(l) => {
                    for e in &l.elts {
                        add_sql(ch.model.resolve_text(e), &mut keys);
                    }
                }
                a => add_sql(ch.model.resolve_text(a), &mut keys),
            }
            continue;
        }
        walk_exprs(a, &mut |e| {
            let Expr::Call(call) = e else {
                return;
            };
            if DISTINCT_FNS
                .iter()
                .any(|f| ch.model.is_spark_function(&call.func, f))
            {
                let mut cols: Vec<String> = call
                    .arguments
                    .args
                    .iter()
                    .map(|x| distinct_arg_key(ch, x))
                    .collect();
                if cols.is_empty() {
                    return;
                }
                cols.sort();
                keys.insert(cols.join(","));
            } else if ch.model.is_spark_function(&call.func, "expr")
                && let Some(s) = call_arg(call, 0, "str")
            {
                add_sql(ch.model.resolve_text(s), &mut keys);
            }
        });
    }
    keys.len()
}

/// `"a"`, `F.col("a")`, `df.a`, `df["a"]` -> `a`; anything else -> its source text.
fn distinct_arg_key(ch: &Checker, e: &Expr) -> String {
    if let Some(n) = super::io_settings::column_name(e) {
        return n.replace('`', "").to_lowercase();
    }
    match e {
        Expr::Attribute(a) if ch.model.ty_of(&a.value).is_dataframe() => {
            a.attr.as_str().to_lowercase()
        }
        Expr::Subscript(s) if ch.model.ty_of(&s.value).is_dataframe() => str_literal(&s.slice)
            .map_or_else(|| ch.slice(e.range()).to_string(), |n| n.to_lowercase()),
        _ => ch
            .slice(e.range())
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect(),
    }
}

// ------------------------------------------------------------------ PRF027

/// `df.orderBy(F.rand())` (and `ORDER BY rand()` in `spark.sql`): a full global sort
/// (a sampling job for the range bounds, then a shuffle of every row) only to put the
/// rows in random order, usually before `limit(n)` to "take n random rows".
/// `Window.orderBy(F.rand())` (per-group sampling) and `sortWithinPartitions` are not
/// global sorts and are not reported.
pub fn order_by_rand(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["orderBy", "sort", "sql"]) else {
        return;
    };
    let (what, help) = match sc.method {
        "orderBy" | "sort" if sc.is_dataframe() => {
            let first = match c.arguments.args.first() {
                Some(Expr::List(l)) => l.elts.first(),
                Some(Expr::Tuple(t)) => t.elts.first(),
                a => a,
            };
            if !first.is_some_and(|k| is_random_key(ch, k)) {
                return;
            }
            (
                format!("{}(rand())", sc.method),
                "To pick random rows use df.sample(fraction=p) (then .limit(n) for at most n rows) or df.where(F.rand() < p); for per-group samples use sampleBy, or row_number() over Window.partitionBy(k).orderBy(F.rand()).",
            )
        }
        "sql" if sc.class().is_session() => {
            if !ch.model.sql_query(c).is_some_and(|q| q.order_by_rand()) {
                return;
            }
            (
                "SQL ORDER BY rand()".to_string(),
                "Use TABLESAMPLE (p PERCENT) or WHERE rand() < p (then LIMIT n) instead of sorting by a random key.",
            )
        }
        _ => return,
    };
    let mut msg = format!(
        "{what} sorts the whole dataset (a sampling job plus a full shuffle) just to randomise the row order."
    );
    if ch.model.in_loop(expr) {
        msg.push_str(" Inside a loop it runs on every iteration.");
    }
    ch.report(Diagnostic::new("PRF027", msg, method_range(expr)).with_help(help));
}

/// `F.rand()`, `F.randn(seed)`, `F.rand().desc()`, `F.desc(F.rand())`, `F.expr("rand()")`.
fn is_random_key(ch: &Checker, e: &Expr) -> bool {
    let Expr::Call(c) = e else {
        return false;
    };
    if let Expr::Attribute(a) = &*c.func {
        let m = a.attr.as_str();
        if (m.starts_with("asc") || m.starts_with("desc")) && c.arguments.args.is_empty() {
            return is_random_key(ch, &a.value);
        }
    }
    let model = &ch.model;
    if ["rand", "randn"]
        .iter()
        .any(|f| model.is_spark_function(&c.func, f))
    {
        return true;
    }
    let ordering = [
        "asc",
        "desc",
        "asc_nulls_first",
        "asc_nulls_last",
        "desc_nulls_first",
        "desc_nulls_last",
    ];
    if ordering.iter().any(|f| model.is_spark_function(&c.func, f)) {
        return c
            .arguments
            .args
            .first()
            .is_some_and(|a| is_random_key(ch, a));
    }
    if model.is_spark_function(&c.func, "expr") {
        return call_arg(c, 0, "str")
            .and_then(str_literal)
            .is_some_and(|s| SqlText::new(s).is_random_call());
    }
    false
}

// ------------------------------------------------------------------ PRF035

/// Methods on `GroupedData` that hand each group to Python as one pandas / Arrow frame.
const GROUP_MAP_FNS: &[&str] = &["applyInPandas", "applyInArrow", "apply", "cogroup"];

/// `df.groupBy().applyInPandas(fn, schema)` (also `groupby()`, `groupBy([])`,
/// `apply(<grouped pandas_udf>)`, `applyInArrow`, `cogroup`): an empty grouping is ONE
/// global group, so the whole frame is shuffled into a single task and loaded into one
/// pandas DataFrame. The grouping call may be chained or bound once to a name.
pub fn keyless_grouped_pandas(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, GROUP_MAP_FNS) else {
        return;
    };
    if sc.class() != Ty::GroupedData {
        return;
    }
    let Some(group) = grouping_call(ch, sc.receiver) else {
        return;
    };
    let Some(g) = ch.model.spark_call_named(group, &["groupBy", "groupby"]) else {
        return;
    };
    if !g.is_dataframe() || !no_grouping_columns(g.call) || g.bounded().is_some() {
        return;
    }
    let mut msg = format!(
        "{}() with no columns before {}(): the whole DataFrame is one group, so a single task loads every row into one pandas DataFrame (no parallelism, OOM once it outgrows one executor).",
        g.method, sc.method
    );
    if ch.model.in_loop(expr) {
        msg.push_str(" Inside a loop that repeats on every iteration.");
    }
    ch.report(Diagnostic::new("PRF035", msg, method_range(expr)).with_help(
        "Group by a real key, or use df.mapInPandas(fn, schema) to run the function once per partition in parallel.",
    ));
}

/// The expression that produced a `GroupedData` receiver: the receiver itself, or the
/// value of the single binding of a name receiver (`g = df.groupBy(); g.applyInPandas(..)`).
fn grouping_call<'a>(ch: &Checker<'a>, recv: &'a Expr) -> Option<&'a Expr> {
    match recv {
        Expr::Call(_) => Some(recv),
        Expr::Name(n) => {
            let b = ch.model.single_binding(n)?;
            ch.model.binding(b).value.filter(|v| v.is_call_expr())
        }
        _ => None,
    }
}

/// `groupBy()` / `groupBy([])` / `groupBy(())`: no grouping columns. Starred or keyword
/// arguments may carry columns, so they count as columns.
fn no_grouping_columns(c: &ast::ExprCall) -> bool {
    let args = &c.arguments;
    if !args.keywords.is_empty() {
        return false;
    }
    match args.args.as_ref() {
        [] => true,
        [Expr::List(l)] => l.elts.is_empty(),
        [Expr::Tuple(t)] => t.elts.is_empty(),
        _ => false,
    }
}

// ------------------------------------------------------------------ PRF036

/// The join strategy hint that forces a shuffle-and-replicate nested-loop join.
const NL_HINT: &str = "SHUFFLE_REPLICATE_NL";

/// `df.hint("shuffle_replicate_nl")` or `/*+ SHUFFLE_REPLICATE_NL(t) */` in `spark.sql`:
/// the hint forces a cartesian product filtered afterwards, even for equi-joins a hash
/// or sort-merge join could key on.
pub fn forced_nested_loop_join_hint(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["hint", "sql"]) else {
        return;
    };
    let what = match sc.method {
        "hint" if sc.is_dataframe() => {
            let Some(name) = call_arg(c, 0, "name").and_then(|a| ch.model.resolve_text(a)) else {
                return;
            };
            if !name.trim().eq_ignore_ascii_case(NL_HINT) {
                return;
            }
            format!("hint(\"{}\")", name.trim())
        }
        "sql" if sc.class().is_session() => {
            if !ch
                .model
                .sql_query(c)
                .is_some_and(|q| q.hints().iter().any(|h| h == NL_HINT))
            {
                return;
            }
            format!("SQL /*+ {NL_HINT} */ hint")
        }
        _ => return,
    };
    let mut msg = format!(
        "{what} forces a shuffle-and-replicate nested-loop join: every row of one side is compared with every row of the other (a cartesian product filtered afterwards), even when the join has an equality key a hash or sort-merge join could use."
    );
    if ch.model.in_loop(expr) {
        msg.push_str(" Inside a loop it runs on every iteration.");
    }
    ch.report(Diagnostic::new("PRF036", msg, method_range(expr)).with_help(
        "Drop the hint and let Spark pick the join (or use BROADCAST / MERGE / SHUFFLE_HASH for an equi-join); keep it only for a theta join of two small inputs, with a comment saying so.",
    ));
}

// ------------------------------------------------------------------ PRF022

pub fn sql_select_star(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    if !ch.model.spark_sql(expr).is_some_and(|q| q.select_star()) {
        return;
    }
    match flow::classify_current(ch) {
        Sink::Action {
            kind: ActionKind::Write | ActionKind::DriverCollect,
            projected: false,
            ..
        } => report_select_star(ch, method_range(expr)),
        Sink::Cache { .. } => report_select_star(ch, method_range(expr)),
        Sink::Derived(b) => ch.state.select_star.push((b, method_range(expr))),
        _ => {}
    }
}

fn report_select_star(ch: &mut Checker, range: TextRange) {
    ch.report(
        Diagnostic::new(
            "PRF022",
            "SELECT * in spark.sql() flows into a write/collect/cache — list the columns you need.",
            range,
        )
        .with_severity(Severity::Info),
    );
}

fn reaches_sink(ch: &Checker, b: BindingId, seen: &mut FxHashSet<BindingId>) -> bool {
    if !seen.insert(b) {
        return false;
    }
    for u in ch.state.flow.uses.get(&b).map(Vec::as_slice).unwrap_or(&[]) {
        match &u.sink {
            Sink::Action {
                kind: ActionKind::Write | ActionKind::DriverCollect,
                projected: false,
                ..
            } => return true,
            Sink::Cache { .. } => return true,
            Sink::Derived(d) => {
                // Only pass-through derivations keep `*`: a later select() prunes.
                let pruned = ch
                    .model
                    .binding(*d)
                    .value
                    .and_then(method_call)
                    .is_some_and(|(_, _, m)| {
                        matches!(m, "select" | "selectExpr" | "agg" | "groupBy")
                    });
                if !pruned && reaches_sink(ch, *d, seen) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

pub fn finish_select_star(ch: &mut Checker) {
    let cands = std::mem::take(&mut ch.state.select_star);
    for (b, range) in cands {
        let mut seen = FxHashSet::default();
        if reaches_sink(ch, b, &mut seen) {
            report_select_star(ch, range);
        }
    }
}
