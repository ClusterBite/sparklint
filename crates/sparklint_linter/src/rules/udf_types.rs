//! UDF002 udf-in-filter, UDF006 udf-missing-return-type, UDF013 udf-whole-row-struct,
//! UDF014 udf-in-join-condition.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, Expr, Operator, Stmt};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::{SqlText, Ty, call_arg, str_literal};

use super::Rule;
use super::actions::method_range;
use super::helpers::*;
use super::plan::{Conj, conjuncts};
use super::udf::is_udf_factory;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Diagnostic, Severity};

/// UDF002/006/013/014 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("UDF002")
        .init(init_registered_udfs)
        .call(udf_in_filter),
    Rule::new("UDF006")
        .stmt(udf006_stmt)
        .call(udf_missing_return_type_call),
    Rule::new("UDF013").call(udf_whole_row),
    Rule::new("UDF014")
        .init(init_registered_udfs)
        .call(udf_in_join),
];

fn udf006_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::FunctionDef(f) = stmt {
        udf_missing_return_type_decorator(ch, f);
    }
}

// ------------------------------------------------------------------ UDF002

/// Names registered with `spark.udf.register("name", ...)` in this file.
pub fn init_registered_udfs(ch: &mut Checker) {
    struct V<'c, 'a> {
        ch: &'c Checker<'a>,
        out: Vec<String>,
    }
    impl<'b> Visitor<'b> for V<'_, '_> {
        fn visit_expr(&mut self, e: &'b Expr) {
            if let Some(sc) = self.ch.model.spark_call_named(e, &["register"])
                && sc.class() == Ty::UdfRegistration
                && let Some(n) = sc.args().first().and_then(str_literal)
            {
                self.out.push(n.to_ascii_lowercase());
            }
            visitor::walk_expr(self, e);
        }
    }
    let mut v = V {
        ch,
        out: Vec::new(),
    };
    for s in &ch.module.body {
        v.visit_stmt(s);
    }
    // Assigned, not extended: UDF002 and UDF014 both run this init.
    ch.state.registered_udfs = v.out;
}

/// The first UDF registered in this file that SQL text `sql` calls.
fn sql_calls_registered(ch: &Checker, sql: &str) -> Option<String> {
    let q = SqlText::new(sql);
    ch.state
        .registered_udfs
        .iter()
        .find(|n| q.calls(n))
        .cloned()
}

pub fn udf_in_filter(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["filter", "where"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let Some(pred) = call_arg(c, 0, "condition") else {
        return;
    };
    let mut hit: Option<String> = None;
    if let Some(s) = str_literal(pred) {
        hit = sql_calls_registered(ch, s);
    } else {
        walk_exprs(pred, &mut |x| {
            if hit.is_some() {
                return;
            }
            let Expr::Call(xc) = x else { return };
            if ch.model.ty_of(&xc.func).ty == Ty::Udf {
                hit = Some(short(ch, &xc.func));
            } else if ch.model.is_spark_function(&xc.func, "expr")
                && let Some(s) = xc.arguments.args.first().and_then(str_literal)
            {
                hit = sql_calls_registered(ch, s);
            }
        });
    }
    let Some(name) = hit else { return };
    ch.report(
        Diagnostic::new(
            "UDF002",
            format!("UDF {name}() used as a filter predicate — prevents predicate pushdown/data skipping and runs Python on every row."),
            method_range(expr),
        )
        .with_help("Rewrite with built-in functions, or filter on native conjuncts first: df.filter(native).filter(udf(...))."),
    );
}

// ------------------------------------------------------------------ UDF014

/// One ANDed conjunct of a join condition, as UDF014 sees it.
enum Piece {
    /// Contains a UDF call: (UDF name, DataFrame names its arguments reference).
    Udf(String, Vec<String>),
    /// No UDF: how it constrains the join keys (PRF032's classification).
    Key(Conj),
}

/// DataFrame names referenced as `df.col` / `df["col"]` inside `e`.
fn frames_in(ch: &Checker, e: &Expr, out: &mut Vec<String>) {
    walk_exprs(e, &mut |x| {
        let base = match x {
            Expr::Attribute(a) => &*a.value,
            Expr::Subscript(s) if str_literal(&s.slice).is_some() => &*s.value,
            _ => return,
        };
        if let Expr::Name(n) = base
            && ch.model.is_dataframe(base)
            && !out.iter().any(|o| o == n.id.as_str())
        {
            out.push(n.id.to_string());
        }
    });
}

/// The first UDF called inside `e` (a Python/pandas UDF value, or SQL text calling a
/// UDF registered in this file), with the frames its arguments reference.
fn udf_inside(ch: &Checker, e: &Expr) -> Option<(String, Vec<String>)> {
    if let Some(s) = str_literal(e) {
        return sql_calls_registered(ch, s).map(|n| (n, Vec::new()));
    }
    let mut hit: Option<(String, Vec<String>)> = None;
    walk_exprs(e, &mut |x| {
        if hit.is_some() {
            return;
        }
        let Expr::Call(xc) = x else { return };
        if ch.model.ty_of(&xc.func).ty == Ty::Udf {
            let mut frames = Vec::new();
            for a in &xc.arguments.args {
                frames_in(ch, a, &mut frames);
            }
            for k in &xc.arguments.keywords {
                frames_in(ch, &k.value, &mut frames);
            }
            hit = Some((short(ch, &xc.func), frames));
        } else if ch.model.is_spark_function(&xc.func, "expr")
            && let Some(s) = xc.arguments.args.first().and_then(str_literal)
            && let Some(n) = sql_calls_registered(ch, s)
        {
            hit = Some((n, Vec::new()));
        }
    });
    hit
}

fn pieces(ch: &Checker, e: &Expr, depth: u32, out: &mut Vec<Piece>) {
    match e {
        Expr::List(ast::ExprList { elts, .. }) | Expr::Tuple(ast::ExprTuple { elts, .. }) => {
            for x in elts {
                pieces(ch, x, depth, out);
            }
        }
        Expr::BinOp(b) if b.op == Operator::BitAnd => {
            pieces(ch, &b.left, depth, out);
            pieces(ch, &b.right, depth, out);
        }
        Expr::Name(n) if ch.model.ty_of(e).is_column() => match ch
            .model
            .single_binding(n)
            .and_then(|b| ch.model.binding(b).value)
        {
            Some(v) if depth < 3 && v.range() != e.range() => pieces(ch, v, depth + 1, out),
            _ => out.push(Piece::Key(Conj::Unknown)),
        },
        other => {
            let mut ks = Vec::new();
            conjuncts(ch, other, 3, &mut ks);
            // An equality is a usable key even when one side is a UDF of one frame.
            if ks.contains(&Conj::Equi) {
                return out.push(Piece::Key(Conj::Equi));
            }
            match udf_inside(ch, other) {
                Some((name, frames)) => out.push(Piece::Udf(name, frames)),
                None => out.extend(ks.into_iter().map(Piece::Key)),
            }
        }
    }
}

fn is_small(ch: &Checker, recv: &Expr, other: Option<&Expr>) -> bool {
    ch.model.ty_of(recv).bounded().is_some()
        || other.is_some_and(|o| {
            ch.model.ty_of(o).bounded().is_some()
                || matches!(o, Expr::Call(oc) if ch.model.is_spark_function(&oc.func, "broadcast"))
        })
}

/// The UDF to report: some conjunct calls a UDF (of both frames, when `both`), and no
/// other conjunct is a usable equality or opaque.
fn udf_without_key(ps: &[Piece], both: bool) -> Option<&str> {
    if ps
        .iter()
        .any(|p| matches!(p, Piece::Key(Conj::Equi | Conj::Unknown)))
    {
        return None;
    }
    ps.iter().find_map(|p| match p {
        Piece::Udf(n, frames) if !both || frames.len() >= 2 => Some(n.as_str()),
        _ => None,
    })
}

pub fn udf_in_join(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch
        .model
        .spark_call_named(expr, &["join", "filter", "where"])
    else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let mut ps = Vec::new();
    let (small, where_) = if sc.method == "join" {
        let Some(on) = call_arg(c, 1, "on") else {
            return;
        };
        pieces(ch, on, 0, &mut ps);
        (
            is_small(ch, sc.receiver, call_arg(c, 0, "other")),
            "a join condition",
        )
    } else {
        // `a.crossJoin(b).where(udf(a.x, b.y))` / `a.join(b).filter(...)`: the filter
        // is evaluated per row pair when it reads both sides.
        let Some(jc) = ch
            .model
            .spark_call_named(sc.receiver, &["crossJoin", "join"])
        else {
            return;
        };
        if !jc.is_dataframe() || (jc.method == "join" && call_arg(jc.call, 1, "on").is_some()) {
            return;
        }
        let Some(pred) = call_arg(c, 0, "condition") else {
            return;
        };
        pieces(ch, pred, 0, &mut ps);
        (
            is_small(ch, jc.receiver, call_arg(jc.call, 0, "other")),
            "a filter over a cross join",
        )
    };
    let Some(name) = udf_without_key(&ps, sc.method != "join").map(str::to_string) else {
        return;
    };
    let mut d = Diagnostic::new(
        "UDF014",
        format!("UDF {name}() in {where_} — Spark can't derive a join key from it, so it runs a nested-loop / cartesian join and calls Python once per row PAIR."),
        method_range(expr),
    )
    .with_help("Join on a cheap equality key first (a blocking key, token or floor(x / bin)), then filter the candidate pairs with the UDF; or use built-ins (levenshtein, soundex) in the condition.");
    if small {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ UDF006

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ret {
    Str,
    NonStr,
    NoneOnly,
    Unknown,
}

fn join(a: Ret, b: Ret) -> Ret {
    match (a, b) {
        (Ret::NonStr, _) | (_, Ret::NonStr) => Ret::NonStr,
        (Ret::Unknown, _) | (_, Ret::Unknown) => Ret::Unknown,
        (Ret::Str, _) | (_, Ret::Str) => Ret::Str,
        _ => Ret::NoneOnly,
    }
}

const STR_METHODS: &[&str] = &[
    "upper",
    "lower",
    "strip",
    "lstrip",
    "rstrip",
    "title",
    "capitalize",
    "casefold",
    "format",
    "join",
    "replace",
    "zfill",
    "ljust",
    "rjust",
    "center",
    "isoformat",
    "strftime",
    "hexdigest",
    "decode",
    "swapcase",
];

fn classify(ch: &Checker, e: &Expr) -> Ret {
    match e {
        Expr::StringLiteral(_) | Expr::FString(_) => Ret::Str,
        Expr::NoneLiteral(_) => Ret::NoneOnly,
        Expr::NumberLiteral(_) | Expr::BooleanLiteral(_) => Ret::NonStr,
        Expr::List(_)
        | Expr::Tuple(_)
        | Expr::Dict(_)
        | Expr::Set(_)
        | Expr::ListComp(_)
        | Expr::DictComp(_)
        | Expr::SetComp(_) => Ret::NonStr,
        Expr::Compare(_) => Ret::NonStr,
        Expr::UnaryOp(u) => match u.op {
            ast::UnaryOp::Not => Ret::NonStr,
            _ => {
                if matches!(&*u.operand, Expr::NumberLiteral(_)) {
                    Ret::NonStr
                } else {
                    Ret::Unknown
                }
            }
        },
        Expr::BinOp(b) => {
            let (l, r) = (classify(ch, &b.left), classify(ch, &b.right));
            if l == Ret::Str || r == Ret::Str {
                return Ret::Str;
            }
            match b.op {
                Operator::Add => {
                    if l == Ret::NonStr || r == Ret::NonStr {
                        Ret::NonStr
                    } else {
                        Ret::Unknown
                    }
                }
                Operator::Mod => {
                    if l == Ret::NonStr || r == Ret::NonStr {
                        Ret::NonStr
                    } else {
                        Ret::Unknown
                    }
                }
                Operator::Mult => {
                    // `s * 2` (string repetition) is rare in UDFs; `x * 2` is not.
                    if l == Ret::NonStr || r == Ret::NonStr {
                        Ret::NonStr
                    } else {
                        Ret::Unknown
                    }
                }
                Operator::Sub
                | Operator::Div
                | Operator::FloorDiv
                | Operator::Pow
                | Operator::MatMult => Ret::NonStr,
                _ => Ret::Unknown,
            }
        }
        Expr::If(i) => join(classify(ch, &i.body), classify(ch, &i.orelse)),
        Expr::Call(c) => {
            if let Some(q) = ch.model.qualified_name(&c.func) {
                match q.as_str() {
                    "builtins.str" | "builtins.repr" | "builtins.format" | "json.dumps" => {
                        return Ret::Str;
                    }
                    "builtins.int" | "builtins.float" | "builtins.bool" | "builtins.len"
                    | "builtins.round" | "builtins.abs" | "builtins.sum" | "builtins.list"
                    | "builtins.dict" | "builtins.set" | "builtins.tuple" | "builtins.sorted"
                    | "json.loads" => return Ret::NonStr,
                    q if q.starts_with("math.") => return Ret::NonStr,
                    _ => {}
                }
            }
            if let Some((_, _, m)) = method_call(e) {
                if STR_METHODS.contains(&m) {
                    return Ret::Str;
                }
                if matches!(
                    m,
                    "split" | "splitlines" | "count" | "find" | "index" | "items"
                ) {
                    return Ret::NonStr;
                }
            }
            Ret::Unknown
        }
        _ => Ret::Unknown,
    }
}

fn annotation_kind(ch: &Checker, ann: &Expr) -> Ret {
    let q = ch.model.qualified_name(ann);
    match q.as_deref() {
        Some("builtins.str") => Ret::Str,
        Some(
            "builtins.int" | "builtins.float" | "builtins.bool" | "builtins.list" | "builtins.dict"
            | "builtins.tuple" | "builtins.set" | "typing.List" | "typing.Dict",
        ) => Ret::NonStr,
        _ => match ann {
            Expr::StringLiteral(s) => match s.value.to_str() {
                "str" => Ret::Str,
                "int" | "float" | "bool" => Ret::NonStr,
                _ => Ret::Unknown,
            },
            Expr::Subscript(s) => annotation_kind(ch, &s.value),
            _ => Ret::Unknown,
        },
    }
}

fn def_returns(ch: &Checker, f: &ast::StmtFunctionDef) -> Ret {
    if let Some(r) = &f.returns {
        let k = annotation_kind(ch, r);
        if k != Ret::Unknown {
            return k;
        }
    }
    struct V<'c, 'a> {
        ch: &'c Checker<'a>,
        acc: Option<Ret>,
    }
    impl<'b> Visitor<'b> for V<'_, '_> {
        fn visit_stmt(&mut self, s: &'b Stmt) {
            match s {
                Stmt::FunctionDef(_) | Stmt::ClassDef(_) => {}
                Stmt::Return(r) => {
                    let k = r
                        .value
                        .as_deref()
                        .map_or(Ret::NoneOnly, |v| classify(self.ch, v));
                    self.acc = Some(self.acc.map_or(k, |a| join(a, k)));
                }
                _ => visitor::walk_stmt(self, s),
            }
        }
        fn visit_expr(&mut self, _: &'b Expr) {}
    }
    let mut v = V { ch, acc: None };
    for s in &f.body {
        v.visit_stmt(s);
    }
    v.acc.unwrap_or(Ret::NoneOnly)
}

fn target_kind(ch: &Checker, target: &Expr) -> Option<Ret> {
    match target {
        Expr::Lambda(l) => Some(classify(ch, &l.body)),
        Expr::Name(n) => {
            let b = ch.model.single_binding(n)?;
            let b = ch.model.binding(b);
            if b.ty.ty == Ty::Udf {
                return None; // already a UDF (has its own type)
            }
            let def = ch.fn_defs.get(&b.range.start()).copied()?;
            Some(def_returns(ch, def))
        }
        _ => None,
    }
}

fn is_return_type_arg(e: &Expr) -> bool {
    // `udf("int")`, `udf(IntegerType())`, `udf(T.LongType())`, `udf(returnType=...)`
    !matches!(e, Expr::Lambda(_) | Expr::Name(_))
        || matches!(e, Expr::Name(n) if n.id.as_str().ends_with("Type") || n.id.as_str().chars().all(|c| c.is_ascii_uppercase() || c == '_'))
}

fn report(ch: &mut Checker, kind: Ret, range: TextRange) {
    let (sev, msg) = match kind {
        Ret::NonStr => (
            Severity::Warning,
            "udf() without returnType defaults to StringType — the function returns a non-string, so results become strings (or NULL).",
        ),
        Ret::Unknown => (
            Severity::Info,
            "udf() without returnType defaults to StringType — state the return type explicitly.",
        ),
        _ => return,
    };
    ch.report(
        Diagnostic::new("UDF006", msg, range)
            .with_severity(sev)
            .with_help("Pass returnType: F.udf(fn, \"long\") / @F.udf(\"double\") / spark.udf.register(\"n\", fn, \"int\")."),
    );
}

/// `F.udf(fn)`, `udf(lambda ...)`, `@udf()`-style factory calls and
/// `spark.udf.register("n", fn)` without a return type.
pub fn udf_missing_return_type_call(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if is_udf_factory(ch, &c.func) {
        if c.arguments
            .keywords
            .iter()
            .any(|k| k.arg.as_ref().is_none_or(|a| a.as_str() == "returnType"))
        {
            return;
        }
        let first = c.arguments.args.first();
        match first {
            Some(f) if !is_return_type_arg(f) => {
                if c.arguments.args.len() >= 2 {
                    return;
                }
                if let Some(k) = target_kind(ch, f) {
                    report(ch, k, expr.range());
                }
            }
            Some(_) => {} // `udf("int")` decorator factory
            None => {
                // `@udf()` / `@udf(useArrow=True)` decorating a def
                if let Some(Node::Stmt(Stmt::FunctionDef(fd))) = ch.parent()
                    && let Some(dec) = fd
                        .decorator_list
                        .iter()
                        .find(|d| d.expression.range() == expr.range())
                {
                    let k = def_returns(ch, fd);
                    report(
                        ch,
                        k,
                        TextRange::new(dec.range.start(), fd.name.range.end()),
                    );
                }
            }
        }
        return;
    }
    if let Some(sc) = ch.model.spark_call_named(expr, &["register"])
        && sc.class() == Ty::UdfRegistration
        && c.arguments.args.len() == 2
        && call_arg(c, 2, "returnType").is_none()
        && !c.arguments.keywords.iter().any(|k| k.arg.is_none())
        && let Some(f) = c.arguments.args.get(1)
        && let Some(k) = target_kind(ch, f)
    {
        report(ch, k, method_range(expr));
    }
}

/// Bare `@udf` on a def.
pub fn udf_missing_return_type_decorator(ch: &mut Checker, f: &ast::StmtFunctionDef) {
    let Some(dec) = f
        .decorator_list
        .iter()
        .find(|d| !matches!(d.expression, Expr::Call(_)) && is_udf_factory(ch, &d.expression))
    else {
        return;
    };
    let k = def_returns(ch, f);
    report(ch, k, TextRange::new(dec.range.start(), f.name.range.end()));
}

// ------------------------------------------------------------------ UDF013

/// `<frame>.columns` on a DataFrame.
fn frame_columns(ch: &Checker, e: &Expr) -> bool {
    matches!(e, Expr::Attribute(a) if a.attr.as_str() == "columns" && ch.model.is_dataframe(&a.value))
}

/// `<frame>.columns`, or an unfiltered comprehension over it (`[F.col(c) for c in df.columns]`).
fn all_columns(ch: &Checker, e: &Expr) -> bool {
    let gens = match e {
        Expr::ListComp(lc) => &lc.generators,
        Expr::Generator(g) => &g.generators,
        _ => return frame_columns(ch, e),
    };
    gens.len() == 1 && gens[0].ifs.is_empty() && frame_columns(ch, &gens[0].iter)
}

/// `"*"` / `F.col("*")` / `df["*"]`.
fn star_column(ch: &Checker, e: &Expr) -> bool {
    match e {
        Expr::Call(c)
            if ch.model.is_spark_function(&c.func, "col")
                || ch.model.is_spark_function(&c.func, "column") =>
        {
            c.arguments.args.len() == 1 && str_literal(&c.arguments.args[0]) == Some("*")
        }
        Expr::Subscript(s) => str_literal(&s.slice) == Some("*") && ch.model.is_dataframe(&s.value),
        _ => str_literal(e) == Some("*"),
    }
}

/// How a UDF argument passes the whole row, if it does.
fn whole_row_arg(ch: &Checker, e: &Expr) -> Option<&'static str> {
    match e {
        Expr::Starred(st) if all_columns(ch, &st.value) => Some("every column as an argument"),
        Expr::Call(c)
            if ch.model.is_spark_function(&c.func, "struct") && c.arguments.keywords.is_empty() =>
        {
            let args = &c.arguments.args;
            let whole = args.iter().any(|a| match a {
                Expr::Starred(st) => all_columns(ch, &st.value),
                _ => star_column(ch, a) || (args.len() == 1 && all_columns(ch, a)),
            });
            whole.then_some("the whole row as a struct")
        }
        _ => None,
    }
}

/// A name bound (on any reaching path) to `pandas_udf(f, schema, functionType=...)` whose
/// function type is not SCALAR / SCALAR_ITER: grouped-map and grouped-agg UDFs take the whole
/// frame by design.
fn non_scalar_pandas_udf(ch: &Checker, func: &Expr) -> bool {
    let Expr::Name(n) = func else { return false };
    ch.model.reaching(n).iter().any(|&b| {
        let Some(Expr::Call(v)) = ch.model.binding(b).value else {
            return false;
        };
        call_arg(v, 2, "functionType").is_some_and(|ft| {
            !matches!(ft, Expr::Attribute(a) if matches!(a.attr.as_str(), "SCALAR" | "SCALAR_ITER"))
        })
    })
}

/// A Python/pandas UDF called with `struct("*")`, `struct(*df.columns)` or `*df.columns`.
pub fn udf_whole_row(ch: &mut Checker, c: &ast::ExprCall, _expr: &Expr) {
    if ch.model.ty_of(&c.func).ty != Ty::Udf || non_scalar_pandas_udf(ch, &c.func) {
        return;
    }
    let Some(how) = c.arguments.args.iter().find_map(|a| whole_row_arg(ch, a)) else {
        return;
    };
    let name = match &*c.func {
        Expr::Call(_) => "UDF".to_string(),
        f => format!("UDF {}", short(ch, f)),
    };
    ch.report(
        Diagnostic::new(
            "UDF013",
            format!("{name}() gets {how} — all columns are shipped to Python and back, and Spark can't prune the unread ones from the scan."),
            c.range(),
        )
        .with_help("Pass only the columns the function reads: f(F.col(\"a\"), F.col(\"b\")). For whole-row hashes or JSON use built-ins: xxhash64(*cols), sha2(concat_ws(...), 256), to_json(struct(\"*\")).")
    );
}
