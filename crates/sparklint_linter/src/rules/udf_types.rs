//! UDF002 udf-in-filter, UDF006 udf-missing-return-type, UDF013 udf-whole-row-struct,
//! UDF014 udf-in-join-condition.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, Expr, Operator, Stmt};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::columns;
use sparklint_semantic::udf_refs::{UdfCall, UdfRef, UdfRefs};
use sparklint_semantic::{Hops, SqlSiteKind, SqlText, Ty, call_arg, str_literal};

use super::Rule;
use super::describe::short;
use super::helpers::*;
use super::msg::Msg;
use super::plan_facts::{Conj, conjuncts, small_side};
use super::udf_target::{Factory, Shape, UdfFn, UdfTarget, decorated_udf, udf_target};
use crate::checker::{Checker, Node};
use crate::diagnostic::Severity;
use crate::msg;

/// UDF002/006/013/014 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("UDF002").call(udf_in_filter).msgs(&[UDF002]),
    Rule::new("UDF006")
        .stmt(udf006_stmt)
        .call(udf_missing_return_type_call)
        .msgs(&[UDF006_NON_STRING, UDF006_UNKNOWN]),
    Rule::new("UDF013").call(udf_whole_row).msgs(&[UDF013]),
    Rule::new("UDF014").call(udf_in_join).msgs(&[UDF014]),
];

const UDF002: Msg = Msg::new(
    "UDF002",
    "filter-predicate",
    "UDF {name}() used as a filter predicate — Spark cannot push it into the scan (no file or row-group skipping on it) and runs Python on every remaining row.",
)
.help("Rewrite the predicate with built-in functions. Splitting it does not help: Spark already applies the native conditions of a filter before the UDF.");
const UDF014: Msg = Msg::new(
    "UDF014",
    "join-condition",
    "UDF {name}() in {place} — Spark can't derive a join key from it, so it runs a nested-loop / cartesian join and calls Python once per row PAIR.",
)
.help("Join on a cheap equality key first (a blocking key, token or floor(x / bin)), then filter the candidate pairs with the UDF; or use built-ins (levenshtein, soundex) in the condition.");
const UDF006_HELP: &str = "Pass returnType: F.udf(fn, \"long\") / @F.udf(\"double\") / spark.udf.register(\"n\", fn, \"int\").";
const UDF006_NON_STRING: Msg = Msg::new(
    "UDF006",
    "non-string-return",
    "udf() without returnType defaults to StringType — the function returns a non-string, so results become strings (or NULL).",
)
.help(UDF006_HELP);
const UDF006_UNKNOWN: Msg = Msg::new(
    "UDF006",
    "unknown-return",
    "udf() without returnType defaults to StringType — state the return type explicitly.",
)
.help(UDF006_HELP);
const UDF013: Msg = Msg::new(
    "UDF013",
    "whole-row",
    "{udf}() gets {how} — all columns are shipped to Python and back, and Spark can't prune the unread ones from the scan.",
)
.help("Pass only the columns the function reads: f(F.col(\"a\"), F.col(\"b\")). For whole-row hashes or JSON use built-ins: xxhash64(*cols), sha2(concat_ws(...), 256), to_json(struct(\"*\")).");

fn udf006_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::FunctionDef(f) = stmt {
        udf_missing_return_type_decorator(ch, f);
    }
}

// ------------------------------------------------------------------ UDF002

/// How a UDF reference is named in a message: the called UDF as written, or the name
/// SQL text calls it by.
fn udf_name(ch: &Checker, r: &UdfRef) -> String {
    match &r.call {
        UdfCall::Python { call, .. } => short(ch, &call.func),
        UdfCall::Registered { name } => name.clone(),
    }
}

pub fn udf_in_filter(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let sites = ch.model.sql_sites(expr);
    let Some(site) = sites.iter().find(|s| s.kind == SqlSiteKind::Condition) else {
        return;
    };
    let Some(pred) = site.arg else {
        return;
    };
    let refs = match &site.text {
        Some(q) => UdfRefs::of_sql(ch.model, q),
        None => UdfRefs::of(ch.model, pred),
    };
    let Some(name) = refs.first().map(|r| udf_name(ch, r)) else {
        return;
    };
    ch.report(msg!(UDF002, method_range(expr), name));
}

// ------------------------------------------------------------------ UDF014

/// One ANDed conjunct of a join condition, as UDF014 sees it.
enum Piece {
    /// Contains a UDF call: (UDF name, DataFrame names its arguments reference).
    Udf(String, Vec<String>),
    /// No UDF: how it constrains the join keys (PRF032's classification).
    Key(Conj),
}

/// The first UDF called inside `e` (a Python/pandas UDF value, or SQL text calling a
/// UDF registered in this file), with the frames its arguments reference.
fn udf_inside(ch: &Checker, e: &Expr) -> Option<(String, Vec<String>)> {
    let refs = match str_literal(e) {
        Some(s) => UdfRefs::of_sql(ch.model, &SqlText::new(s)),
        None => UdfRefs::of(ch.model, e),
    };
    let r = refs.first()?;
    let frames = match &r.call {
        UdfCall::Python { frames, .. } => frames.clone(),
        UdfCall::Registered { .. } => Vec::new(),
    };
    Some((udf_name(ch, r), frames))
}

fn pieces(ch: &Checker, e: &Expr, hops: Hops, out: &mut Vec<Piece>) {
    match e {
        Expr::List(ast::ExprList { elts, .. }) | Expr::Tuple(ast::ExprTuple { elts, .. }) => {
            for x in elts {
                pieces(ch, x, hops, out);
            }
        }
        Expr::BinOp(b) if b.op == Operator::BitAnd => {
            pieces(ch, &b.left, hops, out);
            pieces(ch, &b.right, hops, out);
        }
        Expr::Name(_) if ch.model.ty_of(e).is_column() => match ch.model.hop_one(e, hops) {
            Some((v, h)) => pieces(ch, v, h, out),
            None => out.push(Piece::Key(Conj::Unknown)),
        },
        other => {
            let mut ks = Vec::new();
            conjuncts(ch, other, Hops::DEFAULT, &mut ks);
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
        let Some(on) = sc.arg(1, "on") else {
            return;
        };
        pieces(ch, on, Hops::DEFAULT, &mut ps);
        (small_side(ch, c, sc.receiver_ty), "a join condition")
    } else {
        // `a.crossJoin(b).where(udf(a.x, b.y))` / `a.join(b).filter(...)`: the filter
        // is evaluated per row pair when it reads both sides.
        let Some(jc) = ch
            .model
            .spark_call_named(sc.receiver, &["crossJoin", "join"])
        else {
            return;
        };
        if !jc.is_dataframe() || (jc.method == "join" && jc.arg(1, "on").is_some()) {
            return;
        }
        let Some(pred) = sc.arg(0, "condition") else {
            return;
        };
        pieces(ch, pred, Hops::DEFAULT, &mut ps);
        (
            small_side(ch, jc.call, jc.receiver_ty),
            "a filter over a cross join",
        )
    };
    let Some(name) = udf_without_key(&ps, sc.method != "join").map(str::to_string) else {
        return;
    };
    let mut d = msg!(UDF014, method_range(expr), name, place = where_);
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
    // `Optional[X]`, `Union[X, None]` and `X | None`: the non-None members decide.
    match ann {
        Expr::NoneLiteral(_) => return Ret::NoneOnly,
        Expr::BinOp(b) if b.op == Operator::BitOr => {
            return join(annotation_kind(ch, &b.left), annotation_kind(ch, &b.right));
        }
        Expr::Subscript(s) => match ch.model.qualified_name(&s.value).as_deref() {
            Some("typing.Optional") => return join(Ret::NoneOnly, annotation_kind(ch, &s.slice)),
            Some("typing.Union") => {
                return match &*s.slice {
                    Expr::Tuple(t) => t
                        .elts
                        .iter()
                        .map(|e| annotation_kind(ch, e))
                        .fold(Ret::NoneOnly, join),
                    one => annotation_kind(ch, one),
                };
            }
            _ => {}
        },
        _ => {}
    }
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

/// What the wrapped function returns, joined over every binding of a name argument
/// (`None` unless every binding is a plain lambda or def).
fn target_kind(ch: &Checker, t: &UdfTarget) -> Option<Ret> {
    if !t.resolved.complete() {
        return None;
    }
    let mut acc: Option<Ret> = None;
    for w in &t.resolved.fns {
        if w.binding
            .is_some_and(|b| ch.model.binding(b).ty.ty == Ty::Udf)
        {
            return None; // already a UDF (has its own type)
        }
        let k = match w.func {
            // `udf(lambda x: ..)`, `f = lambda x: x * 2; udf(f)`
            UdfFn::Lambda(l) => classify(ch, &l.body),
            UdfFn::Def(def) => def_returns(ch, def),
        };
        acc = Some(acc.map_or(k, |a| join(a, k)));
    }
    acc
}

fn is_return_type_arg(e: &Expr) -> bool {
    // `udf("int")`, `udf(IntegerType())`, `udf(T.LongType())`, `udf(returnType=...)`
    !matches!(e, Expr::Lambda(_) | Expr::Name(_))
        || matches!(e, Expr::Name(n) if n.id.as_str().ends_with("Type") || n.id.as_str().chars().all(|c| c.is_ascii_uppercase() || c == '_'))
}

fn report(ch: &mut Checker, kind: Ret, range: TextRange) {
    let (sev, m) = match kind {
        Ret::NonStr => (Severity::Warning, &UDF006_NON_STRING),
        Ret::Unknown => (Severity::Info, &UDF006_UNKNOWN),
        _ => return,
    };
    ch.report(msg!(m, range).with_severity(sev));
}

/// `F.udf(fn)`, `udf(lambda ...)`, `@udf()`-style factory calls and
/// `spark.udf.register("n", fn)` without a return type.
pub fn udf_missing_return_type_call(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(t) = udf_target(ch, expr) else {
        return;
    };
    if t.factory != Factory::Udf {
        return;
    }
    if t.shape == Shape::Factory {
        if c.arguments
            .keywords
            .iter()
            .any(|k| k.arg.as_ref().is_none_or(|a| a.as_str() == "returnType"))
        {
            return;
        }
        // The function positionally or as `f=` (`F.udf(f=lambda x: ...)`).
        match t.arg {
            Some(f) if !is_return_type_arg(f) => {
                if c.arguments.args.len() >= 2 {
                    return;
                }
                if let Some(k) = target_kind(ch, &t) {
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
    // `register("n", f)` / `register("n", f=f)` without a return type
    let positional = if t.keyword { 1 } else { 2 };
    if t.shape == Shape::Register
        && c.arguments.args.len() == positional
        && call_arg(c, 2, "returnType").is_none()
        && !c.arguments.keywords.iter().any(|k| k.arg.is_none())
        && let Some(k) = target_kind(ch, &t)
    {
        report(ch, k, method_range(expr));
    }
}

/// Bare `@udf` on a def.
pub fn udf_missing_return_type_decorator(ch: &mut Checker, f: &ast::StmtFunctionDef) {
    let Some(dec) = decorated_udf(ch, f, Factory::Udf)
        .and_then(|t| t.decorator)
        .filter(|d| !matches!(d.expression, Expr::Call(_)))
    else {
        return;
    };
    let k = def_returns(ch, f);
    report(ch, k, TextRange::new(dec.range.start(), f.name.range.end()));
}

// ------------------------------------------------------------------ UDF013

/// `<frame>.columns`, or an unfiltered comprehension over it (`[F.col(c) for c in df.columns]`).
fn all_columns(ch: &Checker, e: &Expr) -> bool {
    columns::all_columns(ch.model, e)
}

/// `"*"` / `F.col("*")` / `df["*"]`.
fn star_column(ch: &Checker, e: &Expr) -> bool {
    columns::column_name(ch.model, e).as_deref() == Some("*")
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
        // `row = F.struct("*"); f(row)`: a name bound once, one hop.
        Expr::Name(_) => {
            let (v, _) = ch.model.hop_one(e, Hops::DEFAULT)?;
            if matches!(v, Expr::Name(_)) {
                return None;
            }
            whole_row_arg(ch, v)
        }
        _ => None,
    }
}

/// A name bound (on any reaching path) to `pandas_udf(f, schema, functionType=...)` whose
/// function type is not SCALAR / SCALAR_ITER: grouped-map and grouped-agg UDFs take the whole
/// frame by design.
fn non_scalar_pandas_udf(ch: &Checker, func: &Expr) -> bool {
    let Some((reached, _)) = ch.model.hop(func, Hops::DEFAULT) else {
        return false;
    };
    reached.iter().any(|r| {
        let Some(Expr::Call(v)) = r.value else {
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
    let Some(how) = c
        .arguments
        .args
        .iter()
        .chain(c.arguments.keywords.iter().map(|k| &k.value))
        .find_map(|a| whole_row_arg(ch, a))
    else {
        return;
    };
    let udf = match &*c.func {
        Expr::Call(_) => "UDF".to_string(),
        f => format!("UDF {}", short(ch, f)),
    };
    ch.report(msg!(UDF013, c.range(), udf, how));
}
