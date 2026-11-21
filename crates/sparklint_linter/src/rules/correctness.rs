//! COR — correctness: COR001 column-eq-none, COR002 python-bool-op-on-column.

use ruff_python_ast::token::TokenKind;
use ruff_python_ast::{self as ast, BoolOp, CmpOp, Expr, UnaryOp};
use ruff_text_size::{Ranged, TextRange, TextSize};

use super::Rule;
use super::helpers::*;
use super::msg::Msg;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Edit, Fix};
use crate::msg;

/// COR001/002 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("COR001").expr(cor001_expr).msgs(&[
        COR001_EQ,
        COR001_NE,
        COR001_IS,
        COR001_IS_NOT,
        COR001_SQL_EQ,
        COR001_SQL_NE,
    ]),
    Rule::new("COR002")
        .expr(cor002_expr)
        .msgs(&[COR002_BOOL_OP, COR002_BOOL_TEST, COR002_NOT]),
];

const COR001_EQ: Msg = Msg::new(
    "COR001",
    "eq-none",
    "Column == None is always NULL (a filter drops every row) — use .isNull().",
);
const COR001_NE: Msg = Msg::new(
    "COR001",
    "ne-none",
    "Column != None is always NULL (a filter drops every row) — use .isNotNull().",
);
const COR001_IS: Msg = Msg::new(
    "COR001",
    "is-none",
    "`Column is None` is always False in Python — use .isNull().",
);
const COR001_IS_NOT: Msg = Msg::new(
    "COR001",
    "is-not-none",
    "`Column is not None` is always True in Python — use .isNotNull().",
);
const COR001_SQL_EQ: Msg = Msg::new(
    "COR001",
    "sql-eq-null",
    "`{op} NULL` in SQL text is always NULL (a filter drops every row) — use IS NULL.",
);
const COR001_SQL_NE: Msg = Msg::new(
    "COR001",
    "sql-ne-null",
    "`{op} NULL` in SQL text is always NULL (a filter drops every row) — use IS NOT NULL.",
);
const COR002_BOOL_OP: Msg = Msg::new(
    "COR002",
    "and-or",
    "'{keyword}' on Columns — use {operator} (with parentheses); Python cannot convert a Column to bool.",
)
.help("Combine Column conditions with & (and), | (or) and ~ (not), each comparison in parentheses.");
const COR002_BOOL_TEST: Msg = Msg::new(
    "COR002",
    "bool-test",
    "This test gets a Column from '{keyword}' (its last operand) — Python cannot convert a Column to bool.",
)
.help("Test Python values here; build Column conditions with & / | and pass them to filter() or when().");
const COR002_NOT: Msg = Msg::new(
    "COR002",
    "not",
    "'not' on a Column — use ~ (with parentheses); Python cannot convert a Column to bool.",
)
.help(
    "Combine Column conditions with & (and), | (or) and ~ (not), each comparison in parentheses.",
);

fn cor001_expr(ch: &mut Checker, expr: &Expr) {
    match expr {
        Expr::Compare(c) => column_eq_none(ch, c, expr),
        Expr::Call(_) => sql_eq_null(ch, expr),
        _ => {}
    }
}

/// `= NULL` / `!= NULL` / `<> NULL` comparisons in SQL text: `spark.sql(..)`,
/// `df.filter("..")` / `where`, `F.expr(..)`, `selectExpr(..)`. One finding per
/// variant and site (no fix: the text may be built from several pieces).
fn sql_eq_null(ch: &mut Checker, expr: &Expr) {
    let mut found: Vec<(ruff_text_size::TextRange, bool)> = Vec::new();
    for site in ch.model.sql_sites(expr) {
        let Some(text) = &site.text else { continue };
        for n in text.null_comparisons() {
            if found.contains(&(site.range, n.negated)) {
                continue;
            }
            found.push((site.range, n.negated));
            let m = if n.negated {
                &COR001_SQL_NE
            } else {
                &COR001_SQL_EQ
            };
            ch.report(msg!(m, site.range, op = n.op));
        }
    }
}

fn cor002_expr(ch: &mut Checker, expr: &Expr) {
    match expr {
        Expr::BoolOp(b) => bool_op_on_column(ch, b, expr),
        Expr::UnaryOp(u) => not_on_column(ch, u, expr),
        _ => {}
    }
}

pub fn column_eq_none(ch: &mut Checker, c: &ast::ExprCompare, expr: &Expr) {
    if c.ops.len() != 1 || c.operands.len() != 2 {
        return;
    }
    let op = c.ops[0];
    if !matches!(op, CmpOp::Eq | CmpOp::NotEq | CmpOp::Is | CmpOp::IsNot) {
        return;
    }
    let (l, r) = (&c.operands[0], &c.operands[1]);
    let is_none = |e: &Expr| e.is_none_literal_expr() || is_lit_none(ch, e);
    let col = if is_none(r) && certain_column(ch, l) {
        l
    } else if is_none(l) && certain_column(ch, r) {
        r
    } else {
        return;
    };
    // `x is None` on a *variable* is the normal Optional[Column] check (a parameter
    // annotated `Column | None`, a helper returning a Column or None): only flag
    // identity tests on expressions that are always a Column (`F.col("a") is None`).
    if matches!(op, CmpOp::Is | CmpOp::IsNot) && !is_always_column_expr(ch, col) {
        return;
    }
    let null = matches!(op, CmpOp::Eq | CmpOp::Is);
    let method = if null { "isNull" } else { "isNotNull" };
    let replacement = format!("{}.{method}()", paren_src(ch, col));
    let (m, fix) = match op {
        CmpOp::Eq | CmpOp::NotEq => (
            if null { &COR001_EQ } else { &COR001_NE },
            Fix::safe(
                format!("Replace with `.{method}()`"),
                vec![Edit::replace(expr.range(), replacement)],
            ),
        ),
        _ => (
            if null { &COR001_IS } else { &COR001_IS_NOT },
            Fix::unsafe_(
                format!("Replace with `.{method}()`"),
                vec![Edit::replace(expr.range(), replacement)],
            ),
        ),
    };
    ch.report(msg!(m, expr.range()).with_fix(fix));
}

/// `a and b` / `a or b` where Python converts a Column to bool (which raises):
/// a Column in any position but the last. A chain whose only Column is its last
/// operand (`msg or default_col`, `opt and F.col("x") == opt`) is the ordinary
/// short-circuit idiom and runs fine; it is reported only when the chain itself is
/// a boolean test (`if x is not None and c == 1:`), without a fix.
pub fn bool_op_on_column(ch: &mut Checker, b: &ast::ExprBoolOp, expr: &Expr) {
    let Some((last, init)) = b.values.split_last() else {
        return;
    };
    let (kw, sym) = match b.op {
        BoolOp::And => ("and", "&"),
        BoolOp::Or => ("or", "|"),
    };
    if !init.iter().any(|v| may_be_column(ch, v)) {
        if may_be_column(ch, last) && in_bool_test(ch, expr) {
            ch.report(msg!(COR002_BOOL_TEST, expr.range(), keyword = kw));
        }
        return;
    }
    let ft = ch.fix_text();
    let kind = match b.op {
        BoolOp::And => TokenKind::And,
        BoolOp::Or => TokenKind::Or,
    };
    let mut edits = Vec::new();
    let mut prev: Option<TextSize> = None;
    for v in &b.values {
        if let Some(end) = prev {
            let Some(e) = ft.replace_operator(TextRange::new(end, v.start()), kind, sym) else {
                return;
            };
            edits.push(e);
        }
        edits.extend(ft.wrap(v, b));
        prev = Some(v.end());
    }
    let title = format!("Replace `{kw}` with `{sym}`");
    // A Python value next to Columns becomes a literal under `&` / `|` (a string is
    // not read as SQL): only a chain of nothing but Columns keeps its meaning.
    let fix = if b.values.iter().all(|v| all_columns(ch, v)) {
        Fix::safe(title, edits)
    } else {
        Fix::unsafe_(title, edits)
    };
    ch.report(msg!(COR002_BOOL_OP, expr.range(), keyword = kw, operator = sym).with_fix(fix));
}

/// A Column, or an `and` / `or` chain of nothing but Columns.
fn all_columns(ch: &Checker, e: &Expr) -> bool {
    match e {
        Expr::BoolOp(b) => b.values.iter().all(|v| all_columns(ch, v)),
        _ => certain_column(ch, e),
    }
}

/// A Column, or an `and` / `or` chain that can evaluate to one.
fn may_be_column(ch: &Checker, e: &Expr) -> bool {
    match e {
        Expr::BoolOp(b) => b.values.iter().any(|v| may_be_column(ch, v)),
        _ => certain_column(ch, e),
    }
}

/// Whether Python converts the value of the current chain `expr` to bool (a test
/// or a `bool()` conversion, see [`Checker::truth_use`]). A chain directly inside
/// another `and` / `or` is the enclosing chain's finding.
fn in_bool_test(ch: &Checker, expr: &Expr) -> bool {
    !matches!(ch.parent(), Some(Node::Expr(Expr::BoolOp(_)))) && ch.truth_use(expr).is_some()
}

pub fn not_on_column(ch: &mut Checker, u: &ast::ExprUnaryOp, expr: &Expr) {
    // `not (a and b)`: the chain's own finding says what converts to bool.
    if u.op != UnaryOp::Not || u.operand.is_bool_op_expr() || !certain_column(ch, &u.operand) {
        return;
    }
    let text = format!("~{}", paren_src(ch, &u.operand));
    let fix = Fix::safe(
        "Replace `not` with `~`",
        vec![Edit::replace(expr.range(), text)],
    );
    ch.report(msg!(COR002_NOT, expr.range()).with_fix(fix));
}

/// Expressions that can never evaluate to Python `None` when they are Columns
/// (Spark calls, `df.x`, `df["x"]`, operators) as opposed to variables. A call
/// to a user or library helper is not one: its `-> Column` / `-> Column | None`
/// annotation is a hint, and `helper(..) is None` is the normal Optional check.
fn is_always_column_expr(ch: &Checker, e: &Expr) -> bool {
    match e {
        Expr::Name(_) => false,
        Expr::Call(c) => spark_api_call(ch, c),
        Expr::Attribute(a) => {
            !matches!(&*a.value, Expr::Name(n) if n.id.as_str() == "self" || n.id.as_str() == "cls")
        }
        _ => true,
    }
}

/// A call into pyspark itself (`F.col(..)`, `F.lit(..)`) or a method on a Spark
/// receiver (`df.col(..)`, `F.col("a").alias(..)`).
fn spark_api_call(ch: &Checker, c: &ast::ExprCall) -> bool {
    if ch
        .model
        .qualified_name(&c.func)
        .is_some_and(|q| q.starts_with("pyspark."))
    {
        return true;
    }
    matches!(&*c.func, Expr::Attribute(a) if ch.model.ty_of(&a.value).ty.is_spark())
}

/// Correctness rules are errors: only fire on operands that are Columns with
/// `Certain` confidence (not on heuristically-typed values such as `x.empty` on a
/// name that merely looks like a DataFrame).
fn certain_column(ch: &Checker, e: &Expr) -> bool {
    let t = ch.model.ty_of(e);
    t.is_column() && t.is_certain()
}
