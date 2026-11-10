//! COR — correctness: COR001 column-eq-none, COR002 python-bool-op-on-column.

use ruff_python_ast::{self as ast, BoolOp, CmpOp, Expr, UnaryOp};
use ruff_text_size::Ranged;

use super::Rule;
use super::helpers::*;
use crate::checker::Checker;
use crate::diagnostic::{Diagnostic, Edit, Fix};

/// COR001/002 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("COR001").expr(cor001_expr),
    Rule::new("COR002").expr(cor002_expr),
];

fn cor001_expr(ch: &mut Checker, expr: &Expr) {
    if let Expr::Compare(c) = expr {
        column_eq_none(ch, c, expr);
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
    if matches!(op, CmpOp::Is | CmpOp::IsNot) && !is_always_column_expr(col) {
        return;
    }
    let null = matches!(op, CmpOp::Eq | CmpOp::Is);
    let method = if null { "isNull" } else { "isNotNull" };
    let replacement = format!("{}.{method}()", paren_src(ch, col));
    let (msg, fix) = match op {
        CmpOp::Eq | CmpOp::NotEq => (
            format!(
                "Column {} None is always NULL (a filter drops every row) — use .{method}().",
                if null { "==" } else { "!=" }
            ),
            Fix::safe(
                format!("Replace with `.{method}()`"),
                vec![Edit::replace(expr.range(), replacement)],
            ),
        ),
        _ => (
            format!(
                "`Column {} None` is always {} in Python — use .{method}().",
                if null { "is" } else { "is not" },
                if null { "False" } else { "True" }
            ),
            Fix::unsafe_(
                format!("Replace with `.{method}()`"),
                vec![Edit::replace(expr.range(), replacement)],
            ),
        ),
    };
    ch.report(Diagnostic::new("COR001", msg, expr.range()).with_fix(fix));
}

pub fn bool_op_on_column(ch: &mut Checker, b: &ast::ExprBoolOp, expr: &Expr) {
    if !b.values.iter().any(|v| certain_column(ch, v)) {
        return;
    }
    let (kw, sym) = match b.op {
        BoolOp::And => ("and", "&"),
        BoolOp::Or => ("or", "|"),
    };
    let parts: Vec<String> = b
        .values
        .iter()
        .map(|v| {
            let s = ch.slice(v.range());
            if matches!(
                v,
                Expr::Name(_) | Expr::Attribute(_) | Expr::Call(_) | Expr::Subscript(_)
            ) {
                s.to_string()
            } else {
                format!("({s})")
            }
        })
        .collect();
    let text = parts.join(&format!(" {sym} "));
    let fix = Fix::safe(
        format!("Replace `{kw}` with `{sym}`"),
        vec![Edit::replace(expr.range(), text)],
    );
    ch.report(
        Diagnostic::new(
            "COR002",
            format!("'{kw}' on Columns — use {sym} (with parentheses); Python cannot convert a Column to bool."),
            expr.range(),
        )
        .with_fix(fix),
    );
}

pub fn not_on_column(ch: &mut Checker, u: &ast::ExprUnaryOp, expr: &Expr) {
    if u.op != UnaryOp::Not || !certain_column(ch, &u.operand) {
        return;
    }
    let text = format!("~{}", paren_src(ch, &u.operand));
    let fix = Fix::safe(
        "Replace `not` with `~`",
        vec![Edit::replace(expr.range(), text)],
    );
    ch.report(
        Diagnostic::new(
            "COR002",
            "'not' on a Column — use ~ (with parentheses); Python cannot convert a Column to bool.",
            expr.range(),
        )
        .with_fix(fix),
    );
}

/// Expressions that can never evaluate to Python `None` when they are Columns
/// (calls, `df.x`, `df["x"]`, operators) as opposed to variables.
fn is_always_column_expr(e: &Expr) -> bool {
    match e {
        Expr::Name(_) => false,
        Expr::Attribute(a) => {
            !matches!(&*a.value, Expr::Name(n) if n.id.as_str() == "self" || n.id.as_str() == "cls")
        }
        _ => true,
    }
}

/// Correctness rules are errors: only fire on operands that are Columns with
/// `Certain` confidence (not on heuristically-typed values such as `x.empty` on a
/// name that merely looks like a DataFrame).
fn certain_column(ch: &Checker, e: &Expr) -> bool {
    let t = ch.model.ty_of(e);
    t.is_column() && t.is_certain()
}
