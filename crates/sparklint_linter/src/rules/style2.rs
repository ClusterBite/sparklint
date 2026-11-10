//! STY002 join-missing-how, STY003 redundant-drop-before-select.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::str_literal;

use super::Rule;
use super::actions::method_range;
use super::helpers::*;
use crate::checker::Checker;
use crate::diagnostic::{Diagnostic, Edit, Fix};

/// STY002/003 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("STY002").call(join_missing_how),
    Rule::new("STY003").call(redundant_drop_before_select),
];

pub fn join_missing_how(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["join"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let args = &c.arguments;
    if args.args.len() >= 3
        || args.args.iter().any(Expr::is_starred_expr)
        || args
            .keywords
            .iter()
            .any(|k| k.arg.as_ref().is_none_or(|a| a.as_str() == "how"))
    {
        return;
    }
    if args.args.is_empty() && args.keywords.is_empty() {
        return;
    }
    let mut d = Diagnostic::new(
        "STY002",
        "join() without an explicit how= — state the join type (defaults to inner).",
        method_range(expr),
    );
    let last_end = args
        .args
        .iter()
        .map(Ranged::end)
        .chain(args.keywords.iter().map(Ranged::end))
        .max();
    // Positional `on` is required before a positional how; use a keyword.
    if let Some(end) = last_end {
        d = d.with_fix(Fix::safe(
            "Add how=\"inner\"",
            vec![Edit::insert(end, ", how=\"inner\"")],
        ));
    }
    ch.report(d);
}

fn is_star_or_dynamic(ch: &Checker, e: &Expr) -> bool {
    match e {
        Expr::Starred(_) => true,
        Expr::StringLiteral(s) => s.value.to_str() == "*" || s.value.to_str().ends_with(".*"),
        Expr::Attribute(a) => a.attr.as_str() == "columns",
        Expr::List(_) | Expr::ListComp(_) | Expr::Name(_) => !ch.model.is_column(e),
        Expr::Call(c) => {
            ch.model.is_spark_function(&c.func, "col")
                && c.arguments
                    .args
                    .first()
                    .and_then(str_literal)
                    .is_some_and(|s| s == "*" || s.ends_with(".*"))
        }
        _ => false,
    }
}

fn mentioned_names(e: &Expr) -> Vec<String> {
    let mut out = Vec::new();
    walk_exprs(e, &mut |x| {
        if let Some(s) = str_literal(x) {
            out.push(s.to_string());
        }
        if let Expr::Attribute(a) = x {
            out.push(a.attr.as_str().to_string());
        }
    });
    out
}

pub fn redundant_drop_before_select(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some((_, recv, m)) = method_call(expr) else {
        return;
    };
    if m != "select" || c.arguments.args.is_empty() || !c.arguments.keywords.is_empty() {
        return;
    }
    let Some(drop) = ch.model.spark_call_named(recv, &["drop"]) else {
        return;
    };
    if !drop.is_dataframe() {
        return;
    }
    let (dc, drecv) = (drop.call, drop.receiver);
    if c.arguments.args.iter().any(|a| is_star_or_dynamic(ch, a)) {
        return;
    }
    let mut d = Diagnostic::new(
        "STY003",
        "drop() before select() of explicit columns is redundant — select() already picks the columns.",
        method_range(recv),
    );
    let dropped: Vec<String> = dc.arguments.args.iter().flat_map(mentioned_names).collect();
    let selected: Vec<String> = c.arguments.args.iter().flat_map(mentioned_names).collect();
    let refers_dropped = dropped.iter().any(|x| selected.contains(x));
    if !refers_dropped {
        d = d.with_fix(Fix::safe(
            "Remove the .drop()",
            vec![Edit::delete(TextRange::new(
                drecv.range().end(),
                recv.range().end(),
            ))],
        ));
    }
    ch.report(d);
}
