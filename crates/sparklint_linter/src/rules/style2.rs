//! STY002 join-missing-how, STY003 redundant-drop-before-select.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::columns::ColumnRefs;

use super::Rule;
use super::helpers::*;
use super::msg::Msg;
use crate::checker::Checker;
use crate::diagnostic::{Edit, Fix};
use crate::msg;

/// STY002/003 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("STY002").call(join_missing_how).msgs(&[STY002]),
    Rule::new("STY003")
        .call(redundant_drop_before_select)
        .msgs(&[STY003]),
];

const STY002: Msg = Msg::new(
    "STY002",
    "missing-how",
    "join() without an explicit how= — state the join type (defaults to inner).",
);
const STY003: Msg = Msg::new(
    "STY003",
    "drop-before-select",
    "drop() before select() of explicit columns is redundant — select() already picks the columns.",
);

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
    let mut d = msg!(STY002, method_range(expr));
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

/// A select argument whose columns are not spelled out (`*cols`, a list or `.columns`,
/// a variable that is not a Column).
fn is_dynamic(ch: &Checker, e: &Expr) -> bool {
    match e {
        Expr::Starred(_) => true,
        Expr::Attribute(a) => a.attr.as_str() == "columns",
        Expr::List(_) | Expr::ListComp(_) | Expr::Name(_) => !ch.model.is_column(e),
        _ => false,
    }
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
    if c.arguments.args.iter().any(|a| is_dynamic(ch, a)) {
        return;
    }
    let selected: Vec<ColumnRefs> = c
        .arguments
        .args
        .iter()
        .map(|a| ColumnRefs::of(ch.model, a))
        .collect();
    if selected.iter().any(|r| r.star) {
        return; // `"*"` / `"t.*"` selects the remaining columns
    }
    let mut d = msg!(STY003, method_range(recv));
    let dropped: Vec<ColumnRefs> = dc
        .arguments
        .args
        .iter()
        .map(|a| ColumnRefs::of(ch.model, a))
        .collect();
    // The fix is safe only when no selected column can be a dropped one.
    let refers_dropped = dropped.iter().chain(&selected).any(|r| r.unknown)
        || dropped
            .iter()
            .flat_map(|r| &r.names)
            .any(|x| selected.iter().any(|s| s.contains(x)));
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
