//! Plan facts several rules share: how a join condition constrains the join keys
//! ([`join_cond`] for PRF004, [`conjuncts`] for PRF032 / UDF014) and whether one join
//! side is small ([`small_side`]: PRF004, PRF032, UDF014).

use ruff_python_ast::{self as ast, CmpOp, Expr};
use ruff_text_size::Ranged;
use sparklint_semantic::{Hops, SqlCond as JoinCond, SqlConjunct, Ty, call_arg};

use super::helpers::*;
use crate::checker::Checker;

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
                if f_expr_site(ch, x)
                    .and_then(|s| s.text)
                    .is_some_and(|t| t.cond() == JoinCond::Equi)
                {
                    found = true;
                }
            }
            _ => {}
        }
    });
    found
}

pub(crate) fn join_cond(ch: &Checker, on: &Expr, depth: u32) -> JoinCond {
    if let Some(site) = f_expr_site(ch, on) {
        return site.text.map_or(JoinCond::Unknown, |t| t.cond());
    }
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
        Expr::Name(_) => {
            let t = ch.model.ty_of(on);
            match t.ty {
                Ty::Str | Ty::List(sparklint_semantic::Elem::Str) => return JoinCond::Equi,
                Ty::Column => {}
                _ => return JoinCond::Unknown,
            }
            if depth >= 3 {
                return JoinCond::Unknown;
            }
            match ch.model.hop_one(on, Hops::DEFAULT) {
                Some((v, _)) => join_cond(ch, v, depth + 1),
                None => JoinCond::Unknown,
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

/// One side of a `join` call is small: a known row bound, or marked for broadcast
/// (`F.broadcast(x)`, `x.hint("broadcast" | "broadcastjoin" | "mapjoin")`).
/// One join side is small: a row bound, `F.broadcast(..)` or a broadcast-family `hint(..)`
/// on either side (PRF004, PRF032, UDF014).
pub(crate) fn small_side(
    ch: &Checker,
    c: &ast::ExprCall,
    rt: sparklint_semantic::TypeInfo,
) -> bool {
    let broadcast_marked = |e: &Expr| {
        match e {
        Expr::Call(oc) if ch.model.is_spark_function(&oc.func, "broadcast") => true,
        _ => ch.model.spark_call_named(e, &["hint"]).is_some_and(|sc| {
            sc.is_dataframe()
                && matches!(e, Expr::Call(hc) if hc.arguments.args.first().and_then(|a| ch.model.const_str(a))
                    .is_some_and(|h| matches!(h.to_ascii_lowercase().as_str(),
                        "broadcast" | "broadcastjoin" | "mapjoin")))
        }),
    }
    };
    let receiver_marked = matches!(&*c.func, Expr::Attribute(a) if broadcast_marked(&a.value));
    rt.size().broadcastable()
        || receiver_marked
        || call_arg(c, 0, "other")
            .is_some_and(|o| ch.model.ty_of(o).size().broadcastable() || broadcast_marked(o))
}

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

fn flatten_op<'e>(e: &'e Expr, op: ast::Operator, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::BinOp(b) if b.op == op => {
            flatten_op(&b.left, op, out);
            flatten_op(&b.right, op, out);
        }
        _ => out.push(e),
    }
}

/// Key equalities ANDed directly into `e` (`a.k == b.k`, `a.k.eqNullSafe(b.k)`),
/// as comparable text: whitespace dropped, sides sorted (Catalyst treats
/// `a == b` and `b == a` as the same expression).
fn and_keys(ch: &Checker, e: &Expr) -> Vec<String> {
    let norm = |x: &Expr| -> String {
        ch.slice(x.range())
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect()
    };
    let key = |op: &str, l: &Expr, r: &Expr| {
        let (mut a, mut b) = (norm(l), norm(r));
        if b < a {
            std::mem::swap(&mut a, &mut b);
        }
        format!("{a}{op}{b}")
    };
    let mut parts = Vec::new();
    flatten_op(e, ast::Operator::BitAnd, &mut parts);
    let mut out = Vec::new();
    for p in parts {
        match p {
            Expr::Compare(c) => {
                if let Some((l, CmpOp::Eq, r)) = c.as_single()
                    && !is_literalish(ch, l)
                    && !is_literalish(ch, r)
                {
                    out.push(key("==", l, r));
                }
            }
            Expr::Call(c) => {
                if let Some((_, recv, "eqNullSafe")) = method_call(p)
                    && c.arguments.keywords.is_empty()
                    && let [arg] = &c.arguments.args[..]
                    && !is_literalish(ch, arg)
                {
                    out.push(key("<=>", recv, arg));
                }
            }
            _ => {}
        }
    }
    out
}

/// An equality ANDed into every branch of an OR: Catalyst's BooleanSimplification
/// rewrites `(k & a) | (k & b)` to `k & (a | b)`, so the join still has a key.
fn common_or_key(ch: &Checker, e: &Expr) -> bool {
    let mut branches = Vec::new();
    flatten_op(e, ast::Operator::BitOr, &mut branches);
    let mut it = branches.into_iter();
    let Some(first) = it.next() else {
        return false;
    };
    let mut common = and_keys(ch, first);
    for b in it {
        let k = and_keys(ch, b);
        common.retain(|x| k.contains(x));
    }
    !common.is_empty()
}

pub(crate) fn conjuncts(ch: &Checker, e: &Expr, hops: Hops, out: &mut Vec<Conj>) {
    if let Some(site) = f_expr_site(ch, e) {
        match site.text {
            Some(t) => out.extend(t.conjuncts().into_iter().map(|k| match k {
                SqlConjunct::Equi => Conj::Equi,
                SqlConjunct::OrWithEq => Conj::OrWithEq,
                SqlConjunct::Other => Conj::Other,
            })),
            None => out.push(Conj::Unknown),
        }
        return;
    }
    match e {
        Expr::List(ast::ExprList { elts, .. }) | Expr::Tuple(ast::ExprTuple { elts, .. }) => {
            for x in elts {
                conjuncts(ch, x, hops, out);
            }
        }
        Expr::StringLiteral(_) => out.push(Conj::Equi),
        Expr::BinOp(b) if b.op == ast::Operator::BitAnd => {
            conjuncts(ch, &b.left, hops, out);
            conjuncts(ch, &b.right, hops, out);
        }
        Expr::Name(_) => {
            let t = ch.model.ty_of(e);
            match t.ty {
                Ty::Str | Ty::List(sparklint_semantic::Elem::Str) => return out.push(Conj::Equi),
                Ty::Column => {}
                _ => return out.push(Conj::Unknown),
            }
            match ch.model.hop_one(e, hops) {
                Some((v, h)) => conjuncts(ch, v, h, out),
                None => out.push(Conj::Unknown),
            }
        }
        Expr::BinOp(b) if b.op == ast::Operator::BitOr => {
            let t = ch.model.ty_of(e);
            if !(t.is_column() && t.is_certain()) {
                return out.push(Conj::Unknown);
            }
            out.push(if common_or_key(ch, e) {
                Conj::Equi
            } else if has_equality(ch, e) {
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
