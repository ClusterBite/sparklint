//! Shared helpers for rules.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::Ranged;
use sparklint_semantic::{Ty, is_logger_name};

use crate::checker::Checker;

/// `recv.method(...)` → (call, receiver, method).
pub fn method_call(e: &Expr) -> Option<(&ast::ExprCall, &Expr, &str)> {
    let Expr::Call(c) = e else { return None };
    let Expr::Attribute(a) = &*c.func else {
        return None;
    };
    Some((c, &a.value, a.attr.as_str()))
}

/// Root name of a call/attribute/subscript chain (`df.a().b` → `df`).
pub fn root_name(mut e: &Expr) -> Option<&ast::ExprName> {
    loop {
        match e {
            Expr::Name(n) => return Some(n),
            Expr::Attribute(a) => e = &a.value,
            Expr::Call(c) => e = &c.func,
            Expr::Subscript(s) => e = &s.value,
            _ => return None,
        }
    }
}

pub fn has_no_args(c: &ast::ExprCall) -> bool {
    c.arguments.args.is_empty() && c.arguments.keywords.is_empty()
}

/// Expressions that never need parentheses when used as a receiver/operand.
pub fn is_atomic(e: &Expr) -> bool {
    matches!(
        e,
        Expr::Name(_)
            | Expr::Attribute(_)
            | Expr::Call(_)
            | Expr::Subscript(_)
            | Expr::StringLiteral(_)
            | Expr::NumberLiteral(_)
            | Expr::BooleanLiteral(_)
            | Expr::NoneLiteral(_)
            | Expr::List(_)
            | Expr::Dict(_)
            | Expr::Set(_)
            | Expr::ListComp(_)
            | Expr::DictComp(_)
            | Expr::SetComp(_)
            | Expr::FString(_)
    ) || matches!(e, Expr::Tuple(t) if t.parenthesized)
        || matches!(e, Expr::Generator(g) if g.parenthesized)
}

/// Source of `e`, parenthesised unless atomic.
pub fn paren_src(ch: &Checker, e: &Expr) -> String {
    let s = ch.slice(e.range());
    if is_atomic(e) {
        s.to_string()
    } else {
        format!("({s})")
    }
}

/// A short human description of an expression for messages (`df`, `orders_df…`).
pub fn short(ch: &Checker, e: &Expr) -> String {
    let s = ch.slice(e.range());
    if s.len() <= 32 && !s.contains('\n') {
        return s.to_string();
    }
    match root_name(e) {
        Some(n) => format!("{}…", n.id.as_str()),
        None => "…".to_string(),
    }
}

/// `True` / `"true"` literal → Some(true), `False` / `"false"` → Some(false).
pub fn bool_literal(e: &Expr) -> Option<bool> {
    match e {
        Expr::BooleanLiteral(b) => Some(b.value),
        Expr::StringLiteral(s) => match s.value.to_str().to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// `F.lit(None)`.
pub fn is_lit_none(ch: &Checker, e: &Expr) -> bool {
    let Expr::Call(c) = e else { return false };
    ch.model.is_spark_function(&c.func, "lit")
        && c.arguments.args.len() == 1
        && c.arguments.args[0].is_none_literal_expr()
}

const LOG_METHODS: &[&str] = &[
    "debug",
    "info",
    "warning",
    "warn",
    "error",
    "critical",
    "exception",
    "log",
    "fatal",
];

/// Whether a call is a logging/printing sink (`print`, `display`, `logging.info`,
/// a typed or conventionally named logger, `dbutils.notebook.exit`,
/// `mlflow.log_metric`).
pub fn is_logging_sink(ch: &Checker, c: &ast::ExprCall) -> bool {
    if let Some(q) = ch.model.qualified_name(&c.func) {
        if matches!(
            q.as_str(),
            "builtins.print"
                | "databricks.display"
                | "mlflow.log_metric"
                | "mlflow.log_metrics"
                | "mlflow.log_param"
                | "mlflow.log_params"
        ) {
            return true;
        }
        if let Some(m) = q.strip_prefix("logging.")
            && LOG_METHODS.contains(&m)
        {
            return true;
        }
        if let Some(m) = q.strip_prefix("loguru.logger.")
            && (LOG_METHODS.contains(&m) || m == "success")
        {
            return true;
        }
    }
    let Expr::Attribute(a) = &*c.func else {
        return false;
    };
    let m = a.attr.as_str();
    // dbutils.notebook.exit(...)
    if m == "exit"
        && let Expr::Attribute(inner) = &*a.value
        && inner.attr.as_str() == "notebook"
        && ch.model.ty_of(&inner.value).ty == Ty::DbUtils
    {
        return true;
    }
    if !LOG_METHODS.contains(&m) {
        return false;
    }
    let recv = ch.model.ty_of(&a.value);
    if recv.ty == Ty::Logger {
        return true;
    }
    if recv.is_unknown()
        && ch.settings.heuristics
        && let Expr::Attribute(ra) = &*a.value
    {
        // self.logger.info(...)
        return is_logger_name(ra.attr.as_str());
    }
    false
}

/// Is `e` a `DataFrame.count()` call with no arguments? Returns the receiver.
pub fn df_count_call<'e>(ch: &Checker, e: &'e Expr) -> Option<&'e Expr> {
    let sc = ch.model.spark_call_named(e, &["count"])?;
    // Counting a bounded frame (`df.limit(1).count()`) is cheap: not an issue.
    (sc.has_no_args() && sc.is_dataframe() && sc.bounded().is_none()).then_some(sc.receiver)
}

/// Visit every expression below `e` (not entering lambdas or nested scopes).
pub fn walk_exprs<'e>(e: &'e Expr, f: &mut dyn FnMut(&'e Expr)) {
    use ruff_python_ast::visitor::{self, Visitor};
    struct W<'f, 'e>(&'f mut dyn FnMut(&'e Expr));
    impl<'e> Visitor<'e> for W<'_, 'e> {
        fn visit_expr(&mut self, e: &'e Expr) {
            (self.0)(e);
            if !matches!(e, Expr::Lambda(_)) {
                visitor::walk_expr(self, e);
            }
        }
    }
    W(f).visit_expr(e);
}

/// Number of statements in the block that directly contains `stmt`.
pub fn block_len(ch: &Checker, stmt: &ast::Stmt) -> usize {
    use ast::Stmt;
    let r = stmt.range();
    let find = |body: &[Stmt]| body.iter().any(|s| s.range() == r).then_some(body.len());
    let Some(parent) = ch.parent_stmt_of_current() else {
        return find(&ch.module.body).unwrap_or(2);
    };
    let n = match parent {
        Stmt::FunctionDef(f) => find(&f.body),
        Stmt::ClassDef(c) => find(&c.body),
        Stmt::For(f) => find(&f.body).or_else(|| find(&f.orelse)),
        Stmt::While(w) => find(&w.body).or_else(|| find(&w.orelse)),
        Stmt::If(i) => {
            find(&i.body).or_else(|| i.elif_else_clauses.iter().find_map(|c| find(&c.body)))
        }
        Stmt::With(w) => find(&w.body),
        Stmt::Try(t) => find(&t.body)
            .or_else(|| find(&t.orelse))
            .or_else(|| find(&t.finalbody))
            .or_else(|| {
                t.handlers.iter().find_map(|h| {
                    let ast::ExceptHandler::ExceptHandler(h) = h;
                    find(&h.body)
                })
            }),
        Stmt::Match(m) => m.cases.iter().find_map(|c| find(&c.body)),
        _ => None,
    };
    n.unwrap_or(2)
}
