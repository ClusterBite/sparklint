//! How a message names a piece of the user's source: the ONE place for "the real call"
//! wording, so every rule shows the call the way it is written
//! (`orders.toArrow().to_pylist()`), never a hand-built or hard-coded one (`df.…`).
//!
//! - [`short`]: any expression, verbatim when it is short and on one line, else its
//!   root name and `…` (`orders_df…`).
//! - [`receiver`]: the receiver of a method: verbatim when short; else hop by hop
//!   (`spark.read.format("delta").load(…)`, arguments as [`args`] shows them, then all
//!   elided); else [`short`] (an attribute keeps its name: `orders….rdd`).
//! - [`call`]: a call or attribute as written: [`receiver`], the method or attribute
//!   name, and the arguments via [`args`] (verbatim when short, else `(…)`); a plain
//!   function call with long arguments shows its first argument as [`receiver`] does
//!   (`display(orders.filter(…).groupBy("region"))`).
//! - [`method`]: the same from parts, for a call the rule only has the receiver of
//!   (`method(ch, recv, "collect", None)` → `orders.collect()`).

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::Ranged;

use super::helpers::root_name;
use crate::checker::Checker;

/// Longest expression [`short`] shows verbatim.
const SHORT_MAX: usize = 32;
/// Longest argument list (with its parentheses) [`call`] shows verbatim.
const ARGS_MAX: usize = 24;
/// Longest receiver [`receiver`] shows hop by hop.
const CHAIN_MAX: usize = 48;

fn fits(s: &str, max: usize) -> bool {
    s.len() <= max && !s.contains('\n')
}

/// A short human description of an expression for messages (`df`, `orders_df…`).
pub fn short(ch: &Checker, e: &Expr) -> String {
    let s = ch.slice(e.range());
    if fits(s, SHORT_MAX) {
        return s.to_string();
    }
    match root_name(e) {
        Some(n) => format!("{}…", n.id.as_str()),
        None => "…".to_string(),
    }
}

/// A call's argument list as a message shows it: `()`, `("records")`, `(…)`.
pub fn args(ch: &Checker, a: &ast::Arguments) -> String {
    if a.args.is_empty() && a.keywords.is_empty() {
        return "()".to_string();
    }
    let s = ch.slice(a.range);
    if fits(s, ARGS_MAX) {
        s.to_string()
    } else {
        "(…)".to_string()
    }
}

/// The receiver of a method as a message shows it: verbatim when short; else hop by
/// hop with the arguments as [`args`] shows them (`df.coalesce(1).write.mode("overwrite")`),
/// then with every argument elided (`spark.read.format(…).load(…)`); else [`short`], an
/// attribute keeping its name (`orders….rdd`).
pub fn receiver(ch: &Checker, e: &Expr) -> String {
    if fits(ch.slice(e.range()), SHORT_MAX) {
        return ch.slice(e.range()).to_string();
    }
    for elide in [false, true] {
        if let Some(r) = chain(ch, e, elide)
            && fits(&r, CHAIN_MAX)
        {
            return r;
        }
    }
    match e {
        Expr::Attribute(a) => format!("{}.{}", short(ch, &a.value), a.attr.as_str()),
        _ => short(ch, e),
    }
}

/// `e` hop by hop from its root (`spark.read.format(…)`); `None` when the root (the
/// first value that is no attribute or call) is not a name and does not fit.
fn chain(ch: &Checker, e: &Expr, elide: bool) -> Option<String> {
    match e {
        Expr::Name(n) => Some(n.id.to_string()),
        Expr::Attribute(a) => Some(format!(
            "{}.{}",
            chain(ch, &a.value, elide)?,
            a.attr.as_str()
        )),
        Expr::Call(c) => {
            let f = chain(ch, &c.func, elide)?;
            let a = &c.arguments;
            if elide && !(a.args.is_empty() && a.keywords.is_empty()) {
                Some(format!("{f}(…)"))
            } else {
                Some(format!("{f}{}", args(ch, a)))
            }
        }
        _ => {
            let s = ch.slice(e.range());
            fits(s, SHORT_MAX).then(|| s.to_string())
        }
    }
}

/// `recv.m(args)` as a message shows it; `args: None` = called with no arguments.
pub fn method(ch: &Checker, recv: &Expr, m: &str, a: Option<&ast::Arguments>) -> String {
    let a = a.map_or_else(|| "()".to_string(), |a| args(ch, a));
    format!("{}.{m}{a}", receiver(ch, recv))
}

/// The real call (or attribute) `e` as a message names it: `orders.toArrow().to_pylist()`,
/// `spark.sparkContext`, `orders.show(5)`, `orders.write.save(…)`.
pub fn call(ch: &Checker, e: &Expr) -> String {
    match e {
        Expr::Call(c) => match &*c.func {
            Expr::Attribute(a) => method(ch, &a.value, a.attr.as_str(), Some(&c.arguments)),
            f => function_call(ch, f, &c.arguments),
        },
        Expr::Attribute(a) => format!("{}.{}", receiver(ch, &a.value), a.attr.as_str()),
        _ => short(ch, e),
    }
}

/// `f(args)` for a plain function call: the arguments verbatim when short; else the
/// first positional argument as [`receiver`] shows it, then `…` for the rest
/// (`display(orders.filter(…).groupBy("region"))`, `print(rows…, …)`); else `f(…)`.
fn function_call(ch: &Checker, f: &Expr, a: &ast::Arguments) -> String {
    let name = short(ch, f);
    if fits(ch.slice(a.range), ARGS_MAX) || a.args.is_empty() && a.keywords.is_empty() {
        return format!("{name}{}", args(ch, a));
    }
    match a.args.first() {
        Some(first) if !matches!(first, Expr::Starred(_)) => {
            let rest = if a.args.len() > 1 || !a.keywords.is_empty() {
                ", …"
            } else {
                ""
            };
            format!("{name}({}{rest})", receiver(ch, first))
        }
        _ => format!("{name}(…)"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::testing::with_checker;
    use ruff_python_ast::visitor::{self, Visitor};

    /// Describe the first expression whose source is `needle` with `f`.
    fn describe(src: &str, needle: &str, f: fn(&Checker, &Expr) -> String) -> String {
        with_checker(src, &["ALL"], |ch| {
            struct V<'s, 'b> {
                src: &'s str,
                needle: &'s str,
                found: Option<&'b Expr>,
            }
            impl<'b> Visitor<'b> for V<'_, 'b> {
                fn visit_expr(&mut self, e: &'b Expr) {
                    if self.found.is_none() && &self.src[e.range()] == self.needle {
                        self.found = Some(e);
                    }
                    visitor::walk_expr(self, e);
                }
            }
            let mut v = V {
                src,
                needle,
                found: None,
            };
            v.visit_body(&ch.module.body);
            let e = v
                .found
                .unwrap_or_else(|| panic!("no expression `{needle}`"));
            f(ch, e)
        })
    }

    #[test]
    fn short_table() {
        let long = "orders_with_a_long_name.filter(F.col('amount') > 0)";
        for (src, want) in [
            ("df", "df"),
            ("orders.toPandas()", "orders.toPandas()"),
            (long, "orders_with_a_long_name…"),
            ("[a, b][0].collect_list_of_things_long_x()", "…"),
        ] {
            assert_eq!(describe(src, src, short), want, "{src}");
        }
        // multi-line: never verbatim
        let ml = "(df\n  .filter(x))";
        assert_eq!(describe(ml, "df\n  .filter(x)", short), "df…");
    }

    #[test]
    fn call_table() {
        for (src, want) in [
            // the card's example: same wording for every shape
            (
                "orders.toArrow().to_pylist()",
                "orders.toArrow().to_pylist()",
            ),
            ("orders.collect()", "orders.collect()"),
            ("orders.collect( )", "orders.collect()"),
            (
                "orders.toPandas().to_dict(\"records\")",
                "orders.toPandas().to_dict(\"records\")",
            ),
            ("orders.show(5)", "orders.show(5)"),
            // arguments elided when long or multi-line
            (
                "orders.show(n=20, truncate=False, vertical=True)",
                "orders.show(…)",
            ),
            ("orders.show(\n  5)", "orders.show(…)"),
            // a long receiver: hop by hop, then arguments elided, then `short`
            (
                "orders_with_a_long_name.filter(F.col('a')).rdd.isEmpty()",
                "orders_with_a_long_name.filter(F.col('a')).rdd.isEmpty()",
            ),
            (
                "orders_with_a_long_name.filter(F.col('amount') > 0).collect()",
                "orders_with_a_long_name.filter(…).collect()",
            ),
            (
                "dfs[0].filter(F.col('amount') > 1000000).collect()",
                "dfs[0].filter(…).collect()",
            ),
            (
                "spark.read.format(\"delta\").option(\"a\", \"b\").option(\"c\", \"d\").load(path).collect()",
                "spark….collect()",
            ),
            (
                "orders_with_a_very_long_name_indeed_yes.rdd.collect()",
                "orders_with_a_very_long_name_indeed_yes.rdd.collect()",
            ),
            (
                "orders_with_a_very_long_name_indeed_yes_really.something_else_long.rdd.collect()",
                "orders_with_a_very_long_name_indeed_yes_really….rdd.collect()",
            ),
            (
                "orders.write.mode(\"append\").save(path)",
                "orders.write.mode(\"append\").save(path)",
            ),
            // plain function calls and attributes
            ("display(orders)", "display(orders)"),
            // long arguments: the first one as a receiver, the rest elided
            (
                "display(orders.filter(\"amount > 100 and region = 'EU'\"))",
                "display(orders.filter(…))",
            ),
            (
                "display(orders.groupBy(\"region\").count(), streamName=\"x\")",
                "display(orders.groupBy(\"region\").count(), …)",
            ),
            (
                "print(some_very_long_value_name_here_x_y, end='')",
                "print(some_very_long_value_name_here_x_y, …)",
            ),
            ("print(*rows_with_a_long_name_x, sep=',')", "print(…)"),
            ("spark.sparkContext", "spark.sparkContext"),
            (
                "SparkSession.builder.appName('a_long_application_name').getOrCreate().sparkContext",
                "SparkSession.builder.appName(…).getOrCreate().sparkContext",
            ),
            // anything else: `short`
            ("orders", "orders"),
        ] {
            assert_eq!(describe(src, src, call), want, "{src}");
        }
        // a multi-line chain is shown hop by hop on one line
        let ml = "(orders\n    .filter(x)\n    .groupBy(\"k\")\n    .count()\n    .collect())";
        assert_eq!(
            describe(ml, &ml[1..ml.len() - 1], call),
            "orders.filter(x).groupBy(\"k\").count().collect()"
        );
    }

    #[test]
    fn method_from_parts() {
        let got = describe("orders.limit(5)", "orders.limit(5)", |ch, e| {
            let Expr::Call(c) = e else { unreachable!() };
            let Expr::Attribute(a) = &*c.func else {
                unreachable!()
            };
            format!(
                "{} / {}",
                method(ch, &a.value, "collect", None),
                method(ch, e, "count", None)
            )
        });
        assert_eq!(got, "orders.collect() / orders.limit(5).count()");
    }
}
