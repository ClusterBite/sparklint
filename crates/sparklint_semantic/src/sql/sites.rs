//! Where SQL text sits in a call: [`SemanticModel::sql_sites`] answers "which SQL
//! texts does this call pass to Spark?" for `F.expr(str)`, `df.filter(condition)` /
//! `df.where(condition)`, every item of `df.selectExpr(*expr)` and `spark.sql(sqlQuery)`.
//! Each [`SqlSite`] carries the text ([`SqlText`]), the f-string interpolations it was
//! built from (`holes`) and the range of the argument.
//!
//! Every site reads its text the same way ([`SemanticModel::sql_text`]): a string
//! literal or f-string (interpolations become `{}`), or a name whose every reaching
//! binding holds the same such text (followed up to [`CONST_DEPTH`] hops, the
//! `const_str` rule). The argument is the keyword (`str=`, `condition=`, `sqlQuery=`)
//! or the first positional one; `selectExpr` items are every positional argument, list
//! literals and `*[..]` flattened. `spark.sql("EXPLAIN ..")` is no site: it only plans.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};

use super::SqlText;
use crate::constant::CONST_DEPTH;
use crate::infer::{call_arg, static_text};
use crate::model::SemanticModel;

/// What a SQL site holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlSiteKind {
    /// `spark.sql(sqlQuery)`: a whole statement.
    Query,
    /// `df.filter(condition)` / `df.where(condition)`: a boolean expression.
    Condition,
    /// One item of `df.selectExpr(*expr)`: a select-list expression.
    SelectExpr,
    /// `F.expr(str)`: a Column expression.
    Expression,
}

/// One SQL text passed by a call.
#[derive(Debug, Clone)]
pub struct SqlSite<'e> {
    pub kind: SqlSiteKind,
    pub call: &'e ast::ExprCall,
    /// The function / method name as called (`expr`, `filter`, `where`, `selectExpr`, `sql`).
    pub method: &'e str,
    /// The argument holding the text; `None` when the call has none there.
    pub arg: Option<&'e Expr>,
    /// The text, when it can be read ([`SemanticModel::sql_text`]).
    pub text: Option<SqlText>,
    /// The interpolated expressions of the f-string(s) the text was read from (format
    /// specs included; every agreeing binding's).
    pub holes: Vec<&'e Expr>,
    /// The argument's range (the call's when there is no argument).
    pub range: TextRange,
}

/// The positional arguments of a call as SQL items: list literals flattened, starred
/// arguments unwrapped (`*["a", "b"]`).
fn items(c: &ast::ExprCall) -> Vec<&Expr> {
    let mut out = Vec::new();
    for a in c.arguments.args.iter() {
        let a = match a {
            Expr::Starred(s) => &*s.value,
            _ => a,
        };
        match a {
            Expr::List(l) => out.extend(l.elts.iter()),
            _ => out.push(a),
        }
    }
    out
}

impl<'a> SemanticModel<'a> {
    /// The SQL text of `e`, with the f-string holes it came from: a string literal or
    /// f-string, or a name whose every reaching binding holds the same text.
    pub fn sql_text<'e>(&self, e: &'e Expr) -> Option<(String, Vec<&'e Expr>)>
    where
        'a: 'e,
    {
        self.sql_text_at(e, 0)
    }

    fn sql_text_at<'e>(&self, e: &'e Expr, depth: u32) -> Option<(String, Vec<&'e Expr>)>
    where
        'a: 'e,
    {
        if depth > CONST_DEPTH {
            return None;
        }
        if let Some(t) = static_text(e) {
            return Some((t, crate::literal::holes(e)));
        }
        let mut acc: Option<(String, Vec<&'e Expr>)> = None;
        for v in self.const_bindings(e)? {
            let (t, h) = self.sql_text_at(v, depth + 1)?;
            match &mut acc {
                Some((a, _)) if *a != t => return None,
                Some((_, ah)) => ah.extend(h),
                None => acc = Some((t, h)),
            }
        }
        acc
    }

    /// The SQL texts call `e` passes to Spark, in argument order: `F.expr(str)`
    /// (`pyspark.sql.functions.expr`), `filter` / `where` on a DataFrame, each item of a
    /// DataFrame's `selectExpr`, `sql` on a session (its query, not an `EXPLAIN`). A
    /// recognised call whose argument is missing or unreadable still yields a site
    /// (`arg` / `text` `None`), except `spark.sql`.
    pub fn sql_sites<'e>(&self, e: &'e Expr) -> Vec<SqlSite<'e>>
    where
        'a: 'e,
    {
        let Expr::Call(c) = e else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let push = |kind, method, arg: Option<&'e Expr>, out: &mut Vec<SqlSite<'e>>| {
            let (text, holes) = match arg.and_then(|a| self.sql_text(a)) {
                Some((t, h)) => (Some(SqlText::new(&t)), h),
                None => (None, Vec::new()),
            };
            out.push(SqlSite {
                kind,
                call: c,
                method,
                arg,
                text,
                holes,
                range: arg.map_or(c.range(), Ranged::range),
            });
        };
        if self.is_spark_function(&c.func, "expr") {
            let method = match &*c.func {
                Expr::Attribute(a) => a.attr.as_str(),
                Expr::Name(n) => n.id.as_str(),
                _ => "expr",
            };
            push(
                SqlSiteKind::Expression,
                method,
                call_arg(c, 0, "str"),
                &mut out,
            );
            return out;
        }
        let Expr::Attribute(attr) = &*c.func else {
            return out;
        };
        let method = attr.attr.as_str();
        let kind = match method {
            "filter" | "where" => SqlSiteKind::Condition,
            "selectExpr" => SqlSiteKind::SelectExpr,
            "sql" => SqlSiteKind::Query,
            _ => return out,
        };
        let Some(sc) = self.spark_call_named(e, &[method]) else {
            return out;
        };
        match kind {
            SqlSiteKind::Query if sc.class().is_session() => {
                let Some(arg) = call_arg(c, 0, "sqlQuery") else {
                    return out;
                };
                let Some((t, holes)) = self.sql_text(arg) else {
                    return out;
                };
                let text = SqlText::new(&t);
                if text.explain() {
                    return out;
                }
                out.push(SqlSite {
                    kind,
                    call: c,
                    method,
                    arg: Some(arg),
                    text: Some(text),
                    holes,
                    range: arg.range(),
                });
            }
            SqlSiteKind::Condition if sc.is_dataframe() => {
                push(kind, method, call_arg(c, 0, "condition"), &mut out);
            }
            SqlSiteKind::SelectExpr if sc.is_dataframe() => {
                for a in items(c) {
                    push(kind, method, Some(a), &mut out);
                }
            }
            _ => {}
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSummaries, SemanticOptions, build_model};
    use ruff_python_ast::visitor::{self, Visitor};
    use ruff_python_ast::{Expr, ModModule};
    use ruff_text_size::TextSize;

    fn parse(src: &str) -> ModModule {
        ruff_python_parser::parse_module(src)
            .expect("valid test source")
            .into_syntax()
    }

    struct Calls<'a> {
        src: &'a str,
        found: Vec<(&'a str, &'a Expr)>,
    }
    impl<'a> Visitor<'a> for Calls<'a> {
        fn visit_expr(&mut self, e: &'a Expr) {
            if matches!(e, Expr::Call(_)) {
                let r = e.range();
                self.found
                    .push((&self.src[usize::from(r.start())..usize::from(r.end())], e));
            }
            visitor::walk_expr(self, e);
        }
    }

    const PRELUDE: &str = "from pyspark.sql import SparkSession, DataFrame\n\
        import pyspark.sql.functions as F\n\
        from pyspark.sql.functions import expr\n\
        spark = SparkSession.builder.getOrCreate()\n\
        df = spark.table(\"t\")\n";

    type Site = (SqlSiteKind, String, Vec<String>);

    /// (kind, text or "-", hole sources) of every site of the LAST call whose source
    /// is the last line of `body` (stripped; after PRELUDE).
    fn sites(body: &str) -> Vec<Site> {
        let needle = body.lines().last().unwrap_or(body).trim();
        let needle = needle.strip_prefix("return ").unwrap_or(needle);
        let src = format!("{PRELUDE}{body}\n");
        let m = parse(&src);
        let model = build_model(
            &m,
            TextSize::of(src.as_str()),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let mut v = Calls {
            src: &src,
            found: Vec::new(),
        };
        v.visit_body(&m.body);
        let (_, e) = v
            .found
            .iter()
            .rev()
            .find(|(s, _)| *s == needle)
            .unwrap_or_else(|| panic!("no call `{needle}`"));
        model
            .sql_sites(e)
            .into_iter()
            .map(|s| {
                let holes = s.holes.iter().map(|h| src[h.range()].to_string()).collect();
                (
                    s.kind,
                    s.text.map_or("-".to_string(), |t| t.raw().to_string()),
                    holes,
                )
            })
            .collect()
    }

    fn s(kind: SqlSiteKind, t: &str, holes: &[&str]) -> Site {
        (
            kind,
            t.to_string(),
            holes.iter().map(|h| h.to_string()).collect(),
        )
    }

    #[test]
    fn expression_shapes() {
        use SqlSiteKind::Expression as E;
        let cases: &[(&str, Vec<Site>)] = &[
            ("F.expr('a = b')", vec![s(E, "a = b", &[])]),
            ("expr('a = b')", vec![s(E, "a = b", &[])]),
            ("F.expr(str='a')", vec![s(E, "a", &[])]),
            ("F.expr(f'x > {n:{w}}')", vec![s(E, "x > {}", &["n", "w"])]),
            ("c = 'a = b'\nF.expr(c)", vec![s(E, "a = b", &[])]),
            // every reaching binding must agree; chains are followed
            (
                "if p:\n    c = 'a'\nelse:\n    c = 'a'\nF.expr(c)",
                vec![s(E, "a", &[])],
            ),
            (
                "if p:\n    c = 'a'\nelse:\n    c = 'b'\nF.expr(c)",
                vec![s(E, "-", &[])],
            ),
            (
                "c = f'k IN ({ids})'\nd = c\nF.expr(d)",
                vec![s(E, "k IN ({})", &["ids"])],
            ),
            ("def f(c):\n    return F.expr(c)", vec![s(E, "-", &[])]),
            ("F.expr()", vec![s(E, "-", &[])]),
            ("pd.expr('a')", vec![]),
        ];
        for (body, want) in cases {
            assert_eq!(&sites(body), want, "{body}");
        }
    }

    #[test]
    fn method_shapes() {
        use SqlSiteKind::{Condition as C, Query as Q, SelectExpr as X};
        let cases: &[(&str, Vec<Site>)] = &[
            ("df.filter('a > 1')", vec![s(C, "a > 1", &[])]),
            ("df.where(condition='a')", vec![s(C, "a", &[])]),
            ("df.filter(f'k = {v}')", vec![s(C, "k = {}", &["v"])]),
            ("df.filter(F.col('a'))", vec![s(C, "-", &[])]),
            ("xs = [1]\nxs.filter('a')", vec![]),
            (
                "df.selectExpr('a', ['b', 'c'], *['d'])",
                vec![
                    s(X, "a", &[]),
                    s(X, "b", &[]),
                    s(X, "c", &[]),
                    s(X, "d", &[]),
                ],
            ),
            ("cols = 'x'\ndf.selectExpr(cols)", vec![s(X, "x", &[])]),
            ("spark.sql('SELECT 1')", vec![s(Q, "SELECT 1", &[])]),
            (
                "spark.sql(sqlQuery=f'SELECT {a}')",
                vec![s(Q, "SELECT {}", &["a"])],
            ),
            ("q = 'SELECT 2'\nspark.sql(q)", vec![s(Q, "SELECT 2", &[])]),
            ("spark.sql('EXPLAIN SELECT 1')", vec![]),
            ("spark.sql(q)", vec![]),
            ("df.select('a')", vec![]),
        ];
        for (body, want) in cases {
            assert_eq!(&sites(body), want, "{body}");
        }
    }
}
