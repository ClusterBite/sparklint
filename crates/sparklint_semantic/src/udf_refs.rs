//! "Does this column expression call a Python UDF?": the one answer every rule asks
//! instead of walking column expressions for UDF calls itself.
//!
//! [`UdfRefs::of`] returns every UDF reference an expression makes, in source order:
//!
//! - [`UdfCall::Python`]: a Python / pandas UDF value called (`my_udf(F.col("a"))`,
//!   the oracle types the callee [`Ty::Udf`]), with the DataFrames its arguments
//!   reference (`df.a` / `df["a"]`: UDF014's "a UDF of both join sides");
//! - [`UdfCall::Registered`]: SQL text (`F.expr(..)`, a filter string, …) calling a
//!   function registered in this file with `spark.udf.register("name", ..)`. The
//!   registered names are computed once per module ([`SemanticModel::registered_udfs`]);
//! - either of those behind a Column variable bound once (`c = my_udf(x)` …
//!   `filter(c)`, up to [`NAME_HOPS`] hops; [`UdfRef::via_name`]);
//! - [`UdfRefs::opaque`]: something that may call a UDF but cannot be read (`F.expr`
//!   text that cannot be resolved, a Column variable bound more than once).
//!
//! Which function a UDF wraps (`F.udf(fn)` → `fn`) is a different question: the
//! linter's `rules/udf_target.rs` answers it.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, Expr, ModModule};
use ruff_text_size::Ranged;

use crate::constant::str_literal;
use crate::model::SemanticModel;
use crate::sql::{SqlSiteKind, SqlText};
use crate::types::Ty;

/// How many Column-variable hops [`UdfRefs::of`] follows.
pub const NAME_HOPS: u32 = 3;

/// One UDF call.
#[derive(Debug, Clone, PartialEq)]
pub enum UdfCall<'e> {
    /// A Python / pandas UDF value called: `call.func` is the UDF (`my_udf`).
    Python {
        call: &'e ast::ExprCall,
        /// DataFrame names the call's arguments reference as `df.col` / `df["col"]`
        /// (first-seen order, no duplicates).
        frames: Vec<String>,
    },
    /// SQL text calling a UDF registered in this file: the registered name
    /// (lower-case).
    Registered { name: String },
}

/// One UDF reference of an expression.
#[derive(Debug, Clone, PartialEq)]
pub struct UdfRef<'e> {
    pub call: UdfCall<'e>,
    /// Reached through a Column variable (`c = my_udf(x)` … `c`).
    pub via_name: bool,
}

/// Every UDF reference of an expression, in source order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UdfRefs<'e> {
    pub refs: Vec<UdfRef<'e>>,
    /// Something that may call a UDF cannot be read: `F.expr` text that cannot be
    /// resolved, a Column variable not bound exactly once or past [`NAME_HOPS`].
    pub opaque: bool,
}

impl<'e> UdfRefs<'e> {
    /// The UDF references of `e` (not entering lambdas).
    pub fn of<'a>(model: &SemanticModel<'a>, e: &'e Expr) -> Self
    where
        'a: 'e,
    {
        let mut out = UdfRefs::default();
        Walk {
            model,
            out: &mut out,
            depth: 0,
        }
        .visit_expr(e);
        out
    }

    /// The UDF references of a call: the SQL text the call itself takes
    /// (`selectExpr("f(x)")`, `filter("f(x) > 1")`, `F.expr(..)`, `spark.sql(..)`), then
    /// its arguments (positional, then keywords).
    pub fn of_call<'a>(model: &SemanticModel<'a>, e: &'e Expr) -> Self
    where
        'a: 'e,
    {
        let mut out = UdfRefs::default();
        let Expr::Call(c) = e else { return out };
        for site in model.sql_sites(e) {
            if let Some(q) = &site.text {
                out.refs
                    .extend(registered_calls(model, q).map(|name| UdfRef {
                        call: UdfCall::Registered { name },
                        via_name: false,
                    }));
            }
        }
        let mut w = Walk {
            model,
            out: &mut out,
            depth: 0,
        };
        for a in c
            .arguments
            .args
            .iter()
            .chain(c.arguments.keywords.iter().map(|k| &k.value))
        {
            w.visit_expr(a);
        }
        out
    }

    /// The UDFs registered in this module that SQL text `q` calls (registration
    /// order).
    pub fn of_sql(model: &SemanticModel, q: &SqlText) -> Self {
        UdfRefs {
            refs: registered_calls(model, q)
                .map(|name| UdfRef {
                    call: UdfCall::Registered { name },
                    via_name: false,
                })
                .collect(),
            opaque: false,
        }
    }

    /// Whether any UDF is called.
    pub fn any(&self) -> bool {
        !self.refs.is_empty()
    }

    /// The first reference.
    pub fn first(&self) -> Option<&UdfRef<'e>> {
        self.refs.first()
    }
}

fn registered_calls<'m>(
    model: &'m SemanticModel,
    q: &'m SqlText,
) -> impl Iterator<Item = String> + 'm {
    model
        .registered_udfs()
        .iter()
        .filter(|n| q.calls(n))
        .cloned()
}

struct Walk<'m, 'a, 'o, 'e> {
    model: &'m SemanticModel<'a>,
    out: &'o mut UdfRefs<'e>,
    depth: u32,
}

impl<'m, 'a: 'e, 'o, 'e> Walk<'m, 'a, 'o, 'e> {
    fn push(&mut self, call: UdfCall<'e>) {
        self.out.refs.push(UdfRef {
            call,
            via_name: self.depth > 0,
        });
    }
}

impl<'e, 'a: 'e> Visitor<'e> for Walk<'_, 'a, '_, 'e> {
    fn visit_expr(&mut self, e: &'e Expr) {
        let model = self.model;
        match e {
            Expr::Lambda(_) => return,
            Expr::Call(c) if model.ty_of(&c.func).ty == Ty::Udf => {
                let mut frames = Vec::new();
                for a in c
                    .arguments
                    .args
                    .iter()
                    .chain(c.arguments.keywords.iter().map(|k| &k.value))
                {
                    frames_in(model, a, &mut frames);
                }
                self.push(UdfCall::Python { call: c, frames });
            }
            Expr::Call(_) => {
                if let Some(site) = model
                    .sql_sites(e)
                    .into_iter()
                    .find(|s| s.kind == SqlSiteKind::Expression)
                {
                    match &site.text {
                        Some(q) => {
                            for name in registered_calls(model, q).collect::<Vec<_>>() {
                                self.push(UdfCall::Registered { name });
                            }
                        }
                        None => self.out.opaque = true,
                    }
                }
            }
            Expr::Name(n) if model.ty_of(e).is_column() => {
                match model.single_binding(n).and_then(|b| model.binding(b).value) {
                    Some(v) if self.depth < NAME_HOPS && v.range() != e.range() => {
                        self.depth += 1;
                        self.visit_expr(v);
                        self.depth -= 1;
                    }
                    _ => self.out.opaque = true,
                }
                return;
            }
            _ => {}
        }
        visitor::walk_expr(self, e);
    }
}

/// DataFrame names referenced as `df.col` / `df["col"]` inside `e` (not in lambdas).
fn frames_in(model: &SemanticModel, e: &Expr, out: &mut Vec<String>) {
    struct F<'m, 'a, 'o> {
        model: &'m SemanticModel<'a>,
        out: &'o mut Vec<String>,
    }
    impl<'e> Visitor<'e> for F<'_, '_, '_> {
        fn visit_expr(&mut self, x: &'e Expr) {
            let base = match x {
                Expr::Lambda(_) => return,
                Expr::Attribute(a) => Some(&*a.value),
                Expr::Subscript(s) if str_literal(&s.slice).is_some() => Some(&*s.value),
                _ => None,
            };
            if let Some(base @ Expr::Name(n)) = base
                && self.model.is_dataframe(base)
                && !self.out.iter().any(|o| o == n.id.as_str())
            {
                self.out.push(n.id.to_string());
            }
            visitor::walk_expr(self, x);
        }
    }
    F { model, out }.visit_expr(e);
}

/// Names registered with `spark.udf.register("name", ..)` anywhere in the module
/// (lower-case, source order): computed once when the model is built.
pub(crate) fn registered_names(model: &SemanticModel, module: &ModModule) -> Vec<String> {
    struct V<'m, 'a> {
        model: &'m SemanticModel<'a>,
        out: Vec<String>,
    }
    impl<'b> Visitor<'b> for V<'_, '_> {
        fn visit_expr(&mut self, e: &'b Expr) {
            if let Some(sc) = self.model.spark_call_named(e, &["register"])
                && sc.class() == Ty::UdfRegistration
                && let Some(n) = sc.str_arg(self.model, 0, "name")
            {
                self.out.push(n.to_ascii_lowercase());
            }
            visitor::walk_expr(self, e);
        }
    }
    let mut v = V {
        model,
        out: Vec::new(),
    };
    for s in &module.body {
        v.visit_stmt(s);
    }
    v.out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSummaries, SemanticOptions, build_model};
    use ruff_text_size::TextSize;

    const PRELUDE: &str = "from pyspark.sql import SparkSession\n\
        import pyspark.sql.functions as F\n\
        spark = SparkSession.builder.getOrCreate()\n\
        a = spark.table(\"a\")\n\
        b = spark.table(\"b\")\n\
        def f(x):\n    return x\n\
        my_udf = F.udf(f, \"int\")\n\
        spark.udf.register(\"Plus_One\", f, \"int\")\n\
        spark.udf.register(\"other\", f, \"int\")\n\
        c = my_udf(F.col(\"z\"))\n\
        c2 = c\n\
        c3 = c2\n\
        c4 = c3\n\
        q = \"plus_one(k) > 1\"\n\
        if cond:\n    d = F.col(\"x\")\nelse:\n    d = my_udf(F.col(\"x\"))\n";

    /// Runs `f` on the expression of the last statement of PRELUDE + `body`.
    fn with_expr<R>(body: &str, f: impl for<'x> FnOnce(&SemanticModel<'x>, &'x Expr) -> R) -> R {
        let src = format!("{PRELUDE}{body}\n");
        let m = ruff_python_parser::parse_module(&src)
            .expect("valid test source")
            .into_syntax();
        let model = build_model(
            &m,
            TextSize::of(src.as_str()),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let Some(ruff_python_ast::Stmt::Expr(s)) = m.body.last() else {
            panic!("last statement is not an expression: {body}");
        };
        f(&model, &s.value)
    }

    /// A reference as text: `py:<callee>[frames]`, `reg:<name>`, `~` prefix via a name.
    fn show(src: &str, r: &UdfRef) -> String {
        let s = match &r.call {
            UdfCall::Python { call, frames } => {
                let callee = &src[usize::from(call.func.start())..usize::from(call.func.end())];
                if frames.is_empty() {
                    format!("py:{callee}")
                } else {
                    format!("py:{callee}{frames:?}")
                }
            }
            UdfCall::Registered { name } => format!("reg:{name}"),
        };
        if r.via_name { format!("~{s}") } else { s }
    }

    #[test]
    fn registered_names_once_per_module() {
        with_expr("1", |m, _| {
            assert_eq!(m.registered_udfs(), ["plus_one", "other"])
        });
    }

    #[test]
    fn refs_per_shape() {
        // (source, references, opaque)
        let cases: &[(&str, &[&str], bool)] = &[
            ("my_udf(F.col(\"a\")) > 1", &["py:my_udf"], false),
            ("my_udf(a.x, b[\"y\"])", &["py:my_udf[\"a\", \"b\"]"], false),
            ("my_udf(a.x, a.y)", &["py:my_udf[\"a\"]"], false),
            // nested: outer first
            (
                "my_udf(my_udf(a.x))",
                &["py:my_udf[\"a\"]", "py:my_udf[\"a\"]"],
                false,
            ),
            (
                "my_udf(my_udf(F.col(\"x\")), b.y)",
                &["py:my_udf[\"b\"]", "py:my_udf"],
                false,
            ),
            // SQL text calling a registered UDF (case-insensitive), resolved names
            ("F.expr(\"PLUS_ONE(x) > 1\")", &["reg:plus_one"], false),
            ("F.expr(q)", &["reg:plus_one"], false),
            (
                "F.expr(\"other(plus_one(x))\")",
                &["reg:plus_one", "reg:other"],
                false,
            ),
            ("F.expr(\"upper(x)\")", &[], false),
            ("F.expr(unknown_text)", &[], true),
            // Column variables bound once, up to three hops
            ("c > 1", &["~py:my_udf"], false),
            ("c3 > 1", &["~py:my_udf"], false),
            ("c4 > 1", &[], true),
            ("d > 1", &[], true),
            // built-ins, literals, lambdas
            ("F.upper(F.col(\"a\")) > 1", &[], false),
            ("\"plus_one(x)\"", &[], false),
            ("F.transform(\"xs\", lambda v: my_udf(v))", &[], false),
            (
                "F.when(c > 1, F.expr(\"other(x)\"))",
                &["~py:my_udf", "reg:other"],
                false,
            ),
        ];
        for (src, want, opaque) in cases {
            let full = format!("{PRELUDE}{src}\n");
            with_expr(src, |m, e| {
                let r = UdfRefs::of(m, e);
                let got: Vec<String> = r.refs.iter().map(|x| show(&full, x)).collect();
                assert_eq!(got, *want, "{src}");
                assert_eq!(r.opaque, *opaque, "opaque {src}");
                assert_eq!(r.any(), !want.is_empty(), "any {src}");
            });
        }
    }

    #[test]
    fn refs_of_call_and_sql() {
        // (call source, references): the call's own SQL text, then its arguments only
        let cases: &[(&str, &[&str])] = &[
            (
                "a.withColumn(\"n\", my_udf(a.x)).select(F.expr(\"other(y)\"))",
                &["reg:other"],
            ),
            ("a.withColumn(\"n\", my_udf(a.x))", &["py:my_udf[\"a\"]"]),
            (
                "a.selectExpr(\"plus_one(x) AS y\", \"other(z)\")",
                &["reg:plus_one", "reg:other"],
            ),
            ("a.filter(\"other(x) > 1\")", &["reg:other"]),
            ("a.filter(c)", &["~py:my_udf"]),
            (
                "spark.sql(\"SELECT plus_one(x) FROM t\")",
                &["reg:plus_one"],
            ),
            ("a.select(\"plus_one(x)\")", &[]),
            ("a.groupBy(\"k\").count()", &[]),
        ];
        for (src, want) in cases {
            let full = format!("{PRELUDE}{src}\n");
            with_expr(src, |m, e| {
                let r = UdfRefs::of_call(m, e);
                let got: Vec<String> = r.refs.iter().map(|x| show(&full, x)).collect();
                assert_eq!(got, *want, "{src}");
            });
        }
        with_expr("1", |m, _| {
            let r = UdfRefs::of_sql(m, &SqlText::new("SELECT other(x), plus_one(y) FROM t"));
            let names: Vec<_> = r
                .refs
                .iter()
                .map(|x| match &x.call {
                    UdfCall::Registered { name } => name.as_str(),
                    UdfCall::Python { .. } => "py",
                })
                .collect();
            assert_eq!(names, ["plus_one", "other"]);
        });
    }
}
