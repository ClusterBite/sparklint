//! "Which function does this UDF wrap?" for every UDF rule.
//!
//! [`udf_target`] answers it for a call: `F.udf(fn_or_lambda, ...)` / bare `udf(...)`,
//! `pandas_udf(fn, ...)` and `spark.udf.register("name", fn, ...)`, with the function
//! positionally or as `f=`. [`decorated_udf`] answers it for a def decorated with the
//! factory (`@udf`, `@F.udf("long")`, `@pandas_udf(...)`). [`wrapped_fns`] is the shared
//! name resolution behind both, also used for other function-taking calls
//! (`df.foreach(fn)`, `rdd.map(fn)`, `mapInPandas(fn)`).
//!
//! A name argument is followed through EVERY binding that reaches it (`f = lambda ..` in one
//! branch, `def f` in the other): each binding that is a lambda or a same-file def becomes
//! one [`Wrapped`]; [`Resolved::bindings`] says how many bindings reached the name, so a
//! rule can tell "fully resolved" (`fns.len() == bindings`) from "some paths unknown".

use ruff_python_ast::{self as ast, Expr};
use sparklint_semantic::{BindingId, Ty, call_arg};

use crate::checker::Checker;

/// The function a UDF runs.
#[derive(Clone, Copy, Debug)]
pub(crate) enum UdfFn<'a, 't> {
    Lambda(&'t ast::ExprLambda),
    Def(&'a ast::StmtFunctionDef),
}

/// How the UDF is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shape {
    /// `F.udf(f, ...)`, `udf(lambda ...)`, `pandas_udf(f, ...)`.
    Factory,
    /// `spark.udf.register("name", f, ...)`.
    Register,
    /// `@udf` / `@udf(...)` / `@pandas_udf(...)` on a def.
    Decorator,
}

/// Which factory builds the UDF.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Factory {
    /// `pyspark.sql.functions.udf` (or an unbound `udf` under heuristics), and
    /// `spark.udf.register`.
    Udf,
    /// `pyspark.sql.functions.pandas_udf`.
    PandasUdf,
}

/// One function the argument may be.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Wrapped<'a, 't> {
    pub func: UdfFn<'a, 't>,
    /// The binding this came through (`None` for an inline lambda or a decorated def).
    pub binding: Option<BindingId>,
}

/// The functions an argument expression may be.
#[derive(Clone, Debug, Default)]
pub(crate) struct Resolved<'a, 't> {
    pub fns: Vec<Wrapped<'a, 't>>,
    /// Bindings reaching a name argument (0 for an inline lambda or anything else).
    pub bindings: usize,
}

impl Resolved<'_, '_> {
    /// Every reaching binding resolved to a function.
    pub(crate) fn complete(&self) -> bool {
        !self.fns.is_empty() && (self.bindings == 0 || self.fns.len() == self.bindings)
    }
}

/// What a UDF call or decorator wraps.
#[derive(Clone, Debug)]
pub(crate) struct UdfTarget<'a, 't> {
    pub shape: Shape,
    pub factory: Factory,
    /// The function argument (`None` for a decorator, or a factory call without one:
    /// `udf("int")` is a return type, see the rule).
    pub arg: Option<&'t Expr>,
    /// The function argument was passed by keyword (`f=`).
    pub keyword: bool,
    pub resolved: Resolved<'a, 't>,
    /// The decorator (shape [`Shape::Decorator`]).
    pub decorator: Option<&'a ast::Decorator>,
}

/// The name a message shows: a def's name, a name-bound lambda's name, else `<lambda>`.
pub(crate) fn shown_name(arg: Option<&Expr>, w: &Wrapped) -> String {
    match (w.func, arg) {
        (UdfFn::Def(f), _) => f.name.as_str().to_string(),
        (UdfFn::Lambda(_), Some(Expr::Name(n))) => n.id.as_str().to_string(),
        (UdfFn::Lambda(_), _) => "<lambda>".to_string(),
    }
}

/// `pyspark.sql.functions.udf`, or an unbound `udf` name under heuristics.
pub(crate) fn is_udf_factory(ch: &Checker, func: &Expr) -> bool {
    if matches!(
        ch.model.qualified_name(func).as_deref(),
        Some("pyspark.sql.functions.udf")
    ) {
        return true;
    }
    // An unbound `udf` name (snippets/notebooks that rely on star imports).
    ch.settings.heuristics
        && matches!(func, Expr::Name(n) if n.id.as_str() == "udf"
            && ch.model.use_site(n).is_some_and(|u| u.reaching.is_empty()))
}

/// `pyspark.sql.functions.pandas_udf`.
pub(crate) fn is_pandas_udf(ch: &Checker, e: &Expr) -> bool {
    matches!(
        ch.model.qualified_name(e).as_deref(),
        Some("pyspark.sql.functions.pandas_udf")
    )
}

/// The functions `arg` may be: an inline lambda, or a name followed through every reaching
/// binding to a lambda value or a same-file def.
pub(crate) fn wrapped_fns<'a: 't, 't>(ch: &Checker<'a>, arg: &'t Expr) -> Resolved<'a, 't> {
    match arg {
        Expr::Lambda(l) => Resolved {
            fns: vec![Wrapped {
                func: UdfFn::Lambda(l),
                binding: None,
            }],
            bindings: 0,
        },
        Expr::Name(n) => {
            let reaching = ch.model.reaching(n);
            let fns = reaching
                .iter()
                .filter_map(|&id| {
                    let b = ch.model.binding(id);
                    let func = if let Some(Expr::Lambda(l)) = b.value {
                        UdfFn::Lambda(l)
                    } else {
                        UdfFn::Def(ch.fn_defs.get(&b.range.start()).copied()?)
                    };
                    Some(Wrapped {
                        func,
                        binding: Some(id),
                    })
                })
                .collect();
            Resolved {
                fns,
                bindings: reaching.len(),
            }
        }
        _ => Resolved::default(),
    }
}

/// `F.udf(f)`, `udf(f=lambda ..)`, `pandas_udf(f, ...)`, `spark.udf.register("n", f)` /
/// `register("n", f=..)`: what the call wraps. `None` when `expr` is no such call.
pub(crate) fn udf_target<'a: 't, 't>(
    ch: &Checker<'a>,
    expr: &'t Expr,
) -> Option<UdfTarget<'a, 't>> {
    let Expr::Call(c) = expr else { return None };
    let (shape, factory, index) = if is_udf_factory(ch, &c.func) {
        (Shape::Factory, Factory::Udf, 0)
    } else if is_pandas_udf(ch, &c.func) {
        (Shape::Factory, Factory::PandasUdf, 0)
    } else if ch
        .model
        .spark_call_named(expr, &["register"])
        .is_some_and(|sc| sc.class() == Ty::UdfRegistration)
    {
        (Shape::Register, Factory::Udf, 1)
    } else {
        return None;
    };
    let arg = call_arg(c, index, "f");
    let keyword = arg.is_some_and(|a| {
        c.arguments
            .keywords
            .iter()
            .any(|k| std::ptr::eq(&k.value, a))
    });
    Some(UdfTarget {
        shape,
        factory,
        arg,
        keyword,
        resolved: arg.map(|a| wrapped_fns(ch, a)).unwrap_or_default(),
        decorator: None,
    })
}

/// The first `factory` decorator on `f` (`@udf`, `@F.udf("long")`, `@pandas_udf(..)`).
pub(crate) fn decorated_udf<'a>(
    ch: &Checker<'a>,
    f: &'a ast::StmtFunctionDef,
    factory: Factory,
) -> Option<UdfTarget<'a, 'a>> {
    let dec = f.decorator_list.iter().find(|d| {
        let callee = match &d.expression {
            Expr::Call(c) => &*c.func,
            other => other,
        };
        match factory {
            Factory::Udf => is_udf_factory(ch, callee),
            Factory::PandasUdf => is_pandas_udf(ch, callee),
        }
    })?;
    Some(UdfTarget {
        shape: Shape::Decorator,
        factory,
        arg: None,
        keyword: false,
        resolved: Resolved {
            fns: vec![Wrapped {
                func: UdfFn::Def(f),
                binding: None,
            }],
            bindings: 0,
        },
        decorator: Some(dec),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::testing::with_checker;
    use ruff_python_ast::Stmt;
    use ruff_python_ast::visitor::{self, Visitor};
    use ruff_text_size::Ranged;

    const PRELUDE: &str = "from pyspark.sql import SparkSession\n\
                           from pyspark.sql import functions as F\n\
                           from pyspark.sql.functions import udf, pandas_udf\n\
                           spark = SparkSession.builder.getOrCreate()\n";

    /// (shape, factory, keyword, bindings, [(shown name, lambda?)]) for the
    /// call whose text is `needle`.
    type Row = (Shape, Factory, bool, usize, Vec<(String, bool)>);

    fn target_of(body: &str, needle: &str) -> Option<Row> {
        let src = format!("{PRELUDE}{body}");
        with_checker(&src, &["ALL"], |ch| {
            struct V<'s, 'b> {
                src: &'s str,
                needle: &'s str,
                found: Option<&'b Expr>,
            }
            impl<'b> Visitor<'b> for V<'_, 'b> {
                fn visit_expr(&mut self, e: &'b Expr) {
                    if self.found.is_none()
                        && matches!(e, Expr::Call(_))
                        && &self.src[e.range()] == self.needle
                    {
                        self.found = Some(e);
                    }
                    visitor::walk_expr(self, e);
                }
            }
            let mut v = V {
                src: &src,
                needle,
                found: None,
            };
            v.visit_body(&ch.module.body);
            let e = v.found.unwrap_or_else(|| panic!("no call `{needle}`"));
            udf_target(ch, e).map(|t| {
                let fns = t
                    .resolved
                    .fns
                    .iter()
                    .map(|w| (shown_name(t.arg, w), matches!(w.func, UdfFn::Lambda(_))))
                    .collect();
                (t.shape, t.factory, t.keyword, t.resolved.bindings, fns)
            })
        })
    }

    fn fns(v: &[(&str, bool)]) -> Vec<(String, bool)> {
        v.iter().map(|&(n, l)| (n.to_string(), l)).collect()
    }

    #[test]
    fn udf_target_table() {
        use Factory::{PandasUdf, Udf};
        use Shape::Register;
        let cases: &[(&str, &str, Option<Row>)] = &[
            // positional inline lambda
            (
                "u = F.udf(lambda x: x + 1)\n",
                "F.udf(lambda x: x + 1)",
                Some((Shape::Factory, Udf, false, 0, fns(&[("<lambda>", true)]))),
            ),
            // f= keyword
            (
                "u = udf(f=lambda x: x, returnType='int')\n",
                "udf(f=lambda x: x, returnType='int')",
                Some((Shape::Factory, Udf, true, 0, fns(&[("<lambda>", true)]))),
            ),
            // a def by name
            (
                "def g(x):\n    return x\nu = F.udf(g, 'int')\n",
                "F.udf(g, 'int')",
                Some((Shape::Factory, Udf, false, 1, fns(&[("g", false)]))),
            ),
            // a lambda bound to a name shows that name
            (
                "h = lambda x: x\nu = F.udf(h)\n",
                "F.udf(h)",
                Some((Shape::Factory, Udf, false, 1, fns(&[("h", true)]))),
            ),
            // every reaching binding: if/else
            (
                "if c:\n    h = lambda x: x\nelse:\n    def h(x):\n        return x\nu = F.udf(h)\n",
                "F.udf(h)",
                Some((
                    Shape::Factory,
                    Udf,
                    false,
                    2,
                    fns(&[("h", true), ("h", false)]),
                )),
            ),
            // a binding that is neither: counted, not resolved
            (
                "if c:\n    h = lambda x: x\nelse:\n    h = make()\nu = F.udf(h)\n",
                "F.udf(h)",
                Some((Shape::Factory, Udf, false, 2, fns(&[("h", true)]))),
            ),
            // register(name, f)
            (
                "def g(x):\n    return x\nspark.udf.register('g', g)\n",
                "spark.udf.register('g', g)",
                Some((Register, Udf, false, 1, fns(&[("g", false)]))),
            ),
            // register(name, f=...)
            (
                "spark.udf.register('g', f=lambda x: x)\n",
                "spark.udf.register('g', f=lambda x: x)",
                Some((Register, Udf, true, 0, fns(&[("<lambda>", true)]))),
            ),
            // pandas_udf
            (
                "def g(s):\n    return s\nu = pandas_udf(g, 'long')\n",
                "pandas_udf(g, 'long')",
                Some((Shape::Factory, PandasUdf, false, 1, fns(&[("g", false)]))),
            ),
            // a factory call without a function (decorator factory): no fns
            (
                "u = udf('int')\n",
                "udf('int')",
                Some((Shape::Factory, Udf, false, 0, vec![])),
            ),
            // starred before the function: unknown
            (
                "u = F.udf(*args)\n",
                "F.udf(*args)",
                Some((Shape::Factory, Udf, false, 0, vec![])),
            ),
            // not a UDF call
            ("u = len(lambda x: x)\n", "len(lambda x: x)", None),
            (
                "xs.register('a', lambda x: x)\n",
                "xs.register('a', lambda x: x)",
                None,
            ),
        ];
        for (body, needle, want) in cases {
            assert_eq!(&target_of(body, needle), want, "{body}");
        }
    }

    #[test]
    fn resolved_complete() {
        let src = format!(
            "{PRELUDE}if c:\n    h = lambda x: x\nelse:\n    h = make()\nu = F.udf(h)\nk = lambda x: x\nv = F.udf(k)\n"
        );
        with_checker(&src, &["ALL"], |ch| {
            let Stmt::Assign(u) = &ch.module.body[ch.module.body.len() - 3] else {
                panic!()
            };
            let t = udf_target(ch, &u.value).unwrap();
            assert!(!t.resolved.complete());
            let Stmt::Assign(v) = ch.module.body.last().unwrap() else {
                panic!()
            };
            let t = udf_target(ch, &v.value).unwrap();
            assert!(t.resolved.complete());
        });
    }

    #[test]
    fn decorator_table() {
        let src = format!(
            "{PRELUDE}@udf\ndef a(x):\n    return x\n@F.udf('long')\ndef b(x):\n    return x\n@pandas_udf('long')\ndef c(s):\n    return s\ndef d(x):\n    return x\n"
        );
        with_checker(&src, &["ALL"], |ch| {
            let defs: Vec<&ast::StmtFunctionDef> = ch
                .module
                .body
                .iter()
                .filter_map(|s| s.as_function_def_stmt())
                .collect();
            let got: Vec<(bool, bool)> = defs
                .iter()
                .map(|f| {
                    (
                        decorated_udf(ch, f, Factory::Udf).is_some(),
                        decorated_udf(ch, f, Factory::PandasUdf).is_some(),
                    )
                })
                .collect();
            assert_eq!(
                got,
                vec![(true, false), (true, false), (false, true), (false, false)]
            );
            let t = decorated_udf(ch, defs[1], Factory::Udf).unwrap();
            assert_eq!(t.shape, Shape::Decorator);
            assert_eq!(shown_name(t.arg, &t.resolved.fns[0]), "b");
        });
    }
}
