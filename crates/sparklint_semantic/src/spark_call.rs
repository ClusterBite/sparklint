//! Spark method calls answered by the oracle.
//!
//! [`SemanticModel::spark_call`] resolves `recv.method(...)` to a qualified Spark method
//! (`pyspark.sql.DataFrame.count`) plus the receiver's [`TypeInfo`] (confidence and
//! facts such as `bounded`). It answers `None` unless the receiver is a value the
//! oracle places in the Spark universe ([`Ty::is_spark`]). That is the invariant behind
//! the ambiguous methods: `count`, `filter`, `collect`, `first`, `join`, `select`,
//! `show`, `limit`, `distinct` on a list, a pandas frame, a Py4J handle
//! (`self._jdf.count()`) or an unknown value are never Spark calls.
//!
//! Confidence is exposed, not hidden: a heuristic (`Likely`) receiver such as a
//! parameter named `df` is a Spark call with `confidence == Likely`; rules that need
//! proof ask [`SparkCall::is_certain`].
//!
//! Arguments are read through the call with ONE policy ([`SparkCall::arg`] and the typed
//! `str_arg` / `int_arg` / `num_arg`): the keyword wins, else the position, and a starred
//! argument at or before the position makes it unknown. Typed reads go through the one
//! constant reader ([`SemanticModel::constant`], `crate::constant`): names are followed
//! through EVERY binding that reaches them (`if a: N = 8 else: N = 8` is 8; branches
//! that disagree are no constant). [`SparkCall::given`] is the argument as pyspark sees
//! it: `None` on every path is not given.

use ruff_python_ast::{self as ast, Expr};

use crate::constant::Const;
use crate::infer::call_arg;
use crate::model::SemanticModel;
use crate::types::{Confidence, Ty, TypeInfo};

/// A method call on a Spark value.
#[derive(Debug, Clone, Copy)]
pub struct SparkCall<'e> {
    pub call: &'e ast::ExprCall,
    /// The receiver expression (`df` in `df.count()`).
    pub receiver: &'e Expr,
    /// The method name (`count`).
    pub method: &'e str,
    /// The receiver's type, confidence and facts.
    pub receiver_ty: TypeInfo,
}

impl<'e> SparkCall<'e> {
    /// The receiver's Spark type (`Ty::DataFrame` for `df.count()`).
    pub fn class(&self) -> Ty {
        self.receiver_ty.ty
    }

    /// `pyspark.sql.DataFrame.count`.
    pub fn qualname(&self) -> String {
        format!(
            "{}.{}",
            self.class().qualified_class().unwrap_or("pyspark"),
            self.method
        )
    }

    /// `Class.method` on a receiver of type `ty` (any confidence).
    pub fn is(&self, ty: Ty, method: &str) -> bool {
        self.class() == ty && self.method == method
    }

    /// One of `methods` on a receiver of type `ty` (any confidence).
    pub fn is_any(&self, ty: Ty, methods: &[&str]) -> bool {
        self.class() == ty && methods.contains(&self.method)
    }

    /// A method of `pyspark.sql.DataFrame` (any confidence).
    pub fn is_dataframe(&self) -> bool {
        self.class() == Ty::DataFrame
    }

    pub fn confidence(&self) -> Confidence {
        self.receiver_ty.confidence
    }

    /// The receiver type is proven (annotation, import, known origin), not heuristic.
    pub fn is_certain(&self) -> bool {
        self.receiver_ty.is_certain()
    }

    /// The receiver has at most this many rows (`limit(n)`, aggregate-only SQL, …).
    pub fn bounded(&self) -> Option<u64> {
        self.receiver_ty.bounded()
    }

    /// How big the receiver is (see [`crate::Size`]).
    pub fn size(&self) -> crate::Size {
        self.receiver_ty.size()
    }

    /// The receiver fits in driver memory: `self.size().fits_driver()`.
    pub fn driver_sized(&self) -> bool {
        self.receiver_ty.driver_sized()
    }

    /// Positional arguments.
    pub fn args(&self) -> &'e [Expr] {
        &self.call.arguments.args
    }

    /// Called with no positional and no keyword arguments.
    pub fn has_no_args(&self) -> bool {
        self.call.arguments.args.is_empty() && self.call.arguments.keywords.is_empty()
    }

    /// Argument `kw=`, else positional argument `i`; `None` when a starred argument comes
    /// at or before position `i` (its position is unknown).
    pub fn arg(&self, i: usize, kw: &str) -> Option<&'e Expr> {
        call_arg(self.call, i, kw)
    }

    /// [`Self::arg`] as given: `None` when it is missing or `None` on every path
    /// (pyspark's `if x is not None:` default handling).
    pub fn given<'a: 'e>(&self, model: &SemanticModel<'a>, i: usize, kw: &str) -> Option<&'e Expr> {
        model.given(self.arg(i, kw)?)
    }

    /// [`Self::arg`] as a constant ([`SemanticModel::constant`]).
    pub fn const_arg<'a: 'e>(
        &self,
        model: &SemanticModel<'a>,
        i: usize,
        kw: &str,
    ) -> Option<Const<'e>> {
        model.constant(self.arg(i, kw)?)
    }

    /// [`Self::arg`] as a string constant (a literal, or a name bound to one).
    pub fn str_arg<'a: 'e>(
        &self,
        model: &SemanticModel<'a>,
        i: usize,
        kw: &str,
    ) -> Option<&'e str> {
        self.const_arg(model, i, kw)?.as_str()
    }

    /// [`Self::arg`] as an integer constant (`10 * 1024`, `2 ** 30`, a name bound to one).
    pub fn int_arg<'a: 'e>(&self, model: &SemanticModel<'a>, i: usize, kw: &str) -> Option<i128> {
        self.const_arg(model, i, kw)?.as_int()
    }

    /// [`Self::arg`] as a number constant (`0`, `0.0`, `-0.5`, a name bound to one).
    pub fn num_arg<'a: 'e>(&self, model: &SemanticModel<'a>, i: usize, kw: &str) -> Option<f64> {
        self.const_arg(model, i, kw)?.as_num()
    }
}

/// `recv.method(...)` → (call, receiver, method), Spark or not.
pub(crate) fn attribute_call(e: &Expr) -> Option<(&ast::ExprCall, &Expr, &str)> {
    let Expr::Call(c) = e else { return None };
    let Expr::Attribute(a) = &*c.func else {
        return None;
    };
    Some((c, &a.value, a.attr.as_str()))
}

impl SemanticModel<'_> {
    /// `e` as a method call on a Spark receiver; `None` for non-calls, plain function
    /// calls and calls on non-Spark or unknown receivers.
    pub fn spark_call<'e>(&self, e: &'e Expr) -> Option<SparkCall<'e>> {
        let (call, receiver, method) = attribute_call(e)?;
        self.resolve(call, receiver, method)
    }

    /// [`Self::spark_call`] restricted to the given method names (checked before the
    /// receiver is typed, so rules pay for the oracle only on candidate calls).
    pub fn spark_call_named<'e>(&self, e: &'e Expr, methods: &[&str]) -> Option<SparkCall<'e>> {
        let (call, receiver, method) = attribute_call(e)?;
        if !methods.contains(&method) {
            return None;
        }
        self.resolve(call, receiver, method)
    }

    fn resolve<'e>(
        &self,
        call: &'e ast::ExprCall,
        receiver: &'e Expr,
        method: &'e str,
    ) -> Option<SparkCall<'e>> {
        let receiver_ty = self.ty_of(receiver);
        receiver_ty.ty.is_spark().then_some(SparkCall {
            call,
            receiver,
            method,
            receiver_ty,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant::CONST_DEPTH;
    use crate::{NoSummaries, SemanticOptions, build_model};
    use ruff_python_ast::visitor::{self, Visitor};
    use ruff_python_ast::{Expr, ModModule};
    use ruff_text_size::{Ranged, TextSize};

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

    /// (qualname, confidence, bounded) of the call whose source text is `needle`.
    fn resolve(src: &str, needle: &str) -> Option<(String, Confidence, Option<u64>)> {
        resolve_with(src, needle, |m, e| m.spark_call(e))
    }

    fn resolve_with(
        src: &str,
        needle: &str,
        f: impl for<'e> Fn(&SemanticModel<'_>, &'e Expr) -> Option<SparkCall<'e>>,
    ) -> Option<(String, Confidence, Option<u64>)> {
        let m = parse(src);
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let mut v = Calls {
            src,
            found: Vec::new(),
        };
        v.visit_body(&m.body);
        let (_, e) = v
            .found
            .iter()
            .find(|(s, _)| *s == needle)
            .unwrap_or_else(|| panic!("no call `{needle}`"));
        f(&model, e).map(|c| (c.qualname(), c.confidence(), c.bounded()))
    }

    const PRELUDE: &str = "from pyspark.sql import SparkSession, DataFrame\n\
                           spark = SparkSession.builder.getOrCreate()\n";

    #[test]
    fn known_dataframe_methods_resolve_certain() {
        let src = format!("{PRELUDE}df = spark.table('t')\ndf.count()\ndf.limit(5).collect()\n");
        let (q, c, b) = resolve(&src, "df.count()").unwrap();
        assert_eq!(q, "pyspark.sql.DataFrame.count");
        assert_eq!(c, Confidence::Certain);
        assert_eq!(b, None);
        let (q, _, b) = resolve(&src, "df.limit(5).collect()").unwrap();
        assert_eq!(q, "pyspark.sql.DataFrame.collect");
        assert_eq!(b, Some(5));
    }

    #[test]
    fn heuristic_receivers_are_likely() {
        let src = "def f(df):\n    return df.count()\n";
        let (q, c, _) = resolve(src, "df.count()").unwrap();
        assert_eq!(q, "pyspark.sql.DataFrame.count");
        assert_eq!(c, Confidence::Likely);
    }

    #[test]
    fn ambiguous_methods_on_non_spark_receivers_are_not_spark_calls() {
        let src = format!(
            "{PRELUDE}import pandas as pd\n\
             xs = [1, 2, 2]\n\
             xs.count(2)\n\
             pdf = pd.DataFrame({{'a': [1]}})\n\
             pdf.filter(items=['a'])\n\
             pdf.join(pdf)\n\
             'a,b'.join(['x'])\n\
             s = {{1}}\n\
             s.union({{2}})\n\
             thing.collect()\n"
        );
        for needle in [
            "xs.count(2)",
            "pdf.filter(items=['a'])",
            "pdf.join(pdf)",
            "'a,b'.join(['x'])",
            "s.union({2})",
            "thing.collect()",
        ] {
            assert_eq!(resolve(&src, needle), None, "{needle}");
        }
    }

    #[test]
    fn py4j_handles_are_not_spark() {
        let src = format!(
            "{PRELUDE}class W:\n    def __init__(self, df: DataFrame):\n        self._jdf = df._jdf\n\n    def n(self):\n        return self._jdf.count()\n"
        );
        assert_eq!(resolve(&src, "self._jdf.count()"), None);
    }

    #[test]
    fn other_spark_classes_resolve() {
        let src = format!(
            "{PRELUDE}df = spark.table('t')\n\
             df.groupBy('k').count()\n\
             df.rdd.collect()\n\
             spark.sql('select 1')\n\
             df.write.saveAsTable('x')\n"
        );
        assert_eq!(
            resolve(&src, "df.groupBy('k').count()").unwrap().0,
            "pyspark.sql.GroupedData.count"
        );
        assert_eq!(
            resolve(&src, "df.rdd.collect()").unwrap().0,
            "pyspark.RDD.collect"
        );
        assert_eq!(
            resolve(&src, "spark.sql('select 1')").unwrap().0,
            "pyspark.sql.SparkSession.sql"
        );
        assert_eq!(
            resolve(&src, "df.write.saveAsTable('x')").unwrap().0,
            "pyspark.sql.DataFrameWriter.saveAsTable"
        );
    }

    #[test]
    fn named_filter_checks_the_method_first() {
        let src = format!("{PRELUDE}df = spark.table('t')\ndf.count()\ndf.show()\n");
        let named = |needle| {
            resolve_with(&src, needle, |m, e| {
                m.spark_call_named(e, &["count", "first"])
            })
        };
        assert!(named("df.count()").is_some());
        assert!(named("df.show()").is_none());
    }

    /// Typed argument reads of the call whose text is `needle`: (arg text, str, int, num).
    fn args_of(
        src: &str,
        needle: &str,
        i: usize,
        kw: &str,
    ) -> (Option<String>, Option<String>, Option<i128>, Option<f64>) {
        let m = parse(src);
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let mut v = Calls {
            src,
            found: Vec::new(),
        };
        v.visit_body(&m.body);
        let (_, e) = v
            .found
            .iter()
            .find(|(s, _)| *s == needle)
            .unwrap_or_else(|| panic!("no call `{needle}`"));
        let sc = model.spark_call(e).expect("a Spark call");
        (
            sc.arg(i, kw).map(|a| src[a.range()].to_string()),
            sc.str_arg(&model, i, kw).map(str::to_string),
            sc.int_arg(&model, i, kw),
            sc.num_arg(&model, i, kw),
        )
    }

    #[test]
    fn argument_shapes() {
        let pre = format!("{PRELUDE}df = spark.table('t')\n");
        type Want = (
            Option<&'static str>,
            Option<&'static str>,
            Option<i128>,
            Option<f64>,
        );
        let cases: &[(&str, &str, usize, &str, Want)] = &[
            // positional
            (
                "df.repartition(8)\n",
                "df.repartition(8)",
                0,
                "numPartitions",
                (Some("8"), None, Some(8), Some(8.0)),
            ),
            // keyword wins over position
            (
                "df.repartition(numPartitions=4)\n",
                "df.repartition(numPartitions=4)",
                0,
                "numPartitions",
                (Some("4"), None, Some(4), Some(4.0)),
            ),
            // missing
            (
                "df.repartition()\n",
                "df.repartition()",
                0,
                "numPartitions",
                (None, None, None, None),
            ),
            // starred at or before the position: unknown
            (
                "df.repartition(*xs)\n",
                "df.repartition(*xs)",
                0,
                "numPartitions",
                (None, None, None, None),
            ),
            (
                "df.join(*xs, 'k')\n",
                "df.join(*xs, 'k')",
                1,
                "on",
                (None, None, None, None),
            ),
            // starred after the position does not matter
            (
                "df.join(o, 'k', *xs)\n",
                "df.join(o, 'k', *xs)",
                1,
                "on",
                (Some("'k'"), Some("k"), None, None),
            ),
            // string literal and a name bound to one
            (
                "df.join(o, 'k')\n",
                "df.join(o, 'k')",
                1,
                "on",
                (Some("'k'"), Some("k"), None, None),
            ),
            (
                "KEY = 'id'\ndf.join(o, KEY)\n",
                "df.join(o, KEY)",
                1,
                "on",
                (Some("KEY"), Some("id"), None, None),
            ),
            // integer arithmetic, through names
            (
                "N = 2 ** 10\nM = N * 4\ndf.repartition(M)\n",
                "df.repartition(M)",
                0,
                "numPartitions",
                (Some("M"), None, Some(4096), Some(4096.0)),
            ),
            (
                "df.repartition(1 << 4)\n",
                "df.repartition(1 << 4)",
                0,
                "numPartitions",
                (Some("1 << 4"), None, Some(16), Some(16.0)),
            ),
            (
                "df.repartition(-1)\n",
                "df.repartition(-1)",
                0,
                "numPartitions",
                (Some("-1"), None, Some(-1), Some(-1.0)),
            ),
            // floats
            (
                "ERR = 0.0\ndf.approxQuantile('a', [0.5], ERR)\n",
                "df.approxQuantile('a', [0.5], ERR)",
                2,
                "relativeError",
                (Some("ERR"), None, None, Some(0.0)),
            ),
            (
                "df.approxQuantile('a', [0.5], relativeError=-0.0)\n",
                "df.approxQuantile('a', [0.5], relativeError=-0.0)",
                2,
                "relativeError",
                (Some("-0.0"), None, None, Some(-0.0)),
            ),
            // every reaching binding: branches that agree are a constant, others are not
            (
                "if c:\n    N = 4\nelse:\n    N = 8\ndf.repartition(N)\n",
                "df.repartition(N)",
                0,
                "numPartitions",
                (Some("N"), None, None, None),
            ),
            (
                "if c:\n    N = 8\nelse:\n    N = 2 * 4\ndf.repartition(N)\n",
                "df.repartition(N)",
                0,
                "numPartitions",
                (Some("N"), None, Some(8), Some(8.0)),
            ),
            (
                "if c:\n    K = 'id'\nelse:\n    K = 'id'\ndf.join(o, K)\n",
                "df.join(o, K)",
                1,
                "on",
                (Some("K"), Some("id"), None, None),
            ),
            (
                "if c:\n    K = 'id'\nelse:\n    K = 'key'\ndf.join(o, K)\n",
                "df.join(o, K)",
                1,
                "on",
                (Some("K"), None, None, None),
            ),
            // one path without a constant value (the parameter)
            (
                "def f(N=4):\n    if c:\n        N = 4\n    return df.repartition(N)\n",
                "df.repartition(N)",
                0,
                "numPartitions",
                (Some("N"), None, None, None),
            ),
            // a non-constant value
            (
                "n = len(xs)\ndf.repartition(n)\n",
                "df.repartition(n)",
                0,
                "numPartitions",
                (Some("n"), None, None, None),
            ),
        ];
        for (body, needle, i, kw, want) in cases {
            let src = format!("{pre}{body}");
            let got = args_of(&src, needle, *i, kw);
            let want = (
                want.0.map(str::to_string),
                want.1.map(str::to_string),
                want.2,
                want.3,
            );
            assert_eq!(got, want, "{body}");
        }
    }

    #[test]
    fn const_depth_is_bounded() {
        // a chain longer than CONST_DEPTH names is not followed
        let mut body = String::from("A0 = 1\n");
        for k in 1..=CONST_DEPTH + 1 {
            body.push_str(&format!("A{k} = A{}\n", k - 1));
        }
        let last = format!("A{}", CONST_DEPTH + 1);
        let src = format!("{PRELUDE}df = spark.table('t')\n{body}df.repartition({last})\n");
        let needle = format!("df.repartition({last})");
        assert_eq!(args_of(&src, &needle, 0, "numPartitions").2, None);
        let short =
            format!("{PRELUDE}df = spark.table('t')\nA0 = 1\nA1 = A0\ndf.repartition(A1)\n");
        assert_eq!(
            args_of(&short, "df.repartition(A1)", 0, "numPartitions").2,
            Some(1)
        );
    }

    #[test]
    fn plain_function_calls_are_not_method_calls() {
        let src = format!("{PRELUDE}df = spark.table('t')\nlen(df.collect())\n");
        assert_eq!(resolve(&src, "len(df.collect())"), None);
    }
}
