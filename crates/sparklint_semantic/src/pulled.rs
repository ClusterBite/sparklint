//! "Does this expression hold rows pulled from Spark, and how did they get here?"
//!
//! A `collect()` / `toLocalIterator()` / `toPandas()` result travels through Python
//! before a rule sees it: a name, an index or a slice, a comprehension over the rows,
//! `list(..)` / `sorted(..)`, a pandas column and its `.tolist()`, `iterrows()`.
//! [`SemanticModel::pulled_rows`] follows an expression back to the pull and answers
//! with one [`Pulled`] per way the rows got there:
//!
//! - the [`Pull`]: the pull call (also the overlap site of the rules that report the
//!   rows), the pulled frame, its receiver type, the classifier's [`Run`] (one pull
//!   list: [`crate::call_run`]'s unbounded driver pulls) and the [`Size`] verdict of
//!   the pulled frame;
//! - the [`PathStep`]s the rows took, outermost first, with the expression each step
//!   produced;
//! - what the expression [`Holds`] now (a row list, a row iterator, one row, values
//!   derived per row, a local frame, a pandas column).
//!
//! When the pull is out of sight (a helper's return value), the type oracle's
//! `from_spark` list / pandas facts still say the value came from Spark: a [`Pulled`]
//! without a pull, sized by the oracle type.
//!
//! Names are followed through every reaching binding (up to [`NAME_HOPS`] names
//! deep); a comprehension follows its first generator; wraps follow every positional
//! argument. A step that makes no sense for what it is applied to (`len` of an
//! iterator is the rule's call, but `rows.tolist()` or `[0]` on an iterator is not a
//! path) drops that way.
//!
//! ACT004 (row iteration), ACT008 (collect fed back into a filter), ACT009 (driver
//! compute over a collect) keep only their sink patterns and wording; other callers ask
//! [`SemanticModel::pull_call`] for the pull list.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::Ranged;

use crate::call_run::{Recv, Returns, Run};
use crate::constant::int_literal;
use crate::model::{BindingId, SemanticModel};
use crate::size::Size;
use crate::spark_call::attribute_call;
use crate::types::{Elem, Ty};

/// What an expression holds after the rows' path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Holds {
    /// A list of rows (`df.collect()`, `rows[1:]`, `list(it)`).
    RowList,
    /// Rows to iterate once (`df.toLocalIterator()`, `pdf.iterrows()`,
    /// `enumerate(rows)`): not indexable, no `len()`.
    RowIter,
    /// One row (`df.collect()[0]`).
    Row,
    /// Values derived from the rows (`[r.id for r in rows]`, `pdf["id"].tolist()`,
    /// `rdd.collectAsMap()`).
    Values,
    /// A pandas frame on the driver (`df.toPandas()`).
    PandasFrame,
    /// Another driver-side frame or array (`df.toArrow()`, `psdf.to_numpy()`).
    OtherFrame,
    /// One column (or a derived frame view) of a pandas frame (`pdf["id"]`, `pdf.id`).
    Series,
}

impl Holds {
    /// Several rows (a list or an iterator of them).
    pub fn rows(self) -> bool {
        matches!(self, Holds::RowList | Holds::RowIter)
    }

    /// A collection of pulled values a Python loop / builtin walks element by element:
    /// rows or values derived from them.
    pub fn values(self) -> bool {
        matches!(self, Holds::RowList | Holds::RowIter | Holds::Values)
    }

    /// A driver-side frame (pandas, Arrow, numpy).
    pub fn frame(self) -> bool {
        matches!(self, Holds::PandasFrame | Holds::OtherFrame)
    }
}

/// One step of the rows' path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathStep {
    /// `rows[0]`, `pdf[0]`.
    Index,
    /// `rows[1:]`, `rows[:n]`.
    Slice,
    /// `[f(r) for r in rows]` (the first generator's iterable).
    Comprehension,
    /// A name, through the binding followed.
    Name(BindingId),
    /// `list(..)`, `sorted(..)`, `set(..)`, `tuple(..)`, `reversed(..)`, `enumerate(..)`,
    /// `zip(..)`.
    Wrap,
    /// A pandas column or frame operation (`pdf["id"]`, `pdf.id`, `pdf.dropna()`).
    Pandas,
    /// A column's values (`.tolist()`, `.to_list()`, `.unique()`).
    ColumnValues,
    /// One item per row of a local frame (`pdf.iterrows()`, `itertuples()`, `items()`,
    /// `to_dict("records")`, `table.to_pylist()`).
    FrameRows,
}

/// A pull of rows to the driver.
#[derive(Debug, Clone, Copy)]
pub struct Pull<'e> {
    /// The pull call (`df.collect()`): the overlap site.
    pub call: &'e Expr,
    /// The pulled frame (the call's receiver).
    pub frame: &'e Expr,
    /// The receiver type (`DataFrame`, `RDD`, pandas-on-Spark frame).
    pub recv: Ty,
    /// The method as written (`collect`, `toPandas`).
    pub method: &'static str,
    /// The classifier's answer for the call.
    pub run: Run,
    /// The size verdict of the pulled frame.
    pub size: Size,
}

/// One way rows pulled from Spark reach an expression.
#[derive(Debug, Clone)]
pub struct Pulled<'e> {
    /// The pull; `None` when only the type oracle knows the value came from Spark.
    pub pull: Option<Pull<'e>>,
    /// The steps from the expression asked about back to the pull, outermost first,
    /// each with the expression it produced.
    pub path: Vec<(PathStep, &'e Expr)>,
    /// The innermost expression: the pull call, or the expression the oracle typed.
    pub at: &'e Expr,
    /// What the expression holds.
    pub holds: Holds,
    /// The size verdict of the pulled frame (the oracle type's, without a pull).
    pub size: Size,
}

impl<'e> Pulled<'e> {
    /// The pulled frame fits in driver memory (see [`Size::fits_driver`]).
    pub fn fits_driver(&self) -> bool {
        self.size.fits_driver()
    }

    /// The expression is the pull call itself.
    pub fn direct(&self) -> bool {
        self.path.is_empty() && self.pull.is_some()
    }

    /// The expression step `i` of [`Self::path`] was applied to.
    pub fn inner(&self, i: usize) -> &'e Expr {
        self.path.get(i + 1).map_or(self.at, |(_, e)| e)
    }

    /// The bindings followed on the way, outermost first.
    pub fn bindings(&self) -> impl Iterator<Item = BindingId> + '_ {
        self.path.iter().filter_map(|(s, _)| match s {
            PathStep::Name(b) => Some(*b),
            _ => None,
        })
    }

    /// Whether the path has a step of this kind (`PathStep::Name(_)` matches any name).
    pub fn has(&self, step: PathStep) -> bool {
        self.path.iter().any(|(s, _)| match (s, step) {
            (PathStep::Name(_), PathStep::Name(_)) => true,
            (a, b) => *a == b,
        })
    }
}

/// The ONE pull list: an unbounded pull of rows (or a local frame) to the driver.
pub fn pulls_rows(run: Run) -> bool {
    run.unbounded_pull()
}

/// A method of `ty` that pulls rows (for receivers the oracle cannot type).
pub fn pull_method(ty: Ty, method: &str) -> bool {
    crate::call_run::run_of_ty(ty, method).is_some_and(pulls_rows)
}

/// How many names deep a value is followed (`rows = df.collect()`; `ids = rows`).
pub const NAME_HOPS: u32 = crate::values::NAME_HOPS;
/// How many steps deep the walk goes.
const MAX_DEPTH: u32 = 16;

/// Builtins that pass the rows of their argument(s) through.
const WRAPS: &[&str] = &[
    "builtins.list",
    "builtins.set",
    "builtins.frozenset",
    "builtins.sorted",
    "builtins.tuple",
    "builtins.reversed",
    "builtins.enumerate",
    "builtins.zip",
];
/// `pdf.iterrows()`-like methods: one item per row of a pandas frame.
const FRAME_ROWS: &[&str] = &["iterrows", "itertuples", "items"];
/// Pandas frame attributes that describe the frame, not its rows.
const FRAME_METADATA: &[&str] = &[
    "columns", "dtypes", "shape", "ndim", "size", "empty", "axes",
];
/// A pandas column's values.
const COLUMN_VALUES: &[&str] = &["tolist", "to_list", "unique"];

/// `orient="records"` (positional or keyword) on a `to_dict` call.
fn records_orient(c: &ast::ExprCall) -> bool {
    let arg = c
        .arguments
        .args
        .first()
        .or_else(|| c.arguments.find_keyword("orient").map(|k| &k.value));
    matches!(arg, Some(Expr::StringLiteral(s)) if s.value.to_str() == "records")
}

/// What a step makes of the rows it is applied to (`None`: not a path); `pairs` = the
/// wrap is `enumerate` / `zip`.
fn after(step: PathStep, inner: Holds, e: &Expr, pairs: bool) -> Option<Holds> {
    use Holds::*;
    match step {
        PathStep::Name(_) => Some(inner),
        PathStep::Comprehension => {
            matches!(inner, RowList | RowIter | Values | Series).then_some(Values)
        }
        PathStep::Wrap => match inner {
            RowList | RowIter if pairs => Some(RowIter),
            RowList | RowIter => Some(RowList),
            Values | Series => Some(Values),
            _ => None,
        },
        PathStep::Index => match inner {
            RowList => Some(Row),
            PandasFrame | Series => Some(Series),
            _ => None,
        },
        PathStep::Slice => {
            matches!(inner, RowList | Values | PandasFrame | Series).then_some(inner)
        }
        PathStep::Pandas => match (inner, e) {
            (PandasFrame, Expr::Subscript(s)) if matches!(&*s.slice, Expr::List(_)) => {
                Some(PandasFrame)
            }
            (PandasFrame, Expr::Attribute(a)) if FRAME_METADATA.contains(&a.attr.as_str()) => None,
            // A method result (`pdf.dropna()`, `pdf.to_dict()`) has an unknown shape: only
            // its values / columns are followed.
            (PandasFrame | Series, _) => Some(Series),
            _ => None,
        },
        PathStep::ColumnValues => {
            matches!(inner, PandasFrame | Series | OtherFrame).then_some(Values)
        }
        PathStep::FrameRows => match (inner, method_name(e)) {
            (OtherFrame, Some("to_pylist")) => Some(RowList),
            (PandasFrame, Some("to_dict")) => Some(RowList),
            (PandasFrame, Some(m)) if FRAME_ROWS.contains(&m) => Some(RowIter),
            _ => None,
        },
    }
}

fn method_name(e: &Expr) -> Option<&str> {
    match e {
        Expr::Call(c) => match &*c.func {
            Expr::Attribute(a) => Some(a.attr.as_str()),
            _ => None,
        },
        _ => None,
    }
}

impl<'a> SemanticModel<'a> {
    /// `e` as a pull of rows to the driver (see [`pulls_rows`]): a DataFrame, RDD or
    /// pandas-on-Spark call that pulls the whole result.
    pub fn pull_call<'e>(&self, e: &'e Expr) -> Option<Pull<'e>>
    where
        'a: 'e,
    {
        let run = self.call_run(e)?;
        let Recv::Ty(recv @ (Ty::DataFrame | Ty::Rdd | Ty::PandasOnSparkFrame)) = run.recv else {
            return None;
        };
        if !pulls_rows(run.run) {
            return None;
        }
        let sc = run.spark?;
        Some(Pull {
            call: e,
            frame: sc.receiver,
            recv,
            method: run.method,
            run: run.run,
            size: sc.size(),
        })
    }

    /// Every way rows pulled from Spark reach `e` (see the module docs), in walk
    /// order; empty when `e` holds none.
    pub fn pulled_rows<'e>(&self, e: &'e Expr) -> Vec<Pulled<'e>>
    where
        'a: 'e,
    {
        self.pulled_walk(e, 0, 0)
    }

    fn pull_holds(&self, p: &Pull) -> Holds {
        match p.run.returns {
            Returns::Rows => Holds::RowList,
            Returns::RowIter => Holds::RowIter,
            Returns::Row => Holds::Row,
            Returns::LocalFrame if self.ty_of(p.call).ty == Ty::PandasFrame => Holds::PandasFrame,
            Returns::LocalFrame => Holds::OtherFrame,
            Returns::Map | Returns::Value | Returns::Nothing => Holds::Values,
        }
    }

    /// The oracle's answer for `e` when no pull is in sight: a list / pandas frame
    /// produced on the driver from a Spark value.
    fn oracle_pulled<'e>(&self, e: &'e Expr) -> Option<Pulled<'e>> {
        let t = self.ty_of(e);
        if !t.facts.from_spark {
            return None;
        }
        let holds = match t.ty {
            Ty::List(Elem::Row | Elem::Unknown) => Holds::RowList,
            Ty::List(_) => Holds::Values,
            Ty::PandasFrame => Holds::PandasFrame,
            _ => return None,
        };
        Some(Pulled {
            pull: None,
            path: Vec::new(),
            at: e,
            holds,
            size: t.size(),
        })
    }

    fn pulled_walk<'e>(&self, e: &'e Expr, depth: u32, hops: u32) -> Vec<Pulled<'e>>
    where
        'a: 'e,
    {
        let mut out = Vec::new();
        if depth > MAX_DEPTH {
            return out;
        }
        if let Some(p) = self.pull_call(e) {
            let holds = self.pull_holds(&p);
            out.push(Pulled {
                pull: Some(p),
                path: Vec::new(),
                at: e,
                holds,
                size: p.size,
            });
            return out;
        }
        // One step `step` producing `e` over `inner`'s ways.
        // Whether some way reached a part of `e` (kept or not): the oracle then has
        // nothing to add (`list(pdf)` is the column labels, whatever its type says).
        let mut reached = false;
        let mut push =
            |out: &mut Vec<Pulled<'e>>, step: PathStep, inner: Vec<Pulled<'e>>, pairs| {
                reached |= !inner.is_empty();
                for mut p in inner {
                    if let Some(h) = after(step, p.holds, e, pairs) {
                        p.holds = h;
                        p.path.insert(0, (step, e));
                        out.push(p);
                    }
                }
            };
        let d = depth + 1;
        match e {
            Expr::Name(n) if hops < NAME_HOPS => {
                for b in self.reaching(n) {
                    let Some(v) = self.binding(*b).value else {
                        continue;
                    };
                    if v.range() != e.range() {
                        let inner = self.pulled_walk(v, d, hops + 1);
                        push(&mut out, PathStep::Name(*b), inner, false);
                    }
                }
            }
            Expr::ListComp(_) | Expr::SetComp(_) | Expr::DictComp(_) | Expr::Generator(_) => {
                let first = match e {
                    Expr::ListComp(c) => c.generators.first(),
                    Expr::SetComp(c) => c.generators.first(),
                    Expr::DictComp(c) => c.generators.first(),
                    Expr::Generator(c) => c.generators.first(),
                    _ => None,
                };
                if let Some(g) = first {
                    let inner = self.pulled_walk(&g.iter, d, hops);
                    push(&mut out, PathStep::Comprehension, inner, false);
                }
            }

            Expr::Call(c) => {
                if let Some(q) = self.qualified_name(&c.func)
                    && WRAPS.contains(&q.as_str())
                {
                    let pairs = matches!(q.as_str(), "builtins.enumerate" | "builtins.zip");
                    for a in &c.arguments.args {
                        let inner = self.pulled_walk(a, d, hops);
                        push(&mut out, PathStep::Wrap, inner, pairs);
                    }
                } else if let Some((_, recv, m)) = attribute_call(e) {
                    let step = if COLUMN_VALUES.contains(&m) {
                        PathStep::ColumnValues
                    } else if FRAME_ROWS.contains(&m)
                        || (m == "to_dict" && records_orient(c))
                        || m == "to_pylist"
                    {
                        PathStep::FrameRows
                    } else {
                        PathStep::Pandas
                    };
                    let inner = self.pulled_walk(recv, d, hops);
                    push(&mut out, step, inner, false);
                }
            }
            Expr::Subscript(s) => {
                let step = match &*s.slice {
                    Expr::Slice(_) => PathStep::Slice,
                    k if int_literal(k).is_some() => PathStep::Index,
                    _ => PathStep::Pandas,
                };
                let inner = self.pulled_walk(&s.value, d, hops);
                push(&mut out, step, inner, false);
            }
            Expr::Attribute(a) => {
                let inner = self.pulled_walk(&a.value, d, hops);
                push(&mut out, PathStep::Pandas, inner, false);
            }
            _ => {}
        }
        if !reached && let Some(p) = self.oracle_pulled(e) {
            out.push(p);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSummaries, SemanticOptions, build_model};
    use ruff_python_ast::Stmt;
    use ruff_text_size::TextSize;

    const PRELUDE: &str = "from pyspark.sql import SparkSession\n\
        import pyspark.pandas as ps\n\
        spark = SparkSession.builder.getOrCreate()\n\
        df = spark.table(\"t\")\n\
        psdf = ps.read_table(\"t\")\n";

    /// One way, summarised: (pull method or `oracle`, steps outermost first, holds,
    /// fits the driver).
    type Way = (String, Vec<&'static str>, Holds, bool);

    fn step_name(s: PathStep) -> &'static str {
        match s {
            PathStep::Index => "index",
            PathStep::Slice => "slice",
            PathStep::Comprehension => "comp",
            PathStep::Name(_) => "name",
            PathStep::Wrap => "wrap",
            PathStep::Pandas => "pandas",
            PathStep::ColumnValues => "values",
            PathStep::FrameRows => "frame-rows",
        }
    }

    /// The ways rows reach the value of the last statement of `src` (after PRELUDE).
    fn ways(src: &str) -> Vec<Way> {
        let full = format!("{PRELUDE}{src}");
        let m = ruff_python_parser::parse_module(&full)
            .expect("valid test source")
            .into_syntax();
        let model = build_model(
            &m,
            TextSize::of(full.as_str()),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let Some(Stmt::Expr(last)) = m.body.last() else {
            panic!("last statement must be an expression: {src}");
        };
        model
            .pulled_rows(&last.value)
            .iter()
            .map(|p| {
                (
                    p.pull
                        .map_or("oracle".to_string(), |x| x.method.to_string()),
                    p.path.iter().map(|(s, _)| step_name(*s)).collect(),
                    p.holds,
                    p.fits_driver(),
                )
            })
            .collect()
    }

    fn w(pull: &str, steps: &[&'static str], holds: Holds, fits: bool) -> Way {
        (pull.to_string(), steps.to_vec(), holds, fits)
    }

    /// One case per path shape (and per way a step drops).
    #[test]
    fn path_shapes() {
        use Holds::*;
        let cases: &[(&str, &[Way])] = &[
            // direct pulls: the one pull list
            ("df.collect()", &[w("collect", &[], RowList, false)]),
            (
                "df.toLocalIterator()",
                &[w("toLocalIterator", &[], RowIter, false)],
            ),
            ("df.toPandas()", &[w("toPandas", &[], PandasFrame, false)]),
            ("df.toArrow()", &[w("toArrow", &[], OtherFrame, false)]),
            ("df.rdd.collect()", &[w("collect", &[], RowList, false)]),
            (
                "df.rdd.collectAsMap()",
                &[w("collectAsMap", &[], Values, false)],
            ),
            (
                "psdf.to_pandas()",
                &[w("to_pandas", &[], PandasFrame, false)],
            ),
            // a self-bounded pull is no pull of the rows; the oracle still knows its
            // list came from Spark (and fits the driver)
            ("df.take(5)", &[w("oracle", &[], RowList, true)]),
            ("df.first()", &[]),
            // size verdict of the pulled frame
            ("df.limit(5).collect()", &[w("collect", &[], RowList, true)]),
            (
                "spark.createDataFrame([(1,)], [\"a\"]).collect()",
                &[w("collect", &[], RowList, true)],
            ),
            // index / slice
            ("df.collect()[0]", &[w("collect", &["index"], Row, false)]),
            (
                "df.collect()[1:]",
                &[w("collect", &["slice"], RowList, false)],
            ),
            ("df.toLocalIterator()[0]", &[]),
            // comprehension (first generator)
            (
                "[r.id for r in df.collect()]",
                &[w("collect", &["comp"], Values, false)],
            ),
            (
                "{r.k: r.v for r in df.toLocalIterator()}",
                &[w("toLocalIterator", &["comp"], Values, false)],
            ),
            ("[c for c in df.toPandas()]", &[]),
            // names: every reaching binding
            (
                "rows = df.collect()\nrows",
                &[w("collect", &["name"], RowList, false)],
            ),
            (
                "if x:\n    rows = df.collect()\nelse:\n    rows = df.limit(3).collect()\nrows",
                &[
                    w("collect", &["name"], RowList, false),
                    w("collect", &["name"], RowList, true),
                ],
            ),
            (
                "rows = df.collect()\nids = [r.id for r in rows]\nids",
                &[w("collect", &["name", "comp", "name"], Values, false)],
            ),
            // past NAME_HOPS names the oracle's from_spark list type answers
            (
                "a = df.collect()\nb = a\nc = b\nd = c\ne = d\ne",
                &[w(
                    "oracle",
                    &["name", "name", "name", "name"],
                    RowList,
                    false,
                )],
            ),
            // wraps
            (
                "list(df.toLocalIterator())",
                &[w("toLocalIterator", &["wrap"], RowList, false)],
            ),
            (
                "enumerate(df.collect())",
                &[w("collect", &["wrap"], RowIter, false)],
            ),
            (
                "sorted(set(r.id for r in df.collect()))",
                &[w("collect", &["wrap", "wrap", "comp"], Values, false)],
            ),
            ("list(df.toPandas())", &[]),
            // pandas columns
            (
                "df.toPandas()[\"id\"]",
                &[w("toPandas", &["pandas"], Series, false)],
            ),
            (
                "pdf = df.toPandas()\npdf[\"id\"].tolist()",
                &[w("toPandas", &["values", "pandas", "name"], Values, false)],
            ),
            (
                "list(df.toPandas().id)",
                &[w("toPandas", &["wrap", "pandas"], Values, false)],
            ),
            ("list(df.toPandas().columns)", &[]),
            // frame rows
            (
                "df.toPandas().iterrows()",
                &[w("toPandas", &["frame-rows"], RowIter, false)],
            ),
            (
                "df.toPandas().to_dict(\"records\")",
                &[w("toPandas", &["frame-rows"], RowList, false)],
            ),
            (
                "df.toArrow().to_pylist()",
                &[w("toArrow", &["frame-rows"], RowList, false)],
            ),
            ("df.toPandas().to_dict().items()", &[]),
            // not pulled rows
            ("[1, 2, 3]", &[]),
            ("df.select(\"a\")", &[]),
        ];
        let bad: Vec<String> = cases
            .iter()
            .filter(|(src, want)| ways(src) != *want)
            .map(|(src, _)| format!("{src:?}: {:?}", ways(src)))
            .collect();
        assert!(bad.is_empty(), "{}", bad.join("\n"));
    }

    #[test]
    fn holds_groups() {
        use Holds::*;
        assert!(RowList.rows() && RowIter.rows() && !Values.rows() && !Row.rows());
        assert!(Values.values() && RowList.values() && !Series.values());
        assert!(PandasFrame.frame() && OtherFrame.frame() && !Series.frame());
    }

    #[test]
    fn one_pull_list() {
        for (ty, m, want) in [
            (Ty::DataFrame, "collect", true),
            (Ty::DataFrame, "toLocalIterator", true),
            (Ty::DataFrame, "toPandas", true),
            (Ty::DataFrame, "toArrow", true),
            (Ty::DataFrame, "take", false),
            (Ty::DataFrame, "count", false),
            (Ty::Rdd, "collect", true),
            (Ty::PandasOnSparkFrame, "to_pandas", true),
        ] {
            assert_eq!(pull_method(ty, m), want, "{ty:?}.{m}");
        }
    }
}
