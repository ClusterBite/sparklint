//! Usage evidence: what the uses of an untyped value say it is.
//!
//! One question, asked by every walker that guesses a receiver from how it is used
//! (the parameter walker in `builder.rs`, the unknown-receiver heuristic in
//! `infer.rs`, rules that need evidence for a heuristic receiver):
//!
//! - [`of_use`]: what ONE use `x.attr` / `x.attr(..)` says (DataFrame, session,
//!   pandas, or nothing). The call shape is part of it: `x.withColumn("a", "int")`
//!   is another library's builder, not DataFrame evidence.
//! - [`Evidence`]: the verdict over several uses. Mixed evidence (a pandas-only
//!   attribute next to a Spark-only one) is no evidence at all.

use ruff_python_ast::{self as ast, Expr};

use crate::types::Ty;

/// What one use of a value says about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Use {
    /// A DataFrame-only method or attribute.
    DataFrame,
    /// A SparkSession-only attribute.
    Session,
    /// A pandas(-like) frame attribute no Spark DataFrame, Column or session has.
    Pandas,
}

/// What the use `x.<attr>` says about `x`; `call` is the call when the attribute is
/// called (`x.<attr>(..)`).
pub fn of_use(attr: &str, call: Option<&ast::ExprCall>) -> Option<Use> {
    if call.is_some_and(|c| call_shape_rules_out_dataframe(attr, c)) {
        return None;
    }
    if DATAFRAME_ONLY_METHODS.contains(&attr) {
        Some(Use::DataFrame)
    } else if SESSION_ONLY_ATTRS.contains(&attr) {
        Some(Use::Session)
    } else if PANDAS_ONLY_ATTRS.contains(&attr) {
        Some(Use::Pandas)
    } else {
        None
    }
}

/// Whether `m` exists only on Spark DataFrames (whatever the call looks like).
pub fn is_dataframe_only(m: &str) -> bool {
    DATAFRAME_ONLY_METHODS.contains(&m)
}

/// Evidence gathered over the uses of one value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Evidence {
    /// The first Spark verdict seen (DataFrame or SparkSession).
    spark: Option<Ty>,
    /// Some use is pandas-only.
    pandas: bool,
}

impl Evidence {
    pub fn add(&mut self, u: Option<Use>) {
        match u {
            Some(Use::DataFrame) => {
                self.spark.get_or_insert(Ty::DataFrame);
            }
            Some(Use::Session) => {
                self.spark.get_or_insert(Ty::SparkSession);
            }
            Some(Use::Pandas) => self.pandas = true,
            None => {}
        }
    }

    /// The evidence of plain attribute uses (`x.attr`, call shapes not seen).
    pub fn of_attrs<'a>(attrs: impl IntoIterator<Item = &'a str>) -> Evidence {
        let mut e = Evidence::default();
        for a in attrs {
            e.add(of_use(a, None));
        }
        e
    }

    /// Some use is pandas-only: whatever else the value is used as, it is not
    /// known to be a Spark DataFrame.
    pub fn has_pandas(&self) -> bool {
        self.pandas
    }

    /// The type the uses agree on: a Spark DataFrame / session, a pandas frame, or
    /// nothing (no evidence, or mixed Spark and pandas evidence).
    pub fn verdict(&self) -> Option<Ty> {
        match (self.spark, self.pandas) {
            (s, false) => s,
            (None, true) => Some(Ty::PandasFrame),
            (Some(_), true) => None,
        }
    }
}

/// A call of a DataFrame-only method name whose arguments cannot be the DataFrame
/// method, so it is no evidence that an untyped receiver is a DataFrame.
/// `DataFrame.withColumn(colName, col)` raises unless `col` is a Column: a literal
/// value, a third positional argument or another keyword (`template=`, `**opts`)
/// is another library's builder (dbldatagen `gen.withColumn("a", "int", minValue=1)`).
fn call_shape_rules_out_dataframe(m: &str, c: &ast::ExprCall) -> bool {
    match m {
        "withColumn" => {
            let args = &c.arguments.args;
            args.len() > 2
                || args.get(1).is_some_and(is_non_column_literal)
                || c.arguments.keywords.iter().any(|k| {
                    !k.arg
                        .as_ref()
                        .is_some_and(|a| matches!(a.as_str(), "colName" | "col"))
                })
        }
        _ => false,
    }
}

/// A literal that is never a Column (strings, numbers, booleans, None, containers).
fn is_non_column_literal(e: &Expr) -> bool {
    matches!(
        e,
        Expr::StringLiteral(_)
            | Expr::FString(_)
            | Expr::BytesLiteral(_)
            | Expr::NumberLiteral(_)
            | Expr::BooleanLiteral(_)
            | Expr::NoneLiteral(_)
            | Expr::List(_)
            | Expr::Tuple(_)
            | Expr::Dict(_)
            | Expr::Set(_)
    )
}

/// Methods that only exist on Spark DataFrames (used as evidence for untyped values).
/// Deliberately excludes names shared with pandas, lists, strings or RDDs.
pub const DATAFRAME_ONLY_METHODS: &[&str] = &[
    "withColumn",
    "withColumns",
    "withColumnRenamed",
    "withColumnsRenamed",
    "groupBy",
    "unionByName",
    "unionAll",
    "toPandas",
    "createOrReplaceTempView",
    "createTempView",
    "createOrReplaceGlobalTempView",
    "createGlobalTempView",
    "registerTempTable",
    "selectExpr",
    "crossJoin",
    "dropDuplicates",
    "printSchema",
    "repartitionByRange",
    "sortWithinPartitions",
    "withWatermark",
    "writeTo",
    "toLocalIterator",
    "localCheckpoint",
    "mapInPandas",
    "mapInArrow",
    "withMetadata",
    "dropDuplicatesWithinWatermark",
    "unpivot",
    "writeStream",
    "isStreaming",
    "toDF",
    "alias",
    "colRegex",
];

/// Methods that only exist on a SparkSession (evidence for untyped parameters).
pub const SESSION_ONLY_ATTRS: &[&str] = &[
    "read",
    "readStream",
    "createDataFrame",
    "sparkContext",
    "catalog",
    "udf",
    "newSession",
    "getActiveSession",
];

/// Attributes of pandas (and pandas-on-Spark) frames that no Spark DataFrame,
/// Column or session has: usage evidence against the `df` heuristic.
pub const PANDAS_ONLY_ATTRS: &[&str] = &[
    "iloc",
    "loc",
    "iat",
    "shape",
    "iterrows",
    "itertuples",
    "to_numpy",
    "sort_values",
    "reset_index",
    "set_index",
    "value_counts",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn call(src: &str) -> ast::ExprCall {
        match ruff_python_parser::parse_expression(src)
            .expect("valid expression")
            .into_expr()
        {
            Expr::Call(c) => c,
            _ => panic!("not a call: {src}"),
        }
    }

    fn use_of(src: &str) -> Option<Use> {
        let c = call(src);
        let Expr::Attribute(a) = &*c.func else {
            panic!("not a method call: {src}")
        };
        of_use(a.attr.as_str(), Some(&c))
    }

    #[test]
    fn one_use() {
        for (src, want) in [
            ("x.withColumn('a', F.lit(1))", Some(Use::DataFrame)),
            ("x.withColumn(colName='a', col=c)", Some(Use::DataFrame)),
            ("x.withColumn('a', 'int')", None),
            ("x.withColumn('a', c, 3)", None),
            ("x.withColumn('a', c, minValue=1)", None),
            ("x.withColumn('a', 1)", None),
            ("x.groupBy('k')", Some(Use::DataFrame)),
            ("x.createDataFrame(rows)", Some(Use::Session)),
            ("x.newSession()", Some(Use::Session)),
            ("x.sort_values('a')", Some(Use::Pandas)),
            ("x.to_numpy()", Some(Use::Pandas)),
            ("x.count()", None),
            ("x.filter(c)", None),
            ("x.join(y)", None),
        ] {
            assert_eq!(use_of(src), want, "{src}");
        }
        assert_eq!(of_use("withColumn", None), Some(Use::DataFrame));
        assert_eq!(of_use("iloc", None), Some(Use::Pandas));
        assert_eq!(of_use("read", None), Some(Use::Session));
        assert_eq!(of_use("columns", None), None);
        assert!(is_dataframe_only("selectExpr") && !is_dataframe_only("drop"));
    }

    #[test]
    fn verdict_over_uses() {
        let v = |uses: &[Option<Use>]| {
            let mut e = Evidence::default();
            for u in uses {
                e.add(*u);
            }
            (e.verdict(), e.has_pandas())
        };
        use Use::*;
        assert_eq!(v(&[]), (None, false));
        assert_eq!(v(&[None]), (None, false));
        assert_eq!(v(&[Some(DataFrame)]), (Some(Ty::DataFrame), false));
        assert_eq!(v(&[Some(Session)]), (Some(Ty::SparkSession), false));
        // the first Spark verdict wins
        assert_eq!(
            v(&[Some(DataFrame), Some(Session)]),
            (Some(Ty::DataFrame), false)
        );
        assert_eq!(
            v(&[Some(Session), Some(DataFrame)]),
            (Some(Ty::SparkSession), false)
        );
        assert_eq!(v(&[Some(Pandas)]), (Some(Ty::PandasFrame), true));
        // mixed evidence is no evidence, whatever the order
        assert_eq!(v(&[Some(Pandas), Some(DataFrame)]), (None, true));
        assert_eq!(v(&[Some(DataFrame), Some(Pandas)]), (None, true));
        assert_eq!(v(&[Some(Session), None, Some(Pandas)]), (None, true));
    }
}
