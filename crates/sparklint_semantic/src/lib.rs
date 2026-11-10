//! sparklint's semantic layer: scopes, bindings, def-use and the Spark type oracle.
//!
//! ```text
//! let model = build_model(&module, len, SemanticOptions::default(), &NoSummaries);
//! model.ty_of(expr)            // -> TypeInfo { ty, confidence, facts }
//! model.qualified_name(expr)   // -> Some("pyspark.sql.functions.col")
//! model.spark_call(expr)       // -> Some(SparkCall { "pyspark.sql.DataFrame.count", confidence, bounded, .. })
//! model.in_loop(&node)         // lexically inside a loop of the same function
//! model.reaching(name) / model.binding_uses(id)   // def-use
//! ```
//! The model is file-pure; cross-file knowledge enters only through a
//! [`SummaryProvider`] (see [`index::ProjectIndex`]).

pub mod api;
mod builder;
pub mod index;
mod infer;
mod model;
mod spark_call;
pub mod sql;
pub mod types;

pub use builder::build_model;
pub use index::{ProjectIndex, module_name_for_path};
pub use infer::{
    call_arg, int_literal, is_dataframe_name, is_logger_name, is_session_name, static_text,
    str_literal,
};
pub use model::*;
pub use spark_call::SparkCall;
pub use sql::{SetPair, SqlCond, SqlConjunct, SqlText};
pub use types::{Confidence, Elem, Facts, Ty, TypeInfo};

#[cfg(test)]
mod tests {
    use super::*;
    use ruff_python_ast::visitor::{self, Visitor};
    use ruff_python_ast::{Expr, ModModule};
    use ruff_text_size::{Ranged, TextSize};

    fn parse(src: &str) -> ModModule {
        ruff_python_parser::parse_module(src)
            .expect("valid test source")
            .into_syntax()
    }

    struct Find<'a> {
        src: &'a str,
        needle: &'a str,
        found: Vec<&'a Expr>,
    }
    impl<'a> Visitor<'a> for Find<'a> {
        fn visit_expr(&mut self, e: &'a Expr) {
            let r = e.range();
            if &self.src[usize::from(r.start())..usize::from(r.end())] == self.needle {
                self.found.push(e);
            }
            visitor::walk_expr(self, e);
        }
    }

    fn with_opts(src: &str, opts: SemanticOptions, needle: &str, nth: usize) -> TypeInfo {
        let m = parse(src);
        let model = build_model(&m, TextSize::of(src), opts, &NoSummaries);
        let mut f = Find {
            src,
            needle,
            found: Vec::new(),
        };
        for s in &m.body {
            f.visit_stmt(s);
        }
        let e = f
            .found
            .get(nth)
            .unwrap_or_else(|| panic!("needle {needle:?} #{nth} not found"));
        model.ty_of(e)
    }

    fn ty(src: &str, needle: &str) -> TypeInfo {
        with_opts(src, SemanticOptions::default(), needle, 0)
    }

    fn ty_last(src: &str, needle: &str) -> TypeInfo {
        let m = parse(src);
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let mut f = Find {
            src,
            needle,
            found: Vec::new(),
        };
        for s in &m.body {
            f.visit_stmt(s);
        }
        model.ty_of(f.found.last().expect("needle"))
    }

    #[test]
    fn session_origins() {
        let src = "from pyspark.sql import SparkSession\nspark = SparkSession.builder.appName('x').getOrCreate()\ns2 = SparkSession.getActiveSession()\nspark\ns2\n";
        let t = ty_last(src, "spark");
        assert_eq!(t.ty, Ty::SparkSession);
        assert!(t.is_certain());
        assert_eq!(ty_last(src, "s2").ty, Ty::SparkSession);
        let src = "from databricks.connect import DatabricksSession\nspark = DatabricksSession.builder.serverless().getOrCreate()\nspark\n";
        assert_eq!(ty_last(src, "spark").ty, Ty::SparkSession);
    }

    #[test]
    fn notebook_globals_and_heuristics() {
        let src = "spark\n";
        let t = ty(src, "spark");
        assert_eq!((t.ty, t.confidence), (Ty::SparkSession, Confidence::Likely));
        let opts = SemanticOptions {
            is_notebook: true,
            ..Default::default()
        };
        let t = with_opts(src, opts, "spark", 0);
        assert_eq!(
            (t.ty, t.confidence),
            (Ty::SparkSession, Confidence::Certain)
        );
        let opts = SemanticOptions {
            heuristics: false,
            ..Default::default()
        };
        assert!(with_opts(src, opts, "spark", 0).is_unknown());
        let opts = SemanticOptions {
            known_globals: vec![("ss".into(), Ty::SparkSession)],
            ..Default::default()
        };
        assert_eq!(with_opts("ss\n", opts, "ss", 0).ty, Ty::SparkSession);
    }

    #[test]
    fn imports_resolve_to_qualified_names() {
        let src = "import pyspark.sql.functions as sf\nfrom pyspark.sql import functions as F\nfrom pyspark.sql.functions import col as c, udf\nsf.col('a')\nF.lit(1)\nc('x')\n";
        let m = parse(src);
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let mut names = Vec::new();
        for s in &m.body[3..] {
            if let ruff_python_ast::Stmt::Expr(e) = s
                && let Expr::Call(c) = &*e.value
            {
                names.push(model.qualified_name(&c.func).unwrap());
                assert_eq!(model.ty_of(&e.value).ty, Ty::Column);
            }
        }
        assert_eq!(
            names,
            vec![
                "pyspark.sql.functions.col",
                "pyspark.sql.functions.lit",
                "pyspark.sql.functions.col"
            ]
        );
    }

    #[test]
    fn dataframe_chain_and_facts() {
        let src = "df = spark.read.parquet('/p')\na = df.filter(df.x > 1)\nb = df.limit(10)\nc = df.agg({'x': 'sum'})\nd = df.groupBy('k').agg({'x': 'sum'})\ne = df.cache()\nw = df.write.mode('overwrite')\na\nb\nc\nd\ne\nw\n";
        let a = ty_last(src, "a");
        assert!(a.is_dataframe() && a.facts.reduced && a.bounded().is_none());
        assert_eq!(ty_last(src, "b").bounded(), Some(10));
        assert_eq!(ty_last(src, "c").bounded(), Some(1));
        let d = ty_last(src, "d");
        assert!(d.is_dataframe() && d.facts.reduced && d.bounded().is_none());
        assert!(ty_last(src, "e").facts.cached);
        assert_eq!(ty_last(src, "w").ty, Ty::DataFrameWriter);
        assert_eq!(ty(src, "df.x").ty, Ty::Column);
        assert_eq!(ty(src, "df.x > 1").ty, Ty::Column);
    }

    #[test]
    fn global_aggregate_select_is_one_row() {
        let src = "from pyspark.sql import functions as F\ndf = spark.table('t')\na = df.select(F.max('x').alias('m'), F.count('*'))\nb = df.select(F.max('x').over(w))\nc = spark.sql('SELECT max(x) FROM t')\na\nb\nc\n";
        assert_eq!(ty_last(src, "a").bounded(), Some(1));
        assert_eq!(ty_last(src, "b").bounded(), None);
        assert_eq!(ty_last(src, "c").bounded(), Some(1));
    }

    #[test]
    fn sql_query_facts_follow_names() {
        let src = "q = 'SELECT * FROM t LIMIT 5'\na = spark.sql(q)\nb = spark.sql(sqlQuery=f'SELECT max(x) FROM {t}')\nc = spark.sql('SELECT * FROM t -- LIMIT 1')\na\nb\nc\n";
        assert_eq!(ty_last(src, "a").bounded(), Some(5));
        assert_eq!(ty_last(src, "b").bounded(), Some(1));
        assert_eq!(ty_last(src, "c").bounded(), None);
    }

    #[test]
    fn branch_paths_and_max_weight() {
        let src = "if a:\n    x = 1\n    if b:\n        y = 2\n    else:\n        z = 3\nelse:\n    w = 4\nv = 5\n";
        let m = parse(src);
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let off = |s: &str| TextSize::from(src.find(s).unwrap() as u32);
        assert_eq!(model.branch_path(off("x = 1")).len(), 1);
        assert_eq!(model.branch_path(off("y = 2")).len(), 2);
        assert!(model.branch_path(off("v = 5")).is_empty());
        assert!(model.mutually_exclusive(off("y = 2"), off("z = 3")));
        assert!(model.mutually_exclusive(off("x = 1"), off("w = 4")));
        assert!(!model.mutually_exclusive(off("x = 1"), off("y = 2")));
        let item = |s: &str| (model.branch_path(off(s)), 1u32);
        let items = vec![
            item("x = 1"),
            item("y = 2"),
            item("z = 3"),
            item("w = 4"),
            item("v = 5"),
        ];
        // path x,y(or z),v = 3 beats w,v = 2
        assert_eq!(SemanticModel::max_path_weight(&items), 3);
    }

    #[test]
    fn reassignment_kills_old_facts() {
        let src = "df = spark.table('t').limit(5)\ndf\ndf = spark.table('u')\ndf\n";
        assert_eq!(
            with_opts(src, SemanticOptions::default(), "df", 1).bounded(),
            Some(5)
        );
        assert_eq!(ty_last(src, "df").bounded(), None);
    }

    #[test]
    fn branches_join() {
        let src = "if c:\n    x = spark.table('a').limit(1)\nelse:\n    x = spark.table('b').limit(3)\nx\n";
        let t = ty_last(src, "x");
        assert!(t.is_dataframe());
        assert_eq!(t.bounded(), Some(3));
        let src = "x = spark.table('a')\nif c:\n    x = [1]\nx\n";
        assert!(ty_last(src, "x").is_unknown());
    }

    #[test]
    fn pandas_on_spark_derived_values() {
        let pre = "import pyspark.pandas as ps\nimport pandas as pd\npsdf = ps.read_table('t')\n";
        let ps_cert = |body: &str, name: &str| {
            let t = ty_last(&format!("{pre}{body}\n{name}\n"), name);
            (t.ty, t.confidence)
        };
        let want = (Ty::PandasOnSparkFrame, Confidence::Certain);
        assert_eq!(ps_cert("s = psdf['x']", "s"), want);
        assert_eq!(ps_cert("s = psdf[['a', 'b']]", "s"), want);
        assert_eq!(ps_cert("s = psdf[psdf['x'] > 0]", "s"), want);
        assert_eq!(ps_cert("s = psdf['x'] * 2 + 1", "s"), want);
        assert_eq!(ps_cert("s = psdf.sort_values('t')['x']", "s"), want);
        // groupby / reductions / positional lookups are not frames the rules model,
        // and not `pyspark.sql` objects either (no name heuristic on `agg_df`)
        let other = |body: &str, name: &str| ty_last(&format!("{pre}{body}\n{name}\n"), name).ty;
        assert_eq!(other("g = psdf.groupby('k')['x']", "g"), Ty::NonSpark);
        assert_eq!(other("v = psdf['x'].sum()", "v"), Ty::NonSpark);
        assert_eq!(other("v = psdf['x'][0]", "v"), Ty::NonSpark);
        assert_eq!(
            other("agg_df = psdf.groupby('k').sum()", "agg_df"),
            Ty::NonSpark
        );
        assert_eq!(other("row_df = psdf.iloc[0]", "row_df"), Ty::NonSpark);
        // the documented ways back to Spark
        assert_eq!(other("sdf = psdf.to_spark()", "sdf"), Ty::DataFrame);
        assert_eq!(other("sdf = psdf.spark.frame()", "sdf"), Ty::DataFrame);
        assert_eq!(
            other("sdf = psdf._internal.spark_frame", "sdf"),
            Ty::DataFrame
        );
        assert_eq!(
            other("sdf = psdf._to_internal_frame()", "sdf"),
            Ty::DataFrame
        );
        // plain pandas stays pandas
        assert_eq!(other("p = pd.DataFrame({})\nq = p['a']", "q"), Ty::NonSpark);
    }

    #[test]
    fn local_data_values_never_become_spark() {
        let pre = "import numpy as np\nimport pandas as pd\nimport polars as pl\nfrom pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\npd_df = pd.read_parquet('x')\nzones_df = pd.read_csv('z')\n";
        let last = |body: &str, name: &str| ty_last(&format!("{pre}{body}\n{name}\n"), name);
        // The corpus shape: a pandas `join` result bound to a `*_df` name.
        let t = last(
            "pd_df = pd_df.join(zones_df.set_index('id'), on='k')",
            "pd_df",
        );
        assert_eq!((t.ty, t.confidence), (Ty::NonSpark, Confidence::Certain));
        for body in [
            "x_df = pd_df.merge(zones_df, on='k')",
            "x_df = pd_df.groupby('k').sum()",
            "x_df = pd_df.filter(items=['a'])",
            "x_df = pd_df.select_dtypes('number')",
            "x_df = pd_df.drop(columns=['a']).sort_values('b').head(5)",
            "x_df = pd_df[pd_df['a'] > 1]",
            "x_df = pd_df.loc[pd_df.a > 1, ['a']]",
            "x_df = pd_df * 2",
            "x_df = -pd_df",
            "x_df = pd_df['a'] > 1",
            "x_df = pd_df.values",
            "x_df = pd.Series([1]).to_frame()",
            "x_df = pd.json_normalize([{}])",
            "x_df = np.array([[1]]).reshape(1, 1)",
            "x_df = pl.read_parquet('x').select('a').filter(pl.col('a') > 1)",
            "x_df = spark.table('t').toPandas().reset_index()",
            "x_df = spark.table('t').pandas_api().to_pandas().dropna()",
            "x_df = pd_df if cond else pd_df.dropna()",
            "for x_df in np.array_split(pd_df, 4):\n    pass",
        ] {
            let t = last(body, "x_df");
            assert!(t.ty.is_local_data(), "{body}: {:?}", t.ty);
            assert!(!t.ty.is_spark(), "{body}");
        }
        // The DataFrame-only method heuristic does not apply to them either.
        assert_eq!(
            last("y = pd_df.groupby('k').sum().withColumn('a', 1)", "y").ty,
            Ty::NonSpark
        );
        // Spark values built from them are Spark.
        let t = last("sdf = spark.createDataFrame(pd_df.dropna())", "sdf");
        assert_eq!((t.ty, t.confidence), (Ty::DataFrame, Confidence::Certain));
        // An unknown value with a DataFrame-ish name is still a (likely) DataFrame.
        assert_eq!(last("out_df = load()", "out_df").ty, Ty::DataFrame);
    }

    #[test]
    fn local_function_with_unknown_return_is_unknown_not_none() {
        // The body of `load` is walked after the module body: its call must not be
        // typed `None` in the meantime (that hid `events_df` from the name heuristic).
        let src = "def load(spark):\n    return make(spark).build()\n\ndef log(msg):\n    print(msg)\n\nevents_df = load(spark)\nevents_df\nnothing = log('x')\nnothing\n";
        let t = ty_last(src, "events_df");
        assert_eq!((t.ty, t.confidence), (Ty::DataFrame, Confidence::Likely));
        // A function without a `return` value still returns None.
        assert_eq!(ty_last(src, "nothing").ty, Ty::NoneTy);
    }

    #[test]
    fn foreign_builders_are_not_dataframes() {
        // dbldatagen's DataGenerator mirrors `withColumn`; it is not a DataFrame.
        let pre = "import dbldatagen as dg\nfrom tempo import TSDF\nimport mylib\n";
        let last = |body: &str, name: &str| ty_last(&format!("{pre}{body}\n{name}\n"), name);
        for body in [
            "spec = dg.DataGenerator(spark, rows=10).withIdOutput().withColumn('a', 'int')",
            "spec = dg.DataGenerator(spark, rows=10)\nspec = spec.withColumn('a', 'int')\nspec = spec.withColumn('b', 'int')",
            "spec = dg.DataGenerator(spark, rows=10)\nfor c in cols:\n    spec = spec.withColumn(c, 'int')",
            "spec = TSDF(sdf, ts_col='t').withColumn('a', 1)",
        ] {
            assert!(last(body, "spec").is_unknown(), "{body}");
        }
        // What the builder builds is unknown, so a `df` name still decides.
        let t = last(
            "df = dg.DataGenerator(spark, rows=10).withColumn('a', 'int').build()",
            "df",
        );
        assert_eq!((t.ty, t.confidence), (Ty::DataFrame, Confidence::Likely));
        // A lower-case function of another library may return a DataFrame: the
        // DataFrame-only method is still evidence. So is it on an unknown value.
        for body in [
            "y = mylib.load(spark).withColumn('a', 1)",
            "x = mylib.load(spark)\ny = x.withColumn('a', 1)",
            "y = get().withColumn('a', 1)",
        ] {
            let t = last(body, "y");
            assert_eq!(
                (t.ty, t.confidence),
                (Ty::DataFrame, Confidence::Likely),
                "{body}"
            );
        }
        // Spark's own classes are not foreign.
        let src = "from pyspark.sql import SparkSession\ny = SparkSession.builder.getOrCreate().table('t').withColumn('a', 1)\ny\n";
        assert_eq!(ty_last(src, "y").ty, Ty::DataFrame);
    }

    #[test]
    fn non_spark_lookalikes_are_not_dataframes() {
        let src =
            "import pandas as pd\nitems = [1, 2]\npdf = pd.read_csv('x')\ns = ','\nitems\npdf\ns\n";
        assert_eq!(ty_last(src, "items").ty, Ty::List(Elem::Other));
        assert_eq!(ty_last(src, "pdf").ty, Ty::PandasFrame);
        assert_eq!(ty_last(src, "s").ty, Ty::Str);
        // name heuristic does not override a known non-Spark binding
        let src = "import pandas as pd\ndf = pd.DataFrame({})\ndf\n";
        assert_eq!(ty_last(src, "df").ty, Ty::PandasFrame);
        // unknown receiver with an ambiguous method stays unknown
        let src = "x = get()\ny = x.filter(1)\ny\n";
        assert!(ty_last(src, "y").is_unknown());
        // ...but a DataFrame-only method is evidence
        let src = "x = get()\ny = x.withColumn('a', 1)\ny\n";
        let t = ty_last(src, "y");
        assert_eq!((t.ty, t.confidence), (Ty::DataFrame, Confidence::Likely));
    }

    #[test]
    fn annotations() {
        let src = "from pyspark.sql import DataFrame\nfrom typing import Optional\ndef f(a: DataFrame, b: 'DataFrame', c: Optional[DataFrame], d: DataFrame | None, e):\n    a\n    b\n    c\n    d\n    e\n";
        for n in ["a", "b", "c", "d"] {
            let t = ty_last(src, n);
            assert_eq!(t.ty, Ty::DataFrame, "{n}");
            assert!(t.is_certain(), "{n}");
        }
        assert!(ty_last(src, "e").is_unknown());
        let src = "import pyspark.sql as ps\ndef f(x: ps.DataFrame, y: 'list[ps.DataFrame]'):\n    x\n    y\n";
        assert_eq!(ty_last(src, "x").ty, Ty::DataFrame);
        assert_eq!(ty_last(src, "y").ty, Ty::List(Elem::DataFrame));
    }

    #[test]
    fn function_summaries_and_param_evidence() {
        let src = "def load(spark):\n    return spark.read.table('t')\n\ndef clean(d):\n    return d.withColumn('a', 1)\n\nx = load(spark)\ny = clean(x)\nx\ny\n";
        assert!(ty_last(src, "x").is_dataframe());
        assert!(ty_last(src, "y").is_dataframe());
        // parameter typed by call-site evidence
        let src = "def f(t):\n    t\n\nf(spark.table('x'))\n";
        let t = ty(src, "t");
        assert_eq!(
            (t.ty, t.confidence),
            (Ty::DataFrame, Confidence::Likely),
            "{t:?}"
        );
        // methods and self attributes
        let src = "class Job:\n    def __init__(self, spark):\n        self.spark = spark\n    def read(self):\n        return self.spark.table('t')\n\nj = Job(spark)\nr = j.read()\nr\n";
        assert!(ty_last(src, "r").is_dataframe());
    }

    #[test]
    fn loop_targets() {
        let src = "df = spark.table('t')\nfor row in df.collect():\n    row\nfor c in df.columns:\n    c\nfor i, r in enumerate(df.take(3)):\n    r\nfor part in df.randomSplit([0.5, 0.5]):\n    part\n";
        assert_eq!(ty_last(src, "row").ty, Ty::Row);
        assert_eq!(ty_last(src, "c").ty, Ty::Str);
        assert_eq!(ty_last(src, "r").ty, Ty::Row);
        assert!(ty_last(src, "part").is_dataframe());
    }

    #[test]
    fn loop_carried_bindings_and_in_loop() {
        let src = "df = spark.table('t')\nfor c in cols:\n    df = df.withColumn(c, F.lit(1))\n";
        let m = parse(src);
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let off = TextSize::from(src.find("df.withColumn").unwrap() as u32);
        assert!(model.loop_depth(off) == 1);
        assert!(model.loop_depth(TextSize::from(0)) == 0);
        // the receiver `df` inside the loop is reached by both the pre-loop and the
        // loop-carried binding
        let use_site = model.uses.get(&off).unwrap();
        assert_eq!(use_site.reaching.len(), 2);
    }

    #[test]
    fn window_specs() {
        let src = "from pyspark.sql.window import Window as W\nw1 = W.orderBy('ts')\nw2 = W.partitionBy('k').orderBy('ts')\nw3 = w1.partitionBy('k')\nw4 = W.partitionBy()\nw1\nw2\nw3\nw4\n";
        let w1 = ty_last(src, "w1");
        assert_eq!(w1.ty, Ty::WindowSpec);
        assert!(w1.facts.win_ordered && !w1.facts.win_partitioned);
        assert!(ty_last(src, "w2").facts.win_partitioned);
        assert!(ty_last(src, "w3").facts.win_partitioned);
        assert!(!ty_last(src, "w4").facts.win_partitioned);
    }

    #[test]
    fn window_frames() {
        let src = "import sys\nfrom pyspark.sql.window import Window as W\nw1 = W.orderBy('t').rowsBetween(W.unboundedPreceding, W.currentRow)\nw2 = W.orderBy('t').rowsBetween(-sys.maxsize - 1, 0)\nw3 = W.orderBy('t').rowsBetween(W.unboundedPreceding, W.unboundedFollowing)\nw4 = W.orderBy('t').rowsBetween(-3, 0)\nw5 = W.orderBy('t').rangeBetween(start=-sys.maxsize, end=5)\nw6 = w1.rowsBetween(-1, 0)\nw7 = W.orderBy('t').rowsBetween(lo, 0)\nw1\nw2\nw3\nw4\nw5\nw6\nw7\n";
        let g = |n| ty_last(src, n).facts.win_frame_growing;
        assert!(g("w1") && g("w2") && g("w5"));
        assert!(!g("w3") && !g("w4") && !g("w6") && !g("w7"));
        assert!(ty_last(src, "w7").facts.win_framed);
    }

    #[test]
    fn cache_marker_statement() {
        let src = "df = spark.table('t')\ndf.cache()\ndf\n";
        assert!(ty_last(src, "df").facts.cached);
    }

    #[test]
    fn external_summaries() {
        let mut idx = ProjectIndex::new();
        idx.add_module(
            "pkg.io",
            &[("load".into(), TypeInfo::certain(Ty::DataFrame))],
        );
        let src = "from pkg.io import load\nx = load()\nx\n";
        let m = parse(src);
        let model = build_model(&m, TextSize::of(src), SemanticOptions::default(), &idx);
        let last = match &m.body[2] {
            ruff_python_ast::Stmt::Expr(e) => &*e.value,
            _ => unreachable!(),
        };
        assert!(model.ty_of(last).is_dataframe());
        assert_eq!(model.exported_summaries().len(), 0);
    }

    #[test]
    fn udfs() {
        let src = "from pyspark.sql.functions import udf\n@udf('string')\ndef up(s):\n    return s.upper()\nf = udf(lambda x: x)\nup(c)\nf(c)\n";
        assert_eq!(ty_last(src, "up(c)").ty, Ty::Column);
        assert_eq!(ty_last(src, "f(c)").ty, Ty::Column);
    }
}
