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
pub mod evidence;
pub mod facts;
pub mod index;
mod infer;
pub mod library;
mod model;
mod spark_call;
pub mod sql;
pub mod types;

pub use builder::build_model;
pub use facts::{Facts, Partitioning, Step};
pub use index::{ProjectIndex, module_name_for_path};
pub use infer::{
    call_arg, int_literal, is_dataframe_name, is_logger_name, is_session_name, static_text,
    str_literal,
};
pub use library::Lib;
pub use model::*;
pub use spark_call::SparkCall;
pub use sql::{SetPair, SqlCond, SqlConjunct, SqlText};
pub use types::{Confidence, Elem, Ty, TypeInfo};

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
    fn aggregate_shapes_are_one_row() {
        let pre = "from pyspark.sql import functions as F\ndf = spark.table('t')\n";
        let b = |rhs: &str| ty_last(&format!("{pre}a = {rhs}\na\n"), "a").bounded();
        // a list of aggregates, aggregate SQL expressions, keyless groupings
        assert_eq!(
            b("df.select([F.count('a').alias('n'), F.max('b')])"),
            Some(1)
        );
        assert_eq!(b("df.selectExpr('min(v) as lo', 'max(v) as hi')"), Some(1));
        assert_eq!(b("df.selectExpr(['variance(label-prediction)'])"), Some(1));
        assert_eq!(
            b("df.select('c').groupBy().agg(F.min('c'), F.max('c'))"),
            Some(1)
        );
        assert_eq!(b("df.groupby().pivot('k').sum('v')"), Some(1));
        assert_eq!(b("df.groupBy().count()"), Some(1));
        // a column beside the aggregate, a window, keys, opaque text, grouped pandas
        assert_eq!(b("df.select([F.max('b'), 'c'])"), None);
        assert_eq!(b("df.selectExpr('max(v)', 'k')"), None);
        assert_eq!(b("df.selectExpr('max(v) over (partition by k)')"), None);
        assert_eq!(b("df.selectExpr(expr_list)"), None);
        assert_eq!(b("df.groupBy('k').agg(F.max('v'))"), None);
        assert_eq!(b("df.groupBy(*keys).agg(F.max('v'))"), None);
        assert_eq!(b("df.groupBy(keys).count()"), None);
        assert_eq!(b("df.groupBy().applyInPandas(fn, schema)"), None);
    }

    #[test]
    fn row_wise_functions_of_aggregates_are_one_row() {
        let pre = "from pyspark.sql import functions as F\ndf = spark.table('t')\n";
        let b = |sel: &str| ty_last(&format!("{pre}a = df.select({sel})\na\n"), "a").bounded();
        assert_eq!(b("F.to_date(F.max('d')).alias('m')"), Some(1));
        assert_eq!(b("F.round(F.avg('x'), 2)"), Some(1));
        assert_eq!(b("F.coalesce(F.sum('x'), F.lit(0))"), Some(1));
        assert_eq!(b("F.date_format(F.max('d'), 'yyyy-MM')"), Some(1));
        // a generator over an aggregate is several rows; a plain column is per row
        assert_eq!(b("F.explode(F.collect_list('x'))"), None);
        assert_eq!(b("F.to_date('d')"), None);
        assert_eq!(b("F.coalesce(F.max('x'), F.col('y'))"), None);
        assert_eq!(b("F.lit(1)"), None);
        assert_eq!(b("my_fn(F.max('x'))"), None);
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
    fn observations_are_typed() {
        for src in [
            "from pyspark.sql import Observation\nx = Observation('m')\nx\n",
            "import pyspark.sql as ps\nx = ps.Observation()\nx\n",
            "from pyspark.sql.observation import Observation as O\nx = O('m')\nx\n",
            "from pyspark.sql import Observation\ndef f(x: Observation | None):\n    x\n",
            "from pyspark.sql import Observation\nfrom typing import Optional\ndef f(x: Optional['Observation']):\n    x\n",
        ] {
            assert_eq!(ty_last(src, "x").ty, Ty::Observation, "{src}");
        }
        for src in [
            "class Observation:\n    pass\nx = Observation()\nx\n",
            "def f(x: 'Observation'):\n    x\n",
        ] {
            assert_ne!(ty_last(src, "x").ty, Ty::Observation, "{src}");
        }
    }

    #[test]
    fn frames_from_driver_data_are_local() {
        let pre = "import pandas as pd\nfrom pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F\nspark = SparkSession.builder.getOrCreate()\npdf = pd.DataFrame({})\nbig = spark.table('t')\nrows = [(1, 2)]\n";
        let local = |body: &str| ty_last(&format!("{pre}{body}\nx\n"), "x").facts.local;
        for body in [
            "x = spark.createDataFrame(pdf)",
            "x = spark.createDataFrame(pdf.dropna())",
            "x = spark.createDataFrame(rows, ['a', 'b'])",
            "x = spark.createDataFrame([(1,)], ['a'])",
            "x = spark.createDataFrame([(u, 1) for u in 'ab'])",
            "x = spark.createDataFrame(big.collect())",
            "x = spark.createDataFrame(data=pdf).withColumn('b', F.lit(1)).select('b')",
            "x = spark.createDataFrame(pdf).union(spark.createDataFrame(rows))",
            // Catalog metadata commands are sized by the catalog, not the data.
            "x = spark.sql('SHOW TBLPROPERTIES t')",
            "x = spark.sql(f'DESCRIBE TABLE EXTENDED {name}').select('col_name')",
            "x = spark.sql('SHOW CATALOGS').collect()",
            "x = spark.sql('SHOW TABLES').toPandas()",
        ] {
            assert!(local(body), "{body}");
        }
        for body in [
            "x = big",
            "x = spark.createDataFrame(big.rdd)",
            "x = spark.createDataFrame(unknown_data)",
            "x = spark.createDataFrame(pdf).join(big, 'a')",
            "x = big.unionByName(spark.createDataFrame(pdf))",
            "x = spark.createDataFrame(pdf) if c else big",
            "x = spark.sql('SHOW PARTITIONS t')",
            "x = spark.sql('DESCRIBE HISTORY t').collect()",
            "x = spark.sql('SHOW TABLES').join(big, 'tableName')",
            "x = big.collect()",
        ] {
            assert!(!local(body), "{body}");
        }
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
        ] {
            assert_eq!(
                last(body, "spec").ty,
                Ty::Library(Lib::DataGenerator),
                "{body}"
            );
        }
        // TSDF has no `withColumn`: an unknown member, and not a DataFrame.
        assert!(last("spec = TSDF(sdf, ts_col='t').withColumn('a', 1)", "spec").is_unknown());
        // An unlisted class of another library: its builder stays unknown.
        let src = "import otherlib\nb = otherlib.Builder().withColumn('a', 1)\nb\n";
        assert!(ty_last(src, "b").is_unknown());
        // A lower-case function of another library may return a DataFrame: the
        // DataFrame-only method is still evidence. So is it on an unknown value.
        for body in [
            "y = mylib.load(spark).withColumn('a', F.lit(1))",
            "x = mylib.load(spark)\ny = x.withColumn('a', F.lit(1))",
            "y = get().withColumn('a', c)",
        ] {
            let t = last(body, "y");
            assert_eq!(
                (t.ty, t.confidence),
                (Ty::DataFrame, Confidence::Likely),
                "{body}"
            );
        }
        // A call shape no DataFrame accepts (`col` must be a Column; no other
        // keywords) is no evidence: `_, gen = make_gen(spark)` then
        // `gen = gen.withColumn(c, "int", **opts)` is a dbldatagen builder.
        for body in [
            "y = get().withColumn('a', 'int')",
            "y = get().withColumn('a', 1)",
            "y = get().withColumn('a', c, 'x')",
            "y = get().withColumn('a', template=t)",
            "y = get().withColumn('a', c, **opts)",
            "_, g = make(spark)\nfor c in cols:\n    g = g.withColumn(c, 'string', **opts)\ny = g",
        ] {
            assert!(last(body, "y").is_unknown(), "{body}");
        }
        let t = last("y = get().withColumn(colName='a', col=c)", "y");
        assert_eq!((t.ty, t.confidence), (Ty::DataFrame, Confidence::Likely));
        // Spark's own classes are not foreign.
        let src = "from pyspark.sql import SparkSession\ny = SparkSession.builder.getOrCreate().table('t').withColumn('a', 1)\ny\n";
        assert_eq!(ty_last(src, "y").ty, Ty::DataFrame);
    }

    #[test]
    fn known_library_members_are_dataframes() {
        let pre = "import dbldatagen as dg\nfrom tempo import TSDF\nfrom databricks.feature_engineering import FeatureEngineeringClient\nfrom databricks.feature_store import FeatureStoreClient\n";
        let last = |body: &str, name: &str| ty_last(&format!("{pre}{body}\n{name}\n"), name);
        for body in [
            "logs = dg.DataGenerator(spark, rows=10).withColumn('a', 'int').build()",
            "spec = dg.DataGenerator(spark, rows=10)\nspec = spec.withColumn('a', 'int')\nlogs = spec.build()",
            "spec = (dg.DataGenerator(spark, rows=10)\n    .withIdOutput()\n    .withColumn('a', 'int'))\nlogs = spec.build().drop('a')",
            "fe = FeatureEngineeringClient()\nlogs = fe.read_table(name='c.s.t')",
            "fe = FeatureStoreClient()\nlogs = fe.score_batch(model_uri='m', df=x)",
            "fe = FeatureEngineeringClient()\nts = fe.create_training_set(df=x, feature_lookups=[], label='y')\nlogs = ts.load_df()",
            "t = TSDF(sdf, ts_col='t', partition_cols=['k'])\nlogs = t.df",
            "logs = TSDF(sdf, ts_col='t').resample(freq='min', func='mean').df",
            "logs = TSDF(sdf, ts_col='t').describe()",
        ] {
            let t = last(body, "logs");
            assert_eq!(
                (t.ty, t.confidence),
                (Ty::DataFrame, Confidence::Certain),
                "{body}"
            );
        }
        let t = last(
            "logs = dg.DataGenerator(spark).build(withStreaming=True)",
            "logs",
        );
        assert!(t.facts.streaming);
        assert!(
            !last("logs = dg.DataGenerator(spark).build()", "logs")
                .facts
                .streaming
        );
        // Unlisted members are unknown; a non-Databricks feature store is not modelled.
        for body in [
            "logs = dg.DataGenerator(spark).build_order()",
            "logs = TSDF(sdf, ts_col='t').ts_col",
            "logs = FeatureEngineeringClient().get_table(name='t')",
        ] {
            assert!(last(body, "logs").is_unknown(), "{body}");
        }
        let src = "from azureml.featurestore import FeatureStoreClient\nfs = FeatureStoreClient()\nlogs = fs.read_table('t')\nlogs\n";
        assert!(ty_last(src, "logs").is_unknown());
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
        let src = "x = get()\ny = x.withColumn('a', F.lit(1))\ny\n";
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
        let src = "def load(spark):\n    return spark.read.table('t')\n\ndef clean(d):\n    return d.withColumn('a', F.lit(1))\n\nx = load(spark)\ny = clean(x)\nx\ny\n";
        assert!(ty_last(src, "x").is_dataframe());
        assert!(ty_last(src, "y").is_dataframe());
        // a call shape no DataFrame accepts is no usage evidence for a parameter
        let src = "def f(g):\n    g\n    return g.withColumn('a', 'int', minValue=1)\n";
        assert!(ty(src, "g").is_unknown());
        // pandas-only attributes make a parameter a pandas frame (no `df` heuristic)
        let src = "def f(df):\n    df\n    chunk = df.iloc[0:2]\n    return df.join(chunk)\n";
        assert_eq!(ty(src, "df").ty, Ty::PandasFrame);
        let src = "def f(df):\n    df\n    n = df.shape[0]\n";
        assert_eq!(ty(src, "df").ty, Ty::PandasFrame);
        // mixed evidence is no evidence: the name heuristic still applies
        let src = "def f(df):\n    df\n    x = df.loc\n    return df.withColumn('a', F.lit(1))\n";
        let t = ty(src, "df");
        assert_eq!((t.ty, t.confidence), (Ty::DataFrame, Confidence::Likely));
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
    fn pandas_use_blocks_dataframe_guesses() {
        // A DataFrame-only method on an untyped value is DataFrame evidence, unless
        // the value also has a pandas-only use (before or after the call).
        let src = "x = get()\nx.iloc[0]\na = x.withColumn('a', c)\ny = get()\nb = y.withColumn('a', c)\nz = get()\nd = z.withColumn('a', c)\nz.shape\nr_df = get()\nr_df.sort_values('a')\nm_df = get()\nm_df.loc[0]\nm_df.groupBy('k')\nq_df = get()\ndef f(p):\n    p.loc[0]\n    return p.groupBy('k')\na\nb\nd\nr_df\nm_df\nq_df\nf(get())\n";
        assert!(ty_last(src, "a").is_unknown());
        assert!(ty_last(src, "b").is_dataframe());
        assert!(ty_last(src, "d").is_unknown());
        // only pandas evidence also overrides the `*_df` name heuristic; mixed
        // evidence is none, so the name still counts (as for parameters)
        assert!(ty_last(src, "r_df").is_unknown());
        assert!(ty_last(src, "m_df").is_dataframe());
        assert!(ty_last(src, "q_df").is_dataframe());
        // mixed parameter evidence: no guess through the helper's return either
        assert!(ty_last(src, "f(get())").is_unknown());
    }

    #[test]
    fn fresh_builders() {
        let src = concat!(
            "from pyspark.sql import DataFrameReader\n",
            "def mk() -> DataFrameReader:\n    return spark.read\n",
            "def g(r: DataFrameReader):\n    r\n",
            "r1 = spark.read.format('csv').option('a', 1)\n",
            "w1 = df.write.mode('append').partitionBy('k')\n",
            "w2 = df.writeTo('t').using('delta')\n",
            "s1 = spark.readStream.format('kafka')\n",
            "w3 = df.writeStream.option('a', 1)\n",
            "r2 = mk().option('a', 1)\n",
            "if c:\n    r3 = spark.read.format('csv')\nelse:\n    r3 = spark.read.format('json')\n",
            "if c:\n    r4 = spark.read\nelse:\n    r4 = mk()\n",
            "r1\nw1\nw2\ns1\nw3\nr2\nr3\nr4\n",
        );
        let fresh = |n| ty_last(src, n).facts.fresh_builder;
        for n in ["r1", "w1", "w2", "s1", "w3", "r3"] {
            assert!(fresh(n), "{n}");
        }
        for n in ["r", "r2", "r4"] {
            assert!(!fresh(n), "{n}");
        }
        assert!(!ty_last(src, "df").facts.fresh_builder);
    }

    #[test]
    fn window_specs() {
        let src = "from pyspark.sql.window import Window as W\nw1 = W.orderBy('ts')\nw2 = W.partitionBy('k').orderBy('ts')\nw3 = w1.partitionBy('k')\nw4 = W.partitionBy()\nw1\nw2\nw3\nw4\n";
        let p = |n| ty_last(src, n).facts.win_partitioning;
        let w1 = ty_last(src, "w1");
        assert_eq!(w1.ty, Ty::WindowSpec);
        assert!(w1.facts.win_ordered);
        assert_eq!(p("w1"), Partitioning::Global);
        assert_eq!(p("w2"), Partitioning::Partitioned);
        assert_eq!(p("w3"), Partitioning::Partitioned);
        assert_eq!(p("w4"), Partitioning::Global);
    }

    #[test]
    fn window_global_needs_every_path() {
        let src = concat!(
            "from pyspark.sql import Window, WindowSpec\n",
            "def base(keys) -> WindowSpec:\n    w = Window.orderBy('ts')\n    if keys:\n        w = w.partitionBy(*keys)\n    return w\n",
            "def top(win: WindowSpec):\n    win\n",
            "w5 = Window.partitionBy(*keys).orderBy('t') if keys else Window.orderBy('t')\n",
            "w6 = Window.orderBy('t') if a else Window.orderBy('u').rowsBetween(-1, 0)\n",
            "w7 = base(keys)\nw5\nw6\nw7\n",
        );
        let p = |n| ty_last(src, n).facts.win_partitioning;
        assert_eq!(p("w5"), Partitioning::Unknown);
        assert_eq!(p("w6"), Partitioning::Global);
        assert_eq!(p("w7"), Partitioning::Unknown);
        assert_eq!(p("win"), Partitioning::Unknown);
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
