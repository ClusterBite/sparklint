//! The PySpark API model (`spec/pyspark_api.json`, DESIGN §3.3) plus a hand-written
//! overlay for semantics the annotations don't carry (actions, RDD generics, Builder
//! chaining, Databricks extras).

use std::sync::LazyLock;

use rustc_hash::FxHashMap;
use serde::Deserialize;

use crate::types::{Elem, Ty};

static API_JSON: &str = include_str!("../../../spec/pyspark_api.json");

#[derive(Debug, Deserialize)]
struct RawMethod {
    returns: Option<String>,
    #[serde(default)]
    property: bool,
    #[serde(default, rename = "static")]
    is_static: bool,
    #[serde(default)]
    deprecated: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawApi {
    classes: FxHashMap<String, FxHashMap<String, RawMethod>>,
    functions: FxHashMap<String, RawMethod>,
    pyspark_version: String,
}

/// One method/property of a modelled class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MethodInfo {
    pub returns: Ty,
    pub property: bool,
    pub is_static: bool,
    pub deprecated: Option<String>,
}

/// The parsed API model.
#[derive(Debug)]
pub struct ApiModel {
    classes: FxHashMap<String, FxHashMap<String, MethodInfo>>,
    functions: FxHashMap<String, Ty>,
    pub pyspark_version: String,
}

static API: LazyLock<ApiModel> = LazyLock::new(ApiModel::load);

/// The process-wide API model (parsed once).
pub fn api() -> &'static ApiModel {
    &API
}

impl ApiModel {
    fn load() -> Self {
        let raw: RawApi = match serde_json::from_str(API_JSON) {
            Ok(r) => r,
            // The spec is compiled in and covered by tests; an empty model is the
            // safest degradation if it is ever malformed.
            Err(_) => RawApi {
                classes: FxHashMap::default(),
                functions: FxHashMap::default(),
                pyspark_version: "unknown".into(),
            },
        };
        let mut classes: FxHashMap<String, FxHashMap<String, MethodInfo>> = FxHashMap::default();
        for (cls, methods) in raw.classes {
            let mut out = FxHashMap::default();
            for (name, m) in methods {
                let returns = m
                    .returns
                    .as_deref()
                    .map(Ty::from_api_name)
                    .unwrap_or(Ty::Unknown);
                out.insert(
                    name,
                    MethodInfo {
                        returns,
                        property: m.property,
                        is_static: m.is_static,
                        deprecated: m.deprecated,
                    },
                );
            }
            classes.insert(cls, out);
        }
        let functions = raw
            .functions
            .into_iter()
            .map(|(k, v)| {
                let t = v
                    .returns
                    .as_deref()
                    .map(Ty::from_api_name)
                    .unwrap_or(Ty::Unknown);
                (k, t)
            })
            .collect();
        let mut model = ApiModel {
            classes,
            functions,
            pyspark_version: raw.pyspark_version,
        };
        model.apply_overlay();
        model
    }

    fn set(&mut self, cls: &str, name: &str, returns: Ty, property: bool) {
        let entry = self.classes.entry(cls.to_string()).or_default();
        let m = entry.entry(name.to_string()).or_insert(MethodInfo {
            returns,
            property,
            is_static: false,
            deprecated: None,
        });
        m.returns = returns;
        m.property = property;
    }

    fn apply_overlay(&mut self) {
        // SparkSession
        self.set("SparkSession", "builder", Ty::SessionBuilder, true);
        self.set("SparkSession", "conf", Ty::RuntimeConfig, true);
        self.set("SparkSession", "catalog", Ty::Catalog, true);
        self.set("SparkSession", "udf", Ty::UdfRegistration, true);
        self.set("SparkSession", "version", Ty::Str, true);
        self.set("SparkSession", "streams", Ty::Object, true);
        // Builder: getOrCreate/create come from the spec; everything else chains.
        self.set("Builder", "getOrCreate", Ty::SparkSession, false);
        self.set("Builder", "create", Ty::SparkSession, false);
        // DataFrame overlay
        self.set("DataFrame", "toPandas", Ty::PandasFrame, false);
        self.set("DataFrame", "toLocalIterator", Ty::List(Elem::Row), false);
        self.set("DataFrame", "columns", Ty::List(Elem::Str), true);
        self.set("DataFrame", "dtypes", Ty::List(Elem::Other), true);
        self.set("DataFrame", "schema", Ty::DataType, true);
        self.set("DataFrame", "randomSplit", Ty::List(Elem::DataFrame), false);
        self.set(
            "DataFrame",
            "to_pandas_on_spark",
            Ty::PandasOnSparkFrame,
            false,
        );
        self.set("DataFrame", "pandas_api", Ty::PandasOnSparkFrame, false);
        self.set("DataFrame", "is_cached", Ty::Bool, true);
        self.set("DataFrame", "display", Ty::NoneTy, false);
        // RDD: the spec loses the generic parameter; transformations return RDD.
        let rdd_non_rdd = [
            "aggregate",
            "collectAsMap",
            "countByKey",
            "countByValue",
            "first",
            "fold",
            "max",
            "min",
            "reduce",
            "sum",
            "stats",
            "histogram",
            "treeAggregate",
            "treeReduce",
            "getStorageLevel",
            "name",
            "toDebugString",
            "getCheckpointFile",
            "getResourceProfile",
            "meanApprox",
            "sumApprox",
        ];
        if let Some(rdd) = self.classes.get_mut("RDD") {
            for (name, m) in rdd.iter_mut() {
                if m.returns == Ty::Unknown && !rdd_non_rdd.contains(&name.as_str()) {
                    m.returns = Ty::Rdd;
                }
            }
        }
        self.set("RDD", "collect", Ty::List(Elem::Unknown), false);
        self.set("RDD", "toLocalIterator", Ty::List(Elem::Unknown), false);
        for m in [
            "parallelize",
            "textFile",
            "emptyRDD",
            "range",
            "union",
            "wholeTextFiles",
            "binaryFiles",
            "binaryRecords",
            "pickleFile",
            "sequenceFile",
            "hadoopFile",
            "hadoopRDD",
            "newAPIHadoopFile",
            "newAPIHadoopRDD",
        ] {
            self.set("SparkContext", m, Ty::Rdd, false);
        }
        self.set("Row", "asDict", Ty::Dict, false);
        self.set("UDFRegistration", "register", Ty::Udf, false);
        self.set("UDFRegistration", "registerJavaFunction", Ty::NoneTy, false);
        // DeltaTable (delta-spark), modelled by hand.
        for (m, t) in [
            ("toDF", Ty::DataFrame),
            ("alias", Ty::DeltaTable),
            ("history", Ty::DataFrame),
            ("detail", Ty::DataFrame),
            ("optimize", Ty::Object),
            ("vacuum", Ty::DataFrame),
            ("merge", Ty::Object),
            ("delete", Ty::NoneTy),
            ("update", Ty::NoneTy),
        ] {
            self.set("DeltaTable", m, t, false);
        }
        // pandas-on-Spark
        for (m, t) in [
            ("to_spark", Ty::DataFrame),
            ("to_pandas", Ty::PandasFrame),
            ("to_numpy", Ty::Object),
        ] {
            self.set("PandasOnSparkFrame", m, t, false);
        }
    }

    pub fn method(&self, class: &str, name: &str) -> Option<&MethodInfo> {
        self.classes.get(class)?.get(name)
    }

    /// Method/property lookup by receiver type.
    pub fn method_of(&self, ty: Ty, name: &str) -> Option<&MethodInfo> {
        let cls = match ty {
            Ty::UdfRegistration => "UDFRegistration",
            Ty::DeltaTable => "DeltaTable",
            Ty::PandasOnSparkFrame => "PandasOnSparkFrame",
            other => other.api_class()?,
        };
        self.method(cls, name)
    }

    pub fn has_class_member(&self, class: &str, name: &str) -> bool {
        self.method(class, name).is_some()
    }

    /// Return type of `pyspark.sql.functions.<name>`; `None` if not a known function.
    pub fn function(&self, name: &str) -> Option<Ty> {
        self.functions.get(name).copied()
    }

    pub fn class_names(&self) -> impl Iterator<Item = &str> {
        self.classes.keys().map(String::as_str)
    }
}

/// Kind of eager work triggered by an action (RULES_CATALOG ACT001).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActionKind {
    Job,
    DriverCollect,
    Write,
    StreamStart,
    Display,
    Checkpoint,
}

impl ActionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActionKind::Job => "job",
            ActionKind::DriverCollect => "driver-collect",
            ActionKind::Write => "write",
            ActionKind::StreamStart => "stream-start",
            ActionKind::Display => "display",
            ActionKind::Checkpoint => "checkpoint",
        }
    }
}

/// Whether `receiver.method(...)` is a Spark action, and of which kind.
pub fn action_kind(receiver: Ty, method: &str) -> Option<ActionKind> {
    use ActionKind::*;
    match receiver {
        Ty::DataFrame => match method {
            "count" | "first" | "isEmpty" | "foreach" | "foreachPartition" => Some(Job),
            "collect" | "toPandas" | "take" | "head" | "tail" | "toLocalIterator" | "toArrow" => {
                Some(DriverCollect)
            }
            "show" | "display" => Some(Display),
            "checkpoint" => Some(Checkpoint),
            _ => None,
        },
        Ty::Rdd => match method {
            "count" | "first" | "isEmpty" | "foreach" | "foreachPartition" | "reduce" | "fold"
            | "aggregate" | "sum" | "max" | "min" | "mean" | "stats" | "countByKey"
            | "countByValue" | "treeReduce" | "treeAggregate" => Some(Job),
            "collect" | "take" | "top" | "takeOrdered" | "takeSample" | "collectAsMap"
            | "toLocalIterator" => Some(DriverCollect),
            "saveAsTextFile"
            | "saveAsPickleFile"
            | "saveAsHadoopFile"
            | "saveAsSequenceFile"
            | "saveAsNewAPIHadoopFile" => Some(Write),
            _ => None,
        },
        Ty::DataFrameWriter => match method {
            "save" | "saveAsTable" | "insertInto" | "parquet" | "csv" | "json" | "orc" | "text"
            | "jdbc" | "xml" => Some(Write),
            _ => None,
        },
        Ty::DataFrameWriterV2 => match method {
            "create"
            | "replace"
            | "createOrReplace"
            | "append"
            | "overwrite"
            | "overwritePartitions" => Some(Write),
            _ => None,
        },
        Ty::MergeIntoWriter => (method == "merge").then_some(Write),
        Ty::DataStreamWriter => matches!(method, "start" | "toTable").then_some(StreamStart),
        Ty::PandasOnSparkFrame => {
            matches!(method, "to_pandas" | "to_numpy").then_some(DriverCollect)
        }
        _ => None,
    }
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

/// Canonicalise a dotted qualified name: collapse PySpark submodule paths and Spark
/// Connect paths to the public names (`pyspark.sql.window.Window` → `pyspark.sql.Window`).
pub fn canonical_qualified(q: &str) -> String {
    const PREFIXES: &[(&str, &str)] = &[
        (
            "pyspark.sql.connect.functions.builtin",
            "pyspark.sql.functions",
        ),
        ("pyspark.sql.connect.functions", "pyspark.sql.functions"),
        ("pyspark.sql.functions.builtin", "pyspark.sql.functions"),
        (
            "pyspark.sql.connect.dataframe.DataFrame",
            "pyspark.sql.DataFrame",
        ),
        ("pyspark.sql.dataframe.DataFrame", "pyspark.sql.DataFrame"),
        (
            "pyspark.sql.classic.dataframe.DataFrame",
            "pyspark.sql.DataFrame",
        ),
        (
            "pyspark.sql.connect.session.SparkSession",
            "pyspark.sql.SparkSession",
        ),
        (
            "pyspark.sql.session.SparkSession",
            "pyspark.sql.SparkSession",
        ),
        ("pyspark.sql.connect.column.Column", "pyspark.sql.Column"),
        ("pyspark.sql.column.Column", "pyspark.sql.Column"),
        ("pyspark.sql.classic.column.Column", "pyspark.sql.Column"),
        ("pyspark.sql.window.WindowSpec", "pyspark.sql.WindowSpec"),
        ("pyspark.sql.window.Window", "pyspark.sql.Window"),
        ("pyspark.sql.connect.window.Window", "pyspark.sql.Window"),
        ("pyspark.sql.group.GroupedData", "pyspark.sql.GroupedData"),
        (
            "pyspark.sql.readwriter.DataFrameReader",
            "pyspark.sql.DataFrameReader",
        ),
        (
            "pyspark.sql.readwriter.DataFrameWriterV2",
            "pyspark.sql.DataFrameWriterV2",
        ),
        (
            "pyspark.sql.readwriter.DataFrameWriter",
            "pyspark.sql.DataFrameWriter",
        ),
        ("pyspark.sql.types.Row", "pyspark.sql.Row"),
        ("pyspark.sql.context.SQLContext", "pyspark.sql.SQLContext"),
        (
            "pyspark.sql.udf.UserDefinedFunction",
            "pyspark.sql.UserDefinedFunction",
        ),
        ("pyspark.context.SparkContext", "pyspark.SparkContext"),
        ("pyspark.rdd.RDD", "pyspark.RDD"),
        (
            "pyspark.sql.streaming.query.StreamingQuery",
            "pyspark.sql.streaming.StreamingQuery",
        ),
        (
            "pyspark.sql.streaming.readwriter.DataStreamReader",
            "pyspark.sql.streaming.DataStreamReader",
        ),
        (
            "pyspark.sql.streaming.readwriter.DataStreamWriter",
            "pyspark.sql.streaming.DataStreamWriter",
        ),
        (
            "pyspark.sql.pandas.functions.pandas_udf",
            "pyspark.sql.functions.pandas_udf",
        ),
        ("pyspark.sql.pandas.functions", "pyspark.sql.functions"),
        ("pyspark.sql.udf.udf", "pyspark.sql.functions.udf"),
        (
            "databricks.connect.session.DatabricksSession",
            "databricks.connect.DatabricksSession",
        ),
        ("pyspark.pandas.frame.DataFrame", "pyspark.pandas.DataFrame"),
        ("pyspark.pandas.series.Series", "pyspark.pandas.Series"),
        (
            "pyspark.pandas.config.set_option",
            "pyspark.pandas.set_option",
        ),
        (
            "pyspark.pandas.config.option_context",
            "pyspark.pandas.option_context",
        ),
        ("pandas.core.frame.DataFrame", "pandas.DataFrame"),
        ("databricks.koalas", "pyspark.pandas"),
    ];
    for (from, to) in PREFIXES {
        if let Some(rest) = q.strip_prefix(from)
            && (rest.is_empty() || rest.starts_with('.'))
        {
            return format!("{to}{rest}");
        }
    }
    q.to_string()
}

/// Type of a value denoted by a (canonical) qualified name used as an *expression*
/// (not called), e.g. `pyspark.sql.SparkSession.builder`.
pub fn qualified_value_type(q: &str) -> Option<Ty> {
    Some(match q {
        "pyspark.sql.SparkSession.builder" | "databricks.connect.DatabricksSession.builder" => {
            Ty::SessionBuilder
        }
        "databricks.sdk.runtime.spark" => Ty::SparkSession,
        "databricks.sdk.runtime.dbutils" => Ty::DbUtils,
        "databricks.sdk.runtime.sc" => Ty::SparkContext,
        "databricks.sdk.runtime.sqlContext" => Ty::SqlContext,
        "pyspark.sql.Window.unboundedPreceding"
        | "pyspark.sql.Window.unboundedFollowing"
        | "pyspark.sql.Window.currentRow" => Ty::Int,
        "loguru.logger" => Ty::Logger,
        _ => return None,
    })
}

/// Map a (canonical) qualified class name used in an *annotation* to a type.
pub fn annotation_type(q: &str) -> Option<Ty> {
    Some(match q {
        "pyspark.sql.DataFrame" => Ty::DataFrame,
        "pyspark.sql.Column" | "pyspark.sql.column.Column" => Ty::Column,
        "pyspark.sql.SparkSession" | "databricks.connect.DatabricksSession" => Ty::SparkSession,
        "pyspark.sql.GroupedData" => Ty::GroupedData,
        "pyspark.sql.WindowSpec" => Ty::WindowSpec,
        "pyspark.sql.Row" => Ty::Row,
        "pyspark.RDD" => Ty::Rdd,
        "pyspark.SparkContext" => Ty::SparkContext,
        "pyspark.sql.SQLContext" => Ty::SqlContext,
        "pyspark.sql.DataFrameReader" => Ty::DataFrameReader,
        "pyspark.sql.DataFrameWriter" => Ty::DataFrameWriter,
        "pyspark.sql.DataFrameWriterV2" => Ty::DataFrameWriterV2,
        "pyspark.sql.streaming.StreamingQuery" => Ty::StreamingQuery,
        "pyspark.sql.streaming.DataStreamReader" => Ty::DataStreamReader,
        "pyspark.sql.streaming.DataStreamWriter" => Ty::DataStreamWriter,
        "pyspark.sql.UserDefinedFunction" => Ty::Udf,
        "pyspark.pandas.DataFrame" | "pyspark.pandas.Series" => Ty::PandasOnSparkFrame,
        "pandas.DataFrame" | "pandas.Series" => Ty::PandasFrame,
        "delta.tables.DeltaTable" | "delta.DeltaTable" => Ty::DeltaTable,
        "logging.Logger" | "logging.LoggerAdapter" => Ty::Logger,
        "builtins.int" => Ty::Int,
        "builtins.float" => Ty::Float,
        "builtins.str" => Ty::Str,
        "builtins.bool" => Ty::Bool,
        "builtins.dict" | "typing.Dict" | "typing.Mapping" => Ty::Dict,
        "builtins.tuple" | "typing.Tuple" => Ty::Tuple,
        "builtins.set" | "typing.Set" => Ty::Set,
        "builtins.list" | "typing.List" | "typing.Sequence" | "typing.Iterable" => {
            Ty::List(Elem::Unknown)
        }
        "typing.Any" => Ty::Unknown,
        _ => {
            if q.starts_with("pyspark.sql.types.") {
                Ty::DataType
            } else {
                return None;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_loads_and_overlay_applies() {
        let a = api();
        assert_eq!(a.pyspark_version, "4.2.0");
        assert_eq!(
            a.method_of(Ty::DataFrame, "groupBy").unwrap().returns,
            Ty::GroupedData
        );
        assert_eq!(
            a.method_of(Ty::GroupedData, "agg").unwrap().returns,
            Ty::DataFrame
        );
        let w = a.method_of(Ty::DataFrame, "write").unwrap();
        assert!(w.property);
        assert_eq!(w.returns, Ty::DataFrameWriter);
        assert_eq!(
            a.method_of(Ty::SparkSession, "builder").unwrap().returns,
            Ty::SessionBuilder
        );
        assert_eq!(a.method_of(Ty::Rdd, "map").unwrap().returns, Ty::Rdd);
        assert_eq!(a.function("col"), Some(Ty::Column));
        assert_eq!(a.function("broadcast"), Some(Ty::DataFrame));
        assert_eq!(a.function("not_a_function"), None);
        assert!(a.method("Window", "orderBy").unwrap().is_static);
    }

    #[test]
    fn canonical_names() {
        assert_eq!(
            canonical_qualified("pyspark.sql.window.Window"),
            "pyspark.sql.Window"
        );
        assert_eq!(
            canonical_qualified("pyspark.sql.connect.functions.col"),
            "pyspark.sql.functions.col"
        );
        assert_eq!(
            canonical_qualified("pyspark.sql.functionsx"),
            "pyspark.sql.functionsx"
        );
    }

    #[test]
    fn actions() {
        assert_eq!(
            action_kind(Ty::DataFrame, "collect"),
            Some(ActionKind::DriverCollect)
        );
        assert_eq!(
            action_kind(Ty::DataFrameWriter, "saveAsTable"),
            Some(ActionKind::Write)
        );
        assert_eq!(action_kind(Ty::DataFrame, "select"), None);
        assert_eq!(action_kind(Ty::List(Elem::Unknown), "count"), None);
    }
}
