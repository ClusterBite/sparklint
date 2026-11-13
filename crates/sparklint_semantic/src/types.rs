//! The Spark type lattice (DESIGN §3.1, RULES_CATALOG §2).

pub use crate::facts::Facts;
use crate::library::Lib;

/// Element type of a Python list/iterator value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Elem {
    Unknown,
    Row,
    Str,
    DataFrame,
    Column,
    Other,
}

/// A small, `Copy` type for every value the oracle can place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ty {
    /// Nothing is known (may still be Spark).
    Unknown,

    // ---- Spark ----
    SessionBuilder,
    SparkSession,
    SparkContext,
    SqlContext,
    /// `pyspark.sql.DataFrame` (streaming-ness is a fact).
    DataFrame,
    Column,
    GroupedData,
    PandasCogroupedOps,
    WindowSpec,
    Rdd,
    Row,
    DataFrameReader,
    DataFrameWriter,
    DataFrameWriterV2,
    MergeIntoWriter,
    DataStreamReader,
    DataStreamWriter,
    StreamingQuery,
    NaFunctions,
    StatFunctions,
    Catalog,
    RuntimeConfig,
    UdfRegistration,
    /// A `UserDefinedFunction` (result of `udf(...)`, `@udf`, `spark.udf.register`).
    Udf,
    /// `StructType` / any `DataType`.
    DataType,
    /// pandas-on-Spark DataFrame / Series.
    PandasOnSparkFrame,
    /// A plain pandas DataFrame (known non-Spark; `from_spark` fact when from `toPandas`).
    PandasFrame,
    /// A value of a local data library that is known not to be a `pyspark.sql` object:
    /// what a method, attribute, index or operator of a pandas frame gives (Series,
    /// GroupBy, ndarray, scalar, another frame), a numpy / polars value, an untyped
    /// pandas-on-Spark result. Everything derived from it is `NonSpark` too, so no
    /// name or method-name heuristic turns it into a Spark DataFrame.
    NonSpark,
    DeltaTable,
    DbUtils,
    /// `pyspark.sql.Observation`: metrics filled in by the next action on an
    /// observed DataFrame (`df.observe(obs, ..)`).
    Observation,

    // ---- Python / misc (known non-Spark) ----
    Logger,
    Int,
    Float,
    Str,
    Bool,
    NoneTy,
    Dict,
    Tuple,
    Set,
    List(Elem),
    /// A Python function or lambda object.
    Function,
    /// An instance of a class defined in this module (payload: class scope id).
    Instance(u32),
    /// An object of another library whose Spark-producing members are known
    /// (`dbldatagen.DataGenerator`, `tempo.TSDF`, …; see `crate::library`).
    Library(Lib),
    /// Some other known, non-Spark value (module, class, opaque object).
    Object,
}

impl Ty {
    pub fn is_unknown(self) -> bool {
        matches!(self, Ty::Unknown)
    }

    pub fn is_dataframe(self) -> bool {
        matches!(self, Ty::DataFrame)
    }

    pub fn is_column(self) -> bool {
        matches!(self, Ty::Column)
    }

    /// A driver-side value of a local data library (pandas, numpy, polars).
    pub fn is_local_data(self) -> bool {
        matches!(self, Ty::PandasFrame | Ty::NonSpark)
    }

    /// Whether this is part of the Spark universe.
    pub fn is_spark(self) -> bool {
        matches!(
            self,
            Ty::SessionBuilder
                | Ty::SparkSession
                | Ty::SparkContext
                | Ty::SqlContext
                | Ty::DataFrame
                | Ty::Column
                | Ty::GroupedData
                | Ty::PandasCogroupedOps
                | Ty::WindowSpec
                | Ty::Rdd
                | Ty::Row
                | Ty::DataFrameReader
                | Ty::DataFrameWriter
                | Ty::DataFrameWriterV2
                | Ty::MergeIntoWriter
                | Ty::DataStreamReader
                | Ty::DataStreamWriter
                | Ty::StreamingQuery
                | Ty::NaFunctions
                | Ty::StatFunctions
                | Ty::Catalog
                | Ty::RuntimeConfig
                | Ty::UdfRegistration
                | Ty::Udf
                | Ty::DataType
                | Ty::PandasOnSparkFrame
                | Ty::DeltaTable
        )
    }

    /// A session-like receiver (`spark.read`, `spark.sql`, …).
    /// A reader or writer builder (`DataFrameReader`, `DataStreamReader`,
    /// `DataFrameWriter`, `DataFrameWriterV2`, `DataStreamWriter`).
    pub fn is_io_builder(self) -> bool {
        matches!(
            self,
            Ty::DataFrameReader
                | Ty::DataStreamReader
                | Ty::DataFrameWriter
                | Ty::DataFrameWriterV2
                | Ty::DataStreamWriter
        )
    }

    pub fn is_session(self) -> bool {
        matches!(self, Ty::SparkSession | Ty::SqlContext)
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Ty::Unknown => "Unknown",
            Ty::SessionBuilder => "SparkSession.Builder",
            Ty::SparkSession => "SparkSession",
            Ty::SparkContext => "SparkContext",
            Ty::SqlContext => "SQLContext",
            Ty::DataFrame => "DataFrame",
            Ty::Column => "Column",
            Ty::GroupedData => "GroupedData",
            Ty::PandasCogroupedOps => "PandasCogroupedOps",
            Ty::WindowSpec => "WindowSpec",
            Ty::Rdd => "RDD",
            Ty::Row => "Row",
            Ty::DataFrameReader => "DataFrameReader",
            Ty::DataFrameWriter => "DataFrameWriter",
            Ty::DataFrameWriterV2 => "DataFrameWriterV2",
            Ty::MergeIntoWriter => "MergeIntoWriter",
            Ty::DataStreamReader => "DataStreamReader",
            Ty::DataStreamWriter => "DataStreamWriter",
            Ty::StreamingQuery => "StreamingQuery",
            Ty::NaFunctions => "DataFrameNaFunctions",
            Ty::StatFunctions => "DataFrameStatFunctions",
            Ty::Catalog => "Catalog",
            Ty::RuntimeConfig => "RuntimeConfig",
            Ty::UdfRegistration => "UDFRegistration",
            Ty::Udf => "UserDefinedFunction",
            Ty::DataType => "DataType",
            Ty::PandasOnSparkFrame => "pyspark.pandas.DataFrame",
            Ty::PandasFrame => "pandas.DataFrame",
            Ty::NonSpark => "non-Spark data object",
            Ty::DeltaTable => "DeltaTable",
            Ty::DbUtils => "dbutils",
            Ty::Observation => "Observation",
            Ty::Logger => "Logger",
            Ty::Int => "int",
            Ty::Float => "float",
            Ty::Str => "str",
            Ty::Bool => "bool",
            Ty::NoneTy => "None",
            Ty::Dict => "dict",
            Ty::Tuple => "tuple",
            Ty::Set => "set",
            Ty::List(Elem::Row) => "list[Row]",
            Ty::List(Elem::Str) => "list[str]",
            Ty::List(Elem::DataFrame) => "list[DataFrame]",
            Ty::List(Elem::Column) => "list[Column]",
            Ty::List(_) => "list",
            Ty::Function => "function",
            Ty::Instance(_) => "object",
            Ty::Library(l) => l.display_name(),
            Ty::Object => "object",
        }
    }

    /// Map a class name used by `spec/pyspark_api.json` to a type.
    pub fn from_api_name(name: &str) -> Ty {
        match name {
            "Builder" => Ty::SessionBuilder,
            "SparkSession" => Ty::SparkSession,
            "SparkContext" => Ty::SparkContext,
            "SQLContext" => Ty::SqlContext,
            "DataFrame" => Ty::DataFrame,
            "Column" => Ty::Column,
            "GroupedData" => Ty::GroupedData,
            "PandasCogroupedOps" => Ty::PandasCogroupedOps,
            "WindowSpec" => Ty::WindowSpec,
            "RDD" => Ty::Rdd,
            "Row" => Ty::Row,
            "DataFrameReader" => Ty::DataFrameReader,
            "DataFrameWriter" => Ty::DataFrameWriter,
            "DataFrameWriterV2" => Ty::DataFrameWriterV2,
            "MergeIntoWriter" => Ty::MergeIntoWriter,
            "DataStreamReader" => Ty::DataStreamReader,
            "DataStreamWriter" => Ty::DataStreamWriter,
            "StreamingQuery" => Ty::StreamingQuery,
            "DataFrameNaFunctions" => Ty::NaFunctions,
            "DataFrameStatFunctions" => Ty::StatFunctions,
            "Catalog" => Ty::Catalog,
            "RuntimeConfig" => Ty::RuntimeConfig,
            "PandasOnSparkDataFrame" => Ty::PandasOnSparkFrame,
            "PandasDataFrame" => Ty::PandasFrame,
            "list[Row]" => Ty::List(Elem::Row),
            "list" => Ty::List(Elem::Unknown),
            "int" => Ty::Int,
            "float" => Ty::Float,
            "str" => Ty::Str,
            "bool" => Ty::Bool,
            "None" => Ty::NoneTy,
            _ => Ty::Unknown,
        }
    }

    /// The API-model class name for a Spark receiver type.
    /// Fully qualified class name of a Spark type (`pyspark.sql.DataFrame`), used for
    /// qualified method names (`crate::SparkCall::qualname`).
    pub fn qualified_class(self) -> Option<&'static str> {
        Some(match self {
            Ty::SessionBuilder => "pyspark.sql.SparkSession.Builder",
            Ty::SparkSession => "pyspark.sql.SparkSession",
            Ty::SqlContext => "pyspark.sql.SQLContext",
            Ty::SparkContext => "pyspark.SparkContext",
            Ty::DataFrame => "pyspark.sql.DataFrame",
            Ty::Column => "pyspark.sql.Column",
            Ty::GroupedData => "pyspark.sql.GroupedData",
            Ty::PandasCogroupedOps => "pyspark.sql.PandasCogroupedOps",
            Ty::WindowSpec => "pyspark.sql.WindowSpec",
            Ty::Rdd => "pyspark.RDD",
            Ty::Row => "pyspark.sql.Row",
            Ty::DataFrameReader => "pyspark.sql.DataFrameReader",
            Ty::DataFrameWriter => "pyspark.sql.DataFrameWriter",
            Ty::DataFrameWriterV2 => "pyspark.sql.DataFrameWriterV2",
            Ty::MergeIntoWriter => "pyspark.sql.MergeIntoWriter",
            Ty::DataStreamReader => "pyspark.sql.streaming.DataStreamReader",
            Ty::DataStreamWriter => "pyspark.sql.streaming.DataStreamWriter",
            Ty::StreamingQuery => "pyspark.sql.streaming.StreamingQuery",
            Ty::NaFunctions => "pyspark.sql.DataFrameNaFunctions",
            Ty::StatFunctions => "pyspark.sql.DataFrameStatFunctions",
            Ty::Catalog => "pyspark.sql.Catalog",
            Ty::RuntimeConfig => "pyspark.sql.conf.RuntimeConfig",
            Ty::UdfRegistration => "pyspark.sql.UDFRegistration",
            Ty::Udf => "pyspark.sql.udf.UserDefinedFunction",
            Ty::DataType => "pyspark.sql.types.DataType",
            Ty::PandasOnSparkFrame => "pyspark.pandas.DataFrame",
            Ty::DeltaTable => "delta.tables.DeltaTable",
            _ => return None,
        })
    }

    pub fn api_class(self) -> Option<&'static str> {
        Some(match self {
            Ty::SessionBuilder => "Builder",
            Ty::SparkSession | Ty::SqlContext => "SparkSession",
            Ty::SparkContext => "SparkContext",
            Ty::DataFrame => "DataFrame",
            Ty::Column => "Column",
            Ty::GroupedData => "GroupedData",
            Ty::PandasCogroupedOps => "PandasCogroupedOps",
            Ty::WindowSpec => "WindowSpec",
            Ty::Rdd => "RDD",
            Ty::Row => "Row",
            Ty::DataFrameReader => "DataFrameReader",
            Ty::DataFrameWriter => "DataFrameWriter",
            Ty::DataFrameWriterV2 => "DataFrameWriterV2",
            Ty::MergeIntoWriter => "MergeIntoWriter",
            Ty::DataStreamReader => "DataStreamReader",
            Ty::DataStreamWriter => "DataStreamWriter",
            Ty::StreamingQuery => "StreamingQuery",
            Ty::NaFunctions => "DataFrameNaFunctions",
            Ty::StatFunctions => "DataFrameStatFunctions",
            Ty::Catalog => "Catalog",
            Ty::RuntimeConfig => "RuntimeConfig",
            _ => return None,
        })
    }

    pub fn elem(self) -> Elem {
        match self {
            Ty::Row => Elem::Row,
            Ty::Str => Elem::Str,
            Ty::DataFrame => Elem::DataFrame,
            Ty::Column => Elem::Column,
            Ty::Unknown => Elem::Unknown,
            _ => Elem::Other,
        }
    }

    pub fn from_elem(e: Elem) -> Ty {
        match e {
            Elem::Row => Ty::Row,
            Elem::Str => Ty::Str,
            Elem::DataFrame => Ty::DataFrame,
            Elem::Column => Ty::Column,
            Elem::Unknown => Ty::Unknown,
            Elem::Other => Ty::Object,
        }
    }
}

/// How sure the oracle is about a type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Confidence {
    /// From name heuristics or usage evidence.
    Likely,
    /// From an annotation, an import, or a known origin.
    Certain,
}

/// A type with confidence and facts — what [`crate::SemanticModel::ty_of`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeInfo {
    pub ty: Ty,
    pub confidence: Confidence,
    pub facts: Facts,
}

impl Default for TypeInfo {
    fn default() -> Self {
        Self::UNKNOWN
    }
}

impl TypeInfo {
    pub const UNKNOWN: TypeInfo = TypeInfo {
        ty: Ty::Unknown,
        confidence: Confidence::Likely,
        facts: Facts::NONE,
    };

    pub fn certain(ty: Ty) -> Self {
        Self {
            ty,
            confidence: Confidence::Certain,
            facts: Facts::default(),
        }
    }

    pub fn likely(ty: Ty) -> Self {
        Self {
            ty,
            confidence: Confidence::Likely,
            facts: Facts::default(),
        }
    }

    pub fn with_facts(mut self, facts: Facts) -> Self {
        self.facts = facts;
        self
    }

    pub fn with_confidence(mut self, c: Confidence) -> Self {
        self.confidence = self.confidence.min(c);
        self
    }

    pub fn is_unknown(&self) -> bool {
        self.ty.is_unknown()
    }

    pub fn is_dataframe(&self) -> bool {
        self.ty.is_dataframe()
    }

    pub fn is_column(&self) -> bool {
        self.ty.is_column()
    }

    pub fn is_certain(&self) -> bool {
        self.confidence == Confidence::Certain
    }

    /// Bounded to a known number of rows.
    pub fn bounded(&self) -> Option<u64> {
        self.facts.bounded
    }

    /// Known to fit in driver memory: a known row bound, or driver-built / catalog
    /// metadata data ([`crate::facts::Local`]).
    pub fn driver_sized(&self) -> bool {
        self.facts.bounded.is_some() || self.facts.local
    }

    /// Join of two branch types (if/else, loop back edges, multiple returns).
    pub fn join(self, other: TypeInfo) -> TypeInfo {
        if self.ty == other.ty {
            return TypeInfo {
                ty: self.ty,
                confidence: self.confidence.min(other.confidence),
                facts: self.facts.meet(other.facts),
            };
        }
        match (self.ty, other.ty) {
            // Optional[X]: None joins to X without losing confidence.
            (Ty::NoneTy, _) => other,
            (_, Ty::NoneTy) => self,
            (Ty::Unknown, _) => other.with_confidence(Confidence::Likely),
            (_, Ty::Unknown) => self.with_confidence(Confidence::Likely),
            // A pandas frame on one branch, another local data value on the other.
            (a, b) if a.is_local_data() && b.is_local_data() => TypeInfo::certain(Ty::NonSpark)
                .with_confidence(self.confidence.min(other.confidence)),
            (Ty::List(_), Ty::List(_)) => TypeInfo {
                ty: Ty::List(Elem::Unknown),
                confidence: self.confidence.min(other.confidence),
                facts: self.facts.meet(other.facts),
            },
            _ => TypeInfo::UNKNOWN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_rules() {
        let df = TypeInfo::certain(Ty::DataFrame);
        let none = TypeInfo::certain(Ty::NoneTy);
        assert_eq!(df.join(none), df);
        assert_eq!(none.join(df), df);
        let j = df.join(TypeInfo::UNKNOWN);
        assert_eq!(j.ty, Ty::DataFrame);
        assert_eq!(j.confidence, Confidence::Likely);
        assert!(df.join(TypeInfo::certain(Ty::PandasFrame)).is_unknown());
        let local = TypeInfo::certain(Ty::PandasFrame).join(TypeInfo::certain(Ty::NonSpark));
        assert_eq!(local, TypeInfo::certain(Ty::NonSpark));
        let b = TypeInfo::certain(Ty::DataFrame).with_facts(Facts {
            bounded: Some(10),
            ..Facts::default()
        });
        assert_eq!(b.join(df).bounded(), None);
        assert_eq!(b.join(b).bounded(), Some(10));
    }
}
