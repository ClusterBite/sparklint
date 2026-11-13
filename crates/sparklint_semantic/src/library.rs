//! Classes of other libraries that hand out Spark DataFrames: what their methods and
//! attributes return, by qualified class name. Only documented, annotated return types
//! are listed; any other member is unknown (never guessed from a name).
//!
//! - dbldatagen `DataGenerator`: `with*` builder methods return the generator,
//!   `build()` returns a DataFrame.
//! - Databricks Feature Engineering / Feature Store APIs: `read_table()` and
//!   `score_batch()` return a DataFrame, `create_training_set()` a `TrainingSet` whose
//!   `load_df()` returns a DataFrame.
//! - tempo `TSDF`: the `.df` attribute and `describe()` / `extractStateIntervals()` are
//!   DataFrames; the time-series operations return another `TSDF`.

use crate::types::Ty;

/// A class of another library whose members the oracle knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lib {
    /// `dbldatagen.DataGenerator`.
    DataGenerator,
    /// `databricks.feature_engineering.FeatureEngineeringClient` /
    /// `databricks.feature_store.FeatureStoreClient`.
    FeatureClient,
    /// What `create_training_set()` returns.
    TrainingSet,
    /// `tempo.TSDF`.
    Tsdf,
}

impl Lib {
    pub fn display_name(self) -> &'static str {
        match self {
            Lib::DataGenerator => "dbldatagen.DataGenerator",
            Lib::FeatureClient => "FeatureEngineeringClient",
            Lib::TrainingSet => "TrainingSet",
            Lib::Tsdf => "tempo.TSDF",
        }
    }
}

/// The class a qualified constructor call creates (`dg.DataGenerator(..)` →
/// `dbldatagen.DataGenerator`).
pub(crate) fn constructor(q: &str) -> Option<Lib> {
    Some(match q {
        "dbldatagen.DataGenerator" | "dbldatagen.data_generator.DataGenerator" => {
            Lib::DataGenerator
        }
        "databricks.feature_engineering.FeatureEngineeringClient"
        | "databricks.feature_engineering.client.FeatureEngineeringClient"
        | "databricks.feature_store.FeatureStoreClient"
        | "databricks.feature_store.client.FeatureStoreClient" => Lib::FeatureClient,
        "tempo.TSDF" | "tempo.tsdf.TSDF" => Lib::Tsdf,
        _ => return None,
    })
}

/// What `lib.m(..)` returns, if known.
pub(crate) fn method(lib: Lib, m: &str) -> Option<Ty> {
    let lib_ty = Ty::Library(lib);
    Some(match lib {
        Lib::DataGenerator => match m {
            "build" => Ty::DataFrame,
            "withRowCount" | "withIdOutput" | "withSchema" | "withColumnSpecs"
            | "withColumnSpec" | "withColumn" | "withStructColumn" | "withConstraint"
            | "withConstraints" | "withSqlConstraint" | "option" | "options" => lib_ty,
            _ => return None,
        },
        Lib::FeatureClient => match m {
            "read_table" | "score_batch" => Ty::DataFrame,
            "create_training_set" => Ty::Library(Lib::TrainingSet),
            _ => return None,
        },
        Lib::TrainingSet => match m {
            "load_df" => Ty::DataFrame,
            _ => return None,
        },
        Lib::Tsdf => match m {
            "describe" | "extractStateIntervals" => Ty::DataFrame,
            "select" | "at" | "before" | "atOrBefore" | "after" | "atOrAfter" | "between"
            | "earliest" | "latest" | "priorTo" | "subsequentTo" | "asofJoin"
            | "withPartitionCols" | "vwap" | "EMA" | "withRangeStats" | "withGroupedStats"
            | "resample" | "interpolate" | "calc_bars" | "fourier_transform" => lib_ty,
            _ => return None,
        },
    })
}

/// What the attribute `lib.a` is, if known.
pub(crate) fn attribute(lib: Lib, a: &str) -> Option<Ty> {
    match (lib, a) {
        (Lib::Tsdf, "df") => Some(Ty::DataFrame),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_members() {
        assert_eq!(
            constructor("dbldatagen.DataGenerator"),
            Some(Lib::DataGenerator)
        );
        assert_eq!(constructor("azureml.featurestore.FeatureStoreClient"), None);
        assert_eq!(constructor("tempo.tsdf.TSDF"), Some(Lib::Tsdf));
        assert_eq!(method(Lib::DataGenerator, "build"), Some(Ty::DataFrame));
        assert_eq!(
            method(Lib::DataGenerator, "withColumn"),
            Some(Ty::Library(Lib::DataGenerator))
        );
        assert_eq!(method(Lib::DataGenerator, "build_order"), None);
        assert_eq!(
            method(Lib::FeatureClient, "create_training_set"),
            Some(Ty::Library(Lib::TrainingSet))
        );
        assert_eq!(method(Lib::TrainingSet, "load_df"), Some(Ty::DataFrame));
        // `withLookbackFeatures` returns a DataFrame or a TSDF: unknown.
        assert_eq!(method(Lib::Tsdf, "withLookbackFeatures"), None);
        assert_eq!(method(Lib::Tsdf, "show"), None);
        assert_eq!(attribute(Lib::Tsdf, "df"), Some(Ty::DataFrame));
        assert_eq!(attribute(Lib::Tsdf, "ts_col"), None);
    }
}
