//! Dataflow facts the oracle attaches to a value: one row per fact.
//!
//! Each fact is a [`Fact`] row stating, in one place,
//! - its value when nothing is known ([`Fact::UNKNOWN`], also the start of every
//!   value whose origin sets nothing),
//! - how it merges where control-flow paths meet ([`Fact::merge`]: if/else, loop
//!   back edges, several `return`s),
//! - what one DataFrame, RDD or WindowSpec step does to it ([`Fact::step`]),
//! - and (in its doc comment) where it starts.
//!
//! The [`facts!`] table below turns the rows into the [`Facts`] struct, its
//! [`Facts::meet`], [`Facts::NONE`] and [`Facts::after`]. A new fact is one row
//! type + one table line; `infer.rs` only says *which* [`Step`] a call is.

use std::hash::{Hash, Hasher};

use ruff_text_size::TextRange;

use crate::types::Ty;

/// One oracle fact.
pub trait Fact {
    type Value: Copy;
    /// The value when nothing is known (the default of every value).
    const UNKNOWN: Self::Value;
    /// The fact where two control-flow paths meet.
    fn merge(a: Self::Value, b: Self::Value) -> Self::Value;
    /// The fact after one DataFrame / RDD / WindowSpec step.
    fn step(v: Self::Value, s: &Step) -> Self::Value;
}

/// A step from one DataFrame (or RDD) to the next, or from one WindowSpec to the next,
/// as far as the facts are concerned. `infer.rs` classifies the call; the rows decide.
/// RDD steps reuse the DataFrame variants (`filter` is [`Step::Reduce`], `coalesce`
/// [`Step::Keep`], `union` [`Step::Combine`], `map` [`Step::Other`]) plus
/// [`Step::Values`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    // ---- DataFrame -> DataFrame (and DataFrame property access)
    /// `limit(n)` (`u64::MAX` for a non-literal `n`).
    Limit(u64),
    /// One row per call: `agg(..)`, `select(<aggregates only>)`.
    Aggregate,
    /// `describe()` / `summary()`: at most 10 rows.
    Describe,
    /// Row-reducing: filter/where/distinct/dropDuplicates/sample/dropna/intersect/subtract.
    Reduce,
    /// `cache()` / `persist()` / `checkpoint()` / `localCheckpoint()`.
    Cache,
    /// `join` / `crossJoin` / `union*` / `lateralJoin` with the first argument's facts;
    /// `keeps_columns`: the result has the receiver's columns (a union, a semi / anti
    /// join).
    Combine {
        union: bool,
        other: Option<Facts>,
        keeps_columns: bool,
    },
    /// Another rows-only step: the receiver's columns, in order (`orderBy`,
    /// `repartition`, `coalesce`, `alias`, `hint`, `withWatermark`, `unpersist`).
    Keep,
    /// A `select(..)` whose arguments say which columns it picks.
    Project(Columns),
    /// Any other DataFrame -> DataFrame step (`withColumn`, `select`, a property, ...).
    Other,
    // ---- RDD -> RDD
    /// The second item of each pair: `rdd.values()`, `rdd.map(lambda kv: kv[1])`.
    Values,
    // ---- WindowSpec -> WindowSpec
    /// `partitionBy(<at least one key>)`.
    PartitionBy,
    /// `orderBy(..)`.
    OrderBy,
    /// `rowsBetween` / `rangeBetween`; `growing` = starts unbounded preceding and
    /// ends at a finite offset.
    Frame { growing: bool },
}

impl Step {
    fn is_window(&self) -> bool {
        matches!(self, Step::PartitionBy | Step::OrderBy | Step::Frame { .. })
    }
}

// ------------------------------------------------------------------- columns

/// The identity of an ordered column list: the names of a literal list
/// (`select("a", "b")`, `select(["a", "b"])`), or the variable holding the list
/// (`select(cols)`: the name and the bindings that reach it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnList(u64);

impl ColumnList {
    /// A literal list of column names, in order.
    pub fn names<'s>(names: impl IntoIterator<Item = &'s str>) -> Self {
        let mut h = std::hash::DefaultHasher::new();
        0u8.hash(&mut h);
        for n in names {
            n.hash(&mut h);
        }
        Self(h.finish())
    }

    /// The list a name holds, identified by the name and the bindings reaching it.
    pub fn variable(name: &str, bindings: &[TextRange]) -> Self {
        let mut h = std::hash::DefaultHasher::new();
        1u8.hash(&mut h);
        name.hash(&mut h);
        bindings.hash(&mut h);
        Self(h.finish())
    }
}

/// Where a frame whose columns are not spelled out came from: two frames with the
/// same origin have the same columns in the same order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Origin {
    /// The value of this expression (a read, a call, an aggregate, ...).
    Expr(TextRange),
    /// The value of a name reached by these bindings (a parameter, a loop variable, a
    /// name bound on several paths), or of an attribute chain with no calls
    /// (`self.df`, by its dotted text): a hash.
    Name(u64),
}

impl Origin {
    /// A name reached by `bindings` (their target ranges).
    pub fn bindings(name: &str, bindings: &[TextRange]) -> Self {
        let mut h = std::hash::DefaultHasher::new();
        0u8.hash(&mut h);
        name.hash(&mut h);
        bindings.hash(&mut h);
        Origin::Name(h.finish())
    }

    /// An attribute chain with no calls, by its dotted text (`self.df`).
    pub fn path<'s>(parts: impl IntoIterator<Item = &'s str>) -> Self {
        let mut h = std::hash::DefaultHasher::new();
        1u8.hash(&mut h);
        for p in parts {
            p.hash(&mut h);
        }
        Origin::Name(h.finish())
    }
}

/// Which columns, in which order, a DataFrame has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Columns {
    /// Not known here. On a value the oracle typed, the model's
    /// `frame_columns` answers [`Columns::SameAs`] its own origin instead.
    #[default]
    Unknown,
    /// Exactly this column list, in order (a `select` of a readable list).
    Known(ColumnList),
    /// The same columns, in the same order, as the frame at this origin.
    SameAs(Origin),
}

impl Columns {
    /// Both sides have the same columns in the same order (neither unknown).
    pub fn agrees(self, other: Columns) -> bool {
        self != Columns::Unknown && self == other
    }
}

// ------------------------------------------------------------------- the rows

/// At most this many rows (`u64::MAX` = bounded by a non-literal `limit`).
/// Origin: `createDataFrame(<literal list>)`, `sc.parallelize(<literal list>)`, small
/// `range(n)`, `spark.sql(".. LIMIT n")`, `take(n)`/`head(n)`/`first()` results. Merge: the larger bound if bounded on both
/// paths. Step: `limit` lowers it, aggregates set 1, `describe` 10, join multiplies and
/// union adds the two bounds.
pub struct Bounded;
impl Fact for Bounded {
    type Value = Option<u64>;
    const UNKNOWN: Option<u64> = None;
    fn merge(a: Option<u64>, b: Option<u64>) -> Option<u64> {
        match (a, b) {
            (Some(a), Some(b)) => Some(a.max(b)),
            _ => None,
        }
    }
    fn step(v: Option<u64>, s: &Step) -> Option<u64> {
        match s {
            Step::Limit(n) => Some(v.map_or(*n, |b| b.min(*n))),
            Step::Aggregate => Some(1),
            Step::Describe => Some(v.map_or(10, |b| b.min(10))),
            Step::Combine { union, other, .. } => match (v, other.and_then(|o| o.bounded)) {
                (Some(a), Some(b)) if *union => Some(a.saturating_add(b)),
                (Some(a), Some(b)) => Some(a.saturating_mul(b)),
                _ => None,
            },
            _ => v,
        }
    }
}

/// The row bound comes from a limit-like step or a small origin, not only from an
/// aggregate: a one-row `agg(..)`, `describe()` / `summary()` or aggregate-only SQL over
/// an unbounded frame still reads every input row. Origin: `createDataFrame(<literal
/// list>)`, small `range(n)`, `spark.sql(".. LIMIT n")` and `EXPLAIN` (not when the query
/// aggregates the table it reads). Merge: on every path. Step: `limit` sets it,
/// aggregates keep it (`df.limit(10).agg(..)` read ten rows), join/union keep it only
/// when both sides have it.
pub struct RowLimited;
impl Fact for RowLimited {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a && b
    }
    fn step(v: bool, s: &Step) -> bool {
        match s {
            Step::Limit(_) => true,
            Step::Combine { other, .. } => v && other.is_some_and(|o| o.row_limited),
            _ => v,
        }
    }
}

/// Passed through filter/where/groupBy-agg/distinct/sample. Origin: a GroupedData
/// aggregate. Merge: on every path. Step: set by reducing steps, cleared by
/// join/union.
pub struct Reduced;
impl Fact for Reduced {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a && b
    }
    fn step(v: bool, s: &Step) -> bool {
        match s {
            Step::Reduce => true,
            Step::Combine { .. } => false,
            _ => v,
        }
    }
}

/// The value itself is the result of `cache()`/`persist()` (or the name after a
/// `df.cache()` statement). Merge: on every path. Step: set by the cache step, cleared
/// by every other DataFrame step (a derived frame is not the cached one); window steps
/// keep it.
pub struct Cached;
impl Fact for Cached {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a && b
    }
    fn step(v: bool, s: &Step) -> bool {
        match s {
            Step::Cache => true,
            s if s.is_window() => v,
            _ => false,
        }
    }
}

/// A streaming DataFrame. Origin: `spark.readStream`, a `DataStreamReader` load,
/// `spec.build(withStreaming=True)`. Merge: on any path. Step: kept.
pub struct Streaming;
impl Fact for Streaming {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a || b
    }
    fn step(v: bool, _: &Step) -> bool {
        v
    }
}

/// For lists / pandas frames: produced on the driver from a Spark value
/// (`collect`, `toPandas`, ...). Merge: on any path. Step: kept.
pub struct FromSpark;
impl Fact for FromSpark {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a || b
    }
    fn step(v: bool, _: &Step) -> bool {
        v
    }
}

/// DataFrame built from driver-side data (or a catalog metadata command such as
/// `spark.sql("SHOW TBLPROPERTIES t")`) and kept through row-wise steps: it fits in
/// driver memory, so a shuffle or a collect of it is cheap. Origin: [`Local::of_data`],
/// `SqlText::catalog_metadata` and, for RDDs, `sc.parallelize(<literal list>)` (`df.rdd`
/// keeps the frame's fact). Merge: on every path. Step: kept; a join/union is
/// local only when both sides are.
pub struct Local;
impl Local {
    /// `createDataFrame(data)`: local when `data` is a list, tuple, pandas or numpy value.
    pub fn of_data(data: Ty) -> bool {
        matches!(data, Ty::List(_) | Ty::Tuple) || data.is_local_data()
    }
}
impl Fact for Local {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a && b
    }
    fn step(v: bool, s: &Step) -> bool {
        match s {
            Step::Combine { other, .. } => v && other.is_some_and(|o| o.local),
            _ => v,
        }
    }
}

/// WindowSpec partitioning, known on every path or not at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Partitioning {
    /// Origin unknown (a parameter, an annotated helper result), or partitioned on
    /// some paths only.
    #[default]
    Unknown,
    /// Built from the `Window` class and never given a non-empty `partitionBy`: a
    /// single-partition window.
    Global,
    /// Has `partitionBy` with at least one key.
    Partitioned,
}

/// WindowSpec partitioning. Origin: [`Partitioning::Global`] for the `Window` class
/// ([`Facts::WINDOW_CLASS`]). Merge: equal on both paths, else unknown. Step:
/// `partitionBy(<keys>)` makes it partitioned, whatever the origin.
pub struct WinPartitioning;
impl Fact for WinPartitioning {
    type Value = Partitioning;
    const UNKNOWN: Partitioning = Partitioning::Unknown;
    fn merge(a: Partitioning, b: Partitioning) -> Partitioning {
        if a == b { a } else { Partitioning::Unknown }
    }
    fn step(v: Partitioning, s: &Step) -> Partitioning {
        match s {
            Step::PartitionBy => Partitioning::Partitioned,
            _ => v,
        }
    }
}

/// WindowSpec has `orderBy`. Merge: on any path. Step: set by `orderBy`.
pub struct WinOrdered;
impl Fact for WinOrdered {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a || b
    }
    fn step(v: bool, s: &Step) -> bool {
        v || matches!(s, Step::OrderBy)
    }
}

/// WindowSpec has `rowsBetween`/`rangeBetween`. Merge: on any path. Step: set by a
/// frame.
pub struct WinFramed;
impl Fact for WinFramed {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a || b
    }
    fn step(v: bool, s: &Step) -> bool {
        v || matches!(s, Step::Frame { .. })
    }
}

/// WindowSpec: the (last) explicit frame starts unbounded preceding and ends at a
/// finite offset (a running frame that grows with every row). Merge: on every path.
/// Step: each frame replaces it.
pub struct WinFrameGrowing;
impl Fact for WinFrameGrowing {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a && b
    }
    fn step(v: bool, s: &Step) -> bool {
        match s {
            Step::Frame { growing } => *growing,
            _ => v,
        }
    }
}

/// Reader / writer: rooted at a fresh builder on every path (`spark.read`,
/// `spark.readStream`, `df.write`, `df.writeStream`, `df.writeTo(..)`), so every
/// builder call that configured it is in view. A builder that is a parameter, a
/// helper's result or anything else of unknown origin is not fresh. Origin: those
/// properties / methods ([`Facts::FRESH_BUILDER`]). Merge: on every path. Step:
/// DataFrame and window steps never apply to builders (kept); a builder call
/// (`option`, `format`, `mode`, ...) keeps it: the oracle copies it from the
/// receiver whenever a reader / writer method returns a reader / writer.
pub struct FreshBuilder;
impl Fact for FreshBuilder {
    type Value = bool;
    const UNKNOWN: bool = false;
    fn merge(a: bool, b: bool) -> bool {
        a && b
    }
    fn step(v: bool, _: &Step) -> bool {
        v
    }
}

/// Which columns, in which order, the frame has ([`Columns`]). Origin: none
/// ([`Columns::Unknown`]: the model's `frame_columns` names the value's own origin).
/// Merge: equal on both paths, else unknown. Step: rows-only steps (`limit`, filters,
/// de-duplication, caches, sorts, repartitions, `alias`, `hint`, a union, a semi / anti
/// join: [`Step::Keep`] and the column-keeping [`Step::Combine`]) keep it, a `select` of a
/// readable list sets it ([`Step::Project`]), any other step forgets it.
pub struct FrameColumns;
impl Fact for FrameColumns {
    type Value = Columns;
    const UNKNOWN: Columns = Columns::Unknown;
    fn merge(a: Columns, b: Columns) -> Columns {
        if a == b { a } else { Columns::Unknown }
    }
    fn step(v: Columns, s: &Step) -> Columns {
        match s {
            Step::Limit(_) | Step::Reduce | Step::Cache | Step::Keep => v,
            Step::Combine { keeps_columns, .. } => {
                if *keeps_columns {
                    v
                } else {
                    Columns::Unknown
                }
            }
            Step::Project(c) => *c,
            Step::Aggregate | Step::Describe | Step::Other | Step::Values => Columns::Unknown,
            Step::PartitionBy | Step::OrderBy | Step::Frame { .. } => v,
        }
    }
}

/// What an RDD holds when it was read from files as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FileText {
    /// Not read from text files, or not known on every path.
    #[default]
    Unknown,
    /// `(path, contents)` pairs of whole files: `sc.wholeTextFiles(p)`.
    PathAndText,
    /// The raw text of files: the lines of `sc.textFile(p)`, the contents of
    /// `sc.wholeTextFiles(p).values()`. `spark.read.json(p)` / `spark.read.text(p)`
    /// could read the same files directly.
    Text,
}

/// RDD of raw file text ([`FileText`]). Origin: `sc.textFile(p)` ([`FileText::Text`]),
/// `sc.wholeTextFiles(p)` ([`FileText::PathAndText`]). Merge: equal on both paths, else
/// unknown. Step: [`Step::Values`] turns `(path, contents)` pairs into the contents;
/// rows-only steps (`filter`, `distinct`, `sample`, `coalesce`, `repartition`, `cache`:
/// [`Step::Reduce`], [`Step::Keep`], [`Step::Cache`], [`Step::Limit`]) keep it, a union
/// keeps it when both sides hold the same text; every other step (`map`, `flatMap`, a
/// join) changes the values and forgets it.
pub struct RawText;
impl Fact for RawText {
    type Value = FileText;
    const UNKNOWN: FileText = FileText::Unknown;
    fn merge(a: FileText, b: FileText) -> FileText {
        if a == b { a } else { FileText::Unknown }
    }
    fn step(v: FileText, s: &Step) -> FileText {
        match s {
            Step::Values if v == FileText::PathAndText => FileText::Text,
            Step::Reduce | Step::Keep | Step::Cache | Step::Limit(_) => v,
            Step::Combine {
                union: true,
                other: Some(o),
                ..
            } if o.file_text == v => v,
            _ => FileText::Unknown,
        }
    }
}

// ------------------------------------------------------------------- the table

macro_rules! facts {
    ($( $(#[$doc:meta])* $field:ident : $row:ident, )*) => {
        /// Dataflow facts attached to a value (mostly DataFrame values): one field
        /// per [`Fact`] row.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct Facts {
            $( $(#[$doc])* pub $field: <$row as Fact>::Value, )*
        }

        impl Facts {
            /// Nothing known.
            pub const NONE: Facts = Facts { $( $field: <$row as Fact>::UNKNOWN, )* };

            /// The facts where two control-flow paths meet.
            pub fn meet(self, other: Facts) -> Facts {
                Facts { $( $field: <$row as Fact>::merge(self.$field, other.$field), )* }
            }

            /// The facts after one DataFrame / RDD / WindowSpec step.
            pub fn after(self, s: &Step) -> Facts {
                Facts { $( $field: <$row as Fact>::step(self.$field, s), )* }
            }
        }
    };
}

facts! {
    /// See [`Bounded`].
    bounded: Bounded,
    /// See [`RowLimited`].
    row_limited: RowLimited,
    /// See [`Reduced`].
    reduced: Reduced,
    /// See [`Cached`].
    cached: Cached,
    /// See [`Streaming`].
    streaming: Streaming,
    /// See [`FromSpark`].
    from_spark: FromSpark,
    /// See [`Local`].
    local: Local,
    /// See [`WinPartitioning`].
    win_partitioning: WinPartitioning,
    /// See [`WinOrdered`].
    win_ordered: WinOrdered,
    /// See [`WinFramed`].
    win_framed: WinFramed,
    /// See [`WinFrameGrowing`].
    win_frame_growing: WinFrameGrowing,
    /// See [`FreshBuilder`].
    fresh_builder: FreshBuilder,
    /// See [`FrameColumns`].
    columns: FrameColumns,
    /// See [`RawText`].
    file_text: RawText,
}

impl Default for Facts {
    fn default() -> Self {
        Self::NONE
    }
}

impl Facts {
    /// A spec built from the `Window` class: global until partitioned.
    pub const WINDOW_CLASS: Facts = Facts {
        win_partitioning: Partitioning::Global,
        ..Facts::NONE
    };

    /// A reader / writer straight from `spark.read` / `df.write` / `df.writeTo(..)`.
    pub const FRESH_BUILDER: Facts = Facts {
        fresh_builder: true,
        ..Facts::NONE
    };

    /// An RDD of raw file text ([`FileText::Text`]) on every path.
    pub fn raw_file_text(&self) -> bool {
        self.file_text == FileText::Text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: bool = true;
    const F: bool = false;

    /// `(a, b) -> merged` for a bool row.
    fn merges<R: Fact<Value = bool>>(and: bool) {
        for (a, b) in [(F, F), (F, T), (T, F), (T, T)] {
            let want = if and { a && b } else { a || b };
            assert_eq!(R::merge(a, b), want, "merge({a}, {b})");
        }
    }

    const DF_STEPS: &[Step] = &[
        Step::Limit(5),
        Step::Aggregate,
        Step::Describe,
        Step::Reduce,
        Step::Cache,
        Step::Combine {
            union: false,
            other: None,
            keeps_columns: false,
        },
        Step::Keep,
        Step::Project(Columns::Unknown),
        Step::Other,
        Step::Values,
    ];
    const WIN_STEPS: &[Step] = &[
        Step::PartitionBy,
        Step::OrderBy,
        Step::Frame { growing: true },
        Step::Frame { growing: false },
    ];

    #[test]
    fn bounded() {
        let b = Bounded::merge;
        assert_eq!(b(Some(3), Some(7)), Some(7));
        assert_eq!(b(Some(3), None), None);
        assert_eq!(b(None, None), None);
        let s = Bounded::step;
        let combine = |union, o: Option<u64>| Step::Combine {
            union,
            other: Some(Facts {
                bounded: o,
                ..Facts::NONE
            }),
            keeps_columns: false,
        };
        for (v, step, want) in [
            (None, Step::Limit(5), Some(5)),
            (Some(3), Step::Limit(5), Some(3)),
            (Some(9), Step::Limit(u64::MAX), Some(9)),
            (None, Step::Aggregate, Some(1)),
            (Some(50), Step::Aggregate, Some(1)),
            (None, Step::Describe, Some(10)),
            (Some(4), Step::Describe, Some(4)),
            (Some(4), combine(true, Some(5)), Some(9)),
            (Some(4), combine(false, Some(5)), Some(20)),
            (Some(u64::MAX), combine(false, Some(5)), Some(u64::MAX)),
            (Some(4), combine(false, None), None),
            (
                Some(4),
                Step::Combine {
                    union: true,
                    other: None,
                    keeps_columns: false,
                },
                None,
            ),
            (Some(4), Step::Reduce, Some(4)),
            (Some(4), Step::Cache, Some(4)),
            (Some(4), Step::Other, Some(4)),
            (Some(4), Step::PartitionBy, Some(4)),
        ] {
            assert_eq!(s(v, &step), want, "{v:?} after {step:?}");
        }
    }

    #[test]
    fn row_limited() {
        merges::<RowLimited>(true);
        let combine = |o: bool| Step::Combine {
            union: true,
            other: Some(Facts {
                row_limited: o,
                ..Facts::NONE
            }),
            keeps_columns: false,
        };
        for (v, step, want) in [
            (F, Step::Limit(5), T),
            (F, Step::Limit(u64::MAX), T),
            // A one-row aggregate of an unbounded frame still reads all of it.
            (F, Step::Aggregate, F),
            (F, Step::Describe, F),
            (T, Step::Aggregate, T),
            (T, Step::Describe, T),
            (T, combine(true), T),
            (T, combine(false), F),
            (
                T,
                Step::Combine {
                    union: false,
                    other: None,
                    keeps_columns: false,
                },
                F,
            ),
            (T, Step::Reduce, T),
            (T, Step::Cache, T),
            (T, Step::Other, T),
            (F, Step::Other, F),
        ] {
            assert_eq!(RowLimited::step(v, &step), want, "{v} after {step:?}");
        }
    }

    #[test]
    fn reduced() {
        merges::<Reduced>(true);
        for (v, step, want) in [
            (F, Step::Reduce, T),
            (
                T,
                Step::Combine {
                    union: true,
                    other: None,
                    keeps_columns: false,
                },
                F,
            ),
            (T, Step::Limit(1), T),
            (T, Step::Aggregate, T),
            (T, Step::Cache, T),
            (T, Step::Other, T),
            (F, Step::Other, F),
        ] {
            assert_eq!(Reduced::step(v, &step), want, "{v} after {step:?}");
        }
    }

    #[test]
    fn cached() {
        merges::<Cached>(true);
        assert!(Cached::step(F, &Step::Cache));
        for s in DF_STEPS.iter().filter(|s| **s != Step::Cache) {
            assert!(
                !Cached::step(T, s),
                "derived frame after {s:?} is not the cache"
            );
        }
        for s in WIN_STEPS {
            assert!(Cached::step(T, s) && !Cached::step(F, s));
        }
    }

    #[test]
    fn streaming_and_from_spark_are_kept() {
        merges::<Streaming>(false);
        merges::<FromSpark>(false);
        for s in DF_STEPS.iter().chain(WIN_STEPS) {
            for v in [T, F] {
                assert_eq!(Streaming::step(v, s), v);
                assert_eq!(FromSpark::step(v, s), v);
            }
        }
    }

    #[test]
    fn local() {
        merges::<Local>(true);
        assert!(Local::of_data(Ty::List(crate::types::Elem::Row)));
        assert!(Local::of_data(Ty::Tuple) && Local::of_data(Ty::PandasFrame));
        assert!(!Local::of_data(Ty::DataFrame) && !Local::of_data(Ty::Unknown));
        let with = |l| Step::Combine {
            union: false,
            other: Some(Facts {
                local: l,
                ..Facts::NONE
            }),
            keeps_columns: false,
        };
        assert!(Local::step(T, &with(T)));
        assert!(!Local::step(T, &with(F)) && !Local::step(F, &with(T)));
        assert!(!Local::step(
            T,
            &Step::Combine {
                union: true,
                other: None,
                keeps_columns: false
            }
        ));
        for s in [
            Step::Reduce,
            Step::Limit(1),
            Step::Cache,
            Step::Aggregate,
            Step::Other,
        ] {
            assert!(Local::step(T, &s));
        }
    }

    #[test]
    fn win_partitioning() {
        use Partitioning::*;
        for (a, b, want) in [
            (Global, Global, Global),
            (Partitioned, Partitioned, Partitioned),
            (Global, Partitioned, Unknown),
            (Global, Unknown, Unknown),
            (Partitioned, Unknown, Unknown),
            (Unknown, Unknown, Unknown),
        ] {
            assert_eq!(WinPartitioning::merge(a, b), want);
            assert_eq!(WinPartitioning::merge(b, a), want);
        }
        for v in [Global, Partitioned, Unknown] {
            assert_eq!(WinPartitioning::step(v, &Step::PartitionBy), Partitioned);
            for s in [Step::OrderBy, Step::Frame { growing: true }, Step::Other] {
                assert_eq!(WinPartitioning::step(v, &s), v);
            }
        }
        assert_eq!(Facts::WINDOW_CLASS.win_partitioning, Global);
        assert_eq!(Facts::NONE.win_partitioning, Unknown);
    }

    #[test]
    fn window_order_and_frames() {
        merges::<WinOrdered>(false);
        merges::<WinFramed>(false);
        merges::<WinFrameGrowing>(true);
        assert!(WinOrdered::step(F, &Step::OrderBy) && WinOrdered::step(T, &Step::PartitionBy));
        assert!(!WinOrdered::step(F, &Step::Frame { growing: true }));
        assert!(WinFramed::step(F, &Step::Frame { growing: false }));
        assert!(!WinFramed::step(F, &Step::OrderBy));
        assert!(WinFrameGrowing::step(F, &Step::Frame { growing: true }));
        assert!(!WinFrameGrowing::step(T, &Step::Frame { growing: false }));
        assert!(WinFrameGrowing::step(T, &Step::OrderBy));
    }

    #[test]
    fn fresh_builder() {
        merges::<FreshBuilder>(true);
        for s in DF_STEPS.iter().chain(WIN_STEPS) {
            assert!(FreshBuilder::step(T, s) && !FreshBuilder::step(F, s));
        }
        assert_eq!(
            (
                Facts::FRESH_BUILDER.fresh_builder,
                Facts::NONE.fresh_builder
            ),
            (true, false)
        );
    }

    #[test]
    fn table_drives_meet_and_after() {
        let a = Facts {
            bounded: Some(2),
            cached: true,
            streaming: true,
            win_partitioning: Partitioning::Global,
            ..Facts::NONE
        };
        let m = a.meet(Facts::NONE);
        assert_eq!(m.bounded, None);
        assert!(!m.cached && m.streaming);
        assert_eq!(m.win_partitioning, Partitioning::Unknown);
        assert_eq!(Facts::default(), Facts::NONE);
        let b = a.after(&Step::Limit(1));
        assert_eq!(b.bounded, Some(1));
        assert!(!b.cached && b.streaming);
    }

    #[test]
    fn raw_text() {
        use FileText::{PathAndText, Text, Unknown};
        let union = |o| Step::Combine {
            union: true,
            other: Some(Facts {
                file_text: o,
                ..Facts::NONE
            }),
            keeps_columns: true,
        };
        for (a, b, want) in [
            (Text, Text, Text),
            (PathAndText, PathAndText, PathAndText),
            (Text, PathAndText, Unknown),
            (Text, Unknown, Unknown),
            (Unknown, Unknown, Unknown),
        ] {
            assert_eq!(RawText::merge(a, b), want);
            assert_eq!(RawText::merge(b, a), want);
        }
        for (v, step, want) in [
            (PathAndText, Step::Values, Text),
            (Text, Step::Values, Unknown),
            (Unknown, Step::Values, Unknown),
            (Text, Step::Other, Unknown),
            (PathAndText, Step::Other, Unknown),
            (Text, Step::Reduce, Text),
            (Text, Step::Keep, Text),
            (Text, Step::Cache, Text),
            (PathAndText, Step::Reduce, PathAndText),
            (Text, union(Text), Text),
            (Text, union(PathAndText), Unknown),
            (Text, union(Unknown), Unknown),
            (
                Text,
                Step::Combine {
                    union: true,
                    other: None,
                    keeps_columns: true,
                },
                Unknown,
            ),
            (
                Text,
                Step::Combine {
                    union: false,
                    other: Some(Facts {
                        file_text: Text,
                        ..Facts::NONE
                    }),
                    keeps_columns: false,
                },
                Unknown,
            ),
            (Text, Step::Aggregate, Unknown),
        ] {
            assert_eq!(RawText::step(v, &step), want, "{v:?} after {step:?}");
        }
        let f = |file_text| Facts {
            file_text,
            ..Facts::NONE
        };
        assert!(f(Text).raw_file_text());
        assert!(!f(PathAndText).raw_file_text() && !Facts::NONE.raw_file_text());
    }

    #[test]
    fn frame_columns() {
        use Columns::{Known, SameAs, Unknown};
        let x = SameAs(Origin::Expr(TextRange::default()));
        let list = Known(ColumnList::names(["a", "b"]));
        assert_eq!(FrameColumns::merge(x, x), x);
        assert_eq!(FrameColumns::merge(list, list), list);
        assert_eq!(FrameColumns::merge(x, list), Unknown);
        assert_eq!(FrameColumns::merge(Unknown, x), Unknown);
        let combine = |keeps_columns| Step::Combine {
            union: true,
            other: None,
            keeps_columns,
        };
        for (step, want) in [
            (Step::Limit(3), x),
            (Step::Reduce, x),
            (Step::Cache, x),
            (Step::Keep, x),
            (combine(true), x),
            (combine(false), Unknown),
            (Step::Project(list), list),
            (Step::Aggregate, Unknown),
            (Step::Describe, Unknown),
            (Step::Other, Unknown),
            (Step::PartitionBy, x),
        ] {
            assert_eq!(FrameColumns::step(x, &step), want, "{step:?}");
        }
        assert_eq!(ColumnList::names(["a", "b"]), ColumnList::names(["a", "b"]));
        assert_ne!(ColumnList::names(["a", "b"]), ColumnList::names(["b", "a"]));
        assert_ne!(ColumnList::names(["a"]), ColumnList::variable("a", &[]));
        let r = [TextRange::default()];
        assert_ne!(Origin::bindings("a", &r), Origin::path(["a"]));
        assert!(list.agrees(list) && !Unknown.agrees(Unknown) && !x.agrees(list));
        assert_eq!(Facts::NONE.columns, Unknown);
    }
}
