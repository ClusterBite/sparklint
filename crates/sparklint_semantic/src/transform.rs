//! "What does this transformation do to the plan?" — the ONE table of lazy DataFrame
//! steps, the transformation counterpart of [`crate::call_run`].
//!
//! A row says, for one (receiver, method) pair, whether the step shuffles (and which
//! arguments are its keys), combines its DataFrame argument into the result, runs
//! opaque Python, caches or checkpoints, has an effect even when its result is
//! discarded (a temp view, printed output), is a narrow per-partition step, which rows
//! it keeps (the oracle's [`crate::Step`] classification), which columns it keeps, and
//! whether a filter on other columns commutes with it. Rules, the oracle and the
//! project-level analysis ask the row; they never keep their own method lists. A new transformation is
//! one row here plus a [`tests`] case.
//!
//! Argument-dependent answers stay next to their row: the shuffle keys
//! ([`Shuffle::key_args`]), a checkpoint's `eager=` ([`checkpoint_runs_now`]) and the
//! join type / `allowMissingColumns=` that decide whether the columns are kept
//! ([`Cols`]).

use ruff_python_ast::{self as ast, Expr};

use crate::infer::call_arg;
use crate::model::SemanticModel;
use crate::spark_call::SparkCall;
use crate::types::Ty;

/// How a shuffle distributes the rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Part {
    /// Hash-partitioned by its keys (`groupBy`, `join`, `repartition(cols)`,
    /// `distinct`, set operations). With no key columns: round robin / all columns.
    Hash,
    /// Range-partitioned by its keys (`orderBy`, `sort`, `repartitionByRange`).
    Range,
    /// Everything into one partition (a keyless `df.agg(..)`).
    Single,
}

/// Which arguments of the call are the shuffle keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keys {
    /// Every positional argument (`groupBy("a", "b")`, `orderBy(..)`).
    Args,
    /// Every positional argument except number literals (`repartition(8, "k")`).
    NonNumberArgs,
    /// One argument, by position or keyword (`join(o, on=..)`, `dropDuplicates(subset)`).
    Arg(usize, &'static str),
    /// All columns of the frame (`distinct()`, set operations): no key arguments.
    AllColumns,
    /// The keys of the `groupBy` the grouped data came from.
    Grouping,
    /// The `partitionBy` of the window specification (`F.x().over(w)`).
    Window,
    /// No keys (a keyless aggregate).
    None,
}

/// A step that exchanges rows between partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Shuffle {
    pub part: Part,
    pub keys: Keys,
}

impl Shuffle {
    /// The key argument expressions of `call` (empty when the keys are not arguments or
    /// absent). `None` for [`Keys::Grouping`] / [`Keys::Window`] / [`Keys::None`]: the
    /// keys are elsewhere or there are none.
    pub fn key_args<'e>(&self, call: &'e ast::ExprCall) -> Option<Vec<&'e Expr>> {
        let args = &call.arguments.args;
        Some(match self.keys {
            Keys::Args => args.iter().collect(),
            Keys::NonNumberArgs => args
                .iter()
                .filter(|a| !matches!(a, Expr::NumberLiteral(_)))
                .collect(),
            Keys::Arg(i, kw) => call_arg(call, i, kw).into_iter().collect(),
            Keys::AllColumns => Vec::new(),
            Keys::Grouping | Keys::Window | Keys::None => return None,
        })
    }
}

/// How a DataFrame argument flows into the result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Combine {
    /// `join` / `crossJoin` / `lateralJoin`.
    Join,
    /// `union` / `unionAll` / `unionByName`.
    Union,
    /// `intersect` / `intersectAll` / `subtract` / `exceptAll` (rows of the receiver).
    SetOp,
}

/// What a caching step keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cache {
    /// `cache()` / `persist()`: executor memory / disk, lazily.
    Memory,
    /// `checkpoint()` (reliable storage) / `localCheckpoint()` (executor storage):
    /// eager unless `eager=False`.
    Checkpoint { local: bool },
}

/// An effect a call has even when its result is discarded (besides caching).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Effect {
    /// Registers a temporary view (`createOrReplaceTempView`, `registerTempTable`).
    View,
    /// Prints to stdout (`printSchema`, `explain`).
    Print,
    /// `unpersist()`: frees the cached blocks.
    Unpersist,
}

/// A narrow step: no shuffle, every output partition is one input partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Narrow {
    /// Per-row column work, every row kept (`select`, `withColumn`, `drop`, `alias`).
    Project,
    /// Drops rows (`filter`, `where`, `dropna`).
    Filter,
    /// Rewrites values in place (`fillna`, `replace`).
    Fill,
    /// Sorts each partition (`sortWithinPartitions`).
    SortWithin,
    /// Marks the plan without touching data (`cache`, `persist`, `hint`).
    Mark,
}

/// What the step does to the row set, as far as the oracle's row facts care.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rows {
    /// Not one of the shapes below (kept, reordered, multiplied, unknown).
    Other,
    /// Keeps a subset of the rows (`filter`, `distinct`, `sample`, set operations).
    Reduce,
    /// `limit(n)`.
    Limit,
    /// One row per call (`agg(..)`).
    Aggregate,
    /// `describe()` / `summary()`: a few statistics rows.
    Describe,
}

/// Which columns, in which order, the result has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cols {
    /// Exactly the receiver's columns, in order (rows only: `filter`, `orderBy`,
    /// `cache`, `union`).
    Keep,
    /// The columns the arguments pick (`select(..)`): read from the arguments.
    Select,
    /// A join: a `left_semi` / `left_anti` join keeps the receiver's columns, any other
    /// join type changes them.
    Join,
    /// `unionByName(other, allowMissingColumns=True)` adds the other side's columns;
    /// otherwise the receiver's columns are kept.
    UnionByName,
    /// Something else (added, dropped, renamed or aggregated columns).
    Change,
}

/// Whether a filter on columns the step does not create commutes with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Commutes {
    Never,
    Always,
    /// Only when the call has no arguments (`dropDuplicates()` on every column).
    NoArgs,
}

/// One transformation row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transform {
    pub recv: Ty,
    pub method: &'static str,
    pub shuffle: Option<Shuffle>,
    pub combine: Option<Combine>,
    /// Runs opaque Python over the rows (`mapInPandas`, `applyInPandas`).
    pub python: bool,
    pub cache: Option<Cache>,
    pub effect: Option<Effect>,
    pub narrow: Option<Narrow>,
    pub rows: Rows,
    pub cols: Cols,
    pub commutes: Commutes,
}

impl Transform {
    /// Adds a shuffle, a join or opaque Python work to the plan: what makes a lineage
    /// worth caching when it is recomputed (PRF006 and the project-level analysis'
    /// "expensive" steps ask this).
    pub fn expensive(&self) -> bool {
        self.shuffle.is_some() || self.python || self.combine == Some(Combine::Join)
    }

    /// Hash-partitions the rows by key columns (Spark can reuse the partitioning).
    pub fn hash_partitioned(&self) -> bool {
        self.shuffle.is_some_and(|s| s.part == Part::Hash)
    }

    /// The step does something even when its result is discarded: caches,
    /// checkpoints, unpersists, registers a view or prints.
    pub fn side_effect(&self) -> bool {
        self.cache.is_some() || self.effect.is_some()
    }

    /// The result's partition count and layout are the receiver's: a narrow step that
    /// neither sorts nor moves rows between partitions.
    pub fn keeps_partitions(&self) -> bool {
        matches!(
            self.narrow,
            Some(Narrow::Project | Narrow::Filter | Narrow::Fill | Narrow::Mark)
        )
    }

    /// A projection-only step (or a per-partition sort) that does no work worth a
    /// shuffle of its own.
    pub fn cheap(&self) -> bool {
        matches!(self.narrow, Some(Narrow::Project | Narrow::SortWithin))
    }

    /// The result has exactly the receiver's columns, in the same order: a rows-only
    /// step, a `left_semi` / `left_anti` join (`how=` or the third argument, a
    /// constant), a `unionByName` without `allowMissingColumns=True`.
    pub fn keeps_columns(&self, model: &SemanticModel, call: &ast::ExprCall) -> bool {
        match self.cols {
            Cols::Keep => true,
            Cols::Join => {
                let how = call
                    .arguments
                    .find_keyword("how")
                    .map(|k| &k.value)
                    .or_else(|| call.arguments.args.get(2));
                how.and_then(|h| model.const_str(h)).is_some_and(|h| {
                    matches!(
                        h.to_ascii_lowercase().replace('_', "").as_str(),
                        "semi" | "leftsemi" | "anti" | "leftanti"
                    )
                })
            }
            Cols::UnionByName => call_arg(call, 1, "allowMissingColumns")
                .is_none_or(|a| matches!(a, Expr::BooleanLiteral(b) if !b.value)),
            Cols::Select | Cols::Change => false,
        }
    }

    /// Whether a filter on other columns commutes with this call.
    pub fn commutes_with_filter(&self, call: &ast::ExprCall) -> bool {
        match self.commutes {
            Commutes::Never => false,
            Commutes::Always => true,
            Commutes::NoArgs => {
                call.arguments.args.is_empty() && call.arguments.keywords.is_empty()
            }
        }
    }
}

// ------------------------------------------------------------------- row builders

const fn t(recv: Ty, method: &'static str) -> Transform {
    Transform {
        recv,
        method,
        shuffle: None,
        combine: None,
        python: false,
        cache: None,
        effect: None,
        narrow: None,
        rows: Rows::Other,
        cols: Cols::Change,
        commutes: Commutes::Never,
    }
}

impl Transform {
    const fn shuffle(mut self, part: Part, keys: Keys) -> Self {
        self.shuffle = Some(Shuffle { part, keys });
        self
    }
    const fn combine(mut self, c: Combine) -> Self {
        self.combine = Some(c);
        self
    }
    const fn python(mut self) -> Self {
        self.python = true;
        self
    }
    const fn cache(mut self, c: Cache) -> Self {
        self.cache = Some(c);
        self
    }
    const fn effect(mut self, e: Effect) -> Self {
        self.effect = Some(e);
        self
    }
    const fn narrow(mut self, n: Narrow) -> Self {
        self.narrow = Some(n);
        self
    }
    const fn rows(mut self, r: Rows) -> Self {
        self.rows = r;
        self
    }
    const fn cols(mut self, c: Cols) -> Self {
        self.cols = c;
        self
    }
    /// Keeps the receiver's columns.
    const fn keep(self) -> Self {
        self.cols(Cols::Keep)
    }
    const fn commutes(mut self, c: Commutes) -> Self {
        self.commutes = c;
        self
    }
}

const DF: Ty = Ty::DataFrame;
const GD: Ty = Ty::GroupedData;

use Commutes::{Always, NoArgs};
const HASH: Part = Part::Hash;
const RANGE: Part = Part::Range;
const SINGLE: Part = Part::Single;

/// Every DataFrame / GroupedData transformation the rules and the project analysis
/// distinguish: (receiver, method) → what it does to the plan.
pub const ROWS: &[Transform] = &[
    // ---- joins and set operations
    t(DF, "join")
        .shuffle(HASH, Keys::Arg(1, "on"))
        .combine(Combine::Join)
        .cols(Cols::Join),
    t(DF, "crossJoin").combine(Combine::Join),
    t(DF, "lateralJoin").combine(Combine::Join),
    t(DF, "union").combine(Combine::Union).keep(),
    t(DF, "unionAll").combine(Combine::Union).keep(),
    t(DF, "unionByName")
        .combine(Combine::Union)
        .cols(Cols::UnionByName),
    t(DF, "intersect")
        .shuffle(HASH, Keys::AllColumns)
        .combine(Combine::SetOp)
        .rows(Rows::Reduce)
        .keep(),
    t(DF, "intersectAll")
        .shuffle(HASH, Keys::AllColumns)
        .combine(Combine::SetOp)
        .rows(Rows::Reduce)
        .keep(),
    t(DF, "subtract")
        .shuffle(HASH, Keys::AllColumns)
        .combine(Combine::SetOp)
        .rows(Rows::Reduce)
        .keep(),
    t(DF, "exceptAll")
        .shuffle(HASH, Keys::AllColumns)
        .combine(Combine::SetOp)
        .rows(Rows::Reduce)
        .keep(),
    // ---- aggregation and de-duplication
    t(DF, "groupBy").shuffle(HASH, Keys::Args),
    t(DF, "groupby").shuffle(HASH, Keys::Args),
    t(DF, "rollup").shuffle(HASH, Keys::Args),
    t(DF, "cube").shuffle(HASH, Keys::Args),
    t(DF, "agg")
        .shuffle(SINGLE, Keys::None)
        .rows(Rows::Aggregate),
    t(DF, "distinct")
        .shuffle(HASH, Keys::AllColumns)
        .rows(Rows::Reduce)
        .keep()
        .commutes(Always),
    t(DF, "dropDuplicates")
        .shuffle(HASH, Keys::Arg(0, "subset"))
        .rows(Rows::Reduce)
        .keep()
        .commutes(NoArgs),
    t(DF, "drop_duplicates")
        .shuffle(HASH, Keys::Arg(0, "subset"))
        .rows(Rows::Reduce)
        .keep()
        .commutes(NoArgs),
    t(DF, "dropDuplicatesWithinWatermark")
        .rows(Rows::Reduce)
        .keep(),
    t(DF, "describe").rows(Rows::Describe),
    t(DF, "summary").rows(Rows::Describe),
    // Every GroupedData method runs the grouping's shuffle.
    t(GD, "agg").shuffle(HASH, Keys::Grouping),
    t(GD, "avg").shuffle(HASH, Keys::Grouping),
    t(GD, "count").shuffle(HASH, Keys::Grouping),
    t(GD, "max").shuffle(HASH, Keys::Grouping),
    t(GD, "mean").shuffle(HASH, Keys::Grouping),
    t(GD, "min").shuffle(HASH, Keys::Grouping),
    t(GD, "sum").shuffle(HASH, Keys::Grouping),
    t(GD, "pivot").shuffle(HASH, Keys::Grouping),
    t(GD, "apply").shuffle(HASH, Keys::Grouping).python(),
    t(GD, "applyInPandas")
        .shuffle(HASH, Keys::Grouping)
        .python(),
    t(GD, "applyInArrow").shuffle(HASH, Keys::Grouping).python(),
    t(GD, "applyInPandasWithState")
        .shuffle(HASH, Keys::Grouping)
        .python(),
    t(GD, "transformWithState")
        .shuffle(HASH, Keys::Grouping)
        .python(),
    t(GD, "transformWithStateInPandas")
        .shuffle(HASH, Keys::Grouping)
        .python(),
    // ---- sorting and partitioning
    t(DF, "orderBy")
        .shuffle(RANGE, Keys::Args)
        .keep()
        .commutes(Always),
    t(DF, "sort")
        .shuffle(RANGE, Keys::Args)
        .keep()
        .commutes(Always),
    t(DF, "sortWithinPartitions")
        .narrow(Narrow::SortWithin)
        .keep()
        .commutes(Always),
    t(DF, "repartition")
        .shuffle(HASH, Keys::NonNumberArgs)
        .keep()
        .commutes(Always),
    t(DF, "repartitionByRange")
        .shuffle(RANGE, Keys::Args)
        .keep()
        .commutes(Always),
    t(DF, "coalesce").keep().commutes(Always),
    t(DF, "hint").narrow(Narrow::Mark).keep().commutes(Always),
    // ---- opaque Python
    t(DF, "mapInPandas").python(),
    t(DF, "mapInArrow").python(),
    // ---- caching
    t(DF, "cache")
        .cache(Cache::Memory)
        .narrow(Narrow::Mark)
        .keep()
        .commutes(Always),
    t(DF, "persist")
        .cache(Cache::Memory)
        .narrow(Narrow::Mark)
        .keep()
        .commutes(Always),
    t(DF, "checkpoint")
        .cache(Cache::Checkpoint { local: false })
        .keep(),
    t(DF, "localCheckpoint")
        .cache(Cache::Checkpoint { local: true })
        .keep(),
    t(DF, "unpersist").effect(Effect::Unpersist).keep(),
    // ---- side effects (the result is not a plan step)
    t(DF, "createOrReplaceTempView").effect(Effect::View),
    t(DF, "createTempView").effect(Effect::View),
    t(DF, "createGlobalTempView").effect(Effect::View),
    t(DF, "createOrReplaceGlobalTempView").effect(Effect::View),
    t(DF, "registerTempTable").effect(Effect::View),
    t(DF, "printSchema").effect(Effect::Print),
    t(DF, "explain").effect(Effect::Print),
    // ---- narrow per-row steps
    t(DF, "select").narrow(Narrow::Project).cols(Cols::Select),
    t(DF, "selectExpr").narrow(Narrow::Project),
    t(DF, "withColumn").narrow(Narrow::Project),
    t(DF, "withColumns").narrow(Narrow::Project),
    t(DF, "withColumnRenamed").narrow(Narrow::Project),
    t(DF, "withColumnsRenamed").narrow(Narrow::Project),
    t(DF, "withMetadata").narrow(Narrow::Project),
    t(DF, "toDF").narrow(Narrow::Project),
    t(DF, "drop").narrow(Narrow::Project).commutes(Always),
    t(DF, "alias")
        .narrow(Narrow::Project)
        .keep()
        .commutes(Always),
    t(DF, "filter")
        .narrow(Narrow::Filter)
        .rows(Rows::Reduce)
        .keep(),
    t(DF, "where")
        .narrow(Narrow::Filter)
        .rows(Rows::Reduce)
        .keep(),
    t(DF, "dropna")
        .narrow(Narrow::Filter)
        .rows(Rows::Reduce)
        .keep(),
    t(DF, "fillna").narrow(Narrow::Fill).keep(),
    t(DF, "replace").narrow(Narrow::Fill).keep(),
    // ---- row-count steps
    t(DF, "limit").rows(Rows::Limit).keep(),
    t(DF, "sample").rows(Rows::Reduce).keep(),
    t(DF, "sampleBy").rows(Rows::Reduce).keep(),
    t(DF, "withWatermark").keep(),
];

/// The window shuffle of `F.x().over(w)` (a `window` step of the project-level analysis).
pub const WINDOW: Transform = t(Ty::Column, "over").shuffle(HASH, Keys::Window);

/// The row for `recv.method(..)`, if it is a transformation the table knows.
pub fn row(recv: Ty, method: &str) -> Option<&'static Transform> {
    ROWS.iter().find(|r| r.recv == recv && r.method == method)
}

/// The row for a DataFrame method.
pub fn frame_row(method: &str) -> Option<&'static Transform> {
    row(DF, method)
}

/// The row of a typed Spark call.
pub fn of_call(sc: &SparkCall) -> Option<&'static Transform> {
    row(sc.class(), sc.method)
}

/// Reader formats whose scan parses text or goes through a remote database
/// (`csv`, `json`, `jdbc`, `xml`): costly enough to count as expensive lineage.
pub const COSTLY_FORMATS: &[&str] = &["csv", "json", "jdbc", "xml"];

/// Whether a reader format (any case) is one of [`COSTLY_FORMATS`].
pub fn costly_format(format: &str) -> bool {
    COSTLY_FORMATS.contains(&format.to_ascii_lowercase().as_str())
}

/// For a `checkpoint()` / `localCheckpoint()` call: whether it materialises the frame
/// now. `eager=False` (keyword or first argument, a literal) only marks the frame it
/// returns. `None` for any other call.
pub fn checkpoint_runs_now(sc: &SparkCall) -> Option<bool> {
    let r = of_call(sc)?;
    let Some(Cache::Checkpoint { .. }) = r.cache else {
        return None;
    };
    Some(
        !sc.arg(0, "eager")
            .is_some_and(|a| matches!(a, Expr::BooleanLiteral(b) if !b.value)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One letter per attribute: S shuffle (h/r/1 = hash/range/single), C combine
    /// (j/u/s), P python, K cache (m/c/l), E effect (v/p/u), N narrow (p/f/x/s/m),
    /// R rows (r/l/a/d), cols (k/s/j/n), commutes (a/0).
    fn code(r: &Transform) -> String {
        let mut s = String::new();
        if let Some(sh) = r.shuffle {
            s.push('S');
            s.push(match sh.part {
                Part::Hash => 'h',
                Part::Range => 'r',
                Part::Single => '1',
            });
        }
        if let Some(c) = r.combine {
            s.push('C');
            s.push(match c {
                Combine::Join => 'j',
                Combine::Union => 'u',
                Combine::SetOp => 's',
            });
        }
        if r.python {
            s.push('P');
        }
        if let Some(k) = r.cache {
            s.push('K');
            s.push(match k {
                Cache::Memory => 'm',
                Cache::Checkpoint { local: false } => 'c',
                Cache::Checkpoint { local: true } => 'l',
            });
        }
        if let Some(e) = r.effect {
            s.push('E');
            s.push(match e {
                Effect::View => 'v',
                Effect::Print => 'p',
                Effect::Unpersist => 'u',
            });
        }
        if let Some(n) = r.narrow {
            s.push('N');
            s.push(match n {
                Narrow::Project => 'p',
                Narrow::Filter => 'f',
                Narrow::Fill => 'x',
                Narrow::SortWithin => 's',
                Narrow::Mark => 'm',
            });
        }
        match r.rows {
            Rows::Other => {}
            Rows::Reduce => s.push_str("Rr"),
            Rows::Limit => s.push_str("Rl"),
            Rows::Aggregate => s.push_str("Ra"),
            Rows::Describe => s.push_str("Rd"),
        }
        s.push(match r.cols {
            Cols::Keep => 'k',
            Cols::Select => 's',
            Cols::Join => 'j',
            Cols::UnionByName => 'u',
            Cols::Change => 'n',
        });
        match r.commutes {
            Commutes::Never => {}
            Commutes::Always => s.push('a'),
            Commutes::NoArgs => s.push('0'),
        }
        s
    }

    /// (receiver, method) → attribute code: the whole contract.
    #[test]
    fn table() {
        let cases: &[(Ty, &str, &str)] = &[
            (DF, "join", "ShCjj"),
            (DF, "crossJoin", "Cjn"),
            (DF, "lateralJoin", "Cjn"),
            (DF, "union", "Cuk"),
            (DF, "unionAll", "Cuk"),
            (DF, "unionByName", "Cuu"),
            (DF, "intersect", "ShCsRrk"),
            (DF, "intersectAll", "ShCsRrk"),
            (DF, "subtract", "ShCsRrk"),
            (DF, "exceptAll", "ShCsRrk"),
            (DF, "groupBy", "Shn"),
            (DF, "groupby", "Shn"),
            (DF, "rollup", "Shn"),
            (DF, "cube", "Shn"),
            (DF, "agg", "S1Ran"),
            (DF, "distinct", "ShRrka"),
            (DF, "dropDuplicates", "ShRrk0"),
            (DF, "drop_duplicates", "ShRrk0"),
            (DF, "dropDuplicatesWithinWatermark", "Rrk"),
            (DF, "describe", "Rdn"),
            (DF, "summary", "Rdn"),
            (GD, "agg", "Shn"),
            (GD, "avg", "Shn"),
            (GD, "count", "Shn"),
            (GD, "max", "Shn"),
            (GD, "mean", "Shn"),
            (GD, "min", "Shn"),
            (GD, "sum", "Shn"),
            (GD, "pivot", "Shn"),
            (GD, "apply", "ShPn"),
            (GD, "applyInPandas", "ShPn"),
            (GD, "applyInArrow", "ShPn"),
            (GD, "applyInPandasWithState", "ShPn"),
            (GD, "transformWithState", "ShPn"),
            (GD, "transformWithStateInPandas", "ShPn"),
            (DF, "orderBy", "Srka"),
            (DF, "sort", "Srka"),
            (DF, "sortWithinPartitions", "Nska"),
            (DF, "repartition", "Shka"),
            (DF, "repartitionByRange", "Srka"),
            (DF, "coalesce", "ka"),
            (DF, "hint", "Nmka"),
            (DF, "mapInPandas", "Pn"),
            (DF, "mapInArrow", "Pn"),
            (DF, "cache", "KmNmka"),
            (DF, "persist", "KmNmka"),
            (DF, "checkpoint", "Kck"),
            (DF, "localCheckpoint", "Klk"),
            (DF, "unpersist", "Euk"),
            (DF, "createOrReplaceTempView", "Evn"),
            (DF, "createTempView", "Evn"),
            (DF, "createGlobalTempView", "Evn"),
            (DF, "createOrReplaceGlobalTempView", "Evn"),
            (DF, "registerTempTable", "Evn"),
            (DF, "printSchema", "Epn"),
            (DF, "explain", "Epn"),
            (DF, "select", "Nps"),
            (DF, "selectExpr", "Npn"),
            (DF, "withColumn", "Npn"),
            (DF, "withColumns", "Npn"),
            (DF, "withColumnRenamed", "Npn"),
            (DF, "withColumnsRenamed", "Npn"),
            (DF, "withMetadata", "Npn"),
            (DF, "toDF", "Npn"),
            (DF, "drop", "Npna"),
            (DF, "alias", "Npka"),
            (DF, "filter", "NfRrk"),
            (DF, "where", "NfRrk"),
            (DF, "dropna", "NfRrk"),
            (DF, "fillna", "Nxk"),
            (DF, "replace", "Nxk"),
            (DF, "limit", "Rlk"),
            (DF, "sample", "Rrk"),
            (DF, "sampleBy", "Rrk"),
            (DF, "withWatermark", "k"),
        ];
        for (ty, m, want) in cases {
            let r = row(*ty, m).unwrap_or_else(|| panic!("no row for {ty:?}.{m}"));
            assert_eq!(code(r), *want, "{ty:?}.{m}");
        }
        assert_eq!(cases.len(), ROWS.len(), "every row has a case");
        for (ty, m) in [
            (DF, "count"),
            (DF, "collect"),
            (GD, "cogroup"),
            (Ty::Rdd, "map"),
        ] {
            assert!(row(ty, m).is_none(), "{ty:?}.{m}");
        }
        assert_eq!(code(&WINDOW), "Shn");
        for (i, a) in ROWS.iter().enumerate() {
            assert!(
                ROWS[..i]
                    .iter()
                    .all(|b| (b.recv, b.method) != (a.recv, a.method)),
                "duplicate row {:?}.{}",
                a.recv,
                a.method
            );
        }
    }

    #[test]
    fn derived_questions() {
        let names = |f: fn(&Transform) -> bool| -> Vec<&str> {
            ROWS.iter().filter(|r| f(r)).map(|r| r.method).collect()
        };
        assert_eq!(
            names(|r| r.recv == DF && r.expensive()),
            [
                "join",
                "crossJoin",
                "lateralJoin",
                "intersect",
                "intersectAll",
                "subtract",
                "exceptAll",
                "groupBy",
                "groupby",
                "rollup",
                "cube",
                "agg",
                "distinct",
                "dropDuplicates",
                "drop_duplicates",
                "orderBy",
                "sort",
                "repartition",
                "repartitionByRange",
                "mapInPandas",
                "mapInArrow"
            ]
        );
        assert!(
            ROWS.iter()
                .filter(|r| r.recv == GD)
                .all(Transform::expensive)
        );
        assert_eq!(
            names(Transform::cheap),
            [
                "sortWithinPartitions",
                "select",
                "selectExpr",
                "withColumn",
                "withColumns",
                "withColumnRenamed",
                "withColumnsRenamed",
                "withMetadata",
                "toDF",
                "drop",
                "alias"
            ]
        );
        assert_eq!(
            names(Transform::keeps_partitions),
            [
                "hint",
                "cache",
                "persist",
                "select",
                "selectExpr",
                "withColumn",
                "withColumns",
                "withColumnRenamed",
                "withColumnsRenamed",
                "withMetadata",
                "toDF",
                "drop",
                "alias",
                "filter",
                "where",
                "dropna",
                "fillna",
                "replace"
            ]
        );
        assert_eq!(
            names(|r| r.hash_partitioned() && r.recv == DF),
            [
                "join",
                "intersect",
                "intersectAll",
                "subtract",
                "exceptAll",
                "groupBy",
                "groupby",
                "rollup",
                "cube",
                "distinct",
                "dropDuplicates",
                "drop_duplicates",
                "repartition"
            ]
        );
        assert_eq!(
            names(Transform::side_effect),
            [
                "cache",
                "persist",
                "checkpoint",
                "localCheckpoint",
                "unpersist",
                "createOrReplaceTempView",
                "createTempView",
                "createGlobalTempView",
                "createOrReplaceGlobalTempView",
                "registerTempTable",
                "printSchema",
                "explain"
            ]
        );
        assert!(costly_format("CSV") && costly_format("jdbc") && !costly_format("delta"));
    }

    #[test]
    fn key_args_and_checkpoint_eagerness() {
        use crate::{NoSummaries, SemanticOptions, build_model};
        use ruff_text_size::TextSize;
        let src = "\
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.table('t')
df.groupBy('a', 'b')
df.repartition(8, 'k')
df.join(df, on='id')
df.dropDuplicates()
df.checkpoint()
df.checkpoint(eager=False)
df.localCheckpoint(False)
df.cache()
";
        let module = ruff_python_parser::parse_module(src)
            .expect("valid test source")
            .into_syntax();
        let model = build_model(
            &module,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let calls: Vec<&Expr> = module
            .body
            .iter()
            .skip(3)
            .filter_map(|s| s.as_expr_stmt().map(|e| &*e.value))
            .collect();
        let keys = |e: &Expr| -> Option<usize> {
            let sc = model.spark_call(e)?;
            of_call(&sc)?.shuffle?.key_args(sc.call).map(|v| v.len())
        };
        assert_eq!(keys(calls[0]), Some(2));
        assert_eq!(keys(calls[1]), Some(1));
        assert_eq!(keys(calls[2]), Some(1));
        assert_eq!(keys(calls[3]), Some(0));
        let eager = |e: &Expr| checkpoint_runs_now(&model.spark_call(e).unwrap());
        assert_eq!(eager(calls[4]), Some(true));
        assert_eq!(eager(calls[5]), Some(false));
        assert_eq!(eager(calls[6]), Some(false));
        assert_eq!(eager(calls[7]), None);
    }
}
