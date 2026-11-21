//! "What does this call run?" — the ONE classifier for eager calls.
//!
//! A Spark call either builds a plan (lazy) or runs something: a job, a pull of rows
//! to the driver, a write, a display, a stream start, a checkpoint or a Delta Lake
//! table operation. [`run_of`] answers that for a (receiver, method) pair from one
//! table of rows ([`ROWS`]); [`SemanticModel::call_run`] answers it for a call
//! expression: typed Spark method calls, Delta Lake builder chains
//! (`dt.merge(..)…execute()`, `dt.optimize()…executeCompaction()`,
//! `DeltaTable.create(spark)…execute()`, followed through names bound once) and the
//! few function rows (`display(df)`, `DeltaTable.convertToDelta(..)`).
//!
//! The answer is a [`Run`]: its [`RunKind`], what the call [`Returns`] and whether it
//! bounds its own row count (`take` / `head` / `tail` / `first`, RDD `top` /
//! `takeOrdered` / `takeSample`). Rules filter on the answer;
//! each rule still decides itself whether a self-bounded pull matters. A new action
//! (a Spark 4 API, another Delta operation) is one row here.

use ruff_python_ast::{self as ast, Expr};

use crate::model::SemanticModel;
use crate::spark_call::{SparkCall, attribute_call};
use crate::types::Ty;
use crate::values::Hops;

/// What kind of eager work a call starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunKind {
    /// A Spark job whose result stays small or is a side effect (`count`, `isEmpty`,
    /// `foreach`, RDD `reduce`).
    Job,
    /// A job that pulls rows (or a local frame) to the driver (`collect`, `toPandas`,
    /// `take`, `first`).
    DriverPull,
    /// A batch write (`df.write…save()`, `writeTo(t).append()`, RDD `saveAs*`).
    Write,
    /// Starts a streaming query (`writeStream…start()` / `toTable()`).
    StreamStart,
    /// `show()` / `display()`: a job, usually for a person reading the output.
    Display,
    /// `df.checkpoint()`: materialises the plan.
    Checkpoint,
    /// A Delta Lake table operation (`delete`, `update`, `vacuum`, `merge…execute()`,
    /// `optimize…executeCompaction()`, `DeltaTable.create…execute()`, …). A job over
    /// the table, but not a DataFrame writer write.
    DeltaOp,
}

impl RunKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RunKind::Job => "job",
            RunKind::DriverPull => "driver-collect",
            RunKind::Write => "write",
            RunKind::StreamStart => "stream-start",
            RunKind::Display => "display",
            RunKind::Checkpoint => "checkpoint",
            RunKind::DeltaOp => "delta",
        }
    }

    /// Runs a batch Spark job over the data: a job action, a driver pull, a write or a
    /// Delta table operation (not a display, a stream start or a checkpoint).
    pub fn runs_job(self) -> bool {
        matches!(
            self,
            RunKind::Job | RunKind::DriverPull | RunKind::Write | RunKind::DeltaOp
        )
    }
}

/// What the call gives back to Python.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Returns {
    /// `None` (or nothing worth reading: a write, `show()`, `foreach`).
    Nothing,
    /// A number, a bool, a dict of counts, a handle (`count`, `isEmpty`, `start`).
    Value,
    /// One row (`first()`).
    Row,
    /// A list of rows (`collect`, `take`, `head(n)`).
    Rows,
    /// An iterator over the rows (`toLocalIterator`): iterable once, not indexable.
    RowIter,
    /// A dict built from key-value rows (RDD `collectAsMap`).
    Map,
    /// A driver-side frame or array (`toPandas`, `toArrow`, `to_numpy`).
    LocalFrame,
}

/// The classifier's answer for one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Run {
    pub kind: RunKind,
    pub returns: Returns,
    /// The call bounds its own row count (`take(n)`, `head()`, `tail(n)`, `first()`).
    pub self_bounded: bool,
    /// A probe: the job stops after a few rows (an implicit `LIMIT`: `first()`,
    /// `head()`, `take(n)`, `isEmpty()`), so it does not read the whole input.
    /// `tail(n)` and RDD `top` / `takeOrdered` / `takeSample` bound their result but
    /// still scan everything.
    pub probe: bool,
}

impl Run {
    /// A run of `kind` with no other property (a SQL statement's write, a test value).
    pub const fn of_kind(kind: RunKind) -> Self {
        Run {
            kind,
            returns: Returns::Nothing,
            self_bounded: false,
            probe: false,
        }
    }

    /// The result holds rows of the frame (one row, rows or a local frame).
    pub fn returns_rows(self) -> bool {
        matches!(
            self.returns,
            Returns::Row | Returns::Rows | Returns::RowIter | Returns::Map | Returns::LocalFrame
        )
    }

    /// A pull of the whole result to the driver (not bounded by the call itself).
    pub fn unbounded_pull(self) -> bool {
        self.kind == RunKind::DriverPull && !self.self_bounded
    }
}

/// The receiver side of a row: a Spark type, a Delta Lake builder (not an oracle type:
/// recognised by walking the chain back to its root) or a plain function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Recv {
    Ty(Ty),
    /// `dt.merge(src, cond).whenMatched…()` (a `DeltaMergeBuilder`).
    DeltaMergeBuilder,
    /// `dt.optimize()[.where(..)]` (a `DeltaOptimizeBuilder`).
    DeltaOptimizeBuilder,
    /// `DeltaTable.create(spark)…` (a `DeltaTableBuilder`).
    DeltaTableBuilder,
    /// A function called by qualified name (`method` is the qualified name).
    Function,
}

/// One classifier row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunRow {
    pub recv: Recv,
    /// Method name (qualified name for [`Recv::Function`] rows).
    pub method: &'static str,
    /// The user-facing name of what runs (`merge` for a merge builder's `execute`).
    pub name: &'static str,
    pub run: Run,
}

use Returns::{LocalFrame, Map, Nothing, Row, RowIter, Rows, Value};
use RunKind::{Checkpoint, DeltaOp, Display, DriverPull, Job, StreamStart, Write};

const fn row(recv: Recv, method: &'static str, kind: RunKind, returns: Returns) -> RunRow {
    RunRow {
        recv,
        method,
        name: method,
        run: Run {
            kind,
            returns,
            self_bounded: false,
            probe: false,
        },
    }
}

const fn bounded(recv: Recv, method: &'static str, returns: Returns) -> RunRow {
    RunRow {
        recv,
        method,
        name: method,
        run: Run {
            kind: DriverPull,
            returns,
            self_bounded: true,
            probe: false,
        },
    }
}

const fn named(recv: Recv, method: &'static str, name: &'static str) -> RunRow {
    RunRow {
        recv,
        method,
        name,
        run: Run {
            kind: DeltaOp,
            returns: Nothing,
            self_bounded: false,
            probe: false,
        },
    }
}

impl RunRow {
    /// The same row, marked a probe ([`Run::probe`]).
    const fn probe(self) -> Self {
        RunRow {
            run: Run {
                probe: true,
                ..self.run
            },
            ..self
        }
    }
}

const DF: Recv = Recv::Ty(Ty::DataFrame);
const RDD: Recv = Recv::Ty(Ty::Rdd);
const W: Recv = Recv::Ty(Ty::DataFrameWriter);
const W2: Recv = Recv::Ty(Ty::DataFrameWriterV2);
const PS: Recv = Recv::Ty(Ty::PandasOnSparkFrame);
const DT: Recv = Recv::Ty(Ty::DeltaTable);
const FN: Recv = Recv::Function;

/// Every call that runs something: (receiver, method) → kind, result, self-bounded.
pub const ROWS: &[RunRow] = &[
    // ---- DataFrame
    row(DF, "count", Job, Value),
    row(DF, "isEmpty", Job, Value).probe(),
    row(DF, "foreach", Job, Nothing),
    row(DF, "foreachPartition", Job, Nothing),
    bounded(DF, "first", Row).probe(),
    bounded(DF, "head", Rows).probe(),
    bounded(DF, "take", Rows).probe(),
    bounded(DF, "tail", Rows),
    row(DF, "collect", DriverPull, Rows),
    row(DF, "toLocalIterator", DriverPull, RowIter),
    row(DF, "toPandas", DriverPull, LocalFrame),
    row(DF, "toArrow", DriverPull, LocalFrame),
    row(DF, "show", Display, Nothing),
    row(DF, "display", Display, Nothing),
    row(DF, "checkpoint", Checkpoint, Value),
    // ---- RDD
    row(RDD, "count", Job, Value),
    row(RDD, "isEmpty", Job, Value).probe(),
    row(RDD, "foreach", Job, Nothing),
    row(RDD, "foreachPartition", Job, Nothing),
    row(RDD, "reduce", Job, Value),
    row(RDD, "fold", Job, Value),
    row(RDD, "aggregate", Job, Value),
    row(RDD, "sum", Job, Value),
    row(RDD, "max", Job, Value),
    row(RDD, "min", Job, Value),
    row(RDD, "mean", Job, Value),
    row(RDD, "stats", Job, Value),
    row(RDD, "countByKey", Job, Value),
    row(RDD, "countByValue", Job, Value),
    row(RDD, "treeReduce", Job, Value),
    row(RDD, "treeAggregate", Job, Value),
    bounded(RDD, "first", Row).probe(),
    bounded(RDD, "take", Rows).probe(),
    bounded(RDD, "top", Rows),
    bounded(RDD, "takeOrdered", Rows),
    bounded(RDD, "takeSample", Rows),
    row(RDD, "collect", DriverPull, Rows),
    row(RDD, "collectAsMap", DriverPull, Map),
    row(RDD, "toLocalIterator", DriverPull, RowIter),
    row(RDD, "saveAsTextFile", Write, Nothing),
    row(RDD, "saveAsPickleFile", Write, Nothing),
    row(RDD, "saveAsHadoopFile", Write, Nothing),
    row(RDD, "saveAsSequenceFile", Write, Nothing),
    row(RDD, "saveAsNewAPIHadoopFile", Write, Nothing),
    // ---- writers
    row(W, "save", Write, Nothing),
    row(W, "saveAsTable", Write, Nothing),
    row(W, "insertInto", Write, Nothing),
    row(W, "parquet", Write, Nothing),
    row(W, "csv", Write, Nothing),
    row(W, "json", Write, Nothing),
    row(W, "orc", Write, Nothing),
    row(W, "text", Write, Nothing),
    row(W, "jdbc", Write, Nothing),
    row(W, "xml", Write, Nothing),
    row(W2, "create", Write, Nothing),
    row(W2, "replace", Write, Nothing),
    row(W2, "createOrReplace", Write, Nothing),
    row(W2, "append", Write, Nothing),
    row(W2, "overwrite", Write, Nothing),
    row(W2, "overwritePartitions", Write, Nothing),
    row(Recv::Ty(Ty::MergeIntoWriter), "merge", Write, Nothing),
    row(Recv::Ty(Ty::DataStreamWriter), "start", StreamStart, Value),
    row(
        Recv::Ty(Ty::DataStreamWriter),
        "toTable",
        StreamStart,
        Value,
    ),
    // ---- pandas-on-Spark
    row(PS, "to_pandas", DriverPull, LocalFrame),
    row(PS, "to_numpy", DriverPull, LocalFrame),
    // ---- Delta Lake (delta-spark): eager table operations
    named(DT, "delete", "delete"),
    named(DT, "update", "update"),
    named(DT, "updateExpr", "updateExpr"),
    named(DT, "vacuum", "vacuum"),
    named(DT, "restoreToVersion", "restoreToVersion"),
    named(DT, "restoreToTimestamp", "restoreToTimestamp"),
    named(DT, "generate", "generate"),
    named(DT, "upgradeTableProtocol", "upgradeTableProtocol"),
    named(DT, "clone", "clone"),
    named(DT, "cloneAtVersion", "cloneAtVersion"),
    named(DT, "cloneAtTimestamp", "cloneAtTimestamp"),
    named(Recv::DeltaMergeBuilder, "execute", "merge"),
    named(Recv::DeltaOptimizeBuilder, "executeCompaction", "optimize"),
    named(Recv::DeltaOptimizeBuilder, "executeZOrderBy", "optimize"),
    // The name is the builder factory (`create`, `createIfNotExists`, …).
    named(Recv::DeltaTableBuilder, "execute", "create"),
    // ---- functions
    named(
        FN,
        "delta.tables.DeltaTable.convertToDelta",
        "convertToDelta",
    ),
    named(FN, "delta.DeltaTable.convertToDelta", "convertToDelta"),
    RunRow {
        recv: FN,
        method: "databricks.display",
        name: "display",
        run: Run::of_kind(Display),
    },
];

/// The row for `recv.method(...)`, if the call runs something.
pub fn row_of(recv: Recv, method: &str) -> Option<&'static RunRow> {
    ROWS.iter().find(|r| r.recv == recv && r.method == method)
}

/// What `recv.method(...)` runs, if anything.
pub fn run_of(recv: Recv, method: &str) -> Option<Run> {
    row_of(recv, method).map(|r| r.run)
}

/// What a method of a value of Spark type `ty` runs, if anything.
pub fn run_of_ty(ty: Ty, method: &str) -> Option<Run> {
    run_of(Recv::Ty(ty), method)
}

/// `DeltaMergeBuilder` clauses between `merge(..)` and `execute()`.
const DELTA_MERGE_STEPS: &[&str] = &[
    "whenMatchedUpdate",
    "whenMatchedUpdateAll",
    "whenMatchedDelete",
    "whenNotMatchedInsert",
    "whenNotMatchedInsertAll",
    "whenNotMatchedBySourceUpdate",
    "whenNotMatchedBySourceDelete",
    "withSchemaEvolution",
];
/// `DeltaTable.create(spark)` builder factories ending in `execute()`.
const DELTA_CREATE: &[&str] = &["create", "createIfNotExists", "replace", "createOrReplace"];

/// A call expression classified by [`SemanticModel::call_run`].
#[derive(Debug, Clone, Copy)]
pub struct CallRun<'e> {
    pub run: Run,
    /// Which row matched.
    pub recv: Recv,
    /// The method as written (`execute`), or the function's qualified name.
    pub method: &'static str,
    /// The user-facing name of what runs (`count`, `merge`, `optimize`, `create`,
    /// `convertToDelta`, `display`).
    pub name: &'static str,
    pub call: &'e ast::ExprCall,
    /// The value the call runs on: the method's receiver, `display(x)`'s argument;
    /// `None` for `DeltaTable.convertToDelta(..)`.
    pub on: Option<&'e Expr>,
    /// The Spark method call, when the receiver is a typed Spark value.
    pub spark: Option<SparkCall<'e>>,
    /// Delta builder chains: the root call (`dt.merge(src, cond)`, `dt.optimize()`,
    /// `DeltaTable.create(spark)`), possibly reached through names.
    pub delta_root: Option<&'e ast::ExprCall>,
}

impl CallRun<'_> {
    pub fn kind(&self) -> RunKind {
        self.run.kind
    }
}

/// A statement that sets up eager work but never runs it: the chain stops before the
/// call that would run it (`df.write.mode("overwrite")` with no `save()`), so nothing
/// happens. See [`SemanticModel::unrun_builder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Unrun {
    /// `df.write…` (`DataFrameWriter`).
    Write,
    /// `df.writeTo(t)…` (`DataFrameWriterV2`).
    WriteV2,
    /// `df.writeStream…` (`DataStreamWriter`).
    Stream,
    /// `df.mergeInto(t, cond)…` (Spark 4 `MergeIntoWriter`).
    MergeInto,
    /// `dt.merge(src, cond)…` (Delta Lake `DeltaMergeBuilder`).
    DeltaMerge,
    /// `dt.optimize()…` (Delta Lake `DeltaOptimizeBuilder`).
    DeltaOptimize,
    /// `DeltaTable.create(spark)…` (Delta Lake `DeltaTableBuilder`).
    DeltaCreate,
}

impl Unrun {
    /// What never runs, for a message: `a write`, `a streaming query`, `a merge`, ...
    pub fn what(self) -> &'static str {
        match self {
            Unrun::Write | Unrun::WriteV2 => "a write",
            Unrun::Stream => "a streaming query",
            Unrun::MergeInto | Unrun::DeltaMerge => "a merge",
            Unrun::DeltaOptimize => "an OPTIMIZE",
            Unrun::DeltaCreate => "a table creation",
        }
    }

    /// The calls that would run it.
    pub fn finish(self) -> &'static str {
        match self {
            Unrun::Write => "save() / saveAsTable() / insertInto()",
            Unrun::WriteV2 => "append() / overwrite() / create() / createOrReplace()",
            Unrun::Stream => "start() / toTable()",
            Unrun::MergeInto => "merge()",
            Unrun::DeltaMerge | Unrun::DeltaCreate => "execute()",
            Unrun::DeltaOptimize => "executeCompaction() / executeZOrderBy()",
        }
    }
}

impl<'a> SemanticModel<'a> {
    /// What the call `e` runs (see the module docs), `None` for a lazy call, a
    /// non-Spark call or a Delta builder that is never executed.
    pub fn call_run<'e>(&self, e: &'e Expr) -> Option<CallRun<'e>>
    where
        'a: 'e,
    {
        let Expr::Call(c) = e else { return None };
        let fn_row = |q: &str| row_of(Recv::Function, q);
        if let Some(q) = self.qualified_name(&c.func)
            && let Some(r) = fn_row(&q)
        {
            let on = c.arguments.args.first();
            if r.run.kind == RunKind::Display && !on.is_some_and(|a| self.is_dataframe(a)) {
                return None;
            }
            return Some(CallRun {
                run: r.run,
                recv: r.recv,
                method: r.method,
                name: r.name,
                call: c,
                on: if r.run.kind == RunKind::Display {
                    on
                } else {
                    None
                },
                spark: None,
                delta_root: None,
            });
        }
        let (_, recv, m) = attribute_call(e)?;
        let builder = match m {
            "execute" => self
                .delta_builder_root(recv, DELTA_MERGE_STEPS, &["merge"])
                .map(|root| (Recv::DeltaMergeBuilder, root, None))
                .or_else(|| {
                    self.delta_create_root(recv)
                        .map(|(root, f)| (Recv::DeltaTableBuilder, root, Some(f)))
                }),
            "executeCompaction" | "executeZOrderBy" => self
                .delta_builder_root(recv, &["where"], &["optimize"])
                .map(|root| (Recv::DeltaOptimizeBuilder, root, None)),
            _ => None,
        };
        if let Some((rv, root, factory)) = builder {
            let r = row_of(rv, m)?;
            return Some(CallRun {
                run: r.run,
                recv: rv,
                method: r.method,
                name: factory.unwrap_or(r.name),
                call: c,
                on: Some(recv),
                spark: None,
                delta_root: Some(root),
            });
        }
        let sc = self.spark_call(e)?;
        let r = row_of(Recv::Ty(sc.class()), m)?;
        Some(CallRun {
            run: r.run,
            recv: r.recv,
            method: r.method,
            name: r.name,
            call: c,
            on: Some(recv),
            spark: Some(sc),
            delta_root: None,
        })
    }

    fn is_delta_table(&self, e: &Expr) -> bool {
        let mut cur = e;
        while let Some((_, r, "alias")) = attribute_call(cur) {
            cur = r;
        }
        self.ty_of(cur).ty == Ty::DeltaTable
    }

    /// Follow `steps` back from `e` to a `root` method called on a `DeltaTable`; the
    /// root call.
    fn delta_builder_root<'e>(
        &self,
        e: &'e Expr,
        steps: &[&str],
        roots: &[&'static str],
    ) -> Option<&'e ast::ExprCall>
    where
        'a: 'e,
    {
        let mut cur = e;
        let mut hops = Hops::DEFAULT;
        loop {
            if let Some((v, h)) = self.hop_one(cur, hops) {
                hops = h;
                cur = v;
                continue;
            }
            let (c, r, m) = attribute_call(cur)?;
            if roots.contains(&m) {
                return self.is_delta_table(r).then_some(c);
            }
            if !steps.contains(&m) {
                return None;
            }
            cur = r;
        }
    }

    /// `e` (an expression statement's value) sets up a write, a stream, a merge, an
    /// OPTIMIZE or a table creation and stops before the call that runs it. Only a
    /// chain that starts the builder in the same expression counts (`df.write…`,
    /// `df.writeTo(t)…`, `dt.merge(..)…`, `DeltaTable.create(spark)…`): Spark's writers
    /// and some Delta builders change themselves in place, so `w.format("delta")` on a
    /// writer held in a name is a real configuration step.
    pub fn unrun_builder(&self, e: &Expr) -> Option<Unrun> {
        if !matches!(e, Expr::Call(_) | Expr::Attribute(_)) {
            return None;
        }
        let delta = |steps: &[&str], root: &str| -> bool {
            let mut cur = e;
            while let Some((_, r, m)) = attribute_call(cur) {
                if m == root {
                    return self.is_delta_table(r);
                }
                if !steps.contains(&m) {
                    return false;
                }
                cur = r;
            }
            false
        };
        if delta(DELTA_MERGE_STEPS, "merge") {
            return Some(Unrun::DeltaMerge);
        }
        if delta(&["where"], "optimize") {
            return Some(Unrun::DeltaOptimize);
        }
        if self.fresh_delta_create(e) {
            return Some(Unrun::DeltaCreate);
        }
        let unrun = match self.ty_of(e).ty {
            Ty::DataFrameWriter => Unrun::Write,
            Ty::DataFrameWriterV2 => Unrun::WriteV2,
            Ty::DataStreamWriter => Unrun::Stream,
            Ty::MergeIntoWriter | Ty::MergeIntoClause => Unrun::MergeInto,
            _ => return None,
        };
        self.fresh_writer(e).then_some(unrun)
    }

    /// The chain under `e` starts at `<frame>.write` / `.writeStream` /
    /// `.writeTo(..)` / `.mergeInto(..)` with no name in between.
    fn fresh_writer(&self, e: &Expr) -> bool {
        let mut cur = e;
        loop {
            match cur {
                Expr::Attribute(a) if matches!(a.attr.as_str(), "write" | "writeStream") => {
                    return self.is_dataframe(&a.value);
                }
                Expr::Attribute(a) => cur = &a.value,
                Expr::Call(c) => {
                    let Expr::Attribute(a) = &*c.func else {
                        return false;
                    };
                    if matches!(a.attr.as_str(), "writeTo" | "mergeInto") {
                        return self.is_dataframe(&a.value);
                    }
                    cur = &a.value;
                }
                _ => return false,
            }
        }
    }

    /// `DeltaTable.create(spark)…` (no `execute()`), with no name in between.
    fn fresh_delta_create(&self, e: &Expr) -> bool {
        if matches!(attribute_call(e), Some((_, _, "execute"))) {
            return false;
        }
        let mut cur = e;
        while let Expr::Call(c) = cur {
            if let Some(q) = self.qualified_name(&c.func) {
                let short = q
                    .strip_prefix("delta.tables.DeltaTable.")
                    .or_else(|| q.strip_prefix("delta.DeltaTable."));
                if short.is_some_and(|s| DELTA_CREATE.contains(&s)) {
                    return true;
                }
            }
            let Expr::Attribute(a) = &*c.func else {
                return false;
            };
            cur = &a.value;
        }
        false
    }

    /// `DeltaTable.create(spark).tableName(..).addColumn(..)….execute()`: the factory
    /// call and its name.
    fn delta_create_root<'e>(&self, e: &'e Expr) -> Option<(&'e ast::ExprCall, &'static str)>
    where
        'a: 'e,
    {
        let mut cur = e;
        let mut hops = Hops::DEFAULT;
        loop {
            if let Some((v, h)) = self.hop_one(cur, hops) {
                hops = h;
                cur = v;
                continue;
            }
            let Expr::Call(c) = cur else { return None };
            if let Some(q) = self.qualified_name(&c.func) {
                let short = q
                    .strip_prefix("delta.tables.DeltaTable.")
                    .or_else(|| q.strip_prefix("delta.DeltaTable."));
                if let Some(s) = short
                    && let Some(&f) = DELTA_CREATE.iter().find(|f| **f == s)
                {
                    return Some((c, f));
                }
            }
            let Expr::Attribute(a) = &*c.func else {
                return None;
            };
            cur = &a.value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// (receiver type, method) → (kind, returns, self-bounded): the whole contract.
    #[test]
    fn table() {
        use Recv::Ty as T;
        type Want = Option<(RunKind, Returns, bool)>;
        let cases: &[(Recv, &str, Want)] = &[
            (T(Ty::DataFrame), "count", Some((Job, Value, false))),
            (T(Ty::DataFrame), "isEmpty", Some((Job, Value, false))),
            (T(Ty::DataFrame), "foreach", Some((Job, Nothing, false))),
            (T(Ty::DataFrame), "first", Some((DriverPull, Row, true))),
            (T(Ty::DataFrame), "head", Some((DriverPull, Rows, true))),
            (T(Ty::DataFrame), "take", Some((DriverPull, Rows, true))),
            (T(Ty::DataFrame), "tail", Some((DriverPull, Rows, true))),
            (T(Ty::DataFrame), "collect", Some((DriverPull, Rows, false))),
            (
                T(Ty::DataFrame),
                "toLocalIterator",
                Some((DriverPull, RowIter, false)),
            ),
            (
                T(Ty::DataFrame),
                "toPandas",
                Some((DriverPull, LocalFrame, false)),
            ),
            (
                T(Ty::DataFrame),
                "toArrow",
                Some((DriverPull, LocalFrame, false)),
            ),
            (T(Ty::DataFrame), "show", Some((Display, Nothing, false))),
            (
                T(Ty::DataFrame),
                "checkpoint",
                Some((Checkpoint, Value, false)),
            ),
            (T(Ty::DataFrame), "select", None),
            (T(Ty::DataFrame), "limit", None),
            (T(Ty::DataFrame), "localCheckpoint", None),
            (T(Ty::Rdd), "first", Some((DriverPull, Row, true))),
            (T(Ty::Rdd), "takeOrdered", Some((DriverPull, Rows, true))),
            (T(Ty::Rdd), "collectAsMap", Some((DriverPull, Map, false))),
            (T(Ty::Rdd), "reduce", Some((Job, Value, false))),
            (T(Ty::Rdd), "saveAsTextFile", Some((Write, Nothing, false))),
            (T(Ty::Rdd), "map", None),
            (
                T(Ty::DataFrameWriter),
                "saveAsTable",
                Some((Write, Nothing, false)),
            ),
            (T(Ty::DataFrameWriter), "mode", None),
            (
                T(Ty::DataFrameWriterV2),
                "append",
                Some((Write, Nothing, false)),
            ),
            (
                T(Ty::MergeIntoWriter),
                "merge",
                Some((Write, Nothing, false)),
            ),
            (T(Ty::MergeIntoClause), "updateAll", None),
            (
                T(Ty::DataStreamWriter),
                "start",
                Some((StreamStart, Value, false)),
            ),
            (
                T(Ty::PandasOnSparkFrame),
                "to_pandas",
                Some((DriverPull, LocalFrame, false)),
            ),
            (T(Ty::DeltaTable), "vacuum", Some((DeltaOp, Nothing, false))),
            (T(Ty::DeltaTable), "delete", Some((DeltaOp, Nothing, false))),
            (T(Ty::DeltaTable), "toDF", None),
            (T(Ty::DeltaTable), "merge", None),
            (
                Recv::DeltaMergeBuilder,
                "execute",
                Some((DeltaOp, Nothing, false)),
            ),
            (
                Recv::DeltaOptimizeBuilder,
                "executeZOrderBy",
                Some((DeltaOp, Nothing, false)),
            ),
            (
                Recv::DeltaTableBuilder,
                "execute",
                Some((DeltaOp, Nothing, false)),
            ),
            (
                Recv::Function,
                "databricks.display",
                Some((Display, Nothing, false)),
            ),
            (T(Ty::List(crate::types::Elem::Unknown)), "count", None),
            (T(Ty::PandasFrame), "head", None),
        ];
        for (recv, m, want) in cases {
            let got = run_of(*recv, m).map(|r| (r.kind, r.returns, r.self_bounded));
            assert_eq!(got, *want, "{recv:?}.{m}");
        }
    }

    /// The probe rows ([`Run::probe`]): exactly the calls that stop after a few rows.
    #[test]
    fn probes() {
        let mut got: Vec<String> = ROWS
            .iter()
            .filter(|r| r.run.probe)
            .map(|r| format!("{:?}.{}", r.recv, r.method))
            .collect();
        got.sort();
        let want = [
            "Ty(DataFrame).first",
            "Ty(DataFrame).head",
            "Ty(DataFrame).isEmpty",
            "Ty(DataFrame).take",
            "Ty(Rdd).first",
            "Ty(Rdd).isEmpty",
            "Ty(Rdd).take",
        ];
        assert_eq!(got, want);
        // bounded results that still scan everything are no probes
        for (recv, m) in [
            (Ty::DataFrame, "tail"),
            (Ty::DataFrame, "count"),
            (Ty::Rdd, "top"),
            (Ty::Rdd, "takeOrdered"),
            (Ty::Rdd, "takeSample"),
        ] {
            assert!(!run_of_ty(recv, m).is_some_and(|r| r.probe), "{recv:?}.{m}");
        }
        assert!(!Run::of_kind(Write).probe);
    }

    #[test]
    fn rows_are_unique() {
        for (i, a) in ROWS.iter().enumerate() {
            for b in &ROWS[i + 1..] {
                assert!(
                    !(a.recv == b.recv && a.method == b.method),
                    "duplicate row {:?}.{}",
                    a.recv,
                    a.method
                );
            }
        }
    }

    #[test]
    fn kinds() {
        assert!(DeltaOp.runs_job() && DriverPull.runs_job() && Write.runs_job());
        assert!(!Display.runs_job() && !StreamStart.runs_job() && !Checkpoint.runs_job());
        let first = run_of_ty(Ty::DataFrame, "first").unwrap();
        assert!(first.returns_rows() && !first.unbounded_pull());
        assert!(
            run_of_ty(Ty::DataFrame, "toArrow")
                .unwrap()
                .unbounded_pull()
        );
        assert!(!run_of_ty(Ty::DataFrame, "count").unwrap().returns_rows());
    }

    /// (call text → kind, name, delta root text) of every call in `src` that runs.
    fn runs(src: &str) -> Vec<(String, RunKind, &'static str, Option<String>)> {
        use crate::{NoSummaries, SemanticOptions, build_model};
        use ruff_python_ast::visitor::{self, Visitor};
        use ruff_text_size::{Ranged, TextSize};
        let m = ruff_python_parser::parse_module(src)
            .expect("valid test source")
            .into_syntax();
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        struct V<'s, 'm, 'a> {
            src: &'s str,
            model: &'m SemanticModel<'a>,
            out: Vec<(String, RunKind, &'static str, Option<String>)>,
        }
        impl<'b> Visitor<'b> for V<'_, '_, 'b> {
            fn visit_expr(&mut self, e: &'b Expr) {
                if let Some(r) = self.model.call_run(e) {
                    self.out.push((
                        self.src[e.range()].to_string(),
                        r.kind(),
                        r.name,
                        r.delta_root.map(|c| self.src[c.range()].to_string()),
                    ));
                }
                visitor::walk_expr(self, e);
            }
        }
        let mut v = V {
            src,
            model: &model,
            out: Vec::new(),
        };
        v.visit_body(&m.body);
        v.out
    }

    #[test]
    fn calls() {
        let src = "\
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.table('s')
dt = DeltaTable.forName(spark, 't')
dt.vacuum()
dt.alias('t').delete('x')
pending = dt.merge(df, 'c').whenMatchedUpdateAll()
pending.execute()
dt.optimize().where('d').executeZOrderBy('id')
DeltaTable.createIfNotExists(spark).tableName('e').execute()
DeltaTable.convertToDelta(spark, 'parquet.`/p`')
never = dt.merge(df, 'c').whenMatchedDelete()
display(df)
display([1])
df.first()
df.select('a')
class Job:
    def execute(self):
        return 1
Job().execute()
";
        let got = runs(src);
        let want: &[(&str, RunKind, &str, Option<&str>)] = &[
            ("dt.vacuum()", DeltaOp, "vacuum", None),
            ("dt.alias('t').delete('x')", DeltaOp, "delete", None),
            (
                "pending.execute()",
                DeltaOp,
                "merge",
                Some("dt.merge(df, 'c')"),
            ),
            (
                "dt.optimize().where('d').executeZOrderBy('id')",
                DeltaOp,
                "optimize",
                Some("dt.optimize()"),
            ),
            (
                "DeltaTable.createIfNotExists(spark).tableName('e').execute()",
                DeltaOp,
                "createIfNotExists",
                Some("DeltaTable.createIfNotExists(spark)"),
            ),
            (
                "DeltaTable.convertToDelta(spark, 'parquet.`/p`')",
                DeltaOp,
                "convertToDelta",
                None,
            ),
            ("display(df)", Display, "display", None),
            ("df.first()", DriverPull, "first", None),
        ];
        let got: Vec<_> = got
            .iter()
            .map(|(t, k, n, r)| (t.as_str(), *k, *n, r.as_deref()))
            .collect();
        assert_eq!(got, want);
    }

    /// Expression statements of `src` → what never runs (`None` = not an unrun builder).
    #[test]
    fn unrun_builders() {
        use crate::{NoSummaries, SemanticOptions, build_model};
        use ruff_python_ast::Stmt;
        use ruff_text_size::{Ranged, TextSize};
        let src = "\
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.table('s')
dt = DeltaTable.forName(spark, 't')
df.write.mode('overwrite').format('delta')
df.write
df.select('a').write.partitionBy('d')
df.writeStream.format('delta').option('checkpointLocation', '/c')
df.writeTo('t').using('delta')
df.mergeInto('t', df.a == 1).whenMatched().updateAll()
dt.merge(df, 'c').whenMatchedUpdateAll()
dt.alias('t').merge(df, 'c')
dt.optimize().where('d = 1')
DeltaTable.create(spark).tableName('e').addColumn('id', 'INT')
df.write.mode('overwrite').saveAsTable('t')
df.writeStream.format('delta').start()
dt.merge(df, 'c').whenMatchedUpdateAll().execute()
dt.optimize().executeCompaction()
DeltaTable.create(spark).tableName('e').execute()
w = df.write.mode('overwrite')
w.format('delta')
ob = dt.optimize()
ob.where('d = 1')
b = DeltaTable.create(spark)
b.tableName('e')
df.select('a')
dt.toDF()
";
        let m = ruff_python_parser::parse_module(src)
            .expect("valid test source")
            .into_syntax();
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let got: Vec<(&str, Option<Unrun>)> = m
            .body
            .iter()
            .filter_map(|s| match s {
                Stmt::Expr(e) => Some((&src[e.range()], model.unrun_builder(&e.value))),
                _ => None,
            })
            .collect();
        use Unrun::*;
        let want: &[(&str, Option<Unrun>)] = &[
            ("df.write.mode('overwrite').format('delta')", Some(Write)),
            ("df.write", Some(Write)),
            ("df.select('a').write.partitionBy('d')", Some(Write)),
            (
                "df.writeStream.format('delta').option('checkpointLocation', '/c')",
                Some(Stream),
            ),
            ("df.writeTo('t').using('delta')", Some(WriteV2)),
            (
                "df.mergeInto('t', df.a == 1).whenMatched().updateAll()",
                Some(MergeInto),
            ),
            ("dt.merge(df, 'c').whenMatchedUpdateAll()", Some(DeltaMerge)),
            ("dt.alias('t').merge(df, 'c')", Some(DeltaMerge)),
            ("dt.optimize().where('d = 1')", Some(DeltaOptimize)),
            (
                "DeltaTable.create(spark).tableName('e').addColumn('id', 'INT')",
                Some(DeltaCreate),
            ),
            ("df.write.mode('overwrite').saveAsTable('t')", None),
            ("df.writeStream.format('delta').start()", None),
            ("dt.merge(df, 'c').whenMatchedUpdateAll().execute()", None),
            ("dt.optimize().executeCompaction()", None),
            ("DeltaTable.create(spark).tableName('e').execute()", None),
            // Writers and Delta builders held in a name change in place.
            ("w.format('delta')", None),
            ("ob.where('d = 1')", None),
            ("b.tableName('e')", None),
            ("df.select('a')", None),
            ("dt.toDF()", None),
        ];
        assert_eq!(got, want);
    }
}
