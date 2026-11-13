//! Lineage counting: how often a DataFrame's plan is computed, and what happens to
//! its caches. PRF006 recomputed-lineage, PRF008 cache-single-use, PRF009
//! cache-never-unpersisted (the flow-based rules; see [`super::flow`]).
//!
//! - [`CountPolicy`]: the one answer to "does this consumer compute the frame's plan
//!   again?" — asked by the per-file [`Counter`] (PRF006, PRF008's reuse count) and
//!   by PRF008's direct-use weights.
//! - [`Counter`]: binding → the use sites ending its chains, through every derived
//!   frame, deduplicated by site, and the most of them on one execution path.
//! - [`cache_fate`]: whether a cache is unpersisted / escapes (PRF008, PRF009).

use ruff_python_ast::{self as ast, Expr, ExprContext, Stmt};
use ruff_text_size::{Ranged, TextRange};
use rustc_hash::FxHashSet;
use sparklint_semantic::api::ActionKind;
use sparklint_semantic::{BindingId, BindingKind, MODULE_SCOPE, SemanticModel, Ty};

use super::Rule;
use super::actions::method_range;
use super::exec_context::Run;
use super::flow::Sink;
use super::helpers::*;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Diagnostic, Edit, Fix};

/// PRF006/008/009 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PRF006").finish(finish_recomputed_lineage),
    Rule::new("PRF008").finish(finish_cache),
    Rule::new("PRF009")
        .call(note_clear_cache)
        .finish(finish_cache_never_unpersisted),
];

// ------------------------------------------------------------------ counting policy

/// What ends a chain over a frame, as far as counting recomputation cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consumer {
    /// A Spark job over the plan: a job action (`count`), a driver collect, a batch
    /// write.
    Job,
    /// `show()` / `display()`: a job too, but usually debugging.
    Display,
    /// `cache()` / `persist()` / `checkpoint()`: computes the plan once; consumers of
    /// the cached frame read the cache, so the walk ends here.
    Cache,
    /// Anything else (a metadata action, a streaming sink, ...).
    Other,
}

impl Consumer {
    pub(crate) fn of_action(kind: ActionKind) -> Self {
        match kind {
            ActionKind::Job | ActionKind::DriverCollect | ActionKind::Write => Consumer::Job,
            ActionKind::Display => Consumer::Display,
            _ => Consumer::Other,
        }
    }
}

/// The one counting policy for "this consumer computes the frame's plan again".
///
/// - Counted consumers: jobs and caches; `show()` / `display()` only with
///   [`Self::displays`].
/// - A site that never completes on the passing path ([`Run::completes`]: an
///   `assert` message, a `raise`, a `pytest.raises` body) contributes nothing, not
///   even the frames derived there.
/// - A cache ends the walk: what reads the cached frame does not recompute the
///   lineage above it.
/// - How many: the most counted consumers on one execution path (exclusive
///   `if`/`else` arms once each). The per-file [`Counter`] also weights a use in a
///   loop that does not contain the frame's binding as 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CountPolicy {
    /// `show()` / `display()` count as a consumer.
    pub displays: bool,
}

impl CountPolicy {
    /// PRF006: is the lineage recomputed? (display calls are debugging)
    pub(crate) const RECOMPUTE: Self = Self { displays: false };
    /// PRF008: is the cache read again? (a `show()` of a derived frame reads it too)
    pub(crate) const CACHE_REUSE: Self = Self { displays: true };

    /// Whether a use at a site that runs like `run` takes part at all.
    pub(crate) fn site_counts(self, run: Run) -> bool {
        run.completes()
    }

    /// Whether a consumer ending a chain counts as one computation of the plan.
    pub(crate) fn consumer_counts(self, c: Consumer) -> bool {
        match c {
            Consumer::Job | Consumer::Cache => true,
            Consumer::Display => self.displays,
            Consumer::Other => false,
        }
    }
}

// ------------------------------------------------------------------ PRF006

/// Methods that add a shuffle, a window or opaque Python work to a plan.
const EXPENSIVE_METHODS: &[&str] = &[
    "join",
    "crossJoin",
    "lateralJoin",
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
    "intersect",
    "intersectAll",
    "subtract",
    "exceptAll",
    "pivot",
    "mapInPandas",
    "mapInArrow",
    "applyInPandas",
    "applyInArrow",
];

/// Whether the plan behind `e` contains an expensive node (join, aggregation,
/// window, sort, UDF, or a JDBC/CSV/JSON source).
pub fn lineage_expensive(ch: &Checker, e: &Expr, depth: u32) -> bool {
    if depth > 6 {
        return false;
    }
    let mut cur = e;
    loop {
        if let Some((c, recv, m)) = method_call(cur) {
            let rt = ch.model.ty_of(recv);
            if EXPENSIVE_METHODS.contains(&m) && matches!(rt.ty, Ty::DataFrame | Ty::GroupedData) {
                return true;
            }
            if rt.ty.is_session()
                && m == "sql"
                && ch.model.sql_query(c).is_some_and(|q| q.is_expensive())
            {
                return true;
            }
            if matches!(rt.ty, Ty::DataFrameReader)
                && (matches!(m, "csv" | "json" | "jdbc" | "xml")
                    || (m == "format"
                        && c.arguments
                            .args
                            .first()
                            .and_then(sparklint_semantic::str_literal)
                            .is_some_and(|f| {
                                matches!(
                                    f.to_ascii_lowercase().as_str(),
                                    "csv" | "json" | "jdbc" | "xml"
                                )
                            })))
            {
                return true;
            }
            let mut hit = false;
            for a in c
                .arguments
                .args
                .iter()
                .chain(c.arguments.keywords.iter().map(|k| &k.value))
            {
                walk_exprs(a, &mut |x| {
                    if hit {
                        return;
                    }
                    if let Expr::Call(xc) = x {
                        if ch.model.ty_of(&xc.func).ty == Ty::Udf {
                            hit = true;
                        }
                        if let Some((_, _, xm)) = method_call(x)
                            && xm == "over"
                        {
                            hit = true;
                        }
                    }
                });
            }
            if hit {
                return true;
            }
            cur = recv;
            continue;
        }
        match cur {
            Expr::Attribute(a) => cur = &a.value,
            Expr::Subscript(s) => cur = &s.value,
            Expr::Name(n) => {
                return ch.model.reaching(n).iter().any(|b| {
                    let b = ch.model.binding(*b);
                    b.value.is_some_and(|v| {
                        v.range() != cur.range() && lineage_expensive(ch, v, depth + 1)
                    })
                });
            }
            _ => return false,
        }
    }
}

/// One action/write/cache reached from a binding: the use site that ends the
/// chain, its branch path and its loop weight.
#[derive(Debug, Clone)]
pub(crate) struct Reached {
    pub at: TextRange,
    pub path: Vec<sparklint_semantic::BranchStep>,
    pub w: u32,
}

/// Counts the consumers reached from a binding under a [`CountPolicy`].
pub(crate) struct Counter<'c, 'a> {
    ch: &'c Checker<'a>,
    policy: CountPolicy,
    memo: rustc_hash::FxHashMap<BindingId, std::rc::Rc<Vec<Reached>>>,
    visiting: FxHashSet<BindingId>,
}

impl<'c, 'a> Counter<'c, 'a> {
    pub(crate) fn new(ch: &'c Checker<'a>, policy: CountPolicy) -> Self {
        Self {
            ch,
            policy,
            memo: Default::default(),
            visiting: Default::default(),
        }
    }

    /// Consumers reachable on one execution path from binding `b` (derived
    /// DataFrames included: their actions recompute `b` too).
    pub(crate) fn count(&mut self, b: BindingId) -> u32 {
        let items: Vec<_> = self
            .reached(b)
            .iter()
            .map(|r| (r.path.clone(), r.w))
            .collect();
        SemanticModel::max_path_weight(&items)
    }

    /// The use sites ending `b`'s chains, each counted once: a name rebound on
    /// one branch (`if c: df = df.join(x)`) makes a later `df.count()` reach
    /// both bindings, but it runs once, through whichever binding was live.
    pub(crate) fn reached(&mut self, b: BindingId) -> std::rc::Rc<Vec<Reached>> {
        if let Some(r) = self.memo.get(&b) {
            return r.clone();
        }
        if !self.visiting.insert(b) {
            return Default::default();
        }
        let policy = self.policy;
        let uses = self.ch.state.flow.uses.get(&b).cloned().unwrap_or_default();
        let mut out: Vec<Reached> = Vec::new();
        let mut push = |r: Reached| match out.iter_mut().find(|o| o.at == r.at) {
            Some(o) => {
                if r.w > o.w {
                    *o = r;
                }
            }
            None => out.push(r),
        };
        for u in &uses {
            if !policy.site_counts(u.run) {
                continue;
            }
            let f = if u.repeat { 2 } else { 1 };
            let own = Reached {
                at: u.range,
                path: u.path.clone(),
                w: f,
            };
            match &u.sink {
                Sink::Action { kind, .. } if policy.consumer_counts(Consumer::of_action(*kind)) => {
                    push(own)
                }
                Sink::Derived(d) if *d != b => {
                    // A derived frame that is cached itself ends the walk.
                    if self.ch.model.binding(*d).ty.facts.cached {
                        if policy.consumer_counts(Consumer::Cache) {
                            push(own);
                        }
                    } else {
                        for r in self.reached(*d).iter() {
                            push(Reached {
                                w: r.w.saturating_mul(f),
                                ..r.clone()
                            });
                        }
                    }
                }
                Sink::Cache { .. } if policy.consumer_counts(Consumer::Cache) => push(own),
                _ => {}
            }
        }
        self.visiting.remove(&b);
        let out = std::rc::Rc::new(out);
        self.memo.insert(b, out.clone());
        out
    }
}

pub fn finish_recomputed_lineage(ch: &mut Checker) {
    let mut counter = Counter::new(ch, CountPolicy::RECOMPUTE);
    let mut cands: Vec<BindingId> = counter.ch.state.flow.tracked.iter().copied().collect();
    cands.sort_unstable();
    let mut out: Vec<(TextRange, String)> = Vec::new();
    for b in cands {
        let binding = counter.ch.model.binding(b);
        if !matches!(
            binding.kind,
            BindingKind::Assign | BindingKind::AnnAssign | BindingKind::Walrus
        ) {
            continue;
        }
        let t = binding.ty;
        if t.facts.cached || t.facts.streaming || t.bounded().is_some() {
            continue;
        }
        let Some(value) = binding.value else { continue };
        let n = counter.count(b);
        if n < 2 {
            continue;
        }
        // If one derived DataFrame carries all the recomputation, report that one.
        let uses = counter
            .ch
            .state
            .flow
            .uses
            .get(&b)
            .cloned()
            .unwrap_or_default();
        let child_carries = uses.iter().any(|u| match &u.sink {
            Sink::Derived(d) if *d != b && !counter.ch.model.binding(*d).ty.facts.cached => {
                counter.count(*d) >= n
            }
            _ => false,
        });
        if child_carries {
            continue;
        }
        if !lineage_expensive(counter.ch, value, 0) {
            continue;
        }
        let name = binding.name.clone();
        out.push((
            binding.range,
            format!(
                "`{name}` feeds {n} actions/writes without cache — its full lineage (joins/aggregations) is recomputed each time."
            ),
        ));
    }
    for (range, msg) in out {
        ch.report(Diagnostic::new("PRF006", msg, range).with_help(
            "Persist it once (`x = x.persist()` … `x.unpersist()`), or restructure so one job produces all outputs.",
        ));
    }
}

// ------------------------------------------------------------------ PRF009

pub fn note_clear_cache(ch: &mut Checker, _call: &ast::ExprCall, expr: &Expr) {
    if let Some(sc) = ch.model.spark_call_named(expr, &["clearCache"])
        && matches!(sc.class(), Ty::Catalog | Ty::SparkSession | Ty::SqlContext)
    {
        ch.state.clear_cache_seen = true;
    }
}

pub fn finish_cache_never_unpersisted(ch: &mut Checker) {
    if ch.state.clear_cache_seen {
        return;
    }
    let model = ch.model;
    let mut out: Vec<(TextRange, String, String)> = Vec::new();
    for (i, b) in model.bindings().iter().enumerate() {
        let id = i as BindingId;
        let Some(v) = b.value else { continue };
        let Some(sc) = model.spark_call_named(v, &["cache", "persist"]) else {
            continue;
        };
        if !sc.is_dataframe() {
            continue;
        }
        let (recv, m) = (sc.receiver, sc.method);
        match b.kind {
            BindingKind::Assign | BindingKind::AnnAssign | BindingKind::Walrus => {}
            BindingKind::CacheMarker => {}
            _ => continue,
        }
        if model.function_scope_at(b.stmt_range.start()) == MODULE_SCOPE && !ch.is_notebook() {
            continue; // a job's session ends with the job
        }
        let (unpersisted, escapes) = cache_fate(ch, id, recv, b.stmt_range.start());
        if unpersisted || escapes {
            continue;
        }
        out.push((method_range(v), b.name.clone(), m.to_string()));
    }
    out.sort_by_key(|(r, _, _)| r.start());
    out.dedup_by_key(|(r, _, _)| *r);
    for (range, name, m) in out {
        ch.report(
            Diagnostic::new(
                "PRF009",
                format!("`{name}` is .{m}()-ed but never unpersisted in this scope — the blocks hold executor memory until the session ends."),
                range,
            )
            .with_help("Call .unpersist() when done (try/finally), or let the job end release it."),
        );
    }
}

/// Follows a cached binding `id` (`x = recv.cache()` or a bare `recv.cache()` marker),
/// its aliases and every DataFrame derived from it, after `after`. Returns
/// `(unpersisted, escapes)`: an alias is `.unpersist()`-ed / the cache or a derived
/// frame is returned, yielded, stored or passed to an unknown call (its consumers
/// are out of sight). Shared by PRF008 and PRF009.
pub(crate) fn cache_fate(
    ch: &Checker,
    id: BindingId,
    recv: &Expr,
    after: ruff_text_size::TextSize,
) -> (bool, bool) {
    let model = ch.model;
    // Aliases: the binding itself, the receiver of `x = df.cache()` (same
    // Dataset object), and plain-name copies.
    let mut aliases: Vec<BindingId> = vec![id];
    if let Expr::Name(rn) = recv {
        let reaching = model.reaching(rn);
        // A bare `df.persist()` statement on a parameter caches the caller's
        // DataFrame object for the caller: its uses are out of sight.
        if model.binding(id).kind == BindingKind::CacheMarker
            && reaching
                .iter()
                .any(|b| model.binding(*b).kind == BindingKind::Param)
        {
            return (false, true);
        }
        aliases.extend(reaching.iter().copied());
    }
    // Walk aliases (unpersist counts) and every DataFrame derived from them
    // (a derived frame that is returned or stored keeps needing the cache).
    let mut seen: Vec<(BindingId, bool)> = aliases.iter().map(|a| (*a, true)).collect();
    let mut k = 0;
    let mut unpersisted = false;
    let mut escapes = false;
    while k < seen.len() {
        let (a, is_alias) = seen[k];
        k += 1;
        for u in ch.state.flow.uses.get(&a).map(Vec::as_slice).unwrap_or(&[]) {
            if u.range.start() < after && !u.repeat {
                continue; // before the cache
            }
            if u.other_scope && !matches!(u.sink, Sink::Unpersist) {
                // Captured by a lambda / nested def: the closure may outlive this
                // scope and run any number of times.
                escapes = true;
                continue;
            }
            match &u.sink {
                Sink::Unpersist if is_alias => unpersisted = true,
                Sink::Escape => escapes = true,
                Sink::Derived(d) | Sink::Cache { derived: Some(d) }
                    if !seen.iter().any(|(x, _)| x == d) =>
                {
                    let plain = matches!(model.binding(*d).value, Some(Expr::Name(_)));
                    seen.push((*d, is_alias && plain));
                }
                _ => {}
            }
        }
    }
    (unpersisted, escapes)
}

// ------------------------------------------------------------------ PRF008

/// A cached DataFrame binding (PRF008).
#[derive(Debug, Clone)]
pub struct CacheCandidate {
    pub report_range: TextRange,
    pub name: String,
    pub method: String,
    pub weight: u32,
    /// Range to delete to drop the `.cache()` suffix (unsafe fix).
    pub delete_range: Option<TextRange>,
}

const NON_CONSUMING: &[&str] = &[
    "unpersist",
    "is_cached",
    "storageLevel",
    "columns",
    "schema",
    "dtypes",
    "printSchema",
    "explain",
    "cache",
    "persist",
    "isStreaming",
];

pub fn init_cache_candidates(ch: &mut Checker) {
    let model = ch.model;
    for (i, b) in model.bindings().iter().enumerate() {
        let Some(v) = b.value else { continue };
        let Some(sc) = model.spark_call_named(v, &["cache", "persist"]) else {
            continue;
        };
        if !sc.is_dataframe() {
            continue;
        }
        let (recv, m) = (sc.receiver, sc.method);
        let delete_range = match b.kind {
            BindingKind::Assign | BindingKind::AnnAssign | BindingKind::Walrus => {
                Some(TextRange::new(recv.range().end(), v.range().end()))
            }
            BindingKind::CacheMarker => None,
            _ => continue,
        };
        ch.state.cache.insert(
            i as u32,
            CacheCandidate {
                report_range: method_range(v),
                name: b.name.clone(),
                method: m.to_string(),
                weight: 0,
                delete_range,
            },
        );
    }
}

pub fn cache_use(ch: &mut Checker, n: &ast::ExprName) {
    if n.ctx != ExprContext::Load {
        return;
    }
    let reaching: Vec<u32> = ch
        .model
        .reaching(n)
        .iter()
        .copied()
        .filter(|b| ch.state.cache.contains_key(b))
        .collect();
    if reaching.is_empty() {
        return;
    }
    // Same policy as the derived-frame count: a use in an `assert` message, a
    // `raise` or a `pytest.raises` body never completes on the passing path.
    if !CountPolicy::CACHE_REUSE.site_counts(ch.run_at(n.range)) {
        return;
    }
    let r = n.range;
    let weight = match ch.parent() {
        Some(Node::Expr(Expr::Attribute(a))) if a.value.range() == r => {
            let attr = a.attr.as_str();
            if NON_CONSUMING.contains(&attr) {
                0
            } else if attr.contains("TempView") || attr == "registerTempTable" {
                2
            } else {
                1
            }
        }
        Some(Node::Stmt(Stmt::Return(_))) => 2,
        Some(Node::Expr(Expr::Yield(_) | Expr::YieldFrom(_) | Expr::Lambda(_))) => 2,
        Some(Node::Stmt(Stmt::Assign(_) | Stmt::AnnAssign(_))) => 2,
        Some(Node::Expr(Expr::Call(c))) if c.func.range() != r => 2,
        Some(Node::Expr(Expr::Starred(_) | Expr::List(_) | Expr::Tuple(_) | Expr::Dict(_))) => 2,
        _ => 1,
    };
    for b in reaching {
        let binding = ch.model.binding(b);
        let mut w = weight;
        if w > 0 {
            let repeated = ch
                .model
                .in_loop_not_containing(r.start(), binding.stmt_range.start());
            let other_fn = ch.model.function_scope_at(r.start())
                != ch.model.function_scope_at(binding.stmt_range.start());
            if repeated || other_fn {
                w = 2;
            }
        }
        if let Some(c) = ch.state.cache.get_mut(&b) {
            c.weight += w;
        }
    }
}

pub fn finish_cache(ch: &mut Checker) {
    let model = ch.model;
    let mut cands: Vec<CacheCandidate> = ch
        .state
        .cache
        .iter()
        .filter(|(_, c)| c.weight <= 1)
        .filter(|(id, _)| {
            // A cache whose frame (or a frame derived from it) is returned, yielded or
            // stored is consumed by code we cannot see: no single-use claim.
            let b = model.binding(**id);
            let Some((_, recv, _)) = b.value.and_then(method_call) else {
                return true;
            };
            // Read once, by a derived frame that feeds two actions
            // (`s = df.agg(..); s.show(); s.collect()`): the cache is reused.
            if Counter::new(ch, CountPolicy::CACHE_REUSE).count(**id) >= 2 {
                return false;
            }
            !cache_fate(ch, **id, recv, b.stmt_range.start()).1
        })
        .map(|(_, c)| c.clone())
        .collect();
    cands.sort_by_key(|c| c.report_range.start());
    for c in cands {
        let how = if c.weight == 0 {
            "never consumed"
        } else {
            "consumed only once"
        };
        let mut d = Diagnostic::new(
            "PRF008",
            format!(
                "`{}` is cached but {how} — .{}() costs a materialisation and memory with no reuse.",
                c.name, c.method
            ),
            c.report_range,
        );
        if let Some(r) = c.delete_range {
            d = d.with_fix(Fix::unsafe_(
                format!("Remove .{}()", c.method),
                vec![Edit::delete(r)],
            ));
        }
        ch.report(d);
    }
}

#[cfg(test)]
mod tests {
    use super::{CountPolicy, Counter};
    use crate::checker::testing::with_walked_checker;
    use sparklint_semantic::BindingKind;

    const PRELUDE: &str = "import pytest\nfrom pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\n";

    /// (PRF006 recompute count, PRF008 cache-reuse count) of the first assignment
    /// of `x` in `body`.
    fn counts(body: &str) -> (u32, u32) {
        let src = format!("{PRELUDE}{body}");
        with_walked_checker(&src, &["PRF006", "PRF008"], |ch| {
            let (b, _) = ch
                .model
                .bindings()
                .iter()
                .enumerate()
                .find(|(_, b)| b.name == "x" && b.kind == BindingKind::Assign)
                .unwrap_or_else(|| panic!("no `x = ..` in {body}"));
            let b = b as u32;
            (
                Counter::new(ch, CountPolicy::RECOMPUTE).count(b),
                Counter::new(ch, CountPolicy::CACHE_REUSE).count(b),
            )
        })
    }

    #[test]
    fn counter_table() {
        let cases: &[(&str, (u32, u32))] = &[
            ("x = spark.table('t')\nx.count()\nx.collect()\n", (2, 2)),
            ("x = spark.table('t')\nx.write.saveAsTable('o')\n", (1, 1)),
            // show()/display() count for cache reuse only
            ("x = spark.table('t')\nx.show()\nx.count()\n", (1, 2)),
            ("x = spark.table('t')\ndisplay(x)\nx.count()\n", (1, 2)),
            // metadata is not a consumer
            (
                "x = spark.table('t')\nx.printSchema()\nx.columns\nx.count()\n",
                (1, 1),
            ),
            // exclusive branches: one path
            (
                "x = spark.table('t')\nif c:\n    x.count()\nelse:\n    x.collect()\n",
                (1, 1),
            ),
            // derived frames carry their actions back
            (
                "x = spark.table('t')\ny = x.filter('a')\ny.count()\ny.collect()\n",
                (2, 2),
            ),
            ("x = spark.table('t')\ny = x.union(x)\ny.count()\n", (1, 1)),
            // rebound on one branch: the later action runs once
            (
                "x = spark.table('t')\nif c:\n    x = x.filter('a')\nx.count()\n",
                (1, 1),
            ),
            // a loop not containing the binding repeats the action
            (
                "x = spark.table('t')\nfor t in ts:\n    x.count()\n",
                (2, 2),
            ),
            // a cache ends the walk and counts once
            (
                "x = spark.table('t')\ny = x.cache()\ny.count()\ny.collect()\n",
                (1, 1),
            ),
            (
                "x = spark.table('t')\ny = x.filter('a').cache()\ny.count()\nx.count()\n",
                (2, 2),
            ),
            // sites that never complete on the passing path
            (
                "x = spark.table('t')\nx.count()\nassert ok, x.collect()\n",
                (1, 1),
            ),
            (
                "x = spark.table('t')\nx.count()\nif bad:\n    raise ValueError(x.count())\n",
                (1, 1),
            ),
            (
                "x = spark.table('t')\nx.count()\nwith pytest.raises(ValueError):\n    x.collect()\n",
                (1, 1),
            ),
            // ... but a try probe and a warns block run to the end
            (
                "x = spark.table('t')\ntry:\n    x.count()\nexcept Exception:\n    pass\nx.collect()\n",
                (2, 2),
            ),
            (
                "x = spark.table('t')\nx.count()\nwith pytest.warns(UserWarning):\n    x.collect()\n",
                (2, 2),
            ),
            // the assert condition itself runs
            (
                "x = spark.table('t')\nassert x.count() == x.distinct().count()\n",
                (2, 2),
            ),
        ];
        for (body, want) in cases {
            assert_eq!(counts(body), *want, "{body}");
        }
    }
}
