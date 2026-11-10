//! PRF006 recomputed-lineage, PRF009 cache-never-unpersisted, PRF022 sql-select-star
//! (the flow-based rules; see [`super::flow`]).

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};
use rustc_hash::FxHashSet;
use sparklint_semantic::api::ActionKind;
use sparklint_semantic::{BindingId, BindingKind, MODULE_SCOPE, SemanticModel, Ty};

use super::Rule;
use super::actions::method_range;
use super::flow::{self, Sink};
use super::helpers::*;
use crate::checker::Checker;
use crate::diagnostic::{Diagnostic, Severity};

/// PRF006/009/022 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PRF006").finish(finish_recomputed_lineage),
    Rule::new("PRF009")
        .call(note_clear_cache)
        .finish(finish_cache_never_unpersisted),
    Rule::new("PRF022")
        .call(sql_select_star)
        .finish(finish_select_star),
];

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

fn counts_as_action(kind: ActionKind) -> bool {
    matches!(
        kind,
        ActionKind::Job | ActionKind::DriverCollect | ActionKind::Write
    )
}

struct Counter<'c, 'a> {
    ch: &'c Checker<'a>,
    memo: rustc_hash::FxHashMap<BindingId, u32>,
    visiting: FxHashSet<BindingId>,
}

impl Counter<'_, '_> {
    /// Actions reachable on one execution path from binding `b` (derived
    /// DataFrames included: their actions recompute `b` too).
    fn count(&mut self, b: BindingId) -> u32 {
        if let Some(n) = self.memo.get(&b) {
            return *n;
        }
        if !self.visiting.insert(b) {
            return 0;
        }
        let uses = self.ch.state.flow.uses.get(&b).cloned().unwrap_or_default();
        let mut items = Vec::new();
        for u in &uses {
            let w = match &u.sink {
                Sink::Action { kind, .. } if counts_as_action(*kind) => 1,
                Sink::Derived(d) if *d != b => {
                    let db = self.ch.model.binding(*d);
                    if db.ty.facts.cached {
                        1
                    } else {
                        self.count(*d)
                    }
                }
                Sink::Cache { .. } => 1,
                _ => 0,
            };
            if w == 0 {
                continue;
            }
            let w = if u.repeat { w.max(1) * 2 } else { w };
            items.push((u.path.clone(), w));
        }
        let n = SemanticModel::max_path_weight(&items);
        self.visiting.remove(&b);
        self.memo.insert(b, n);
        n
    }
}

pub fn finish_recomputed_lineage(ch: &mut Checker) {
    let mut counter = Counter {
        ch,
        memo: Default::default(),
        visiting: Default::default(),
    };
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
        aliases.extend(model.reaching(rn).iter().copied());
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

// ------------------------------------------------------------------ PRF022

pub fn sql_select_star(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    if !ch.model.spark_sql(expr).is_some_and(|q| q.select_star()) {
        return;
    }
    match flow::classify_current(ch) {
        Sink::Action {
            kind: ActionKind::Write | ActionKind::DriverCollect,
            projected: false,
            ..
        } => report_select_star(ch, method_range(expr)),
        Sink::Cache { .. } => report_select_star(ch, method_range(expr)),
        Sink::Derived(b) => ch.state.select_star.push((b, method_range(expr))),
        _ => {}
    }
}

fn report_select_star(ch: &mut Checker, range: TextRange) {
    ch.report(
        Diagnostic::new(
            "PRF022",
            "SELECT * in spark.sql() flows into a write/collect/cache — list the columns you need.",
            range,
        )
        .with_severity(Severity::Info),
    );
}

fn reaches_sink(ch: &Checker, b: BindingId, seen: &mut FxHashSet<BindingId>) -> bool {
    if !seen.insert(b) {
        return false;
    }
    for u in ch.state.flow.uses.get(&b).map(Vec::as_slice).unwrap_or(&[]) {
        match &u.sink {
            Sink::Action {
                kind: ActionKind::Write | ActionKind::DriverCollect,
                projected: false,
                ..
            } => return true,
            Sink::Cache { .. } => return true,
            Sink::Derived(d) => {
                // Only pass-through derivations keep `*`: a later select() prunes.
                let pruned = ch
                    .model
                    .binding(*d)
                    .value
                    .and_then(method_call)
                    .is_some_and(|(_, _, m)| {
                        matches!(m, "select" | "selectExpr" | "agg" | "groupBy")
                    });
                if !pruned && reaches_sink(ch, *d, seen) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

pub fn finish_select_star(ch: &mut Checker) {
    let cands = std::mem::take(&mut ch.state.select_star);
    for (b, range) in cands {
        let mut seen = FxHashSet::default();
        if reaches_sink(ch, b, &mut seen) {
            report_select_star(ch, range);
        }
    }
}
