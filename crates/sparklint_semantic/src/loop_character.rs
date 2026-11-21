//! What kind of loop is this? One answer for every rule that judges work done per
//! iteration (ACT005, IOD006 and the whole-project loop rules).
//!
//! [`crate::loop_exit`] answers "does this loop repeat the code?"
//! ([`crate::SemanticModel::repeating_loops_at`]). This module answers the rest,
//! for one loop ([`LoopCharacter`], from [`crate::SemanticModel::loop_character`]):
//!
//! - [`LoopCharacter::polling`]: a `while` loop waiting for something outside the
//!   program (it sleeps, or re-asks the same question in its condition);
//! - [`LoopCharacter::iterable`]: what the loop iterates over ([`IterKind`], the
//!   item count of a literal / constant iterable, date intervals), with
//!   [`Iterable::small`] the "few literal items" test;
//! - [`LoopCharacter::varies`] / [`LoopCharacter::source_varies`]: whether an
//!   expression (the frame an action runs on) changes from one iteration to the
//!   next: orchestration over many sources rather than the N+1 pattern;
//! - [`LoopCharacter::carried`]: whether a name read in the body may hold the
//!   previous iteration's value.
//!
//! [`SemanticModel::depends_on`] answers the general question behind `varies`:
//! which loops and parameters an expression is computed from (constant = neither).

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{Expr, Stmt};
use ruff_text_size::{Ranged, TextRange, TextSize};

use crate::call_run::{Returns, RunKind, run_of_ty};
use crate::model::{BindingId, BindingKind, LoopInfo, LoopKind, SemanticModel};
use crate::types::Ty;
use crate::values::{Hops, Reached};

/// The AST of a loop, recorded by the model builder.
#[derive(Debug, Clone)]
pub struct LoopNode<'a> {
    /// The iterable (`for` loops, the first comprehension generator).
    pub(crate) iter: Option<&'a Expr>,
    /// Loop targets: the `for` target, every comprehension generator's target.
    pub(crate) targets: Vec<&'a Expr>,
    /// The `while` condition.
    pub(crate) test: Option<&'a Expr>,
    /// The loop body (empty for comprehensions).
    pub(crate) body: &'a [Stmt],
}

/// What a loop iterates over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IterKind {
    /// A literal list/tuple/set/dict, or a name bound only to one.
    Literal,
    /// `range(n)`.
    Range,
    /// Generated from bounds (`date_range`, a comprehension over `range`).
    Generated,
    /// Collected from Spark (`collect()`, `toLocalIterator()`, `toPandas()`).
    Collected,
    /// Anything else (a call result, a parameter, `while`).
    Other,
}

impl IterKind {
    pub fn as_str(self) -> &'static str {
        match self {
            IterKind::Literal => "literal",
            IterKind::Range => "range",
            IterKind::Generated => "generated",
            IterKind::Collected => "collected",
            IterKind::Other => "other",
        }
    }

    /// Rank when combining alternatives (worst wins).
    fn rank(self) -> u8 {
        match self {
            IterKind::Literal => 0,
            IterKind::Other => 1,
            IterKind::Range | IterKind::Generated => 2,
            IterKind::Collected => 3,
        }
    }
}

/// A loop's iterable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Iterable {
    pub kind: IterKind,
    /// Items of a literal or constant iterable (module constants, starred literal
    /// parts, `zip` / `enumerate` / `sorted` of literals followed).
    pub len: Option<usize>,
    /// The loop walks date/time intervals: `for start, end in …` or an iterable
    /// named like `intervals` / `periods` / `windows`.
    pub interval: bool,
}

impl Iterable {
    /// A literal / constant iterable of at most `max` items (the
    /// `driver-loop-small-max` setting): a few iterations, not N.
    pub fn small(&self, max: usize) -> bool {
        self.kind == IterKind::Literal && self.len.is_some_and(|n| n <= max)
    }

    /// `range(n)`, generated or collected: the count is data, not code.
    pub fn unbounded(&self) -> bool {
        matches!(
            self.kind,
            IterKind::Range | IterKind::Generated | IterKind::Collected
        )
    }
}

/// What an expression is computed from ([`SemanticModel::depends_on`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Depends {
    /// The loops (by [`LoopInfo::range`]) whose iterations can change the value, in
    /// walk order: the loop of a loop / comprehension variable it reads (and what
    /// that loop's iterable depends on), every loop whose body (re)binds a name it
    /// reads.
    pub loops: Vec<TextRange>,
    /// The parameters it is computed from, in walk order.
    pub params: Vec<BindingId>,
}

impl Depends {
    /// Neither a loop nor a parameter changes the value: literals, module
    /// constants, names bound once outside loops (call results included).
    pub fn constant(&self) -> bool {
        self.loops.is_empty() && self.params.is_empty()
    }

    /// Whether the iterations of loop `l` can change the value.
    pub fn on_loop(&self, l: &LoopInfo) -> bool {
        self.loops.contains(&l.range)
    }

    fn add_loop(&mut self, r: TextRange) -> bool {
        let new = !self.loops.contains(&r);
        if new {
            self.loops.push(r);
        }
        new
    }
}

/// One loop of the model, with the questions rules ask about it.
#[derive(Clone, Copy)]
pub struct LoopCharacter<'m, 'a> {
    model: &'m SemanticModel<'a>,
    info: &'m LoopInfo,
    node: Option<&'m LoopNode<'a>>,
}

impl<'a> SemanticModel<'a> {
    /// The character of loop `l` (one of [`Self::loops`] / [`Self::loops_at`]).
    pub fn loop_character<'m>(&'m self, l: &'m LoopInfo) -> LoopCharacter<'m, 'a> {
        LoopCharacter {
            model: self,
            info: l,
            node: self.loop_nodes.get(&l.range),
        }
    }

    /// The loop that repeats the code at `offset`: the innermost of
    /// [`Self::repeating_loops_at`]. Per-iteration rules judge this loop only.
    pub fn repeating_loop_at(&self, offset: TextSize) -> Option<LoopCharacter<'_, 'a>> {
        self.repeating_loops_at(offset)
            .last()
            .map(|l| self.loop_character(l))
    }

    /// What `e` is computed from: which loops' iterations and which parameters can
    /// change it. Every name in `e` (not inside lambdas) is followed through every
    /// reaching binding with the [`Hops`] policy of [`crate::values`]:
    ///
    /// - a loop / comprehension variable depends on its loop and on whatever the
    ///   loop's iterable depends on (`for t in tables_of(d)` changes with `d`'s loop);
    /// - a parameter is a dependency of its own;
    /// - a held value (`q = f"..{d}"`) is followed while the hop budget lasts;
    /// - a binding made inside a loop body changes with that loop (and every loop
    ///   around it) when its value reads program state (a call, an attribute:
    ///   `d = load()`) or cannot be followed (`n += 1`, `with .. as x`, the budget
    ///   spent); a value computed from constants (`name = "t"`) does not;
    /// - imports and definitions are constant.
    ///
    /// Loops and parameters of every function are reported; callers that judge
    /// one function keep its own.
    pub fn depends_on(&self, e: &Expr) -> Depends {
        let mut out = Depends::default();
        let mut walked = rustc_hash::FxHashMap::default();
        self.depends_into(e, Hops::DEFAULT, &mut out, &mut walked);
        out
    }

    fn depends_into(
        &self,
        e: &Expr,
        h: Hops,
        out: &mut Depends,
        walked: &mut rustc_hash::FxHashMap<BindingId, u32>,
    ) {
        let mut names: Vec<&Expr> = Vec::new();
        walk(e, &mut |x| {
            if matches!(x, Expr::Name(_)) {
                names.push(x);
            }
        });
        for x in names {
            let Expr::Name(n) = x else { continue };
            // Past the budget the bindings still count by kind and place.
            let (reached, next) = self.hop(x, h).unwrap_or_else(|| {
                let r = self.reaching(n).iter();
                (
                    r.map(|&binding| Reached {
                        binding,
                        value: None,
                    })
                    .collect(),
                    h,
                )
            });
            for r in reached {
                let b = self.binding(r.binding);
                // Rebound per iteration of the loops around the binding: their
                // iterations change it unless the value is computed the same way
                // from constants each time (`name = "t"`, `q = f"..{d}"` follows `d`).
                let rebound = |out: &mut Depends| {
                    for l in self.loops_at(b.stmt_range.start()) {
                        out.add_loop(l.range);
                    }
                };
                match b.kind {
                    BindingKind::LoopVar | BindingKind::ComprehensionVar => {
                        if self.loops.iter().any(|l| l.range == b.stmt_range)
                            && out.add_loop(b.stmt_range)
                        {
                            match self.loop_nodes.get(&b.stmt_range).and_then(|l| l.iter) {
                                Some(iter) => {
                                    if reads_state(iter) {
                                        rebound(out);
                                    }
                                    self.depends_into(iter, next, out, walked);
                                }
                                None => rebound(out),
                            }
                        }
                    }
                    BindingKind::Param => {
                        if !out.params.contains(&r.binding) {
                            out.params.push(r.binding);
                        }
                    }
                    BindingKind::Import | BindingKind::FunctionDef | BindingKind::ClassDef => {}
                    _ => match r.value {
                        Some(v) => {
                            if reads_state(v) {
                                rebound(out);
                            }
                            // Each binding's value once per budget (if/else-bound
                            // names reached along many paths).
                            if walked.get(&r.binding).is_none_or(|&w| w < next.left()) {
                                walked.insert(r.binding, next.left());
                                self.depends_into(v, next, out, walked);
                            }
                        }
                        // `x += 1`, `with .. as x`, a cycle, the budget spent.
                        None => rebound(out),
                    },
                }
            }
        }
    }

    /// For a `for`-loop variable binding (`k` of `for k, v in xs:`): the loop's
    /// target and iterable. `None` for any other binding (comprehension variables
    /// included).
    pub fn for_loop_of(&self, b: crate::model::BindingId) -> Option<(&'a Expr, &'a Expr)> {
        let binding = self.binding(b);
        if binding.kind != BindingKind::LoopVar {
            return None;
        }
        let node = self.loop_nodes.get(&binding.stmt_range)?;
        Some((*node.targets.first()?, node.iter?))
    }
}

impl<'m, 'a> LoopCharacter<'m, 'a> {
    pub fn info(&self) -> &'m LoopInfo {
        self.info
    }

    /// Names bound by the loop target(s).
    pub fn targets(&self) -> Vec<&'a str> {
        let mut out = Vec::new();
        for t in self.node.map(|n| &n.targets[..]).unwrap_or_default() {
            walk(t, &mut |e| {
                if let Expr::Name(n) = e {
                    out.push(n.id.as_str());
                }
            });
        }
        out
    }

    /// Whether `e` changes from one iteration of this loop to the next
    /// ([`SemanticModel::depends_on`]): it reads the loop variable, a name rebound
    /// in the body, or a name bound from one of those.
    pub fn varies(&self, e: &Expr) -> bool {
        self.model.depends_on(e).on_loop(self.info)
    }

    /// Whether the name `n`, read at `at` in the body, may hold the previous
    /// iteration's value: a binding of it made in the body at or after `at`
    /// reaches it (`df = df.withColumn(..)`: a loop-carried value).
    pub fn carried(&self, n: &ruff_python_ast::ExprName, at: TextSize) -> bool {
        let body = self.info.body;
        self.model.reaching(n).iter().any(|b| {
            let b = self.model.binding(*b);
            body.contains_range(b.stmt_range) && b.stmt_range.start() >= at
        })
    }

    /// Whether each iteration reads a different source through the frame `e`
    /// (`spark.table(t)` with `t` the loop variable, `helper(t)` or
    /// `helper(table=t)`, `helper(frame)` with a frame that varies, a loop variable
    /// that is itself a DataFrame, a name bound in the body from one of those):
    /// orchestration rather than the N+1 pattern. A loop without targets (`while`)
    /// has no per-iteration item, so its source never varies.
    pub fn source_varies(&self, e: &Expr) -> bool {
        let targets = self.targets();
        !targets.is_empty() && self.source_varies_at(e, &targets, 0)
    }

    fn source_varies_at(&self, mut e: &Expr, targets: &[&str], depth: u32) -> bool {
        let model = self.model;
        loop {
            if let Some((c, recv)) = method_call(e) {
                let rt = model.ty_of(recv).ty;
                let mut args = c
                    .arguments
                    .args
                    .iter()
                    .chain(c.arguments.keywords.iter().map(|k| &k.value));
                // `DeltaTable.forName(spark, t)` / `forPath(spark, p)` reads a table too.
                let delta_by_name = matches!(&*c.func, Expr::Attribute(a)
                    if matches!(a.attr.as_str(), "forName" | "forPath"))
                    && model.ty_of(e).ty == Ty::DeltaTable;
                let reader = delta_by_name
                    || matches!(
                        rt,
                        Ty::SparkSession
                            | Ty::SqlContext
                            | Ty::DataFrameReader
                            | Ty::DataStreamReader
                            | Ty::Catalog
                            | Ty::DeltaTable
                    );
                if reader {
                    // `spark.sql(f"... WHERE d = '{d}'")` over named tables reads the
                    // same tables each time: only a hole in a table name changes the
                    // source.
                    if fixed_sql_tables(model, rt, c) {
                        return false;
                    }
                    if args.any(|a| self.varies(a)) {
                        return true;
                    }
                } else if args.any(|a| {
                    // `base.join(spark.table(t), ..)`: a frame argument read per item
                    model.ty_of(a).ty == Ty::DataFrame && self.source_varies_at(a, targets, depth)
                }) {
                    return true;
                }
                e = recv;
                continue;
            }
            match e {
                Expr::Attribute(a) => e = &a.value,
                Expr::Subscript(s) => e = &s.value,
                Expr::Call(c) => {
                    // helper(t) / helper(f"db.{t}") / helper(frame_of_t) returning a
                    // DataFrame: a frame argument by its source (a fixed table re-read
                    // per item is the same source), anything else by its value.
                    if c.arguments
                        .args
                        .iter()
                        .chain(c.arguments.keywords.iter().map(|k| &k.value))
                        .any(|a| {
                            if model.ty_of(a).ty == Ty::DataFrame {
                                self.source_varies_at(a, targets, depth)
                            } else {
                                self.varies(a) || self.source_varies_at(a, targets, depth)
                            }
                        })
                    {
                        return true;
                    }
                    e = &c.func;
                }
                Expr::Name(n) => {
                    if targets.contains(&n.id.as_str()) {
                        return true;
                    }
                    if depth >= 3 {
                        return false;
                    }
                    let body = self.info.body;
                    let Some((reached, _)) = model.hop(e, Hops::DEFAULT) else {
                        return false;
                    };
                    return reached.iter().any(|r| {
                        body.contains_range(model.binding(r.binding).stmt_range)
                            && r.value
                                .is_some_and(|v| self.source_varies_at(v, targets, depth + 1))
                    });
                }
                _ => return false,
            }
        }
    }

    /// Whether the body of a `while` loop sleeps (`time.sleep(..)`, not counting
    /// nested functions, classes and lambdas).
    pub fn sleeps(&self) -> bool {
        struct V<'c, 'a> {
            model: &'c SemanticModel<'a>,
            found: bool,
        }
        impl<'b> Visitor<'b> for V<'_, '_> {
            fn visit_stmt(&mut self, s: &'b Stmt) {
                if !matches!(s, Stmt::FunctionDef(_) | Stmt::ClassDef(_)) && !self.found {
                    visitor::walk_stmt(self, s);
                }
            }
            fn visit_expr(&mut self, e: &'b Expr) {
                if self.found {
                    return;
                }
                if let Expr::Call(c) = e
                    && self
                        .model
                        .qualified_name(&c.func)
                        .is_some_and(|q| q == "time.sleep")
                {
                    self.found = true;
                    return;
                }
                if !matches!(e, Expr::Lambda(_)) {
                    visitor::walk_expr(self, e);
                }
            }
        }
        if self.info.kind != LoopKind::While {
            return false;
        }
        let Some(node) = self.node else { return false };
        let mut v = V {
            model: self.model,
            found: false,
        };
        for s in node.body {
            v.visit_stmt(s);
        }
        v.found
    }

    /// Whether this is a polling `while` loop for the action `action` on the frame
    /// `recv`: it waits for something outside the program (a table filled by a
    /// stream, a file landing) instead of iterating over data. Either the body
    /// sleeps ([`Self::sleeps`]), or the action sits in the loop condition and its
    /// frame is the same on every iteration (nothing it reads is rebound in the
    /// loop), so only an outside change can end the loop.
    pub fn polling(&self, action: &Expr, recv: &Expr) -> bool {
        if self.info.kind != LoopKind::While {
            return false;
        }
        let Some(test) = self.node.and_then(|n| n.test) else {
            return false;
        };
        (test.range().contains_range(action.range()) && !self.varies(recv)) || self.sleeps()
    }

    /// What the loop iterates over. `text` is the file's source (the model keeps
    /// no text).
    pub fn iterable(&self, text: &str) -> Iterable {
        let node = self.node;
        let (Some(it), Some(target)) = (
            node.and_then(|n| n.iter),
            node.and_then(|n| n.targets.first().copied()),
        ) else {
            return Iterable {
                kind: IterKind::Other,
                len: None,
                interval: false,
            };
        };
        let (kind, len) = Classify { model: self.model }.iter(it, 0);
        // The iterable as shown in messages: its first line, 70 characters.
        let shown: String = slice(text, it.range())
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .chars()
            .take(70)
            .collect::<String>()
            .to_ascii_lowercase();
        let named = ["interval", "period", "window"]
            .iter()
            .any(|w| shown.contains(w));
        Iterable {
            kind,
            len,
            interval: interval_target(target) || named,
        }
    }
}

/// `spark.sql(q)` whose query text is known and names every table it reads
/// without an f-string hole.
fn fixed_sql_tables(model: &SemanticModel, rt: Ty, c: &ruff_python_ast::ExprCall) -> bool {
    let Expr::Attribute(a) = &*c.func else {
        return false;
    };
    if a.attr.as_str() != "sql" || !matches!(rt, Ty::SparkSession | Ty::SqlContext) {
        return false;
    }
    let Some(q) = model.sql_query(c) else {
        return false;
    };
    let tables = q.source_tables();
    !tables.is_empty() && tables.iter().all(|t| !t.contains("{}"))
}

fn slice(text: &str, r: TextRange) -> &str {
    text.get(usize::from(r.start())..usize::from(r.end()))
        .unwrap_or("")
}

/// `recv.method(...)` → (call, receiver).
fn method_call(e: &Expr) -> Option<(&ruff_python_ast::ExprCall, &Expr)> {
    let Expr::Call(c) = e else { return None };
    let Expr::Attribute(a) = &*c.func else {
        return None;
    };
    Some((c, &a.value))
}

/// Visit every expression below `e` (not entering lambdas).
fn walk<'e>(e: &'e Expr, f: &mut dyn FnMut(&'e Expr)) {
    struct W<'f, 'e>(&'f mut dyn FnMut(&'e Expr));
    impl<'e> Visitor<'e> for W<'_, 'e> {
        fn visit_expr(&mut self, e: &'e Expr) {
            (self.0)(e);
            if !matches!(e, Expr::Lambda(_)) {
                visitor::walk_expr(self, e);
            }
        }
    }
    W(f).visit_expr(e);
}

/// Whether evaluating `e` reads program state beyond its names: a call, an
/// attribute, `await` / `yield` (not inside lambdas). Such a value bound in a loop
/// body may differ per iteration even when its names do not.
fn reads_state(e: &Expr) -> bool {
    let mut hit = false;
    walk(e, &mut |x| {
        if matches!(
            x,
            Expr::Call(_)
                | Expr::Attribute(_)
                | Expr::Await(_)
                | Expr::Yield(_)
                | Expr::YieldFrom(_)
        ) {
            hit = true;
        }
    });
    hit
}

/// `start`/`end`-like pairs: `for start, end in …`.
fn interval_target(t: &Expr) -> bool {
    let Expr::Tuple(tp) = t else { return false };
    let names: Vec<String> = tp
        .elts
        .iter()
        .filter_map(|e| match e {
            Expr::Name(n) => Some(n.id.as_str().to_ascii_lowercase()),
            _ => None,
        })
        .collect();
    if names.len() < 2 {
        return false;
    }
    let has = |n: &str, words: &[&str]| n.split('_').any(|p| words.contains(&p));
    let start = [
        "start", "begin", "from", "since", "lo", "low", "min", "first",
    ];
    let end = ["end", "stop", "to", "until", "hi", "high", "max", "last"];
    names.iter().any(|n| has(n, &start)) && names.iter().any(|n| has(n, &end))
}

fn combine(
    a: (IterKind, Option<usize>),
    b: (IterKind, Option<usize>),
) -> (IterKind, Option<usize>) {
    match (a, b) {
        ((IterKind::Literal, x), (IterKind::Literal, y)) => (
            IterKind::Literal,
            match (x, y) {
                (Some(x), Some(y)) => Some(x.max(y)),
                _ => None,
            },
        ),
        _ if a.0.rank() >= b.0.rank() => (a.0, None),
        _ => (b.0, None),
    }
}

struct Classify<'m, 'a> {
    model: &'m SemanticModel<'a>,
}

impl Classify<'_, '_> {
    /// Whether `e` contains a Spark pull of rows to the driver (`collect`,
    /// `toLocalIterator`, `toPandas`, `take`, `tail`, RDD `collectAsMap`, …; not
    /// `first()`'s single row).
    fn collects(&self, e: &Expr) -> bool {
        let mut hit = false;
        walk(e, &mut |x| {
            if !hit
                && let Some(sc) = self.model.spark_call(x)
                && run_of_ty(sc.class(), sc.method).is_some_and(|r| {
                    r.kind == RunKind::DriverPull
                        && matches!(
                            r.returns,
                            Returns::Rows | Returns::RowIter | Returns::Map | Returns::LocalFrame
                        )
                })
            {
                hit = true;
            }
        });
        hit
    }

    /// A literal or a name bound only to literals (with the item count),
    /// `range(n)`, generated, collected from Spark, or something else.
    fn iter(&self, e: &Expr, depth: u32) -> (IterKind, Option<usize>) {
        let model = self.model;
        if depth > 4 {
            return (IterKind::Other, None);
        }
        let elts = |elts: &[Expr]| -> (IterKind, Option<usize>) {
            let mut n = 0usize;
            for x in elts {
                if let Expr::Starred(st) = x {
                    match self.iter(&st.value, depth + 1) {
                        (IterKind::Literal, Some(k)) => n += k,
                        other => return combine(other, (IterKind::Other, None)),
                    }
                } else {
                    n += 1;
                }
            }
            (IterKind::Literal, Some(n))
        };
        match e {
            Expr::List(l) => elts(&l.elts),
            Expr::Tuple(t) => elts(&t.elts),
            Expr::Set(st) => elts(&st.elts),
            Expr::Dict(d) => (IterKind::Literal, Some(d.items.len())),
            Expr::Name(_) => {
                // `depth` bounds the walk; one name hop per call.
                let Some((reached, _)) = model.hop(e, Hops::DEFAULT) else {
                    return (IterKind::Other, None);
                };
                let mut acc: Option<(IterKind, Option<usize>)> = None;
                for r in reached {
                    let c = match r.value {
                        Some(v) => match self.iter(v, depth + 1) {
                            // `rows = []` filled by `rows.append(..)`: the literal is
                            // only the start, its length is data.
                            (IterKind::Literal, _) if model.mutated(r.binding) => {
                                (IterKind::Other, None)
                            }
                            c => c,
                        },
                        None => (IterKind::Other, None),
                    };
                    acc = Some(match acc {
                        None => c,
                        Some(a) => combine(a, c),
                    });
                }
                acc.unwrap_or((IterKind::Other, None))
            }
            Expr::Call(c) => {
                if let Some(q) = model.qualified_name(&c.func) {
                    match q.as_str() {
                        "builtins.range" => return (IterKind::Range, None),
                        "builtins.enumerate" | "builtins.sorted" | "builtins.reversed"
                        | "builtins.list" | "builtins.tuple" | "builtins.set" | "builtins.iter" => {
                            return match c.arguments.args.first() {
                                Some(a) => self.iter(a, depth + 1),
                                None => (IterKind::Other, None),
                            };
                        }
                        "builtins.zip" => {
                            let mut acc = (IterKind::Literal, Some(usize::MAX));
                            for a in &c.arguments.args {
                                let x = self.iter(a, depth + 1);
                                acc = match (acc, x) {
                                    (
                                        (IterKind::Literal, Some(p)),
                                        (IterKind::Literal, Some(q)),
                                    ) => (IterKind::Literal, Some(p.min(q))),
                                    _ => combine(acc, x),
                                };
                            }
                            if acc == (IterKind::Literal, Some(usize::MAX)) {
                                return (IterKind::Other, None);
                            }
                            return acc;
                        }
                        _ if ["date_range", "period_range", "timedelta_range", "rrule"]
                            .iter()
                            .any(|g| q.ends_with(g)) =>
                        {
                            return (IterKind::Generated, None);
                        }
                        _ => {}
                    }
                }
                if let Some((_, recv)) = method_call(e)
                    && let Expr::Attribute(a) = &*c.func
                    && matches!(a.attr.as_str(), "items" | "keys" | "values")
                    && !self.collects(recv)
                {
                    return self.iter(recv, depth + 1);
                }
                if self.collects(e) {
                    (IterKind::Collected, None)
                } else {
                    (IterKind::Other, None)
                }
            }
            Expr::ListComp(_) | Expr::SetComp(_) | Expr::Generator(_) => {
                if self.collects(e) {
                    return (IterKind::Collected, None);
                }
                let g = match e {
                    Expr::ListComp(c) => c.generators.first(),
                    Expr::SetComp(c) => c.generators.first(),
                    Expr::Generator(c) => c.generators.first(),
                    _ => None,
                };
                match g.map(|g| (self.iter(&g.iter, depth + 1), g.ifs.is_empty())) {
                    Some(((IterKind::Literal, n), true)) => (IterKind::Literal, n),
                    _ => (IterKind::Generated, None),
                }
            }
            _ if self.collects(e) => (IterKind::Collected, None),
            _ => (IterKind::Other, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSummaries, SemanticOptions, build_model};
    use ruff_python_ast::ModModule;

    const PRELUDE: &str = "import time\n\
        from pyspark.sql import SparkSession\n\
        import pyspark.sql.functions as F\n\
        spark = SparkSession.builder.getOrCreate()\n\
        df = spark.table(\"t\")\n";

    fn parse(src: &str) -> ModModule {
        ruff_python_parser::parse_module(src)
            .expect("valid test source")
            .into_syntax()
    }

    /// The innermost call whose source is `needle`.
    struct Find<'s, 'a> {
        src: &'s str,
        needle: &'s str,
        found: Option<&'a Expr>,
    }
    impl<'a> Visitor<'a> for Find<'_, 'a> {
        fn visit_expr(&mut self, e: &'a Expr) {
            if matches!(e, Expr::Call(_)) && slice(self.src, e.range()) == self.needle {
                self.found = Some(e);
            }
            visitor::walk_expr(self, e);
        }
    }

    #[derive(Debug, PartialEq)]
    struct Got {
        repeats: bool,
        polling: bool,
        kind: IterKind,
        len: Option<usize>,
        interval: bool,
        small: bool,
        varies: bool,
    }

    /// The character of the loop repeating the call `needle` in PRELUDE + `body`.
    fn character(body: &str, needle: &str) -> Got {
        let src = format!("{PRELUDE}{body}\n");
        let m = parse(&src);
        let model = build_model(
            &m,
            TextSize::of(src.as_str()),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let mut f = Find {
            src: &src,
            needle,
            found: None,
        };
        for s in &m.body {
            f.visit_stmt(s);
        }
        let e = f.found.unwrap_or_else(|| panic!("{needle:?} not found"));
        let recv = method_call(e).map_or(e, |(_, r)| r);
        let Some(l) = model.repeating_loop_at(e.start()) else {
            return Got {
                repeats: false,
                polling: false,
                kind: IterKind::Other,
                len: None,
                interval: false,
                small: false,
                varies: false,
            };
        };
        let it = l.iterable(&src);
        Got {
            repeats: true,
            polling: l.polling(e, recv),
            kind: it.kind,
            len: it.len,
            interval: it.interval,
            small: it.small(10),
            varies: l.source_varies(recv),
        }
    }

    #[test]
    fn character_per_loop_shape() {
        use IterKind::*;
        // (body, call, repeats, polling, kind, len, interval, small(10), source varies)
        type Case = (
            &'static str,
            &'static str,
            bool,
            bool,
            IterKind,
            Option<usize>,
            bool,
            bool,
            bool,
        );
        let cases: &[Case] = &[
            // orchestration: a different table per item
            (
                "for t in tables:\n    spark.table(t).count()",
                "spark.table(t).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // a Delta table named per item is a different source too
            (
                "from delta.tables import DeltaTable\nfor t in tables:\n    DeltaTable.forName(spark, t).vacuum()",
                "DeltaTable.forName(spark, t).vacuum()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            (
                "from delta.tables import DeltaTable\nfor d in days:\n    DeltaTable.forName(spark, 'x').delete(d)",
                "DeltaTable.forName(spark, 'x').delete(d)",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // N+1 over a short literal: same source, a few iterations
            (
                "for t in ['a', 'b']:\n    df.filter(F.col('k') == t).count()",
                "df.filter(F.col('k') == t).count()",
                true,
                false,
                Literal,
                Some(2),
                false,
                true,
                false,
            ),
            // a module constant of literals, starred parts followed
            (
                "A = ['a', 'b']\nB = ('c',)\nfor t in [*A, *B]:\n    df.count()",
                "df.count()",
                true,
                false,
                Literal,
                Some(3),
                false,
                true,
                false,
            ),
            // dict literal through .items()
            (
                "for k, v in {'a': 1}.items():\n    df.count()",
                "df.count()",
                true,
                false,
                Literal,
                Some(1),
                false,
                true,
                false,
            ),
            // 11 literal items: not small
            (
                "for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:\n    df.count()",
                "df.count()",
                true,
                false,
                Literal,
                Some(11),
                false,
                false,
                false,
            ),
            (
                "for i in range(30):\n    df.count()",
                "df.count()",
                true,
                false,
                Range,
                None,
                false,
                false,
                false,
            ),
            (
                "for r in spark.table('k').collect():\n    df.count()",
                "df.count()",
                true,
                false,
                Collected,
                None,
                false,
                false,
                false,
            ),
            // date intervals
            (
                "for start, end in windows:\n    df.count()",
                "df.count()",
                true,
                false,
                Other,
                None,
                true,
                false,
                false,
            ),
            // a frame bound in the body from the loop variable
            (
                "for t in tables:\n    x = spark.table(t)\n    x.count()",
                "x.count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // helper(t) returning a frame
            (
                "for t in tables:\n    load(t).count()",
                "load(t).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // helper(frame) with a frame bound from the loop variable, keyword helper
            (
                "for t in tables:\n    x = spark.table(t)\n    show_counts(x).count()",
                "show_counts(x).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            (
                "for t in tables:\n    load(name=t).count()",
                "load(name=t).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // helper over a fixed frame: the same source each time
            (
                "for k in keys:\n    x = spark.table('t')\n    keep(x, 'k').count()",
                "keep(x, 'k').count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // a predicate hole in SQL over a named table: the same source
            (
                "for m in months:\n    spark.sql(f\"SELECT * FROM sales WHERE m = '{m}'\").collect()",
                "spark.sql(f\"SELECT * FROM sales WHERE m = '{m}'\").collect()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // a hole in the table name: a different source
            (
                "for t in tables:\n    spark.sql(f\"SELECT * FROM db.{t}\").collect()",
                "spark.sql(f\"SELECT * FROM db.{t}\").collect()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // a frame argument read per item (join / union)
            (
                "for t in tables:\n    df.join(spark.table(t), 'k').count()",
                "df.join(spark.table(t), 'k').count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // a column argument named by the loop variable is not a source
            (
                "for c in cols:\n    df.select(c).count()",
                "df.select(c).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // the loop variable is a frame
            (
                "for f in frames:\n    f.count()",
                "f.count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // comprehension
            (
                "[spark.table(t).count() for t in tables]",
                "spark.table(t).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // nested: judged on the inner loop, where the table is fixed
            (
                "for t in tables:\n    for c in cols:\n        spark.table(t).select(c).count()",
                "spark.table(t).select(c).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // polling: the body sleeps
            (
                "while True:\n    df.count()\n    time.sleep(5)",
                "df.count()",
                true,
                true,
                Other,
                None,
                false,
                false,
                false,
            ),
            // polling: the same question in the condition
            (
                "while spark.table('t').count() == 0:\n    pass",
                "spark.table('t').count()",
                true,
                true,
                Other,
                None,
                false,
                false,
                false,
            ),
            // not polling: the condition's frame is rebound in the body
            (
                "while d.count() == 0:\n    d = load()",
                "d.count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // not polling: a sleep in a nested function does not count
            (
                "while more():\n    def f():\n        time.sleep(1)\n    df.count()",
                "df.count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // a list literal filled in place: its length is data
            (
                "xs = []\nfor a in src:\n    xs.append(a)\nfor x in xs:\n    df.filter(F.col('x') == x).count()",
                "df.filter(F.col('x') == x).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // ... or a dict filled by item assignment
            (
                "d = {}\nfor a in src:\n    d[a] = 1\nfor k in d:\n    df.filter(F.col('x') == k).count()",
                "df.filter(F.col('x') == k).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // a literal only read stays a small literal
            (
                "xs = ['a']\nprint(xs[0])\nfor x in xs:\n    df.filter(F.col('x') == x).count()",
                "df.filter(F.col('x') == x).count()",
                true,
                false,
                Literal,
                Some(1),
                false,
                true,
                false,
            ),
            // a helper argument computed from the loop variable in the body
            (
                "for t in tables:\n    name = f'db.{t}'\n    load(name).count()",
                "load(name).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                true,
            ),
            // ... but not one rebound to the same constant each time
            (
                "for t in tables:\n    name = 'db.t'\n    load(name).count()",
                "load(name).count()",
                true,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
            // a retry loop runs the action once: no repeating loop
            (
                "for a in range(3):\n    df.count()\n    break",
                "df.count()",
                false,
                false,
                Other,
                None,
                false,
                false,
                false,
            ),
        ];
        for (body, call, repeats, polling, kind, len, interval, small, varies) in cases {
            let want = Got {
                repeats: *repeats,
                polling: *polling,
                kind: *kind,
                len: *len,
                interval: *interval,
                small: *small,
                varies: *varies,
            };
            assert_eq!(character(body, call), want, "{body}");
        }
    }

    /// The last expression whose source is `needle`.
    struct FindAny<'s, 'a> {
        src: &'s str,
        needle: &'s str,
        found: Option<&'a Expr>,
    }
    impl<'a> Visitor<'a> for FindAny<'_, 'a> {
        fn visit_expr(&mut self, e: &'a Expr) {
            if slice(self.src, e.range()) == self.needle {
                self.found = Some(e);
            }
            visitor::walk_expr(self, e);
        }
    }

    /// `depends_on(needle)` in PRELUDE + `body`: the loops by their first line and
    /// the parameters by name, sorted.
    fn depends(body: &str, needle: &str) -> (Vec<String>, Vec<String>) {
        let src = format!("{PRELUDE}{body}\n");
        let m = parse(&src);
        let model = build_model(
            &m,
            TextSize::of(src.as_str()),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let mut f = FindAny {
            src: &src,
            needle,
            found: None,
        };
        for s in &m.body {
            f.visit_stmt(s);
        }
        let e = f.found.unwrap_or_else(|| panic!("{needle:?} not found"));
        let d = model.depends_on(e);
        let mut loops: Vec<String> = d
            .loops
            .iter()
            .map(|r| {
                slice(&src, *r)
                    .lines()
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string()
            })
            .collect();
        let mut params: Vec<String> = d
            .params
            .iter()
            .map(|b| model.binding(*b).name.clone())
            .collect();
        loops.sort();
        params.sort();
        assert_eq!(
            d.constant(),
            loops.is_empty() && params.is_empty(),
            "{body}"
        );
        (loops, params)
    }

    #[test]
    fn depends_on_per_binding_shape() {
        // (body, expression, loops by first line, parameters)
        let cases: &[(&str, &str, &[&str], &[&str])] = &[
            // the loop variable
            ("for t in ts:\n    use(t)", "use(t)", &["for t in ts:"], &[]),
            // through a name bound from it in the body
            (
                "for d in ds:\n    q = f'd = {d}'\n    use(q)",
                "use(q)",
                &["for d in ds:"],
                &[],
            ),
            // rebound to the same constant each iteration
            (
                "for d in ds:\n    name = 'cat.t'\n    use(name)",
                "use(name)",
                &[],
                &[],
            ),
            // rebound from a call each iteration
            (
                "for d in ds:\n    x = load()\n    use(x)",
                "use(x)",
                &["for d in ds:"],
                &[],
            ),
            // updated in place each iteration
            (
                "n = 0\nfor d in ds:\n    n += 1\n    use(n)",
                "use(n)",
                &["for d in ds:"],
                &[],
            ),
            // a name bound before the loop from constants
            ("X = 3\nfor d in ds:\n    use(X)", "use(X)", &[], &[]),
            // a parameter
            ("def f(day):\n    use(day)", "use(day)", &[], &["day"]),
            // a loop over a parameter: the loop and the parameter
            (
                "def f(days):\n    for d in days:\n        use(d)",
                "use(d)",
                &["for d in days:"],
                &["days"],
            ),
            // if/else-bound: every path counts
            (
                "def f(a):\n    if a:\n        x = 'p'\n    else:\n        x = a\n    use(x)",
                "use(x)",
                &[],
                &["a"],
            ),
            // an inner loop over constants does not change with the outer loop
            (
                "for t in ts:\n    for c in ['a', 'b']:\n        use(c)",
                "use(c)",
                &["for c in ['a', 'b']:"],
                &[],
            ),
            // an inner loop whose iterable is computed from the outer variable does
            (
                "for t in ts:\n    for c in cols_of(t):\n        use(c)",
                "use(c)",
                &["for c in cols_of(t):", "for t in ts:"],
                &[],
            ),
            // a comprehension variable
            (
                "[use(t) for t in ts]",
                "use(t)",
                &["[use(t) for t in ts]"],
                &[],
            ),
            // lambdas are not entered (they run later, with their own arguments)
            (
                "for t in ts:\n    use(lambda: t)",
                "use(lambda: t)",
                &[],
                &[],
            ),
            // past the hop budget a name rebound in the body still counts
            (
                "for d in ds:\n    a = d\n    b = a\n    c = b\n    e = c\n    g = e\n    use(g)",
                "use(g)",
                &["for d in ds:"],
                &[],
            ),
            // imports and definitions are constant
            ("import os\nfor d in ds:\n    use(os)", "use(os)", &[], &[]),
        ];
        for (body, needle, loops, params) in cases {
            let want = (
                loops.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                params.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            );
            assert_eq!(depends(body, needle), want, "{body}");
        }
    }
}
