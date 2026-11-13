//! DataFrame def-use flow shared by PRF006 (recomputed lineage), PRF008 (cache reuse
//! through derived frames), PRF009 (cache never unpersisted) and PRF022 (`SELECT *`
//! flowing into a sink).
//!
//! During the walk, every load of a tracked DataFrame binding is classified by
//! climbing the expression chain it heads (`df.filter(..).write.save(p)`) into a
//! [`Sink`]: an action, a derived DataFrame binding, a cache, an unpersist, an escape
//! (returned / stored / passed to an unknown function) or nothing interesting. Each
//! record carries the use's branch path and a loop weight so rules can count
//! "actions on one execution path".

use ruff_python_ast::{self as ast, Expr, ExprContext, Stmt};
use ruff_text_size::{Ranged, TextRange};
use rustc_hash::{FxHashMap, FxHashSet};
use sparklint_semantic::api::{ActionKind, action_kind};
use sparklint_semantic::{BindingId, BindingKind, BranchStep, Ty};

use super::exec_context::Run;
use crate::checker::{Checker, Node};

/// What happens to a DataFrame value at one use site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sink {
    /// A Spark action / write ends the chain.
    Action {
        kind: ActionKind,
        method: String,
        /// The chain passed through a projection/aggregation first (`select`, `agg`, …).
        projected: bool,
    },
    /// The chain result is assigned to another DataFrame binding.
    Derived(BindingId),
    /// The chain passes through `.cache()`/`.persist()`/`.checkpoint()`.
    Cache {
        derived: Option<BindingId>,
    },
    /// `x.unpersist()`.
    Unpersist,
    /// Returned, yielded, stored on an object/container or passed to an unknown call.
    Escape,
    Other,
}

#[derive(Debug, Clone)]
pub struct UseRecord {
    pub range: TextRange,
    pub sink: Sink,
    pub path: Vec<BranchStep>,
    /// 2 when the use repeats relative to the binding (a loop not containing it).
    pub repeat: bool,
    /// The use is in another function/lambda than the binding.
    pub other_scope: bool,
    /// How the use site runs (an `assert` message / `raise` / `pytest.raises` body
    /// never completes on the passing path; see [`Run::completes`]).
    pub run: Run,
}

#[derive(Debug, Default)]
pub struct FlowState {
    pub tracked: FxHashSet<BindingId>,
    pub uses: FxHashMap<BindingId, Vec<UseRecord>>,
}

/// DataFrame methods whose DataFrame *arguments* flow into the result.
const COMBINING: &[&str] = &[
    "join",
    "crossJoin",
    "union",
    "unionAll",
    "unionByName",
    "intersect",
    "intersectAll",
    "subtract",
    "exceptAll",
    "lateralJoin",
];

pub fn init(ch: &mut Checker) {
    let model = ch.model;
    for (i, b) in model.bindings().iter().enumerate() {
        if !matches!(
            b.kind,
            BindingKind::Assign
                | BindingKind::AnnAssign
                | BindingKind::Walrus
                | BindingKind::CacheMarker
        ) {
            continue;
        }
        if b.ty.is_dataframe() {
            ch.state.flow.tracked.insert(i as BindingId);
        }
    }
}

/// Classify the current `Name` load (the node on top of the parent stack).
pub fn on_name(ch: &mut Checker, n: &ast::ExprName) {
    if n.ctx != ExprContext::Load {
        return;
    }
    let reaching: Vec<BindingId> = ch
        .model
        .reaching(n)
        .iter()
        .copied()
        .filter(|b| ch.state.flow.tracked.contains(b))
        .collect();
    if reaching.is_empty() {
        return;
    }
    // A bare `df.cache()` statement re-binds `df` through a CacheMarker: the
    // receiver use itself is the cache sink of the earlier binding.
    let sink = classify(ch, n.range);
    let path = ch.model.branch_path(n.range.start());
    let run = ch.run_at(n.range);
    for b in reaching {
        let binding = ch.model.binding(b);
        let start = binding.stmt_range.start();
        let repeat = ch.model.in_loop_not_containing(n.range.start(), start);
        let other_scope =
            ch.model.function_scope_at(n.range.start()) != ch.model.function_scope_at(start);
        ch.state.flow.uses.entry(b).or_default().push(UseRecord {
            range: n.range,
            sink: sink.clone(),
            path: path.clone(),
            repeat,
            other_scope,
            run,
        });
    }
}

/// Classify the chain headed by the node currently being visited.
pub fn classify_current(ch: &Checker) -> Sink {
    match ch.parents().last() {
        Some(n) => classify(ch, n.range()),
        None => Sink::Other,
    }
}

/// Climb the chain headed by the current node and classify where it ends.
fn classify(ch: &Checker, start: TextRange) -> Sink {
    let mut cur = start;
    let mut cached = false;
    let mut projected = false;
    let mut i = 0;
    loop {
        let Some(anc) = ch.ancestor(i) else {
            return Sink::Other;
        };
        match anc {
            Node::Expr(Expr::Attribute(a)) if a.value.range() == cur => {
                cur = a.range;
                i += 1;
            }
            Node::Expr(Expr::Call(c)) if c.func.range() == cur => {
                if let Expr::Attribute(a) = &*c.func {
                    let m = a.attr.as_str();
                    let rt = ch.model.ty_of(&a.value);
                    if matches!(
                        m,
                        "select" | "selectExpr" | "agg" | "groupBy" | "groupby" | "count"
                    ) && matches!(rt.ty, Ty::DataFrame | Ty::GroupedData)
                        && action_kind(rt.ty, m).is_none()
                    {
                        projected = true;
                    }
                    match m {
                        "unpersist" if rt.is_dataframe() => return Sink::Unpersist,
                        "cache" | "persist" | "checkpoint" | "localCheckpoint"
                            if rt.is_dataframe() =>
                        {
                            cached = true;
                        }
                        _ => {
                            if let Some(kind) = action_kind(rt.ty, m) {
                                if kind == ActionKind::Checkpoint {
                                    cached = true;
                                } else {
                                    return Sink::Action {
                                        kind,
                                        method: m.to_string(),
                                        projected,
                                    };
                                }
                            }
                        }
                    }
                }
                cur = c.range();
                i += 1;
            }
            Node::Expr(Expr::Call(c)) if c.arguments.range.contains_range(cur) => {
                // An argument: a combining DataFrame method keeps the flow going.
                if let Expr::Attribute(a) = &*c.func
                    && COMBINING.contains(&a.attr.as_str())
                    && ch.model.is_dataframe(&a.value)
                    && c.arguments.args.iter().any(|x| x.range() == cur)
                {
                    cur = c.range();
                    i += 1;
                    continue;
                }
                if ch.model.qualified_name(&c.func).as_deref() == Some("databricks.display") {
                    return Sink::Action {
                        kind: ActionKind::Display,
                        method: "display".into(),
                        projected,
                    };
                }
                if is_known_pure_call(ch, c) {
                    return Sink::Other;
                }
                return Sink::Escape;
            }
            Node::Expr(Expr::Named(nm)) if nm.value.range() == cur => {
                let b = target_binding(ch, &nm.target);
                return wrap(cached, b);
            }
            Node::Expr(
                Expr::List(_) | Expr::Tuple(_) | Expr::Set(_) | Expr::Dict(_) | Expr::Starred(_),
            ) => return Sink::Escape,
            Node::Expr(Expr::Yield(_) | Expr::YieldFrom(_) | Expr::Lambda(_)) => {
                return Sink::Escape;
            }
            Node::Expr(Expr::If(x)) if x.test.range() != cur => {
                // `a if c else b`: the value flows on.
                cur = x.range;
                i += 1;
            }
            Node::Stmt(Stmt::Return(_)) => return Sink::Escape,
            Node::Stmt(Stmt::Assign(a)) if a.value.range() == cur => {
                if let [t] = a.targets.as_slice() {
                    if let Expr::Name(_) = t {
                        return wrap(cached, target_binding(ch, t));
                    }
                    return Sink::Escape; // self.x = df, d[k] = df, a, b = ...
                }
                return Sink::Escape;
            }
            Node::Stmt(Stmt::AnnAssign(a))
                if a.value.as_ref().is_some_and(|v| v.range() == cur) =>
            {
                if let Expr::Name(_) = &*a.target {
                    return wrap(cached, target_binding(ch, &a.target));
                }
                return Sink::Escape;
            }
            Node::Stmt(Stmt::Expr(_)) if cached => return Sink::Cache { derived: None },
            _ => {
                return if cached {
                    Sink::Cache { derived: None }
                } else {
                    Sink::Other
                };
            }
        }
    }
}

fn wrap(cached: bool, b: Option<BindingId>) -> Sink {
    match (cached, b) {
        (true, d) => Sink::Cache { derived: d },
        (false, Some(b)) => Sink::Derived(b),
        (false, None) => Sink::Other,
    }
}

fn target_binding(ch: &Checker, t: &Expr) -> Option<BindingId> {
    let Expr::Name(n) = t else { return None };
    ch.model.binding_at(n.range)
}

/// Calls that only inspect a DataFrame (no escape, no action).
fn is_known_pure_call(ch: &Checker, c: &ast::ExprCall) -> bool {
    if let Some(q) = ch.model.qualified_name(&c.func) {
        if matches!(
            q.as_str(),
            "builtins.isinstance"
                | "builtins.type"
                | "builtins.id"
                | "builtins.repr"
                | "builtins.str"
                | "builtins.print"
                | "builtins.hasattr"
                | "builtins.getattr"
        ) || q.starts_with("logging.")
        {
            return true;
        }
        // F.broadcast(df) used as an argument of a join keeps flowing (handled by
        // the caller only for direct args); treat Spark functions as pure.
        if q.starts_with("pyspark.sql.functions.") {
            return true;
        }
    }
    if let Expr::Attribute(a) = &*c.func {
        let rt = ch.model.ty_of(&a.value);
        if rt.ty == Ty::Logger {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::Sink;
    use crate::checker::testing::with_walked_checker;
    use sparklint_semantic::api::ActionKind;

    const PRELUDE: &str = "from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\nx = spark.table('t')\n";

    /// The sink of each use of `x` in `body`, in source order, as text.
    fn sinks(body: &str) -> Vec<String> {
        let src = format!("{PRELUDE}{body}");
        with_walked_checker(&src, &["PRF006"], |ch| {
            let model = ch.model;
            let mut uses: Vec<_> = ch
                .state
                .flow
                .uses
                .iter()
                .filter(|(b, _)| model.binding(**b).name == "x")
                .flat_map(|(_, us)| us.iter())
                .collect();
            uses.sort_by_key(|u| u.range.start());
            uses.iter()
                .map(|u| match &u.sink {
                    Sink::Action {
                        kind,
                        method,
                        projected,
                    } => {
                        let k = match kind {
                            ActionKind::Write => "write",
                            ActionKind::Display => "display",
                            ActionKind::DriverCollect => "collect",
                            _ => "job",
                        };
                        format!("{k}:{method}{}", if *projected { "+proj" } else { "" })
                    }
                    Sink::Derived(d) => format!("derived:{}", model.binding(*d).name),
                    Sink::Cache { derived } => format!(
                        "cache:{}",
                        derived.map_or("-".into(), |d| model.binding(d).name.clone())
                    ),
                    Sink::Unpersist => "unpersist".into(),
                    Sink::Escape => "escape".into(),
                    Sink::Other => "other".into(),
                })
                .collect()
        })
    }

    #[test]
    fn sink_table() {
        let cases: &[(&str, &[&str])] = &[
            ("x.count()\n", &["job:count"]),
            (
                "x.filter('a').write.saveAsTable('o')\n",
                &["write:saveAsTable"],
            ),
            ("x.select('a').collect()\n", &["collect:collect+proj"]),
            ("x.show()\n", &["display:show"]),
            ("display(x)\n", &["display:display"]),
            ("y = x.filter('a')\n", &["derived:y"]),
            ("y = x if c else None\n", &["derived:y"]),
            ("y = spark.table('u').join(x, 'k')\n", &["derived:y"]),
            ("y = x.cache()\n", &["cache:y"]),
            ("x.cache()\n", &["cache:-"]),
            ("x.unpersist()\n", &["unpersist"]),
            ("def f():\n    return x\n", &["escape"]),
            ("frames = [x]\n", &["escape"]),
            ("helper(x)\n", &["escape"]),
            ("print(x)\nisinstance(x, object)\n", &["other", "other"]),
            ("x.columns\n", &["other"]),
        ];
        for (body, want) in cases {
            assert_eq!(sinks(body), *want, "{body}");
        }
    }
}
