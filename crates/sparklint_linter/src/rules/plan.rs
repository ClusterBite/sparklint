//! Plan-shape rules: PRF004 non-equi-join, PRF012 explode-before-filter,
//! PRF015 collect-dedup-redundant, PRF023 pivot-without-values, PRF024 cube-many-columns,
//! PRF025 not-in-subquery, PRF026 many-distinct-aggregates, PRF027 order-by-rand,
//! PRF032 or-join-condition, PRF035 keyless-grouped-pandas,
//! PRF036 forced-nested-loop-join-hint, PRF022 sql-select-star (`SELECT *` flowing into
//! a write / collect / cache, followed through [`super::flow`]).

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};
use rustc_hash::FxHashSet;
use sparklint_semantic::call_run::RunKind;
use sparklint_semantic::columns::{self, ColumnRefs, column_args, mentions_name};
use sparklint_semantic::transform;
use sparklint_semantic::{
    BindingId, Hops, SparkCall, SqlCond as JoinCond, SqlSite, SqlSiteKind, Ty, str_literal,
};

use super::Rule;
use super::describe::short;
use super::flow::{self, Sink};
use super::helpers::*;
use super::msg::Msg;
use super::plan_facts::{Conj, conjuncts, join_cond, small_side};
use crate::checker::{Checker, Node};
use crate::diagnostic::{Edit, Fix, Severity};
use crate::msg;

/// PRF004/012/015/022/023/024/025/026/027/032/035/036 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PRF004").call(non_equi_join).msgs(&[PRF004]),
    Rule::new("PRF012")
        .call(explode_before_filter)
        .msgs(&[PRF012]),
    Rule::new("PRF015")
        .call(collect_dedup_redundant)
        .msgs(&[PRF015_LIST, PRF015_SET]),
    Rule::new("PRF023")
        .call(pivot_without_values)
        .msgs(&[PRF023, PRF023_LOOP]),
    Rule::new("PRF024").call(cube_many_columns).msgs(&[
        PRF024,
        PRF024_LOOP,
        PRF024_SQL,
        PRF024_SQL_LOOP,
        PRF024_COMBINED,
        PRF024_COMBINED_LOOP,
    ]),
    Rule::new("PRF025")
        .call(not_in_subquery)
        .msgs(&[PRF025, PRF025_LOOP]),
    Rule::new("PRF026")
        .call(many_distinct_aggregates)
        .msgs(&[PRF026, PRF026_LOOP]),
    Rule::new("PRF027").call(order_by_rand).msgs(&[
        PRF027,
        PRF027_LOOP,
        PRF027_SQL,
        PRF027_SQL_LOOP,
    ]),
    Rule::new("PRF032").call(or_join_condition).msgs(&[PRF032]),
    Rule::new("PRF035").call(keyless_grouped_pandas).msgs(&[
        PRF035,
        PRF035_LOOP,
        PRF035_STATEFUL,
        PRF035_STATEFUL_LOOP,
    ]),
    Rule::new("PRF036")
        .call(forced_nested_loop_join_hint)
        .msgs(&[PRF036, PRF036_LOOP]),
    Rule::new("PRF022")
        .call(sql_select_star)
        .finish(finish_select_star)
        .msgs(&[PRF022]),
];

const PRF004: Msg = Msg::new(
    "PRF004",
    "non-equi",
    "Join condition has no equality predicate — it executes as a nested-loop join (O(n·m) comparisons).",
)
.help("Add an equi-key (e.g. a time bucket: floor(ts/3600) on both sides) next to the range predicate, or use a range-join hint on Databricks.");
const PRF032: Msg = Msg::new(
    "PRF032",
    "or-of-equalities",
    "Join condition ORs equalities together — Spark can't use it as a join key, so it runs a nested-loop join (O(n·m) comparisons).",
)
.help("Join once per OR branch on its equality and unionByName the results (dropDuplicates if a row can match both), or explode the candidate keys into one key column and equi-join on it.");
const PRF012: Msg = Msg::new(
    "PRF012",
    "explode-first",
    "explode() runs before a filter that does not use the exploded values — filter first.",
)
.help("Move the filter below the explode: df.filter(cond).withColumn(\"x\", F.explode(...)).");
const PRF015_LIST: Msg = Msg::new(
    "PRF015",
    "collect-list",
    "array_distinct(collect_list(x)) materialises duplicates and then drops them — use collect_set(x).",
);
const PRF015_SET: Msg = Msg::new(
    "PRF015",
    "collect-set",
    "array_distinct(collect_set(x)) is redundant — collect_set already deduplicates.",
);
const PRF023_HELP: &str =
    "Pass the values: .pivot(\"col\", [\"a\", \"b\", ...]) (this also fixes the column order).";
const PRF023: Msg = Msg::new(
    "PRF023",
    "no-values",
    "pivot({column}) without a values list runs an extra Spark job to find the distinct values.",
)
.help(PRF023_HELP);
const PRF023_LOOP: Msg = Msg::new(
    "PRF023",
    "no-values-in-loop",
    "pivot({column}) without a values list runs an extra Spark job to find the distinct values. Inside a loop that job repeats on every iteration.",
)
.help(PRF023_HELP);
const PRF024_HELP: &str = "List only the grouping sets you need: GROUP BY GROUPING SETS ((a, b), (a), ()) in SQL, or df.groupingSets(...) on Spark >= 4.0.";
const PRF024: Msg = Msg::new(
    "PRF024",
    "cube",
    "{op} over {n} columns makes {sets} grouping sets: every input row is copied {sets} times before the shuffle.",
)
.help(PRF024_HELP);
const PRF024_LOOP: Msg = Msg::new(
    "PRF024",
    "cube-in-loop",
    "{op} over {n} columns makes {sets} grouping sets: every input row is copied {sets} times before the shuffle. Inside a loop it runs on every iteration.",
)
.help(PRF024_HELP);
const PRF024_SQL: Msg = Msg::new(
    "PRF024",
    "sql-cube",
    "SQL {op} over {n} columns makes {sets} grouping sets: every input row is copied {sets} times before the shuffle.",
)
.help(PRF024_HELP);
const PRF024_SQL_LOOP: Msg = Msg::new(
    "PRF024",
    "sql-cube-in-loop",
    "SQL {op} over {n} columns makes {sets} grouping sets: every input row is copied {sets} times before the shuffle. Inside a loop it runs on every iteration.",
)
.help(PRF024_HELP);
const PRF024_COMBINED: Msg = Msg::new(
    "PRF024",
    "sql-combined",
    "SQL GROUP BY combines {n} CUBE / ROLLUP / GROUPING SETS items into {sets} grouping sets (the product of each item's sets): every input row is copied {sets} times before the shuffle.",
)
.help(PRF024_HELP);
const PRF024_COMBINED_LOOP: Msg = Msg::new(
    "PRF024",
    "sql-combined-in-loop",
    "SQL GROUP BY combines {n} CUBE / ROLLUP / GROUPING SETS items into {sets} grouping sets (the product of each item's sets): every input row is copied {sets} times before the shuffle. Inside a loop it runs on every iteration.",
)
.help(PRF024_HELP);
const PRF025_HELP: &str = "If the subquery column has no NULLs, write NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k) or use a DataFrame left_anti join: both plan as ordinary hash or sort-merge anti joins.";
const PRF025: Msg = Msg::new(
    "PRF025",
    "not-in-subquery",
    "SQL `NOT IN (SELECT ...)` is planned as a null-aware anti join, usually a broadcast nested-loop join: very slow, or a broadcast failure when the subquery is large.",
)
.help(PRF025_HELP);
const PRF025_LOOP: Msg = Msg::new(
    "PRF025",
    "not-in-subquery-in-loop",
    "SQL `NOT IN (SELECT ...)` is planned as a null-aware anti join, usually a broadcast nested-loop join: very slow, or a broadcast failure when the subquery is large. Inside a loop it runs on every iteration.",
)
.help(PRF025_HELP);
const PRF026_HELP: &str = "Use F.approx_count_distinct where an estimate is enough, or compute each distinct count in its own aggregation over pre-deduplicated rows (df.select(k, c).distinct().groupBy(k).count()) and join the results.";
const PRF026: Msg = Msg::new(
    "PRF026",
    "distinct-aggregates",
    "{n} exact distinct aggregates over different columns in one {what}: Spark copies every input row about {n} times (one copy per distinct column set) before the shuffle.",
)
.help(PRF026_HELP);
const PRF026_LOOP: Msg = Msg::new(
    "PRF026",
    "distinct-aggregates-in-loop",
    "{n} exact distinct aggregates over different columns in one {what}: Spark copies every input row about {n} times (one copy per distinct column set) before the shuffle. Inside a loop that repeats on every iteration.",
)
.help(PRF026_HELP);
const PRF027_HELP: &str = "To pick random rows use df.sample(fraction=p) (then .limit(n) for at most n rows) or df.where(F.rand() < p); for per-group samples use sampleBy, or row_number() over Window.partitionBy(k).orderBy(F.rand()).";
const PRF027: Msg = Msg::new(
    "PRF027",
    "order-by-rand",
    "{method}(rand()) sorts the whole dataset (a sampling job plus a full shuffle) just to randomise the row order.",
)
.help(PRF027_HELP);
const PRF027_LOOP: Msg = Msg::new(
    "PRF027",
    "order-by-rand-in-loop",
    "{method}(rand()) sorts the whole dataset (a sampling job plus a full shuffle) just to randomise the row order. Inside a loop it runs on every iteration.",
)
.help(PRF027_HELP);
const PRF027_SQL_HELP: &str = "Use TABLESAMPLE (p PERCENT) or WHERE rand() < p (then LIMIT n) instead of sorting by a random key.";
const PRF027_SQL: Msg = Msg::new(
    "PRF027",
    "sql-order-by-rand",
    "SQL ORDER BY rand() sorts the whole dataset (a sampling job plus a full shuffle) just to randomise the row order.",
)
.help(PRF027_SQL_HELP);
const PRF027_SQL_LOOP: Msg = Msg::new(
    "PRF027",
    "sql-order-by-rand-in-loop",
    "SQL ORDER BY rand() sorts the whole dataset (a sampling job plus a full shuffle) just to randomise the row order. Inside a loop it runs on every iteration.",
)
.help(PRF027_SQL_HELP);
const PRF035_HELP: &str = "Group by a real key, or use df.mapInPandas(fn, schema) to run the function once per partition in parallel.";
const PRF035: Msg = Msg::new(
    "PRF035",
    "one-group",
    "{grouping} before {method}(): the whole DataFrame is one group, so a single task loads every row into one pandas DataFrame (no parallelism, OOM once it outgrows one executor).",
)
.help(PRF035_HELP);
const PRF035_LOOP: Msg = Msg::new(
    "PRF035",
    "one-group-in-loop",
    "{grouping} before {method}(): the whole DataFrame is one group, so a single task loads every row into one pandas DataFrame (no parallelism, OOM once it outgrows one executor). Inside a loop that repeats on every iteration.",
)
.help(PRF035_HELP);
const PRF035_STATEFUL: Msg = Msg::new(
    "PRF035",
    "one-group-stateful",
    "{grouping} before {method}(): the whole stream is one group, so every micro-batch runs in a single task against one global state (no parallelism).",
)
.help(PRF035_HELP);
const PRF035_STATEFUL_LOOP: Msg = Msg::new(
    "PRF035",
    "one-group-stateful-in-loop",
    "{grouping} before {method}(): the whole stream is one group, so every micro-batch runs in a single task against one global state (no parallelism). Inside a loop that repeats on every iteration.",
)
.help(PRF035_HELP);
const PRF036_HELP: &str = "Drop the hint and let Spark pick the join (or use BROADCAST / MERGE / SHUFFLE_HASH for an equi-join); keep it only for a theta join of two small inputs, with a comment saying so.";
const PRF036: Msg = Msg::new(
    "PRF036",
    "nested-loop-hint",
    "{hint} forces a shuffle-and-replicate nested-loop join: every row of one side is compared with every row of the other (a cartesian product filtered afterwards), even when the join has an equality key a hash or sort-merge join could use.",
)
.help(PRF036_HELP);
const PRF036_LOOP: Msg = Msg::new(
    "PRF036",
    "nested-loop-hint-in-loop",
    "{hint} forces a shuffle-and-replicate nested-loop join: every row of one side is compared with every row of the other (a cartesian product filtered afterwards), even when the join has an equality key a hash or sort-merge join could use. Inside a loop it runs on every iteration.",
)
.help(PRF036_HELP);
const PRF022: Msg = Msg::new(
    "PRF022",
    "select-star",
    "SELECT * in spark.sql() flows into a write/collect/cache — list the columns you need.",
)
.help("Name the columns the write, cache or collect needs; a select() of only some columns before it also clears this.");

/// `a` or its in-a-loop twin `b`, as `expr` repeats in a loop.
fn loop_variant(ch: &Checker, expr: &Expr, a: &'static Msg, b: &'static Msg) -> &'static Msg {
    if ch.model.in_repeating_loop(expr) {
        b
    } else {
        a
    }
}

// ------------------------------------------------------------------ PRF004

pub fn non_equi_join(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["join"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let rt = sc.receiver_ty;
    let Some(on) = sc.arg(1, "on") else {
        return;
    };
    if join_cond(ch, on, 0) != JoinCond::NonEqui {
        return;
    }
    let small = small_side(ch, c, rt);
    let mut d = msg!(PRF004, method_range(expr));
    if small {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ PRF032

pub fn or_join_condition(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["join"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let rt = sc.receiver_ty;
    let Some(on) = sc.arg(1, "on") else {
        return;
    };
    let mut cs = Vec::new();
    conjuncts(ch, on, Hops::DEFAULT, &mut cs);
    if !cs.contains(&Conj::OrWithEq) || cs.iter().any(|k| matches!(k, Conj::Equi | Conj::Unknown)) {
        return;
    }
    let small = small_side(ch, c, rt);
    let mut d = msg!(PRF032, method_range(expr));
    if small {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ PRF012

const EXPLODE_FNS: &[&str] = &[
    "explode",
    "explode_outer",
    "posexplode",
    "posexplode_outer",
    "inline",
    "inline_outer",
];

fn is_explode_call(ch: &Checker, e: &Expr) -> bool {
    let mut found = false;
    walk_exprs(e, &mut |x| {
        if let Expr::Call(c) = x
            && EXPLODE_FNS
                .iter()
                .any(|f| ch.model.is_spark_function(&c.func, f))
        {
            found = true;
        }
    });
    found
}

/// Output column names of an explode inside `select(...)` args.
fn select_explode_outputs(ch: &Checker, c: &ast::ExprCall) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut any = false;
    for a in &c.arguments.args {
        if !is_explode_call(ch, a) {
            continue;
        }
        any = true;
        // `F.explode(x).alias("n")` / `.alias("p", "c")`
        if let Some((ac, _, "alias" | "name" | "as_")) = method_call(a) {
            for n in &ac.arguments.args {
                if let Some(s) = str_literal(n) {
                    out.push(s.to_string());
                }
            }
        } else {
            out.extend(["col", "pos", "key", "value"].map(String::from));
        }
    }
    any.then_some(out)
}

/// Column names referenced by a filter predicate; `None` when it references
/// something we cannot name (a Column variable).
fn predicate_refs(ch: &Checker, e: &Expr, site: &SqlSite) -> Option<FxHashSet<String>> {
    let refs = if site.text.is_some() {
        ColumnRefs::of_site(site)
    } else {
        ColumnRefs::of(ch.model, e)
    };
    (!refs.unknown).then(|| refs.names.into_iter().collect())
}

/// Methods between the explode and the filter that a filter on other columns
/// commutes with. Anything else (limit, sample, union, dropDuplicates(subset),
/// a UDF map...) changes which rows the filter sees: stop.
fn commutes_with_filter(sc: &SparkCall) -> bool {
    transform::frame_row(sc.method).is_some_and(|r| r.commutes_with_filter(sc.call))
}

pub fn explode_before_filter(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["filter", "where"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let recv = sc.receiver;
    // A condition string (`condition=` too) is SQL text.
    let sites = ch.model.sql_sites(expr);
    let Some(site) = sites.iter().find(|s| s.kind == SqlSiteKind::Condition) else {
        return;
    };
    let Some(pred) = site.arg else {
        return;
    };
    let Some(refs) = predicate_refs(ch, pred, site) else {
        return;
    };
    // Walk down the receiver chain; remember columns created above the explode.
    let mut later: Vec<(String, &Expr)> = Vec::new();
    let mut e = recv;
    let mut same_expr = true;
    let mut hops = Hops::DEFAULT;
    let (explode_range, outputs) = loop {
        if let Some(sc) = ch.model.spark_call(e)
            && sc.is_dataframe()
        {
            let (cc, r, mm) = (sc.call, sc.receiver, sc.method);
            match mm {
                "withColumn" => {
                    let name = cc.arguments.args.first().and_then(str_literal);
                    let (Some(n), Some(v)) = (name, cc.arguments.args.get(1)) else {
                        return; // a column we cannot name could be the filter's
                    };
                    if is_explode_call(ch, v) {
                        break (method_range(e), vec![n.to_string()]);
                    }
                    later.push((n.to_string(), v));
                }
                "select" => {
                    if let Some(outs) = select_explode_outputs(ch, cc) {
                        break (method_range(e), outs);
                    }
                    // A select drops/renames columns: stop (refs may be new names).
                    return;
                }
                _ if commutes_with_filter(&sc) => {}
                _ => return,
            }
            e = r;
            continue;
        }
        if let Some((v, h)) = ch.model.hop_one(e, hops) {
            hops = h;
            same_expr = false;
            e = v;
            continue;
        }
        return;
    };
    // Columns derived from the explode output after it.
    let mut derived: FxHashSet<String> = outputs.into_iter().collect();
    for (name, v) in later.iter().rev() {
        // Over-inclusive: any expression string naming a derived column, a
        // DataFrame attribute of one, or a value we cannot read (a Column variable).
        let refs = ColumnRefs::of(ch.model, v);
        if refs.unknown || refs.names.iter().any(|t| derived.contains(t)) {
            derived.insert(name.clone());
        }
    }
    if refs.iter().any(|r| derived.contains(r)) {
        return;
    }
    let range = if same_expr {
        explode_range
    } else {
        method_range(expr)
    };
    if !ch.state.prf012_seen.insert(range) {
        return;
    }
    ch.report(msg!(PRF012, range));
}

// ------------------------------------------------------------------ PRF015

pub fn collect_dedup_redundant(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if !ch.model.is_spark_function(&c.func, "array_distinct") || c.arguments.args.len() != 1 {
        return;
    }
    let arg = &c.arguments.args[0];
    // peel `.over(w)`
    let (agg, over): (&Expr, Option<TextRange>) = match method_call(arg) {
        Some((oc, r, "over")) => (r, Some(TextRange::new(r.range().end(), oc.range().end()))),
        _ => (arg, None),
    };
    let Expr::Call(inner) = agg else { return };
    let kind = if ch.model.is_spark_function(&inner.func, "collect_list") {
        "collect_list"
    } else if ch.model.is_spark_function(&inner.func, "collect_set") {
        "collect_set"
    } else {
        return;
    };
    let v = if kind == "collect_list" {
        &PRF015_LIST
    } else {
        &PRF015_SET
    };
    let mut d = msg!(v, expr.range());
    // Fix: only for `<mod>.collect_list(...)` so the replacement name resolves.
    if let Expr::Attribute(fa) = &*inner.func {
        let module = ch.slice(fa.value.range());
        let args = ch.slice(inner.arguments.range);
        let over_src = over.map(|r| ch.slice(r)).unwrap_or("");
        let text = format!("{module}.collect_set{args}{over_src}");
        // An ordered window keeps collect_list order; collect_set does not.
        let order_sensitive =
            kind == "collect_list" && (over.is_some() || order_sensitive_parent(ch, expr));
        let fix_msg = format!("Replace with `{text}`");
        let edits = vec![Edit::replace(expr.range(), text)];
        d = d.with_fix(if order_sensitive {
            Fix::unsafe_(fix_msg, edits)
        } else {
            Fix::safe(fix_msg, edits)
        });
    }
    ch.report(d);
}

/// The array is consumed by something that depends on element order.
fn order_sensitive_parent(ch: &Checker, expr: &Expr) -> bool {
    match ch.parent() {
        Some(Node::Expr(Expr::Subscript(_))) => true,
        Some(Node::Expr(Expr::Attribute(a))) => {
            matches!(a.attr.as_str(), "getItem" | "__getitem__")
        }
        Some(Node::Expr(Expr::Call(c))) => {
            c.arguments.args.iter().any(|a| a.range() == expr.range())
                && [
                    "array_join",
                    "element_at",
                    "slice",
                    "get",
                    "array_position",
                    "concat_ws",
                ]
                .iter()
                .any(|f| ch.model.is_spark_function(&c.func, f))
        }
        _ => false,
    }
}

// ------------------------------------------------------------------ PRF023

/// `df.groupBy(..).pivot("col")` without a `values` list: Spark runs an extra job
/// (distinct values of the pivot column, collected to the driver) just to build the
/// output schema, every time the plan is built.
pub fn pivot_without_values(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["pivot"]) else {
        return;
    };
    if sc.class() != Ty::GroupedData {
        return;
    }
    let args = &c.arguments;
    if args.args.iter().any(|a| a.is_starred_expr())
        || args.keywords.iter().any(|k| k.arg.is_none())
    {
        return; // `*args` / `**kw`: the values may be passed through them
    }
    let Some(col) = sc.arg(0, "pivot_col") else {
        return;
    };
    match sc.arg(1, "values") {
        None => {}
        Some(v) if v.is_none_literal_expr() => {}
        Some(_) => return,
    }
    let v = loop_variant(ch, expr, &PRF023, &PRF023_LOOP);
    let column = short(ch, col);
    ch.report(msg!(v, method_range(expr), column));
}

// ------------------------------------------------------------------ PRF024

/// `df.cube(c1..cN)` (and `GROUP BY CUBE(..)` / `WITH CUBE` in `spark.sql`) makes 2^N
/// grouping sets: the Expand operator copies every input row 2^N times before the
/// shuffle. `rollup` makes N+1 sets and is reported from a higher column count. In SQL,
/// several CUBE / ROLLUP / GROUPING SETS items of one `GROUP BY` multiply, and the
/// product is reported once it reaches the sets of a `cube-min-columns` CUBE.
pub fn cube_many_columns(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["cube", "rollup", "sql"]) else {
        return;
    };
    let m = sc.method;
    let msg = match m {
        "cube" | "rollup" if sc.is_dataframe() => {
            let args = &c.arguments;
            // `df.cube(["a", "b"])` passes the columns as one list.
            let cols = column_args(args.args.iter());
            if cols.unknown || !args.keywords.is_empty() {
                return; // `*cols` / keywords: the column count is unknown
            }
            single_cube(ch, m == "cube", cols.exprs.len(), false)
        }
        "sql" if sc.class().is_session() => {
            let Some(q) = ch.model.sql_query(c) else {
                return;
            };
            // The clause with the most grouping sets that qualifies.
            let mut best: Option<(u128, Cube)> = None;
            for g in q.cube_groupings() {
                let msg = if g.parts == 1 {
                    single_cube(ch, g.is_cube, g.columns, true)
                } else {
                    combined_cube(ch, &g)
                };
                if let Some(msg) = msg
                    && best.as_ref().is_none_or(|(s, _)| g.sets > *s)
                {
                    best = Some((g.sets, msg));
                }
            }
            best.map(|(_, msg)| msg)
        }
        _ => None,
    };
    let Some(cube) = msg else {
        return;
    };
    let in_loop = ch.model.in_repeating_loop(expr);
    let d = match cube {
        Cube::Single { sql, op, n, sets } => {
            let v = match (sql, in_loop) {
                (false, false) => &PRF024,
                (false, true) => &PRF024_LOOP,
                (true, false) => &PRF024_SQL,
                (true, true) => &PRF024_SQL_LOOP,
            };
            msg!(v, method_range(expr), op, n, sets)
        }
        Cube::Combined { parts, sets } => {
            let v = if in_loop {
                &PRF024_COMBINED_LOOP
            } else {
                &PRF024_COMBINED
            };
            msg!(v, method_range(expr), n = parts, sets)
        }
    };
    ch.report(d);
}

/// What a CUBE / ROLLUP finding says ([`PRF024`]).
enum Cube {
    Single {
        sql: bool,
        op: &'static str,
        n: usize,
        sets: String,
    },
    Combined {
        parts: usize,
        sets: String,
    },
}

fn sets_text(sets: u128) -> String {
    if sets == u128::MAX {
        "2^N".to_string()
    } else {
        sets.to_string()
    }
}

/// Finding for one CUBE / ROLLUP of `n` columns, if it reaches its threshold.
fn single_cube(ch: &Checker, is_cube: bool, n: usize, sql: bool) -> Option<Cube> {
    let min = if is_cube {
        ch.settings.cube_min_columns
    } else {
        ch.settings.rollup_min_columns
    };
    if min == 0 || n < min as usize {
        return None;
    }
    let op = if is_cube { "CUBE" } else { "ROLLUP" };
    let sets = sets_text(sparklint_semantic::sql::grouping_sets(is_cube, n));
    Some(Cube::Single { sql, op, n, sets })
}

/// Finding for a `GROUP BY` with several CUBE / ROLLUP / GROUPING SETS items: reported
/// when one of them reaches its own threshold, or their product reaches the sets of a
/// `cube-min-columns` CUBE.
fn combined_cube(ch: &Checker, g: &sparklint_semantic::sql::CubeGrouping) -> Option<Cube> {
    let alone = single_cube(ch, g.is_cube, g.columns, true).is_some();
    let cube_min = ch.settings.cube_min_columns;
    let product =
        cube_min != 0 && sparklint_semantic::sql::grouping_sets(true, cube_min as usize) <= g.sets;
    if !alone && !product {
        return None;
    }
    let sets = sets_text(g.sets);
    Some(Cube::Combined {
        parts: g.parts,
        sets,
    })
}

// ------------------------------------------------------------------ PRF025

/// `spark.sql("... x NOT IN (SELECT ...)")`: the subquery becomes a null-aware anti
/// join, which Spark plans as a broadcast nested-loop join unless a single-column
/// subquery is small enough to broadcast.
pub fn not_in_subquery(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    if !ch
        .model
        .spark_sql(expr)
        .is_some_and(|q| q.not_in_subquery())
    {
        return;
    }
    let v = loop_variant(ch, expr, &PRF025, &PRF025_LOOP);
    ch.report(msg!(v, method_range(expr)));
}

// ------------------------------------------------------------------ PRF026

const DISTINCT_FNS: &[&str] = &[
    "countDistinct",
    "count_distinct",
    "sumDistinct",
    "sum_distinct",
];

/// Several exact distinct aggregates over different column sets in one aggregation
/// (`agg(F.countDistinct("a"), F.countDistinct("b"), ...)`, `selectExpr` / `F.expr`
/// strings, `spark.sql` SELECT lists): `RewriteDistinctAggregates` adds an Expand that
/// copies every input row once per distinct column set before the shuffle.
pub fn many_distinct_aggregates(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let min = ch.settings.distinct_aggregates_min as usize;
    if min == 0 {
        return;
    }
    let Some(sc) = ch
        .model
        .spark_call_named(expr, &["agg", "select", "selectExpr", "sql"])
    else {
        return;
    };
    let (n, what) = match sc.method {
        "agg" if sc.class() == Ty::GroupedData || sc.is_dataframe() => {
            (distinct_sets(ch, c), "agg()")
        }
        "select" if sc.is_dataframe() => (distinct_sets(ch, c), "select()"),
        "selectExpr" if sc.is_dataframe() => (sql_distinct_sets(ch, expr), "selectExpr()"),
        "sql" if sc.class().is_session() => {
            let Some(q) = ch.model.sql_query(c) else {
                return;
            };
            let n = q
                .distinct_aggregates()
                .into_iter()
                .map(|keys| keys.into_iter().collect::<FxHashSet<_>>().len())
                .max()
                .unwrap_or(0);
            (n, "SQL SELECT")
        }
        _ => return,
    };
    if n < min {
        return;
    }
    let v = loop_variant(ch, expr, &PRF026, &PRF026_LOOP);
    ch.report(msg!(v, method_range(expr), n, what));
}

/// Number of different argument sets among the distinct aggregates in the SQL items of
/// `selectExpr` (its SQL sites).
fn sql_distinct_sets(ch: &Checker, expr: &Expr) -> usize {
    let mut keys: FxHashSet<String> = FxHashSet::default();
    for s in ch.model.sql_sites(expr) {
        if let Some(t) = s.text {
            keys.extend(t.distinct_aggregates().into_iter().flatten());
        }
    }
    keys.len()
}

/// Number of different argument sets among the distinct aggregates in the call's
/// arguments.
fn distinct_sets(ch: &Checker, c: &ast::ExprCall) -> usize {
    let mut keys: FxHashSet<String> = FxHashSet::default();
    for a in c.arguments.args.iter() {
        if let Expr::Starred(s) = a
            && let Some((elt, var, values)) = literal_comprehension(ch, &s.value)
        {
            for v in &values {
                add_distinct_calls(ch, elt, Some((var, v)), &mut keys);
            }
            continue;
        }
        add_distinct_calls(ch, a, None, &mut keys);
    }
    keys.len()
}

/// Adds the argument set of every distinct aggregate below `e` to `keys`. `var` is a
/// comprehension variable and the literal value it holds in this iteration: a call
/// that reads the variable gets a key per value.
fn add_distinct_calls(
    ch: &Checker,
    e: &Expr,
    var: Option<(&str, &str)>,
    keys: &mut FxHashSet<String>,
) {
    walk_exprs(e, &mut |e| {
        let Expr::Call(call) = e else {
            return;
        };
        if DISTINCT_FNS
            .iter()
            .any(|f| ch.model.is_spark_function(&call.func, f))
        {
            let mut cols: Vec<String> = call
                .arguments
                .args
                .iter()
                .map(|x| match (x, var) {
                    // The loop variable itself names the column.
                    (Expr::Name(n), Some((name, value))) if n.id.as_str() == name => {
                        value.to_string()
                    }
                    (x, Some((name, value))) if mentions_name(x, name) => {
                        format!("{}@{value}", distinct_arg_key(ch, x))
                    }
                    (x, _) => distinct_arg_key(ch, x),
                })
                .collect();
            if cols.is_empty() {
                return;
            }
            cols.sort();
            keys.insert(cols.join(","));
        } else if let Some(t) = f_expr_site(ch, e).and_then(|s| s.text) {
            keys.extend(t.distinct_aggregates().into_iter().flatten());
        }
    });
}

/// `[<elt> for v in <literal list>]` (or a generator), inline or a name bound once to
/// one: the element, the loop variable and the source text of each literal value.
/// `None` for several `for` clauses, `if` filters, unpacking targets, or an iterable
/// that is not a literal list / tuple (a name bound once to one is followed).
fn literal_comprehension<'e>(
    ch: &Checker<'e>,
    e: &'e Expr,
) -> Option<(&'e Expr, &'e str, Vec<String>)> {
    let e = ch.model.value_of(e, Hops::DEFAULT);
    let (elt, generators) = match e {
        Expr::ListComp(l) => (&*l.elt, &l.generators),
        Expr::Generator(g) => (&*g.elt, &g.generators),
        _ => return None,
    };
    let [g] = generators.as_slice() else {
        return None;
    };
    let Expr::Name(var) = &g.target else {
        return None;
    };
    if !g.ifs.is_empty() || g.is_async {
        return None;
    }
    let elts = match ch.model.value_of(&g.iter, Hops::DEFAULT) {
        Expr::List(l) => &l.elts,
        Expr::Tuple(t) => &t.elts,
        _ => return None,
    };
    if elts.iter().any(|x| !x.is_literal_expr()) {
        return None;
    }
    let values = elts
        .iter()
        .map(|x| {
            str_literal(x).map_or_else(|| ch.slice(x.range()).to_string(), |s| s.to_lowercase())
        })
        .collect();
    Some((elt, var.id.as_str(), values))
}

/// `"a"`, `F.col("a")`, `df.a`, `df["a"]` -> `a`; anything else -> its source text.
fn distinct_arg_key(ch: &Checker, e: &Expr) -> String {
    if let Some(n) = columns::column_name(ch.model, e) {
        return n.replace('`', "").to_lowercase();
    }
    ch.slice(e.range())
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

// ------------------------------------------------------------------ PRF027

/// `df.orderBy(F.rand())` (and `ORDER BY rand()` in `spark.sql`): a full global sort
/// (a sampling job for the range bounds, then a shuffle of every row) only to put the
/// rows in random order, usually before `limit(n)` to "take n random rows".
/// `Window.orderBy(F.rand())` (per-group sampling) and `sortWithinPartitions` are not
/// global sorts and are not reported.
pub fn order_by_rand(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["orderBy", "sort", "sql"]) else {
        return;
    };
    let (sql, method) = match sc.method {
        "orderBy" | "sort" if sc.is_dataframe() && !sc.driver_sized() => {
            let first = match c.arguments.args.first() {
                Some(Expr::List(l)) => l.elts.first(),
                Some(Expr::Tuple(t)) => t.elts.first(),
                a => a,
            };
            if !first.is_some_and(|k| is_random_key(ch, k)) {
                return;
            }
            (false, sc.method)
        }
        "sql" if sc.class().is_session() => {
            if !ch.model.sql_query(c).is_some_and(|q| q.order_by_rand()) {
                return;
            }
            (true, sc.method)
        }
        _ => return,
    };
    let v = if sql {
        loop_variant(ch, expr, &PRF027_SQL, &PRF027_SQL_LOOP)
    } else {
        loop_variant(ch, expr, &PRF027, &PRF027_LOOP)
    };
    ch.report(msg!(v, method_range(expr), method));
}

/// `F.rand()`, `F.randn(seed)`, `F.rand().desc()`, `F.desc(F.rand())`, `F.expr("rand()")`.
fn is_random_key(ch: &Checker, e: &Expr) -> bool {
    let Expr::Call(c) = e else {
        return false;
    };
    if let Expr::Attribute(a) = &*c.func {
        let m = a.attr.as_str();
        if (m.starts_with("asc") || m.starts_with("desc")) && c.arguments.args.is_empty() {
            return is_random_key(ch, &a.value);
        }
    }
    let model = &ch.model;
    if ["rand", "randn"]
        .iter()
        .any(|f| model.is_spark_function(&c.func, f))
    {
        return true;
    }
    let ordering = [
        "asc",
        "desc",
        "asc_nulls_first",
        "asc_nulls_last",
        "desc_nulls_first",
        "desc_nulls_last",
    ];
    if ordering.iter().any(|f| model.is_spark_function(&c.func, f)) {
        return c
            .arguments
            .args
            .first()
            .is_some_and(|a| is_random_key(ch, a));
    }
    if let Some(site) = f_expr_site(ch, e) {
        return site.text.is_some_and(|t| t.is_random_call());
    }
    false
}

// ------------------------------------------------------------------ PRF035

/// Methods on `GroupedData` that hand each group to Python as one pandas / Arrow frame.
const GROUP_MAP_FNS: &[&str] = &[
    "applyInPandas",
    "applyInArrow",
    "apply",
    "cogroup",
    "applyInPandasWithState",
    "transformWithStateInPandas",
    "transformWithState",
];

/// Streaming stateful grouped functions: one group = one state and one task per batch.
const STATEFUL_FNS: &[&str] = &[
    "applyInPandasWithState",
    "transformWithStateInPandas",
    "transformWithState",
];

/// `df.groupBy().applyInPandas(fn, schema)` (also `groupby()`, `groupBy([])`,
/// `groupBy(F.lit(1))`, `apply(<grouped pandas_udf>)`, `applyInArrow`, `cogroup`, the
/// streaming stateful functions): an empty or constant grouping is ONE global group, so the
/// whole frame is shuffled into a single task and loaded into one pandas DataFrame. The
/// grouping call may be chained or bound once to a name.
pub fn keyless_grouped_pandas(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, GROUP_MAP_FNS) else {
        return;
    };
    if sc.class() != Ty::GroupedData {
        return;
    }
    let Some(group) = grouping_call(ch, sc.receiver) else {
        return;
    };
    let Some(g) = ch.model.spark_call_named(group, &["groupBy", "groupby"]) else {
        return;
    };
    if !g.is_dataframe() || g.driver_sized() {
        return;
    }
    let Some(keys) = grouping_keys(ch, g.call) else {
        return;
    };
    let grouping = match keys {
        GroupKeys::None => format!("{}() with no columns", g.method),
        GroupKeys::Constant => format!("{}() on only a constant F.lit() key", g.method),
    };
    let v = if STATEFUL_FNS.contains(&sc.method) {
        loop_variant(ch, expr, &PRF035_STATEFUL, &PRF035_STATEFUL_LOOP)
    } else {
        loop_variant(ch, expr, &PRF035, &PRF035_LOOP)
    };
    ch.report(msg!(v, method_range(expr), grouping, method = sc.method));
}

/// The expression that produced a `GroupedData` receiver: the receiver itself, or the
/// value of the single binding of a name receiver (`g = df.groupBy(); g.applyInPandas(..)`).
fn grouping_call<'a>(ch: &Checker<'a>, recv: &'a Expr) -> Option<&'a Expr> {
    match recv {
        Expr::Call(_) => Some(recv),
        Expr::Name(_) => ch
            .model
            .hop_one(recv, Hops::DEFAULT)
            .map(|(v, _)| v)
            .filter(|v| v.is_call_expr()),
        _ => None,
    }
}

/// A grouping that makes one global group.
#[derive(Debug, PartialEq)]
enum GroupKeys {
    /// `groupBy()` / `groupBy([])` / `groupBy(())`.
    None,
    /// Only `F.lit(..)` keys (optionally aliased): every row gets the same key.
    Constant,
}

/// `Some` when the grouping call makes one global group. Starred or keyword arguments
/// may carry columns, so they count as columns; plain strings are column NAMES.
fn grouping_keys(ch: &Checker, c: &ast::ExprCall) -> Option<GroupKeys> {
    let args = &c.arguments;
    if !args.keywords.is_empty() {
        return None;
    }
    let keys: &[Expr] = match args.args.as_ref() {
        [Expr::List(l)] => &l.elts,
        [Expr::Tuple(t)] => &t.elts,
        all => all,
    };
    if keys.is_empty() {
        return Some(GroupKeys::None);
    }
    keys.iter()
        .all(|k| is_constant_key(ch, k))
        .then_some(GroupKeys::Constant)
}

/// `F.lit(<literal>)`, also wrapped in `.alias(..)` / `.name(..)`.
fn is_constant_key(ch: &Checker, e: &Expr) -> bool {
    let Expr::Call(c) = e else { return false };
    if let Expr::Attribute(a) = &*c.func
        && matches!(a.attr.as_str(), "alias" | "name")
    {
        return is_constant_key(ch, &a.value);
    }
    ch.model.is_spark_function(&c.func, "lit")
        && c.arguments.keywords.is_empty()
        && matches!(
            c.arguments.args.as_ref(),
            [Expr::StringLiteral(_) | Expr::NumberLiteral(_) | Expr::BooleanLiteral(_)]
        )
}

// ------------------------------------------------------------------ PRF036

/// The join strategy hint that forces a shuffle-and-replicate nested-loop join.
const NL_HINT: &str = "SHUFFLE_REPLICATE_NL";

/// `df.hint("shuffle_replicate_nl")` or `/*+ SHUFFLE_REPLICATE_NL(t) */` in `spark.sql`:
/// the hint forces a cartesian product filtered afterwards, even for equi-joins a hash
/// or sort-merge join could key on.
pub fn forced_nested_loop_join_hint(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["hint", "sql"]) else {
        return;
    };
    let what = match sc.method {
        "hint" if sc.is_dataframe() => {
            let Some(name) = sc.arg(0, "name").and_then(|a| ch.model.resolve_text(a)) else {
                return;
            };
            if !name.trim().eq_ignore_ascii_case(NL_HINT) {
                return;
            }
            format!("hint(\"{}\")", name.trim())
        }
        "sql" if sc.class().is_session() => {
            if !ch
                .model
                .sql_query(c)
                .is_some_and(|q| q.hints().iter().any(|h| h == NL_HINT))
            {
                return;
            }
            format!("SQL /*+ {NL_HINT} */ hint")
        }
        _ => return,
    };
    let v = loop_variant(ch, expr, &PRF036, &PRF036_LOOP);
    ch.report(msg!(v, method_range(expr), hint = what));
}

// ------------------------------------------------------------------ PRF022

pub fn sql_select_star(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    if !ch.model.spark_sql(expr).is_some_and(|q| q.select_star()) {
        return;
    }
    if ch.model.ty_of(expr).size().fits_driver() {
        return; // `SELECT * FROM t LIMIT 10`: a few rows, the column count hardly matters
    }
    match flow::classify_current(ch) {
        Sink::Action {
            kind: RunKind::Write | RunKind::DriverPull,
            projected: false,
            few_rows: false,
            ..
        } => report_select_star(ch, method_range(expr)),
        Sink::Cache { .. } => report_select_star(ch, method_range(expr)),
        Sink::Derived(b) => ch.state.select_star.push((b, method_range(expr))),
        _ => {}
    }
}

fn report_select_star(ch: &mut Checker, range: TextRange) {
    ch.report(msg!(PRF022, range).with_severity(Severity::Info));
}

fn reaches_sink(ch: &Checker, b: BindingId, seen: &mut FxHashSet<BindingId>) -> bool {
    if !seen.insert(b) {
        return false;
    }
    for u in ch.state.flow.uses.get(&b).map(Vec::as_slice).unwrap_or(&[]) {
        match &u.sink {
            Sink::Action {
                kind: RunKind::Write | RunKind::DriverPull,
                projected: false,
                few_rows: false,
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
