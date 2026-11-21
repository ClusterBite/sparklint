//! PRF — plan & shuffle performance: PRF001, PRF002, PRF003, PRF005, PRF029, PRF031.

use ruff_python_ast::{self as ast, Expr, Stmt};
use ruff_text_size::Ranged;
use sparklint_semantic::{Hops, LoopInfo, Partitioning, SqlText, Ty, call_arg};

use super::Rule;
use super::helpers::*;
use super::msg::Msg;
use crate::checker::Checker;
use crate::diagnostic::{Edit, Fix, Severity};
use crate::fix_text::one_line;
use crate::msg;

/// PRF001/002/003/005/029/031 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PRF001")
        .stmt(prf001_stmt)
        .call(withcolumn_reduce)
        .msgs(&[PRF001_LOOP, PRF001_REDUCE]),
    Rule::new("PRF002").stmt(prf002_stmt).msgs(&[PRF002]),
    Rule::new("PRF003")
        .call(cartesian_join)
        .msgs(&[PRF003_CROSS_JOIN, PRF003_JOIN]),
    Rule::new("PRF005").call(global_window).msgs(&[PRF005]),
    Rule::new("PRF031").call(running_collect_list).msgs(&[
        PRF031,
        PRF031_GLOBAL,
        PRF031_SQL,
        PRF031_SQL_GLOBAL,
    ]),
    Rule::new("PRF029")
        .call(replicated_storage_level)
        .msgs(&[PRF029, PRF029_LOOP]),
];

const PRF001_LOOP: Msg = Msg::new(
    "PRF001",
    "loop",
    ".withColumn() in a loop adds one projection per iteration — use withColumns()/select().",
)
.help("Build all expressions first, then apply them once: df.withColumns({{name: expr, ...}}) (Spark >= 3.3) or df.select(\"*\", *exprs).");
const PRF001_REDUCE: Msg = Msg::new(
    "PRF001",
    "reduce",
    "functools.reduce(...withColumn...) adds one projection per element — use withColumns()/select().",
);
const PRF002: Msg = Msg::new(
    "PRF002",
    "loop",
    ".{method}() in a loop adds one plan node per iteration — batch it into one call.",
)
.help("Build the full list first: df.withColumnsRenamed({{old: new, ...}}) (Spark >= 3.4), df.drop(*cols), or one df.select(...).");
const PRF003_HELP: &str = "Join on a key (df.join(other, \"key\", \"inner\")); for a scalar, aggregate it first so one side has one row.";
const PRF003_CROSS_JOIN: Msg = Msg::new(
    "PRF003",
    "cross-join",
    "Cartesian product: crossJoin() produces |A|×|B| rows.",
)
.help(PRF003_HELP);
const PRF003_JOIN: Msg = Msg::new(
    "PRF003",
    "join-without-condition",
    "Cartesian product: join() without a condition produces |A|×|B| rows.",
)
.help(PRF003_HELP);
const PRF005: Msg = Msg::new(
    "PRF005",
    "global-window",
    "Window without partitionBy() moves all rows into a single partition.",
)
.help("Add Window.partitionBy(<key>) — or use df.agg(...) for global totals.");
const PRF031_HELP: &str = "For all values per key use df.groupBy(k).agg(F.sort_array(F.{name}(F.struct(order_col, value)))); for the last n values bound the frame, e.g. .rowsBetween(-n, Window.currentRow).";
const PRF031: Msg = Msg::new(
    "PRF031",
    "running-window",
    "{name}() over a running window (ordered, frame from the partition start to the current row) gives every row its own growing array: O(N²) elements per partition.",
)
.help(PRF031_HELP);
const PRF031_GLOBAL: Msg = Msg::new(
    "PRF031",
    "running-window-unpartitioned",
    "{name}() over a running window (ordered, frame from the partition start to the current row) gives every row its own growing array: O(N²) elements per partition. The window has no partitionBy(), so all rows land in one partition.",
)
.help(PRF031_HELP);
const PRF031_SQL_HELP: &str = "For all values per key use GROUP BY k with sort_array({name}(struct(order_col, value))); for the last n values bound the frame, e.g. ROWS BETWEEN n PRECEDING AND CURRENT ROW.";
const PRF031_SQL: Msg = Msg::new(
    "PRF031",
    "sql-running-window",
    "{name}() over a running window in SQL (ordered, frame from the partition start to the current row) gives every row its own growing array: O(N²) elements per partition.",
)
.help(PRF031_SQL_HELP);
const PRF031_SQL_GLOBAL: Msg = Msg::new(
    "PRF031",
    "sql-running-window-unpartitioned",
    "{name}() over a running window in SQL (ordered, frame from the partition start to the current row) gives every row its own growing array: O(N²) elements per partition. The window has no PARTITION BY, so all rows land in one partition.",
)
.help(PRF031_SQL_HELP);
const PRF029_HELP: &str = "Use StorageLevel.{base} (or plain .cache()); keep replication only for low-latency serving where losing an executor must not trigger recomputation.";
const PRF029: Msg = Msg::new(
    "PRF029",
    "replicated",
    "Storage level {level} keeps every cached partition {times} (extra executor memory/disk and a network copy of each block); recomputation already makes cached data fault-tolerant.",
)
.help(PRF029_HELP);
const PRF029_LOOP: Msg = Msg::new(
    "PRF029",
    "replicated-in-loop",
    "Storage level {level} keeps every cached partition {times} (extra executor memory/disk and a network copy of each block); recomputation already makes cached data fault-tolerant. Inside a loop it is paid on every iteration.",
)
.help(PRF029_HELP);

fn prf001_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::Assign(a) = stmt {
        withcolumn_in_loop(ch, a, stmt);
    }
}

fn prf002_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::Assign(a) = stmt {
        column_op_in_loop(ch, a, stmt);
    }
}

// ------------------------------------------------------------------ PRF001

/// `x = x.<…>.m(…)<…>` inside a loop, where `m` is one of `methods`, the receiver is
/// a DataFrame and the new binding of `x` reaches the receiver on the next
/// iteration (a loop-carried DataFrame). Returns the matching call closest to the
/// root, its method and the innermost loop.
pub(crate) fn loop_carried_call<'a>(
    ch: &Checker,
    a: &'a ast::StmtAssign,
    stmt: &Stmt,
    methods: &[&str],
) -> Option<(&'a Expr, &'a str, LoopInfo)> {
    let [Expr::Name(target)] = a.targets.as_slice() else {
        return None;
    };
    // innermost-to-root walk over the receiver chain; remember the call closest to the root
    let mut e: &Expr = &a.value;
    let mut hit: Option<(&Expr, &str)> = None;
    loop {
        if let Some((_, recv, m)) = method_call(e) {
            if methods.contains(&m) {
                hit = Some((e, m));
            }
            e = recv;
            continue;
        }
        // `df.na.fill(...)`
        if let Expr::Attribute(at) = e
            && matches!(at.attr.as_str(), "na" | "stat")
        {
            e = &at.value;
            continue;
        }
        break;
    }
    let (call, m) = hit?;
    let Expr::Name(root) = e else { return None };
    if root.id != target.id {
        return None;
    }
    let (_, recv, _) = method_call(call)?;
    let recv_df = ch.model.is_dataframe(recv)
        || matches!(recv, Expr::Attribute(at) if ch.model.is_dataframe(&at.value));
    if !recv_df {
        return None;
    }
    // The loop that repeats the statement (ACT005 / IOD006 judge the same one): a
    // retry loop that leaves right after it runs it once.
    let inner = ch.model.repeating_loop_at(stmt.start())?;
    // loop-carried: the previous iteration's value flows into the receiver.
    if !inner.carried(root, stmt.start()) {
        return None;
    }
    Some((call, m, inner.info().clone()))
}

pub fn withcolumn_in_loop(ch: &mut Checker, a: &ast::StmtAssign, stmt: &Stmt) {
    let Some((wc, _, inner)) = loop_carried_call(ch, a, stmt, &["withColumn"]) else {
        return;
    };
    // One finding per loop (cylint parity): the fix is a single rewrite anyway.
    if !ch.state.prf001_loops.insert(inner.range) {
        return;
    }
    let [Expr::Name(target)] = a.targets.as_slice() else {
        return;
    };
    let small = inner.literal_len.is_some_and(|n| n <= 3);
    let mut d = msg!(PRF001_LOOP, method_range(wc)).with_site(inner.range);
    if small {
        d = d.with_severity(Severity::Info);
    }
    if let Some(fix) = withcolumns_fix(ch, a, stmt, target) {
        d = d.with_fix(fix);
    }
    ch.report(d);
}

fn withcolumns_fix(
    ch: &Checker,
    a: &ast::StmtAssign,
    stmt: &Stmt,
    target: &ast::ExprName,
) -> Option<Fix> {
    if ch.settings.spark_version < (3, 3) {
        return None;
    }
    let Some(Stmt::For(f)) = ch.parent_stmt_of_current() else {
        return None;
    };
    if f.body.len() != 1 || f.body[0].range() != stmt.range() || !f.orelse.is_empty() || f.is_async
    {
        return None;
    }
    let (c, recv, m) = method_call(&a.value)?;
    if m != "withColumn" || c.arguments.args.len() != 2 || !c.arguments.keywords.is_empty() {
        return None;
    }
    let Expr::Name(r) = recv else { return None };
    if r.id != target.id {
        return None;
    }
    let key = ch.slice(c.arguments.args[0].range());
    let val = ch.slice(c.arguments.args[1].range());
    let tgt = ch.slice(f.target.range());
    let iter = ch.slice(f.iter.range());
    if !one_line(&[key, val, tgt, iter]) {
        return None;
    }
    let name = target.id.as_str();
    let text = format!("{name} = {name}.withColumns({{{key}: {val} for {tgt} in {iter}}})");
    Some(Fix::unsafe_(
        "Rewrite the loop as a single withColumns()",
        vec![Edit::replace(f.range, text)],
    ))
}

// ------------------------------------------------------------------ PRF002

const LOOP_COLUMN_OPS: &[&str] = &[
    "withColumnRenamed",
    "withColumnsRenamed",
    "withColumns",
    "drop",
    "select",
    "selectExpr",
    "toDF",
    "fill",
    "replace",
];

pub fn column_op_in_loop(ch: &mut Checker, a: &ast::StmtAssign, stmt: &Stmt) {
    let Some((call, m, inner)) = loop_carried_call(ch, a, stmt, LOOP_COLUMN_OPS) else {
        return;
    };
    // `fill`/`replace` only through `df.na`.
    if matches!(m, "fill" | "replace")
        && !matches!(method_call(call), Some((_, Expr::Attribute(at), _)) if at.attr.as_str() == "na")
    {
        return;
    }
    if !ch.state.prf002_loops.insert(inner.range) {
        return;
    }
    let [Expr::Name(target)] = a.targets.as_slice() else {
        return;
    };
    let mut d = msg!(PRF002, method_range(call), method = m).with_site(inner.range);
    if inner.literal_len.is_some_and(|n| n <= 3) {
        d = d.with_severity(Severity::Info);
    }
    if let Some(fix) = column_op_fix(ch, a, stmt, target, m) {
        d = d.with_fix(fix);
    }
    ch.report(d);
}

fn column_op_fix(
    ch: &Checker,
    a: &ast::StmtAssign,
    stmt: &Stmt,
    target: &ast::ExprName,
    m: &str,
) -> Option<Fix> {
    let Some(Stmt::For(f)) = ch.parent_stmt_of_current() else {
        return None;
    };
    if f.body.len() != 1 || f.body[0].range() != stmt.range() || !f.orelse.is_empty() || f.is_async
    {
        return None;
    }
    let (c, recv, _) = method_call(&a.value)?;
    let Expr::Name(r) = recv else { return None };
    if r.id != target.id || !c.arguments.keywords.is_empty() {
        return None;
    }
    let name = target.id.as_str();
    let tgt = ch.slice(f.target.range());
    let iter = ch.slice(f.iter.range());
    let text = match (m, c.arguments.args.as_slice()) {
        ("withColumnRenamed", [old, new]) if ch.settings.spark_version >= (3, 4) => {
            let (o, n) = (ch.slice(old.range()), ch.slice(new.range()));
            if !one_line(&[o, n]) {
                return None;
            }
            format!("{name} = {name}.withColumnsRenamed({{{o}: {n} for {tgt} in {iter}}})")
        }
        ("drop", [Expr::Name(col)]) if matches!(&*f.target, Expr::Name(t) if t.id == col.id) => {
            format!("{name} = {name}.drop(*{iter})")
        }
        _ => return None,
    };
    if !one_line(&[&text]) {
        return None;
    }
    Some(Fix::unsafe_(
        format!("Rewrite the loop as a single {m}() call"),
        vec![Edit::replace(f.range, text)],
    ))
}

/// `functools.reduce(lambda d, c: d.withColumn(...), cols, df)`.
pub fn withcolumn_reduce(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if ch.model.qualified_name(&c.func).as_deref() != Some("functools.reduce") {
        return;
    }
    let Some(Expr::Lambda(l)) = c.arguments.args.first() else {
        return;
    };
    let Some(params) = &l.parameters else { return };
    let Some(acc) = params.iter_non_variadic_params().next() else {
        return;
    };
    let mut e: &Expr = &l.body;
    let mut has_wc = false;
    while let Some((_, recv, m)) = method_call(e) {
        has_wc |= m == "withColumn";
        e = recv;
    }
    let Expr::Name(root) = e else { return };
    if !has_wc || root.id.as_str() != acc.parameter.name.as_str() {
        return;
    }
    if !c
        .arguments
        .args
        .get(2)
        .is_some_and(|init| ch.model.is_dataframe(init))
    {
        return;
    }
    ch.report(msg!(PRF001_REDUCE, expr.range()));
}

// ------------------------------------------------------------------ PRF003

pub fn cartesian_join(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["crossJoin", "join"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let (m, rt) = (sc.method, sc.receiver_ty);
    let Some(other) = sc.arg(0, "other") else {
        return;
    };
    if m == "join" {
        match sc.arg(1, "on") {
            // `on=None` is the same as no condition
            None | Some(Expr::NoneLiteral(_)) => {}
            Some(on) => {
                let lit_true = matches!(on, Expr::BooleanLiteral(b) if b.value)
                    || matches!(on, Expr::Call(lc) if ch.model.is_spark_function(&lc.func, "lit")
                        && matches!(lc.arguments.args.first(), Some(Expr::BooleanLiteral(b)) if b.value));
                if !lit_true {
                    return;
                }
            }
        }
    }
    let ot = ch.model.ty_of(other);
    let (rs, os) = (rt.size(), ot.size());
    if rs.one_row() || os.one_row() {
        return; // scalar broadcast idiom
    }
    let small = rs.broadcastable() || os.broadcastable();
    let v = if m == "crossJoin" {
        &PRF003_CROSS_JOIN
    } else {
        &PRF003_JOIN
    };
    let mut d = msg!(v, method_range(expr));
    if small {
        d = d.with_severity(Severity::Info);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ PRF005

pub fn global_window(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some((_, _, m)) = method_call(expr) else {
        return;
    };
    if m != "over" {
        return;
    }
    let Some(w) = call_arg(c, 0, "window") else {
        return;
    };
    let t = ch.model.ty_of(w);
    // Only a spec known to be unpartitioned on every path: one that is partitioned
    // when the caller supplies keys (`if keys: w = w.partitionBy(*keys)`) is global
    // only when there is a single series by design, and a parameter's origin is unknown.
    if t.ty != Ty::WindowSpec || t.facts.win_partitioning != Partitioning::Global {
        return;
    }
    ch.report(msg!(PRF005, method_range(expr)));
}

// ------------------------------------------------------------------ PRF031

const COLLECT_AGGS: &[&str] = &[
    "pyspark.sql.functions.collect_list",
    "pyspark.sql.functions.collect_set",
    "pyspark.sql.functions.array_agg",
];

/// `F.collect_list(x).over(w)` where `w` is ordered with the default frame (unbounded
/// preceding .. current row) or an explicit frame that starts unbounded preceding and
/// ends at a finite offset: every row gets its own, ever longer copy of the array, so a
/// partition of N rows materialises O(N²) elements. Whole-partition and bounded frames
/// are not reported. The same window in SQL text (`F.expr`, `selectExpr`, `spark.sql`)
/// is read through [`SqlText::running_collects`].
pub fn running_collect_list(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if let Some(found) = sql_running_collect(ch, expr) {
        let name = found.name;
        let v = if found.partitioned {
            &PRF031_SQL
        } else {
            &PRF031_SQL_GLOBAL
        };
        ch.report(msg!(v, method_range(expr), name));
        return;
    }
    let Some((_, recv, m)) = method_call(expr) else {
        return;
    };
    if m != "over" {
        return;
    }
    let Expr::Call(agg) = recv else {
        return;
    };
    let Some(q) = ch.model.qualified_name(&agg.func) else {
        return;
    };
    if !COLLECT_AGGS.contains(&q.as_str()) {
        return;
    }
    let Some(w) = call_arg(c, 0, "window") else {
        return;
    };
    let t = ch.model.ty_of(w);
    let f = t.facts;
    if t.ty != Ty::WindowSpec || !f.win_ordered || (f.win_framed && !f.win_frame_growing) {
        return;
    }
    let name = q.rsplit('.').next().unwrap_or("collect_list");
    let v = if f.win_partitioning != Partitioning::Partitioned {
        &PRF031_GLOBAL
    } else {
        &PRF031
    };
    ch.report(msg!(v, method_range(expr), name));
}

/// The first running array aggregate in the SQL text of `F.expr(..)`,
/// `df.selectExpr(..)` or `spark.sql(..)`.
fn sql_running_collect(ch: &Checker, expr: &Expr) -> Option<sparklint_semantic::RunningCollect> {
    ch.model
        .sql_sites(expr)
        .into_iter()
        .filter_map(|s| s.text)
        .find_map(|q| q.running_collects().into_iter().next())
}

// ------------------------------------------------------------------ PRF029

/// `df.persist(StorageLevel.MEMORY_AND_DISK_2)` (any `*_2`/`*_3` level, or
/// `StorageLevel(.., replication>1)`), `spark.catalog.cacheTable(t, <replicated level>)`
/// and SQL `CACHE TABLE .. OPTIONS ('storageLevel' '.._2')`: every cached block is
/// stored (and sent over the network) twice, although recomputation already makes
/// every level fault-tolerant.
pub fn replicated_storage_level(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch
        .model
        .spark_call_named(expr, &["persist", "cacheTable", "sql"])
    else {
        return;
    };
    let level = match sc.method {
        "persist" if sc.is_dataframe() || sc.class() == Ty::Rdd => sc
            .arg(0, "storageLevel")
            .and_then(|a| replicated_level(ch, a, Hops::DEFAULT)),
        "cacheTable" if sc.class() == Ty::Catalog => sc
            .arg(1, "storageLevel")
            .and_then(|a| replicated_level(ch, a, Hops::DEFAULT)),
        "sql" if sc.class().is_session() => ch
            .model
            .sql_query(c)
            .and_then(|q| sql_cache_level(&q))
            .map(|l| {
                let n = replication_of(&l).unwrap_or(2);
                (l, n)
            }),
        _ => None,
    };
    let Some((name, n)) = level else {
        return;
    };
    let times = if n == 2 {
        "twice".to_string()
    } else {
        format!("{n} times")
    };
    let v = if ch.model.in_repeating_loop(expr) {
        &PRF029_LOOP
    } else {
        &PRF029
    };
    let base = name
        .strip_suffix("_2")
        .or_else(|| name.strip_suffix("_3"))
        .filter(|b| !b.is_empty() && !b.starts_with("StorageLevel("))
        .unwrap_or("MEMORY_AND_DISK");
    ch.report(msg!(v, method_range(expr), level = name, times, base));
}

/// `MEMORY_AND_DISK_2` → 2, `DISK_ONLY_3` → 3.
fn replication_of(level: &str) -> Option<i64> {
    let (head, n) = level.rsplit_once('_')?;
    if head.is_empty() {
        return None;
    }
    match n {
        "2" => Some(2),
        "3" => Some(3),
        _ => None,
    }
}

/// A `pyspark.StorageLevel` expression with replication > 1 → (display name, replication).
fn replicated_level(ch: &Checker, e: &Expr, hops: Hops) -> Option<(String, i64)> {
    match e {
        Expr::Attribute(a) => {
            let q = ch.model.qualified_name(e)?;
            let (cls, attr) = q.rsplit_once('.')?;
            if !is_storage_level_class(cls) || attr != a.attr.as_str() {
                return None;
            }
            replication_of(attr).map(|n| (attr.to_string(), n))
        }
        Expr::Call(call) => {
            let q = ch.model.qualified_name(&call.func)?;
            if !is_storage_level_class(&q) {
                return None;
            }
            let n = i64::try_from(ch.model.const_int(call_arg(call, 4, "replication")?)?).ok()?;
            (n > 1).then(|| (format!("StorageLevel(..., replication={n})"), n))
        }
        Expr::Name(_) => {
            let (v, h) = ch.model.hop_one(e, hops)?;
            replicated_level(ch, v, h)
        }
        _ => None,
    }
}

fn is_storage_level_class(q: &str) -> bool {
    matches!(
        q,
        "pyspark.StorageLevel" | "pyspark.storagelevel.StorageLevel"
    )
}

/// `CACHE [LAZY] TABLE t OPTIONS ('storageLevel' 'MEMORY_AND_DISK_2')` → the
/// replicated level.
fn sql_cache_level(q: &SqlText) -> Option<String> {
    if q.keyword() != Some("CACHE") {
        return None;
    }
    let level = q
        .properties("OPTIONS")
        .into_iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("storageLevel"))?
        .1
        .to_ascii_uppercase();
    replication_of(&level).map(|_| level)
}

#[cfg(test)]
mod prf029_tests {
    use super::{replication_of, sql_cache_level};
    use sparklint_semantic::SqlText;

    #[test]
    fn replicated_levels() {
        assert_eq!(replication_of("MEMORY_AND_DISK_2"), Some(2));
        assert_eq!(replication_of("DISK_ONLY_3"), Some(3));
        assert_eq!(replication_of("MEMORY_AND_DISK"), None);
        assert_eq!(replication_of("_2"), None);
        assert_eq!(replication_of("OFF_HEAP"), None);
    }

    #[test]
    fn sql_cache_options() {
        let l = |s: &str| sql_cache_level(&SqlText::new(s));
        assert_eq!(
            l("CACHE TABLE t OPTIONS ('storageLevel' 'MEMORY_AND_DISK_2')").as_deref(),
            Some("MEMORY_AND_DISK_2")
        );
        assert_eq!(
            l("cache lazy table t options ('storagelevel'='disk_only_3') select * from s")
                .as_deref(),
            Some("DISK_ONLY_3")
        );
        assert_eq!(
            l("  CACHE TABLE t OPTIONS (\"storageLevel\" \"MEMORY_ONLY_2\")").as_deref(),
            Some("MEMORY_ONLY_2")
        );
        assert_eq!(
            l("CACHE TABLE t OPTIONS ('storageLevel' 'MEMORY_AND_DISK')"),
            None
        );
        assert_eq!(l("CACHE TABLE t"), None);
        assert_eq!(l("SELECT 'storageLevel' AS k, 'MEMORY_ONLY_2' AS v"), None);
        assert_eq!(l("UNCACHE TABLE t"), None);
    }
}
