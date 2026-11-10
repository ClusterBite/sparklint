//! PRF — plan & shuffle performance: PRF001, PRF003, PRF005, PRF008, PRF029, PRF031.

use ruff_python_ast::{self as ast, Expr, ExprContext, Stmt};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::{BindingKind, LoopInfo, LoopKind, SqlText, Ty, call_arg, int_literal};

use super::CacheCandidate;
use super::Rule;
use super::actions::method_range;
use super::helpers::*;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Diagnostic, Edit, Fix, Severity};

/// PRF001/002/003/005/008/029/031 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("PRF001")
        .stmt(prf001_stmt)
        .call(withcolumn_reduce),
    Rule::new("PRF002").stmt(prf002_stmt),
    Rule::new("PRF003").call(cartesian_join),
    Rule::new("PRF005").call(global_window),
    Rule::new("PRF031").call(running_collect_list),
    Rule::new("PRF029").call(replicated_storage_level),
    Rule::new("PRF008").finish(finish_cache),
];

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
    let loops: Vec<_> = ch
        .model
        .loops_at(stmt.start())
        .into_iter()
        .filter(|l| l.kind != LoopKind::Comprehension)
        .collect();
    let inner = (*loops.last()?).clone();
    // loop-carried: a binding of the name made in this loop body at or after this
    // statement reaches the receiver (the previous iteration's value flows in).
    let carried = ch.model.reaching(root).iter().any(|b| {
        let b = ch.model.binding(*b);
        inner.body.contains_range(b.stmt_range) && b.stmt_range.start() >= stmt.start()
    });
    if !carried {
        return None;
    }
    Some((call, m, inner))
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
    let mut d = Diagnostic::new(
        "PRF001",
        ".withColumn() in a loop adds one projection per iteration — use withColumns()/select().",
        method_range(wc),
    )
    .with_site(inner.range)
    .with_help("Build all expressions first, then apply them once: df.withColumns({name: expr, ...}) (Spark >= 3.3) or df.select(\"*\", *exprs).");
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
    if [key, val, tgt, iter].iter().any(|s| s.contains('\n')) {
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
    let mut d = Diagnostic::new(
        "PRF002",
        format!(".{m}() in a loop adds one plan node per iteration — batch it into one call."),
        method_range(call),
    )
    .with_site(inner.range)
    .with_help("Build the full list first: df.withColumnsRenamed({old: new, ...}) (Spark >= 3.4), df.drop(*cols), or one df.select(...).");
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
            if [o, n].iter().any(|s| s.contains('\n')) {
                return None;
            }
            format!("{name} = {name}.withColumnsRenamed({{{o}: {n} for {tgt} in {iter}}})")
        }
        ("drop", [Expr::Name(col)]) if matches!(&*f.target, Expr::Name(t) if t.id == col.id) => {
            format!("{name} = {name}.drop(*{iter})")
        }
        _ => return None,
    };
    if text.contains('\n') {
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
    ch.report(Diagnostic::new(
        "PRF001",
        "functools.reduce(...withColumn...) adds one projection per element — use withColumns()/select().",
        expr.range(),
    ));
}

// ------------------------------------------------------------------ PRF003

pub fn cartesian_join(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["crossJoin", "join"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let (m, rt) = (sc.method, sc.receiver_ty);
    let Some(other) = call_arg(c, 0, "other") else {
        return;
    };
    if m == "join" {
        match call_arg(c, 1, "on") {
            None => {}
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
    if rt.bounded() == Some(1) || ot.bounded() == Some(1) {
        return; // scalar broadcast idiom
    }
    let small = rt.bounded().is_some() || ot.bounded().is_some();
    let what = if m == "crossJoin" {
        "crossJoin()"
    } else {
        "join() without a condition"
    };
    let mut d = Diagnostic::new(
        "PRF003",
        format!("Cartesian product: {what} produces |A|×|B| rows."),
        method_range(expr),
    )
    .with_help("Join on a key (df.join(other, \"key\", \"inner\")); for a scalar, aggregate it first so one side has one row.");
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
    if t.ty != Ty::WindowSpec || t.facts.win_partitioned {
        return;
    }
    ch.report(
        Diagnostic::new(
            "PRF005",
            "Window without partitionBy() moves all rows into a single partition.",
            method_range(expr),
        )
        .with_help("Add Window.partitionBy(<key>) — or use df.agg(...) for global totals."),
    );
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
/// are not reported.
pub fn running_collect_list(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
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
    let mut msg = format!(
        "{name}() over a running window (ordered, frame from the partition start to the current row) gives every row its own growing array: O(N²) elements per partition."
    );
    if !f.win_partitioned {
        msg.push_str(" The window has no partitionBy(), so all rows land in one partition.");
    }
    ch.report(
        Diagnostic::new("PRF031", msg, method_range(expr)).with_help(format!(
            "For all values per key use df.groupBy(k).agg(F.sort_array(F.{name}(F.struct(order_col, value)))); for the last n values bound the frame, e.g. .rowsBetween(-n, Window.currentRow)."
        )),
    );
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
        "persist" if sc.is_dataframe() || sc.class() == Ty::Rdd => {
            call_arg(c, 0, "storageLevel").and_then(|a| replicated_level(ch, a, 0))
        }
        "cacheTable" if sc.class() == Ty::Catalog => {
            call_arg(c, 1, "storageLevel").and_then(|a| replicated_level(ch, a, 0))
        }
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
    let mut msg = format!(
        "Storage level {name} keeps every cached partition {times} (extra executor memory/disk and a network copy of each block); recomputation already makes cached data fault-tolerant."
    );
    if ch.model.in_loop(expr) {
        msg.push_str(" Inside a loop it is paid on every iteration.");
    }
    let base = name
        .strip_suffix("_2")
        .or_else(|| name.strip_suffix("_3"))
        .filter(|b| !b.is_empty() && !b.starts_with("StorageLevel("))
        .unwrap_or("MEMORY_AND_DISK");
    ch.report(
        Diagnostic::new("PRF029", msg, method_range(expr)).with_help(format!(
            "Use StorageLevel.{base} (or plain .cache()); keep replication only for low-latency serving where losing an executor must not trigger recomputation."
        )),
    );
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
fn replicated_level(ch: &Checker, e: &Expr, depth: u32) -> Option<(String, i64)> {
    if depth > 4 {
        return None;
    }
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
            let n = int_literal(call_arg(call, 4, "replication")?)?;
            (n > 1).then(|| (format!("StorageLevel(..., replication={n})"), n))
        }
        Expr::Name(n) => {
            let b = ch.model.single_binding(n)?;
            let v = ch.model.binding(b).value?;
            (v.range() != e.range())
                .then(|| replicated_level(ch, v, depth + 1))
                .flatten()
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

// ------------------------------------------------------------------ PRF008

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
            !super::lineage::cache_fate(ch, **id, recv, b.stmt_range.start()).1
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
