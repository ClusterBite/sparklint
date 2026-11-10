//! DBX — platform compatibility: DBX001 spark-context-access, DBX002 rdd-access,
//! DBX003 input-file-name, DBX016 list-tables-existence-check.

use ruff_python_ast::{self as ast, Expr, ExprContext, Stmt};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::{SqlText, Ty, call_arg, static_text, str_literal};

use super::Rule;
use super::actions::{delete_stmt_fix, method_range};
use super::helpers::*;
use crate::checker::{Checker, Node};
use crate::diagnostic::{Diagnostic, Edit, Fix, Severity};

/// DBX001/002/003/016 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("DBX001").expr(dbx001_expr),
    Rule::new("DBX002").expr(dbx002_expr),
    Rule::new("DBX003").call(input_file_name),
    Rule::new("DBX016")
        .expr(list_tables_existence_check)
        .stmt(list_tables_existence_loop)
        .call(show_tables_filter),
];

fn dbx001_expr(ch: &mut Checker, expr: &Expr) {
    if matches!(expr, Expr::Call(_) | Expr::Attribute(_) | Expr::Name(_)) {
        spark_context_access(ch, expr);
    }
}

fn dbx002_expr(ch: &mut Checker, expr: &Expr) {
    if let Expr::Attribute(a) = expr {
        rdd_access(ch, a, expr);
    }
}

// ------------------------------------------------------------------ DBX001

const CONNECT_MSG: &str = "is unavailable on Spark Connect and serverless (and blocked on standard access mode before DBR 14)";

fn report_context(ch: &mut Checker, what: &str, range: TextRange, fix: Option<Fix>) {
    let mut d = Diagnostic::new("DBX001", format!("{what} {CONNECT_MSG}."), range).with_help(
        "Use SparkSession APIs: spark.createDataFrame(rows) for parallelize, spark.conf.get(k) for getConf, F.broadcast(df) for broadcast joins.",
    );
    if let Some(f) = fix {
        d = d.with_fix(f);
    }
    ch.report(d);
}

/// Fixes for common `<ctx>.X` uses: `ctx.setLogLevel(..)` statements and
/// `ctx.applicationId`. `session` is the source of the SparkSession expression.
fn context_use_fix(ch: &Checker, session: Option<&str>) -> Option<Fix> {
    let Some(Node::Expr(Expr::Attribute(pa))) = ch.parent() else {
        return None;
    };
    match pa.attr.as_str() {
        "applicationId" => {
            let s = session?;
            let text = format!("{s}.conf.get(\"spark.app.id\")");
            Some(Fix::unsafe_(
                format!("Replace with `{text}`"),
                vec![Edit::replace(pa.range, text)],
            ))
        }
        "setLogLevel" => {
            let Some(Node::Expr(Expr::Call(c))) = ch.ancestor(1) else {
                return None;
            };
            // The call must be a whole statement.
            match ch.ancestor(2) {
                Some(Node::Stmt(stmt @ Stmt::Expr(se))) if se.value.range() == c.range() => {
                    Some(delete_stmt_fix(ch, stmt))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

pub fn spark_context_access(ch: &mut Checker, expr: &Expr) {
    match expr {
        Expr::Attribute(a) if a.attr.as_str() == "sparkContext" => {
            if !ch.model.ty_of(&a.value).ty.is_session() {
                return;
            }
            let session = paren_src(ch, &a.value);
            let fix = context_use_fix(ch, Some(&session));
            report_context(
                ch,
                &format!("{}.sparkContext", short(ch, &a.value)),
                a.range,
                fix,
            );
        }
        Expr::Name(n) if n.ctx == ExprContext::Load => {
            let name = n.id.as_str();
            if !matches!(name, "sc" | "sqlContext") {
                return;
            }
            if !ch.model.use_site(n).is_some_and(|u| u.reaching.is_empty()) {
                return;
            }
            let t = ch.model.ty_of(expr).ty;
            if !matches!(t, Ty::SparkContext | Ty::SqlContext) {
                return;
            }
            let fix = if name == "sc" && ch.is_notebook() {
                context_use_fix(ch, Some("spark"))
            } else {
                None
            };
            report_context(ch, &format!("The `{name}` global"), n.range, fix);
        }
        Expr::Call(c) => {
            let Some(q) = ch.model.qualified_name(&c.func) else {
                return;
            };
            let what = match q.as_str() {
                "pyspark.SparkContext" | "pyspark.SparkContext.getOrCreate" => "SparkContext",
                "pyspark.sql.SQLContext" | "pyspark.sql.SQLContext.getOrCreate" => "SQLContext",
                "pyspark.sql.HiveContext" => "HiveContext",
                _ => return,
            };
            report_context(ch, what, expr.range(), None);
        }
        _ => {}
    }
}

// ------------------------------------------------------------------ DBX002

pub fn rdd_access(ch: &mut Checker, a: &ast::ExprAttribute, expr: &Expr) {
    if a.attr.as_str() != "rdd" || a.ctx != ExprContext::Load || !ch.model.is_dataframe(&a.value) {
        return;
    }
    let base = paren_src(ch, &a.value);
    let mut fix = None;
    if let Some(Node::Expr(Expr::Attribute(pa))) = ch.parent()
        && let Some(Node::Expr(Expr::Call(pc))) = ch.ancestor(1)
        && pc.func.range() == pa.range
    {
        match pa.attr.as_str() {
            "isEmpty" if has_no_args(pc) && ch.settings.spark_version >= (3, 3) => {
                let text = format!("{base}.isEmpty()");
                fix = Some(Fix::unsafe_(
                    format!("Replace with `{text}`"),
                    vec![Edit::replace(pc.range(), text)],
                ));
            }
            "map" => {
                if let [Expr::Lambda(l)] = pc.arguments.args.as_slice()
                    && pc.arguments.keywords.is_empty()
                    && let Some(params) = &l.parameters
                    && params.len() == 1
                    && let Some(p) = params.iter_non_variadic_params().next()
                    && let Some(Node::Expr(Expr::Attribute(ca))) = ch.ancestor(2)
                    && ca.attr.as_str() == "collect"
                    && let Some(Node::Expr(Expr::Call(cc))) = ch.ancestor(3)
                    && cc.func.range() == ca.range
                    && has_no_args(cc)
                {
                    let body = ch.slice(l.body.range());
                    let pname = p.parameter.name.as_str();
                    if !body.contains('\n') {
                        let text = format!("[{body} for {pname} in {base}.collect()]");
                        fix = Some(Fix::unsafe_(
                            format!("Replace with `{text}`"),
                            vec![Edit::replace(cc.range(), text)],
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    let mut d = Diagnostic::new(
        "DBX002",
        format!(
            "{}.rdd is not supported on Spark Connect, serverless or standard access mode.",
            short(ch, &a.value)
        ),
        expr.range(),
    )
    .with_help("Use DataFrame APIs: df.isEmpty(), df.collect(), mapInPandas/applyInPandas for row-wise Python.");
    if let Some(f) = fix {
        d = d.with_fix(f);
    }
    ch.report(d);
}

// ------------------------------------------------------------------ DBX003

const IFN_MSG: &str = "input_file_name() is not supported on Unity Catalog standard/serverless compute (and is deprecated) — use the _metadata.file_path column.";

pub fn input_file_name(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if ch.model.is_spark_function(&c.func, "input_file_name") {
        let mut d = Diagnostic::new("DBX003", IFN_MSG, expr.range());
        if let Expr::Attribute(fa) = &*c.func {
            let text = format!(
                "{}.col(\"_metadata.file_path\")",
                ch.slice(fa.value.range())
            );
            d = d.with_fix(Fix::unsafe_(
                format!("Replace with `{text}`"),
                vec![Edit::replace(expr.range(), text)],
            ));
        }
        ch.report(d);
        return;
    }
    // SQL text: spark.sql(...), F.expr(...), selectExpr(...), filter("...")
    let sql_args: Vec<&Expr> = if ch.model.is_spark_function(&c.func, "expr") {
        c.arguments.args.iter().take(1).collect()
    } else if let Some(sc) = ch.model.spark_call(expr) {
        let rt = sc.class();
        match sc.method {
            "sql" if rt.is_session() => c.arguments.args.iter().take(1).collect(),
            "selectExpr" | "filter" | "where" if rt == Ty::DataFrame => {
                c.arguments.args.iter().collect()
            }
            _ => Vec::new(),
        }
    } else {
        Vec::new()
    };
    for a in sql_args {
        if static_text(a).is_some_and(|t| SqlText::new(&t).calls("input_file_name")) {
            ch.report(Diagnostic::new("DBX003", IFN_MSG, a.range()));
        }
    }
}

// ------------------------------------------------------------------ DBX016

/// What a full catalog listing enumerates.
#[derive(Clone, Copy, PartialEq)]
enum Listed {
    Tables,
    Schemas,
}

impl Listed {
    fn exists_api(self) -> &'static str {
        match self {
            Listed::Tables => "spark.catalog.tableExists(\"catalog.schema.table\")",
            Listed::Schemas => "spark.catalog.databaseExists(\"catalog.schema\")",
        }
    }
}

/// Fields of a listing row that hold the listed object's name.
const NAME_FIELDS: &[&str] = &["name", "tableName", "databaseName", "namespace"];

/// `spark.catalog.listTables(db)` / `listDatabases()` without a pattern, or
/// `spark.sql("SHOW TABLES [IN db]")` (no pattern), optionally `.select(..)`, and
/// `.collect()` / `.toLocalIterator()` on it. Returns the listed kind and the range of
/// the listing call.
fn listing(ch: &Checker, e: &Expr) -> Option<(Listed, TextRange)> {
    if let Some(sc) = ch
        .model
        .spark_call_named(e, &["listTables", "listDatabases"])
    {
        if sc.class() != Ty::Catalog {
            return None;
        }
        let (kind, pat_pos) = if sc.method == "listTables" {
            (Listed::Tables, 1)
        } else {
            (Listed::Schemas, 0)
        };
        if call_arg(sc.call, pat_pos, "pattern").is_some()
            || sc.call.arguments.args.iter().any(|a| a.is_starred_expr())
            || sc.call.arguments.keywords.iter().any(|k| k.arg.is_none())
        {
            return None;
        }
        return Some((kind, method_range(e)));
    }
    if let Some(sc) = ch
        .model
        .spark_call_named(e, &["collect", "toLocalIterator"])
        && sc.is_dataframe()
    {
        return show_listing(ch, sc.receiver);
    }
    None
}

/// `spark.sql("SHOW TABLES ...")` (no pattern), through `select(..)` calls.
fn show_listing(ch: &Checker, mut e: &Expr) -> Option<(Listed, TextRange)> {
    for _ in 0..8 {
        let sc = ch.model.spark_call_named(e, &["select", "sql"])?;
        if sc.method == "select" {
            if !sc.is_dataframe() {
                return None;
            }
            e = sc.receiver;
            continue;
        }
        if !sc.class().is_session() {
            return None;
        }
        let q = ch.model.sql_query(sc.call)?;
        return show_kind(&q).map(|k| (k, method_range(e)));
    }
    None
}

/// `SHOW TABLES [IN|FROM name]` / `SHOW SCHEMAS|DATABASES` with nothing after it
/// (a `LIKE` / pattern makes the server filter, which is fine).
fn show_kind(q: &SqlText) -> Option<Listed> {
    let words = q.words();
    if words.first() != Some(&"SHOW") {
        return None;
    }
    let kind = match words.get(1).copied() {
        Some("TABLES") => Listed::Tables,
        Some("SCHEMAS" | "DATABASES") => Listed::Schemas,
        _ => return None,
    };
    match &words[2..] {
        [] => Some(kind),
        [kw, _] if *kw == "IN" || *kw == "FROM" => Some(kind),
        _ => None,
    }
}

/// `t.name`, `t.tableName`, `t["tableName"]`, optionally `.lower()`/`.upper()`/
/// `.strip()`/`.casefold()` on it, where `t` is one of `targets`.
fn is_name_ref(e: &Expr, targets: &[&str]) -> bool {
    match e {
        Expr::Attribute(a) => {
            NAME_FIELDS.contains(&a.attr.as_str())
                && matches!(&*a.value, Expr::Name(n) if targets.contains(&n.id.as_str()))
        }
        Expr::Subscript(s) => {
            matches!(&*s.value, Expr::Name(n) if targets.contains(&n.id.as_str()))
                && str_literal(&s.slice).is_some_and(|k| NAME_FIELDS.contains(&k))
        }
        Expr::Call(c) if has_no_args(c) => match &*c.func {
            Expr::Attribute(a)
                if matches!(a.attr.as_str(), "lower" | "upper" | "strip" | "casefold") =>
            {
                is_name_ref(&a.value, targets)
            }
            _ => false,
        },
        _ => false,
    }
}

/// `e` contains `<name ref> == <something else>` (either order).
fn has_name_equality(e: &Expr, targets: &[&str]) -> bool {
    let mut found = false;
    walk_exprs(e, &mut |x| {
        let Expr::Compare(cmp) = x else { return };
        let Some((l, ast::CmpOp::Eq, r)) = cmp.as_single() else {
            return;
        };
        if is_name_ref(l, targets) != is_name_ref(r, targets) {
            found = true;
        }
    });
    found
}

fn comp_targets(t: &Expr) -> Vec<&str> {
    match t {
        Expr::Name(n) => vec![n.id.as_str()],
        _ => Vec::new(),
    }
}

fn report_listing_lookup(ch: &mut Checker, kind: Listed, range: TextRange) {
    let what = match kind {
        Listed::Tables => "table",
        Listed::Schemas => "schema",
    };
    let mut d = Diagnostic::new(
        "DBX016",
        format!(
            "Existence check for one {what} by listing every {what}: the driver fetches the whole listing (one metastore round trip per page on Unity Catalog) to test a single name."
        ),
        range,
    )
    .with_help(format!(
        "Use {} — one lookup. A pattern argument (listTables(db, pattern) / SHOW TABLES LIKE) also keeps the listing small.",
        kind.exists_api()
    ));
    // The listing call is a comprehension's iterable (evaluated once), so only
    // enclosing loops count.
    if ch.model.in_loop(&range) {
        d.message
            .push_str(" Inside a loop, the full listing is fetched on every iteration.");
        d = d.with_severity(Severity::Warning);
    }
    ch.report(d);
}

/// The single generator of a comprehension over a full listing, with its kind.
fn comp_over_listing<'e>(
    ch: &Checker,
    gens: &'e [ast::Comprehension],
) -> Option<(&'e ast::Comprehension, Listed, TextRange)> {
    let [g] = gens else { return None };
    let (k, r) = listing(ch, &g.iter)?;
    Some((g, k, r))
}

pub fn list_tables_existence_check(ch: &mut Checker, expr: &Expr) {
    match expr {
        // any(t.name == x for t in spark.catalog.listTables(db)),
        // [t for t in spark.catalog.listTables(db) if t.name == x]
        Expr::ListComp(ast::ExprListComp {
            elt, generators, ..
        })
        | Expr::SetComp(ast::ExprSetComp {
            elt, generators, ..
        })
        | Expr::Generator(ast::ExprGenerator {
            elt, generators, ..
        }) => {
            let Some((g, kind, range)) = comp_over_listing(ch, generators) else {
                return;
            };
            let targets = comp_targets(&g.target);
            if targets.is_empty() {
                return;
            }
            if has_name_equality(elt, &targets)
                || g.ifs.iter().any(|i| has_name_equality(i, &targets))
            {
                report_listing_lookup(ch, kind, range);
            }
        }
        // x in [t.name for t in spark.catalog.listTables(db)]
        Expr::Compare(cmp) => {
            let Some((_, ast::CmpOp::In | ast::CmpOp::NotIn, rhs)) = cmp.as_single() else {
                return;
            };
            let hit = match rhs {
                Expr::ListComp(ast::ExprListComp {
                    elt, generators, ..
                })
                | Expr::SetComp(ast::ExprSetComp {
                    elt, generators, ..
                })
                | Expr::Generator(ast::ExprGenerator {
                    elt, generators, ..
                }) => comp_over_listing(ch, generators.as_slice()).and_then(|(g, k, r)| {
                    let targets = comp_targets(&g.target);
                    // The comprehension itself already reports an equality lookup.
                    let own = g.ifs.iter().any(|i| has_name_equality(i, &targets))
                        || has_name_equality(elt.as_ref(), &targets);
                    (!own && is_name_ref(elt.as_ref(), &targets)).then_some((k, r))
                }),
                _ => listing(ch, rhs),
            };
            if let Some((kind, range)) = hit {
                report_listing_lookup(ch, kind, range);
            }
        }
        _ => {}
    }
}

/// `for t in spark.catalog.listTables(db): if t.name == x: ...`
pub fn list_tables_existence_loop(ch: &mut Checker, stmt: &Stmt) {
    let Stmt::For(f) = stmt else { return };
    let Some((kind, range)) = listing(ch, &f.iter) else {
        return;
    };
    let targets = comp_targets(&f.target);
    if targets.is_empty() {
        return;
    }
    let hit = f.body.iter().any(|s| match s {
        Stmt::If(i) => has_name_equality(&i.test, &targets),
        _ => false,
    });
    if hit {
        report_listing_lookup(ch, kind, range);
    }
}

/// `spark.sql("SHOW TABLES IN db").filter(col("tableName") == x)` /
/// `.where("tableName = 'x'")`.
pub fn show_tables_filter(ch: &mut Checker, _c: &ast::ExprCall, expr: &Expr) {
    let Some(sc) = ch.model.spark_call_named(expr, &["filter", "where"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let Some((kind, _)) = show_listing(ch, sc.receiver) else {
        return;
    };
    let Some(cond) = sc.args().first() else {
        return;
    };
    let hit = if let Some(text) = static_text(cond) {
        sql_name_equality(&text)
    } else {
        let mut found = false;
        walk_exprs(cond, &mut |x| {
            let Expr::Compare(cmp) = x else { return };
            if let Some((l, ast::CmpOp::Eq, r)) = cmp.as_single() {
                let l = ch.slice(l.range());
                let r = ch.slice(r.range());
                if mentions_name_field(l) != mentions_name_field(r) {
                    found = true;
                }
            }
        });
        found
    };
    if hit {
        report_listing_lookup(ch, kind, method_range(expr));
    }
}

fn mentions_name_field(src: &str) -> bool {
    ["tablename", "databasename", "namespace"]
        .iter()
        .any(|f| src.to_ascii_lowercase().contains(f))
}

/// `tableName = 'x'` / `tableName == 'x'` in a SQL filter string.
fn sql_name_equality(sql: &str) -> bool {
    let s = sql.to_ascii_lowercase();
    ["tablename", "databasename", "namespace"].iter().any(|f| {
        s.match_indices(f).any(|(i, _)| {
            let before_ok = s[..i]
                .chars()
                .next_back()
                .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
            let rest = s[i + f.len()..].trim_start_matches(['`', ' ']);
            before_ok && rest.starts_with('=') && !rest.starts_with("=>")
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_listing_kinds() {
        assert!(show_kind(&SqlText::new("SHOW TABLES")) == Some(Listed::Tables));
        assert!(show_kind(&SqlText::new("  show tables in sales;")) == Some(Listed::Tables));
        assert!(show_kind(&SqlText::new("SHOW TABLES FROM main.sales")) == Some(Listed::Tables));
        assert!(show_kind(&SqlText::new("SHOW SCHEMAS")) == Some(Listed::Schemas));
        assert!(show_kind(&SqlText::new("SHOW DATABASES")) == Some(Listed::Schemas));
        assert!(show_kind(&SqlText::new("SHOW TABLES IN `sales` -- all")) == Some(Listed::Tables));
        // Patterns filter in the metastore; other SHOW statements are not listings.
        assert!(show_kind(&SqlText::new("SHOW TABLES IN sales LIKE 'ord*'")).is_none());
        assert!(show_kind(&SqlText::new("SHOW TABLES 'ord*'")).is_none());
        assert!(show_kind(&SqlText::new("SHOW COLUMNS IN sales.orders")).is_none());
        assert!(show_kind(&SqlText::new("SELECT * FROM t")).is_none());
    }

    #[test]
    fn sql_name_filters() {
        assert!(sql_name_equality("tableName = 'orders'"));
        assert!(sql_name_equality("`tableName`='orders'"));
        assert!(sql_name_equality("namespace == 'sales'"));
        assert!(!sql_name_equality("tableName != 'orders'"));
        assert!(!sql_name_equality("tableName LIKE 'ord%'"));
        assert!(!sql_name_equality("isTemporary = false"));
        assert!(!sql_name_equality("mytableName = 'x'"));
    }
}
