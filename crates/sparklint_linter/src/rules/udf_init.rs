//! UDF011 pandas-udf-per-batch-init, UDF012 per-row-external-connection.
//!
//! Both rules look for expensive set-up (model loads, clients, connections) inside a
//! function Spark calls many times on the executors: once per Arrow batch / group
//! (UDF011) or once per row (UDF012). One level of same-file helper calls is
//! followed; helpers decorated with `functools.lru_cache`/`cache` and assignments to
//! module-level caches (`global _m; _m = load()`, `_CACHE[k] = load()`) are not flagged.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, Expr, Stmt};
use ruff_text_size::{Ranged, TextRange};
use rustc_hash::FxHashSet;
use sparklint_semantic::{Ty, call_arg};

use super::Rule;
use super::helpers::*;
use super::udf::is_udf_factory;
use crate::checker::Checker;
use crate::diagnostic::Diagnostic;

/// UDF011/012 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("UDF011").stmt(udf011_stmt).call(pandas_udf_call),
    Rule::new("UDF012").stmt(udf012_stmt).call(row_sink_call),
];

fn udf011_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::FunctionDef(f) = stmt {
        pandas_udf_decorator(ch, f);
    }
}

fn udf012_stmt(ch: &mut Checker, stmt: &Stmt) {
    match stmt {
        Stmt::FunctionDef(f) => row_udf_decorator(ch, f),
        Stmt::ClassDef(c) => foreach_writer_classdef(ch, c),
        _ => {}
    }
}

// ------------------------------------------------------------------ API tables

fn db_connect(q: &str) -> bool {
    matches!(
        q,
        "psycopg2.connect"
            | "psycopg.connect"
            | "pyodbc.connect"
            | "pymysql.connect"
            | "mysql.connector.connect"
            | "cx_Oracle.connect"
            | "oracledb.connect"
            | "snowflake.connector.connect"
            | "databricks.sql.connect"
            | "sqlalchemy.create_engine"
            | "pymongo.MongoClient"
            | "redis.Redis"
            | "redis.StrictRedis"
            | "redis.from_url"
    )
}

/// Heavy initialisers for UDF011 (model loads, cloud clients, DB connections).
fn heavy_init(q: &str) -> bool {
    (q.starts_with("mlflow.") && q.ends_with(".load_model"))
        || q.ends_with(".from_pretrained")
        || matches!(
            q,
            "joblib.load"
                | "pickle.load"
                | "cloudpickle.load"
                | "dill.load"
                | "torch.load"
                | "transformers.pipeline"
                | "tensorflow.keras.models.load_model"
                | "keras.models.load_model"
                | "keras.saving.load_model"
                | "boto3.client"
                | "boto3.resource"
        )
        || db_connect(q)
}

/// Network clients / connections for UDF012.
fn external_conn(q: &str) -> bool {
    if let Some(m) = q.strip_prefix("requests.") {
        return matches!(
            m,
            "get"
                | "post"
                | "put"
                | "patch"
                | "delete"
                | "head"
                | "request"
                | "Session"
                | "session"
        );
    }
    if let Some(m) = q.strip_prefix("httpx.") {
        return matches!(
            m,
            "get"
                | "post"
                | "put"
                | "patch"
                | "delete"
                | "head"
                | "request"
                | "stream"
                | "Client"
                | "AsyncClient"
        );
    }
    matches!(
        q,
        "urllib.request.urlopen"
            | "urllib3.PoolManager"
            | "boto3.client"
            | "boto3.resource"
            | "socket.socket"
            | "socket.create_connection"
    ) || db_connect(q)
}

// ------------------------------------------------------------------ body scan

/// A function Spark calls repeatedly.
#[derive(Clone, Copy)]
enum Target<'a, 't> {
    Def(&'a ast::StmtFunctionDef),
    Lambda(&'t ast::ExprLambda),
}

/// Where in the body set-up is repeated.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Every statement of the body runs per call.
    Anywhere,
    /// The function receives an iterator of batches: only code inside a loop (or
    /// comprehension) over that parameter repeats.
    InsideLoopOverFirstParam,
}

struct Hit {
    range: TextRange,
    api: String,
    via: Option<String>,
}

fn first_param(f: &ast::StmtFunctionDef) -> Option<String> {
    f.parameters
        .iter_non_variadic_params()
        .next()
        .map(|p| p.parameter.name.as_str().to_string())
}

fn mentions_name(e: &Expr, name: &str) -> bool {
    let mut found = false;
    walk_exprs(e, &mut |x| {
        if matches!(x, Expr::Name(n) if n.id.as_str() == name) {
            found = true;
        }
    });
    found
}

/// Names that are local to `body` (assigned without `global`/`nonlocal`) and the
/// names declared `global`/`nonlocal`.
fn local_names(body: &[Stmt]) -> (FxHashSet<String>, FxHashSet<String>) {
    struct V {
        local: FxHashSet<String>,
        global: FxHashSet<String>,
    }
    impl V {
        fn target(&mut self, t: &Expr) {
            match t {
                Expr::Name(n) => {
                    self.local.insert(n.id.as_str().to_string());
                }
                Expr::Tuple(t) => t.elts.iter().for_each(|e| self.target(e)),
                Expr::List(t) => t.elts.iter().for_each(|e| self.target(e)),
                Expr::Starred(s) => self.target(&s.value),
                _ => {}
            }
        }
    }
    impl<'b> Visitor<'b> for V {
        fn visit_stmt(&mut self, s: &'b Stmt) {
            match s {
                Stmt::FunctionDef(f) => {
                    self.local.insert(f.name.as_str().to_string());
                    return;
                }
                Stmt::ClassDef(c) => {
                    self.local.insert(c.name.as_str().to_string());
                    return;
                }
                Stmt::Global(g) => {
                    self.global
                        .extend(g.names.iter().map(|n| n.as_str().to_string()));
                }
                Stmt::Nonlocal(g) => {
                    self.global
                        .extend(g.names.iter().map(|n| n.as_str().to_string()));
                }
                Stmt::Assign(a) => a.targets.iter().for_each(|t| self.target(t)),
                Stmt::AnnAssign(a) => self.target(&a.target),
                Stmt::AugAssign(a) => self.target(&a.target),
                Stmt::For(f) => self.target(&f.target),
                Stmt::With(w) => {
                    for i in &w.items {
                        if let Some(v) = &i.optional_vars {
                            self.target(v);
                        }
                    }
                }
                Stmt::Import(i) => {
                    for a in &i.names {
                        let n = a.asname.as_ref().unwrap_or(&a.name);
                        let n = n.as_str().split('.').next().unwrap_or_default();
                        self.local.insert(n.to_string());
                    }
                }
                Stmt::ImportFrom(i) => {
                    for a in &i.names {
                        let n = a.asname.as_ref().unwrap_or(&a.name);
                        self.local.insert(n.as_str().to_string());
                    }
                }
                _ => {}
            }
            visitor::walk_stmt(self, s);
        }
        fn visit_expr(&mut self, _: &'b Expr) {}
    }
    let mut v = V {
        local: FxHashSet::default(),
        global: FxHashSet::default(),
    };
    v.visit_body(body);
    for g in &v.global {
        v.local.remove(g);
    }
    (v.local, v.global)
}

struct Scan<'c, 'a> {
    ch: &'c Checker<'a>,
    is_api: fn(&str) -> bool,
    mode: Mode,
    loop_param: Option<String>,
    /// Inside the repeated region (always true for `Mode::Anywhere`).
    active: bool,
    /// Helper depth (0 = the target itself).
    depth: u8,
    params: FxHashSet<String>,
    locals: FxHashSet<String>,
    globals: FxHashSet<String>,
    hits: Vec<Hit>,
}

impl<'c, 'a> Scan<'c, 'a> {
    fn new(ch: &'c Checker<'a>, is_api: fn(&str) -> bool, mode: Mode, depth: u8) -> Self {
        Self {
            ch,
            is_api,
            mode,
            loop_param: None,
            active: mode == Mode::Anywhere,
            depth,
            params: FxHashSet::default(),
            locals: FxHashSet::default(),
            globals: FxHashSet::default(),
            hits: Vec::new(),
        }
    }

    fn run_def(mut self, f: &ast::StmtFunctionDef) -> Vec<Hit> {
        self.params = f
            .parameters
            .iter()
            .map(|p| p.name().as_str().to_string())
            .collect();
        if self.mode == Mode::InsideLoopOverFirstParam {
            self.loop_param = first_param(f);
            if self.loop_param.is_none() {
                return Vec::new();
            }
        }
        let (locals, globals) = local_names(&f.body);
        self.locals = locals;
        self.globals = globals;
        for s in &f.body {
            self.visit_stmt(s);
        }
        self.hits
    }

    fn run_lambda(mut self, l: &ast::ExprLambda) -> Vec<Hit> {
        if let Some(p) = &l.parameters {
            self.params = p.iter().map(|p| p.name().as_str().to_string()).collect();
        }
        if self.mode == Mode::InsideLoopOverFirstParam {
            self.loop_param = l
                .parameters
                .as_ref()
                .and_then(|p| p.iter_non_variadic_params().next())
                .map(|p| p.parameter.name.as_str().to_string());
            if self.loop_param.is_none() {
                return Vec::new();
            }
        }
        self.visit_expr(&l.body);
        self.hits
    }

    /// An assignment target that outlives the call (module-level cache).
    fn non_local_target(&self, t: &Expr) -> bool {
        match t {
            Expr::Name(n) => self.globals.contains(n.id.as_str()),
            Expr::Subscript(_) | Expr::Attribute(_) => root_name(t).is_some_and(|n| {
                let id = n.id.as_str();
                self.globals.contains(id)
                    || (!self.locals.contains(id) && !self.params.contains(id))
            }),
            _ => false,
        }
    }

    fn over_loop_param(&self, iter: &Expr) -> bool {
        self.loop_param
            .as_deref()
            .is_some_and(|p| mentions_name(iter, p))
    }

    fn check_call(&mut self, c: &ast::ExprCall) {
        if !self.active {
            return;
        }
        let ch = self.ch;
        if let Some(q) = ch.model.qualified_name(&c.func) {
            if (self.is_api)(&q) {
                self.hits.push(Hit {
                    range: c.range(),
                    api: q,
                    via: None,
                });
            }
            return;
        }
        // One level into same-file helpers that are not memoised.
        if self.depth > 0 {
            return;
        }
        let Expr::Name(n) = &*c.func else { return };
        let Some(b) = ch.model.single_binding(n) else {
            return;
        };
        let b = ch.model.binding(b);
        let Some(def) = ch.fn_defs.get(&b.range.start()).copied() else {
            return;
        };
        if is_memoised(ch, def) {
            return;
        }
        let inner = Scan::new(ch, self.is_api, Mode::Anywhere, 1).run_def(def);
        if let Some(h) = inner.into_iter().next() {
            self.hits.push(Hit {
                range: c.range(),
                api: h.api,
                via: Some(n.id.as_str().to_string()),
            });
        }
    }
}

impl<'b> Visitor<'b> for Scan<'_, '_> {
    fn visit_stmt(&mut self, s: &'b Stmt) {
        match s {
            Stmt::FunctionDef(_) | Stmt::ClassDef(_) => {}
            Stmt::For(f) if !self.active && self.over_loop_param(&f.iter) => {
                self.visit_expr(&f.iter);
                self.active = true;
                self.visit_body(&f.body);
                self.active = false;
                self.visit_body(&f.orelse);
            }
            Stmt::Assign(a) if a.targets.iter().all(|t| self.non_local_target(t)) => {}
            Stmt::AnnAssign(a) if self.non_local_target(&a.target) => {}
            _ => visitor::walk_stmt(self, s),
        }
    }

    fn visit_expr(&mut self, e: &'b Expr) {
        match e {
            Expr::Call(c) => {
                self.check_call(c);
                visitor::walk_expr(self, e);
            }
            Expr::Generator(ast::ExprGenerator {
                elt, generators, ..
            })
            | Expr::ListComp(ast::ExprListComp {
                elt, generators, ..
            })
            | Expr::SetComp(ast::ExprSetComp {
                elt, generators, ..
            }) if !self.active && generators.iter().any(|g| self.over_loop_param(&g.iter)) => {
                for g in generators {
                    self.visit_expr(&g.iter);
                }
                self.active = true;
                for g in generators {
                    for i in &g.ifs {
                        self.visit_expr(i);
                    }
                }
                self.visit_expr(elt);
                self.active = false;
            }
            _ => visitor::walk_expr(self, e),
        }
    }
}

fn is_memoised(ch: &Checker, f: &ast::StmtFunctionDef) -> bool {
    f.decorator_list.iter().any(|d| {
        let callee = match &d.expression {
            Expr::Call(c) => &*c.func,
            other => other,
        };
        matches!(
            ch.model.qualified_name(callee).as_deref(),
            Some("functools.lru_cache" | "functools.cache" | "functools.cached_property")
        )
    })
}

fn resolve_target<'a, 't>(ch: &Checker<'a>, e: &'t Expr) -> Option<Target<'a, 't>> {
    match e {
        Expr::Lambda(l) => Some(Target::Lambda(l)),
        Expr::Name(n) => {
            let b = ch.model.single_binding(n)?;
            let b = ch.model.binding(b);
            ch.fn_defs.get(&b.range.start()).copied().map(Target::Def)
        }
        _ => None,
    }
}

fn scan(ch: &Checker, t: Target, is_api: fn(&str) -> bool, mode: Mode) -> Vec<Hit> {
    let s = Scan::new(ch, is_api, mode, 0);
    match t {
        Target::Def(f) => s.run_def(f),
        Target::Lambda(l) => s.run_lambda(l),
    }
}

fn target_name(t: Target) -> String {
    match t {
        Target::Def(f) => f.name.as_str().to_string(),
        Target::Lambda(_) => "<lambda>".to_string(),
    }
}

fn hit_text(h: &Hit) -> String {
    match &h.via {
        Some(helper) => format!("{helper}() (which calls {}())", h.api),
        None => format!("{}()", h.api),
    }
}

fn decorator_callee(d: &ast::Decorator) -> &Expr {
    match &d.expression {
        Expr::Call(c) => &c.func,
        other => other,
    }
}

fn is_pandas_udf(ch: &Checker, e: &Expr) -> bool {
    matches!(
        ch.model.qualified_name(e).as_deref(),
        Some("pyspark.sql.functions.pandas_udf")
    )
}

// ------------------------------------------------------------------ UDF011

/// `Iterator[...]` / `Iterable[...]` annotation (also as a string).
fn is_iterator_annotation(ch: &Checker, ann: &Expr) -> bool {
    match ann {
        Expr::Subscript(s) => {
            matches!(
                ch.model.qualified_name(&s.value).as_deref(),
                Some(
                    "typing.Iterator"
                        | "typing.Iterable"
                        | "typing.Generator"
                        | "collections.abc.Iterator"
                        | "collections.abc.Iterable"
                        | "collections.abc.Generator"
                )
            ) || matches!(&*s.value, Expr::Name(n) if matches!(n.id.as_str(), "Iterator" | "Iterable"))
        }
        Expr::StringLiteral(s) => {
            let t = s.value.to_str().trim();
            ["Iterator[", "Iterable[", "typing.Iterator[", "Generator["]
                .iter()
                .any(|p| t.starts_with(p))
        }
        _ => false,
    }
}

/// Whether a pandas UDF def uses the iterator form (state initialised per partition).
fn pandas_udf_is_iterator(ch: &Checker, f: &ast::StmtFunctionDef, factory: Option<&Expr>) -> bool {
    let ann = f
        .parameters
        .iter_non_variadic_params()
        .next()
        .and_then(|p| p.parameter.annotation.as_deref())
        .is_some_and(|a| is_iterator_annotation(ch, a))
        || f.returns
            .as_deref()
            .is_some_and(|r| is_iterator_annotation(ch, r));
    if ann {
        return true;
    }
    // Legacy `functionType=PandasUDFType.SCALAR_ITER`.
    let Some(Expr::Call(c)) = factory else {
        return false;
    };
    c.arguments
        .args
        .iter()
        .chain(c.arguments.keywords.iter().map(|k| &k.value))
        .any(|a| {
            ch.model
                .qualified_name(a)
                .is_some_and(|q| q.ends_with("PandasUDFType.SCALAR_ITER"))
                || matches!(a, Expr::Attribute(at) if at.attr.as_str() == "SCALAR_ITER")
        })
}

#[derive(Clone, Copy)]
enum Kind {
    SeriesUdf,
    IterLoop(&'static str),
    PerGroup(&'static str),
}

fn report_udf011(ch: &mut Checker, hits: Vec<Hit>, kind: Kind, name: &str) {
    for h in hits {
        if !ch.state.udf_init_seen.insert((h.range, "UDF011")) {
            continue;
        }
        let what = hit_text(&h);
        let (msg, help) = match kind {
            Kind::SeriesUdf => (
                format!(
                    "{what} inside pandas UDF {name} runs for every Arrow batch (~10k rows) on every task."
                ),
                "Use the Iterator[pd.Series] -> Iterator[pd.Series] form and load once before the `for batch in it:` loop, or cache the loader at module level (functools.lru_cache).",
            ),
            Kind::IterLoop(api) => (
                format!(
                    "{what} inside the batch loop of {api} function {name} runs once per batch."
                ),
                "Move the load above the `for batch in iterator:` loop so it runs once per partition.",
            ),
            Kind::PerGroup(api) => (
                format!("{what} inside {api} function {name} runs once per group."),
                "Cache the loader at module level (functools.lru_cache / a global), or use mapInPandas and load before the batch loop.",
            ),
        };
        ch.report(Diagnostic::new("UDF011", msg, h.range).with_help(help));
    }
}

/// `@pandas_udf(...)` on a def.
pub fn pandas_udf_decorator(ch: &mut Checker, f: &ast::StmtFunctionDef) {
    let Some(dec) = f
        .decorator_list
        .iter()
        .find(|d| is_pandas_udf(ch, decorator_callee(d)))
    else {
        return;
    };
    pandas_udf_def(ch, f, Some(&dec.expression));
}

fn pandas_udf_def(ch: &mut Checker, f: &ast::StmtFunctionDef, factory: Option<&Expr>) {
    let (mode, kind) = if pandas_udf_is_iterator(ch, f, factory) {
        (
            Mode::InsideLoopOverFirstParam,
            Kind::IterLoop("iterator pandas_udf"),
        )
    } else {
        (Mode::Anywhere, Kind::SeriesUdf)
    };
    let hits = scan(ch, Target::Def(f), heavy_init, mode);
    let name = f.name.as_str().to_string();
    report_udf011(ch, hits, kind, &name);
}

/// `pandas_udf(fn, ...)`, `df.mapInPandas(fn, ...)`, `grouped.applyInPandas(fn, ...)`.
pub fn pandas_udf_call(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if is_pandas_udf(ch, &c.func) {
        let Some(arg) = call_arg(c, 0, "f") else {
            return;
        };
        let Some(t) = resolve_target(ch, arg) else {
            return;
        };
        match t {
            Target::Def(f) => {
                if f.decorator_list
                    .iter()
                    .any(|d| is_pandas_udf(ch, decorator_callee(d)))
                {
                    return; // reported by the decorator path
                }
                pandas_udf_def(ch, f, Some(expr));
            }
            Target::Lambda(_) => {
                let hits = scan(ch, t, heavy_init, Mode::Anywhere);
                let name = target_name(t);
                report_udf011(ch, hits, Kind::SeriesUdf, &name);
            }
        }
        return;
    }
    let Some(sc) = ch.model.spark_call_named(
        expr,
        &["mapInPandas", "mapInArrow", "applyInPandas", "applyInArrow"],
    ) else {
        return;
    };
    let m = sc.method;
    let (mode, kind) = match m {
        "mapInPandas" | "mapInArrow" if sc.is_dataframe() => (
            Mode::InsideLoopOverFirstParam,
            Kind::IterLoop(if m == "mapInPandas" {
                "mapInPandas"
            } else {
                "mapInArrow"
            }),
        ),
        "applyInPandas" | "applyInArrow"
            if matches!(sc.class(), Ty::GroupedData | Ty::PandasCogroupedOps) =>
        {
            (
                Mode::Anywhere,
                Kind::PerGroup(if m == "applyInPandas" {
                    "applyInPandas"
                } else {
                    "applyInArrow"
                }),
            )
        }
        _ => return,
    };
    let Some(arg) = call_arg(c, 0, "func") else {
        return;
    };
    let Some(t) = resolve_target(ch, arg) else {
        return;
    };
    let hits = scan(ch, t, heavy_init, mode);
    let name = target_name(t);
    report_udf011(ch, hits, kind, &name);
}

// ------------------------------------------------------------------ UDF012

fn report_udf012(ch: &mut Checker, hits: Vec<Hit>, sink: &str, name: &str) {
    for h in hits {
        if !ch.state.udf_init_seen.insert((h.range, "UDF012")) {
            continue;
        }
        let what = hit_text(&h);
        ch.report(
            Diagnostic::new(
                "UDF012",
                format!("{what} in {sink} function {name} opens an external connection for every record."),
                h.range,
            )
            .with_help("Open the client once per partition (foreachPartition / mapPartitions / mapInPandas, ForeachWriter.open) or reuse a pooled module-level client."),
        );
    }
}

fn row_target(ch: &mut Checker, arg: &Expr, sink: &str) {
    let Some(t) = resolve_target(ch, arg) else {
        return;
    };
    if let Target::Def(f) = t
        && f.decorator_list
            .iter()
            .any(|d| is_pandas_udf(ch, decorator_callee(d)))
    {
        return; // a pandas UDF works on batches (UDF011)
    }
    let hits = scan(ch, t, external_conn, Mode::Anywhere);
    let name = target_name(t);
    report_udf012(ch, hits, sink, &name);
}

/// `@udf` / `@udf(...)` on a def.
pub fn row_udf_decorator(ch: &mut Checker, f: &ast::StmtFunctionDef) {
    if !f
        .decorator_list
        .iter()
        .any(|d| is_udf_factory(ch, decorator_callee(d)))
    {
        return;
    }
    let hits = scan(ch, Target::Def(f), external_conn, Mode::Anywhere);
    let name = f.name.as_str().to_string();
    report_udf012(ch, hits, "udf", &name);
}

/// A class used as a streaming foreach writer: its `process` runs per row.
fn foreach_writer_class(ch: &mut Checker, class: &ast::StmtClassDef) {
    let Some(process) = class.body.iter().find_map(|s| match s {
        Stmt::FunctionDef(f) if f.name.as_str() == "process" => Some(f),
        _ => None,
    }) else {
        return;
    };
    let hits = scan(ch, Target::Def(process), external_conn, Mode::Anywhere);
    let name = format!("{}.process", class.name.as_str());
    report_udf012(ch, hits, "ForeachWriter", &name);
}

fn has_method(class: &ast::StmtClassDef, name: &str) -> bool {
    class
        .body
        .iter()
        .any(|s| matches!(s, Stmt::FunctionDef(f) if f.name.as_str() == name))
}

/// A class with `open`, `process` and `close` (the ForeachWriter protocol).
pub fn foreach_writer_classdef(ch: &mut Checker, class: &ast::StmtClassDef) {
    if has_method(class, "open") && has_method(class, "process") && has_method(class, "close") {
        foreach_writer_class(ch, class);
    }
}

fn find_class(body: &[Stmt], start: ruff_text_size::TextSize) -> Option<&ast::StmtClassDef> {
    for s in body {
        match s {
            Stmt::ClassDef(c) if c.name.range.start() == start => return Some(c),
            Stmt::ClassDef(c) => {
                if let Some(x) = find_class(&c.body, start) {
                    return Some(x);
                }
            }
            Stmt::FunctionDef(f) => {
                if let Some(x) = find_class(&f.body, start) {
                    return Some(x);
                }
            }
            Stmt::If(i) => {
                if let Some(x) = find_class(&i.body, start) {
                    return Some(x);
                }
                for e in &i.elif_else_clauses {
                    if let Some(x) = find_class(&e.body, start) {
                        return Some(x);
                    }
                }
            }
            Stmt::Try(t) => {
                if let Some(x) = find_class(&t.body, start) {
                    return Some(x);
                }
            }
            _ => {}
        }
    }
    None
}

/// The class of `Writer()` or of a name bound to `Writer()`.
fn writer_class<'a>(ch: &Checker<'a>, arg: &Expr) -> Option<&'a ast::StmtClassDef> {
    let call = match arg {
        Expr::Call(c) => c,
        Expr::Name(n) => {
            let b = ch.model.single_binding(n)?;
            match ch.model.binding(b).value? {
                Expr::Call(c) => c,
                _ => return None,
            }
        }
        _ => return None,
    };
    let Expr::Name(cls) = &*call.func else {
        return None;
    };
    let b = ch.model.single_binding(cls)?;
    find_class(&ch.module.body, ch.model.binding(b).range.start())
}

/// `udf(fn)`, `spark.udf.register("n", fn)`, `df.foreach(fn)`, `rdd.map(fn)`,
/// `df.writeStream.foreach(fn_or_writer)`.
pub fn row_sink_call(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if is_udf_factory(ch, &c.func) {
        let arg = c.arguments.args.first().or_else(|| {
            c.arguments
                .keywords
                .iter()
                .find(|k| k.arg.as_ref().is_some_and(|a| a.as_str() == "f"))
                .map(|k| &k.value)
        });
        if let Some(arg) = arg {
            row_target(ch, arg, "udf");
        }
        return;
    }
    let Some(sc) = ch.model.spark_call(expr) else {
        return;
    };
    let (m, recv_ty) = (sc.method, sc.class());
    match m {
        "register" if recv_ty == Ty::UdfRegistration => {
            if let Some(arg) = c.arguments.args.get(1) {
                row_target(ch, arg, "udf");
            }
        }
        "foreach" if recv_ty == Ty::DataStreamWriter => {
            let Some(arg) = call_arg(c, 0, "f") else {
                return;
            };
            if let Some(class) = writer_class(ch, arg) {
                foreach_writer_class(ch, class);
            } else {
                row_target(ch, arg, "writeStream.foreach");
            }
        }
        "foreach" if sc.is_dataframe() => {
            if let Some(arg) = call_arg(c, 0, "f") {
                row_target(ch, arg, "DataFrame.foreach");
            }
        }
        "map" | "flatMap" | "filter" | "foreach" | "mapValues" | "flatMapValues"
            if recv_ty == Ty::Rdd =>
        {
            if let Some(arg) = call_arg(c, 0, "f") {
                let sink = format!("RDD.{m}");
                row_target(ch, arg, &sink);
            }
        }
        _ => {}
    }
}
