//! What a reader/writer call ends up configured with: format, options, schema and
//! target, and whether that picture is complete.
//!
//! Every IO rule asks this module instead of walking the
//! `.format().option().options()` chain itself. [`IoSettings::of`] starts at a
//! terminal call (`load`, `csv`, `jdbc`, `save`, `saveAsTable`, `start`, `toTable` …)
//! and gathers, with Spark's precedence (the terminal's arguments first, then the
//! builder calls closest to it):
//!
//! - the builder chain, followed back through variables (`r = spark.read.format(..)`
//!   then `r.load()`); a name bound on several paths (`if`/`else`, or an `if`
//!   expression) is gathered per path and merged: a format / option value counts when
//!   every path agrees, an option set on some paths only is set with an unknown value,
//!   and writers whose paths differ beyond options/format/schema are not analysed;
//! - `.option(k, v)`, `.options(k=v)`, `.options(**{..})` and `.options(**d)` with `d`
//!   a dict literal used only there; the terminal's keyword arguments, `load(format=)`
//!   / `start(format=)` / `save(format=)`, positional `schema` / `format` / jdbc
//!   arguments, and the jdbc `properties` dict;
//! - completeness: `**kwargs` we cannot see, `.options(d)`, a non-literal option key,
//!   the stored builder configured by a separate statement (`r.option(..)` /
//!   `r.schema(..)` on its own line: builders mutate in place), a loop-carried builder,
//!   or a builder the oracle does not know to be rooted at `spark.read` /
//!   `spark.readStream` / `df.write` / `df.writeStream` / `df.writeTo` on every path
//!   (`Facts::fresh_builder`: a parameter, a helper's result) make the settings
//!   incomplete — rules that need to know an option is *absent* must check
//!   [`IoSettings::complete`].

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::Ranged;
use rustc_hash::FxHashMap;
use sparklint_semantic::api::{ActionKind, action_kind};
use sparklint_semantic::{BindingId, SparkCall, Ty, call_arg, static_text, str_literal};

use super::helpers::{bool_literal, method_call};
use crate::checker::Checker;

/// Terminals named after their file format (`spark.read.csv(..)`, `df.write.parquet(..)`).
const FORMAT_METHODS: &[&str] = &[
    "csv", "json", "parquet", "orc", "xml", "text", "jdbc", "avro",
];

/// Builder methods that configure a stored reader/writer in place.
const CONFIG_METHODS: &[&str] = &["option", "options", "format", "schema"];

/// How many bindings deep the chain walk follows names (a guard; real chains are
/// a few hops).
const MAX_BINDINGS: usize = 32;

/// The settings of one reader/writer terminal call.
#[derive(Debug)]
pub struct IoSettings<'e> {
    /// The terminal's receiver class (`DataFrameReader`, `DataStreamWriter` …).
    pub class: Ty,
    /// The terminal method (`load`, `csv`, `saveAsTable` …).
    pub method: &'e str,
    /// The terminal call.
    pub call: &'e ast::ExprCall,
    /// Builder calls between the source and the terminal, closest to the terminal
    /// first (`option`, `mode`, `partitionBy` …).
    pub calls: Vec<(&'e str, &'e ast::ExprCall)>,
    /// Writers: the DataFrame the writer came from (`df` in `df.write…` /
    /// `df.writeTo(..)` / `df.writeStream…`).
    pub df: Option<&'e Expr>,
    /// The writer is a `writeTo(..)` (DataFrameWriterV2) chain.
    pub v2: bool,
    /// The chain was followed through a variable.
    pub indirect: bool,
    format: Option<String>,
    /// lower-cased key → value (`None`: `option(k)` without a value).
    options: FxHashMap<String, Option<&'e Expr>>,
    schema: bool,
    complete: bool,
    /// `writeTo(table)` argument.
    write_to: Option<&'e Expr>,
}

fn is_reader(t: Ty) -> bool {
    matches!(t, Ty::DataFrameReader | Ty::DataStreamReader)
}

fn is_writer(t: Ty) -> bool {
    matches!(
        t,
        Ty::DataFrameWriter | Ty::DataFrameWriterV2 | Ty::DataStreamWriter
    )
}

/// Terminal parameters by position, as an option key (`table` → `dbtable`) or one of
/// the special names `path` / `format` / `schema` / `properties` / `target` / `mode`.
fn params(class: Ty, method: &str) -> &'static [&'static str] {
    let reader = is_reader(class);
    match method {
        "load" if reader => &["path", "format", "schema"],
        "csv" | "json" | "xml" if reader => &["path", "schema"],
        "jdbc" if reader => &[
            "url",
            "dbtable",
            "partitioncolumn",
            "lowerbound",
            "upperbound",
            "numpartitions",
            "predicates",
            "properties",
        ],
        "jdbc" => &["url", "dbtable", "mode", "properties"],
        "save" => &["path", "format", "mode", "partitionby"],
        "saveAsTable" => &["target", "format", "mode", "partitionby"],
        "insertInto" => &["target", "overwrite"],
        "start" => &["path", "format", "outputmode", "partitionby", "queryname"],
        "toTable" => &["target", "format", "outputmode", "partitionby", "queryname"],
        "table" => &["target"],
        "parquet" | "orc" | "text" | "avro" if reader => &["path"],
        _ if class == Ty::DataFrameWriter && FORMAT_METHODS.contains(&method) => &["path", "mode"],
        _ => &[],
    }
}

/// Keyword spelling of a terminal parameter.
fn keyword(class: Ty, method: &str, param: &str) -> &'static str {
    match (param, method) {
        ("target", "saveAsTable") => "name",
        ("target", "insertInto" | "toTable" | "table") => "tableName",
        ("path", "text") if is_reader(class) => "paths",
        ("dbtable", _) => "table",
        ("partitioncolumn", _) => "column",
        ("lowerbound", _) => "lowerBound",
        ("upperbound", _) => "upperBound",
        ("numpartitions", _) => "numPartitions",
        ("partitionby", _) => "partitionBy",
        ("outputmode", _) => "outputMode",
        ("queryname", _) => "queryName",
        ("path", _) => "path",
        ("format", _) => "format",
        ("schema", _) => "schema",
        ("properties", _) => "properties",
        ("url", _) => "url",
        ("predicates", _) => "predicates",
        ("mode", _) => "mode",
        ("overwrite", _) => "overwrite",
        _ => "",
    }
}

/// A dict literal behind `**e`: inline, or a name bound once to a dict literal and
/// used nowhere else (so nothing can have mutated it).
fn splat_dict<'e>(ch: &Checker<'e>, e: &'e Expr) -> Option<&'e ast::ExprDict> {
    match e {
        Expr::Dict(d) => Some(d),
        Expr::Name(n) => {
            let b = ch.model.single_binding(n)?;
            if ch.model.binding_uses(b).count() != 1 {
                return None;
            }
            match ch.model.binding(b).value? {
                Expr::Dict(d) => Some(d),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Literal option value text; `False`/`True` as `"false"`/`"true"`.
pub fn value_text(e: &Expr) -> Option<String> {
    static_text(e).or_else(|| match e {
        Expr::BooleanLiteral(b) => Some(b.value.to_string()),
        _ => None,
    })
}

impl<'e> IoSettings<'e> {
    /// Settings of a reader/writer call (`sc` on a `DataFrameReader`,
    /// `DataStreamReader`, `DataFrameWriter(V2)` or `DataStreamWriter`).
    pub fn of(ch: &Checker<'e>, sc: &SparkCall<'e>) -> Option<Self> {
        let class = sc.class();
        if !is_reader(class) && !is_writer(class) {
            return None;
        }
        let mut s = IoSettings {
            class,
            method: sc.method,
            call: sc.call,
            calls: Vec::new(),
            df: None,
            v2: false,
            indirect: false,
            format: None,
            options: FxHashMap::default(),
            schema: false,
            complete: true,
            write_to: None,
        };
        s.terminal(ch);
        s.walk(ch, sc.receiver, &mut Vec::new());
        // The origin comes from the oracle: a builder not rooted at `spark.read` /
        // `df.write…` on every path may carry options set where it was built.
        if !ch.model.ty_of(sc.receiver).facts.fresh_builder {
            s.complete = false;
        }
        Some(s)
    }

    /// A DataFrame write action (`df.write…save()`, `df.writeTo(t).append()`) whose
    /// writer chain leads back to its DataFrame.
    pub fn write(ch: &Checker<'e>, expr: &'e Expr) -> Option<Self> {
        let sc = ch.model.spark_call(expr)?;
        let rt = sc.class();
        if action_kind(rt, sc.method) != Some(ActionKind::Write)
            || !matches!(rt, Ty::DataFrameWriter | Ty::DataFrameWriterV2)
        {
            return None;
        }
        Self::of(ch, &sc).filter(|s| s.df.is_some())
    }

    /// The source/sink format, lower-cased: the terminal's name (`csv(..)`), its
    /// `format=` argument, or the `.format()`/`.using()` call closest to it.
    pub fn format(&self) -> Option<&str> {
        self.format.as_deref()
    }

    /// Whether option `key` (lower-case) is set.
    pub fn has(&self, key: &str) -> bool {
        self.options.contains_key(key)
    }

    /// The value expression of option `key` (lower-case).
    pub fn option(&self, key: &str) -> Option<&'e Expr> {
        self.options.get(key).copied().flatten()
    }

    /// Literal text of option `key` (`True` → `"true"`).
    pub fn text(&self, key: &str) -> Option<String> {
        self.option(key).and_then(value_text)
    }

    /// Boolean value of option `key` (`True` / `"true"`).
    pub fn flag(&self, key: &str) -> Option<bool> {
        self.option(key).and_then(bool_literal)
    }

    /// An explicit schema is set (`.schema(..)`, `schema=`, a positional schema, a
    /// `schema` option).
    pub fn has_schema(&self) -> bool {
        self.schema || self.has("schema")
    }

    /// Every setting is visible (no `**kwargs`, opaque option dicts, non-literal keys
    /// or separate configuring statements).
    pub fn complete(&self) -> bool {
        self.complete
    }

    /// The table/path the call reads or writes: the terminal's target argument, the
    /// `writeTo(..)` table, or the `path` option.
    pub fn target(&self) -> Option<&'e Expr> {
        if self.v2 {
            return self.write_to;
        }
        let c = self.call;
        let direct = match self.method {
            "saveAsTable" => call_arg(c, 0, "name"),
            "insertInto" | "toTable" | "table" => call_arg(c, 0, "tableName"),
            "jdbc" => call_arg(c, 1, "table"),
            "text" if is_reader(self.class) => call_arg(c, 0, "paths"),
            _ => call_arg(c, 0, "path"),
        };
        direct.or_else(|| self.option("path"))
    }

    /// Write mode: the terminal's `mode` argument (`save(p, mode="append")`,
    /// `save(p, "delta", "append")`), else the `.mode()` call closest to the terminal
    /// (lower-cased literal).
    pub fn mode(&self) -> Option<String> {
        let lower = |e: &Expr| str_literal(e).map(str::to_ascii_lowercase);
        let at = params(self.class, self.method)
            .iter()
            .position(|p| *p == "mode");
        let arg = match at {
            Some(i) => call_arg(self.call, i, "mode"),
            None => self
                .call
                .arguments
                .keywords
                .iter()
                .find(|k| k.arg.as_ref().is_some_and(|a| a.as_str() == "mode"))
                .map(|k| &k.value),
        };
        if let Some(v) = arg {
            return lower(v);
        }
        self.calls
            .iter()
            .find(|(m, _)| *m == "mode")
            .and_then(|(_, c)| c.arguments.args.first().and_then(lower))
    }

    /// Column names given to `partitionBy` / `partitionedBy` (`"x"` or `F.col("x")`).
    pub fn partition_by(&self) -> Vec<String> {
        self.calls
            .iter()
            .filter(|(m, _)| *m == "partitionBy" || *m == "partitionedBy")
            .flat_map(|(_, c)| {
                c.arguments.args.iter().flat_map(|a| match a {
                    Expr::List(l) => l.elts.iter().filter_map(column_name).collect::<Vec<_>>(),
                    other => column_name(other).into_iter().collect(),
                })
            })
            .collect()
    }

    // ---------------------------------------------------------------- building

    fn set(&mut self, key: &str, value: Option<&'e Expr>) {
        // Walking from the terminal backwards: the first value seen is the one Spark
        // uses (a later `.option(k, ..)` overrides an earlier one).
        self.options
            .entry(key.to_ascii_lowercase())
            .or_insert(value);
    }

    fn set_format(&mut self, e: Option<&Expr>) {
        if self.format.is_none()
            && let Some(f) = e.and_then(str_literal)
        {
            self.format = Some(f.to_ascii_lowercase());
        }
    }

    /// Keys of a dict literal (`options(**{..})`, jdbc `properties`).
    fn dict(&mut self, d: &'e ast::ExprDict, strict: bool) {
        for it in &d.items {
            match it.key.as_ref().and_then(str_literal) {
                Some(k) => self.set(k, Some(&it.value)),
                None if strict => self.complete = false,
                None => {}
            }
        }
    }

    /// `**e` in an `options()` call or the terminal.
    fn splat(&mut self, ch: &Checker<'e>, e: &'e Expr) {
        match splat_dict(ch, e) {
            Some(d) => self.dict(d, true),
            None => self.complete = false,
        }
    }

    fn terminal(&mut self, ch: &Checker<'e>) {
        let (c, m, class) = (self.call, self.method, self.class);
        if FORMAT_METHODS.contains(&m) {
            self.format = Some(m.to_string());
        }
        let ps = params(class, m);
        let mut used: Vec<&str> = Vec::new();
        for (i, p) in ps.iter().enumerate() {
            let kw = keyword(class, m, p);
            let Some(v) = call_arg(c, i, kw) else {
                continue;
            };
            used.push(kw);
            match *p {
                "format" => self.set_format(Some(v)),
                "schema" => self.schema = true,
                "target" | "mode" | "overwrite" | "partitionby" | "outputmode" | "queryname" => {}
                "properties" => {
                    let props = match v {
                        Expr::Name(n) => ch
                            .model
                            .single_binding(n)
                            .and_then(|b| ch.model.binding(b).value),
                        e => Some(e),
                    };
                    // An opaque properties value (a parameter, a call) may hold any
                    // option.
                    match props {
                        Some(Expr::Dict(d)) => self.dict(d, true),
                        Some(Expr::NoneLiteral(_)) => {}
                        _ => self.complete = false,
                    }
                }
                key => self.set(key, Some(v)),
            }
        }
        for k in &c.arguments.keywords {
            match &k.arg {
                None => self.splat(ch, &k.value),
                Some(a) if used.contains(&a.as_str()) => {}
                Some(a) => match a.as_str() {
                    "schema" => self.schema = true,
                    "format" => self.set_format(Some(&k.value)),
                    other => self.set(other, Some(&k.value)),
                },
            }
        }
    }

    /// One builder call on the chain.
    fn builder(&mut self, ch: &Checker<'e>, m: &'e str, c: &'e ast::ExprCall) {
        self.calls.push((m, c));
        match m {
            "format" | "using" => self.set_format(c.arguments.args.first()),
            "schema" => self.schema = true,
            "option" => match call_arg(c, 0, "key") {
                Some(k) => match str_literal(k) {
                    Some(k) => self.set(k, call_arg(c, 1, "value")),
                    None => self.complete = false,
                },
                None => self.complete = false,
            },
            "options" => {
                if !c.arguments.args.is_empty() {
                    self.complete = false;
                }
                for k in &c.arguments.keywords {
                    match &k.arg {
                        None => self.splat(ch, &k.value),
                        Some(a) => self.set(a.as_str(), Some(&k.value)),
                    }
                }
            }
            _ => {}
        }
    }

    /// Walk the builder chain backwards from `e`, gathering its builder calls. A name
    /// is followed to every binding reaching it: one binding continues the chain,
    /// several are walked one by one and merged ([`IoSettings::merge`]). Whether the
    /// chain starts at a fresh builder is the oracle's answer ([`IoSettings::of`]),
    /// not the walk's.
    fn walk(&mut self, ch: &Checker<'e>, mut e: &'e Expr, seen: &mut Vec<BindingId>) {
        let writer = is_writer(self.class);
        loop {
            if writer
                && let Expr::Attribute(a) = e
                && matches!(a.attr.as_str(), "write" | "writeStream")
                && ch.model.is_dataframe(&a.value)
            {
                self.df = Some(&a.value);
                return;
            }
            if !writer
                && let Expr::Attribute(a) = e
                && matches!(a.attr.as_str(), "read" | "readStream")
            {
                return; // `spark.read` / `spark.readStream`: a fresh reader
            }
            let Some((c, recv, m)) = method_call(e) else {
                break;
            };
            if writer && m == "writeTo" && ch.model.is_dataframe(recv) {
                self.df = Some(recv);
                self.v2 = true;
                self.write_to = call_arg(c, 0, "table");
                return;
            }
            let rt = ch.model.ty_of(recv).ty;
            if !(if writer { is_writer(rt) } else { is_reader(rt) }) {
                // Built by something we cannot see (a helper's return value …).
                self.complete = false;
                return;
            }
            self.builder(ch, m, c);
            e = recv;
        }
        if let Expr::If(x) = e {
            // `spark.read.format("csv") if c else spark.read.format("json")`
            self.indirect = true;
            let paths = [&*x.body, &*x.orelse]
                .into_iter()
                .map(|v| {
                    let mut p = self.empty_path();
                    p.walk(ch, v, seen);
                    p
                })
                .collect();
            self.merge(ch, paths);
            return;
        }
        let Expr::Name(n) = e else {
            // A helper's result, an attribute: the calls that built it are not in view.
            self.complete = false;
            return;
        };
        let reaching = ch.model.reaching(n);
        let mut values = Vec::with_capacity(reaching.len());
        for &b in reaching {
            let binding = ch.model.binding(b);
            // A cycle (`r = r.option(k, v)` in a loop), a parameter or loop target, or
            // a chain too long to follow: the calls that configured it are not in view.
            let Some(v) = binding.value.filter(|v| v.range() != e.range()) else {
                self.complete = false;
                return;
            };
            if seen.contains(&b) || seen.len() >= MAX_BINDINGS {
                self.complete = false;
                return;
            }
            // Builders mutate in place: `reader.option(k, v)` as a separate statement
            // also configures this reader.
            if ch.model.binding_uses(b).any(|u| {
                u.range != n.range
                    && u.called
                    && u.attr
                        .as_deref()
                        .is_some_and(|a| CONFIG_METHODS.contains(&a))
            }) {
                self.complete = false;
            }
            values.push((b, v));
        }
        if values.is_empty() {
            // Unbound (a global): nothing to follow.
            self.complete = false;
            return;
        }
        self.indirect = true;
        if let [(b, v)] = values[..] {
            seen.push(b);
            self.walk(ch, v, seen);
            seen.pop();
            return;
        }
        // Bound on several paths (`if a: r = spark.read.format("csv") else: ...`):
        // walk each path on its own and keep what they agree on.
        let paths: Vec<IoSettings<'e>> = values
            .iter()
            .map(|&(b, v)| {
                let mut p = self.empty_path();
                seen.push(b);
                p.walk(ch, v, seen);
                seen.pop();
                p
            })
            .collect();
        self.merge(ch, paths);
    }

    /// A blank picture for one path of a name bound on several paths.
    fn empty_path(&self) -> IoSettings<'e> {
        IoSettings {
            class: self.class,
            method: self.method,
            call: self.call,
            calls: Vec::new(),
            df: None,
            v2: false,
            indirect: false,
            format: None,
            options: FxHashMap::default(),
            schema: false,
            complete: true,
            write_to: None,
        }
    }

    /// Merge the pictures of the paths reaching a name into this one (whose terminal
    /// side is already gathered and takes precedence): a format or option value
    /// counts only when every path agrees; an option set on some paths only is set
    /// with an unknown value; a schema on any path counts; complete only if every
    /// path is. Writers whose paths differ in anything but options / format /
    /// schema (mode, partitioning, the source DataFrame, `writeTo` table) stay
    /// unanalysed: incomplete, nothing gathered.
    fn merge(&mut self, ch: &Checker<'e>, paths: Vec<IoSettings<'e>>) {
        let text = |e: Option<&Expr>| e.map(|e| ch.slice(e.range()).to_string());
        let shape = |p: &IoSettings<'e>| {
            let calls: Vec<String> = p
                .calls
                .iter()
                .filter(|(m, _)| {
                    !matches!(*m, "option" | "options" | "format" | "using" | "schema")
                })
                .map(|(m, c)| format!("{m}{}", ch.slice(c.arguments.range)))
                .collect();
            (calls, text(p.df), p.v2, text(p.write_to))
        };
        let first = &paths[0];
        if is_writer(self.class) && paths[1..].iter().any(|p| shape(p) != shape(first)) {
            self.complete = false;
            return;
        }
        let agreed_format = paths[1..]
            .iter()
            .all(|p| p.format == first.format)
            .then(|| first.format.clone())
            .flatten();
        if self.format.is_none() {
            self.format = agreed_format;
        }
        let mut keys: Vec<&String> = paths.iter().flat_map(|p| p.options.keys()).collect();
        keys.sort();
        keys.dedup();
        for k in keys {
            let v0 = first.options.get(k).copied().flatten();
            let agreed = paths
                .iter()
                .all(|p| p.options.get(k).is_some_and(|v| text(*v) == text(v0)));
            let k = k.clone();
            self.set(&k, if agreed { v0 } else { None });
        }
        self.schema |= paths.iter().any(|p| p.schema);
        self.complete &= paths.iter().all(|p| p.complete);
        self.calls.extend(first.calls.iter().copied());
        if self.df.is_none() {
            self.df = first.df;
            self.v2 = first.v2;
            self.write_to = first.write_to;
        }
    }
}

/// `"x"` or `F.col("x")` → `x`.
pub fn column_name(e: &Expr) -> Option<String> {
    if let Some(s) = str_literal(e) {
        return Some(s.to_string());
    }
    if let Expr::Call(c) = e
        && let Some(n) = match &*c.func {
            Expr::Name(n) => Some(n.id.as_str()),
            Expr::Attribute(a) => Some(a.attr.as_str()),
            _ => None,
        }
        && matches!(n, "col" | "column")
    {
        return c
            .arguments
            .args
            .first()
            .and_then(str_literal)
            .map(str::to_string);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::IoSettings;
    use crate::checker::Checker;
    use crate::config::LinterSettings;
    use ruff_python_ast::{Expr, Stmt};
    use ruff_python_parser::parse_unchecked_source;
    use ruff_text_size::{Ranged, TextSize};
    use sparklint_semantic::{NoSummaries, build_model};
    use sparklint_source::SourceFile;
    use std::path::Path;

    /// What the module answers for the last statement's call.
    #[derive(Debug, PartialEq)]
    struct Answer {
        format: Option<String>,
        keys: Vec<String>,
        schema: bool,
        complete: bool,
        target: Option<String>,
        mode: Option<String>,
        df: bool,
    }

    fn answer(body: &str) -> Option<Answer> {
        let src = format!(
            "from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\ndf = spark.table('t')\n{body}\n"
        );
        let file = SourceFile::from_text(Path::new("t.py"), src.clone());
        let settings = LinterSettings::for_selectors(&["IOD001"]).unwrap();
        let parsed = parse_unchecked_source(&file.text, file.py_source_type());
        assert!(parsed.errors().is_empty(), "{body}");
        let module = parsed.syntax();
        let opts = crate::semantic_options(&file, &settings, None);
        let model = build_model(module, TextSize::of(src.as_str()), opts, &NoSummaries);
        let ch = Checker::new(&file, &model, &settings, module);
        let expr: &Expr = match module.body.last()? {
            Stmt::Assign(a) => &a.value,
            Stmt::Expr(e) => &e.value,
            _ => return None,
        };
        let sc = ch.model.spark_call(expr)?;
        let io = IoSettings::of(&ch, &sc)?;
        let mut keys: Vec<String> = io.options.keys().cloned().collect();
        keys.sort();
        Some(Answer {
            format: io.format().map(str::to_string),
            keys,
            schema: io.has_schema(),
            complete: io.complete(),
            target: io.target().map(|t| ch.slice(t.range()).to_string()),
            mode: io.mode(),
            df: io.df.is_some(),
        })
    }

    /// (settings in, answers out): format, option keys, schema, complete, target, mode.
    #[test]
    fn settings_table() {
        type Row<'a> = (
            &'a str,
            Option<&'a str>,
            &'a [&'a str],
            bool,
            bool,
            Option<&'a str>,
            Option<&'a str>,
        );
        let rows: &[Row] = &[
            // chain walk, terminal-named format, Spark precedence (last option wins)
            (
                "x = spark.read.format('json').option('inferSchema', True).csv('/p')",
                Some("csv"),
                &["inferschema", "path"],
                false,
                true,
                Some("'/p'"),
                None,
            ),
            // load(format=) beats .format(); positional format and schema
            (
                "x = spark.read.format('json').load('/p', format='CSV')",
                Some("csv"),
                &["path"],
                false,
                true,
                Some("'/p'"),
                None,
            ),
            (
                "x = spark.read.load('/p', 'csv', 'a INT')",
                Some("csv"),
                &["path"],
                true,
                true,
                Some("'/p'"),
                None,
            ),
            (
                "x = spark.read.csv('/p', 'a INT')",
                Some("csv"),
                &["path"],
                true,
                true,
                Some("'/p'"),
                None,
            ),
            (
                "x = spark.read.schema('a INT').json('/p')",
                Some("json"),
                &["path"],
                true,
                true,
                Some("'/p'"),
                None,
            ),
            // options(): keywords, inline dict, opaque dict, positional dict
            (
                "x = spark.read.options(header=True, **{'sep': ';'}).csv('/p')",
                Some("csv"),
                &["header", "path", "sep"],
                false,
                true,
                Some("'/p'"),
                None,
            ),
            (
                "o = {'maxOffsetsPerTrigger': 10}\nx = spark.readStream.format('kafka').options(**o).load()",
                Some("kafka"),
                &["maxoffsetspertrigger"],
                false,
                true,
                None,
                None,
            ),
            (
                "x = spark.read.options(**unknown_opts).csv('/p')",
                Some("csv"),
                &["path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            (
                "o = {'a': 1}\nx = spark.read.options(o).csv('/p')",
                Some("csv"),
                &["path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            // a dict used elsewhere may be mutated: opaque
            (
                "o = {'a': 1}\no['b'] = 2\nx = spark.read.options(**o).csv('/p')",
                Some("csv"),
                &["path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            // non-literal option key
            (
                "k = 'a'\nx = spark.read.option(k, 1).csv('/p')",
                Some("csv"),
                &["path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            // stored builder, followed; separate configuring statement → incomplete
            (
                "r = spark.read.format('csv').option('header', True)\nx = r.load('/p')",
                Some("csv"),
                &["header", "path"],
                false,
                true,
                Some("'/p'"),
                None,
            ),
            (
                "r = spark.read.format('csv')\nr.schema('a INT')\nx = r.load('/p')",
                Some("csv"),
                &["path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            (
                "r = spark.read\nr.option('sep', ';')\nx = r.csv('/p')",
                Some("csv"),
                &["path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            // a name bound on several paths, each a fresh builder: what the paths
            // agree on (an option set on one path only is set, value unknown)
            (
                "if df:\n    r = spark.read.format('csv')\nelse:\n    r = spark.read.format('csv').option('header', True)\nx = r.load('/p')",
                Some("csv"),
                &["header", "path"],
                false,
                true,
                Some("'/p'"),
                None,
            ),
            (
                "r = spark.read.format('csv').schema(s) if df else spark.read.format('json')\nx = r.load('/p')",
                None,
                &["path"],
                true,
                true,
                Some("'/p'"),
                None,
            ),
            (
                "if df:\n    w = df.write.mode('append')\nelse:\n    w = df.write.mode('append').option('mergeSchema', True)\nw.saveAsTable('t')",
                None,
                &["mergeschema"],
                false,
                true,
                Some("'t'"),
                Some("append"),
            ),
            // writer paths that differ beyond options: not analysed
            (
                "if df:\n    w = df.write.mode('append')\nelse:\n    w = df.write.mode('overwrite')\nw.saveAsTable('t')",
                None,
                &[],
                false,
                false,
                Some("'t'"),
                None,
            ),
            // one path of unknown origin, a loop-carried builder, a long chain of names
            (
                "def mk():\n    return spark.read\nr = mk() if df else spark.read\nx = r.option('a', 1).csv('/p')",
                Some("csv"),
                &["a", "path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            (
                "r = spark.read\nfor k in ks:\n    r = r.option('a', k)\nx = r.csv('/p')",
                Some("csv"),
                &["a", "path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            (
                "r0 = spark.read.format('csv')\nr1 = r0\nr2 = r1\nr3 = r2\nr4 = r3\nr5 = r4\nx = r5.load('/p')",
                Some("csv"),
                &["path"],
                false,
                true,
                Some("'/p'"),
                None,
            ),
            // chain origin not visible (helper result, parameter) → incomplete;
            // `spark.read` / `df.writeStream` roots → complete
            (
                "def mk():\n    return spark.read.option('sep', ';')\nx = mk().csv('/p')",
                Some("csv"),
                &["path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            (
                "from pyspark.sql.streaming import DataStreamWriter\ndef g(w: DataStreamWriter) -> DataStreamWriter:\n    return w\nx = g(df.writeStream).start('/p')",
                None,
                &["path"],
                false,
                false,
                Some("'/p'"),
                None,
            ),
            (
                "r = spark.readStream\nx = r.format('delta').load('/p')",
                Some("delta"),
                &["path"],
                false,
                true,
                Some("'/p'"),
                None,
            ),
            (
                "w = df.writeStream\nx = w.start('/p')",
                None,
                &["path"],
                false,
                true,
                Some("'/p'"),
                None,
            ),
            // jdbc arguments and properties dict become options
            (
                "x = spark.read.jdbc('u', 't', column='id', properties={'pushDownPredicate': 'false'})",
                Some("jdbc"),
                &["dbtable", "partitioncolumn", "pushdownpredicate", "url"],
                false,
                true,
                Some("'t'"),
                None,
            ),
            // streaming writer: start(path, format, ...) and **options
            (
                "x = df.writeStream.format('delta').start('/o', checkpointLocation='/c')",
                Some("delta"),
                &["checkpointlocation", "path"],
                false,
                true,
                Some("'/o'"),
                None,
            ),
            (
                "x = df.writeStream.toTable('t', 'delta')",
                Some("delta"),
                &[],
                false,
                true,
                Some("'t'"),
                None,
            ),
            // batch writers: mode keyword / positional / chain, path option, writeTo
            (
                "df.write.format('delta').mode('overwrite').option('path', '/q').save()",
                Some("delta"),
                &["path"],
                false,
                true,
                Some("'/q'"),
                Some("overwrite"),
            ),
            (
                "df.write.mode('overwrite').save('/p', 'parquet', 'append')",
                Some("parquet"),
                &["path"],
                false,
                true,
                Some("'/p'"),
                Some("append"),
            ),
            (
                "df.write.saveAsTable('t', mode='Append')",
                None,
                &[],
                false,
                true,
                Some("'t'"),
                Some("append"),
            ),
            (
                "w = df.write.mode('append')\nw.insertInto('t')",
                None,
                &[],
                false,
                true,
                Some("'t'"),
                Some("append"),
            ),
            (
                "df.writeTo('cat.t').using('delta').append()",
                Some("delta"),
                &[],
                false,
                true,
                Some("'cat.t'"),
                None,
            ),
        ];
        for (src, format, keys, schema, complete, target, mode) in rows {
            let a = answer(src).unwrap_or_else(|| panic!("no settings for {src}"));
            let want = Answer {
                format: format.map(str::to_string),
                keys: keys.iter().map(|k| k.to_string()).collect(),
                schema: *schema,
                complete: *complete,
                target: target.map(str::to_string),
                mode: mode.map(str::to_string),
                df: a.df,
            };
            assert_eq!(a, want, "{src}");
        }
    }

    #[test]
    fn writers_lead_back_to_their_dataframe() {
        assert!(answer("df.write.mode('append').save('/p')").unwrap().df);
        assert!(answer("df.writeTo('t').append()").unwrap().df);
        // Not a reader/writer call.
        assert!(answer("x = df.count()").is_none());
        assert!(answer("x = [1].count(1)").is_none());
    }
}
