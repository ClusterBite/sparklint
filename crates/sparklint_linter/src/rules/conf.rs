//! Spark config: the one config-write matcher and the config-value rules engine.
//!
//! **Matcher.** [`writes`] is the one matcher for Spark config writes with a constant
//! key — `spark.conf.set(k, v)` / `unset(k)`, `SparkSession.builder.config(k, v)` (also
//! `config(map={...})`, a dict bound to a name), `SparkConf().set/setIfMissing/setAll`
//! and `spark.sql("SET k = v")` (through `sparklint_semantic::sql`) — and [`file_sets`]
//! answers "does this file set key k". CFG003 (static confs at runtime), STM001
//! (session checkpoint default) and other analyses use them too; no rule matches
//! config keys in source text.
//!
//! **Config-value rules.** A rule that judges the value a config key is set to is one
//! [`Conf`] row on its `Rule` (`Rule::new("CFG0nn").conf(&Conf::new(KEYS, verdict))`):
//! the session keys it watches (with their usual casing, matched case-insensitively),
//! optionally the table properties it watches in `spark.sql` `TBLPROPERTIES (..)`
//! (`.table(PROPS, table_verdict)`), and verdict functions that turn a value into a
//! [`Finding`]. This module owns the rest: one [`writes`] per call for all enabled rows,
//! per-key dispatch, the display name of the key ([`Setting::key`]), recording, the
//! "set back later" downgrade (a flagged write that a later write of the same key in
//! the file undoes — another value or `spark.conf.unset(k)` — is a scoped workaround:
//! it stays reported, at info), table properties and reporting. Rules with an extra
//! shape keep a separate hook on the same `Rule` (CFG008's JDBC-reader half,
//! `config_stream::jdbc_pushdown_disabled`; IOD019's writer option).
//!
//! Rows here: CFG001 aqe-disabled, CFG002 broadcast-disabled, CFG007
//! broadcast-threshold-too-high, CFG008 predicate-pushdown-disabled, CFG009
//! delta-file-compaction-disabled, CFG010 disk-cache-disabled, CFG011
//! arrow-batch-size-extreme, CFG012 file-partition-size-extreme, CFG013
//! aqe-min-partition-num-set, CFG014 engine-acceleration-disabled, CFG015
//! dynamic-pruning-disabled, CFG016 broadcast-timeout-raised, CFG017
//! driver-result-limit-removed, CFG018 io-compression-disabled, CFG019
//! deletion-vectors-disabled, CFG020 file-statistics-disabled, CFG021
//! optimizer-rules-excluded and CFG022 delta-target-file-size-small (STM011 and
//! IOD019 and PDS005 are rows in their own modules).

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, Expr, Operator, UnaryOp};
use ruff_text_size::{Ranged, TextRange};
use rustc_hash::FxHashSet;
use sparklint_semantic::{SqlText, Ty, call_arg, int_literal};

use super::Rule;
use super::actions::method_range;
use super::helpers::*;
use crate::checker::Checker;
use crate::config::LinterSettings;
use crate::diagnostic::{Diagnostic, Severity};

/// The config-value rules (see [`Conf`]). CFG008's JDBC-reader half is a separate hook.
pub const RULES: &[Rule] = &[
    Rule::new("CFG001").conf(&Conf::new(
        &[
            "spark.sql.adaptive.enabled",
            "spark.sql.adaptive.coalescePartitions.enabled",
            "spark.sql.adaptive.skewJoin.enabled",
        ],
        aqe_disabled,
    )),
    Rule::new("CFG002").conf(&Conf::new(BROADCAST_KEYS, broadcast_disabled)),
    Rule::new("CFG007").conf(&Conf::new(BROADCAST_KEYS, broadcast_threshold_too_high)),
    Rule::new("CFG008")
        .conf(&Conf::new(
            &[
                "spark.sql.parquet.filterPushdown",
                "spark.sql.orc.filterPushdown",
            ],
            predicate_pushdown_disabled,
        ))
        .call(super::config_stream::jdbc_pushdown_disabled),
    Rule::new("CFG009").conf(
        &Conf::new(
            &[
                "spark.databricks.delta.optimizeWrite.enabled",
                "spark.databricks.delta.autoCompact.enabled",
                "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite",
                "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact",
            ],
            delta_file_compaction_disabled,
        )
        .table(
            &[
                "delta.autoOptimize.optimizeWrite",
                "delta.autoOptimize.autoCompact",
            ],
            compaction_properties_off,
        ),
    ),
    Rule::new("CFG010").conf(&Conf::new(
        &["spark.databricks.io.cache.enabled"],
        disk_cache_disabled,
    )),
    Rule::new("CFG011").conf(&Conf::new(
        &["spark.sql.execution.arrow.maxRecordsPerBatch"],
        arrow_batch_size_extreme,
    )),
    Rule::new("CFG012").conf(&Conf::new(
        &[
            "spark.sql.files.maxPartitionBytes",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes",
            // Spark 3.0 name of the advisory size (still read as a fallback).
            "spark.sql.adaptive.shuffle.targetPostShuffleInputSize",
            "spark.sql.files.openCostInBytes",
        ],
        file_partition_size_extreme,
    )),
    Rule::new("CFG013").conf(&Conf::new(
        &[
            "spark.sql.adaptive.coalescePartitions.minPartitionNum",
            // Spark 3.0 name.
            "spark.sql.adaptive.minNumPostShufflePartitions",
        ],
        aqe_min_partition_num_set,
    )),
    Rule::new("CFG014").conf(&Conf::new(
        &[
            "spark.sql.codegen.wholeStage",
            "spark.sql.parquet.enableVectorizedReader",
            "spark.sql.orc.enableVectorizedReader",
        ],
        engine_acceleration_disabled,
    )),
    Rule::new("CFG015").conf(&Conf::new(
        &[
            "spark.databricks.optimizer.dynamicFilePruning",
            "spark.sql.optimizer.dynamicPartitionPruning.enabled",
        ],
        dynamic_pruning_disabled,
    )),
    Rule::new("CFG016").conf(&Conf::new(
        &["spark.sql.broadcastTimeout"],
        broadcast_timeout_raised,
    )),
    Rule::new("CFG017").conf(&Conf::new(
        &["spark.driver.maxResultSize"],
        driver_result_limit_removed,
    )),
    Rule::new("CFG018").conf(&Conf::new(
        &[
            "spark.shuffle.compress",
            "spark.shuffle.spill.compress",
            "spark.broadcast.compress",
            "spark.sql.inMemoryColumnarStorage.compressed",
        ],
        io_compression_disabled,
    )),
    Rule::new("CFG019").conf(
        &Conf::new(
            &["spark.databricks.delta.properties.defaults.enableDeletionVectors"],
            deletion_vectors_disabled,
        )
        .table(
            &["delta.enableDeletionVectors"],
            deletion_vectors_property_off,
        ),
    ),
    Rule::new("CFG020").conf(
        &Conf::new(
            &["spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols"],
            file_statistics_disabled,
        )
        .table(
            &[
                "delta.dataSkippingNumIndexedCols",
                "delta.dataSkippingStatsColumns",
            ],
            file_statistics_properties,
        ),
    ),
    Rule::new("CFG021").conf(&Conf::new(
        &[
            "spark.sql.optimizer.excludedRules",
            "spark.sql.adaptive.optimizer.excludedRules",
        ],
        optimizer_rules_excluded,
    )),
    Rule::new("CFG022").conf(
        &Conf::new(
            &["spark.databricks.delta.properties.defaults.targetFileSize"],
            target_file_size_default_small,
        )
        .table(&["delta.targetFileSize"], target_file_size_property_small),
    ),
];

// ------------------------------------------------------------------ rule rows

/// A session config value as a verdict sees it.
#[derive(Debug, Clone, Copy)]
pub struct Setting<'a> {
    /// The watched key, with the usual casing from the row (for messages).
    pub key: &'static str,
    /// Normalised constant value text (`"false"`, `"-1"`, `"10m"`).
    pub value: &'a str,
    /// How the write reaches Spark (a launch-time key set on a running session is
    /// CFG003's case, not a value problem).
    pub via: Via,
    pub settings: &'a LinterSettings,
}

/// A table property of a `spark.sql` statement's `TBLPROPERTIES (..)`.
#[derive(Debug, Clone)]
pub struct Prop {
    /// The watched property, with the usual casing from the row.
    pub key: &'static str,
    pub value: String,
}

/// What a verdict reports: the message, help and severity (`None` = the rule's
/// default) of one diagnostic; the module adds the code and the range.
#[derive(Debug, Clone, PartialEq)]
pub struct Finding {
    pub message: String,
    pub help: &'static str,
    pub severity: Option<Severity>,
}

impl Finding {
    pub fn new(message: impl Into<String>, help: &'static str) -> Self {
        Self {
            message: message.into(),
            help,
            severity: None,
        }
    }

    pub fn with_severity(mut self, s: Severity) -> Self {
        self.severity = Some(s);
        self
    }

    pub fn info(self) -> Self {
        self.with_severity(Severity::Info)
    }

    fn diagnostic(self, code: &'static str, at: TextRange) -> Diagnostic {
        let d = Diagnostic::new(code, self.message, at).with_help(self.help);
        match self.severity {
            Some(s) => d.with_severity(s),
            None => d,
        }
    }
}

/// Verdict on one session config write of a watched key (None = not flagged).
pub type Verdict = fn(&Setting<'_>) -> Option<Finding>;
/// Verdict on the watched table properties one statement sets (in statement order,
/// never empty): at most one finding per statement.
pub type TableVerdict = fn(&[Prop]) -> Option<Finding>;

/// One config-value rule: the keys it watches and its verdicts (see the module doc).
#[derive(Debug)]
pub struct Conf {
    /// Session config keys with their usual casing (matched case-insensitively).
    pub keys: &'static [&'static str],
    pub verdict: Verdict,
    /// Table properties with their usual casing (matched case-insensitively).
    pub table_keys: &'static [&'static str],
    pub table_verdict: Option<TableVerdict>,
}

impl Conf {
    pub const fn new(keys: &'static [&'static str], verdict: Verdict) -> Self {
        Self {
            keys,
            verdict,
            table_keys: &[],
            table_verdict: None,
        }
    }

    /// Also watch these table properties in `spark.sql` `TBLPROPERTIES (..)`.
    pub const fn table(mut self, keys: &'static [&'static str], verdict: TableVerdict) -> Self {
        self.table_keys = keys;
        self.table_verdict = Some(verdict);
        self
    }

    /// The watched key matching lower-cased `key`, in its usual casing.
    fn key(&self, key: &str) -> Option<&'static str> {
        self.keys
            .iter()
            .copied()
            .find(|k| k.eq_ignore_ascii_case(key))
    }

    fn table_key(&self, key: &str) -> Option<&'static str> {
        self.table_keys
            .iter()
            .copied()
            .find(|k| k.eq_ignore_ascii_case(key))
    }
}

/// One config write of a watched key: reported at the end unless undone later.
#[derive(Debug)]
pub struct ConfWrite {
    pub code: &'static str,
    /// Lower-cased key.
    pub key: String,
    pub at: TextRange,
    /// `Some` when the rule flags this write.
    pub diag: Option<Diagnostic>,
}

/// The call hook of every enabled [`Conf`] row: the config writes of this call are
/// matched once and recorded per watched key; `TBLPROPERTIES` of a session
/// `spark.sql` statement are judged and reported here.
pub(crate) fn on_call(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    for w in writes(ch, c, expr) {
        for i in 0..ch.hooks.conf.len() {
            let (code, row) = ch.hooks.conf[i];
            let Some(key) = row.key(&w.key) else {
                continue;
            };
            let diag = w.value.as_deref().and_then(|value| {
                let s = Setting {
                    key,
                    value,
                    via: w.via,
                    settings: ch.settings,
                };
                (row.verdict)(&s).map(|f| f.diagnostic(code, w.at))
            });
            ch.state.conf_writes.push(ConfWrite {
                code,
                key: w.key.clone(),
                at: w.at,
                diag,
            });
        }
    }
    if !ch.hooks.conf.iter().any(|(_, r)| r.table_verdict.is_some()) {
        return;
    }
    // `ALTER TABLE .. SET TBLPROPERTIES ('delta.x' = 'false')`, `CREATE TABLE ..
    // TBLPROPERTIES (..)`.
    let Some(q) = session_sql(ch, c, expr) else {
        return;
    };
    let props = q.properties("TBLPROPERTIES");
    if props.is_empty() {
        return;
    }
    for i in 0..ch.hooks.conf.len() {
        let (code, row) = ch.hooks.conf[i];
        let Some(verdict) = row.table_verdict else {
            continue;
        };
        let mine: Vec<Prop> = props
            .iter()
            .filter_map(|(k, v)| {
                row.table_key(k).map(|key| Prop {
                    key,
                    value: v.clone(),
                })
            })
            .collect();
        if mine.is_empty() {
            continue;
        }
        if let Some(f) = verdict(&mine) {
            ch.report(f.diagnostic(code, method_range(expr)));
        }
    }
}

/// The finish hook of every enabled [`Conf`] row.
pub(crate) fn on_finish(ch: &mut Checker) {
    for i in 0..ch.hooks.conf.len() {
        let (code, _) = ch.hooks.conf[i];
        finish(ch, code);
    }
}

/// Report `code`'s flagged writes; a write undone later in the file is info.
fn finish(ch: &mut Checker, code: &'static str) {
    let mut mine: Vec<ConfWrite> = Vec::new();
    ch.state.conf_writes.retain_mut(|w| {
        if w.code == code {
            mine.push(ConfWrite {
                code: w.code,
                key: std::mem::take(&mut w.key),
                at: w.at,
                diag: w.diag.take(),
            });
            false
        } else {
            true
        }
    });
    mine.sort_by_key(|w| w.at.start());
    for i in 0..mine.len() {
        let Some(mut d) = mine[i].diag.take() else {
            continue;
        };
        let restored = mine[i + 1..]
            .iter()
            .any(|l| l.key == mine[i].key && l.diag.is_none() && l.at.start() > mine[i].at.start());
        if restored {
            d.message
                .push_str(" (Set back later in this file, so it is scoped to part of the job.)");
            d = d.with_severity(Severity::Info);
        }
        ch.report(d);
    }
}

/// The query of a `spark.sql(..)` call on a session.
fn session_sql(ch: &Checker, c: &ast::ExprCall, expr: &Expr) -> Option<SqlText> {
    let sc = ch.model.spark_call_named(expr, &["sql"])?;
    if !sc.class().is_session() {
        return None;
    }
    ch.model.sql_query(c)
}

// ------------------------------------------------------------------ matcher

/// How a config write reaches Spark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    /// `spark.conf.set(k, v)` / `spark.conf.unset(k)` on the running session.
    RuntimeConf,
    /// `spark.sql("SET k = v")` on the running session.
    Sql,
    /// `SparkSession.builder.config(k, v)` / `config(map={..})`.
    Builder,
    /// `SparkConf().set(k, v)` / `setIfMissing` / `setAll([(k, v), ..])`.
    SparkConf,
}

impl Via {
    /// Changes the configuration of a session that is already running.
    pub fn is_runtime(self) -> bool {
        matches!(self, Via::RuntimeConf | Via::Sql)
    }
}

/// A config write with a constant key.
#[derive(Debug, Clone)]
pub(crate) struct Write {
    /// Lower-cased key.
    pub key: String,
    /// The key as written (trimmed), for messages.
    pub name: String,
    /// Normalised constant value text (`"false"`, `"-1"`, `"10m"`); `None` for an
    /// unset or a value that is not constant.
    pub value: Option<String>,
    /// `spark.conf.unset(k)`.
    pub unset: bool,
    pub via: Via,
    /// The range to report (the method call).
    pub at: TextRange,
}

const SPARKCONF_SETTERS: &[&str] = &[
    "set",
    "setAll",
    "setAppName",
    "setMaster",
    "setIfMissing",
    "setExecutorEnv",
    "setSparkHome",
];

/// `SparkConf()` / `SparkConf().setAppName(..)` / a name bound once to one.
fn is_sparkconf(ch: &Checker, mut e: &Expr, depth: u32) -> bool {
    loop {
        match e {
            Expr::Call(c) => {
                if let Some(q) = ch.model.qualified_name(&c.func)
                    && matches!(q.as_str(), "pyspark.SparkConf" | "pyspark.conf.SparkConf")
                {
                    return true;
                }
                match method_call(e) {
                    Some((_, recv, m)) if SPARKCONF_SETTERS.contains(&m) => e = recv,
                    _ => return false,
                }
            }
            Expr::Name(n) if depth < 3 => {
                return ch
                    .model
                    .single_binding(n)
                    .and_then(|b| ch.model.binding(b).value)
                    .is_some_and(|v| v.range() != e.range() && is_sparkconf(ch, v, depth + 1));
            }
            _ => return false,
        }
    }
}

/// Constant integer value (`10 * 1024 * 1024`, `2 ** 30`, `-1`, `1 << 30`).
fn const_int(ch: &Checker, e: &Expr, depth: u32) -> Option<i128> {
    if depth > 8 {
        return None;
    }
    if let Some(v) = int_literal(e) {
        return Some(v as i128);
    }
    match e {
        Expr::UnaryOp(u) if u.op == UnaryOp::USub => {
            const_int(ch, &u.operand, depth + 1)?.checked_neg()
        }
        Expr::BinOp(b) => {
            let l = const_int(ch, &b.left, depth + 1)?;
            let r = const_int(ch, &b.right, depth + 1)?;
            match b.op {
                Operator::Mult => l.checked_mul(r),
                Operator::Add => l.checked_add(r),
                Operator::Sub => l.checked_sub(r),
                Operator::Pow => u32::try_from(r).ok().and_then(|r| l.checked_pow(r)),
                Operator::LShift => u32::try_from(r)
                    .ok()
                    .filter(|r| *r < 100)
                    .and_then(|r| l.checked_mul(1i128 << r)),
                Operator::FloorDiv if r != 0 => Some(l.div_euclid(r)),
                _ => None,
            }
        }
        Expr::Name(n) => {
            let b = ch.model.single_binding(n)?;
            let v = ch.model.binding(b).value?;
            (v.range() != e.range())
                .then(|| const_int(ch, v, depth + 1))
                .flatten()
        }
        _ => None,
    }
}

/// Normalised constant value text of a config value expression.
fn value_text(ch: &Checker, e: &Expr) -> Option<String> {
    if let Some(b) = bool_literal(e)
        && !matches!(e, Expr::StringLiteral(_))
    {
        return Some(b.to_string());
    }
    if let Some(n) = const_int(ch, e, 0) {
        return Some(n.to_string());
    }
    // `str(10 * 1024 * 1024)`
    if let Expr::Call(c) = e
        && let Expr::Name(f) = &*c.func
        && f.id.as_str() == "str"
        && c.arguments.args.len() == 1
        && c.arguments.keywords.is_empty()
    {
        return const_int(ch, &c.arguments.args[0], 0).map(|n| n.to_string());
    }
    ch.model
        .resolve_text(e)
        .filter(|t| !t.contains("{}"))
        .map(|t| t.trim().to_string())
}

/// Constant key text as written (trimmed).
fn key_text(ch: &Checker, e: &Expr) -> Option<String> {
    ch.model
        .resolve_text(e)
        .filter(|t| !t.contains("{}"))
        .map(|t| t.trim().to_string())
}

/// A dict literal, or a name bound once to one (`config(map=SETTINGS)`).
fn dict_of<'e>(ch: &Checker<'e>, e: &'e Expr) -> Option<&'e ast::ExprDict> {
    match e {
        Expr::Dict(d) => Some(d),
        Expr::Name(n) => match ch
            .model
            .single_binding(n)
            .and_then(|b| ch.model.binding(b).value)
        {
            Some(Expr::Dict(d)) => Some(d),
            _ => None,
        },
        _ => None,
    }
}

/// The config writes with a constant key made by this call (several for
/// `config(map={...})` / `SparkConf().setAll([...])`). This is the one matcher for
/// Spark config writes: rules learn about config only through it and [`file_sets`].
pub(crate) fn writes(ch: &Checker, c: &ast::ExprCall, expr: &Expr) -> Vec<Write> {
    let at = method_range(expr);
    let pair = |via: Via, k: Option<&Expr>, v: Option<&Expr>| -> Vec<Write> {
        match (k.and_then(|k| key_text(ch, k)), v) {
            (Some(name), Some(v)) => vec![Write {
                key: name.to_ascii_lowercase(),
                name,
                value: value_text(ch, v),
                unset: false,
                via,
                at,
            }],
            _ => Vec::new(),
        }
    };
    if let Some(sc) = ch
        .model
        .spark_call_named(expr, &["set", "unset", "config", "sql"])
    {
        return match (sc.class(), sc.method) {
            (Ty::RuntimeConfig, "set") => pair(
                Via::RuntimeConf,
                call_arg(c, 0, "key"),
                call_arg(c, 1, "value"),
            ),
            (Ty::RuntimeConfig, "unset") => call_arg(c, 0, "key")
                .and_then(|k| key_text(ch, k))
                .map(|name| {
                    vec![Write {
                        key: name.to_ascii_lowercase(),
                        name,
                        value: None,
                        unset: true,
                        via: Via::RuntimeConf,
                        at,
                    }]
                })
                .unwrap_or_default(),
            (Ty::SessionBuilder, "config") => {
                let map = call_arg(c, 0, "key")
                    .and_then(|k| dict_of(ch, k))
                    .or_else(|| call_arg(c, 99, "map").and_then(|m| dict_of(ch, m)));
                if let Some(d) = map {
                    d.items
                        .iter()
                        .flat_map(|it| pair(Via::Builder, it.key.as_ref(), Some(&it.value)))
                        .collect()
                } else {
                    pair(Via::Builder, call_arg(c, 0, "key"), call_arg(c, 1, "value"))
                }
            }
            (t, "sql") if t.is_session() => ch
                .model
                .sql_query(c)
                .and_then(|q| q.set_pair())
                .and_then(|p| {
                    Some(vec![Write {
                        key: p.key.to_ascii_lowercase(),
                        name: p.key,
                        value: Some(p.value?),
                        unset: false,
                        via: Via::Sql,
                        at,
                    }])
                })
                .unwrap_or_default(),
            _ => Vec::new(),
        };
    }
    match method_call(expr) {
        Some((_, recv, "set" | "setIfMissing")) if is_sparkconf(ch, recv, 0) => pair(
            Via::SparkConf,
            call_arg(c, 0, "key"),
            call_arg(c, 1, "value"),
        ),
        Some((_, recv, "setAll")) if is_sparkconf(ch, recv, 0) => {
            let items = match call_arg(c, 0, "pairs") {
                Some(Expr::List(l)) => &l.elts[..],
                Some(Expr::Tuple(t)) => &t.elts[..],
                _ => &[],
            };
            items
                .iter()
                .flat_map(|it| match it {
                    Expr::Tuple(t) if t.elts.len() == 2 => {
                        pair(Via::SparkConf, Some(&t.elts[0]), Some(&t.elts[1]))
                    }
                    _ => Vec::new(),
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Whether some call in this file sets config `key` (lower-cased) to a value
/// (anything [`writes`] sees: conf.set, builder config, SparkConf, SQL `SET`). A key
/// mentioned only in a comment or an unrelated string does not count.
pub(crate) fn file_sets(ch: &mut Checker, key: &str) -> bool {
    if ch.state.conf_keys_set.is_none() {
        struct V<'c, 'a> {
            ch: &'c Checker<'a>,
            keys: FxHashSet<String>,
        }
        impl<'b> Visitor<'b> for V<'_, '_> {
            fn visit_expr(&mut self, e: &'b Expr) {
                if let Expr::Call(c) = e {
                    for w in writes(self.ch, c, e) {
                        if !w.unset {
                            self.keys.insert(w.key);
                        }
                    }
                }
                visitor::walk_expr(self, e);
            }
        }
        let mut v = V {
            ch,
            keys: FxHashSet::default(),
        };
        for s in &ch.module.body {
            v.visit_stmt(s);
        }
        let keys = v.keys;
        ch.state.conf_keys_set = Some(keys);
    }
    ch.state
        .conf_keys_set
        .as_ref()
        .is_some_and(|k| k.contains(key))
}

/// Spark's `byteFromString` (bytes unit): `"10485760"`, `"10m"`, `"1gb"`, `"-1"`.
pub(crate) fn parse_bytes(s: &str) -> Option<i128> {
    let s = s.trim().to_ascii_lowercase();
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.as_str()),
    };
    let digits = s.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    let n: i128 = s[..digits].parse().ok()?;
    let mult: i128 = match &s[digits..] {
        "" | "b" => 1,
        "k" | "kb" => 1 << 10,
        "m" | "mb" => 1 << 20,
        "g" | "gb" => 1 << 30,
        "t" | "tb" => 1 << 40,
        "p" | "pb" => 1 << 50,
        _ => return None,
    };
    let v = n.checked_mul(mult)?;
    Some(if neg { -v } else { v })
}

/// `1073741824` → `1 GB`, `10485760` → `10 MB`.
fn human_bytes(n: i128) -> String {
    const UNITS: &[(&str, i128)] = &[
        ("TB", 1 << 40),
        ("GB", 1 << 30),
        ("MB", 1 << 20),
        ("KB", 1 << 10),
    ];
    for (u, m) in UNITS {
        if n >= *m {
            let v = n as f64 / *m as f64;
            return if v.fract() == 0.0 {
                format!("{v:.0} {u}")
            } else {
                format!("{v:.1} {u}")
            };
        }
    }
    format!("{n} bytes")
}

fn is_false(v: &str) -> bool {
    v.eq_ignore_ascii_case("false")
}

// ------------------------------------------------------------------ CFG001

fn aqe_disabled(s: &Setting) -> Option<Finding> {
    if !is_false(s.value) {
        return None;
    }
    let (msg, help) = match s.key {
        "spark.sql.adaptive.enabled" => (
            "Adaptive Query Execution disabled (spark.sql.adaptive.enabled=false): no partition coalescing, skew-join splitting or runtime broadcast conversion for the rest of the session.",
            "Remove the line. If one query misbehaves under AQE, fix that query (a join hint, or a targeted AQE setting) instead of switching AQE off for everything.",
        ),
        "spark.sql.adaptive.coalescePartitions.enabled" => (
            "AQE partition coalescing disabled: every shuffle keeps spark.sql.shuffle.partitions tasks, however little data it has (many tiny tasks and small output files).",
            "Remove the line; to bound task size use spark.sql.adaptive.advisoryPartitionSizeInBytes instead.",
        ),
        _ => (
            "AQE skew-join handling disabled: a skewed join key leaves one straggler task holding the whole stage.",
            "Remove the line; tune spark.sql.adaptive.skewJoin.skewedPartitionFactor / ...ThresholdInBytes if splitting is too eager.",
        ),
    };
    Some(Finding::new(msg, help))
}

// ------------------------------------------------------------------ CFG002 / CFG007

const BROADCAST_KEYS: &[&str] = &[
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.autoBroadcastJoinThreshold",
    "spark.databricks.adaptive.autoBroadcastJoinThreshold",
];

fn broadcast_disabled(s: &Setting) -> Option<Finding> {
    let n = parse_bytes(s.value)?;
    if n > 0 {
        return None;
    }
    let (name, v) = (s.key, s.value);
    let scope = if name.contains(".adaptive.") {
        "runtime (AQE) broadcast conversion"
    } else {
        "automatic broadcast joins"
    };
    Some(Finding::new(
        format!(
            "{name}={v} disables {scope} for the whole session: every small-table join becomes a sort-merge join that shuffles the large side."
        ),
        "Remove it. For the one join that misbehaved, use a hint on that join (df.hint(\"merge\") or hint(\"shuffle_hash\")) instead.",
    ))
}

/// Spark's hard limit for one broadcast relation.
const BROADCAST_HARD_LIMIT: i128 = 8 << 30;

fn broadcast_threshold_too_high(s: &Setting) -> Option<Finding> {
    let n = parse_bytes(s.value)?;
    let max = s.settings.broadcast_threshold_max;
    if max == 0 || n < max as i128 {
        return None;
    }
    let name = s.key;
    let size = human_bytes(n);
    const HELP: &str = "Keep the default and broadcast specific small, filtered frames with F.broadcast(df); keep table statistics fresh (ANALYZE TABLE ... COMPUTE STATISTICS).";
    Some(if n >= BROADCAST_HARD_LIMIT {
        Finding::new(
            format!(
                "{name} set to {size}: at or above Spark's 8 GB broadcast limit, so any join that qualifies fails (\"Cannot broadcast the table that is larger than 8GB\"), usually after the driver runs out of memory building it."
            ),
            HELP,
        )
        .with_severity(Severity::Error)
    } else {
        Finding::new(
            format!(
                "{name} raised to {size}: every table estimated under that size is collected to the driver and copied to every executor (driver OOM / broadcast timeouts; stale statistics let big tables through)."
            ),
            HELP,
        )
    })
}

// ------------------------------------------------------------------ CFG008

fn predicate_pushdown_disabled(s: &Setting) -> Option<Finding> {
    if !is_false(s.value) {
        return None;
    }
    let name = s.key;
    let fmt = if name.contains(".orc.") {
        "ORC"
    } else {
        "Parquet"
    };
    Some(Finding::new(
        format!(
            "{name}=false: {fmt} scans stop using row-group min/max statistics to skip data, so every filtered read decodes all of it."
        ),
        "Remove the line. If one reader bug forced it, set it back right after that read (or noqa it with the reason).",
    ))
}

// ------------------------------------------------------------------ CFG009

const COMPACTION_HELP: &str = "Remove it and let Delta size files (optimized writes + auto compaction). If one latency-bound stream needs it off, set the table property on that table only and schedule OPTIMIZE for it.";

fn delta_file_compaction_disabled(s: &Setting) -> Option<Finding> {
    if !is_false(s.value) {
        return None;
    }
    let name = s.key;
    let what = if name.contains("optimizeWrite") {
        "optimized writes"
    } else {
        "auto compaction"
    };
    let scope = if name.contains(".defaults.") {
        "every Delta table this session creates"
    } else {
        "every Delta table this session writes"
    };
    Some(Finding::new(
        format!(
            "{name}=false turns off {what} for {scope}: appends, MERGEs and streaming writes leave many small files (slower reads, a bigger Delta log, OPTIMIZE debt)."
        ),
        COMPACTION_HELP,
    ))
}

/// `delta.autoOptimize.optimizeWrite` / `autoCompact` = false, one finding naming
/// every such property of the statement.
fn compaction_properties_off(props: &[Prop]) -> Option<Finding> {
    let mut off: Vec<&str> = Vec::new();
    for p in props {
        if is_false(&p.value) && !off.contains(&p.key) {
            off.push(p.key);
        }
    }
    if off.is_empty() {
        return None;
    }
    let names = off.join(", ");
    Some(Finding::new(
        format!(
            "Table property {names} = false: writes to this table stop sizing their files, so appends and streaming writes leave many small files (slower reads, OPTIMIZE debt)."
        ),
        COMPACTION_HELP,
    ))
}

// ------------------------------------------------------------------ CFG010

fn disk_cache_disabled(s: &Setting) -> Option<Finding> {
    if !is_false(s.value) {
        return None;
    }
    Some(Finding::new(
        "spark.databricks.io.cache.enabled=false turns off the Databricks disk cache: every later read of the same Parquet/Delta files goes back to cloud storage instead of the workers' local SSDs.",
        "Remove the line (usually left over from a cold-read benchmark). If a job really must not cache, noqa it with the reason.",
    ))
}

// ------------------------------------------------------------------ CFG011

/// At or above this many rows per Arrow batch, wide rows exhaust Python-worker memory.
const ARROW_BATCH_HIGH: i64 = 100_000;
/// Below this many rows per batch, the vectorised path loses most of its benefit.
const ARROW_BATCH_LOW: i64 = 500;

fn arrow_batch_size_extreme(s: &Setting) -> Option<Finding> {
    let n: i64 = s.value.trim().parse().ok()?;
    const KEY: &str = "spark.sql.execution.arrow.maxRecordsPerBatch";
    const HIGH_HELP: &str = "Keep the default 10000, or lower it for wide rows; for memory per batch use spark.sql.execution.arrow.maxBytesPerBatch and size spark.executor.memoryOverhead for Python.";
    if n <= 0 {
        Some(Finding::new(
            format!(
                "{KEY}={n} removes the Arrow batch limit: each pandas UDF / applyInPandas / mapInPandas / toPandas batch holds a whole partition outside the JVM heap (Python worker killed, exit 137, no JVM error)."
            ),
            HIGH_HELP,
        ))
    } else if n >= ARROW_BATCH_HIGH {
        Some(Finding::new(
            format!(
                "{KEY}={n} (default 10000): Arrow batches live in executor memory overhead, so big batches of wide rows get Python workers killed (exit 137 / container exceeded memory) on every pandas UDF in the job."
            ),
            HIGH_HELP,
        ))
    } else if n < ARROW_BATCH_LOW {
        Some(
            Finding::new(
                format!(
                    "{KEY}={n} (default 10000): batches this small spend more time crossing the JVM/Python boundary than computing, losing most of the vectorised speed-up."
                ),
                "Use the default 10000 unless rows are very wide; if memory forced this, raise spark.executor.memoryOverhead instead.",
            )
            .info(),
        )
    } else {
        None
    }
}

// ------------------------------------------------------------------ CFG012

/// Below this, scan or post-shuffle tasks get so small that scheduling dominates.
const PARTITION_SIZE_LOW: i128 = 8 << 20;

const PARTITION_HIGH_HELP: &str = "Keep the default (128 MB scan / 64 MB advisory) or stay within a few hundred MB; for a skewed join, rely on AQE skew handling instead of bigger partitions.";
const PARTITION_LOW_HELP: &str = "Keep the default. If a CPU-heavy UDF stage needs more tasks, repartition that frame instead of shrinking partitions for the whole session (or noqa it with the reason).";

fn file_partition_size_extreme(s: &Setting) -> Option<Finding> {
    let (name, v) = (s.key, s.value);
    let n = parse_bytes(v)?;
    let (default, high) = match name {
        "spark.sql.files.maxPartitionBytes" => ("128 MB", 1i128 << 30),
        "spark.sql.files.openCostInBytes" => {
            // Only 0 / negative is a problem: every file then counts as free to open.
            if n > 0 {
                return None;
            }
            return Some(
                Finding::new(
                    format!(
                        "spark.sql.files.openCostInBytes={v} (default 4 MB): small files count as free to open, so one scan task gets packed with thousands of them (slow listing-bound straggler tasks)."
                    ),
                    PARTITION_LOW_HELP,
                )
                .info(),
            );
        }
        _ => ("64 MB", 2i128 << 30),
    };
    let what = if name.starts_with("spark.sql.files.") {
        "scan"
    } else {
        "post-shuffle"
    };
    if n <= 0 {
        return None;
    }
    let size = human_bytes(n);
    if n > high {
        Some(Finding::new(
            format!(
                "{name} set to {size} (default {default}): {what} tasks this big spill to disk or run executors out of memory, and too few of them leave cores idle."
            ),
            PARTITION_HIGH_HELP,
        ))
    } else if n < PARTITION_SIZE_LOW {
        Some(
            Finding::new(
                format!(
                    "{name} set to {size} (default {default}): {what} partitions this small turn every stage into thousands of tiny tasks (scheduling overhead, many small output files)."
                ),
                PARTITION_LOW_HELP,
            )
            .info(),
        )
    } else {
        None
    }
}

// ------------------------------------------------------------------ CFG013

fn aqe_min_partition_num_set(s: &Setting) -> Option<Finding> {
    let (name, v) = (s.key, s.value);
    Some(
        Finding::new(
            format!(
                "{name}={v} overrides AQE's minPartitionSize: AQE can no longer merge small post-shuffle partitions below this count, so small shuffles keep many tiny tasks (and small output files)."
            ),
            "Remove it (Databricks: \"Don't set this property\"). To bound partition size use spark.sql.adaptive.advisoryPartitionSizeInBytes / coalescePartitions.minPartitionSize.",
        )
        .info(),
    )
}

// ------------------------------------------------------------------ CFG014

fn engine_acceleration_disabled(s: &Setting) -> Option<Finding> {
    if !is_false(s.value) {
        return None;
    }
    let name = s.key;
    let what = match name {
        "spark.sql.codegen.wholeStage" => {
            "turns off whole-stage code generation: every operator of every query runs row by row through virtual iterator calls instead of one compiled loop"
        }
        "spark.sql.parquet.enableVectorizedReader" => {
            "makes every Parquet/Delta scan decode values row by row instead of in columnar batches"
        }
        _ => "makes every ORC scan decode values row by row instead of in columnar batches",
    };
    Some(Finding::new(
        format!("{name}=false {what}, multiplying CPU time for the whole session."),
        "Remove the line (usually a workaround for one old codegen-size or reader-type error). If one query still needs it, set it back right after that query, or noqa it with the reason.",
    ))
}

// ------------------------------------------------------------------ CFG015

fn dynamic_pruning_disabled(s: &Setting) -> Option<Finding> {
    if !is_false(s.value) {
        return None;
    }
    let name = s.key;
    let what = if name.starts_with("spark.databricks.") {
        "dynamic file pruning: joins and MERGEs stop skipping the Delta files of the big table that cannot match the small side, so they read every file"
    } else {
        "dynamic partition pruning: a join on a partition column reads every partition of the big table instead of only the ones the small side selects"
    };
    Some(Finding::new(
        format!("{name}=false turns off {what}."),
        "Remove the line (usually left from a plan comparison or a benchmark). If one query needs it off, set it back right after that query, or noqa it with the reason.",
    ))
}

// ------------------------------------------------------------------ CFG016

/// Spark's default `spark.sql.broadcastTimeout`, in seconds.
const BROADCAST_TIMEOUT_DEFAULT_S: i128 = 300;

/// Spark's `timeStringAs(s, SECONDS)` in milliseconds: `"600"` (seconds), `"600s"`,
/// `"10min"`, `"1h"`, `"1500ms"`, `"-1"`.
fn parse_seconds_ms(s: &str) -> Option<i128> {
    let s = s.trim().to_ascii_lowercase();
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.as_str()),
    };
    let digits = s.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    let n: i128 = s[..digits].parse().ok()?;
    let ms: i128 = match &s[digits..] {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => n.checked_mul(1000)?,
        "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => n,
        "us" | "usec" | "usecs" | "microsecond" | "microseconds" => n / 1000,
        "m" | "min" | "mins" | "minute" | "minutes" => n.checked_mul(60_000)?,
        "h" | "hour" | "hours" => n.checked_mul(3_600_000)?,
        "d" | "day" | "days" => n.checked_mul(86_400_000)?,
        _ => return None,
    };
    Some(if neg { -ms } else { ms })
}

/// The CFG016 message for a `spark.sql.broadcastTimeout` value, or None (default or
/// lower, or not a constant duration).
fn broadcast_timeout_raised(s: &Setting) -> Option<Finding> {
    let v = s.value;
    let ms = parse_seconds_ms(v)?;
    let msg = if ms < 0 {
        format!(
            "spark.sql.broadcastTimeout={v} (default 300 s): a negative value means a broadcast never times out, so an oversized or stuck broadcast holds the whole cluster until someone kills the job."
        )
    } else {
        if ms <= BROADCAST_TIMEOUT_DEFAULT_S * 1000 {
            return None;
        }
        let secs = ms / 1000;
        let shown = if secs >= 3600 && secs % 3600 == 0 {
            format!("{} h", secs / 3600)
        } else if secs >= 60 && secs % 60 == 0 {
            format!("{} min", secs / 60)
        } else {
            format!("{secs} s")
        };
        format!(
            "spark.sql.broadcastTimeout raised to {shown} (default 300 s): a broadcast that needs longer is usually far bigger than estimated or built by a slow query, and the longer wait keeps the cluster busy on it (AQE broadcast stages ignore this setting since Spark 3.2)."
        )
    };
    Some(
        Finding::new(
            msg,
            "Find the join that times out: shrink or filter its broadcast side, drop a forced broadcast hint, or refresh table statistics (ANALYZE TABLE) so Spark stops broadcasting it; keep the default timeout.",
        )
        .info(),
    )
}

// ------------------------------------------------------------------ CFG017

/// A `spark.driver.maxResultSize` at or above this is reported (info).
const DRIVER_RESULT_HUGE: i128 = 16 << 30;

fn driver_result_limit_removed(s: &Setting) -> Option<Finding> {
    // A launch-time key: on a running session the write has no effect (CFG003).
    if s.via.is_runtime() {
        return None;
    }
    let v = s.value;
    let n = parse_bytes(v)?;
    let (sev, msg) = if n <= 0 {
        (
            Severity::Warning,
            format!(
                "spark.driver.maxResultSize={v} removes the driver's result limit (default 1g): an oversized collect(), toPandas() or broadcast then crashes the driver with an out-of-memory error, losing the session while the cluster sits idle, instead of failing that one job cleanly."
            ),
        )
    } else if n < DRIVER_RESULT_HUGE {
        return None;
    } else {
        (
            Severity::Info,
            format!(
                "spark.driver.maxResultSize raised to {} (default 1g): results this large are serialised and held on the driver, which usually runs out of memory or stalls in garbage collection before the limit is reached.",
                human_bytes(n)
            ),
        )
    };
    Some(
        Finding::new(
            msg,
            "Keep a limit (the default 1g, or a few GB sized below the driver memory) and bring less data to the driver: aggregate or limit in Spark, write the result to a table, or iterate with toLocalIterator().",
        )
        .with_severity(sev),
    )
}

// ------------------------------------------------------------------ CFG018

fn io_compression_disabled(s: &Setting) -> Option<Finding> {
    let name = s.key;
    // The core (non-SQL) keys are read at launch: on a running session the write is
    // rejected or ignored, which CFG003 reports.
    if !is_false(s.value) || (s.via.is_runtime() && !name.starts_with("spark.sql.")) {
        return None;
    }
    let what = match name {
        "spark.shuffle.compress" => {
            "shuffle map output is written and sent uncompressed: several times more shuffle bytes over the network and on local disk"
        }
        "spark.shuffle.spill.compress" => {
            "data spilled during shuffles is written uncompressed: several times more local disk traffic in every spilling stage"
        }
        "spark.broadcast.compress" => {
            "broadcast variables and broadcast-join tables are sent uncompressed to every executor"
        }
        _ => {
            "cached tables and DataFrames are stored uncompressed: they take several times more executor memory, so more blocks are evicted and recomputed"
        }
    };
    Some(Finding::new(
        format!("{name}=false: {what}."),
        "Remove the line (the default true is right for almost every workload); to trade CPU for size, pick a codec with spark.io.compression.codec instead.",
    ))
}

// ------------------------------------------------------------------ CFG019

const DELETION_VECTOR_WHAT: &str = "every DELETE, UPDATE and MERGE rewrites each whole data file that holds a changed row instead of writing a small row bitmap (write amplification on every change)";

const DELETION_VECTOR_HELP: &str = "Remove it (deletion vectors are the Databricks default for new tables). Keep it off only for a table that an old Delta reader or an external engine without deletion-vector support must read, and say so in a noqa.";

fn deletion_vectors_disabled(s: &Setting) -> Option<Finding> {
    if !is_false(s.value) {
        return None;
    }
    Some(Finding::new(
        format!(
            "spark.databricks.delta.properties.defaults.enableDeletionVectors=false creates every new Delta table of this session without deletion vectors: {DELETION_VECTOR_WHAT}."
        ),
        DELETION_VECTOR_HELP,
    ))
}

fn deletion_vectors_property_off(props: &[Prop]) -> Option<Finding> {
    props.iter().any(|p| is_false(&p.value)).then(|| {
        Finding::new(
            format!("Table property delta.enableDeletionVectors = false: {DELETION_VECTOR_WHAT}."),
            DELETION_VECTOR_HELP,
        )
    })
}

// ------------------------------------------------------------------ CFG020

/// CFG020 verdict for a `dataSkippingNumIndexedCols` value: `Some(true)` = 0 (no
/// statistics, warning), `Some(false)` = -1 (statistics on every column, info).
fn stats_cols_verdict(v: &str) -> Option<bool> {
    match v.trim().parse::<i64>().ok()? {
        0 => Some(true),
        -1 => Some(false),
        _ => None,
    }
}

fn stats_finding(subject: &str, none: bool) -> Finding {
    if none {
        Finding::new(
            format!(
                "{subject} = 0 writes no min/max file statistics: every filtered query reads every file (no data skipping), and ZORDER or clustering on the table prunes nothing."
            ),
            "Remove it (the default indexes the first 32 columns), or list the filter columns in delta.dataSkippingStatsColumns.",
        )
    } else {
        Finding::new(
            format!(
                "{subject} = -1 collects statistics on every column: on wide tables each write pays for min/max on hundreds of columns that no filter uses."
            ),
            "List the columns queries filter on in delta.dataSkippingStatsColumns instead (or move them to the front of the schema).",
        )
        .info()
    }
}

fn file_statistics_disabled(s: &Setting) -> Option<Finding> {
    let none = stats_cols_verdict(s.value)?;
    Some(stats_finding(
        "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols (the default for every new Delta table of this session)",
        none,
    ))
}

/// The first statistics property of the statement that turns statistics off (or on
/// for every column).
fn file_statistics_properties(props: &[Prop]) -> Option<Finding> {
    props.iter().find_map(|p| {
        if p.key == "delta.dataSkippingStatsColumns" {
            p.value
                .trim()
                .is_empty()
                .then(|| stats_finding("Table property delta.dataSkippingStatsColumns = ''", true))
        } else {
            stats_cols_verdict(&p.value)
                .map(|none| stats_finding("Table property delta.dataSkippingNumIndexedCols", none))
        }
    })
}

// ------------------------------------------------------------------ CFG021

/// The short names (last dotted segment) of the optimizer rules a
/// `excludedRules` value lists; empty for an empty / blank list.
fn excluded_rule_names(v: &str) -> Vec<&str> {
    v.split(',')
        .map(str::trim)
        .map(|r| r.trim_matches(|c| c == '\'' || c == '"').trim())
        .filter(|r| !r.is_empty())
        .map(|r| r.rsplit('.').next().unwrap_or(r))
        .collect()
}

fn optimizer_rules_excluded(s: &Setting) -> Option<Finding> {
    let names = excluded_rule_names(s.value);
    if names.is_empty() {
        return None;
    }
    let name = s.key;
    let scope = if name.contains(".adaptive.") {
        "every adaptive re-optimisation"
    } else {
        "every query"
    };
    let shown = if names.len() > 3 {
        format!("{} and {} more", names[..3].join(", "), names.len() - 3)
    } else {
        names.join(", ")
    };
    Some(
        Finding::new(
            format!(
                "{name} turns off Catalyst optimizer rules ({shown}) for {scope} of this session: lost optimizations such as filter pushdown, column pruning or join reordering make queries read and shuffle more. Unknown rule names are silently ignored, so a name copied from another Spark version does nothing."
            ),
            "Remove it once the optimizer issue it worked around is fixed; otherwise scope it (set it back right after the affected query) and say why in a comment or noqa.",
        )
        .info(),
    )
}

// ------------------------------------------------------------------ CFG022

/// Below this a fixed Delta target file size is reported (Databricks autotunes
/// 256 MB - 1 GB by table size; its own MERGE-heavy tuning goes no lower than ~16 MB).
const TARGET_FILE_SIZE_MIN: i128 = 16 << 20;

const TARGET_FILE_SIZE_WHAT: &str = "OPTIMIZE, auto compaction and optimized writes then aim for files that small instead of autotuning (256 MB to 1 GB by table size), so every read opens many more files and the Delta log grows with every write";

const TARGET_FILE_SIZE_HELP: &str = "Remove it and let Databricks autotune file sizes; for a MERGE-heavy table set delta.tuneFileSizesForRewrites = true instead of a fixed small size.";

/// The fixed target size `v` when it is a valid size below the threshold.
fn small_target_size(v: &str) -> Option<i128> {
    parse_bytes(v).filter(|n| (1..TARGET_FILE_SIZE_MIN).contains(n))
}

fn target_file_size_default_small(s: &Setting) -> Option<Finding> {
    let n = small_target_size(s.value)?;
    Some(Finding::new(
        format!(
            "{} = {} gives every new Delta table of this session a fixed target file size below 16 MB: {TARGET_FILE_SIZE_WHAT}.",
            s.key,
            human_bytes(n)
        ),
        TARGET_FILE_SIZE_HELP,
    ))
}

fn target_file_size_property_small(props: &[Prop]) -> Option<Finding> {
    props.iter().find_map(|p| {
        small_target_size(&p.value).map(|n| {
            Finding::new(
                format!(
                    "Table property delta.targetFileSize = {} is below 16 MB: {TARGET_FILE_SIZE_WHAT}.",
                    human_bytes(n)
                ),
                TARGET_FILE_SIZE_HELP,
            )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{
        Conf, Finding, Prop, Setting, Severity, Via, excluded_rule_names, parse_bytes,
        parse_seconds_ms,
    };
    use crate::config::LinterSettings;

    /// The [`Conf`] row of `code` from the rule table.
    fn row(code: &str) -> &'static Conf {
        crate::rules::table()
            .find(|r| r.code == code)
            .and_then(|r| r.conf)
            .unwrap_or_else(|| panic!("{code} has no Conf row"))
    }

    /// `code`'s verdict on `key = value` written via `via` (key matched like the
    /// engine does: case-insensitively, shown with the row's casing).
    fn verdict_via(code: &str, key: &str, value: &str, via: Via) -> Option<Finding> {
        let row = row(code);
        let key = row
            .key(&key.to_ascii_lowercase())
            .unwrap_or_else(|| panic!("{code} does not watch {key}"));
        let settings = LinterSettings::default();
        (row.verdict)(&Setting {
            key,
            value,
            via,
            settings: &settings,
        })
    }

    fn verdict(code: &str, key: &str, value: &str) -> Option<Finding> {
        verdict_via(code, key, value, Via::RuntimeConf)
    }

    /// None = not flagged; Some(None) = flagged at the rule's default severity.
    fn sev(code: &str, key: &str, value: &str) -> Option<Option<Severity>> {
        verdict(code, key, value).map(|f| f.severity)
    }

    fn table(code: &str, props: &[(&str, &str)]) -> Option<Finding> {
        let row = row(code);
        let props: Vec<Prop> = props
            .iter()
            .map(|(k, v)| Prop {
                key: row
                    .table_key(k)
                    .unwrap_or_else(|| panic!("{code} does not watch {k}")),
                value: v.to_string(),
            })
            .collect();
        (row.table_verdict.expect("table verdict"))(&props)
    }

    #[test]
    fn rows_watch_distinct_keys_with_display_casing() {
        for r in crate::rules::table() {
            let Some(c) = r.conf else { continue };
            assert!(!c.keys.is_empty(), "{} watches no key", r.code);
            for (i, k) in c.keys.iter().enumerate() {
                assert_eq!(k.trim(), *k, "{}: {k}", r.code);
                assert!(
                    !c.keys[..i].iter().any(|o| o.eq_ignore_ascii_case(k)),
                    "{} lists {k} twice",
                    r.code
                );
            }
            assert_eq!(
                c.table_keys.is_empty(),
                c.table_verdict.is_none(),
                "{}",
                r.code
            );
        }
    }

    #[test]
    fn key_display_name_comes_from_the_row() {
        let f = verdict("CFG002", "SPARK.SQL.AUTOBROADCASTJOINTHRESHOLD", "-1").unwrap();
        assert!(
            f.message
                .starts_with("spark.sql.autoBroadcastJoinThreshold=-1 disables automatic")
        );
        let f = verdict(
            "CFG002",
            "spark.sql.adaptive.autoBroadcastJoinThreshold",
            "0",
        )
        .unwrap();
        assert!(f.message.contains("runtime (AQE) broadcast conversion"));
        let f = verdict("CFG008", "spark.sql.orc.filterpushdown", "false").unwrap();
        assert!(
            f.message
                .starts_with("spark.sql.orc.filterPushdown=false: ORC scans")
        );
    }

    #[test]
    fn false_switch_rows() {
        let off = [
            ("CFG001", "spark.sql.adaptive.enabled"),
            ("CFG001", "spark.sql.adaptive.skewJoin.enabled"),
            ("CFG008", "spark.sql.parquet.filterPushdown"),
            ("CFG009", "spark.databricks.delta.autoCompact.enabled"),
            ("CFG010", "spark.databricks.io.cache.enabled"),
            ("CFG014", "spark.sql.codegen.wholeStage"),
            ("CFG015", "spark.databricks.optimizer.dynamicFilePruning"),
            ("CFG018", "spark.sql.inMemoryColumnarStorage.compressed"),
            (
                "CFG019",
                "spark.databricks.delta.properties.defaults.enableDeletionVectors",
            ),
        ];
        for (code, key) in off {
            assert_eq!(sev(code, key, "false"), Some(None), "{code} {key}");
            assert_eq!(sev(code, key, "FALSE"), Some(None), "{code} {key}");
            assert_eq!(sev(code, key, "true"), None, "{code} {key}");
            assert_eq!(sev(code, key, "0"), None, "{code} {key}");
        }
    }

    #[test]
    fn pandas_on_spark_ops_on_diff_frames_row() {
        let key = "pandas_on_Spark.compute.ops_on_diff_frames";
        assert_eq!(sev("PDS005", key, "true"), Some(None));
        assert_eq!(sev("PDS005", &key.to_ascii_lowercase(), "TRUE"), Some(None));
        assert_eq!(sev("PDS005", key, "false"), None);
        assert_eq!(sev("PDS005", key, "1"), None);
    }

    #[test]
    fn compaction_wording() {
        let m = |k: &str| verdict("CFG009", k, "false").unwrap().message;
        assert!(
            m("spark.databricks.delta.optimizeWrite.enabled")
                .contains("optimized writes for every Delta table this session writes")
        );
        assert!(
            m("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact")
                .contains("auto compaction for every Delta table this session creates")
        );
    }

    #[test]
    fn broadcast_threshold_bands() {
        assert_eq!(
            sev("CFG002", "spark.sql.autoBroadcastJoinThreshold", "10m"),
            None
        );
        assert_eq!(
            sev("CFG002", "spark.sql.autoBroadcastJoinThreshold", "auto"),
            None
        );
        let s = |v: &str| sev("CFG007", "spark.sql.autoBroadcastJoinThreshold", v);
        assert_eq!(s("100m"), None);
        assert_eq!(s("-1"), None);
        assert_eq!(s("1g"), Some(None));
        assert_eq!(s("2g"), Some(None));
        assert_eq!(s("8g"), Some(Some(Severity::Error)));
        assert!(
            verdict(
                "CFG007",
                "spark.sql.autoBroadcastJoinThreshold",
                "2147483648"
            )
            .unwrap()
            .message
            .contains("raised to 2 GB")
        );
    }

    #[test]
    fn launch_time_keys_on_a_running_session_are_cfg003s() {
        let key = "spark.driver.maxResultSize";
        assert!(verdict_via("CFG017", key, "0", Via::RuntimeConf).is_none());
        assert!(verdict_via("CFG017", key, "0", Via::Sql).is_none());
        assert!(verdict_via("CFG017", key, "0", Via::Builder).is_some());
        assert!(verdict_via("CFG017", key, "0", Via::SparkConf).is_some());
        let key = "spark.shuffle.compress";
        assert!(verdict_via("CFG018", key, "false", Via::RuntimeConf).is_none());
        assert!(verdict_via("CFG018", key, "false", Via::Builder).is_some());
        let key = "spark.sql.inMemoryColumnarStorage.compressed";
        assert!(verdict_via("CFG018", key, "false", Via::Sql).is_some());
    }

    #[test]
    fn target_file_size_bands() {
        let key = "spark.databricks.delta.properties.defaults.targetFileSize";
        for (v, flagged) in [
            ("8mb", true),
            ("1048576", true),
            ("16777215", true),
            ("512KB", true),
            ("16mb", false),
            ("16777216", false),
            ("128mb", false),
            ("1g", false),
            ("0", false),
            ("-1", false),
            ("auto", false),
            ("", false),
        ] {
            assert_eq!(sev("CFG022", key, v).is_some(), flagged, "default {v:?}");
            assert_eq!(
                table("CFG022", &[("delta.targetFileSize", v)]).is_some(),
                flagged,
                "property {v:?}"
            );
        }
        let f = verdict("CFG022", key, "8388608").unwrap();
        assert!(f.message.contains("= 8 MB"), "{}", f.message);
        assert_eq!(f.severity, None);
        let f = table("CFG022", &[("DELTA.TARGETFILESIZE", "1mb")]).unwrap();
        assert!(
            f.message
                .starts_with("Table property delta.targetFileSize = 1 MB")
        );
    }

    #[test]
    fn excluded_rule_lists() {
        assert_eq!(
            excluded_rule_names(
                "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates, org.apache.spark.sql.catalyst.optimizer.ColumnPruning"
            ),
            vec!["PushDownPredicates", "ColumnPruning"]
        );
        assert_eq!(
            excluded_rule_names("ConstantFolding"),
            vec!["ConstantFolding"]
        );
        assert_eq!(excluded_rule_names("'org.a.B', ,"), vec!["B"]);
        assert!(excluded_rule_names("").is_empty());
        assert!(excluded_rule_names("  ").is_empty());
        assert!(excluded_rule_names(" , ,").is_empty());
        let m = verdict(
            "CFG021",
            "spark.sql.adaptive.optimizer.excludedRules",
            "a.A,b.B,c.C,d.D,e.E",
        )
        .unwrap();
        assert_eq!(m.severity, Some(Severity::Info));
        assert!(
            m.message
                .contains("(A, B, C and 2 more) for every adaptive re-optimisation")
        );
        assert!(verdict("CFG021", "spark.sql.optimizer.excludedRules", " ").is_none());
    }

    #[test]
    fn tblproperties_compaction_off() {
        let names = |props: &[(&str, &str)]| {
            table("CFG009", props).map(|f| {
                f.message
                    .trim_start_matches("Table property ")
                    .split(" = ")
                    .next()
                    .unwrap()
                    .to_string()
            })
        };
        assert_eq!(
            names(&[("delta.autoOptimize.optimizeWrite", "false")]).as_deref(),
            Some("delta.autoOptimize.optimizeWrite")
        );
        assert_eq!(
            names(&[
                ("delta.autooptimize.autocompact", "false"),
                ("DELTA.AUTOOPTIMIZE.OPTIMIZEWRITE", "FALSE"),
                ("delta.autoOptimize.autoCompact", "false"),
            ])
            .as_deref(),
            Some("delta.autoOptimize.autoCompact, delta.autoOptimize.optimizeWrite")
        );
        assert_eq!(names(&[("delta.autoOptimize.optimizeWrite", "true")]), None);
    }

    #[test]
    fn tblproperties_deletion_vectors_and_stats() {
        assert!(table("CFG019", &[("delta.enableDeletionVectors", "false")]).is_some());
        assert!(table("CFG019", &[("delta.enableDeletionVectors", "true")]).is_none());
        let f = table(
            "CFG020",
            &[
                ("delta.dataSkippingNumIndexedCols", "32"),
                ("delta.dataSkippingStatsColumns", " "),
                ("delta.dataSkippingNumIndexedCols", "-1"),
            ],
        )
        .unwrap();
        assert!(
            f.message
                .starts_with("Table property delta.dataSkippingStatsColumns = '' = 0")
        );
        let f = table("CFG020", &[("delta.dataskippingnumindexedcols", "-1")]).unwrap();
        assert_eq!(f.severity, Some(Severity::Info));
        assert!(table("CFG020", &[("delta.dataSkippingStatsColumns", "a,b")]).is_none());
    }

    #[test]
    fn stats_cols_bands() {
        assert_eq!(super::stats_cols_verdict("0"), Some(true));
        assert_eq!(super::stats_cols_verdict(" 0 "), Some(true));
        assert_eq!(super::stats_cols_verdict("-1"), Some(false));
        assert_eq!(super::stats_cols_verdict("32"), None);
        assert_eq!(super::stats_cols_verdict("-2"), None);
        assert_eq!(super::stats_cols_verdict("none"), None);
    }

    #[test]
    fn arrow_batch_bands() {
        let v = |s: &str| sev("CFG011", "spark.sql.execution.arrow.maxRecordsPerBatch", s);
        assert_eq!(v("10000"), None);
        assert_eq!(v("500"), None);
        assert_eq!(v("99999"), None);
        assert_eq!(v("100000"), Some(None));
        assert_eq!(v(" 1000000 "), Some(None));
        assert_eq!(v("0"), Some(None));
        assert_eq!(v("-1"), Some(None));
        assert_eq!(v("100"), Some(Some(Severity::Info)));
        assert_eq!(v("10k"), None);
        assert_eq!(v("auto"), None);
    }

    #[test]
    fn partition_size_bands() {
        let v = |k: &str, s: &str| sev("CFG012", k, s);
        let mpb = "spark.sql.files.maxpartitionbytes";
        let adv = "spark.sql.adaptive.advisorypartitionsizeinbytes";
        let old = "spark.sql.adaptive.shuffle.targetPostShuffleInputSize";
        let open = "spark.sql.files.opencostinbytes";
        let high = Some(None);
        let low = Some(Some(Severity::Info));
        assert_eq!(v(mpb, "134217728"), None);
        assert_eq!(v(mpb, "256m"), None);
        assert_eq!(v(mpb, "1g"), None);
        assert_eq!(v(mpb, "8m"), None);
        assert_eq!(v(mpb, "2g"), high);
        assert_eq!(v(mpb, "1048576"), low);
        assert_eq!(v(mpb, "4MB"), low);
        assert_eq!(v(mpb, "auto"), None);
        assert_eq!(v(mpb, "0"), None);
        assert_eq!(v(adv, "2g"), None);
        assert_eq!(v(adv, "4g"), high);
        assert_eq!(v(adv, "1m"), low);
        assert_eq!(v(old, "4g"), high);
        assert_eq!(v(open, "4194304"), None);
        assert_eq!(v(open, "0"), low);
        assert_eq!(v(open, "-1"), low);
        let m = verdict("CFG012", adv, "4g").unwrap().message;
        assert!(m.starts_with("spark.sql.adaptive.advisoryPartitionSizeInBytes set to 4 GB (default 64 MB): post-shuffle"));
    }

    #[test]
    fn min_partition_num_any_value() {
        let k = "spark.sql.adaptive.minNumPostShufflePartitions";
        assert_eq!(sev("CFG013", k, "1"), Some(Some(Severity::Info)));
        assert!(
            verdict("CFG013", k, "200")
                .unwrap()
                .message
                .starts_with("spark.sql.adaptive.minNumPostShufflePartitions=200")
        );
    }

    #[test]
    fn bytes_like_spark() {
        assert_eq!(parse_bytes("-1"), Some(-1));
        assert_eq!(parse_bytes("10485760"), Some(10 << 20));
        assert_eq!(parse_bytes("10m"), Some(10 << 20));
        assert_eq!(parse_bytes("10MB"), Some(10 << 20));
        assert_eq!(parse_bytes(" 2g "), Some(2 << 30));
        assert_eq!(parse_bytes("16gb"), Some(16 << 30));
        assert_eq!(parse_bytes("512k"), Some(512 << 10));
        assert_eq!(parse_bytes("100b"), Some(100));
        assert_eq!(parse_bytes("-1g"), Some(-(1 << 30)));
        assert_eq!(parse_bytes("1.5g"), None);
        assert_eq!(parse_bytes("auto"), None);
        assert_eq!(parse_bytes(""), None);
        assert_eq!(parse_bytes("10 m"), None);
    }

    #[test]
    fn seconds_like_spark() {
        assert_eq!(parse_seconds_ms("300"), Some(300_000));
        assert_eq!(parse_seconds_ms(" 600s "), Some(600_000));
        assert_eq!(parse_seconds_ms("10min"), Some(600_000));
        assert_eq!(parse_seconds_ms("10m"), Some(600_000));
        assert_eq!(parse_seconds_ms("1h"), Some(3_600_000));
        assert_eq!(parse_seconds_ms("2d"), Some(172_800_000));
        assert_eq!(parse_seconds_ms("1500ms"), Some(1500));
        assert_eq!(parse_seconds_ms("-1"), Some(-1000));
        assert_eq!(parse_seconds_ms("1.5h"), None);
        assert_eq!(parse_seconds_ms("forever"), None);
        assert_eq!(parse_seconds_ms(""), None);
    }

    #[test]
    fn driver_result_bands() {
        let s = |v: &str| {
            verdict_via("CFG017", "spark.driver.maxResultSize", v, Via::Builder).map(|f| f.severity)
        };
        assert_eq!(s("0"), Some(Some(Severity::Warning)));
        assert_eq!(s("-1"), Some(Some(Severity::Warning)));
        assert_eq!(s("16g"), Some(Some(Severity::Info)));
        assert_eq!(s("32GB"), Some(Some(Severity::Info)));
        assert_eq!(s("20480m"), Some(Some(Severity::Info)));
        assert_eq!(s("1g"), None);
        assert_eq!(s("4g"), None);
        assert_eq!(s("15g"), None);
        assert_eq!(s("2147483648"), None);
        assert_eq!(s("unlimited"), None);
        assert_eq!(s("1.5g"), None);
        assert!(
            verdict_via(
                "CFG017",
                "spark.driver.maxResultSize",
                "64g",
                Via::SparkConf
            )
            .unwrap()
            .message
            .contains("64 GB")
        );
    }

    #[test]
    fn broadcast_timeout_bands() {
        let m = |s: &str| verdict("CFG016", "spark.sql.broadcastTimeout", s).map(|f| f.message);
        let v = |s: &str| m(s).is_some();
        assert!(!v("300"));
        assert!(!v("300s"));
        assert!(!v("5min"));
        assert!(!v("120"));
        assert!(!v("0"));
        assert!(v("301"));
        assert!(v("600"));
        assert!(v("1h"));
        assert!(v("36000"));
        assert!(v("-1"));
        assert!(!v("auto"));
        assert!(m("3600").unwrap().contains("1 h"));
        assert!(m("600").unwrap().contains("10 min"));
        assert!(m("301").unwrap().contains("301 s"));
    }

    /// The message variants quoted in the CFG012-CFG017 catalog entries (the public
    /// rule pages): keep both in step.
    #[test]
    fn catalog_quoted_messages_cfg012_to_cfg017() {
        let msg = |code: &str, key: &str, v: &str, via: Via| {
            verdict_via(code, key, v, via).unwrap().message
        };
        let rt = Via::RuntimeConf;
        assert_eq!(
            msg("CFG012", "spark.sql.files.maxPartitionBytes", "1m", rt),
            "spark.sql.files.maxPartitionBytes set to 1 MB (default 128 MB): scan partitions this small turn every stage into thousands of tiny tasks (scheduling overhead, many small output files)."
        );
        assert_eq!(
            msg("CFG012", "spark.sql.files.openCostInBytes", "0", rt),
            "spark.sql.files.openCostInBytes=0 (default 4 MB): small files count as free to open, so one scan task gets packed with thousands of them (slow listing-bound straggler tasks)."
        );
        assert_eq!(
            msg(
                "CFG013",
                "spark.sql.adaptive.coalescePartitions.minPartitionNum",
                "400",
                rt
            ),
            "spark.sql.adaptive.coalescePartitions.minPartitionNum=400 overrides AQE's minPartitionSize: AQE can no longer merge small post-shuffle partitions below this count, so small shuffles keep many tiny tasks (and small output files)."
        );
        assert_eq!(
            msg(
                "CFG014",
                "spark.sql.parquet.enableVectorizedReader",
                "False",
                rt
            ),
            "spark.sql.parquet.enableVectorizedReader=false makes every Parquet/Delta scan decode values row by row instead of in columnar batches, multiplying CPU time for the whole session."
        );
        assert_eq!(
            msg(
                "CFG015",
                "spark.sql.optimizer.dynamicPartitionPruning.enabled",
                "false",
                rt
            ),
            "spark.sql.optimizer.dynamicPartitionPruning.enabled=false turns off dynamic partition pruning: a join on a partition column reads every partition of the big table instead of only the ones the small side selects."
        );
        assert_eq!(
            msg("CFG016", "spark.sql.broadcastTimeout", "-1", rt),
            "spark.sql.broadcastTimeout=-1 (default 300 s): a negative value means a broadcast never times out, so an oversized or stuck broadcast holds the whole cluster until someone kills the job."
        );
        assert_eq!(
            msg("CFG017", "spark.driver.maxResultSize", "64g", Via::Builder),
            "spark.driver.maxResultSize raised to 64 GB (default 1g): results this large are serialised and held on the driver, which usually runs out of memory or stalls in garbage collection before the limit is reached."
        );
    }
}
