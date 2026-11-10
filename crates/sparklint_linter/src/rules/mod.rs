//! Rule implementations and the rule table.
//!
//! Every implemented rule is one [`Rule`] entry in its module's `RULES` list: the code
//! plus the hooks it uses (`init` / `stmt` / `call` / `expr` / `finish`). [`TABLE`]
//! lists the modules. The implemented-code list, the enabled check and the dispatch are derived from
//! the table: [`Hooks::for_checker`] keeps the hooks of the enabled rules once per file,
//! and the single AST walk calls them per node kind.
//!
//! Adding a rule: write the hook function(s) in a rule module and add one
//! `Rule::new("CODE")…` line to that module's `RULES` (a new module also needs its
//! `pub mod` line and an entry in [`TABLE`]). The registry, `sparklint rules`, the
//! fixture-dir test and the dispatch pick it up from there. A rule that judges a Spark
//! config value is a row instead of a hook: `Rule::new("CODE").conf(&conf::Conf::new(
//! KEYS, verdict))` (see [`conf`]).

use ruff_python_ast::{self as ast, Expr, Stmt};
use ruff_text_size::TextRange;
use rustc_hash::{FxHashMap, FxHashSet};
use sparklint_semantic::BindingId;

use crate::checker::Checker;

pub mod actions;
pub mod conf;
pub mod config_stream;
pub mod correctness;
pub mod correctness2;
pub mod driver;
pub mod flow;
pub mod helpers;
pub mod io;
pub mod io_settings;
pub mod lineage;
pub mod pandas_on_spark;
pub mod perf;
pub mod pipelines;
pub mod plan;
pub mod platform;
pub mod style;
pub mod style2;
pub mod udf;
pub mod udf_init;
pub mod udf_types;
pub mod writes;

/// Per-file hook run once before the walk (`init`) or after it (`finish`).
pub type FileHook = fn(&mut Checker);
/// Called for every statement.
pub type StmtHook = fn(&mut Checker, &Stmt);
/// Called for every call expression (`Expr::Call`), with the call and the expression.
pub type CallHook = fn(&mut Checker, &ast::ExprCall, &Expr);
/// Called for every expression (the hook matches the node kinds it cares about).
pub type ExprHook = fn(&mut Checker, &Expr);

/// One implemented rule: its code and the hooks it uses. Hooks only run when the
/// rule is enabled; a rule reports diagnostics with its own code (several entries may
/// share helper state in [`State`], keyed per rule).
#[derive(Clone, Copy)]
pub struct Rule {
    pub code: &'static str,
    pub init: Option<FileHook>,
    pub stmt: Option<StmtHook>,
    pub call: Option<CallHook>,
    pub expr: Option<ExprHook>,
    pub finish: Option<FileHook>,
    /// A config-value rule row: [`conf`] matches, records and reports it.
    pub conf: Option<&'static conf::Conf>,
}

impl Rule {
    pub const fn new(code: &'static str) -> Self {
        Self {
            code,
            init: None,
            stmt: None,
            call: None,
            expr: None,
            finish: None,
            conf: None,
        }
    }
    pub const fn init(mut self, f: FileHook) -> Self {
        self.init = Some(f);
        self
    }
    pub const fn stmt(mut self, f: StmtHook) -> Self {
        self.stmt = Some(f);
        self
    }
    pub const fn call(mut self, f: CallHook) -> Self {
        self.call = Some(f);
        self
    }
    pub const fn expr(mut self, f: ExprHook) -> Self {
        self.expr = Some(f);
        self
    }
    pub const fn finish(mut self, f: FileHook) -> Self {
        self.finish = Some(f);
        self
    }
    /// A config-value rule (see [`conf::Conf`]).
    pub const fn conf(mut self, c: &'static conf::Conf) -> Self {
        self.conf = Some(c);
        self
    }
}

impl std::fmt::Debug for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rule").field("code", &self.code).finish()
    }
}

/// The rule table: every rule module's `RULES`.
pub const TABLE: &[&[Rule]] = &[
    actions::RULES,
    driver::RULES,
    perf::RULES,
    plan::RULES,
    lineage::RULES,
    writes::RULES,
    io::RULES,
    udf::RULES,
    udf_types::RULES,
    udf_init::RULES,
    config_stream::RULES,
    conf::RULES,
    correctness::RULES,
    correctness2::RULES,
    platform::RULES,
    pipelines::RULES,
    pandas_on_spark::RULES,
    style::RULES,
    style2::RULES,
];

/// Every rule in the table.
pub fn table() -> impl Iterator<Item = &'static Rule> {
    TABLE.iter().flat_map(|m| m.iter())
}

/// Codes with an implementation (the table plus the SYN001 pseudo-rule). Every other
/// registered rule is metadata only.
pub fn implemented_codes() -> impl Iterator<Item = &'static str> {
    let codes = table().map(|r| r.code);
    codes.chain(std::iter::once(crate::registry::SYNTAX_ERROR))
}

pub fn is_implemented(code: &str) -> bool {
    implemented_codes().any(|c| c == code)
}

/// The hooks of the enabled rules for one file (the enabled check happens once here).
#[derive(Default)]
pub(crate) struct Hooks {
    stmt: Vec<StmtHook>,
    call: Vec<CallHook>,
    expr: Vec<ExprHook>,
    finish: Vec<FileHook>,
    init: Vec<FileHook>,
    /// Enabled config-value rows, served by one `conf` call hook and finish.
    conf: Vec<(&'static str, &'static conf::Conf)>,
}

impl Hooks {
    pub(crate) fn for_checker(ch: &Checker) -> Self {
        let mut h = Hooks::default();
        for r in table().filter(|r| ch.enabled(r.code)) {
            h.init.extend(r.init);
            h.stmt.extend(r.stmt);
            h.call.extend(r.call);
            h.expr.extend(r.expr);
            h.finish.extend(r.finish);
            h.conf.extend(r.conf.map(|c| (r.code, c)));
        }
        h
    }
}

/// `n = df.count()` variables whose uses decide ACT006/ACT007 (T2).
#[derive(Debug, Clone)]
pub struct CountVar {
    pub call_range: TextRange,
    pub recv: String,
    pub uses: u32,
    pub emptiness_uses: u32,
    pub logging_uses: u32,
}

/// A cached DataFrame binding (PRF008).
#[derive(Debug, Clone)]
pub struct CacheCandidate {
    pub report_range: TextRange,
    pub name: String,
    pub method: String,
    pub weight: u32,
    /// Range to delete to drop the `.cache()` suffix (unsafe fix).
    pub delete_range: Option<TextRange>,
}

/// Per-file state of deferred rules.
#[derive(Debug, Default)]
pub struct State {
    pub count_vars: FxHashMap<BindingId, CountVar>,
    pub cache: FxHashMap<BindingId, CacheCandidate>,
    pub flow: flow::FlowState,
    /// Loops already reported by PRF001 / PRF002 (one finding per loop).
    pub prf001_loops: FxHashSet<TextRange>,
    pub prf002_loops: FxHashSet<TextRange>,
    pub prf012_seen: FxHashSet<TextRange>,
    /// `spark.catalog.clearCache()` seen (PRF009).
    pub clear_cache_seen: bool,
    /// PRF022: `SELECT *` results assigned to a binding, decided at the end.
    pub select_star: Vec<(BindingId, TextRange)>,
    /// Lower-cased names registered with `spark.udf.register` (UDF002).
    pub registered_udfs: Vec<String>,
    /// UDF011/UDF012 call sites already reported (a function can reach several sinks).
    pub udf_init_seen: FxHashSet<(TextRange, &'static str)>,
    /// Config-value rows (`conf::Conf`): config writes of watched keys, reported at
    /// the end (a later write of the same key downgrades an earlier flagged one).
    pub conf_writes: Vec<conf::ConfWrite>,
    /// Lower-cased config keys some call in the file sets (`conf::file_sets`, built
    /// on first use).
    pub conf_keys_set: Option<FxHashSet<String>>,
    /// CFG003: what the file says about a session that already runs (built on first
    /// use).
    pub session_evidence: Option<config_stream::SessionEvidence>,
}

/// Rules served by the shared def-use analyses (state built in [`init`], fed by
/// every name load, read by the rules' finish hooks).
const COUNT_VAR_RULES: &[&str] = &["ACT006", "ACT007"];
const CACHE_RULES: &[&str] = &["PRF008", "PRF009"];
const FLOW_RULES: &[&str] = &["PRF006", "PRF008", "PRF009", "PRF022"];

pub(crate) fn init(ch: &mut Checker) {
    if ch.any_enabled(COUNT_VAR_RULES) {
        actions::init_count_vars(ch);
    }
    if ch.any_enabled(CACHE_RULES) {
        perf::init_cache_candidates(ch);
    }
    if ch.any_enabled(FLOW_RULES) {
        flow::init(ch);
    }
    for i in 0..ch.hooks.init.len() {
        let h = ch.hooks.init[i];
        h(ch);
    }
}

pub(crate) fn on_stmt(ch: &mut Checker, stmt: &Stmt) {
    for i in 0..ch.hooks.stmt.len() {
        let h = ch.hooks.stmt[i];
        h(ch, stmt);
    }
}

pub(crate) fn on_expr(ch: &mut Checker, expr: &Expr) {
    if let Expr::Name(n) = expr {
        if !ch.state.count_vars.is_empty() {
            actions::count_var_use(ch, n, expr);
        }
        if !ch.state.cache.is_empty() {
            perf::cache_use(ch, n);
        }
        if !ch.state.flow.tracked.is_empty() {
            flow::on_name(ch, n);
        }
    }
    if let Expr::Call(c) = expr {
        for i in 0..ch.hooks.call.len() {
            let h = ch.hooks.call[i];
            h(ch, c, expr);
        }
        if !ch.hooks.conf.is_empty() {
            conf::on_call(ch, c, expr);
        }
    }
    for i in 0..ch.hooks.expr.len() {
        let h = ch.hooks.expr[i];
        h(ch, expr);
    }
}

pub(crate) fn finish(ch: &mut Checker) {
    actions::finish_count_vars(ch);
    for i in 0..ch.hooks.finish.len() {
        let h = ch.hooks.finish[i];
        h(ch);
    }
    conf::on_finish(ch);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn table_codes_are_unique_and_registered() {
        let mut seen = FxHashSet::default();
        for r in table() {
            assert!(
                seen.insert(r.code),
                "{} listed twice in the rule table",
                r.code
            );
            let meta = crate::registry::get(r.code)
                .unwrap_or_else(|| panic!("{} is not in spec/rules.json", r.code));
            assert!(meta.implemented);
            assert!(
                r.init.is_some()
                    || r.stmt.is_some()
                    || r.call.is_some()
                    || r.expr.is_some()
                    || r.finish.is_some()
                    || r.conf.is_some(),
                "{} has no hooks",
                r.code
            );
        }
    }

    #[test]
    fn every_table_entry_has_a_fixture_dir() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
        for r in table() {
            assert!(
                root.join(r.code).is_dir(),
                "missing tests/fixtures/{}/",
                r.code
            );
        }
    }
}
