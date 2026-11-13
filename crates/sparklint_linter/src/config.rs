//! Configuration: `[tool.sparklint]` in `pyproject.toml` or a `sparklint.toml`
//! (closest ancestor directory wins; in one directory `sparklint.toml` beats
//! `pyproject.toml`), rule selection (codes, prefixes, `tag:x`, presets), per-file
//! ignores, excludes, severity overrides and inference options.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use globset::{Glob, GlobSet, GlobSetBuilder};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Deserialize;
use sparklint_semantic::Ty;

use crate::diagnostic::Severity;
use crate::registry::{self, RuleMeta};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid TOML in {path}: {message}")]
    Toml { path: PathBuf, message: String },
    #[error(
        "unknown rule selector `{0}` (expected a code like ACT002, a prefix like ACT, `tag:<name>`, ALL, or a preset: default, review, strict, cylint-compat, serverless)"
    )]
    UnknownSelector(String),
    #[error("invalid glob `{glob}`: {message}")]
    Glob { glob: String, message: String },
    #[error("invalid severity `{0}` (expected error, warning or info)")]
    Severity(String),
    #[error("invalid value for `{key}`: {message}")]
    Value { key: String, message: String },
}

/// The `[tool.sparklint]` table / `sparklint.toml` content.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct ConfigFile {
    pub preset: Option<String>,
    pub select: Option<Vec<String>>,
    pub extend_select: Vec<String>,
    pub ignore: Vec<String>,
    pub per_file_ignores: BTreeMap<String, Vec<String>>,
    pub exclude: Option<Vec<String>>,
    pub extend_exclude: Vec<String>,
    /// selector → severity
    pub severity: BTreeMap<String, String>,
    /// `name` (SparkSession) or `name:Type`
    pub known_spark_globals: Vec<String>,
    /// auto | on | off
    pub notebook: Option<String>,
    pub heuristics: Option<bool>,
    pub spark_version: Option<String>,
    pub collect_limit: Option<u64>,
    /// PRF024: report `cube` with at least this many columns (0 disables).
    pub cube_min_columns: Option<u32>,
    /// PRF024: report `rollup` with at least this many columns (0 disables).
    pub rollup_min_columns: Option<u32>,
    /// DBX017: report a pipeline source streamed by at least this many datasets/flows
    /// (0 disables).
    pub pipeline_fanout_min_flows: Option<u32>,
    /// PRF026: report an aggregation with at least this many distinct aggregates over
    /// different columns (0 disables).
    pub distinct_aggregates_min: Option<u32>,
    /// IOD020: report `ZORDER BY` with more than this many columns (0 disables).
    pub zorder_max_columns: Option<u32>,
    /// CFG007: report autoBroadcastJoinThreshold at or above this size (`"1g"`,
    /// `"512m"`, bytes; `"0"` disables).
    pub broadcast_threshold_max: Option<String>,
    pub functions_aliases: Option<Vec<String>>,
    pub types_aliases: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct Pyproject {
    tool: Option<PyprojectTool>,
}

#[derive(Debug, Deserialize)]
struct PyprojectTool {
    sparklint: Option<ConfigFile>,
}

/// Directories and files excluded by default.
pub const DEFAULT_EXCLUDES: &[&str] = &[
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    ".tox",
    ".nox",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "site-packages",
    ".ipynb_checkpoints",
    "target",
];

impl ConfigFile {
    pub fn from_toml_str(
        path: &Path,
        text: &str,
        is_pyproject: bool,
    ) -> Result<Option<Self>, ConfigError> {
        let err = |e: toml::de::Error| ConfigError::Toml {
            path: path.to_path_buf(),
            message: e.to_string(),
        };
        if is_pyproject {
            let p: Pyproject = toml::from_str(text).map_err(err)?;
            Ok(p.tool.and_then(|t| t.sparklint))
        } else {
            let c: ConfigFile = toml::from_str(text).map_err(err)?;
            Ok(Some(c))
        }
    }

    pub fn load(path: &Path) -> Result<Option<Self>, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let is_pyproject = path.file_name().is_some_and(|n| n == "pyproject.toml");
        Self::from_toml_str(path, &text, is_pyproject)
    }
}

/// Find the closest config file for `dir` (walking up through ancestors).
pub fn find_config(dir: &Path) -> Result<Option<(PathBuf, ConfigFile)>, ConfigError> {
    let mut cur = Some(dir);
    while let Some(d) = cur {
        for name in ["sparklint.toml", ".sparklint.toml", "pyproject.toml"] {
            let p = d.join(name);
            if p.is_file()
                && let Some(c) = ConfigFile::load(&p)?
            {
                return Ok(Some((p, c)));
            }
        }
        cur = d.parent();
    }
    Ok(None)
}

/// A rule selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    All,
    Preset(Preset),
    Tag(String),
    /// A code prefix (`ACT`, `ACT0`) or full code (`ACT002`).
    Prefix(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Preset {
    Default,
    Review,
    Strict,
    CylintCompat,
    Serverless,
}

impl Selector {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let t = s.trim();
        if t.eq_ignore_ascii_case("all") {
            return Ok(Selector::All);
        }
        if let Some(tag) = t.strip_prefix("tag:") {
            return Ok(Selector::Tag(tag.trim().to_ascii_lowercase()));
        }
        let preset = match t.to_ascii_lowercase().as_str() {
            "default" => Some(Preset::Default),
            "review" => Some(Preset::Review),
            "strict" => Some(Preset::Strict),
            "cylint-compat" | "cylint" => Some(Preset::CylintCompat),
            "serverless" | "uc-migration" => Some(Preset::Serverless),
            _ => None,
        };
        if let Some(p) = preset {
            return Ok(Selector::Preset(p));
        }
        // kebab rule name
        if let Some(r) = registry::get(t) {
            return Ok(Selector::Prefix(r.code.clone()));
        }
        let upper = t.to_ascii_uppercase();
        if registry::rules().iter().any(|r| r.code.starts_with(&upper)) && !upper.is_empty() {
            return Ok(Selector::Prefix(upper));
        }
        Err(ConfigError::UnknownSelector(s.to_string()))
    }

    /// Specificity if the selector matches the rule (higher = more specific).
    pub fn matches(&self, r: &RuleMeta) -> Option<u32> {
        match self {
            Selector::All => Some(0),
            Selector::Preset(p) => preset_contains(*p, r).then_some(1),
            Selector::Tag(t) => r.tags.iter().any(|x| x == t).then_some(2),
            Selector::Prefix(p) => r.code.starts_with(p.as_str()).then_some(2 + p.len() as u32),
        }
    }
}

fn preset_contains(p: Preset, r: &RuleMeta) -> bool {
    match p {
        Preset::Default => r.default_on,
        Preset::Review => !r.is_style(),
        Preset::Strict => true,
        Preset::CylintCompat => r.cylint_compat_on,
        Preset::Serverless => r.default_on || r.prefix() == "DBX",
    }
}

pub fn parse_selectors(list: &[String]) -> Result<Vec<Selector>, ConfigError> {
    list.iter().map(|s| Selector::parse(s)).collect()
}

/// Rule selection = select ∪ extend-select − ignore, decided per rule by the most
/// specific matching selector (ties go to ignore).
pub fn resolve_rules(select: &[Selector], ignore: &[Selector]) -> FxHashSet<&'static str> {
    let mut out = FxHashSet::default();
    for r in registry::rules() {
        let s = select.iter().filter_map(|x| x.matches(r)).max();
        let i = ignore.iter().filter_map(|x| x.matches(r)).max();
        let on = match (s, i) {
            (Some(s), Some(i)) => s > i,
            (Some(_), None) => true,
            _ => false,
        };
        if on {
            out.insert(r.code.as_str());
        }
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotebookMode {
    Auto,
    On,
    Off,
}

/// Fully resolved settings for linting one file.
#[derive(Debug, Clone)]
pub struct LinterSettings {
    pub enabled: FxHashSet<&'static str>,
    pub severity_overrides: Vec<(Selector, Severity)>,
    pub heuristics: bool,
    pub notebook: NotebookMode,
    pub known_globals: Vec<(String, Ty)>,
    pub spark_version: (u32, u32),
    pub collect_limit: u64,
    pub cube_min_columns: u32,
    pub rollup_min_columns: u32,
    pub pipeline_fanout_min_flows: u32,
    pub distinct_aggregates_min: u32,
    /// IOD020: Z-order columns allowed before a report (0 = off).
    pub zorder_max_columns: u32,
    /// CFG007 threshold in bytes (0 = off).
    pub broadcast_threshold_max: u64,
    pub functions_aliases: Vec<String>,
    pub types_aliases: Vec<String>,
}

impl Default for LinterSettings {
    fn default() -> Self {
        let select = [Selector::Preset(Preset::Default)];
        Self {
            enabled: resolve_rules(&select, &[]),
            severity_overrides: Vec::new(),
            heuristics: true,
            notebook: NotebookMode::Auto,
            known_globals: Vec::new(),
            spark_version: (3, 5),
            collect_limit: 1000,
            cube_min_columns: 5,
            rollup_min_columns: 8,
            pipeline_fanout_min_flows: 3,
            distinct_aggregates_min: 3,
            zorder_max_columns: 3,
            broadcast_threshold_max: 1 << 30,
            functions_aliases: vec!["F".into(), "sf".into()],
            types_aliases: vec!["T".into()],
        }
    }
}

impl LinterSettings {
    /// `notebook = "on"`: plain modules are read as notebooks, so they are parsed in
    /// IPython mode (magics and shell escapes are not syntax errors).
    pub fn ipython_syntax(&self) -> bool {
        self.notebook == NotebookMode::On
    }

    /// Settings that enable exactly the given selectors (used by tests).
    pub fn for_selectors(select: &[&str]) -> Result<Self, ConfigError> {
        let sel: Vec<String> = select.iter().map(|s| s.to_string()).collect();
        Ok(Self {
            enabled: resolve_rules(&parse_selectors(&sel)?, &[]),
            ..Self::default()
        })
    }

    pub fn is_enabled(&self, code: &str) -> bool {
        self.enabled.contains(code)
    }

    pub fn severity_for(&self, code: &str, default: Severity) -> Severity {
        let Some(r) = registry::get(code) else {
            return default;
        };
        self.severity_overrides
            .iter()
            .filter_map(|(s, sev)| s.matches(r).map(|spec| (spec, *sev)))
            .max_by_key(|(spec, _)| *spec)
            .map_or(default, |(_, s)| s)
    }
}

/// Command-line overrides.
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub select: Option<Vec<String>>,
    pub extend_select: Vec<String>,
    pub ignore: Vec<String>,
}

/// A loaded configuration, able to produce per-file settings.
#[derive(Debug, Clone)]
pub struct Configuration {
    /// Directory the config file lives in (paths/globs are relative to it).
    pub root: PathBuf,
    pub source: Option<PathBuf>,
    base: LinterSettings,
    per_file: Vec<(GlobSet, Vec<Selector>)>,
    exclude: GlobSet,
}

fn parse_known_global(s: &str) -> Result<(String, Ty), ConfigError> {
    let (name, ty) = match s.split_once(':') {
        Some((n, t)) => (n.trim(), t.trim()),
        None => (s.trim(), "SparkSession"),
    };
    let t = match ty {
        "SparkSession" | "spark" => Ty::SparkSession,
        "SparkContext" => Ty::SparkContext,
        "SQLContext" => Ty::SqlContext,
        "DataFrame" => Ty::DataFrame,
        "dbutils" | "DBUtils" => Ty::DbUtils,
        other => {
            return Err(ConfigError::Value {
                key: "known-spark-globals".into(),
                message: format!("unknown type `{other}`"),
            });
        }
    };
    Ok((name.to_string(), t))
}

fn globset(patterns: &[String]) -> Result<GlobSet, ConfigError> {
    let mut b = GlobSetBuilder::new();
    for p in patterns {
        let mut add = |pat: &str| -> Result<(), ConfigError> {
            let g = Glob::new(pat).map_err(|e| ConfigError::Glob {
                glob: p.clone(),
                message: e.to_string(),
            })?;
            b.add(g);
            Ok(())
        };
        add(p)?;
        // `dir` / `dir/` also excludes everything below it and matches at any depth.
        let trimmed = p.trim_end_matches('/');
        if !trimmed.contains('*') {
            add(&format!("**/{trimmed}"))?;
            add(&format!("**/{trimmed}/**"))?;
            add(&format!("{trimmed}/**"))?;
        } else if !p.starts_with("**/") && !p.starts_with('/') {
            add(&format!("**/{p}"))?;
        }
    }
    b.build().map_err(|e| ConfigError::Glob {
        glob: patterns.join(","),
        message: e.to_string(),
    })
}

impl Configuration {
    pub fn new(
        root: &Path,
        source: Option<PathBuf>,
        file: &ConfigFile,
        cli: &CliOverrides,
    ) -> Result<Self, ConfigError> {
        let mut base = LinterSettings::default();
        if let Some(h) = file.heuristics {
            base.heuristics = h;
        }
        if let Some(n) = &file.notebook {
            base.notebook = match n.as_str() {
                "auto" => NotebookMode::Auto,
                "on" | "true" => NotebookMode::On,
                "off" | "false" => NotebookMode::Off,
                other => {
                    return Err(ConfigError::Value {
                        key: "notebook".into(),
                        message: format!("`{other}` (expected auto, on, off)"),
                    });
                }
            };
        }
        for g in &file.known_spark_globals {
            base.known_globals.push(parse_known_global(g)?);
        }
        if let Some(v) = &file.spark_version {
            let mut it = v.split('.').map(|p| p.trim().parse::<u32>());
            match (it.next(), it.next()) {
                (Some(Ok(a)), Some(Ok(b))) => base.spark_version = (a, b),
                (Some(Ok(a)), None) => base.spark_version = (a, 0),
                _ => {
                    return Err(ConfigError::Value {
                        key: "spark-version".into(),
                        message: format!("`{v}`"),
                    });
                }
            }
        }
        if let Some(n) = file.collect_limit {
            base.collect_limit = n;
        }
        if let Some(n) = file.cube_min_columns {
            base.cube_min_columns = n;
        }
        if let Some(n) = file.rollup_min_columns {
            base.rollup_min_columns = n;
        }
        if let Some(n) = file.pipeline_fanout_min_flows {
            base.pipeline_fanout_min_flows = n;
        }
        if let Some(n) = file.distinct_aggregates_min {
            base.distinct_aggregates_min = n;
        }
        if let Some(n) = file.zorder_max_columns {
            base.zorder_max_columns = n;
        }
        if let Some(v) = &file.broadcast_threshold_max {
            match crate::rules::conf::parse_bytes(v) {
                Some(n) if n >= 0 => base.broadcast_threshold_max = n as u64,
                _ => {
                    return Err(ConfigError::Value {
                        key: "broadcast-threshold-max".into(),
                        message: format!("`{v}` (expected a size such as \"1g\" or \"512m\")"),
                    });
                }
            }
        }
        if let Some(a) = &file.functions_aliases {
            base.functions_aliases = a.clone();
        }
        if let Some(a) = &file.types_aliases {
            base.types_aliases = a.clone();
        }
        for (sel, sev) in &file.severity {
            let s = Severity::parse(sev).ok_or_else(|| ConfigError::Severity(sev.clone()))?;
            base.severity_overrides.push((Selector::parse(sel)?, s));
        }
        let mut select_strings: Vec<String> = match (&cli.select, &file.select) {
            (Some(s), _) => s.clone(),
            (None, Some(s)) => s.clone(),
            (None, None) => vec![file.preset.clone().unwrap_or_else(|| "default".into())],
        };
        select_strings.extend(file.extend_select.iter().cloned());
        select_strings.extend(cli.extend_select.iter().cloned());
        let mut ignore_strings = file.ignore.clone();
        ignore_strings.extend(cli.ignore.iter().cloned());
        let select = parse_selectors(&select_strings)?;
        let ignore = parse_selectors(&ignore_strings)?;
        base.enabled = resolve_rules(&select, &ignore);
        let mut per_file = Vec::new();
        for (glob, codes) in &file.per_file_ignores {
            per_file.push((
                globset(std::slice::from_ref(glob))?,
                parse_selectors(codes)?,
            ));
        }
        let mut excludes: Vec<String> = file
            .exclude
            .clone()
            .unwrap_or_else(|| DEFAULT_EXCLUDES.iter().map(|s| s.to_string()).collect());
        excludes.extend(file.extend_exclude.iter().cloned());
        Ok(Self {
            root: root.to_path_buf(),
            source,
            base,
            per_file,
            exclude: globset(&excludes)?,
        })
    }

    /// Configuration with defaults (no config file) rooted at `root`.
    pub fn default_for(root: &Path, cli: &CliOverrides) -> Result<Self, ConfigError> {
        Self::new(root, None, &ConfigFile::default(), cli)
    }

    fn rel<'p>(&self, path: &'p Path) -> &'p Path {
        path.strip_prefix(&self.root).unwrap_or(path)
    }

    /// Whether a path is excluded (relative to the config root).
    pub fn is_excluded(&self, path: &Path) -> bool {
        let rel = self.rel(path);
        self.exclude.is_match(rel)
            || rel
                .file_name()
                .is_some_and(|n| self.exclude.is_match(Path::new(n)))
    }

    /// Settings for one file (applies per-file-ignores).
    pub fn settings_for(&self, path: &Path) -> LinterSettings {
        let rel = self.rel(path);
        let extra: Vec<Selector> = self
            .per_file
            .iter()
            .filter(|(g, _)| g.is_match(rel))
            .flat_map(|(_, s)| s.iter().cloned())
            .collect();
        if extra.is_empty() {
            return self.base.clone();
        }
        // Per-file ignores always win over the selection (like ruff).
        let mut settings = self.base.clone();
        settings.enabled.retain(|code| {
            registry::get(code).is_none_or(|r| !extra.iter().any(|s| s.matches(r).is_some()))
        });
        settings
    }

    pub fn base(&self) -> &LinterSettings {
        &self.base
    }
}

/// Cache of configurations by directory (closest config wins).
#[derive(Debug, Default)]
pub struct ConfigResolver {
    cli: CliOverrides,
    explicit: Option<Configuration>,
    by_dir: std::sync::Mutex<FxHashMap<PathBuf, std::sync::Arc<Configuration>>>,
    fallback_root: PathBuf,
}

impl ConfigResolver {
    /// `explicit`: a `--config` path that applies to every file.
    pub fn new(
        explicit: Option<&Path>,
        cli: CliOverrides,
        fallback_root: &Path,
    ) -> Result<Self, ConfigError> {
        let explicit = match explicit {
            Some(p) => {
                let file = ConfigFile::load(p)?.unwrap_or_default();
                let root = p.parent().map(Path::to_path_buf).unwrap_or_default();
                Some(Configuration::new(
                    &root,
                    Some(p.to_path_buf()),
                    &file,
                    &cli,
                )?)
            }
            None => None,
        };
        Ok(Self {
            cli,
            explicit,
            by_dir: Default::default(),
            fallback_root: fallback_root.to_path_buf(),
        })
    }

    /// The configuration governing files in `dir`.
    pub fn for_dir(&self, dir: &Path) -> Result<std::sync::Arc<Configuration>, ConfigError> {
        if let Some(e) = &self.explicit {
            return Ok(std::sync::Arc::new(e.clone()));
        }
        if let Ok(map) = self.by_dir.lock()
            && let Some(c) = map.get(dir)
        {
            return Ok(c.clone());
        }
        let cfg = match find_config(dir)? {
            Some((path, file)) => {
                let root = path.parent().map(Path::to_path_buf).unwrap_or_default();
                Configuration::new(&root, Some(path), &file, &self.cli)?
            }
            None => Configuration::default_for(&self.fallback_root, &self.cli)?,
        };
        let cfg = std::sync::Arc::new(cfg);
        if let Ok(mut map) = self.by_dir.lock() {
            map.insert(dir.to_path_buf(), cfg.clone());
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(toml_text: &str, cli: CliOverrides) -> Configuration {
        let f = ConfigFile::from_toml_str(Path::new("sparklint.toml"), toml_text, false)
            .unwrap()
            .unwrap();
        Configuration::new(Path::new("/proj"), None, &f, &cli).unwrap()
    }

    #[test]
    fn default_preset() {
        let c = cfg("", CliOverrides::default());
        let s = c.base();
        assert!(s.is_enabled("ACT002"));
        assert!(!s.is_enabled("ACT001"));
        assert!(!s.is_enabled("STY001"));
    }

    #[test]
    fn select_prefix_tag_and_ignore() {
        let c = cfg(
            "select = [\"ACT\", \"tag:correctness\"]\nignore = [\"ACT00\"]\nextend-select = [\"ACT002\"]\n",
            CliOverrides::default(),
        );
        let s = c.base();
        assert!(
            s.is_enabled("ACT002"),
            "more specific select beats prefix ignore"
        );
        assert!(!s.is_enabled("ACT004"));
        assert!(s.is_enabled("ACT010"));
        assert!(s.is_enabled("COR001"));
        assert!(!s.is_enabled("PRF001"));
    }

    #[test]
    fn presets() {
        let c = cfg("preset = \"cylint-compat\"\n", CliOverrides::default());
        assert!(c.base().is_enabled("STY001"));
        let c = cfg("select = [\"review\"]\n", CliOverrides::default());
        assert!(c.base().is_enabled("ACT001"));
        assert!(!c.base().is_enabled("STY001"));
        let c = cfg("select = [\"strict\"]\n", CliOverrides::default());
        assert!(c.base().is_enabled("STY003"));
    }

    #[test]
    fn cli_select_overrides_config() {
        let c = cfg(
            "select = [\"ACT\"]\n",
            CliOverrides {
                select: Some(vec!["COR001".into()]),
                ignore: vec![],
                extend_select: vec![],
            },
        );
        assert!(c.base().is_enabled("COR001"));
        assert!(!c.base().is_enabled("ACT002"));
    }

    #[test]
    fn per_file_ignores_and_excludes() {
        let c = cfg(
            "[per-file-ignores]\n\"tests/**\" = [\"ACT\"]\n",
            CliOverrides::default(),
        );
        assert!(
            !c.settings_for(Path::new("/proj/tests/a.py"))
                .is_enabled("ACT002")
        );
        assert!(
            c.settings_for(Path::new("/proj/src/a.py"))
                .is_enabled("ACT002")
        );
        assert!(c.is_excluded(Path::new("/proj/.venv/lib/x.py")));
        assert!(c.is_excluded(Path::new("/proj/a/__pycache__/x.py")));
        assert!(!c.is_excluded(Path::new("/proj/src/x.py")));
    }

    #[test]
    fn severity_and_globals() {
        let c = cfg(
            "known-spark-globals = [\"ss\", \"ctx:SparkContext\"]\nspark-version = \"3.2\"\n[severity]\nACT = \"error\"\nACT002 = \"info\"\n",
            CliOverrides::default(),
        );
        let s = c.base();
        assert_eq!(s.severity_for("ACT002", Severity::Warning), Severity::Info);
        assert_eq!(s.severity_for("ACT004", Severity::Warning), Severity::Error);
        assert_eq!(s.known_globals[1], ("ctx".to_string(), Ty::SparkContext));
        assert_eq!(s.spark_version, (3, 2));
    }

    #[test]
    fn pyproject_section() {
        let f = ConfigFile::from_toml_str(
            Path::new("pyproject.toml"),
            "[project]\nname='x'\n[tool.sparklint]\nignore = [\"ACT002\"]\n",
            true,
        )
        .unwrap()
        .unwrap();
        assert_eq!(f.ignore, vec!["ACT002".to_string()]);
        let none =
            ConfigFile::from_toml_str(Path::new("pyproject.toml"), "[project]\nname='x'\n", true)
                .unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn unknown_selector_is_an_error() {
        assert!(Selector::parse("XYZ123").is_err());
        assert!(Selector::parse("collect-unbounded").is_ok());
    }
}
