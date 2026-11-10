//! Rule registry: metadata for every catalog rule (generated `spec/rules.json`, from
//! docs/RULES_CATALOG.md via `scripts/gen_rules.py`) plus which ones are implemented.
//!
//! Every catalog rule is registered; a rule without an entry in the rule table
//! (`crate::rules::TABLE`) is marked `implemented: false` (`sparklint rules` shows it
//! as `planned`) and is never run (selecting it is allowed and simply has no effect).

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::diagnostic::Severity;

static RULES_JSON: &str = include_str!("../../../spec/rules.json");

/// Pseudo-rule for files that fail to parse.
pub const SYNTAX_ERROR: &str = "SYN001";

#[derive(Debug, Deserialize)]
struct RawRule {
    code: String,
    name: String,
    family: String,
    severity: String,
    default_on: bool,
    cylint_compat_on: bool,
    type_info: String,
    fix: String,
    tags: Vec<String>,
    message: String,
    docs: String,
}

#[derive(Debug, Deserialize)]
struct RawRules {
    rules: Vec<RawRule>,
}

/// Fix availability as documented in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FixAvailability {
    None,
    Safe,
    Unsafe,
    Sometimes,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleMeta {
    pub code: String,
    pub name: String,
    pub family: String,
    pub severity: Severity,
    pub default_on: bool,
    pub cylint_compat_on: bool,
    pub type_info: String,
    pub fix: FixAvailability,
    pub tags: Vec<String>,
    pub message: String,
    pub docs: String,
    pub implemented: bool,
}

impl RuleMeta {
    pub fn prefix(&self) -> &str {
        &self.code[..self.code.len().min(3)]
    }

    pub fn is_style(&self) -> bool {
        self.prefix() == "STY"
    }
}

static REGISTRY: LazyLock<Vec<RuleMeta>> = LazyLock::new(load);

fn load() -> Vec<RuleMeta> {
    let raw: RawRules = serde_json::from_str(RULES_JSON).unwrap_or(RawRules { rules: Vec::new() });
    let mut out: Vec<RuleMeta> = raw
        .rules
        .into_iter()
        .map(|r| RuleMeta {
            implemented: crate::rules::is_implemented(&r.code),
            severity: Severity::parse(&r.severity).unwrap_or(Severity::Warning),
            fix: match r.fix.as_str() {
                "safe" => FixAvailability::Safe,
                "unsafe" => FixAvailability::Unsafe,
                "none" => FixAvailability::None,
                _ => FixAvailability::Sometimes,
            },
            code: r.code,
            name: r.name,
            family: r.family,
            default_on: r.default_on,
            cylint_compat_on: r.cylint_compat_on,
            type_info: r.type_info,
            tags: r.tags,
            message: r.message,
            docs: r.docs,
        })
        .collect();
    out.push(RuleMeta {
        code: SYNTAX_ERROR.to_string(),
        name: "syntax-error".to_string(),
        family: "Syntax".to_string(),
        severity: Severity::Error,
        default_on: true,
        cylint_compat_on: true,
        type_info: "T0".to_string(),
        fix: FixAvailability::None,
        tags: vec!["correctness".to_string()],
        message: "The file could not be parsed; no rules were run on it.".to_string(),
        docs: "Reported when the Python parser finds a syntax error. sparklint does not run \
               rules on files with syntax errors (the recovered AST would produce unreliable \
               findings); fix the syntax error first."
            .to_string(),
        implemented: true,
    });
    out.sort_by(|a, b| a.code.cmp(&b.code));
    out
}

/// All registered rules, sorted by code.
pub fn rules() -> &'static [RuleMeta] {
    &REGISTRY
}

/// Look up a rule by code (`ACT002`) or kebab name (`collect-unbounded`).
pub fn get(code_or_name: &str) -> Option<&'static RuleMeta> {
    let upper = code_or_name.to_ascii_uppercase();
    REGISTRY
        .iter()
        .find(|r| r.code == upper || r.name == code_or_name)
}

/// The `'static` code string for a code.
pub fn static_code(code: &str) -> Option<&'static str> {
    get(code).map(|r| r.code.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_all_catalog_rules() {
        // Every catalog rule in spec/rules.json plus the SYN001 pseudo-rule.
        let raw: RawRules = serde_json::from_str(RULES_JSON).unwrap();
        assert!(!raw.rules.is_empty());
        assert_eq!(rules().len(), raw.rules.len() + 1);
        let r = get("ACT002").unwrap();
        assert_eq!(r.name, "collect-unbounded");
        assert_eq!(r.severity, Severity::Warning);
        assert!(r.default_on && r.implemented);
        assert!(get("collect-unbounded").is_some());
        assert!(!get("STY001").unwrap().default_on);
        assert!(get("STY001").unwrap().cylint_compat_on);
        assert!(!get("ACT001").unwrap().default_on);
        assert!(get("PRF022").unwrap().implemented);
        assert!(!get("PRF022").unwrap().default_on);
        for code in crate::rules::implemented_codes() {
            assert!(get(code).is_some(), "{code} not in catalog");
        }
        let n_impl = rules().iter().filter(|r| r.implemented).count();
        assert_eq!(n_impl, crate::rules::implemented_codes().count());
    }
}
