//! Rule registry: metadata for every catalog rule (generated `spec/rules.json`, from
//! docs/RULES_CATALOG.md and the rule table's message variants via
//! `scripts/gen_rules.py`) plus which ones are implemented.
//!
//! Every catalog rule is registered; `implemented` says whether the rule table
//! (`crate::rules::TABLE`) runs it.

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::diagnostic::Severity;

static RULES_JSON: &str = include_str!("../../../spec/rules.json");
/// SYN001's metadata (not a catalog rule; also read by `scripts/gen_docs.py`).
static SYNTAX_ERROR_JSON: &str = include_str!("../../../spec/syntax_error.json");

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
    #[serde(default)]
    messages: Vec<MessageMeta>,
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

/// One message variant of an implemented rule, as declared in the rule table
/// ([`crate::rules::msg::Msg`]; templates with `{placeholders}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageMeta {
    pub id: String,
    pub message: String,
    pub help: Option<String>,
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
    /// The first message variant (or, for a planned rule, the catalog's Msg text).
    pub message: String,
    /// Every message variant (empty for a planned rule).
    pub messages: Vec<MessageMeta>,
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

fn meta(r: RawRule) -> RuleMeta {
    RuleMeta {
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
        messages: r.messages,
        docs: r.docs,
    }
}

fn load() -> Vec<RuleMeta> {
    let raw: RawRules = serde_json::from_str(RULES_JSON).unwrap_or(RawRules { rules: Vec::new() });
    let mut out: Vec<RuleMeta> = raw.rules.into_iter().map(meta).collect();
    let syn: RawRule = serde_json::from_str(SYNTAX_ERROR_JSON).expect("spec/syntax_error.json");
    let mut syn = meta(syn);
    syn.implemented = true;
    out.push(syn);
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

    #[test]
    fn every_implemented_rule_documents_its_message() {
        // `sparklint rule CODE`, the docs site and spec/rules.json show this text: a
        // catalog Msg item the generator cannot read leaves it empty.
        let empty: Vec<&str> = rules()
            .iter()
            .filter(|r| r.implemented && r.message.trim().is_empty())
            .map(|r| r.code.as_str())
            .collect();
        assert!(empty.is_empty(), "rules without a message: {empty:?}");
    }

    #[test]
    fn rules_json_messages_are_the_rule_tables() {
        // spec/rules.json is generated from spec/rule_messages.json, which a test
        // keeps equal to the table: stale here = gen_rules.py was not re-run.
        for r in rules()
            .iter()
            .filter(|r| r.implemented && r.code != SYNTAX_ERROR)
        {
            let want: Vec<MessageMeta> = crate::rules::messages_of(&r.code)
                .map(|m| MessageMeta {
                    id: m.id.to_string(),
                    message: crate::rules::msg::display(m.text),
                    help: m.help.map(crate::rules::msg::display),
                })
                .collect();
            assert!(!want.is_empty(), "{} declares no message", r.code);
            assert_eq!(
                r.messages, want,
                "{}: spec/rules.json is stale (python3 scripts/gen_rules.py > spec/rules.json)",
                r.code
            );
            assert_eq!(r.message, want[0].message, "{}", r.code);
        }
    }

    #[test]
    fn syntax_error_meta_matches_the_diagnostic() {
        // SYN001 comes from spec/syntax_error.json (shared with the docs generator).
        let r = get(SYNTAX_ERROR).unwrap();
        assert_eq!(get("syntax-error").unwrap().code, SYNTAX_ERROR);
        assert!(r.implemented && r.default_on && r.cylint_compat_on);
        assert_eq!(r.severity, Severity::Error);
        assert_eq!(r.fix, FixAvailability::None);
        assert_eq!(r.family, "Syntax");
        // The documented message is the one `syntax_diagnostics` writes.
        assert!(r.message.starts_with("Syntax error: "), "{}", r.message);
        assert!(r.docs.starts_with("syntax · error · on · T0 · none\n"));
        for part in [
            "- **Detect:**",
            "- **Not reported:**",
            "- **Fix:**",
            "`noqa`",
        ] {
            assert!(r.docs.contains(part), "{part}");
        }
    }
}
