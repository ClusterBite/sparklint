//! Rule references: the one grammar for naming rules, shared by the configuration
//! (`select`, `ignore`, `per-file-ignores`, `[severity]` keys, `--select`) and the
//! suppression comments (`# noqa: ..`, `# sparklint: ignore[..]` / `file-ignore[..]`,
//! `# cy:ignore ..`).
//!
//! A token is one of: a rule code (`ACT002`), a code prefix (`ACT`, `ACT0`), a rule
//! name (`collect-unbounded`), a tag (`tag:cost`), a preset (`default`, `review`,
//! `strict`, `cylint-compat`, `serverless`), `ALL`, or a cylint code (`CY001`, mapped
//! through RULES_CATALOG §4). Anything else is unrecognised: the configuration
//! rejects it and a comment reports it (the CLI warns); it never widens to "all
//! rules". Each form keeps its own syntax (brackets, colon); only the meaning of the
//! tokens lives here.

use crate::registry::{self, RuleMeta};

/// cylint code → sparklint code (RULES_CATALOG §4).
pub const CYLINT_MAP: &[(&str, &str)] = &[
    ("CY001", "ACT002"),
    ("CY002", "UDF001"),
    ("CY003", "PRF001"),
    ("CY004", "PRF022"),
    ("CY005", "PRF008"),
    ("CY006", "ACT003"),
    ("CY007", "PRF003"),
    ("CY008", "PRF010"),
    ("CY009", "UDF002"),
    ("CY010", "STY002"),
    ("CY011", "PRF002"),
    ("CY012", "ACT010"),
    ("CY013", "IOD003"),
    ("CY014", "PRF006"),
    ("CY015", "PRF004"),
    ("CY016", "COR016"),
    ("CY017", "PRF005"),
    ("CY018", "IOD001"),
    ("CY020", "ACT006"),
    ("CY021", "ACT007"),
    ("CY025", "PRF009"),
    ("CY031", "ACT004"),
    ("CY032", "STY003"),
    ("CY033", "PRF015"),
    ("CY034", "ACT011"),
    ("CY035", "COR010"),
    ("CY036", "IOD004"),
    ("CY037", "STY001"),
    ("CY038", "PRF012"),
];

/// Map a cylint code to its sparklint code.
pub fn map_cylint(code: &str) -> Option<&'static str> {
    CYLINT_MAP
        .iter()
        .find(|(cy, _)| cy.eq_ignore_ascii_case(code))
        .map(|(_, s)| *s)
}

/// A rule selector: what one rule-reference token names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    All,
    Preset(Preset),
    Tag(String),
    /// A code prefix (`ACT`, `ACT0`) or full code (`ACT002`); rule names and cylint
    /// codes resolve to their code.
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
    /// Read one token; `None` = unrecognised.
    pub fn read(s: &str) -> Option<Self> {
        let t = s.trim();
        if t.is_empty() {
            return None;
        }
        if t.eq_ignore_ascii_case("all") {
            return Some(Selector::All);
        }
        if let Some(tag) = strip_prefix_ci(t, "tag:") {
            let tag = tag.trim().to_ascii_lowercase();
            let known = registry::rules().iter().any(|r| r.tags.contains(&tag));
            return known.then_some(Selector::Tag(tag));
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
            return Some(Selector::Preset(p));
        }
        // code or kebab rule name
        if let Some(r) = registry::get(t) {
            return Some(Selector::Prefix(r.code.clone()));
        }
        if let Some(code) = map_cylint(t) {
            return Some(Selector::Prefix(code.to_string()));
        }
        let upper = t.to_ascii_uppercase();
        if registry::rules().iter().any(|r| r.code.starts_with(&upper)) {
            return Some(Selector::Prefix(upper));
        }
        None
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

    /// Whether the selector names the rule with this code (a code outside the
    /// registry is only matched by `ALL` and a prefix of it).
    pub fn matches_code(&self, code: &str) -> bool {
        match registry::get(code) {
            Some(r) => self.matches(r).is_some(),
            None => match self {
                Selector::All => true,
                Selector::Prefix(p) => code.starts_with(p.as_str()),
                _ => false,
            },
        }
    }
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &s[prefix.len()..])
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

/// Whether an unrecognised token is another linter's code (`E501`, `F401`,
/// `PLR0913`): upper-case letters + digits whose letters are neither a sparklint
/// family nor `CY`. `# noqa` comments are shared with flake8 / ruff, so such codes are
/// skipped there without a warning (like ruff's `external` codes).
pub fn is_foreign_code(tok: &str) -> bool {
    let letters: &str = tok.trim_end_matches(|c: char| c.is_ascii_digit());
    if letters.is_empty() || !letters.bytes().all(|b| b.is_ascii_uppercase()) {
        return false;
    }
    if letters.starts_with("CY") {
        return false;
    }
    !registry::rules().iter().any(|r| r.prefix() == letters)
}

/// The result of reading a list of rule-reference tokens.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleRefs {
    pub selectors: Vec<Selector>,
    /// Tokens that name no rule, as written.
    pub unknown: Vec<String>,
    /// Whether the list held any token at all.
    pub any_token: bool,
}

/// How a comment list ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListEnd {
    /// Inside brackets: every token is a reference.
    Bracket,
    /// After `noqa:` / `cy:ignore`: the list goes on while tokens are joined by a
    /// comma or look like a code (`ACT002 PRF001`); the first other word starts free
    /// text (`# noqa: ACT002 - small lookup table`).
    Open,
}

/// Read the rule references of a comment list. `foreign_ok`: skip other linters'
/// codes silently ([`is_foreign_code`], `# noqa` only).
pub fn read_list(s: &str, end: ListEnd, foreign_ok: bool) -> RuleRefs {
    let mut out = RuleRefs::default();
    let mut comma = true; // the first token always belongs to the list
    for piece in s.split(',') {
        for (i, tok) in piece.split_whitespace().enumerate() {
            let joined = comma && i == 0;
            if end == ListEnd::Open && !joined && !code_shaped(tok) {
                return out;
            }
            out.any_token = true;
            match Selector::read(tok) {
                Some(sel) => out.selectors.push(sel),
                None if foreign_ok && is_foreign_code(tok) => {}
                None => out.unknown.push(tok.to_string()),
            }
        }
        comma = true;
    }
    out
}

/// `ACT002`, `PRF`, `ALL`, `cy001`: a token that continues an open list without a
/// comma.
fn code_shaped(tok: &str) -> bool {
    let letters = tok.trim_end_matches(|c: char| c.is_ascii_digit());
    if letters.is_empty() || !letters.bytes().all(|b| b.is_ascii_alphabetic()) {
        return false;
    }
    letters.len() < tok.len() || letters.bytes().all(|b| b.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codes(sel: &Selector) -> Vec<&'static str> {
        registry::rules()
            .iter()
            .filter(|r| sel.matches(r).is_some())
            .map(|r| r.code.as_str())
            .collect()
    }

    /// token → the rules it names (`None` = unrecognised).
    #[test]
    fn token_table() {
        let n_all = registry::rules().len();
        let cases: &[(&str, Option<&[&str]>)] = &[
            ("ACT002", Some(&["ACT002"])),
            ("act002", Some(&["ACT002"])),
            (" PRF004 ", Some(&["PRF004"])),
            ("collect-unbounded", Some(&["ACT002"])),
            ("CY001", Some(&["ACT002"])),
            ("cy015", Some(&["PRF004"])),
            ("CY019", None),
            ("CY999", None),
            ("tag:nope", None),
            ("collect-without-limit", None),
            ("XYZ123", None),
            ("E501", None),
            ("", None),
            ("ACT9", None),
        ];
        for (tok, want) in cases {
            let got = Selector::read(tok).map(|s| codes(&s));
            assert_eq!(got.as_deref(), *want, "token `{tok}`");
        }
        // Wider selectors: check membership rather than spelling out the lists.
        let all = Selector::read("ALL").unwrap();
        assert_eq!(codes(&all).len(), n_all);
        assert_eq!(Selector::read("all"), Some(Selector::All));
        let act = codes(&Selector::read("ACT").unwrap());
        assert!(act.contains(&"ACT002") && act.iter().all(|c| c.starts_with("ACT")));
        let cost = codes(&Selector::read("tag:cost").unwrap());
        assert!(!cost.is_empty() && cost.len() < n_all);
        assert_eq!(Selector::read("TAG:cost"), Selector::read("tag:cost"));
        let def = codes(&Selector::read("default").unwrap());
        assert!(def.contains(&"ACT002") && !def.contains(&"ACT001"));
        assert_eq!(
            Selector::read("cylint"),
            Some(Selector::Preset(Preset::CylintCompat))
        );
        assert_eq!(
            Selector::read("uc-migration"),
            Some(Selector::Preset(Preset::Serverless))
        );
        assert!(Selector::read("strict").unwrap().matches_code("STY003"));
        assert!(Selector::read("ACT").unwrap().matches_code("ACT999"));
        assert!(!Selector::read("ACT002").unwrap().matches_code("ACT004"));
    }

    #[test]
    fn foreign_codes() {
        for t in ["E501", "F401", "PLR0913", "PD901", "TODO", "C901"] {
            assert!(is_foreign_code(t), "{t}");
        }
        for t in ["ACT999", "CY019", "e501", "collect-x", "tag:x", "ACT"] {
            assert!(!is_foreign_code(t), "{t}");
        }
    }

    #[test]
    fn lists() {
        let r = read_list(" ACT002, PRF001 CY001", ListEnd::Open, true);
        assert_eq!(r.selectors.len(), 3);
        assert!(r.unknown.is_empty());
        // free text after an open list
        let r = read_list(" ACT002 - small lookup", ListEnd::Open, true);
        assert_eq!(r.selectors, vec![Selector::Prefix("ACT002".into())]);
        let r = read_list(" collect-without-limit because", ListEnd::Open, true);
        assert!(r.selectors.is_empty() && r.any_token);
        assert_eq!(r.unknown, vec!["collect-without-limit".to_string()]);
        let r = read_list(" E501, ACT002", ListEnd::Open, true);
        assert_eq!(r.selectors.len(), 1);
        assert!(r.unknown.is_empty());
        let r = read_list("E501, tag:cost bogus", ListEnd::Bracket, false);
        assert_eq!(r.selectors.len(), 1);
        assert_eq!(r.unknown, vec!["E501".to_string(), "bogus".to_string()]);
        let r = read_list("  ", ListEnd::Bracket, false);
        assert!(!r.any_token);
    }
}
