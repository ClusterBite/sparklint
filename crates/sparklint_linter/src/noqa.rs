//! Suppression comments.
//!
//! Supported forms (case-insensitive keywords):
//! - `# noqa` — every rule on the line; `# noqa: ACT002,PRF001` — the listed rules.
//! - `# sparklint: ignore` / `# sparklint: ignore[ACT002, PRF001]` — per line.
//! - `# sparklint: file-ignore` / `# sparklint: file-ignore[ACT002]` — whole file.
//! - cylint: `# cy:ignore` (all) / `# cy:ignore CY001,CY012` — mapped to sparklint
//!   codes through RULES_CATALOG §4.
//!
//! The listed tokens follow the configuration's grammar ([`crate::rule_ref`]: codes,
//! prefixes, rule names, `tag:x`, presets, `ALL`, cylint codes). A token that names
//! no rule suppresses nothing and is reported ([`Suppressions::unknown`]); only a
//! comment without a list means every rule. Other linters' codes in `# noqa`
//! (`E501`) are skipped silently.
//!
//! A diagnostic is suppressed when a matching comment sits on its first or last line.

use rustc_hash::FxHashMap;

pub use crate::rule_ref::{CYLINT_MAP, map_cylint};
use crate::rule_ref::{ListEnd, Selector, read_list};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Codes {
    All,
    Some(Vec<Selector>),
}

impl Codes {
    fn matches(&self, code: &str) -> bool {
        match self {
            Codes::All => true,
            Codes::Some(list) => list.iter().any(|s| s.matches_code(code)),
        }
    }

    fn merge(&mut self, other: Codes) {
        match (&mut *self, other) {
            (Codes::All, _) => {}
            (_, Codes::All) => *self = Codes::All,
            (Codes::Some(a), Codes::Some(b)) => a.extend(b),
        }
    }
}

/// Parsed suppressions of one file.
#[derive(Debug, Clone, Default)]
pub struct Suppressions {
    lines: FxHashMap<u32, Codes>,
    file: Option<Codes>,
    unknown: Vec<(u32, String)>,
}

impl Suppressions {
    /// Build from `(line, comment text)` pairs.
    pub fn from_comments<'c>(comments: impl IntoIterator<Item = (u32, &'c str)>) -> Self {
        let mut s = Suppressions::default();
        for (line, text) in comments {
            let parsed = parse_comment(text);
            s.unknown
                .extend(parsed.unknown.into_iter().map(|t| (line, t)));
            for directive in parsed.directives {
                match directive {
                    Directive::Line(codes) => match s.lines.get_mut(&line) {
                        Some(c) => c.merge(codes),
                        None => {
                            s.lines.insert(line, codes);
                        }
                    },
                    Directive::File(codes) => match &mut s.file {
                        Some(c) => c.merge(codes),
                        None => s.file = Some(codes),
                    },
                }
            }
        }
        s
    }

    /// Whether a diagnostic of `code` spanning `first..=last` lines is suppressed.
    pub fn is_suppressed(&self, code: &str, first: u32, last: u32) -> bool {
        if self.file.as_ref().is_some_and(|c| c.matches(code)) {
            return true;
        }
        [first, last]
            .iter()
            .any(|l| self.lines.get(l).is_some_and(|c| c.matches(code)))
    }

    pub fn file_codes(&self) -> Option<&Codes> {
        self.file.as_ref()
    }

    /// Tokens in suppression comments that name no rule: `(1-based line, token)`.
    pub fn unknown(&self) -> &[(u32, String)] {
        &self.unknown
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Directive {
    Line(Codes),
    File(Codes),
}

/// The directives of one comment, plus the tokens in them that name no rule.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Comment {
    pub directives: Vec<Directive>,
    pub unknown: Vec<String>,
}

impl Comment {
    /// Add a directive whose list is `list` (`None` = no list = every rule).
    fn push(&mut self, file: bool, list: Option<(&str, ListEnd, bool)>) {
        let codes = match list {
            None => Codes::All,
            Some((text, end, foreign_ok)) => {
                let refs = read_list(text, end, foreign_ok);
                self.unknown.extend(refs.unknown);
                if refs.any_token {
                    Codes::Some(refs.selectors)
                } else {
                    // `# noqa:` / `ignore[]`: an empty list, nothing mistyped.
                    Codes::All
                }
            }
        };
        self.directives.push(if file {
            Directive::File(codes)
        } else {
            Directive::Line(codes)
        });
    }
}

/// Parse the directives in one comment (text including the leading `#`).
pub fn parse_comment(text: &str) -> Comment {
    let mut out = Comment::default();
    // A comment can hold several `#` segments (`# type: ignore # noqa: X`).
    for seg in text.split('#').map(str::trim).filter(|s| !s.is_empty()) {
        let lower = seg.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("noqa") {
            if rest.starts_with(':') {
                out.push(false, Some((&seg[5..], ListEnd::Open, true)));
            } else if rest.is_empty() || rest.starts_with(char::is_whitespace) {
                out.push(false, None);
            }
            continue;
        }
        if let Some(rest) = lower.strip_prefix("sparklint:") {
            let rest_l = rest.trim_start();
            let pos = seg.len() - rest_l.len();
            let (file, kw_len) = if rest_l.starts_with("file-ignore") {
                (true, "file-ignore".len())
            } else if rest_l.starts_with("ignore") {
                (false, "ignore".len())
            } else if rest_l.starts_with("noqa") {
                out.push(true, None);
                continue;
            } else {
                continue;
            };
            let after = &seg[pos + kw_len..];
            let list = after.trim_start().strip_prefix('[').map(|inner| {
                (
                    inner.split(']').next().unwrap_or(""),
                    ListEnd::Bracket,
                    false,
                )
            });
            out.push(file, list);
            continue;
        }
        if let Some(rest) = lower.strip_prefix("cy:ignore") {
            if !(rest.is_empty() || rest.starts_with(char::is_whitespace)) {
                continue;
            }
            let orig = &seg[seg.len() - rest.len()..];
            out.push(false, Some((orig, ListEnd::Open, false)));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sup(lines: &[(u32, &str)]) -> Suppressions {
        Suppressions::from_comments(lines.iter().copied())
    }

    #[test]
    fn noqa_forms() {
        let s = sup(&[
            (1, "# noqa"),
            (2, "# noqa: ACT002,PRF001"),
            (3, "# NOQA:ACT"),
            (4, "# noqa:"),
        ]);
        assert!(s.is_suppressed("ACT002", 1, 1));
        assert!(s.is_suppressed("PRF001", 2, 2));
        assert!(!s.is_suppressed("COR001", 2, 2));
        assert!(s.is_suppressed("ACT010", 3, 3));
        assert!(s.is_suppressed("COR001", 4, 4));
        assert!(!s.is_suppressed("ACT002", 5, 5));
        assert!(s.is_suppressed("ACT002", 5, 1), "last line counts");
        assert!(s.unknown().is_empty());
    }

    #[test]
    fn noqa_not_a_prefix_of_other_words() {
        assert!(parse_comment("# noqanope").directives.is_empty());
        assert_eq!(
            parse_comment("# type: ignore  # noqa: ACT002")
                .directives
                .len(),
            1
        );
    }

    #[test]
    fn sparklint_directives() {
        let s = sup(&[
            (1, "# sparklint: ignore[ACT002, PRF001]"),
            (2, "# sparklint: ignore"),
            (9, "# sparklint: file-ignore[COR001]"),
        ]);
        assert!(s.is_suppressed("ACT002", 1, 1));
        assert!(!s.is_suppressed("ACT004", 1, 1));
        assert!(s.is_suppressed("ACT004", 2, 2));
        assert!(s.is_suppressed("COR001", 40, 40));
        assert!(!s.is_suppressed("COR002", 40, 40));
        let s = sup(&[(3, "# sparklint: file-ignore")]);
        assert!(s.is_suppressed("ANY001", 1, 1));
    }

    #[test]
    fn cylint_aliases() {
        let s = sup(&[
            (1, "# cy:ignore CY001"),
            (2, "# cy:ignore"),
            (3, "# cy:ignore CY001 , CY012"),
            (4, "# cy:ignore CY999"),
        ]);
        assert!(s.is_suppressed("ACT002", 1, 1));
        assert!(!s.is_suppressed("ACT010", 1, 1));
        assert!(s.is_suppressed("PRF003", 2, 2));
        assert!(s.is_suppressed("ACT010", 3, 3));
        assert!(s.is_suppressed("ACT002", 3, 3));
        assert!(!s.is_suppressed("ACT002", 4, 4));
        assert_eq!(s.unknown(), &[(4, "CY999".to_string())]);
        assert_eq!(map_cylint("cy017"), Some("PRF005"));
    }

    /// The config grammar in comments: names, tags, presets, ALL, cylint codes;
    /// an unknown token suppresses nothing (it used to silence every rule) and is
    /// reported.
    #[test]
    fn shared_grammar_and_unknown_tokens() {
        let s = sup(&[
            (1, "# noqa: collect-unbounded"),
            (2, "# noqa: collect-without-limit"),
            (3, "# noqa: ALL"),
            (4, "# sparklint: ignore[tag:cost]"),
            (5, "# noqa: CY001"),
            (6, "# noqa: ACT002 - small lookup table"),
            (7, "# noqa: E501, F401"),
            (8, "# sparklint: ignore[bogus, ACT004]"),
            (9, "# noqa: ACT002, bogus"),
        ]);
        assert!(s.is_suppressed("ACT002", 1, 1));
        assert!(!s.is_suppressed("ACT004", 1, 1));
        assert!(!s.is_suppressed("ACT002", 2, 2), "unknown name: nothing");
        assert!(s.is_suppressed("COR001", 3, 3), "ALL = every rule");
        assert!(s.is_suppressed("ACT002", 5, 5));
        assert!(s.is_suppressed("ACT002", 6, 6));
        assert!(!s.is_suppressed("ACT004", 6, 6));
        assert!(!s.is_suppressed("ACT002", 7, 7));
        assert!(s.is_suppressed("ACT004", 8, 8));
        assert!(!s.is_suppressed("ACT002", 8, 8));
        assert!(s.is_suppressed("ACT002", 9, 9));
        let cost = crate::registry::rules()
            .iter()
            .find(|r| r.tags.iter().any(|t| t == "cost"))
            .unwrap();
        let other = crate::registry::rules()
            .iter()
            .find(|r| !r.tags.iter().any(|t| t == "cost"))
            .unwrap();
        assert!(s.is_suppressed(&cost.code, 4, 4));
        assert!(!s.is_suppressed(&other.code, 4, 4));
        assert_eq!(
            s.unknown(),
            &[
                (2, "collect-without-limit".to_string()),
                (8, "bogus".to_string()),
                (9, "bogus".to_string()),
            ]
        );
        // file-ignore with an unknown tag no longer silences the file
        let s = sup(&[(1, "# sparklint: file-ignore[tag:nope]")]);
        assert!(!s.is_suppressed("ACT002", 3, 3));
        assert_eq!(s.unknown(), &[(1, "tag:nope".to_string())]);
    }
}
