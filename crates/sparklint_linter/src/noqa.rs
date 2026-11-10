//! Suppression comments.
//!
//! Supported forms (case-insensitive keywords, codes upper-case):
//! - `# noqa` — every rule on the line; `# noqa: ACT002,PRF001` — listed codes
//!   (prefixes such as `ACT` are accepted too).
//! - `# sparklint: ignore` / `# sparklint: ignore[ACT002, PRF001]` — per line.
//! - `# sparklint: file-ignore` / `# sparklint: file-ignore[ACT002]` — whole file.
//! - cylint: `# cy:ignore` (all) / `# cy:ignore CY001,CY012` — mapped to sparklint
//!   codes through RULES_CATALOG §4.
//!
//! A diagnostic is suppressed when a matching comment sits on its first or last line.

use rustc_hash::FxHashMap;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Codes {
    All,
    Some(Vec<String>),
}

impl Codes {
    fn matches(&self, code: &str) -> bool {
        match self {
            Codes::All => true,
            Codes::Some(list) => list.iter().any(|c| code.starts_with(c.as_str())),
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
}

impl Suppressions {
    /// Build from `(line, comment text)` pairs.
    pub fn from_comments<'c>(comments: impl IntoIterator<Item = (u32, &'c str)>) -> Self {
        let mut s = Suppressions::default();
        for (line, text) in comments {
            for directive in parse_comment(text) {
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Directive {
    Line(Codes),
    File(Codes),
}

fn is_code_char(c: char) -> bool {
    c.is_ascii_alphanumeric()
}

/// Parse a code list such as `ACT002, PRF001 CY001` (stops at the first token that
/// is not a code). cylint codes are mapped.
fn parse_codes(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    for tok in s.split([',', ' ', '\t']) {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        if !tok.chars().all(is_code_char)
            || !tok.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        {
            break;
        }
        let upper = tok.to_ascii_uppercase();
        if let Some(mapped) = map_cylint(&upper) {
            out.push(mapped.to_string());
        } else if upper.starts_with("CY") && upper.len() == 5 {
            // unknown cylint code: ignore it
        } else {
            out.push(upper);
        }
    }
    out
}

/// Parse the directives in one comment (text including the leading `#`).
pub fn parse_comment(text: &str) -> Vec<Directive> {
    let mut out = Vec::new();
    // A comment can hold several `#` segments (`# type: ignore # noqa: X`).
    for seg in text.split('#').map(str::trim).filter(|s| !s.is_empty()) {
        let lower = seg.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("noqa") {
            let rest_orig = &seg[4..];
            if rest.starts_with(':') {
                let codes = parse_codes(&rest_orig[1..]);
                out.push(Directive::Line(if codes.is_empty() {
                    Codes::All
                } else {
                    Codes::Some(codes)
                }));
            } else if rest.is_empty() || rest.starts_with(char::is_whitespace) {
                out.push(Directive::Line(Codes::All));
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
                out.push(Directive::File(Codes::All));
                continue;
            } else {
                continue;
            };
            let after = &seg[pos + kw_len..];
            let codes = match after.trim_start().strip_prefix('[') {
                Some(inner) => {
                    let inner = inner.split(']').next().unwrap_or("");
                    let c = parse_codes(inner);
                    if c.is_empty() {
                        Codes::All
                    } else {
                        Codes::Some(c)
                    }
                }
                None => Codes::All,
            };
            out.push(if file {
                Directive::File(codes)
            } else {
                Directive::Line(codes)
            });
            continue;
        }
        if let Some(rest) = lower.strip_prefix("cy:ignore") {
            let orig = &seg[seg.len() - rest.len()..];
            if !(rest.is_empty() || rest.starts_with(char::is_whitespace)) {
                continue;
            }
            let codes = parse_codes(orig);
            let unmapped_only =
                orig.split([',', ' ']).any(|t| !t.trim().is_empty()) && codes.is_empty();
            if unmapped_only {
                // e.g. `# cy:ignore CY999` suppresses nothing
                continue;
            }
            out.push(Directive::Line(if codes.is_empty() {
                Codes::All
            } else {
                Codes::Some(codes)
            }));
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
    }

    #[test]
    fn noqa_not_a_prefix_of_other_words() {
        assert!(parse_comment("# noqanope").is_empty());
        assert_eq!(parse_comment("# type: ignore  # noqa: ACT002").len(), 1);
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
        assert_eq!(map_cylint("cy017"), Some("PRF005"));
    }
}
