//! The text a user reads about a finding: each rule's message variants.
//!
//! A rule declares every message it can report as a [`Msg`] (variant id, message
//! template, help template) and lists them on its table entry
//! (`Rule::new("CODE").….msgs(&[A, B])`); its hooks report through them with
//! [`msg!`](crate::msg!) (`ch.report(msg!(A, range, call = describe::call(ch, e)))`).
//! These declarations are the only copy of the wording: `spec/rule_messages.json`
//! (dumped from them by a test, `UPDATE_SPEC=1` rewrites it) feeds `scripts/gen_rules.py`,
//! so spec/rules.json, `sparklint rule CODE` and the docs site show the same variants.
//!
//! Templates use `format!` syntax restricted to named placeholders: `{call}` is
//! replaced by the argument named `call`, `{{` / `}}` are literal braces.

use std::fmt::{Display, Write};

use ruff_text_size::TextRange;

use crate::diagnostic::Diagnostic;

/// Named arguments for a template (`&[("call", &call)]`; [`msg!`](crate::msg!) builds it).
pub type Args<'a> = &'a [(&'static str, &'a dyn Display)];

/// One message variant of a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Msg {
    /// The rule reporting it.
    pub code: &'static str,
    /// Variant id, unique within the rule (kebab-case).
    pub id: &'static str,
    /// Message template.
    pub text: &'static str,
    /// Help template (the `help:` line under the finding).
    pub help: Option<&'static str>,
}

impl Msg {
    pub const fn new(code: &'static str, id: &'static str, text: &'static str) -> Self {
        Self {
            code,
            id,
            text,
            help: None,
        }
    }

    pub const fn help(mut self, help: &'static str) -> Self {
        self.help = Some(help);
        self
    }

    /// The message with `args` filled in.
    pub fn text(&self, args: Args<'_>) -> String {
        render(self.text, args)
    }

    /// The help line with `args` filled in.
    pub fn help_text(&self, args: Args<'_>) -> Option<String> {
        self.help.map(|h| render(h, args))
    }

    /// A diagnostic of this variant at `range` (the rule's default severity).
    pub fn diag(&self, range: TextRange, args: Args<'_>) -> Diagnostic {
        let mut d = Diagnostic::new(self.code, self.text(args), range);
        d.help = self.help_text(args);
        d.variant = Some(self.id);
        d
    }

    /// Whether `message` is this variant's template with some argument values.
    pub fn matches(&self, message: &str) -> bool {
        match parse(self.text) {
            Ok(parts) => matches_parts(&parts, message),
            Err(_) => false,
        }
    }
}

/// A template piece: literal text or a placeholder name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Part<'a> {
    Lit(String),
    Arg(&'a str),
}

fn is_name(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !s.as_bytes()[0].is_ascii_digit()
}

/// Split a template into literal text and placeholders; an unescaped brace that does
/// not open or close a `{name}` placeholder is an error.
pub fn parse(template: &str) -> Result<Vec<Part<'_>>, String> {
    let mut parts = Vec::new();
    let mut lit = String::new();
    let mut rest = template;
    while let Some(i) = rest.find(['{', '}']) {
        lit.push_str(&rest[..i]);
        let tail = &rest[i..];
        if let Some(t) = tail.strip_prefix("{{") {
            lit.push('{');
            rest = t;
        } else if let Some(t) = tail.strip_prefix("}}") {
            lit.push('}');
            rest = t;
        } else if tail.starts_with('}') {
            return Err(format!("unmatched `}}` in {template:?}"));
        } else {
            let Some(end) = tail.find('}') else {
                return Err(format!("unclosed `{{` in {template:?}"));
            };
            let name = &tail[1..end];
            if !is_name(name) {
                return Err(format!(
                    "`{{{name}}}` is not a named placeholder in {template:?} (write `{{{{` for a brace)"
                ));
            }
            if !lit.is_empty() {
                parts.push(Part::Lit(std::mem::take(&mut lit)));
            }
            parts.push(Part::Arg(name));
            rest = &tail[end + 1..];
        }
    }
    lit.push_str(rest);
    if !lit.is_empty() {
        parts.push(Part::Lit(lit));
    }
    Ok(parts)
}

/// The placeholder names of a template, in order.
pub fn placeholders(template: &str) -> Vec<&str> {
    parse(template)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|p| match p {
            Part::Arg(a) => Some(a),
            Part::Lit(_) => None,
        })
        .collect()
}

/// The JSON of `spec/rule_messages.json`: per rule code (sorted), its variants in
/// declaration order (`{"id", "message", "help"}`, templates as [`display`] shows them).
pub fn dump<'a>(msgs: impl Iterator<Item = &'a Msg>) -> String {
    let mut by_code: std::collections::BTreeMap<&str, Vec<serde_json::Value>> =
        std::collections::BTreeMap::new();
    for m in msgs {
        by_code.entry(m.code).or_default().push(serde_json::json!({
            "id": m.id,
            "message": display(m.text),
            "help": m.help.map(display),
        }));
    }
    let mut out = serde_json::to_string_pretty(&by_code).expect("message dump");
    out.push('\n');
    out
}

/// A template as the docs show it: literal braces unescaped, placeholders kept.
pub fn display(template: &str) -> String {
    template.replace("{{", "{").replace("}}", "}")
}

fn render(template: &str, args: Args<'_>) -> String {
    let parts = match parse(template) {
        Ok(p) => p,
        Err(e) => {
            debug_assert!(false, "{e}");
            return template.to_string();
        }
    };
    let mut out = String::with_capacity(template.len() + 32);
    for p in parts {
        match p {
            Part::Lit(s) => out.push_str(&s),
            Part::Arg(name) => match args.iter().find(|(n, _)| *n == name) {
                Some((_, v)) => {
                    let _ = write!(out, "{v}");
                }
                None => {
                    debug_assert!(false, "no value for `{{{name}}}` in {template:?}");
                    let _ = write!(out, "{{{name}}}");
                }
            },
        }
    }
    out
}

fn matches_parts(parts: &[Part<'_>], s: &str) -> bool {
    match parts.split_first() {
        None => s.is_empty(),
        Some((Part::Lit(l), rest)) => s
            .strip_prefix(l.as_str())
            .is_some_and(|t| matches_parts(rest, t)),
        // a placeholder takes any text, possibly none (backtracking)
        Some((Part::Arg(_), rest)) => s
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(s.len()))
            .any(|i| matches_parts(rest, &s[i..])),
    }
}

/// Report through a [`Msg`]: `msg!(M, range, call = describe::call(ch, e), n)` is
/// `M.diag(range, &[("call", &describe::call(ch, e)), ("n", &n)])`.
#[macro_export]
macro_rules! msg {
    (@v $k:ident) => {
        $k
    };
    (@v $k:ident $v:expr) => {
        $v
    };
    (@args $($k:ident $(= $v:expr)?),*) => {
        &[$((stringify!($k), &$crate::msg!(@v $k $($v)?) as &dyn ::std::fmt::Display)),*]
    };
    ($m:expr, $range:expr $(, $k:ident $(= $v:expr)?)* $(,)?) => {
        $m.diag($range, $crate::msg!(@args $($k $(= $v)?),*))
    };
}

/// A config-value verdict's [`Finding`](crate::rules::conf::Finding) of a [`Msg`]:
/// `finding!(M, key, value = v)`.
#[macro_export]
macro_rules! finding {
    ($m:expr $(, $k:ident $(= $v:expr)?)* $(,)?) => {
        $crate::rules::conf::Finding::of(&$m, $crate::msg!(@args $($k $(= $v)?),*))
    };
}

/// A [`Msg`]'s text with arguments filled in: `msg_text!(M, call = c)`.
#[macro_export]
macro_rules! msg_text {
    ($m:expr $(, $k:ident $(= $v:expr)?)* $(,)?) => {
        $m.text($crate::msg!(@args $($k $(= $v)?),*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    const M: Msg = Msg::new("ACT001", "main", "{call} triggers a job (kind: {kind}).")
        .help("Use {{a: b}} for {call}.");

    #[test]
    fn renders_named_placeholders_and_escapes() {
        let call = "df.count()";
        let d = crate::msg!(M, TextRange::default(), call, kind = "job");
        assert_eq!(d.message, "df.count() triggers a job (kind: job).");
        assert_eq!(d.help.as_deref(), Some("Use {a: b} for df.count()."));
        assert_eq!(d.variant, Some("main"));
        assert_eq!(placeholders(M.text), ["call", "kind"]);
        assert_eq!(display(M.help.unwrap()), "Use {a: b} for {call}.");
    }

    #[test]
    fn matches_rendered_text() {
        assert!(M.matches("orders.collect() triggers a job (kind: action)."));
        assert!(!M.matches("orders.collect() triggers a job."));
        assert!(M.matches(" triggers a job (kind: )."));
        let plain = Msg::new("X", "x", "Plain.");
        assert!(plain.matches("Plain.") && !plain.matches("Plain. "));
    }

    #[test]
    fn bad_templates_are_errors() {
        for t in ["a { b", "a } b", "{Call}", "{df.count()}", "{}"] {
            assert!(parse(t).is_err(), "{t}");
        }
        assert!(parse("{{x}} {y}").is_ok());
    }
}
