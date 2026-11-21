//! Pieces of a string / bytes / f-string literal: one reader for every rule and the
//! oracle.
//!
//! [`pieces`] splits a literal expression (implicit concatenation included) into
//! [`Piece`]s: literal text (the body between a plain part's quotes, or one literal
//! run inside an f-string) and f-string holes (`{..}` replacement fields), each with
//! its part's [`Prefix`] (raw / bytes / u / f). On top of it:
//!
//! - [`static_text`]: the text with every hole as `{}` (SQL text, option values).
//! - [`holes`]: the interpolated expressions, format-spec ones included.
//! - [`escapes`]: the backslash escapes of a non-raw body, valid or not.
//! - [`Piece::raw_prefix_at`]: the one answer to "can this piece take an `r` prefix
//!   without changing what its reader sees?" (the Python value, or a regex engine).
//!
//! Never walk `FStringPartRef` / `InterpolatedStringElement` or compute a body range
//! from `opener_len` in a rule: ask this module.

use ruff_python_ast::{self as ast, AnyStringFlags, Expr, StringFlags};
use ruff_text_size::{Ranged, TextRange, TextSize};

/// The prefix facts of the part a piece belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Prefix {
    pub raw: bool,
    pub bytes: bool,
    /// `u"..."`
    pub unicode: bool,
    /// `f"..."`: the piece is literal text or a hole inside an f-string.
    pub fstring: bool,
}

impl Prefix {
    fn of(flags: AnyStringFlags) -> Self {
        let p = flags.prefix();
        let s = p.as_str();
        Prefix {
            raw: p.is_raw(),
            bytes: flags.is_byte_string(),
            unicode: s.contains(['u', 'U']),
            fstring: flags.is_interpolated_string(),
        }
    }
}

/// What a piece is.
#[derive(Debug, Clone, Copy)]
pub enum PieceKind<'e> {
    /// Literal text; `value` is its decoded text (`None` for bytes).
    Text { value: Option<&'e str> },
    /// An f-string replacement field.
    Hole(&'e ast::InterpolatedElement),
}

/// One piece of a literal.
#[derive(Debug, Clone, Copy)]
pub struct Piece<'e> {
    pub kind: PieceKind<'e>,
    /// The piece's source: a plain part's body (between the quotes), the literal run
    /// or the `{..}` field inside an f-string.
    pub range: TextRange,
    /// The whole part (prefix, quotes and body) the piece belongs to.
    pub part: TextRange,
    pub prefix: Prefix,
}

/// Who reads the value of a literal (see [`Piece::raw_prefix_at`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reader {
    /// Python itself: the value must stay byte for byte.
    Python,
    /// A regex engine: an escape may become the regex escape written in the source
    /// (`\n`, `\t`, `\x41` mean the same to it either way; `\b`, `\1` turn into the
    /// regex escape the author meant). `\\` and line continuations change it.
    Regex,
}

/// One backslash escape in a non-raw body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Escape {
    /// Byte offset of the backslash in the body.
    pub at: usize,
    /// The byte after the backslash.
    pub next: u8,
    /// A recognised escape (`\n`, `\x..`, octal, `\N{..}` / `\u` / `\U` outside bytes,
    /// a line continuation); an invalid one keeps its backslash in the value.
    pub valid: bool,
}

/// The backslash escapes of a non-raw literal body, in order.
pub fn escapes(body: &str, bytes: bool) -> impl Iterator<Item = Escape> + '_ {
    let b = body.as_bytes();
    let mut i = 0;
    std::iter::from_fn(move || {
        while i < b.len() {
            if b[i] != b'\\' {
                i += 1;
                continue;
            }
            let &next = b.get(i + 1)?;
            let valid = match next {
                b'\n'
                | b'\r'
                | b'\\'
                | b'\''
                | b'"'
                | b'a'
                | b'b'
                | b'f'
                | b'n'
                | b'r'
                | b't'
                | b'v'
                | b'x'
                | b'0'..=b'7' => true,
                b'N' | b'u' | b'U' => !bytes,
                _ => false,
            };
            let e = Escape { at: i, next, valid };
            i += 2;
            return Some(e);
        }
        None
    })
}

impl Piece<'_> {
    /// The piece's source text.
    pub fn text<'s>(&self, source: &'s str) -> &'s str {
        &source[self.range]
    }

    pub fn is_text(&self) -> bool {
        matches!(self.kind, PieceKind::Text { .. })
    }

    /// Where an `r` prefix can be inserted (the part's start) so that `reader` sees the
    /// same text: a plain (non-f-string, non-`u`) part that is not raw yet, does not end
    /// in a backslash, and whose every escape is invalid (kept as written by Python) or,
    /// for a regex, means the same to the engine (every escape but `\\` and a line
    /// continuation). `source` is the file text the ranges point into.
    pub fn raw_prefix_at(&self, source: &str, reader: Reader) -> Option<TextSize> {
        if !self.is_text() || self.prefix.fstring || self.prefix.raw || self.prefix.unicode {
            return None;
        }
        let body = self.text(source);
        if body.ends_with('\\') {
            return None;
        }
        let keeps = |e: Escape| {
            !e.valid || (reader == Reader::Regex && !matches!(e.next, b'\\' | b'\n' | b'\r'))
        };
        escapes(body, self.prefix.bytes)
            .all(keeps)
            .then_some(self.part.start())
    }
}

/// The body of a plain part (between its quotes); `None` for a part too short to have
/// quotes (an unterminated literal in broken code).
fn body_of(range: TextRange, flags: AnyStringFlags) -> Option<TextRange> {
    let (open, close) = (flags.opener_len(), flags.closer_len());
    (range.len() >= open + close).then(|| TextRange::new(range.start() + open, range.end() - close))
}

/// The pieces of a string, bytes or f-string literal (empty for anything else), in
/// source order.
pub fn pieces(e: &Expr) -> Vec<Piece<'_>> {
    fn plain<'e>(
        out: &mut Vec<Piece<'e>>,
        range: TextRange,
        flags: AnyStringFlags,
        value: Option<&'e str>,
    ) {
        if let Some(body) = body_of(range, flags) {
            out.push(Piece {
                kind: PieceKind::Text { value },
                range: body,
                part: range,
                prefix: Prefix::of(flags),
            });
        }
    }
    let mut out = Vec::new();
    match e {
        Expr::StringLiteral(s) => {
            for p in s.value.iter() {
                plain(
                    &mut out,
                    p.range,
                    p.flags.as_any_string_flags(),
                    Some(&*p.value),
                );
            }
        }
        Expr::BytesLiteral(s) => {
            for p in s.value.iter() {
                plain(&mut out, p.range, p.flags.as_any_string_flags(), None);
            }
        }
        Expr::FString(f) => {
            for part in f.value.iter() {
                match part {
                    ast::FStringPartRef::Literal(l) => {
                        plain(
                            &mut out,
                            l.range,
                            l.flags.as_any_string_flags(),
                            Some(&*l.value),
                        );
                    }
                    ast::FStringPartRef::FString(fs) => {
                        let prefix = Prefix::of(fs.flags.as_any_string_flags());
                        for el in &fs.elements {
                            let kind = match el {
                                ast::InterpolatedStringElement::Literal(l) => PieceKind::Text {
                                    value: Some(&*l.value),
                                },
                                ast::InterpolatedStringElement::Interpolation(i) => {
                                    PieceKind::Hole(i)
                                }
                            };
                            out.push(Piece {
                                kind,
                                range: el.range(),
                                part: fs.range,
                                prefix,
                            });
                        }
                    }
                }
            }
        }
        _ => {}
    }
    out
}

/// Static text of a string literal or f-string: holes become `{}` (format specs
/// dropped); `None` for anything else (bytes included).
pub fn static_text(e: &Expr) -> Option<String> {
    match e {
        Expr::StringLiteral(s) => Some(s.value.to_str().to_string()),
        Expr::FString(_) => Some(
            pieces(e)
                .iter()
                .map(|p| match p.kind {
                    PieceKind::Text { value } => value.unwrap_or_default(),
                    PieceKind::Hole(_) => "{}",
                })
                .collect(),
        ),
        _ => None,
    }
}

/// The interpolated expressions of an f-string (format-spec interpolations included),
/// in source order.
pub fn holes(e: &Expr) -> Vec<&Expr> {
    fn spec<'e>(i: &'e ast::InterpolatedElement, out: &mut Vec<&'e Expr>) {
        out.push(&i.expression);
        if let Some(s) = &i.format_spec {
            for el in &s.elements {
                if let ast::InterpolatedStringElement::Interpolation(n) = el {
                    spec(n, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    for p in pieces(e) {
        if let PieceKind::Hole(i) = p.kind {
            spec(i, &mut out);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruff_python_parser::parse_expression;

    /// (piece text, kind letter T/H, prefix letters) for every piece of `src`.
    fn shape(src: &str) -> Vec<(String, char, String)> {
        let parsed = parse_expression(src).unwrap();
        pieces(parsed.expr())
            .iter()
            .map(|p| {
                let k = if p.is_text() { 'T' } else { 'H' };
                let mut pre = String::new();
                for (on, c) in [
                    (p.prefix.raw, 'r'),
                    (p.prefix.bytes, 'b'),
                    (p.prefix.unicode, 'u'),
                    (p.prefix.fstring, 'f'),
                ] {
                    if on {
                        pre.push(c);
                    }
                }
                (p.text(src).to_string(), k, pre)
            })
            .collect()
    }

    fn s(t: &str, k: char, p: &str) -> (String, char, String) {
        (t.to_string(), k, p.to_string())
    }

    #[test]
    fn literal_shapes() {
        assert_eq!(shape(r#""a\d""#), vec![s(r"a\d", 'T', "")]);
        assert_eq!(
            shape(r#"r"a\d" "b""#),
            vec![s(r"a\d", 'T', "r"), s("b", 'T', "")]
        );
        assert_eq!(
            shape(r#"b"\x00" rb"\d""#),
            vec![s(r"\x00", 'T', "b"), s(r"\d", 'T', "rb")]
        );
        assert_eq!(shape(r#"u"x""#), vec![s("x", 'T', "u")]);
        assert_eq!(shape(r#"'''tri"ple'''"#), vec![s("tri\"ple", 'T', "")]);
        assert_eq!(
            shape(r#"f"a{x:>{w}}b" "c""#),
            vec![
                s("a", 'T', "f"),
                s("{x:>{w}}", 'H', "f"),
                s("b", 'T', "f"),
                s("c", 'T', ""),
            ]
        );
        assert_eq!(
            shape(r#"rf"\d{x}""#),
            vec![s(r"\d", 'T', "rf"), s("{x}", 'H', "rf")]
        );
        assert_eq!(shape("x"), vec![]);
    }

    #[test]
    fn text_and_holes() {
        let parsed = parse_expression(r#"f"SELECT {a} FROM {t:{w}}" " x""#).unwrap();
        let e = parsed.expr();
        assert_eq!(static_text(e).as_deref(), Some("SELECT {} FROM {} x"));
        let src = r#"f"SELECT {a} FROM {t:{w}}" " x""#;
        let hs: Vec<&str> = holes(e).iter().map(|h| &src[h.range()]).collect();
        assert_eq!(hs, vec!["a", "t", "w"]);
        let parsed = parse_expression(r#""a" 'b'"#).unwrap();
        assert_eq!(static_text(parsed.expr()).as_deref(), Some("ab"));
        let parsed = parse_expression(r#"b"a""#).unwrap();
        assert_eq!(static_text(parsed.expr()), None);
    }

    #[test]
    fn raw_prefix() {
        // (literal, reader, can take r)
        let cases: &[(&str, Reader, bool)] = &[
            (r#""\d+""#, Reader::Python, true),
            (r#""\d+\n""#, Reader::Python, false),
            (r#""\d+\n""#, Reader::Regex, true),
            (r#""\bword\b""#, Reader::Regex, true),
            (r#""\b\\d""#, Reader::Regex, false),
            ("\"\\b\\\n\"", Reader::Regex, false),
            (r#""\1""#, Reader::Regex, true),
            (r#"b"\d""#, Reader::Python, true),
            (r#"b"\N""#, Reader::Python, true),
            (r#""\N{EM DASH}""#, Reader::Python, false),
            (r#"u"\d""#, Reader::Python, false),
            (r#"r"\d""#, Reader::Python, false),
            (r#"f"\d{x}""#, Reader::Python, false),
            (r#""plain""#, Reader::Python, true),
        ];
        for (src, reader, want) in cases {
            let parsed = parse_expression(src).unwrap();
            let ps = pieces(parsed.expr());
            assert_eq!(
                ps[0].raw_prefix_at(src, *reader).is_some(),
                *want,
                "{src} {reader:?}"
            );
        }
        // escapes: offsets and validity
        let es: Vec<(usize, bool)> = escapes(r"a\d\n\\\q", false)
            .map(|e| (e.at, e.valid))
            .collect();
        assert_eq!(es, vec![(1, false), (3, true), (5, true), (7, false)]);
        let es: Vec<bool> = escapes(r"\N\u", true).map(|e| e.valid).collect();
        assert_eq!(es, vec![false, false]);
    }
}
