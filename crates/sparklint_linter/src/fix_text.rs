//! Building fix text: the one place that decides parentheses, operator tokens,
//! multi-line refusals and fresh names for a fix.
//!
//! Rules state what they want ("this operand in parentheses", "this `and` becomes
//! `&`", "this text as a receiver") and get byte-preserving edits back:
//!
//! - [`FixText::parenthesized_range`]: the range of an expression including its own
//!   parentheses, from ruff's `parenthesized_range` over the file's tokens (comments,
//!   line breaks and nested parentheses handled by the tokenizer, never by text).
//! - [`FixText::wrap`]: insert-only parentheses around an expression, unless it is
//!   already parenthesised: comments and line breaks stay as written.
//! - [`FixText::replace_operator`]: replace an operator keyword token found in the gap
//!   between two operands (comments in the gap are tokens, never matched).
//! - [`FixText::operand`]: the source of an expression placed as a receiver / operand
//!   of a tighter operator (`x.isEmpty()`, `~x`), parenthesised where needed.
//! - [`one_line`] / [`FixText::spans_lines`]: refuse multi-line spans.
//! - [`fresh_name`]: a name the copied text does not use.
//!
//! Never rebuild a multi-operand node from operand slices joined by new text (that
//! drops comments between operands): edit the operator tokens and insert parentheses.

use ruff_python_ast::token::{TokenKind, Tokens, parenthesized_range};
use ruff_python_ast::{AnyNodeRef, Expr, ExprRef, OperatorPrecedence};
use ruff_text_size::{Ranged, TextRange};

use crate::checker::Node;
use crate::diagnostic::Edit;

/// Fix-text questions over one file's source and tokens.
#[derive(Clone, Copy)]
pub struct FixText<'a> {
    source: &'a str,
    tokens: &'a Tokens,
}

impl<'a> FixText<'a> {
    /// `source` must be the text `tokens` were lexed from.
    pub fn new(source: &'a str, tokens: &'a Tokens) -> Self {
        Self { source, tokens }
    }

    /// The range of `e` including the parentheses that belong to it (the outermost
    /// pair), `None` when `e` is not parenthesised. `parent` is the node directly
    /// containing `e` (its parentheses never count: `f(x)` is no parenthesised `x`).
    pub fn parenthesized_range<'e>(
        &self,
        e: &'e Expr,
        parent: impl Into<AnyNodeRef<'e>>,
    ) -> Option<TextRange> {
        parenthesized_range(ExprRef::from(e), parent.into(), self.tokens)
    }

    /// Whether `e` sits in its own parentheses inside `parent`.
    pub fn is_parenthesized<'e>(&self, e: &'e Expr, parent: impl Into<AnyNodeRef<'e>>) -> bool {
        self.parenthesized_range(e, parent).is_some()
    }

    /// `e`'s range including its own parentheses (its plain range when it has none).
    pub fn own_range<'e>(&self, e: &'e Expr, parent: impl Into<AnyNodeRef<'e>>) -> TextRange {
        self.parenthesized_range(e, parent)
            .unwrap_or_else(|| e.range())
    }

    /// Insert-only edits putting `range` in parentheses (comments and line breaks
    /// inside stay as written).
    pub fn wrap_range(range: TextRange) -> [Edit; 2] {
        [
            Edit::insert(range.start(), "("),
            Edit::insert(range.end(), ")"),
        ]
    }

    /// Insert-only edits putting `e` in parentheses, none when it already has its own
    /// or is `atomic` (see [`is_atomic`]).
    pub fn wrap<'e>(&self, e: &'e Expr, parent: impl Into<AnyNodeRef<'e>>) -> Vec<Edit> {
        if is_atomic(e) || self.is_parenthesized(e, parent) {
            return Vec::new();
        }
        Self::wrap_range(e.range()).into()
    }

    /// The range of the first operator token of `kind` in `gap` (the text between two
    /// operands: parentheses, line breaks, comments and the operator).
    pub fn operator_in(&self, gap: TextRange, kind: TokenKind) -> Option<TextRange> {
        self.tokens
            .in_range(gap)
            .iter()
            .find(|t| t.kind() == kind)
            .map(Ranged::range)
    }

    /// An edit replacing the operator token `kind` in `gap` with `with`.
    pub fn replace_operator(&self, gap: TextRange, kind: TokenKind, with: &str) -> Option<Edit> {
        self.operator_in(gap, kind)
            .map(|r| Edit::replace(r, with.to_string()))
    }

    /// Whether the source of `range` spans several lines.
    pub fn spans_lines(&self, range: TextRange) -> bool {
        !one_line(&[&self.source[range]])
    }

    /// The source of `e` to be placed as the receiver of `.method()` or the operand
    /// of a tight unary/binary operator (`~x`): parenthesised unless atomic. An atomic
    /// expression that spans lines inside its own parentheses keeps them (its line
    /// breaks are only valid inside brackets).
    pub fn operand(&self, e: &Expr) -> String {
        let s = &self.source[e.range()];
        if is_atomic(e) && !(self.spans_lines(e.range()) && self.parenthesized_anywhere(e)) {
            s.to_string()
        } else {
            format!("({s})")
        }
    }

    /// The source of `e` placed where an expression binding at least as tightly as
    /// `prec` is required (`for v in <iter>` needs `Or`): parenthesised when `e` binds
    /// looser (a lambda, ternary or walrus there).
    pub fn at_least(&self, e: &Expr, prec: OperatorPrecedence) -> String {
        let s = &self.source[e.range()];
        if OperatorPrecedence::from_expr(e) < prec {
            format!("({s})")
        } else {
            s.to_string()
        }
    }

    /// `e` directly enclosed by a `(` .. `)` token pair. Without the parent node the
    /// pair may be a call's own parentheses (`f(e)`); callers use it only to decide to
    /// add parentheses, which is harmless then.
    fn parenthesized_anywhere(&self, e: &Expr) -> bool {
        let before = self.tokens.before(e.start());
        let after = self.tokens.after(e.end());
        let lpar = before
            .iter()
            .rev()
            .find(|t| !t.kind().is_trivia())
            .is_some_and(|t| t.kind() == TokenKind::Lpar);
        let rpar = after
            .iter()
            .find(|t| !t.kind().is_trivia())
            .is_some_and(|t| t.kind() == TokenKind::Rpar);
        lpar && rpar
    }
}

/// Expressions that never need parentheses when used as a receiver / operand.
pub fn is_atomic(e: &Expr) -> bool {
    matches!(
        e,
        Expr::Name(_)
            | Expr::Attribute(_)
            | Expr::Call(_)
            | Expr::Subscript(_)
            | Expr::StringLiteral(_)
            | Expr::NumberLiteral(_)
            | Expr::BooleanLiteral(_)
            | Expr::NoneLiteral(_)
            | Expr::List(_)
            | Expr::Dict(_)
            | Expr::Set(_)
            | Expr::ListComp(_)
            | Expr::DictComp(_)
            | Expr::SetComp(_)
            | Expr::FString(_)
    ) || matches!(e, Expr::Tuple(t) if t.parenthesized)
        || matches!(e, Expr::Generator(g) if g.parenthesized)
}

/// Whether `not <x>` put where a node with this `parent` stands needs parentheses:
/// every parent that binds tighter than `not` (operators, comparisons, attribute /
/// subscript / await / starred).
pub fn not_needs_parens(parent: Option<Node>) -> bool {
    matches!(
        parent,
        Some(Node::Expr(
            Expr::BinOp(_)
                | Expr::UnaryOp(_)
                | Expr::Compare(_)
                | Expr::Attribute(_)
                | Expr::Subscript(_)
                | Expr::Await(_)
                | Expr::Starred(_)
        ))
    )
}

/// Whether every text is on one line (fixes that lay copied pieces out on a new line
/// refuse multi-line pieces).
pub fn one_line(texts: &[&str]) -> bool {
    !texts.iter().any(|s| s.contains('\n') || s.contains('\r'))
}

/// The first of `candidates` that is not `taken` and no word of `text` uses (a name
/// for a new comprehension variable next to copied code).
pub fn fresh_name(text: &str, taken: &[&str], candidates: &[&'static str]) -> Option<&'static str> {
    let words: Vec<&str> = text
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .collect();
    candidates
        .iter()
        .copied()
        .find(|v| !taken.contains(v) && !words.contains(v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruff_python_ast::visitor::{self, Visitor};
    use ruff_python_ast::{AnyNodeRef, Expr};
    use ruff_python_parser::parse_module;

    /// Every (expression, parent) pair of `src` whose source is `needle`.
    fn find<'a>(
        module: &'a ruff_python_ast::ModModule,
        src: &str,
        needle: &str,
    ) -> Vec<(&'a Expr, AnyNodeRef<'a>)> {
        struct V<'a, 's> {
            src: &'s str,
            needle: &'s str,
            stack: Vec<AnyNodeRef<'a>>,
            out: Vec<(&'a Expr, AnyNodeRef<'a>)>,
        }
        impl<'a> Visitor<'a> for V<'a, '_> {
            fn visit_stmt(&mut self, s: &'a ruff_python_ast::Stmt) {
                self.stack.push(AnyNodeRef::from(s));
                visitor::walk_stmt(self, s);
                self.stack.pop();
            }
            fn visit_expr(&mut self, e: &'a Expr) {
                if &self.src[e.range()] == self.needle
                    && let Some(p) = self.stack.last()
                {
                    self.out.push((e, *p));
                }
                self.stack.push(AnyNodeRef::from(e));
                visitor::walk_expr(self, e);
                self.stack.pop();
            }
            fn visit_arguments(&mut self, a: &'a ruff_python_ast::Arguments) {
                self.stack.push(AnyNodeRef::from(a));
                visitor::walk_arguments(self, a);
                self.stack.pop();
            }
        }
        let mut v = V {
            src,
            needle,
            stack: Vec::new(),
            out: Vec::new(),
        };
        v.visit_body(&module.body);
        v.out
    }

    fn apply(src: &str, edits: &[Edit]) -> String {
        let mut edits = edits.to_vec();
        edits.sort_by_key(|e| (e.range.start(), e.range.end()));
        let mut out = String::new();
        let mut last = 0;
        for e in &edits {
            out.push_str(&src[last..usize::from(e.range.start())]);
            out.push_str(&e.content);
            last = usize::from(e.range.end());
        }
        out.push_str(&src[last..]);
        out
    }

    /// Tricky layouts: (source, operand text, parenthesised range text or "").
    #[test]
    fn parenthesized_range_layouts() {
        let cases: &[(&str, &str, &str)] = &[
            ("a > (x & y)\n", "x & y", "(x & y)"),
            ("a > x & y\n", "x & y", ""),
            // a comment before the closing parenthesis (COR003's old text check missed it)
            ("a > (x & y  # note\n)\n", "x & y", "(x & y  # note\n)"),
            (
                "a > (  # lead\n    x & y)\n",
                "x & y",
                "(  # lead\n    x & y)",
            ),
            // nested pairs: the outermost one that belongs to the expression
            ("a > ((x & y))\n", "x & y", "((x & y))"),
            // a call's parentheses are not the argument's
            ("f(x & y)\n", "x & y", ""),
            ("f((x & y))\n", "x & y", "(x & y)"),
            // continuation lines
            ("z = a > \\\n    (x & y)\n", "x & y", "(x & y)"),
            ("z = (a and\n     (b))\n", "b", "(b)"),
            ("z = (a and\n     b)\n", "b", ""),
            // a tuple's parentheses are its own
            ("z = (a, b)\n", "a", ""),
        ];
        for (src, needle, want) in cases {
            let parsed = parse_module(src).unwrap();
            let ft = FixText::new(src, parsed.tokens());
            let found = find(parsed.syntax(), src, needle);
            assert!(!found.is_empty(), "{needle:?} not found in {src:?}");
            let (e, parent) = found[0];
            let got = ft.parenthesized_range(e, parent).map_or("", |r| &src[r]);
            assert_eq!(got, *want, "{needle:?} in {src:?}");
        }
    }

    #[test]
    fn wrap_and_operators() {
        // (source, operand to wrap, expected after wrapping)
        let wraps: &[(&str, &str, &str)] = &[
            ("c = a and b > 1\n", "b > 1", "c = a and (b > 1)\n"),
            ("c = a and (b > 1)\n", "b > 1", "c = a and (b > 1)\n"),
            (
                "c = a and (  # why\n    b > 1)\n",
                "b > 1",
                "c = a and (  # why\n    b > 1)\n",
            ),
            ("c = a and b\n", "b", "c = a and b\n"),
            (
                "c = (a and\n     b > 1  # note\n)\n",
                "b > 1",
                "c = (a and\n     (b > 1)  # note\n)\n",
            ),
        ];
        for (src, needle, want) in wraps {
            let parsed = parse_module(src).unwrap();
            let ft = FixText::new(src, parsed.tokens());
            let (e, parent) = find(parsed.syntax(), src, needle)[0];
            assert_eq!(apply(src, &ft.wrap(e, parent)), *want, "{src:?}");
        }
        // The keyword in the gap, never one in a comment.
        let src = "c = (a  # a and b\n     and b)\n";
        let parsed = parse_module(src).unwrap();
        let ft = FixText::new(src, parsed.tokens());
        let a_end = ruff_text_size::TextSize::from(src.find("a  #").unwrap() as u32 + 1);
        let b_start = ruff_text_size::TextSize::from(src.rfind('b').unwrap() as u32);
        let gap = TextRange::new(a_end, b_start);
        let e = ft.replace_operator(gap, TokenKind::And, "&").unwrap();
        assert_eq!(apply(src, &[e]), "c = (a  # a and b\n     & b)\n");
        assert!(ft.operator_in(gap, TokenKind::Or).is_none());
    }

    #[test]
    fn operand_text() {
        let cases: &[(&str, &str, &str)] = &[
            ("x = df.count()\n", "df", "df"),
            ("x = (a + b).count()\n", "a + b", "(a + b)"),
            ("x = (df).count()\n", "df", "df"),
            // multi-line receiver: valid only inside its own parentheses
            (
                "x = (df\n     .filter(c)).count()\n",
                "df\n     .filter(c)",
                "(df\n     .filter(c))",
            ),
            // multi-line inside the call's brackets: valid as is
            (
                "x = df.filter(\n    c\n).count()\n",
                "df.filter(\n    c\n)",
                "df.filter(\n    c\n)",
            ),
        ];
        for (src, needle, want) in cases {
            let parsed = parse_module(src).unwrap();
            let ft = FixText::new(src, parsed.tokens());
            let (e, _) = find(parsed.syntax(), src, needle)[0];
            assert_eq!(ft.operand(e), *want, "{src:?}");
        }
    }

    #[test]
    fn names_and_lines() {
        assert_eq!(
            fresh_name("[v for v in x]", &["r"], &["v", "value"]),
            Some("value")
        );
        assert_eq!(
            fresh_name("r.items", &["v"], &["v", "value"]),
            Some("value")
        );
        assert_eq!(fresh_name("v value", &[], &["v", "value"]), None);
        assert!(one_line(&["a", "b.c(d)"]));
        assert!(!one_line(&["a", "b.c(\n d)"]));
    }
}
