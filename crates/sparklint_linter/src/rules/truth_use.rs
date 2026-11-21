//! "Does Python test this value as a bool?" — one answer for every rule.
//!
//! [`truth_use`] (asked as `Checker::truth_use`) classifies how the value of the
//! current expression is used as a truth value:
//!
//! - [`TruthUse::Test`]: it decides a branch — the test of `if` / `elif` / `while` /
//!   `assert` / a ternary / a comprehension `if` / a `match` guard, reached directly or
//!   through a walrus (`if (n := x):`) and through `not` / `and` / `or` (any operand:
//!   the chain's truth is the test).
//! - [`TruthUse::Converted`]: Python calls `bool()` on it but no test follows: a
//!   non-last operand of `and` / `or`, or the operand of `not`, outside a test.
//! - `None`: the value itself is used (assigned, passed, returned, the last operand of
//!   a chain whose value is used, an `assert` message).
//!
//! Never walk parents for `if` / `while` / `not` / `and` in a rule: ask this.

use ruff_python_ast::{self as ast, Expr, Stmt, UnaryOp};
use ruff_text_size::{Ranged, TextRange};

use crate::checker::Node;

/// How a value is used as a truth value (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruthUse {
    /// It decides a branch. `named`: the path passes a walrus, so the value is also
    /// kept in a name (rewriting the value changes what the name holds).
    Test { named: bool },
    /// `bool()` is applied (non-last `and` / `or` operand, `not` operand), no test.
    Converted,
}

impl TruthUse {
    pub fn is_test(self) -> bool {
        matches!(self, TruthUse::Test { .. })
    }
}

/// How the value of the expression at `range` is used as a truth value, given its
/// ancestors innermost first (parent first).
pub fn truth_use<'a>(
    range: TextRange,
    ancestors: impl IntoIterator<Item = Node<'a>>,
) -> Option<TruthUse> {
    let mut cur = range;
    let mut converted = false;
    let mut named = false;
    for node in ancestors {
        let test = Some(TruthUse::Test { named });
        match node {
            Node::Stmt(Stmt::If(s)) => {
                let is_test = s.test.range() == cur
                    || s.elif_else_clauses
                        .iter()
                        .any(|c| c.test.as_ref().is_some_and(|t| t.range() == cur));
                return if is_test { test } else { None };
            }
            Node::Stmt(Stmt::While(s)) if s.test.range() == cur => return test,
            Node::Stmt(Stmt::Assert(s)) if s.test.range() == cur => return test,
            Node::Stmt(Stmt::Match(m))
                if m.cases
                    .iter()
                    .any(|c| c.guard.as_ref().is_some_and(|g| g.range() == cur)) =>
            {
                return test;
            }
            Node::Expr(Expr::If(x)) if x.test.range() == cur => return test,
            Node::Expr(e)
                if generators(e).is_some_and(|gs| {
                    gs.iter().any(|g| g.ifs.iter().any(|t| t.range() == cur))
                }) =>
            {
                return test;
            }
            Node::Expr(Expr::BoolOp(b)) => {
                if b.values.last().is_some_and(|v| v.range() != cur) {
                    converted = true;
                }
                cur = b.range;
            }
            Node::Expr(Expr::UnaryOp(u)) if u.op == UnaryOp::Not => {
                converted = true;
                cur = u.range;
            }
            Node::Expr(Expr::Named(n)) if n.value.range() == cur => {
                named = true;
                cur = n.range;
            }
            _ => break,
        }
    }
    converted.then_some(TruthUse::Converted)
}

/// The generators of a comprehension expression.
fn generators(e: &Expr) -> Option<&[ast::Comprehension]> {
    match e {
        Expr::ListComp(c) => Some(&c.generators),
        Expr::SetComp(c) => Some(&c.generators),
        Expr::DictComp(c) => Some(&c.generators),
        Expr::Generator(c) => Some(&c.generators),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruff_python_ast::visitor::{self, Visitor};
    use ruff_python_parser::parse_module;

    /// The truth use of the (first) expression whose source is `needle`.
    fn use_of(src: &str, needle: &str) -> Option<TruthUse> {
        struct V<'a, 's> {
            src: &'s str,
            needle: &'s str,
            stack: Vec<Node<'a>>,
            found: Option<(TextRange, Vec<Node<'a>>)>,
        }
        impl<'a> Visitor<'a> for V<'a, '_> {
            fn visit_stmt(&mut self, s: &'a Stmt) {
                self.stack.push(Node::Stmt(s));
                visitor::walk_stmt(self, s);
                self.stack.pop();
            }
            fn visit_expr(&mut self, e: &'a Expr) {
                if self.found.is_none() && &self.src[e.range()] == self.needle {
                    let ancestors = self.stack.iter().rev().copied().collect();
                    self.found = Some((e.range(), ancestors));
                }
                self.stack.push(Node::Expr(e));
                visitor::walk_expr(self, e);
                self.stack.pop();
            }
        }
        let parsed = parse_module(src).unwrap();
        let mut v = V {
            src,
            needle,
            stack: Vec::new(),
            found: None,
        };
        v.visit_body(&parsed.syntax().body);
        let (range, ancestors) = v.found.unwrap_or_else(|| panic!("{needle:?} in {src:?}"));
        truth_use(range, ancestors)
    }

    const T: Option<TruthUse> = Some(TruthUse::Test { named: false });
    const TN: Option<TruthUse> = Some(TruthUse::Test { named: true });
    const C: Option<TruthUse> = Some(TruthUse::Converted);

    #[test]
    fn python_shapes() {
        let cases: &[(&str, &str, Option<TruthUse>)] = &[
            ("if x:\n    pass\n", "x", T),
            ("if a:\n    pass\nelif x:\n    pass\n", "x", T),
            ("while x:\n    pass\n", "x", T),
            ("assert x\n", "x", T),
            ("assert a, x\n", "x", None),
            ("y = a if x else b\n", "x", T),
            ("y = x if a else b\n", "x", None),
            ("y = [r for r in rows if x]\n", "x", T),
            ("y = {r for r in rows if x}\n", "x", T),
            ("y = {r: 1 for r in rows if x}\n", "x", T),
            ("y = any(r for r in rows if x)\n", "x", T),
            ("y = [x for r in rows]\n", "x", None),
            ("match v:\n    case _ if x:\n        pass\n", "x", T),
            ("if (n := x):\n    pass\n", "x", TN),
            ("if (n := x) > 0:\n    pass\n", "x", None),
            ("if not x:\n    pass\n", "x", T),
            ("if x and a:\n    pass\n", "x", T),
            ("if a or x:\n    pass\n", "x", T),
            ("if a and not (b or x):\n    pass\n", "x", T),
            ("while not (n := x):\n    pass\n", "x", TN),
            ("y = not x\n", "x", C),
            ("y = x and a\n", "x", C),
            ("y = a or x\n", "x", None),
            ("y = f(not (a or x))\n", "x", C),
            ("y = x\n", "x", None),
            ("y = f(x)\n", "x", None),
            ("if f(x):\n    pass\n", "x", None),
            ("if x > 0:\n    pass\n", "x", None),
            ("if a:\n    x\n", "x", None),
        ];
        for (src, needle, want) in cases {
            assert_eq!(use_of(src, needle), *want, "{needle:?} in {src:?}");
        }
    }
}
