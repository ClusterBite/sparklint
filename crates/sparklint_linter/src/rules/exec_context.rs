//! How a site runs: the one place that knows the error-probe and failure-path
//! shapes of Python code.
//!
//! [`Run`] answers "does the code at this site run normally, only after something
//! already failed, or as a deliberate probe for an expected error?":
//!
//! - [`Run::FailureOnly`]: an `assert` message or anything inside a `raise`
//!   statement — it only runs when a check already failed.
//! - [`Run::Probe`]: the site's statement sits directly in a body that expects an
//!   error or warning ([`Probe`]): a `try:` body with an `except` handler, a
//!   `with pytest.raises(..)` / `self.assertRaises(..)`-style block, or a
//!   `with pytest.warns(..)` / `self.assertWarns(..)`-style block.
//! - [`Run::Normal`]: everything else.
//!
//! Which answers exempt which rule (one policy, decided here):
//! - Discarded work (ACT007 discarded action, COR007 discarded transformation):
//!   any [`Run::Probe`] — the statement runs for its error, not its result.
//! - Counting how often a frame is computed (PRF006, PRF008; see
//!   `lineage::CountPolicy`): [`Run::completes`] — failure-only sites and bodies a
//!   test expects to raise never produce a result on the passing path; a `try:`
//!   probe and a `warns` block do run to the end, so they still count.
//!
//! "Directly" means the statement containing the site is an element of that body
//! (`try: df.count()`), not nested deeper in an `if` / loop / `with` inside it.
//!
//! The index is built once per file on first use ([`Checker::run_at`]) and answers by
//! source range, so it serves both the checker's rules (which also have a parent
//! stack) and whole-file walkers that keep none. Rules choose which
//! answers exempt them; they never walk parents for these shapes themselves.

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{Expr, ModModule, Stmt};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::SemanticModel;

/// pytest context managers asserting the body raises / warns (qualified names).
const PYTEST_RAISES: &[&str] = &["pytest.raises"];
const PYTEST_WARNS: &[&str] = &["pytest.warns"];
/// unittest `self.assertRaises(..)`-style context managers (method names).
const UNITTEST_RAISES: &[&str] = &["assertRaises", "assertRaisesRegex", "assertRaisesRegexp"];
const UNITTEST_WARNS: &[&str] = &["assertWarns", "assertWarnsRegex"];

/// A body whose statements run to provoke an error (or warning) on purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// A `try:` body with at least one `except` handler.
    Try,
    /// `with pytest.raises(..)` / `with self.assertRaises(..)` (and `..Regex`).
    Raises,
    /// `with pytest.warns(..)` / `with self.assertWarns(..)` (and `..Regex`).
    Warns,
}

/// How the code at a site runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Run {
    Normal,
    /// Inside an `assert` message or a `raise` statement: runs only after a check
    /// already failed. Takes precedence over [`Run::Probe`].
    FailureOnly,
    /// Directly in a body that expects an error.
    Probe(Probe),
}

impl Run {
    pub fn is_probe(self) -> bool {
        matches!(self, Run::Probe(_))
    }

    /// The site runs to the end on the passing path: not failure-only and not in a
    /// body a test expects to raise. What counting rules count.
    pub fn completes(self) -> bool {
        !matches!(self, Run::FailureOnly | Run::Probe(Probe::Raises))
    }
}

#[derive(Debug)]
struct StmtEntry {
    range: TextRange,
    parent: Option<u32>,
    /// The body this statement is a direct element of expects an error.
    probe: Option<Probe>,
}

/// Per-file index answering [`Run`] by source range.
#[derive(Debug, Default)]
pub struct ExecIndex {
    /// Every statement, in source (pre-)order.
    stmts: Vec<StmtEntry>,
    /// Disjoint failure-only regions (assert messages, raise statements), by start.
    failure: Vec<TextRange>,
}

impl ExecIndex {
    pub fn build(module: &ModModule, model: &SemanticModel) -> Self {
        let mut b = Builder {
            model,
            ix: ExecIndex::default(),
            stack: Vec::new(),
            pending: None,
        };
        b.visit_body(&module.body);
        let mut ix = b.ix;
        ix.failure.sort_by_key(|r| r.start());
        ix
    }

    /// How the code at `site` runs.
    pub fn run_at(&self, site: TextRange) -> Run {
        let k = self.failure.partition_point(|r| r.start() <= site.start());
        if k > 0 && self.failure[k - 1].contains_range(site) {
            return Run::FailureOnly;
        }
        match self.innermost_stmt(site) {
            Some(s) => s.probe.map_or(Run::Normal, Run::Probe),
            None => Run::Normal,
        }
    }

    /// The innermost statement containing `site`: an ancestor-or-self of the last
    /// statement (pre-order) starting at or before it.
    fn innermost_stmt(&self, site: TextRange) -> Option<&StmtEntry> {
        let k = self
            .stmts
            .partition_point(|s| s.range.start() <= site.start());
        let mut i = k.checked_sub(1)?;
        loop {
            let s = &self.stmts[i];
            if s.range.contains_range(site) {
                return Some(s);
            }
            i = s.parent? as usize;
        }
    }
}

struct Builder<'m> {
    model: &'m SemanticModel<'m>,
    ix: ExecIndex,
    /// Index of the enclosing statements.
    stack: Vec<u32>,
    /// Probe kind for the direct elements of the body about to be visited:
    /// (body statement range → probe), set by the compound statement.
    pending: Option<(Vec<TextRange>, Probe)>,
}

impl Builder<'_> {
    fn with_probe(&self, w: &ruff_python_ast::StmtWith) -> Option<Probe> {
        w.items.iter().find_map(|item| {
            let Expr::Call(c) = &item.context_expr else {
                return None;
            };
            if let Some(q) = self.model.qualified_name(&c.func) {
                if PYTEST_RAISES.contains(&q.as_str()) {
                    return Some(Probe::Raises);
                }
                if PYTEST_WARNS.contains(&q.as_str()) {
                    return Some(Probe::Warns);
                }
            }
            match &*c.func {
                Expr::Attribute(a) if UNITTEST_RAISES.contains(&a.attr.as_str()) => {
                    Some(Probe::Raises)
                }
                Expr::Attribute(a) if UNITTEST_WARNS.contains(&a.attr.as_str()) => {
                    Some(Probe::Warns)
                }
                _ => None,
            }
        })
    }

    fn probe_of_child(&self, child: &Stmt) -> Option<Probe> {
        self.pending
            .as_ref()
            .and_then(|(body, p)| body.contains(&child.range()).then_some(*p))
    }
}

impl<'a> Visitor<'a> for Builder<'_> {
    fn visit_stmt(&mut self, stmt: &'a Stmt) {
        let parent = self.stack.last().copied();
        let probe = self.probe_of_child(stmt);
        let id = self.ix.stmts.len() as u32;
        self.ix.stmts.push(StmtEntry {
            range: stmt.range(),
            parent,
            probe,
        });
        match stmt {
            Stmt::Raise(r) => self.ix.failure.push(r.range),
            Stmt::Assert(a) => {
                if let Some(m) = &a.msg {
                    self.ix.failure.push(m.range());
                }
            }
            _ => {}
        }
        let saved = self.pending.take();
        self.pending = match stmt {
            Stmt::Try(t) if !t.handlers.is_empty() => {
                Some((t.body.iter().map(Ranged::range).collect(), Probe::Try))
            }
            Stmt::With(w) => self
                .with_probe(w)
                .map(|p| (w.body.iter().map(Ranged::range).collect(), p)),
            _ => None,
        };
        self.stack.push(id);
        visitor::walk_stmt(self, stmt);
        self.stack.pop();
        self.pending = saved;
    }
}

#[cfg(test)]
mod tests {
    use super::Run;
    use crate::checker::testing::with_checker;
    use ruff_text_size::{TextRange, TextSize};

    /// The [`Run`] of the first occurrence of `needle` in `src`.
    fn run_of(src: &str, needle: &str) -> Run {
        let at = src
            .find(needle)
            .unwrap_or_else(|| panic!("{needle} in {src}"));
        let site = TextRange::at(TextSize::new(at as u32), TextSize::of(needle));
        with_checker(src, &["ALL"], |ch| ch.run_at(site))
    }

    #[test]
    fn run_table() {
        use super::Probe::{Raises, Try, Warns};
        use Run::{FailureOnly, Normal};
        let cases: &[(&str, &str, Run)] = &[
            ("df.count()\n", "df.count()", Normal),
            (
                "try:\n    df.count()\nexcept Exception:\n    pass\n",
                "df.count()",
                Run::Probe(Try),
            ),
            // a try/finally has no handler: not a probe
            (
                "try:\n    df.count()\nfinally:\n    pass\n",
                "df.count()",
                Normal,
            ),
            // the handler and else arms run normally
            (
                "try:\n    x = 1\nexcept Exception:\n    df.count()\n",
                "df.count()",
                Normal,
            ),
            (
                "try:\n    x = 1\nexcept Exception:\n    pass\nelse:\n    df.count()\n",
                "df.count()",
                Normal,
            ),
            // only direct elements of the body
            (
                "try:\n    if c:\n        df.count()\nexcept Exception:\n    pass\n",
                "df.count()",
                Normal,
            ),
            (
                "try:\n    if c:\n        df.count()\nexcept Exception:\n    pass\n",
                "if c:",
                Run::Probe(Try),
            ),
            (
                "try:\n    n = df.count()\nexcept Exception:\n    pass\n",
                "df.count()",
                Run::Probe(Try),
            ),
            (
                "import pytest\nwith pytest.raises(ValueError):\n    df.count()\n",
                "df.count()",
                Run::Probe(Raises),
            ),
            (
                "import pytest\nwith pytest.warns(UserWarning):\n    df.count()\n",
                "df.count()",
                Run::Probe(Warns),
            ),
            (
                "from pytest import raises\nwith raises(ValueError):\n    df.count()\n",
                "df.count()",
                Run::Probe(Raises),
            ),
            // not pytest: an unrelated `raises` helper
            (
                "def raises(e):\n    pass\nwith raises(ValueError):\n    df.count()\n",
                "df.count()",
                Normal,
            ),
            (
                "with self.assertRaisesRegex(ValueError, 'x'):\n    df.count()\n",
                "df.count()",
                Run::Probe(Raises),
            ),
            (
                "with self.assertWarns(UserWarning):\n    df.count()\n",
                "df.count()",
                Run::Probe(Warns),
            ),
            (
                "with open(p) as f, self.assertRaises(ValueError):\n    df.count()\n",
                "df.count()",
                Run::Probe(Raises),
            ),
            ("with open(p) as f:\n    df.count()\n", "df.count()", Normal),
            // a with nested inside a probe body resets the context
            (
                "try:\n    with open(p):\n        df.count()\nexcept Exception:\n    pass\n",
                "df.count()",
                Normal,
            ),
            // nested probes: the innermost body decides
            (
                "import pytest\ntry:\n    with pytest.raises(E):\n        df.count()\nexcept Exception:\n    pass\n",
                "df.count()",
                Run::Probe(Raises),
            ),
            (
                "assert df.count() > 0, df.collect()\n",
                "df.collect()",
                FailureOnly,
            ),
            (
                "assert df.count() > 0, df.collect()\n",
                "df.count()",
                Normal,
            ),
            ("raise ValueError(df.count())\n", "df.count()", FailureOnly),
            (
                "def f():\n    raise ValueError(f'{df.count()}')\n",
                "df.count()",
                FailureOnly,
            ),
            // failure-only wins over a probe body
            (
                "try:\n    raise ValueError(df.count())\nexcept Exception:\n    pass\n",
                "df.count()",
                FailureOnly,
            ),
            // a function defined in a try body: its statements are not body elements
            (
                "try:\n    def f():\n        df.count()\nexcept Exception:\n    pass\n",
                "df.count()",
                Normal,
            ),
        ];
        for (src, needle, want) in cases {
            assert_eq!(run_of(src, needle), *want, "{needle} in:\n{src}");
        }
    }

    #[test]
    fn completes_policy() {
        use super::Probe;
        assert!(Run::Normal.completes());
        assert!(Run::Probe(Probe::Try).completes());
        assert!(Run::Probe(Probe::Warns).completes());
        assert!(!Run::Probe(Probe::Raises).completes());
        assert!(!Run::FailureOnly.completes());
    }
}
