//! The `Checker`: one AST walk per file that dispatches nodes to rules (ruff's model).

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{Expr, ModModule, Stmt, StmtFunctionDef};
use ruff_text_size::{Ranged, TextRange, TextSize};
use rustc_hash::FxHashMap;
use sparklint_semantic::SemanticModel;
use sparklint_source::SourceFile;

use crate::config::LinterSettings;
use crate::diagnostic::Diagnostic;
use crate::rules;

/// A node on the parent stack.
#[derive(Debug, Clone, Copy)]
pub enum Node<'a> {
    Stmt(&'a Stmt),
    Expr(&'a Expr),
}

impl Ranged for Node<'_> {
    fn range(&self) -> TextRange {
        match self {
            Node::Stmt(s) => s.range(),
            Node::Expr(e) => e.range(),
        }
    }
}

pub struct Checker<'a> {
    pub source: &'a SourceFile,
    pub model: &'a SemanticModel<'a>,
    pub settings: &'a LinterSettings,
    pub module: &'a ModModule,
    /// Function definitions keyed by the start of their name identifier.
    pub fn_defs: FxHashMap<TextSize, &'a StmtFunctionDef>,
    parents: Vec<Node<'a>>,
    diagnostics: Vec<Diagnostic>,
    pub(crate) state: rules::State,
    /// Hooks of the enabled rules (from the rule table).
    pub(crate) hooks: rules::Hooks,
}

impl<'a> Checker<'a> {
    pub fn new(
        source: &'a SourceFile,
        model: &'a SemanticModel<'a>,
        settings: &'a LinterSettings,
        module: &'a ModModule,
    ) -> Self {
        let mut fn_defs = FxHashMap::default();
        struct Defs<'b, 'm>(&'m mut FxHashMap<TextSize, &'b StmtFunctionDef>);
        impl<'b> Visitor<'b> for Defs<'b, '_> {
            fn visit_stmt(&mut self, stmt: &'b Stmt) {
                if let Stmt::FunctionDef(f) = stmt {
                    self.0.insert(f.name.range.start(), f);
                }
                visitor::walk_stmt(self, stmt);
            }
        }
        let mut d = Defs(&mut fn_defs);
        d.visit_body(&module.body);
        let mut ch = Self {
            source,
            model,
            settings,
            module,
            fn_defs,
            parents: Vec::new(),
            diagnostics: Vec::new(),
            state: rules::State::default(),
            hooks: rules::Hooks::default(),
        };
        ch.hooks = rules::Hooks::for_checker(&ch);
        rules::init(&mut ch);
        ch
    }

    pub fn run(mut self) -> Vec<Diagnostic> {
        let module = self.module;
        self.visit_body(&module.body);
        rules::finish(&mut self);
        crate::overlap::resolve(&mut self.diagnostics);
        self.diagnostics
    }

    pub fn enabled(&self, code: &str) -> bool {
        self.settings.is_enabled(code)
    }

    pub fn any_enabled(&self, codes: &[&str]) -> bool {
        codes.iter().any(|c| self.enabled(c))
    }

    pub fn report(&mut self, mut d: Diagnostic) {
        if !self.enabled(d.code) {
            return;
        }
        d.severity = self.settings.severity_for(d.code, d.severity);
        self.diagnostics.push(d);
    }

    pub fn is_notebook(&self) -> bool {
        self.model.is_notebook()
    }

    /// Whether `stmt` is the last top-level statement of its notebook cell. A
    /// notebook displays the value of an expression statement in that position, so it
    /// is used, not discarded. Always `false` outside notebooks.
    pub fn is_cell_final(&self, stmt: &Stmt) -> bool {
        if self.source.cells.is_empty() {
            return false;
        }
        let body = &self.module.body;
        let Ok(i) = body.binary_search_by_key(&stmt.start(), |s| s.start()) else {
            return false;
        };
        if body[i].range() != stmt.range() {
            return false;
        }
        let cell = |s: &Stmt| self.source.cell_index_at(s.start());
        body.get(i + 1).is_none_or(|next| cell(next) != cell(stmt))
    }

    /// Source text of a range.
    pub fn slice(&self, range: TextRange) -> &'a str {
        self.source.slice(range)
    }

    /// The parent stack; the last element is the node currently being visited.
    pub fn parents(&self) -> &[Node<'a>] {
        &self.parents
    }

    /// The parent of the node currently being visited.
    pub fn parent(&self) -> Option<Node<'a>> {
        let n = self.parents.len();
        if n >= 2 {
            Some(self.parents[n - 2])
        } else {
            None
        }
    }

    /// The `i`-th ancestor (0 = parent).
    pub fn ancestor(&self, i: usize) -> Option<Node<'a>> {
        let n = self.parents.len();
        n.checked_sub(2 + i).map(|k| self.parents[k])
    }

    /// The innermost statement containing the current node.
    pub fn current_stmt(&self) -> Option<&'a Stmt> {
        self.parents.iter().rev().find_map(|n| match n {
            Node::Stmt(s) => Some(*s),
            Node::Expr(_) => None,
        })
    }

    /// How the code at `site` runs: normally, only after a failed check (assert
    /// message, `raise`), or as a probe for an expected error (see
    /// [`rules::exec_context`]). Built once per file on first use.
    pub fn run_at(&self, site: TextRange) -> rules::exec_context::Run {
        self.state
            .exec
            .get_or_init(|| rules::exec_context::ExecIndex::build(self.module, self.model))
            .run_at(site)
    }

    /// The statement enclosing the current statement (for sibling lookups).
    pub fn parent_stmt_of_current(&self) -> Option<&'a Stmt> {
        let mut it = self.parents.iter().rev().filter_map(|n| match n {
            Node::Stmt(s) => Some(*s),
            Node::Expr(_) => None,
        });
        it.next();
        it.next()
    }
}

impl<'a> Visitor<'a> for Checker<'a> {
    fn visit_stmt(&mut self, stmt: &'a Stmt) {
        self.parents.push(Node::Stmt(stmt));
        rules::on_stmt(self, stmt);
        visitor::walk_stmt(self, stmt);
        self.parents.pop();
    }

    fn visit_expr(&mut self, expr: &'a Expr) {
        self.parents.push(Node::Expr(expr));
        rules::on_expr(self, expr);
        visitor::walk_expr(self, expr);
        self.parents.pop();
    }
}

/// Test support: a [`Checker`] built from source.
#[cfg(test)]
pub(crate) mod testing {
    use super::Checker;
    use crate::config::LinterSettings;
    use ruff_python_ast::visitor::Visitor;
    use ruff_python_parser::parse_unchecked_source;
    use ruff_text_size::TextSize;
    use sparklint_semantic::{NoSummaries, build_model};
    use sparklint_source::SourceFile;
    use std::path::Path;

    /// Parse `src` (no syntax errors allowed), build its model and a checker with
    /// `selectors` enabled, run `f` on it before the walk.
    pub(crate) fn with_checker<R>(
        src: &str,
        selectors: &[&str],
        f: impl FnOnce(&mut Checker) -> R,
    ) -> R {
        let file = SourceFile::from_text(Path::new("t.py"), src.to_string());
        let settings = LinterSettings::for_selectors(selectors).unwrap();
        let parsed = parse_unchecked_source(&file.text, file.py_source_type());
        assert!(parsed.errors().is_empty(), "{src}");
        let module = parsed.syntax();
        let opts = crate::semantic_options(&file, &settings, None);
        let model = build_model(module, TextSize::of(src), opts, &NoSummaries);
        let mut ch = Checker::new(&file, &model, &settings, module);
        f(&mut ch)
    }

    /// Like [`with_checker`], but after the walk: the per-file state the rules'
    /// name-load analyses fill (flow uses, cache weights) is there; finish hooks
    /// have not run.
    pub(crate) fn with_walked_checker<R>(
        src: &str,
        selectors: &[&str],
        f: impl FnOnce(&mut Checker) -> R,
    ) -> R {
        with_checker(src, selectors, |ch| {
            let module = ch.module;
            ch.visit_body(&module.body);
            f(ch)
        })
    }
}
