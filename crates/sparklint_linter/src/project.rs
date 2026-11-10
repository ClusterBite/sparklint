//! Project linting: the one pipeline behind [`crate::lint_project`] (and so the
//! fixture harness) and `sparklint check`.
//!
//! 1. [`index`] (phase A): every file's exported function summaries → the cross-file
//!    [`ProjectIndex`].
//! 2. [`lint_files`] (phase B): the per-file rules. `check --fix` runs its own
//!    fixing loop here instead and keeps the (fixed) sources and their diagnostics.
//!
//! Callers only load files and render: nothing else wires these phases.

use rayon::prelude::*;
use sparklint_semantic::ProjectIndex;
use sparklint_source::SourceFile;

use crate::config::LinterSettings;
use crate::diagnostic::Diagnostic;
use crate::{exported_summaries, lint_source};

/// One file of the project.
pub struct ProjectFile<'a> {
    pub source: &'a SourceFile,
    pub settings: &'a LinterSettings,
    /// Dotted module name (`pkg.mod`).
    pub module: String,
}

/// Phase A: the cross-file index of every file's exported function summaries (in
/// input order).
pub fn index(files: &[ProjectFile]) -> ProjectIndex {
    let summaries: Vec<_> = files
        .par_iter()
        .map(|f| exported_summaries(f.source, f.settings, Some(f.module.clone())))
        .collect();
    let mut index = ProjectIndex::new();
    for (f, s) in files.iter().zip(&summaries) {
        index.add_module(&f.module, s);
    }
    index
}

/// Phase B: the per-file rules of every file (suppressions applied), in input order.
pub fn lint_files(files: &[ProjectFile], index: &ProjectIndex) -> Vec<Vec<Diagnostic>> {
    files
        .par_iter()
        .map(|f| lint_source(f.source, f.settings, index, Some(f.module.clone())))
        .collect()
}

/// Lint a project: [`index`], then [`lint_files`]. One diagnostics list per input
/// file, in input order.
pub fn lint(files: &[ProjectFile]) -> Vec<Vec<Diagnostic>> {
    lint_files(files, &index(files))
}
