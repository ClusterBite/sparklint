//! sparklint's rule engine: registry, checker, rules, suppressions, configuration
//! and fixes. File-pure: [`lint_source`] takes a loaded source, settings and a
//! cross-file [`SummaryProvider`] and returns sorted diagnostics.

pub mod checker;
pub mod config;
pub mod diagnostic;
pub mod fix;
pub mod noqa;
pub mod overlap;
pub mod project;
pub mod registry;
pub mod rules;

use ruff_python_ast::token::TokenKind;
use ruff_python_parser::parse_unchecked_source;
use ruff_text_size::{Ranged, TextSize};
use sparklint_semantic::{SemanticOptions, SummaryProvider, TypeInfo, build_model};
use sparklint_source::SourceFile;

pub use config::{LinterSettings, NotebookMode};
pub use diagnostic::{Applicability, Diagnostic, Edit, Fix, Severity};

use crate::checker::Checker;
use crate::noqa::Suppressions;

/// Semantic options derived from the settings for one file.
pub fn semantic_options(
    source: &SourceFile,
    settings: &LinterSettings,
    module_name: Option<String>,
) -> SemanticOptions {
    let is_notebook = match settings.notebook {
        NotebookMode::Auto => source.is_notebook(),
        NotebookMode::On => true,
        NotebookMode::Off => false,
    };
    SemanticOptions {
        is_notebook,
        heuristics: settings.heuristics,
        known_globals: settings.known_globals.clone(),
        module_name,
    }
}

/// SYN001 diagnostics for parse errors found in the text starting at `base`: the
/// first error of every source line (one bad line makes the parser report several).
fn syntax_diagnostics(
    source: &SourceFile,
    errors: &[ruff_python_parser::ParseError],
    base: TextSize,
) -> Vec<Diagnostic> {
    let mut out: Vec<Diagnostic> = errors
        .iter()
        .map(|e| {
            Diagnostic::new(
                registry::SYNTAX_ERROR,
                format!("Syntax error: {}", e.error),
                e.location + base,
            )
        })
        .collect();
    out.sort_by_key(|d| d.range.start());
    out.dedup_by_key(|d| source.line_index.line_of(d.range.start()));
    out
}

/// A parsed source file: what the rules may look at and the syntax errors to report.
pub struct ParsedSource {
    pub parsed: ruff_python_parser::Parsed<ruff_python_ast::ModModule>,
    /// SYN001 diagnostics, at most one per source line.
    pub syntax_errors: Vec<Diagnostic>,
    /// `parsed` is error-free and can be linted. `false` when a syntax error could not
    /// be confined to notebook cells: then only the syntax errors are reported.
    pub lintable: bool,
}

/// Parse a source file.
///
/// A module with a syntax error is not linted (only the errors are reported). A
/// notebook is a sequence of cells that the kernel compiles one by one, so a cell that
/// does not compile says nothing about the others: each cell with an error of its own
/// is reported and blanked (same offsets), and the remaining cells are linted.
pub fn parse_source(source: &SourceFile) -> ParsedSource {
    let ty = source.py_source_type();
    let parsed = parse_unchecked_source(&source.text, ty);
    if parsed.errors().is_empty() {
        return ParsedSource {
            parsed,
            syntax_errors: Vec::new(),
            lintable: true,
        };
    }
    let whole = |parsed: ruff_python_parser::Parsed<ruff_python_ast::ModModule>| ParsedSource {
        syntax_errors: syntax_diagnostics(source, parsed.errors(), TextSize::from(0)),
        parsed,
        lintable: false,
    };
    if source.cells.is_empty() {
        return whole(parsed);
    }
    let mut bytes = source.text.clone().into_bytes();
    let mut syntax_errors = Vec::new();
    for cell in &source.cells {
        let body = source.slice(cell.range);
        if body.trim().is_empty() {
            continue;
        }
        let cell_parse = parse_unchecked_source(body, ty);
        if !cell_parse.errors().is_empty() {
            let mut errors = syntax_diagnostics(source, cell_parse.errors(), cell.range.start());
            // "Unexpected end of file" sits after the cell's text: report it on the
            // cell's last line, not on the first line of the next cell.
            let last = cell.range.start() + TextSize::of(body.trim_end());
            for d in &mut errors {
                if d.range.start() > last {
                    d.range = ruff_text_size::TextRange::empty(last);
                }
            }
            errors.dedup_by_key(|d| source.line_index.line_of(d.range.start()));
            syntax_errors.extend(errors);
            sparklint_source::blank_range(&mut bytes, cell.range);
        }
    }
    // No cell fails on its own (the error needs several cells), or what is left still
    // does not parse: treat the file like a module.
    let Ok(text) = String::from_utf8(bytes) else {
        return whole(parsed);
    };
    if syntax_errors.is_empty() {
        return whole(parsed);
    }
    let rest = parse_unchecked_source(&text, ty);
    if !rest.errors().is_empty() {
        return whole(parsed);
    }
    ParsedSource {
        parsed: rest,
        syntax_errors,
        lintable: true,
    }
}

/// Lint one source file.
pub fn lint_source(
    source: &SourceFile,
    settings: &LinterSettings,
    index: &dyn SummaryProvider,
    module_name: Option<String>,
) -> Vec<Diagnostic> {
    lint_parsed(source, &parse_source(source), settings, index, module_name)
}

fn lint_parsed(
    source: &SourceFile,
    p: &ParsedSource,
    settings: &LinterSettings,
    index: &dyn SummaryProvider,
    module_name: Option<String>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    if p.lintable {
        let module = p.parsed.syntax();
        let opts = semantic_options(source, settings, module_name);
        let model = build_model(module, TextSize::of(source.text.as_str()), opts, index);
        diagnostics = Checker::new(source, &model, settings, module).run();
        let suppressions = suppressions_of(source, &p.parsed);
        diagnostics.retain(|d| !is_suppressed(source, &suppressions, d));
    }
    if settings.is_enabled(registry::SYNTAX_ERROR) {
        diagnostics.extend(p.syntax_errors.iter().cloned());
    }
    sort_diagnostics(&mut diagnostics);
    diagnostics
}

pub(crate) fn suppressions_of(
    source: &SourceFile,
    parsed: &ruff_python_parser::Parsed<ruff_python_ast::ModModule>,
) -> Suppressions {
    let comments = parsed
        .tokens()
        .iter()
        .filter(|t| t.kind() == TokenKind::Comment)
        .map(|t| {
            (
                source.line_index.line_of(t.start()),
                source.slice(t.range()),
            )
        });
    Suppressions::from_comments(comments)
}

pub(crate) fn is_suppressed(source: &SourceFile, s: &Suppressions, d: &Diagnostic) -> bool {
    let first = source.line_index.line_of(d.range.start());
    let last = source.line_index.line_of(d.range.end());
    s.is_suppressed(d.code, first, last)
}

fn sort_diagnostics(diagnostics: &mut Vec<Diagnostic>) {
    diagnostics.sort_by(|a, b| {
        (a.range.start(), a.code, &a.message, a.range.end()).cmp(&(
            b.range.start(),
            b.code,
            &b.message,
            b.range.end(),
        ))
    });
    diagnostics.dedup_by(|a, b| a.code == b.code && a.range == b.range && a.message == b.message);
}

/// Lint a whole project ([`project::lint`]): the cross-file index, then every file
/// with it. One diagnostics list per input file, in input order.
pub fn lint_project(files: &[project::ProjectFile]) -> Vec<Vec<Diagnostic>> {
    project::lint(files)
}

/// Phase A of the project index: function summaries exported by one file.
pub fn exported_summaries(
    source: &SourceFile,
    settings: &LinterSettings,
    module_name: Option<String>,
) -> Vec<(String, TypeInfo)> {
    let p = parse_source(source);
    if !p.lintable {
        return Vec::new();
    }
    let parsed = &p.parsed;
    let opts = semantic_options(source, settings, module_name);
    let model = build_model(
        parsed.syntax(),
        TextSize::of(source.text.as_str()),
        opts,
        &sparklint_semantic::NoSummaries,
    );
    model.exported_summaries()
}

/// Result of linting with fixes applied.
#[derive(Debug, Clone)]
pub struct FixOutcome {
    /// Diagnostics remaining after fixing.
    pub diagnostics: Vec<Diagnostic>,
    /// The fixed source text, if anything changed.
    pub fixed_text: Option<String>,
    pub fixes_applied: usize,
}

/// Lint, apply fixes (safe only unless `unsafe_fixes`), re-lint until stable (max
/// 10 rounds). A round whose result does not parse is discarded, and a file with any
/// syntax error (also in a single notebook cell) is never rewritten. Edits are applied
/// to [`SourceFile::raw_text`], so bare magics and language cells are written back
/// byte for byte.
pub fn lint_and_fix(
    source: &SourceFile,
    settings: &LinterSettings,
    index: &dyn SummaryProvider,
    module_name: Option<String>,
    unsafe_fixes: bool,
) -> FixOutcome {
    let mut current = source.clone();
    let first = parse_source(&current);
    let mut diagnostics = lint_parsed(&current, &first, settings, index, module_name.clone());
    // Writing fixes back into .ipynb JSON is not supported yet.
    if source.kind == sparklint_source::SourceKind::Jupyter || !first.syntax_errors.is_empty() {
        return FixOutcome {
            diagnostics,
            fixed_text: None,
            fixes_applied: 0,
        };
    }
    let mut total = 0;
    for _ in 0..10 {
        let (text, applied) = fix::apply_fixes(current.raw_text(), &diagnostics, unsafe_fixes);
        if applied == 0 {
            break;
        }
        let candidate = SourceFile::from_text(&source.path, text);
        let parsed = parse_source(&candidate);
        if !parsed.syntax_errors.is_empty() {
            break;
        }
        total += applied;
        diagnostics = lint_parsed(&candidate, &parsed, settings, index, module_name.clone());
        current = candidate;
    }
    FixOutcome {
        diagnostics,
        fixed_text: (current.raw_text() != source.raw_text())
            .then(|| current.raw_text().to_string()),
        fixes_applied: total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn lint(src: &str, select: &[&str]) -> Vec<Diagnostic> {
        let s = LinterSettings::for_selectors(select).unwrap();
        let f = SourceFile::from_text(Path::new("t.py"), src.to_string());
        lint_source(&f, &s, &sparklint_semantic::NoSummaries, None)
    }

    #[test]
    fn syntax_error_is_reported_not_panicking() {
        let d = lint("def f(:\n  pass\n", &["ALL"]);
        assert!(!d.is_empty());
        assert!(d.iter().all(|d| d.code == "SYN001"));
    }

    fn codes(d: &[Diagnostic]) -> Vec<(&'static str, u32)> {
        d.iter()
            .map(|d| (d.code, u32::from(d.range.start())))
            .collect()
    }

    fn lines(src: &str, d: &[Diagnostic]) -> Vec<(&'static str, usize)> {
        d.iter()
            .map(|d| {
                (
                    d.code,
                    src[..usize::from(d.range.start())].matches('\n').count() + 1,
                )
            })
            .collect()
    }

    const NB: &str = "# Databricks notebook source\n";

    #[test]
    fn notebook_magics_are_not_syntax_errors() {
        // Line magics, shell escapes, a backslash-continued magic, `x = !cmd`,
        // `x = %magic`, an indented magic, IPython help — and the cell is still linted
        // with unchanged offsets.
        let src = format!(
            "{NB}%pip install --quiet a==1.0 \\\n    b==2.0\n!pip install c\n%run ./setup $mode=\"x\"\n%restart_python\ndf = spark.table('t')\nfiles = !ls /tmp\nhere = %pwd\nif files:\n    %pip install d\ndf?\nrows = df.collect()\n"
        );
        let d = lint(&src, &["ALL"]);
        assert!(d.iter().all(|d| d.code != "SYN001"), "{d:?}");
        let at = src.find(".collect()").unwrap() as u32 + 1;
        assert!(codes(&d).contains(&("ACT002", at)), "{d:?}");
        // The same text as a plain module is a syntax error (no magics there).
        let module = src.strip_prefix(NB).unwrap();
        let d = lint(module, &["ALL"]);
        assert!(!d.is_empty() && d.iter().all(|d| d.code == "SYN001"));
    }

    #[test]
    fn magic_lookalikes_inside_python_are_python() {
        // `%` / `!` at the start of a physical line that is not a logical line start.
        let src = format!(
            "{NB}df = spark.table('t')\nn = (10\n     % 3)\nq = \"\"\"\n%sql select 1\n!ls\n\"\"\"\nok = n \\\n    != 1\nrows = df.collect()\n"
        );
        let d = lint(&src, &["ACT002", "SYN001"]);
        assert_eq!(lines(&src, &d), [("ACT002", 11)]);
    }

    #[test]
    fn language_cells_are_not_parsed() {
        let src = format!(
            "{NB}%sh\nls -l | wc -l\nrm -rf /tmp/x\n\n# COMMAND ----------\n\n%sql\nSELECT 'it''s' FROM t\n\n# COMMAND ----------\n\n%scala\nval x = spark.table(\"t\").collect()\n\n# COMMAND ----------\n\ndf = spark.table('t')\nrows = df.collect()\n"
        );
        let d = lint(&src, &["ACT002", "SYN001"]);
        assert_eq!(lines(&src, &d), [("ACT002", 19)]);
    }

    #[test]
    fn syntax_error_in_one_cell_keeps_the_other_cells() {
        let src = format!(
            "{NB}df = spark.table('t')\n\n# COMMAND ----------\n\ntoken_count = len(tokenizer.encode(text))y\nlost = df.collect()\n\n# COMMAND ----------\n\nrows = df.collect()\n\n# COMMAND ----------\n\ns = 'unterminated\nmore = (1,\n\n# COMMAND ----------\n\nagain = df.collect()\n"
        );
        let d = lint(&src, &["ACT002", "SYN001"]);
        // One SYN001 per bad line; the two good cells after bad ones are linted.
        assert_eq!(
            lines(&src, &d),
            [
                ("SYN001", 6),
                ("ACT002", 11),
                ("SYN001", 15),
                ("SYN001", 16),
                ("ACT002", 20)
            ]
        );
        // SYN001 off: the good cells are still linted.
        let d = lint(&src, &["ACT002"]);
        assert_eq!(lines(&src, &d), [("ACT002", 11), ("ACT002", 20)]);
        // A module keeps the "syntax errors only" behaviour.
        let module = "df = spark.table('t')\nx = len(a))y\nrows = df.collect()\n";
        let d = lint(module, &["ACT002", "SYN001"]);
        assert_eq!(lines(module, &d), [("SYN001", 2)]);
        assert!(lint(module, &["ACT002"]).is_empty());
    }

    #[test]
    fn one_syntax_error_per_line() {
        // The parser reports this line three times.
        let module = "%pip install --quiet databricks-genai==1.1.7 mlflow==2.19.0\n";
        let parsed = parse_unchecked_source(module, ruff_python_ast::PySourceType::Python);
        assert!(parsed.errors().len() > 1);
        assert_eq!(lines(module, &lint(module, &["ALL"])), [("SYN001", 1)]);
    }

    #[test]
    fn fixes_keep_magics_byte_for_byte() {
        let s = LinterSettings::for_selectors(&["ACT002"]).unwrap();
        let src = format!(
            "{NB}%pip install a \\\n    b\n!pip install c\n\n# COMMAND ----------\n\n# DBTITLE 1,Shell\n%sh\nls -l | wc -l\n\n# COMMAND ----------\n\ndf = spark.table('t')\nfiles = !ls /tmp\nrows = df.collect()\n"
        );
        let f = SourceFile::from_text(Path::new("t.py"), src.clone());
        assert_ne!(f.text, src, "the %sh cell is blanked in the Python view");
        let out = lint_and_fix(&f, &s, &sparklint_semantic::NoSummaries, None, true);
        assert_eq!(out.fixes_applied, 1);
        assert_eq!(
            out.fixed_text.unwrap(),
            src.replace("df.collect()", "df.limit(1000).collect()")
        );
        // Nothing to fix: no rewrite (the blanked view is never written back).
        let safe = lint_and_fix(&f, &s, &sparklint_semantic::NoSummaries, None, false);
        assert!(safe.fixed_text.is_none());
        // A notebook with a syntax error in any cell is not rewritten.
        let broken = format!("{src}\n# COMMAND ----------\n\nx = 1 +\n");
        let f = SourceFile::from_text(Path::new("t.py"), broken);
        let out = lint_and_fix(&f, &s, &sparklint_semantic::NoSummaries, None, true);
        assert!(out.fixed_text.is_none());
        assert!(out.diagnostics.iter().any(|d| d.code == "ACT002"));
    }

    #[test]
    fn noqa_suppresses() {
        let src =
            "df = spark.table('t')\nrows = df.collect()  # noqa: ACT002\nrows2 = df.collect()\n";
        let d = lint(src, &["ACT002"]);
        assert_eq!(d.len(), 1);
        let src = "df = spark.table('t')\nrows = df.collect()  # cy:ignore CY001\n";
        assert!(lint(src, &["ACT002"]).is_empty());
        let src = "# sparklint: file-ignore[ACT002]\ndf = spark.table('t')\nrows = df.collect()\n";
        assert!(lint(src, &["ACT002"]).is_empty());
    }

    #[test]
    fn fixes_apply_and_reparse() {
        let s = LinterSettings::for_selectors(&["ACT006", "COR001"]).unwrap();
        let src = "from pyspark.sql import functions as F\ndf = spark.table('t')\nif df.count() > 0:\n    df.filter(F.col('a') == None)\n";
        let f = SourceFile::from_text(Path::new("t.py"), src.to_string());
        let out = lint_and_fix(&f, &s, &sparklint_semantic::NoSummaries, None, false);
        let fixed = out.fixed_text.unwrap();
        assert!(fixed.contains("if not df.isEmpty():"), "{fixed}");
        assert!(fixed.contains("F.col('a').isNull()"), "{fixed}");
        assert_eq!(out.fixes_applied, 2);
        assert!(out.diagnostics.is_empty());
    }

    #[test]
    fn conf_rules_severity() {
        use crate::diagnostic::Severity;
        let sev = |src: &str, code: &str| -> Vec<Severity> {
            lint(src, &[code]).iter().map(|d| d.severity).collect()
        };
        let src = "spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '2g')\nspark.conf.set('spark.sql.autoBroadcastJoinThreshold', '8g')\n";
        assert_eq!(sev(src, "CFG007"), vec![Severity::Warning, Severity::Error]);
        // Set back later: the earlier write is a scoped workaround (info).
        let src = "spark.conf.set('spark.sql.adaptive.enabled', 'false')\nx = 1\nspark.conf.set('spark.sql.adaptive.enabled', 'true')\n";
        assert_eq!(sev(src, "CFG001"), vec![Severity::Info]);
        let src = "spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)\nspark.conf.unset('spark.sql.autoBroadcastJoinThreshold')\n";
        assert_eq!(sev(src, "CFG002"), vec![Severity::Info]);
        // A restore BEFORE the disable does not count.
        let src = "spark.conf.set('spark.sql.adaptive.enabled', 'true')\nspark.conf.set('spark.sql.adaptive.enabled', 'false')\n";
        assert_eq!(sev(src, "CFG001"), vec![Severity::Warning]);
        // Threshold is configurable; 0 turns CFG007 off.
        let mut s = LinterSettings::for_selectors(&["CFG007"]).unwrap();
        s.broadcast_threshold_max = 0;
        let f = SourceFile::from_text(
            Path::new("t.py"),
            "spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '16g')\n".into(),
        );
        assert!(lint_source(&f, &s, &sparklint_semantic::NoSummaries, None).is_empty());
    }

    #[test]
    fn unsafe_fixes_need_opt_in() {
        let s = LinterSettings::for_selectors(&["ACT002"]).unwrap();
        let f = SourceFile::from_text(
            Path::new("t.py"),
            "df = spark.table('t')\nr = df.collect()\n".into(),
        );
        let out = lint_and_fix(&f, &s, &sparklint_semantic::NoSummaries, None, false);
        assert!(out.fixed_text.is_none());
        let out = lint_and_fix(&f, &s, &sparklint_semantic::NoSummaries, None, true);
        assert!(out.fixed_text.unwrap().contains("df.limit(1000).collect()"));
    }
}
