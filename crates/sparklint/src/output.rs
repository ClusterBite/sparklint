//! Output formats: text (ruff-style with code frame), json, github, sarif, and
//! `--statistics`.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::time::Instant;

use serde_json::{Value, json};
use sparklint_linter::registry;
use sparklint_linter::{Applicability, Diagnostic, Severity};
use sparklint_source::{SourceFile, SourceKind};

use crate::OutputFormat;

pub struct FileReport {
    pub display: String,
    pub source: SourceFile,
    pub diagnostics: Vec<Diagnostic>,
}

pub struct Summary {
    pub files: usize,
    pub findings: usize,
    pub fixed: usize,
    pub fixable_safe: usize,
    pub fixable_unsafe: usize,
    pub fix_mode: bool,
    /// When the run started (the summary line shows the elapsed time).
    pub started: Instant,
}

/// Start/end positions of a diagnostic.
pub struct Span {
    pub line: u32,
    pub column: u32,
    pub end_line: u32,
    pub end_column: u32,
    pub cell: Option<u32>,
    pub cell_line: Option<u32>,
}

pub fn span(source: &SourceFile, d: &Diagnostic) -> Span {
    let s = source.location(d.range.start());
    let e = source.location(d.range.end());
    Span {
        line: s.line,
        column: s.column,
        end_line: e.line,
        end_column: e.column,
        cell: s.cell,
        cell_line: s.cell_line,
    }
}

/// `path:line:col` (Jupyter: `path:cell N:line:col` with cell-relative lines).
pub fn location_prefix(r: &FileReport, sp: &Span) -> String {
    match (r.source.kind, sp.cell, sp.cell_line) {
        (SourceKind::Jupyter, Some(c), Some(l)) => {
            format!("{}:cell {c}:{l}:{}", r.display, sp.column)
        }
        _ => format!("{}:{}:{}", r.display, sp.line, sp.column),
    }
}

fn fix_marker(d: &Diagnostic) -> &'static str {
    match &d.fix {
        Some(f) if f.applicability == Applicability::Safe => " [*]",
        _ => "",
    }
}

/// The `path:line:col: CODE message` line of a finding and its code frame.
pub fn text_finding(out: &mut impl Write, r: &FileReport, d: &Diagnostic) -> io::Result<()> {
    let sp = span(&r.source, d);
    let cell = match (r.source.kind, sp.cell) {
        (SourceKind::DatabricksNotebook, Some(c)) => format!(" (cell {c})"),
        _ => String::new(),
    };
    writeln!(
        out,
        "{}: {}{} {}{cell}",
        location_prefix(r, &sp),
        d.code,
        fix_marker(d),
        d.message
    )?;
    code_frame(out, r, d, &sp)
}

/// The `= help:` and `= fix:` lines of a finding.
pub fn text_notes(out: &mut impl Write, d: &Diagnostic) -> io::Result<()> {
    if let Some(h) = &d.help {
        writeln!(out, "  = help: {h}")?;
    }
    if let Some(f) = &d.fix {
        let kind = match f.applicability {
            Applicability::Safe => "fix",
            Applicability::Unsafe => "unsafe fix",
        };
        writeln!(out, "  = {kind}: {}", f.message)?;
    }
    Ok(())
}

pub fn text(out: &mut impl Write, reports: &[FileReport]) -> io::Result<()> {
    for r in reports {
        for d in &r.diagnostics {
            text_finding(out, r, d)?;
            text_notes(out, d)?;
            writeln!(out)?;
        }
    }
    Ok(())
}

pub fn code_frame(
    out: &mut impl Write,
    r: &FileReport,
    d: &Diagnostic,
    sp: &Span,
) -> io::Result<()> {
    let src = &r.source;
    let jupyter = src.kind == SourceKind::Jupyter && sp.cell_line.is_some();
    // Displayed number for a text line.
    let shown = |line: u32| -> u32 {
        if jupyter {
            line + sp.cell_line.unwrap_or(sp.line) - sp.line
        } else {
            line
        }
    };
    let cell_first = if jupyter {
        src.cell_at(d.range.start()).map_or(1, |c| c.first_line)
    } else {
        1
    };
    let first = sp.line.saturating_sub(1).max(cell_first).max(1);
    let last = (sp.line + 1).min(src.line_index.line_count());
    let width = shown(last).max(shown(sp.line)).to_string().len();
    let pad = " ".repeat(width);
    writeln!(out, "{pad} |")?;
    for line in first..=last {
        let text = src.line_text(line);
        if line > sp.line && text.trim().is_empty() {
            break;
        }
        writeln!(out, "{:>width$} | {text}", shown(line))?;
        if line == sp.line {
            let col = sp.column.saturating_sub(1) as usize;
            let line_chars = text.chars().count();
            let end = if sp.end_line == sp.line {
                (sp.end_column.saturating_sub(1) as usize).max(col + 1)
            } else {
                line_chars.max(col + 1)
            };
            let lead: String = text
                .chars()
                .take(col)
                .map(|c| if c == '\t' { '\t' } else { ' ' })
                .collect();
            writeln!(
                out,
                "{pad} | {lead}{} {}",
                "^".repeat(end.saturating_sub(col).max(1)),
                d.code
            )?;
        }
    }
    writeln!(out, "{pad} |")
}

pub fn summary(out: &mut impl Write, s: &Summary) -> io::Result<()> {
    if s.fix_mode && s.fixed > 0 {
        writeln!(
            out,
            "Fixed {} finding{} ({} remaining).",
            s.fixed,
            plural(s.fixed),
            s.findings
        )?;
    }
    if s.findings == 0 {
        if !(s.fix_mode && s.fixed > 0) {
            writeln!(
                out,
                "All checks passed! ({} file{} checked)",
                s.files,
                plural(s.files)
            )?;
        }
        return Ok(());
    }
    writeln!(
        out,
        "Found {} finding{} in {} file{} checked ({:.2}s).",
        s.findings,
        plural(s.findings),
        s.files,
        plural(s.files),
        s.started.elapsed().as_secs_f64()
    )?;
    if s.fixable_safe > 0 {
        writeln!(
            out,
            "[*] {} fixable with the `--fix` option.",
            s.fixable_safe
        )?;
    }
    if s.fixable_unsafe > 0 {
        writeln!(
            out,
            "{} more fix{} available with `--unsafe-fixes`.",
            s.fixable_unsafe,
            if s.fixable_unsafe == 1 { "" } else { "es" }
        )?;
    }
    Ok(())
}

pub fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

/// One finding as a JSON object.
pub fn finding_json(r: &FileReport, d: &Diagnostic) -> Value {
    let sp = span(&r.source, d);
    let fix = d.fix.as_ref().map(|f| {
        json!({
            "applicability": f.applicability,
            "message": f.message,
            "edits": f.edits.iter().map(|e| {
                let s = r.source.location(e.range.start());
                let en = r.source.location(e.range.end());
                json!({
                    "content": e.content,
                    "location": {"row": s.line, "column": s.column},
                    "end_location": {"row": en.line, "column": en.column},
                })
            }).collect::<Vec<_>>(),
        })
    });
    json!({
        "code": d.code,
        "name": d.name(),
        "message": d.message,
        "severity": d.severity,
        "filename": r.display,
        "location": {"row": sp.line, "column": sp.column},
        "end_location": {"row": sp.end_line, "column": sp.end_column},
        "cell": sp.cell,
        "cell_line": sp.cell_line,
        "help": d.help,
        "fix": fix,
    })
}

/// Pretty-print a JSON document followed by a newline.
pub fn write_json(out: &mut impl Write, v: &impl serde::Serialize) -> io::Result<()> {
    serde_json::to_writer_pretty(&mut *out, v)?;
    writeln!(out)
}

pub fn json(out: &mut impl Write, reports: &[FileReport]) -> io::Result<()> {
    let all: Vec<Value> = reports
        .iter()
        .flat_map(|r| r.diagnostics.iter().map(move |d| finding_json(r, d)))
        .collect();
    write_json(out, &all)
}

pub fn escape_property(s: &str) -> String {
    s.replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
        .replace(':', "%3A")
        .replace(',', "%2C")
}

pub fn escape_data(s: &str) -> String {
    s.replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
}

pub fn github_level(s: Severity) -> &'static str {
    match s {
        Severity::Error => "error",
        Severity::Warning => "warning",
        Severity::Info => "notice",
    }
}

/// The text of a finding's GitHub annotation: `path:line:col: CODE message`.
pub fn github_message(r: &FileReport, d: &Diagnostic) -> String {
    let sp = span(&r.source, d);
    format!("{}: {} {}", location_prefix(r, &sp), d.code, d.message)
}

/// One `::level title=…,file=…,line=…::message` workflow command for a finding.
pub fn github_annotation(
    out: &mut impl Write,
    r: &FileReport,
    d: &Diagnostic,
    message: &str,
) -> io::Result<()> {
    let sp = span(&r.source, d);
    writeln!(
        out,
        "::{} title={},file={},line={},col={},endLine={},endColumn={}::{}",
        github_level(d.severity),
        escape_property(&format!("sparklint ({})", d.code)),
        escape_property(&r.display),
        sp.line,
        sp.column,
        sp.end_line,
        sp.end_column,
        escape_data(message)
    )
}

pub fn github(out: &mut impl Write, reports: &[FileReport]) -> io::Result<()> {
    for r in reports {
        for d in &r.diagnostics {
            github_annotation(out, r, d, &github_message(r, d))?;
        }
    }
    Ok(())
}

pub fn sarif_level(s: Severity) -> &'static str {
    match s {
        Severity::Error => "error",
        Severity::Warning => "warning",
        Severity::Info => "note",
    }
}

/// The codes with a finding (sorted; a result's `ruleIndex` points into it) and
/// their SARIF rule descriptors.
pub fn sarif_rules(reports: &[FileReport]) -> (Vec<&'static str>, Vec<Value>) {
    let mut used: Vec<&str> = reports
        .iter()
        .flat_map(|r| r.diagnostics.iter().map(|d| d.code))
        .collect();
    used.sort_unstable();
    used.dedup();
    let rules = used
        .iter()
        .filter_map(|c| registry::get(c))
        .map(|r| {
            json!({
                "id": r.code,
                "name": r.name,
                "shortDescription": {"text": r.message},
                "fullDescription": {"text": r.docs},
                "help": {"text": r.docs},
                "defaultConfiguration": {"level": sarif_level(r.severity)},
                "properties": {"tags": r.tags},
            })
        })
        .collect();
    (used, rules)
}

/// One finding as a SARIF result (`used`: the codes of [`sarif_rules`]).
pub fn sarif_result(r: &FileReport, d: &Diagnostic, used: &[&str]) -> Value {
    let sp = span(&r.source, d);
    json!({
        "ruleId": d.code,
        "ruleIndex": used.iter().position(|c| *c == d.code),
        "level": sarif_level(d.severity),
        "message": {"text": d.message},
        "locations": [{
            "physicalLocation": {
                "artifactLocation": {"uri": r.display.replace('\\', "/")},
                "region": {
                    "startLine": sp.line,
                    "startColumn": sp.column,
                    "endLine": sp.end_line,
                    "endColumn": sp.end_column,
                }
            }
        }],
    })
}

/// The SARIF 2.1.0 log of one run.
pub fn sarif_log(rules: Vec<Value>, results: Vec<Value>) -> Value {
    json!({
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [{
            "tool": {"driver": {
                "name": "sparklint",
                "version": env!("CARGO_PKG_VERSION"),
                "informationUri": env!("CARGO_PKG_REPOSITORY"),
                "rules": rules,
            }},
            "results": results,
        }],
    })
}

pub fn sarif(out: &mut impl Write, reports: &[FileReport]) -> io::Result<()> {
    let (used, rules) = sarif_rules(reports);
    let results = reports
        .iter()
        .flat_map(|r| r.diagnostics.iter().map(|d| sarif_result(r, d, &used)))
        .collect();
    write_json(out, &sarif_log(rules, results))
}

pub fn statistics(
    out: &mut impl Write,
    reports: &[FileReport],
    format: OutputFormat,
) -> io::Result<()> {
    let mut counts: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for d in reports.iter().flat_map(|r| &r.diagnostics) {
        let e = counts.entry(d.code).or_default();
        e.0 += 1;
        if d.fix
            .as_ref()
            .is_some_and(|f| f.applicability == Applicability::Safe)
        {
            e.1 += 1;
        }
    }
    let mut rows: Vec<(&str, usize, usize)> =
        counts.into_iter().map(|(c, (n, f))| (c, n, f)).collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    if format == OutputFormat::Json {
        let v: Vec<Value> = rows
            .iter()
            .map(|(c, n, f)| {
                json!({
                    "code": c,
                    "name": registry::get(c).map(|r| r.name.as_str()),
                    "count": n,
                    "fixable": f,
                })
            })
            .collect();
        return write_json(out, &v);
    }
    let width = rows
        .iter()
        .map(|r| r.1.to_string().len())
        .max()
        .unwrap_or(1);
    for (c, n, f) in rows {
        let name = registry::get(c).map_or("", |r| r.name.as_str());
        let mark = if f > 0 { "[*]" } else { "   " };
        writeln!(out, "{n:>width$}\t{c}\t{mark} {name}")?;
    }
    Ok(())
}
