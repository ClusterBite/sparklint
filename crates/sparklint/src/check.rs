//! `sparklint check`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use rayon::prelude::*;
use sparklint_linter::config::{CliOverrides, ConfigResolver, Configuration};
use sparklint_linter::project::{self, ProjectFile};
use sparklint_linter::{Applicability, Diagnostic, lint_and_fix, lint_source};
use sparklint_semantic::module_name_for_path;
use sparklint_source::SourceFile;

use crate::output::{self, FileReport};
use crate::{CheckArgs, EXIT_CLEAN, EXIT_ERROR, EXIT_FINDINGS};

struct Loaded {
    path: PathBuf,
    /// The text as read from standard input (`check -`); fixes go to stdout.
    stdin: Option<String>,
    display: String,
    source: SourceFile,
    config: Arc<Configuration>,
    module: String,
}

fn display_path(cwd: &Path, path: &Path) -> String {
    path.strip_prefix(cwd)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

pub fn run(args: &CheckArgs) -> anyhow::Result<u8> {
    let started = Instant::now();
    let cwd = std::env::current_dir().context("cannot determine the current directory")?;
    let dash = args.paths.iter().any(|p| p.as_os_str() == "-");
    if dash && args.paths.len() > 1 {
        anyhow::bail!("`-` (standard input) cannot be combined with other paths");
    }
    let stdin = dash || (args.paths.is_empty() && args.stdin_filename.is_some());
    let roots: Vec<PathBuf> = if stdin {
        Vec::new()
    } else if args.paths.is_empty() {
        vec![cwd.clone()]
    } else {
        args.paths
            .iter()
            .map(|p| std::path::absolute(p).unwrap_or_else(|_| p.clone()))
            .collect()
    };
    let cli = CliOverrides {
        select: args.select.clone(),
        extend_select: args.extend_select.clone(),
        ignore: args.ignore.clone(),
    };
    let explicit = args
        .config
        .as_ref()
        .map(|p| std::path::absolute(p).unwrap_or_else(|_| p.clone()));
    let resolver = Arc::new(ConfigResolver::new(explicit.as_deref(), cli, &cwd)?);
    for r in &roots {
        if r.is_file() {
            resolver.for_dir(r.parent().unwrap_or(&cwd))?;
        }
    }
    // `check -`: one file from standard input, named by `--stdin-filename` (relative
    // to the current directory) or `-`.
    let stdin_path = stdin.then(|| match &args.stdin_filename {
        Some(p) => std::path::absolute(p).unwrap_or_else(|_| p.clone()),
        None => cwd.join("-"),
    });
    let stdin_text = match &stdin_path {
        Some(_) => {
            let mut text = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)
                .context("cannot read standard input")?;
            Some(text)
        }
        None => None,
    };
    let (files, warnings) = crate::discover::discover(&roots, &resolver)?;
    // A path that does not exist is an error (exit 2), shown relative to the current
    // directory like every other path; other discovery problems (an unreadable
    // directory entry) stay warnings.
    let mut errors = 0usize;
    for r in roots.iter().filter(|r| !r.is_file() && !r.is_dir()) {
        eprintln!(
            "sparklint: error: {}: no such file or directory",
            display_path(&cwd, r)
        );
        errors += 1;
    }
    for w in warnings.iter().filter(|w| !w.contains("no such file")) {
        eprintln!("sparklint: warning: {w}");
    }

    // Load every file (parallel).
    let mut loaded: Vec<Result<Loaded, String>> = files
        .par_iter()
        .map(|path| {
            let display = display_path(&cwd, path);
            let config = resolver
                .for_dir(path.parent().unwrap_or(&cwd))
                .map_err(|e| format!("{display}: {e}"))?;
            let source = SourceFile::load(path).map_err(|e| format!("{display}: {e}"))?;
            let module = module_name_for_path(&cwd, path);
            Ok(Loaded {
                path: path.clone(),
                stdin: None,
                display,
                source,
                config,
                module,
            })
        })
        .collect();
    if let (Some(path), Some(text)) = (stdin_path, stdin_text) {
        loaded.push(load_stdin(&cwd, &resolver, path, text));
    }
    let mut sources = Vec::with_capacity(loaded.len());
    for l in loaded {
        match l {
            Ok(l) => sources.push(l),
            Err(e) => {
                eprintln!("sparklint: error: {e}");
                errors += 1;
            }
        }
    }

    // Phase A: cross-file function summaries.
    let settings: Vec<_> = sources
        .iter()
        .map(|l| l.config.settings_for(&l.path))
        .collect();
    for (l, s) in sources.iter_mut().zip(&settings) {
        if s.ipython_syntax() {
            l.source = l.source.clone().with_ipython_syntax(true);
        }
    }
    let files: Vec<ProjectFile> = sources
        .iter()
        .zip(&settings)
        .map(|(l, s)| ProjectFile {
            source: &l.source,
            settings: s,
            module: l.module.clone(),
        })
        .collect();
    let index = project::index(&files);
    drop(files);

    // Phase B: lint (and fix) each file.
    let fix = args.fix || args.unsafe_fixes;
    let results: Vec<(Loaded, Vec<Diagnostic>, Option<String>, usize)> = sources
        .into_par_iter()
        .zip(&settings)
        .map(|(l, settings)| {
            let module = Some(l.module.clone());
            if fix {
                let out = lint_and_fix(&l.source, settings, &index, module, args.unsafe_fixes);
                (l, out.diagnostics, out.fixed_text, out.fixes_applied)
            } else {
                let d = lint_source(&l.source, settings, &index, module);
                (l, d, None, 0)
            }
        })
        .collect();

    let mut reports = Vec::with_capacity(results.len());
    let mut fixed_total = 0;
    for (l, diagnostics, fixed_text, applied) in results {
        if let Some(original) = &l.stdin {
            if fix {
                // Like ruff: the (fixed) source goes to stdout, the report to stderr.
                let text = fixed_text.as_deref().unwrap_or(original);
                let stdout = std::io::stdout();
                let mut out = stdout.lock();
                out.write_all(text.as_bytes())?;
                out.flush()?;
                if fixed_text.is_some() {
                    fixed_total += applied;
                }
            }
            let source = match fixed_text {
                Some(text) => l.source.with_contents(text).unwrap_or(l.source),
                None => l.source,
            };
            reports.push(FileReport {
                display: l.display,
                source,
                diagnostics,
            });
            continue;
        }
        let source = match fixed_text {
            Some(text) => {
                if let Err(e) = std::fs::write(&l.path, &text) {
                    eprintln!("sparklint: error: cannot write {}: {e}", l.display);
                    errors += 1;
                    l.source
                } else {
                    fixed_total += applied;
                    l.source.with_contents(text).unwrap_or(l.source)
                }
            }
            None => l.source,
        };
        reports.push(FileReport {
            display: l.display,
            source,
            diagnostics,
        });
    }

    reports.sort_by(|a, b| a.display.cmp(&b.display));

    let total: usize = reports.iter().map(|r| r.diagnostics.len()).sum();
    let fixable_safe = count_fixable(&reports, Applicability::Safe);
    let fixable_unsafe = count_fixable(&reports, Applicability::Unsafe);
    let summary = output::Summary {
        files: reports.len(),
        findings: total,
        fixed: fixed_total,
        fixable_safe,
        fixable_unsafe,
        fix_mode: fix,
        errors,
        started,
    };
    // With `check - --fix` stdout carries the fixed source, so the report goes to stderr.
    let mut out: Box<dyn Write> = if stdin && fix {
        Box::new(std::io::stderr().lock())
    } else {
        Box::new(std::io::stdout().lock())
    };
    print(&mut out, args, &reports, &summary)?;
    out.flush()?;

    Ok(if errors > 0 {
        EXIT_ERROR
    } else if total > 0 && !args.exit_zero {
        EXIT_FINDINGS
    } else {
        EXIT_CLEAN
    })
}

/// Print the findings (or `--statistics`) and, for text output, the summary.
fn print(
    out: &mut impl Write,
    args: &CheckArgs,
    reports: &[FileReport],
    summary: &output::Summary,
) -> std::io::Result<()> {
    if args.statistics {
        output::statistics(out, reports, args.output_format)?;
    } else {
        match args.output_format {
            crate::OutputFormat::Text => output::text(out, reports)?,
            crate::OutputFormat::Json => output::json(out, reports)?,
            crate::OutputFormat::Github => output::github(out, reports)?,
            crate::OutputFormat::Sarif => output::sarif(out, reports)?,
        }
    }
    if args.output_format == crate::OutputFormat::Text && !args.quiet {
        output::summary(out, summary)?;
    }
    Ok(())
}

/// The file read from standard input: config and module name from its stated path,
/// `.ipynb` JSON when that path says so; nothing is read from or written to disk.
fn load_stdin(
    cwd: &Path,
    resolver: &ConfigResolver,
    path: PathBuf,
    text: String,
) -> Result<Loaded, String> {
    let display = display_path(cwd, &path);
    let config = resolver
        .for_dir(path.parent().unwrap_or(cwd))
        .map_err(|e| format!("{display}: {e}"))?;
    let ipynb = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("ipynb"));
    let source = if ipynb {
        SourceFile::from_ipynb_json(&path, &text).map_err(|e| format!("{display}: {e}"))?
    } else {
        SourceFile::from_text(&path, text.clone())
    };
    let module = module_name_for_path(cwd, &path);
    Ok(Loaded {
        path,
        stdin: Some(text),
        display,
        source,
        config,
        module,
    })
}

fn count_fixable(reports: &[FileReport], a: Applicability) -> usize {
    reports
        .iter()
        .flat_map(|r| &r.diagnostics)
        .filter(|d| d.fix.as_ref().is_some_and(|f| f.applicability == a))
        .count()
}
