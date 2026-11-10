//! File discovery: walks the given paths honouring `.gitignore` and the configured
//! `exclude` globs (closest config wins); explicitly named files are always linted.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ignore::WalkBuilder;
use sparklint_linter::config::{ConfigError, ConfigResolver};

/// Whether a path looks like a lintable file.
pub fn is_lintable(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("py") || e.eq_ignore_ascii_case("ipynb"))
}

/// Collect the files to lint (absolute, sorted, de-duplicated).
pub fn discover(
    roots: &[PathBuf],
    resolver: &Arc<ConfigResolver>,
) -> Result<(Vec<PathBuf>, Vec<String>), ConfigError> {
    let mut files = Vec::new();
    let mut warnings = Vec::new();
    for root in roots {
        if root.is_file() {
            files.push(root.clone());
            continue;
        }
        if !root.is_dir() {
            warnings.push(format!("{}: no such file or directory", root.display()));
            continue;
        }
        // Surface config errors up-front rather than silently skipping.
        resolver.for_dir(root)?;
        let r = Arc::clone(resolver);
        let walk_root = root.clone();
        let walker = WalkBuilder::new(root)
            .hidden(false)
            .git_ignore(true)
            .git_exclude(true)
            .git_global(false)
            .require_git(false)
            .parents(true)
            .sort_by_file_path(|a, b| a.cmp(b))
            .filter_entry(move |entry| {
                let path = entry.path();
                let Some(parent) = path.parent() else {
                    return true;
                };
                match r.for_dir(parent) {
                    // Exclude globs are relative to the config root. When the walked
                    // directory is outside it (no config found: the fallback root is the
                    // cwd) or is itself excluded (named explicitly, e.g. `sparklint check
                    // build/`), match relative to the walked directory instead, like ruff.
                    Ok(cfg) => {
                        let rel = if path.starts_with(&cfg.root) && !cfg.is_excluded(&walk_root) {
                            path
                        } else {
                            path.strip_prefix(&walk_root).unwrap_or(path)
                        };
                        !cfg.is_excluded(rel)
                    }
                    Err(_) => true,
                }
            })
            .build();
        for entry in walker {
            match entry {
                Ok(e) => {
                    if e.file_type().is_some_and(|t| t.is_file()) && is_lintable(e.path()) {
                        files.push(e.into_path());
                    }
                }
                Err(err) => warnings.push(err.to_string()),
            }
        }
    }
    files.sort();
    files.dedup();
    Ok((files, warnings))
}
