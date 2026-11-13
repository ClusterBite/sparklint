//! Fixture harness: `tests/fixtures/<CODE>/*.{py,ipynb}` and multi-file projects in
//! `tests/fixtures/<CODE>/<case>/` (every file under the subdirectory is linted as
//! one project, module names relative to it — for cross-file cases).
//!
//! Each fixture is linted with only `<CODE>` selected. Every line carrying a trailing
//! `# expect: CODE[, CODE…]` comment must produce that diagnostic (on its first line),
//! and no other line may produce it. If `<name>.py.fixed` (`<name>.ipynb.fixed`) exists, applying the safe
//! fixes to `<name>.py` must give exactly that text (for `.ipynb`: the notebook JSON);
//! likewise `<name>.py.unsafe-fixed` for safe + unsafe fixes (`--unsafe-fixes`).
//!
//! A `sparklint.toml` in a unit's root (the rule directory for single files, the case
//! directory for projects) supplies non-selection options (rule thresholds and the
//! like); the selection always comes from the test.
//!
//! The preset pass ([`preset_fixtures`]) lints the same files with `ALL` and with the
//! default selection: a rule's findings in its own fixture dir must not depend on the
//! selection, except that a finding is dropped when a higher-precedence rule
//! (`sparklint_linter::overlap::PRECEDENCE`) fired at the same site. In a rule's
//! `overlap.py` every finding must be dropped that way under every preset that
//! enables the rule beating it (under `ALL`, always), and every precedence pair must
//! be exercised by some fixture.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use sparklint_linter::config::{CliOverrides, ConfigFile, Configuration};
use sparklint_linter::overlap::PRECEDENCE;
use sparklint_linter::project::ProjectFile;
use sparklint_linter::{Diagnostic, LinterSettings, lint_and_fix};
use sparklint_semantic::{NoSummaries, module_name_for_path};
use sparklint_source::SourceFile;

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures")
}

fn expected_lines(source: &SourceFile, code: &str) -> BTreeSet<u32> {
    let mut out = BTreeSet::new();
    for line in 1..=source.line_index.line_count() {
        let text = source.line_text(line);
        if let Some(idx) = text.find("# expect:") {
            let codes = &text[idx + "# expect:".len()..];
            if codes.split(',').any(|c| c.trim() == code) {
                out.insert(line);
            }
        }
    }
    out
}

fn is_source(p: &Path) -> bool {
    p.extension().is_some_and(|e| e == "py" || e == "ipynb")
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            files_under(&p, out);
        } else if is_source(&p) {
            out.push(p);
        }
    }
}

/// One lint unit: a single fixture file, or every file of a project subdirectory.
struct Unit {
    /// Module names are relative to this root.
    root: PathBuf,
    files: Vec<PathBuf>,
}

fn fixture_units(dir: &Path) -> Vec<Unit> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect();
    entries.sort();
    let mut units = Vec::new();
    for p in entries {
        if p.is_dir() {
            let mut files = Vec::new();
            files_under(&p, &mut files);
            if !files.is_empty() {
                units.push(Unit { root: p, files });
            }
        } else if is_source(&p) {
            units.push(Unit {
                root: dir.to_path_buf(),
                files: vec![p],
            });
        }
    }
    units
}

/// Settings for a unit: `select` plus the options of the unit's `sparklint.toml`.
fn unit_settings(unit: &Unit, select: &[&str]) -> LinterSettings {
    let toml = unit.root.join("sparklint.toml");
    if !toml.is_file() {
        return if select == ["default"] {
            LinterSettings::default()
        } else {
            LinterSettings::for_selectors(select).unwrap()
        };
    }
    let file = ConfigFile::load(&toml).unwrap().unwrap();
    let cli = CliOverrides {
        select: Some(select.iter().map(|s| s.to_string()).collect()),
        ..CliOverrides::default()
    };
    Configuration::new(&unit.root, Some(toml), &file, &cli)
        .unwrap()
        .base()
        .clone()
}

/// Lint one unit as a project: per file, its source and diagnostics.
fn lint_unit(
    unit: &Unit,
    settings: &LinterSettings,
) -> Vec<(PathBuf, SourceFile, Vec<Diagnostic>)> {
    let sources: Vec<SourceFile> = unit
        .files
        .iter()
        .map(|f| SourceFile::load(f).unwrap())
        .collect();
    let files: Vec<ProjectFile> = sources
        .iter()
        .zip(&unit.files)
        .map(|(s, p)| ProjectFile {
            source: s,
            settings,
            module: module_name_for_path(&unit.root, p),
        })
        .collect();
    let diags = sparklint_linter::lint_project(&files);
    drop(files);
    unit.files
        .iter()
        .cloned()
        .zip(sources)
        .zip(diags)
        .map(|((p, s), d)| (p, s, d))
        .collect()
}

#[test]
fn rule_fixtures() {
    let root = fixtures_root();
    let dirs = rule_dirs();
    assert!(!dirs.is_empty());
    let mut failures = Vec::new();
    let mut checked = 0;
    for dir in &dirs {
        let code = dir.file_name().unwrap().to_string_lossy().into_owned();
        let mut positives = 0;
        let mut pure_negatives = 0;
        for unit in fixture_units(dir) {
            let settings = unit_settings(&unit, &[code.as_str()]);
            let single = unit.files.len() == 1 && unit.root == *dir;
            for (file, source, diags) in lint_unit(&unit, &settings) {
                let actual: BTreeSet<u32> = diags
                    .iter()
                    .filter(|d| d.code == code)
                    .map(|d| source.line_index.line_of(d.range.start()))
                    .collect();
                let expected = expected_lines(&source, &code);
                positives += expected.len();
                if expected.is_empty() {
                    pure_negatives += 1;
                }
                let name = file.strip_prefix(&root).unwrap().display().to_string();
                for l in expected.difference(&actual) {
                    failures.push(format!("{name}:{l}: expected {code}, not reported"));
                }
                for l in actual.difference(&expected) {
                    let msg = diags
                        .iter()
                        .find(|d| source.line_index.line_of(d.range.start()) == *l)
                        .map(|d| d.message.clone())
                        .unwrap_or_default();
                    failures.push(format!("{name}:{l}: unexpected {code}: {msg}"));
                }
                if single {
                    let ext = file.extension().unwrap().to_string_lossy();
                    let fixed_path = file.with_extension(format!("{}.fixed", ext));
                    if fixed_path.is_file() {
                        let want = std::fs::read_to_string(&fixed_path).unwrap();
                        let out = lint_and_fix(&source, &settings, &NoSummaries, None, false);
                        let got = out
                            .fixed_text
                            .unwrap_or_else(|| source.raw_text().to_string());
                        if got != want {
                            failures.push(format!("{name}: fix output differs:\n{got}"));
                        }
                    }
                    let unsafe_path = file.with_extension(format!("{}.unsafe-fixed", ext));
                    if unsafe_path.is_file() {
                        let want = std::fs::read_to_string(&unsafe_path).unwrap();
                        let out = lint_and_fix(&source, &settings, &NoSummaries, None, true);
                        let got = out
                            .fixed_text
                            .unwrap_or_else(|| source.raw_text().to_string());
                        if got != want {
                            failures.push(format!("{name}: unsafe-fix output differs:\n{got}"));
                        }
                    }
                }
                checked += 1;
            }
        }
        if positives == 0 {
            failures.push(format!("{code}: no positive fixture"));
        }
        if pure_negatives == 0 {
            failures.push(format!(
                "{code}: no negative fixture (a file without expectations)"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{checked} fixtures, failures:\n{}",
        failures.join("\n")
    );
}

#[test]
fn every_implemented_rule_has_fixtures() {
    let root = fixtures_root();
    for r in sparklint_linter::registry::rules() {
        if r.implemented && r.code != sparklint_linter::registry::SYNTAX_ERROR {
            assert!(
                root.join(&r.code).is_dir(),
                "missing fixtures for {}",
                r.code
            );
        }
    }
}

/// Whether `hi` outranks `lo` in the precedence table (transitively).
fn outranks(hi: &str, lo: &str) -> bool {
    let mut frontier = vec![lo];
    let mut seen = BTreeSet::new();
    while let Some(c) = frontier.pop() {
        for (h, l) in PRECEDENCE {
            if *l == c && seen.insert(*h) {
                if *h == hi {
                    return true;
                }
                frontier.push(h);
            }
        }
    }
    false
}

/// The rule directories of `tests/fixtures`, sorted.
fn rule_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(fixtures_root())
        .expect("tests/fixtures exists")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs
}

/// The higher-precedence findings at `d`'s sites.
fn higher_at(all: &[Diagnostic], d: &Diagnostic) -> Vec<&'static str> {
    all.iter()
        .filter(|h| h.sites().any(|s| s == d.site_range()) && outranks(h.code, d.code))
        .map(|h| h.code)
        .collect()
}

#[test]
fn preset_fixtures() {
    let root = fixtures_root();
    let mut failures = Vec::new();
    // (higher, lower) pairs seen suppressing a finding.
    let mut exercised: BTreeSet<(&str, &str)> = BTreeSet::new();
    let key = |d: &Diagnostic| {
        (
            u32::from(d.range.start()),
            u32::from(d.range.end()),
            d.message.clone(),
        )
    };
    for dir in rule_dirs() {
        let code = dir.file_name().unwrap().to_string_lossy().into_owned();
        for unit in fixture_units(&dir) {
            let alone = unit_settings(&unit, &[code.as_str()]);
            let all_settings = unit_settings(&unit, &["ALL"]);
            let presets = [
                ("ALL", all_settings.clone()),
                ("default", unit_settings(&unit, &["default"])),
            ];
            let own_all = lint_unit(&unit, &alone);
            let under_all = lint_unit(&unit, &all_settings);
            for (preset, settings) in presets.iter() {
                if !settings.is_enabled(&code) {
                    continue;
                }
                let owned;
                let results = if *preset == "ALL" {
                    &under_all
                } else {
                    owned = lint_unit(&unit, settings);
                    &owned
                };
                for (((file, source, own), (_, _, all)), (_, _, everything)) in
                    own_all.iter().zip(results).zip(&under_all)
                {
                    let own: Vec<&Diagnostic> = own.iter().filter(|d| d.code == code).collect();
                    let name = file.strip_prefix(&root).unwrap().display().to_string();
                    let got: BTreeSet<_> = all.iter().filter(|d| d.code == code).map(key).collect();
                    let want: BTreeSet<_> = own.iter().map(|d| key(d)).collect();
                    for d in all.iter().filter(|d| d.code == code) {
                        if !want.contains(&key(d)) {
                            let l = source.line_index.line_of(d.range.start());
                            failures.push(format!(
                                "{name}:{l}: [{preset}] {code} reported only with other rules enabled: {}",
                                d.message
                            ));
                        }
                    }
                    // `overlap.py`: every finding of the rule loses to a higher one whenever
                    // the preset enables a rule that beats it under ALL.
                    let overlap = file.file_stem().is_some_and(|s| s == "overlap");
                    if overlap {
                        for d in own.iter().filter(|d| got.contains(&key(d))) {
                            let beaten_by = higher_at(everything, d);
                            if *preset == "ALL" || beaten_by.iter().any(|h| settings.is_enabled(h))
                            {
                                let l = source.line_index.line_of(d.range.start());
                                failures.push(format!(
                                    "{name}:{l}: [{preset}] {code} should lose to a higher-precedence rule"
                                ));
                            }
                        }
                    }
                    for d in own.iter().filter(|d| !got.contains(&key(d))) {
                        let l = source.line_index.line_of(d.range.start());
                        let higher = higher_at(all, d);
                        if higher.is_empty() {
                            failures.push(format!(
                                "{name}:{l}: [{preset}] {code} dropped without a higher-precedence finding at its site"
                            ));
                        }
                        for h in higher {
                            exercised.insert((h, d.code));
                        }
                    }
                }
            }
        }
    }
    // Every precedence pair is exercised by some fixture.
    for (hi, lo) in PRECEDENCE {
        if !exercised.contains(&(*hi, *lo)) {
            failures.push(format!(
                "precedence {hi} > {lo} is not exercised by any fixture"
            ));
        }
    }
    assert!(failures.is_empty(), "failures:\n{}", failures.join("\n"));
}
