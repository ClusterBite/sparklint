//! Fixture harness: `tests/fixtures/<CODE>/*.{py,ipynb}` and multi-file projects in
//! `tests/fixtures/<CODE>/<case>/` (every file under the subdirectory is linted as
//! one project, module names relative to it — for cross-file cases).
//!
//! Each fixture is linted with only `<CODE>` selected. Every line carrying a trailing
//! `# expect: CODE[, CODE…]` comment must produce that diagnostic (on its first line),
//! and no other line may produce it. Each code may carry checks that every diagnostic of
//! that code on the line must pass (see [`parse_expect`]):
//! `# expect: ACT004[info] ~"rows from df.collect()" !~"Inside a loop" help~"limit("`
//! = severity, message contains / does not contain, help contains (`"` or `'` quotes). If `<name>.py.fixed` (`<name>.ipynb.fixed`) exists, applying the safe
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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use sparklint_linter::config::{CliOverrides, ConfigFile, Configuration};
use sparklint_linter::diagnostic::Severity;
use sparklint_linter::overlap::PRECEDENCE;
use sparklint_linter::project::ProjectFile;
use sparklint_linter::rules::msg::Msg;
use sparklint_linter::{Diagnostic, LinterSettings, lint_and_fix};
use sparklint_semantic::{NoSummaries, module_name_for_path};
use sparklint_source::SourceFile;

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures")
}

/// What one `# expect:` entry asks of the diagnostics of its code on its line.
#[derive(Debug, Default, Clone)]
struct Expect {
    code: String,
    severity: Option<Severity>,
    /// Fragments the message must contain.
    contains: Vec<String>,
    /// Fragments the message must not contain.
    lacks: Vec<String>,
    /// Fragments the help text must contain.
    help: Vec<String>,
}

impl Expect {
    /// Why `d` fails this expectation, if it does.
    fn mismatch(&self, d: &Diagnostic) -> Option<String> {
        if let Some(want) = self.severity.filter(|w| *w != d.severity) {
            return Some(format!(
                "severity {}, expected {}: {}",
                d.severity.as_str(),
                want.as_str(),
                d.message
            ));
        }
        if let Some(f) = self
            .contains
            .iter()
            .find(|f| !d.message.contains(f.as_str()))
        {
            return Some(format!("message lacks {f:?}: {}", d.message));
        }
        if let Some(f) = self.lacks.iter().find(|f| d.message.contains(f.as_str())) {
            return Some(format!("message contains {f:?}: {}", d.message));
        }
        let help = d.help.as_deref().unwrap_or_default();
        if let Some(f) = self
            .help
            .iter()
            .find(|f| d.help.is_none() || !help.contains(f.as_str()))
        {
            return Some(format!("help lacks {f:?}: {help}"));
        }
        None
    }
}

/// Why `d` was not reported through one of its rule's declared message variants
/// (`sparklint_linter::rules::msg::Msg`): the variant id must be declared and the
/// message and help must be its templates filled in. Those declarations are what
/// spec/rules.json, `sparklint rule CODE` and the docs show.
fn variant_mismatch(d: &Diagnostic) -> Option<String> {
    let Some(id) = d.variant else {
        return Some(format!(
            "reported without a declared message variant (use msg!): {}",
            d.message
        ));
    };
    let Some(m) = sparklint_linter::rules::messages_of(d.code).find(|m| m.id == id) else {
        return Some(format!(
            "variant `{id}` is not declared on the rule's table entry"
        ));
    };
    // config-value rows add one shared note when the key is set back later
    let set_back = d
        .message
        .strip_suffix(sparklint_linter::rules::conf::SET_BACK_NOTE)
        .is_some_and(|base| m.matches(base));
    if !m.matches(&d.message) && !set_back {
        return Some(format!(
            "message is not variant `{id}` ({:?}): {}",
            m.text, d.message
        ));
    }
    match (m.help, &d.help) {
        (None, None) => None,
        (Some(t), Some(h)) if Msg::new(m.code, m.id, t).matches(h) => None,
        (t, h) => Some(format!(
            "help {h:?} is not variant `{id}`'s help template {t:?}"
        )),
    }
}

/// Parses the text after `# expect:`: comma-separated entries
/// `CODE [ '[' severity ']' ] { ~"msg" | !~"msg" | help~"text" }` (fragments in `"` or
/// `'` quotes, no escapes; commas inside quotes are part of the fragment).
fn parse_expect(text: &str) -> Result<Vec<Expect>, String> {
    let mut out = Vec::new();
    let mut rest = text.trim_start();
    loop {
        let end = rest
            .find(|c: char| !c.is_ascii_alphanumeric())
            .unwrap_or(rest.len());
        let code = &rest[..end];
        if code.is_empty() {
            return Err(format!("expected a rule code at {rest:?}"));
        }
        let mut e = Expect {
            code: code.to_string(),
            ..Expect::default()
        };
        rest = &rest[end..];
        if let Some(r) = rest.strip_prefix('[') {
            let close = r.find(']').ok_or("unclosed `[`")?;
            let name = &r[..close];
            e.severity = Some(Severity::parse(name).ok_or(format!("unknown severity {name:?}"))?);
            rest = &r[close + 1..];
        }
        loop {
            rest = rest.trim_start();
            let (list, r) = if let Some(r) = rest.strip_prefix("!~") {
                (&mut e.lacks, r)
            } else if let Some(r) = rest.strip_prefix("help~") {
                (&mut e.help, r)
            } else if let Some(r) = rest.strip_prefix('~') {
                (&mut e.contains, r)
            } else {
                break;
            };
            let quote = r
                .chars()
                .next()
                .filter(|q| *q == '"' || *q == '\'')
                .ok_or(format!("expected a quoted fragment at {r:?}"))?;
            let close = r[1..]
                .find(quote)
                .ok_or(format!("unclosed {quote} fragment"))?;
            list.push(r[1..1 + close].to_string());
            rest = &r[close + 2..];
        }
        out.push(e);
        rest = rest.trim_start();
        if rest.is_empty() {
            return Ok(out);
        }
        rest = rest
            .strip_prefix(',')
            .ok_or(format!("expected `,` or the end of the line at {rest:?}"))?
            .trim_start();
    }
}

/// `# expect:` entries by line (1-based).
type Expectations = BTreeMap<u32, Vec<Expect>>;

/// The `# expect:` entries of every line, and a parse error per bad line.
fn expectations(source: &SourceFile) -> (Expectations, Vec<(u32, String)>) {
    let mut out = BTreeMap::new();
    let mut errors = Vec::new();
    for line in 1..=source.line_index.line_count() {
        let text = source.line_text(line);
        if let Some(idx) = text.find("# expect:") {
            match parse_expect(&text[idx + "# expect:".len()..]) {
                Ok(entries) => {
                    out.insert(line, entries);
                }
                Err(e) => errors.push((line, e)),
            }
        }
    }
    (out, errors)
}

/// The lines expecting `code`, with that code's entries.
fn expected_for<'a>(all: &'a Expectations, code: &str) -> BTreeMap<u32, Vec<&'a Expect>> {
    all.iter()
        .filter_map(|(l, es)| {
            let mine: Vec<&Expect> = es.iter().filter(|e| e.code == code).collect();
            (!mine.is_empty()).then_some((*l, mine))
        })
        .collect()
}

#[test]
fn expect_syntax() {
    let es = parse_expect(
        " ACT004[info] ~\"rows from df.collect()\" !~'Inside, a loop' help~\"x\", ACT002",
    )
    .unwrap();
    assert_eq!(es.len(), 2);
    assert_eq!(es[0].code, "ACT004");
    assert_eq!(es[0].severity, Some(Severity::Info));
    assert_eq!(es[0].contains, ["rows from df.collect()"]);
    assert_eq!(es[0].lacks, ["Inside, a loop"]);
    assert_eq!(es[0].help, ["x"]);
    assert_eq!(es[1].code, "ACT002");
    assert!(es[1].severity.is_none() && es[1].contains.is_empty());
    let plain = parse_expect(" ACT002, PRF001").unwrap();
    assert_eq!(
        plain.iter().map(|e| e.code.as_str()).collect::<Vec<_>>(),
        ["ACT002", "PRF001"]
    );
    for bad in [
        " ACT002 note",
        " ACT002[loud]",
        " ACT002 ~\"open",
        " ACT002,",
        " ACT002 ~x",
        "",
    ] {
        assert!(parse_expect(bad).is_err(), "{bad:?}");
    }
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

/// Declared message variants no input reaches yet (keep this list short: a variant
/// here is wording no fixture checks).
const UNREACHED_VARIANTS: &[(&str, &str)] = &[
    // the oracle never marks a pandas-on-Spark frame filtered/aggregated (`reduced`)
    ("ACT003", "reduced-pandas-on-spark"),
];

#[test]
fn rule_fixtures() {
    let root = fixtures_root();
    let dirs = rule_dirs();
    assert!(!dirs.is_empty());
    let mut failures = Vec::new();
    let mut checked = 0;
    let mut exercised: BTreeSet<(&str, &str)> = BTreeSet::new();
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
                let name = file.strip_prefix(&root).unwrap().display().to_string();
                let (all_expect, errors) = expectations(&source);
                for (l, e) in errors {
                    failures.push(format!("{name}:{l}: bad `# expect:` comment: {e}"));
                }
                let entries = expected_for(&all_expect, &code);
                let expected: BTreeSet<u32> = entries.keys().copied().collect();
                positives += expected.len();
                if expected.is_empty() {
                    pure_negatives += 1;
                }
                for d in diags.iter().filter(|d| d.code == code) {
                    let l = source.line_index.line_of(d.range.start());
                    if let Some(why) = variant_mismatch(d) {
                        failures.push(format!("{name}:{l}: {code}: {why}"));
                    }
                    if let Some(id) = d.variant {
                        exercised.insert((d.code, id));
                    }
                    for e in entries.get(&l).into_iter().flatten() {
                        if let Some(why) = e.mismatch(d) {
                            failures.push(format!("{name}:{l}: {code}: {why}"));
                        }
                    }
                }
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
    // Every declared message variant is reported by some fixture.
    for m in sparklint_linter::rules::messages() {
        if !exercised.contains(&(m.code, m.id)) && !UNREACHED_VARIANTS.contains(&(m.code, m.id)) {
            failures.push(format!(
                "{}: message variant `{}` is reported by no fixture",
                m.code, m.id
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

/// Property over every fixture file, linted with `ALL`: each fix on its own leaves a
/// file that parses (no new syntax error), and fixing to a fixed point converges (no
/// fix is proposed again after the fix loop: a fix that re-creates its own finding
/// would keep the loop going until its round limit).
#[test]
fn every_fix_parses_and_converges() {
    let mut files = Vec::new();
    files_under(&fixtures_root(), &mut files);
    let settings = LinterSettings::for_selectors(&["ALL"]).unwrap();
    let mut failures = Vec::new();
    let mut checked = 0;
    for file in &files {
        let name = file.display().to_string();
        let source = SourceFile::load(file).unwrap();
        let before = sparklint_linter::parse_source(&source).syntax_errors.len();
        let diags = sparklint_linter::lint_source(&source, &settings, &NoSummaries, None);
        // One fix at a time (plain text files; notebook JSON is covered by the loop below).
        if file.extension().is_some_and(|e| e == "py") {
            for d in diags.iter().filter(|d| d.fix.is_some()) {
                let (text, applied) = sparklint_linter::fix::apply_fixes(
                    source.raw_text(),
                    std::slice::from_ref(d),
                    true,
                );
                if applied == 0 {
                    continue;
                }
                checked += 1;
                let fixed = SourceFile::from_text(file, text.clone());
                let after = sparklint_linter::parse_source(&fixed).syntax_errors.len();
                if after > before {
                    let line = source.line_index.line_of(d.range.start());
                    failures.push(format!(
                        "{name}:{line}: {} fix `{}` does not parse:\n{text}",
                        d.code,
                        d.fix.as_ref().unwrap().message
                    ));
                }
            }
        }
        let out = lint_and_fix(&source, &settings, &NoSummaries, None, true);
        for d in out.diagnostics.iter().filter(|d| d.fix.is_some()) {
            let line = d.range.start();
            failures.push(format!(
                "{name}: {} at offset {line:?} still has a fix after fixing ({})",
                d.code,
                d.fix.as_ref().unwrap().message
            ));
        }
    }
    assert!(checked > 100, "only {checked} fixes checked");
    assert!(failures.is_empty(), "failures:\n{}", failures.join("\n"));
}
