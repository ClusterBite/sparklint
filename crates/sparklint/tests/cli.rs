//! End-to-end tests of the `sparklint` binary.

use std::path::Path;
use std::process::{Command, Output};

fn run(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_sparklint"))
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run sparklint")
}

/// Run with `input` on standard input.
fn run_stdin(dir: &Path, args: &[&str], input: &str) -> Output {
    use std::io::Write;
    use std::process::Stdio;
    let mut child = Command::new(env!("CARGO_BIN_EXE_sparklint"))
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run sparklint");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().expect("wait for sparklint")
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn write(dir: &Path, rel: &str, text: &str) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, text).unwrap();
}

const COLLECT: &str = "df = spark.table('t')\nrows = df.collect()\n";

#[test]
fn exit_codes_and_text_output() {
    let t = tempfile::tempdir().unwrap();
    write(t.path(), "clean.py", "x = [1, 2]\nx.count(1)\n");
    let o = run(t.path(), &["check", "clean.py"]);
    assert_eq!(o.status.code(), Some(0), "{}", stdout(&o));
    assert!(stdout(&o).contains("All checks passed"));

    write(t.path(), "bad.py", COLLECT);
    let o = run(t.path(), &["check"]);
    assert_eq!(o.status.code(), Some(1));
    let out = stdout(&o);
    assert!(out.contains("bad.py:2:"), "{out}");
    assert!(out.contains("ACT002"), "{out}");
    assert!(out.contains("^^^"), "code frame: {out}");
    assert!(out.contains("= help:"), "{out}");

    let o = run(t.path(), &["check", "--exit-zero"]);
    assert_eq!(o.status.code(), Some(0));

    let o = run(t.path(), &["check", "--select", "NOPE999"]);
    assert_eq!(o.status.code(), Some(2));
    let o = run(t.path(), &["check", "missing_dir"]);
    assert_eq!(o.status.code(), Some(2));
    let out = stdout(&o);
    assert!(!out.contains("All checks passed"), "{out}");
    assert!(
        out.contains("No findings, but 1 error (see above): 0 files checked."),
        "{out}"
    );
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("sparklint: error: missing_dir: no such file or directory"),
        "{err}"
    );

    // a missing path next to a clean file: still exit 2, still no "passed"
    let o = run(t.path(), &["check", "clean.py", "missing.py"]);
    assert_eq!(o.status.code(), Some(2));
    let out = stdout(&o);
    assert!(
        out.contains("No findings, but 1 error (see above): 1 file checked."),
        "{out}"
    );

    // findings and a missing path: the findings summary plus the error count
    let o = run(t.path(), &["check", "bad.py", "missing.py"]);
    assert_eq!(o.status.code(), Some(2));
    let out = stdout(&o);
    assert!(out.contains("Found 1 finding in 1 file"), "{out}");
    assert!(out.contains("1 error (see above)."), "{out}");
}

#[test]
fn stdin_input() {
    let t = tempfile::tempdir().unwrap();
    // Named `-` by default; nothing is written to disk.
    let o = run_stdin(t.path(), &["check", "-"], COLLECT);
    assert_eq!(o.status.code(), Some(1), "{}", stdout(&o));
    assert!(stdout(&o).contains("-:2:"), "{}", stdout(&o));
    assert!(stdout(&o).contains("1 file checked"), "{}", stdout(&o));
    assert_eq!(std::fs::read_dir(t.path()).unwrap().count(), 0);

    // `--stdin-filename` alone implies `-`, names the file and picks its config.
    let o = run_stdin(
        t.path(),
        &["check", "--stdin-filename", "src/etl.py"],
        COLLECT,
    );
    assert_eq!(o.status.code(), Some(1));
    assert!(stdout(&o).contains("src/etl.py:2:"), "{}", stdout(&o));
    write(t.path(), "sub/sparklint.toml", "ignore = [\"ACT002\"]\n");
    let o = run_stdin(
        t.path(),
        &["check", "-", "--stdin-filename", "sub/job.py"],
        COLLECT,
    );
    assert_eq!(o.status.code(), Some(0), "{}", stdout(&o));
    // Even when that path does not exist on disk, and in JSON.
    let o = run_stdin(
        t.path(),
        &[
            "check",
            "-",
            "--stdin-filename",
            "gone/job.py",
            "--output-format",
            "json",
        ],
        COLLECT,
    );
    assert_eq!(o.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v[0]["filename"], "gone/job.py");
    assert_eq!(v[0]["code"], "ACT002");

    // `.ipynb` input is read as notebook JSON.
    let nb = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/ACT010/display.ipynb"),
    )
    .unwrap();
    let o = run_stdin(
        t.path(),
        &[
            "check",
            "-",
            "--stdin-filename",
            "nb.ipynb",
            "--select",
            "ACT010",
        ],
        &nb,
    );
    assert_eq!(o.status.code(), Some(1), "{}", stdout(&o));
    assert!(stdout(&o).contains("nb.ipynb:cell "), "{}", stdout(&o));

    // `--fix`: the fixed source on stdout, the report on stderr, no file written.
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/ACT006");
    let src = std::fs::read_to_string(fixture.join("fix.py")).unwrap();
    let fixed = std::fs::read_to_string(fixture.join("fix.py.fixed")).unwrap();
    let o = run_stdin(
        t.path(),
        &["check", "-", "--fix", "--select", "ACT006"],
        &src,
    );
    assert_eq!(o.status.code(), Some(0));
    assert_eq!(stdout(&o), fixed);
    assert!(String::from_utf8_lossy(&o.stderr).contains("Fixed 2 findings"));
    // Nothing to fix: the source comes back unchanged.
    let o = run_stdin(t.path(), &["check", "-", "--fix"], "x = 1\n");
    assert_eq!(stdout(&o), "x = 1\n");

    // `-` with other paths is a usage error.
    write(t.path(), "a.py", "x = 1\n");
    let o = run_stdin(t.path(), &["check", "-", "a.py"], "x = 1\n");
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("cannot be combined"));
}

#[test]
fn json_github_sarif_statistics() {
    let t = tempfile::tempdir().unwrap();
    write(t.path(), "a.py", COLLECT);
    let o = run(t.path(), &["check", "--output-format", "json"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v[0]["code"], "ACT002");
    assert_eq!(v[0]["filename"], "a.py");
    assert_eq!(v[0]["location"]["row"], 2);
    assert_eq!(v[0]["fix"]["applicability"], "unsafe");

    let o = run(t.path(), &["check", "--output-format", "github"]);
    let out = stdout(&o);
    assert!(
        out.starts_with("::warning title=sparklint (ACT002),file=a.py,line=2"),
        "{out}"
    );

    let o = run(t.path(), &["check", "--output-format", "sarif"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["version"], "2.1.0");
    assert_eq!(v["runs"][0]["results"][0]["ruleId"], "ACT002");
    assert_eq!(v["runs"][0]["tool"]["driver"]["rules"][0]["id"], "ACT002");

    let o = run(t.path(), &["check", "--statistics"]);
    let out = stdout(&o);
    assert!(
        out.contains("ACT002") && out.contains("collect-unbounded"),
        "{out}"
    );
}

#[test]
fn config_gitignore_and_excludes() {
    let t = tempfile::tempdir().unwrap();
    write(t.path(), "src/a.py", COLLECT);
    write(t.path(), "build/gen.py", COLLECT);
    write(t.path(), "ignored/x.py", COLLECT);
    write(t.path(), "tests/t.py", COLLECT);
    write(t.path(), ".gitignore", "ignored/\n");
    write(
        t.path(),
        "pyproject.toml",
        "[project]\nname='x'\n[tool.sparklint]\nextend-select = [\"ACT001\"]\n[tool.sparklint.per-file-ignores]\n\"tests/**\" = [\"ACT\"]\n",
    );
    let o = run(t.path(), &["check", "--output-format", "json"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    let mut seen: Vec<(String, String)> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["filename"].as_str().unwrap().to_string(),
                d["code"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("src/a.py".to_string(), "ACT001".to_string()),
            ("src/a.py".to_string(), "ACT002".to_string()),
        ]
    );
    // An excluded directory named explicitly is linted.
    let o = run(t.path(), &["check", "--output-format", "json", "build"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v[0]["filename"], "build/gen.py", "{v}");
    // sparklint.toml beats pyproject.toml in the same directory; nested config wins.
    write(t.path(), "src/sparklint.toml", "ignore = [\"ACT002\"]\n");
    let o = run(t.path(), &["check", "--output-format", "json", "src"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 0, "{v}");
}

#[test]
fn cross_file_helper_return() {
    let t = tempfile::tempdir().unwrap();
    write(
        t.path(),
        "pkg/io.py",
        "from pyspark.sql import SparkSession\n\ndef load(name):\n    s = SparkSession.builder.getOrCreate()\n    return s.table(name)\n",
    );
    write(t.path(), "pkg/__init__.py", "");
    write(
        t.path(),
        "job.py",
        "from pkg.io import load\n\nevents = load('e')\nrows = events.collect()\n",
    );
    let o = run(t.path(), &["check", "--output-format", "json"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v[0]["filename"], "job.py", "{v}");
    assert_eq!(v[0]["code"], "ACT002");
}

#[test]
fn fix_writes_files_and_syntax_errors_do_not_abort() {
    let t = tempfile::tempdir().unwrap();
    write(
        t.path(),
        "a.py",
        "df = spark.table('t')\nif df.count() == 0:\n    pass\n",
    );
    write(t.path(), "broken.py", "def f(:\n");
    let o = run(t.path(), &["check", "--fix"]);
    assert_eq!(o.status.code(), Some(1), "syntax error remains");
    let fixed = std::fs::read_to_string(t.path().join("a.py")).unwrap();
    assert!(fixed.contains("if df.isEmpty():"), "{fixed}");
    let out = stdout(&o);
    assert!(
        out.contains("broken.py:1:") && out.contains("SYN001"),
        "{out}"
    );
}

#[test]
fn notebook_on_reads_plain_module_magics() {
    // `notebook = "on"` reads a plain .py as a notebook: magics and shell escapes
    // parse (IPython mode) instead of hiding the file behind SYN001.
    let t = tempfile::tempdir().unwrap();
    let job = "%pip install x\nfiles = !ls\ndf = spark.read.table(\"t\")\nrows = df.collect()\n!echo hi\n";
    write(t.path(), "job.py", job);
    let o = run(t.path(), &["check", "--select", "SYN001,ACT002"]);
    assert!(stdout(&o).contains("SYN001"), "{}", stdout(&o));
    assert!(!stdout(&o).contains("ACT002"), "{}", stdout(&o));
    write(
        t.path(),
        "pyproject.toml",
        "[tool.sparklint]\nnotebook = \"on\"\n",
    );
    let o = run(t.path(), &["check", "--select", "SYN001,ACT002"]);
    assert_eq!(o.status.code(), Some(1), "{}", stdout(&o));
    assert!(!stdout(&o).contains("SYN001"), "{}", stdout(&o));
    assert!(stdout(&o).contains("job.py:4:11: ACT002"), "{}", stdout(&o));
    // Fixing keeps IPython mode for the re-check: the magics stay and nothing is
    // reported as a syntax error afterwards.
    let o = run(
        t.path(),
        &[
            "check",
            "--select",
            "SYN001,ACT002",
            "--fix",
            "--unsafe-fixes",
        ],
    );
    assert_eq!(o.status.code(), Some(0), "{}", stdout(&o));
    assert_eq!(
        std::fs::read_to_string(t.path().join("job.py")).unwrap(),
        job.replace("df.collect()", "df.limit(1000).collect()")
    );
}

#[test]
fn fix_writes_ipynb_cells() {
    let t = tempfile::tempdir().unwrap();
    let nb = r##"{
 "cells": [
  {"cell_type": "markdown", "metadata": {}, "source": ["# Title"]},
  {"cell_type": "code", "execution_count": null, "metadata": {}, "outputs": [],
   "source": ["%pip install x\n", "df = spark.table('t')\n", "if df.count() == 0:\n", "    pass"]},
  {"cell_type": "code", "execution_count": null, "metadata": {}, "outputs": [],
   "source": ["s = <broken\n", "if df.count() == 0:\n", "    pass"]}
 ],
 "metadata": {},
 "nbformat": 4,
 "nbformat_minor": 4
}
"##;
    write(t.path(), "nb.ipynb", nb);
    let o = run(t.path(), &["check", "--fix", "--select", "ACT006"]);
    assert_eq!(o.status.code(), Some(0), "{}", stdout(&o));
    assert!(stdout(&o).contains("Fixed 1 finding"), "{}", stdout(&o));
    let fixed = std::fs::read_to_string(t.path().join("nb.ipynb")).unwrap();
    let v: serde_json::Value = serde_json::from_str(&fixed).unwrap();
    let cell = |i: usize| -> String {
        v["cells"][i]["source"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l.as_str().unwrap())
            .collect()
    };
    assert_eq!(cell(0), "# Title");
    assert_eq!(
        cell(1),
        "%pip install x\ndf = spark.table('t')\nif df.isEmpty():\n    pass"
    );
    // The cell that does not parse is left as it is.
    assert_eq!(cell(2), "s = <broken\nif df.count() == 0:\n    pass");
    // A second run has nothing left to fix and leaves the file alone.
    let o = run(t.path(), &["check", "--fix", "--select", "ACT006"]);
    assert_eq!(o.status.code(), Some(0));
    assert_eq!(
        std::fs::read_to_string(t.path().join("nb.ipynb")).unwrap(),
        fixed
    );
}

#[test]
fn notebook_cells_in_output() {
    let t = tempfile::tempdir().unwrap();
    write(
        t.path(),
        "nb.py",
        "# Databricks notebook source\nx = 1\n\n# COMMAND ----------\n\ndf = spark.table('t')\ndisplay(df)\nrows = df.collect()\n",
    );
    let o = run(t.path(), &["check", "--output-format", "json"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    let d = v
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["code"] == "ACT002")
        .unwrap();
    assert_eq!(d["cell"], 2);
    assert_eq!(d["location"]["row"], 8);
    let o = run(t.path(), &["check"]);
    assert!(stdout(&o).contains("(cell 2)"));
}

#[test]
fn rule_and_rules_commands() {
    let t = tempfile::tempdir().unwrap();
    let o = run(t.path(), &["rule", "ACT002"]);
    assert_eq!(o.status.code(), Some(0));
    let out = stdout(&o);
    assert!(
        out.contains("collect-unbounded") && out.contains("CY001"),
        "{out}"
    );
    let o = run(t.path(), &["rule", "collect-unbounded"]);
    assert_eq!(o.status.code(), Some(0));
    let o = run(t.path(), &["rule", "XXX"]);
    assert_eq!(o.status.code(), Some(2));
    let o = run(t.path(), &["rules", "--format", "json"]);
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v.as_array().unwrap().len() > 80);
    let o = run(t.path(), &["rules", "--implemented"]);
    assert!(stdout(&o).lines().count() >= 16);
    let o = run(t.path(), &["version"]);
    assert!(stdout(&o).starts_with("sparklint "));
}
