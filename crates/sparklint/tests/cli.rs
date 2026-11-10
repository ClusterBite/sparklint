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
