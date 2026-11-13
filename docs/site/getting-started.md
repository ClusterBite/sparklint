---
title: Getting started
description: Install sparklint, lint PySpark code and notebooks, apply fixes and read the output.
---

# Getting started

## Install

```bash
pip install sparklint      # or: uv tool install sparklint / pipx install sparklint
sparklint --version
```

Wheels are published for Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows
(x86_64). They contain a native binary, so Python is only needed to install it; linting
does not need PySpark or a JVM. Other platforms build from the source distribution, which
needs a Rust toolchain (1.97+).

`python -m sparklint` runs the same binary.

## Lint

```bash
sparklint check                    # the current directory
sparklint check src/ notebooks/    # .py files, Databricks source notebooks, .ipynb
sparklint check jobs/etl.py        # one file
```

Discovery honours `.gitignore` (also outside git repos), the configured `exclude` /
`extend-exclude`, and default excludes such as `.venv`, `build` and `__pycache__`. Files
named on the command line are always linted.

Editors and scripts can pipe one file through standard input:

```bash
sparklint check - --stdin-filename etl.py < etl.py
```

`--stdin-filename` picks the configuration, the file kind (`.ipynb`) and the name in the
output. With `--fix`, the fixed source goes to stdout and the report to stderr.

## Understand a finding

Every finding has a rule code. `sparklint rule` explains it, and each rule has a page in
the [rule list](rules/index.md):

```bash
sparklint rule ACT002              # by code
sparklint rule collect-unbounded   # or by name
sparklint rules                    # every rule of your installed version
sparklint rules --format json
```

## Fix

```bash
sparklint check --fix              # apply safe fixes
sparklint check --unsafe-fixes     # also apply fixes that can change behaviour
```

Findings with a fix are marked `[*]` in the text output. A **safe** fix keeps the
program's meaning, for example `if df.count() > 0:` → `if not df.isEmpty():`. An
**unsafe** fix is probably what you want but can change results or needs a newer Spark,
for example `.collect()` → `.limit(1000).collect()`. Set `spark-version` in the
[configuration](configuration.md) so fixes that need a newer Spark are not offered.

Fixes work in Jupyter notebooks too: the fixed code is written back into the cells. A
file with a syntax error is not rewritten (in a notebook, only the broken cell is left
alone).

## Output

`--output-format text|json|github|sarif`

- `text` (default): ruff-style, with a code frame and the suggested fix.
- `json`: one object per finding (code, name, severity, location, fix edits).
- `github`: GitHub Actions workflow annotations.
- `sarif`: SARIF 2.1.0, for GitHub code scanning and other SARIF viewers.

`--statistics` prints counts per rule instead of each finding, and `-q` drops the
summary line.

## Exit codes

- `0`: no findings.
- `1`: findings.
- `2`: error (bad configuration, unknown rule selector, missing path).

`--exit-zero` exits with 0 even when there are findings.

## Next steps

- Choose rules and set options: [Configuration](configuration.md).
- Run it in CI or pre-commit: [CI and editors](ci.md).
- Coming from cylint: [Migrating from cylint](cylint.md).
