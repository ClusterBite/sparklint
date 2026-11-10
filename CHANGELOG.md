# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor versions may change rules and
output).

## [Unreleased]

## [0.1.0] - 2026-11-10

First public release.

- `sparklint check` for `.py` files, Databricks source notebooks and `.ipynb` notebooks,
  built on Ruff's Python parser.
- Notebook-aware: `%magic` and `!shell` lines and non-Python cells are skipped, the last
  expression of a cell counts as displayed, and a syntax error in one cell does not hide
  findings in the others.
- A Spark type oracle (SparkSession, DataFrame, Column, GroupedData, Window, RDD,
  readers/writers, streaming) with cross-file function summaries, so rules fire only on
  known Spark receivers.
- 91 rules across the ACT, PRF, UDF, IOD, CFG, STM, COR, DBX, PDS and STY families,
  plus SYN001 for files that do not parse.
- Safe and unsafe fixes (`--fix`, `--unsafe-fixes`).
- Output formats: text, json, github, sarif; `--statistics`; exit codes 0/1/2.
- Configuration in `[tool.sparklint]` (`pyproject.toml`) or `sparklint.toml`: selectors,
  tags, presets (default, review, strict, cylint-compat, serverless), per-file ignores,
  severities, rule options.
- Suppressions: `# noqa`, `# sparklint: ignore[...]`, `# sparklint: file-ignore[...]`,
  and cylint's `# cy:ignore` (mapped to sparklint codes).
- `sparklint rules` and `sparklint rule <CODE>` print the rule catalog.
