# sparklint

A fast linter for PySpark code and Databricks notebooks, written in Rust and installed
with pip.

sparklint looks for Spark code habits that make jobs slow and expensive: unbounded
`collect()`s, `count()` used as an emptiness check or only for a log line, Python UDFs
where a built-in function exists, actions and writes inside loops, caches used once,
cartesian joins, `coalesce(1)` before a write, risky Spark configs, and so on. On
Databricks these patterns turn directly into extra cluster hours.

It tracks Spark types (SparkSession, DataFrame, Column, GroupedData, Window, RDD, readers,
writers, streams), so rules fire only on real Spark objects. A `count()` on a Python list
or a pandas DataFrame is not reported, and DataFrames passed into functions or returned
from helpers in other modules are still recognised.

## What sparklint does not tell you

sparklint reads code. It cannot tell you which finding costs real money, or how much:
that depends on how big the data is, how often the job runs and what the cluster costs.
And many Databricks savings are not in the code at all: cluster and warehouse sizing,
idle time, job compute versus all-purpose compute, spot instances, tables nobody reads.

[ClusterBite](https://clusterbite.com/?utm_source=github&utm_medium=readme&utm_campaign=sparklint) finds those from your workspace's own usage data and measures what each change saved.

**Documentation: [clusterbite.github.io/sparklint](https://clusterbite.github.io/sparklint/)**

**Status: early (0.x).** Rules, codes and output may still change between minor versions.
Reports of false positives are especially welcome.

## Install

```bash
pip install sparklint      # or: uv tool install sparklint / pipx install sparklint
sparklint --version
```

Wheels are published for Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows
(x86_64). They contain a native binary, so Python is only needed to install it. Other
platforms build from the source distribution, which needs a Rust toolchain (1.97+).

## Quickstart

```bash
sparklint check                    # lint the current directory
sparklint check src/ notebooks/    # .py files, Databricks source notebooks, .ipynb
sparklint check --fix              # apply safe fixes
sparklint check --unsafe-fixes     # also apply fixes that can change behaviour
sparklint check --statistics       # counts per rule
sparklint check - --stdin-filename etl.py < etl.py   # read stdin (editors); --fix prints the fixed source
sparklint rules                    # list the rules
sparklint rule ACT002              # explain one rule (code or name, e.g. collect-unbounded)
```

Example: save this as `etl.py` and run `sparklint check etl.py`.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
orders = spark.read.parquet("/data/orders")
print(f"rows: {orders.count()}")
if orders.count() > 0:
    orders.write.mode("overwrite").saveAsTable("analytics.big_orders")
```

```text
etl.py:5:23: ACT007 orders.count() is computed only to be logged — a full job for a log line.
  |
4 | orders = spark.read.parquet("/data/orders")
5 | print(f"rows: {orders.count()}")
  |                       ^^^^^^^ ACT007
6 | if orders.count() > 0:
  |
  = help: Drop it, read numOutputRows from the Delta commit, or use df.observe() to compute the metric in the same job.

etl.py:6:4: ACT006 [*] orders.count() used as an emptiness check scans every row — use orders.isEmpty().
  |
5 | print(f"rows: {orders.count()}")
6 | if orders.count() > 0:
  |    ^^^^^^^^^^^^^^^^^^ ACT006
7 |     orders.write.mode("overwrite").saveAsTable("analytics.big_orders")
  |
  = fix: Replace with `not orders.isEmpty()`

Found 2 findings in 1 file checked (0.00s).
[*] 1 fixable with the `--fix` option.
```

Exit codes: `0` no findings, `1` findings, `2` error (bad config, unknown rule selector,
missing path). `--exit-zero` exits 0 even when there are findings.

### Output formats

`--output-format text|json|github|sarif`

- `text` (default): ruff-style, with a code frame and the suggested fix.
- `json`: one object per finding (code, name, severity, location, fix edits).
- `github`: GitHub Actions workflow annotations.
- `sarif`: SARIF 2.1.0, for GitHub code scanning and other SARIF viewers.

### Selecting rules

Rule codes are a three-letter family plus three digits: `ACT` actions and driver, `PRF`
plan and shuffle performance, `UDF` user-defined functions, `IOD` I/O and Delta, `CFG`
Spark config, `STM` Structured Streaming, `COR` correctness, `DBX` Databricks platform,
`PDS` pandas-on-Spark, `STY` style.

```bash
sparklint check --select ACT,PRF          # families or single codes
sparklint check --extend-select ACT001    # add to the configured selection
sparklint check --ignore STY              # turn rules off
sparklint check --select tag:cost         # by tag
sparklint check --select review           # presets: default, review, strict, cylint-compat, serverless
```

### Configuration

Configuration goes in `[tool.sparklint]` in `pyproject.toml`, or in `sparklint.toml`
(the closest file wins):

```toml
[tool.sparklint]
extend-select = ["ACT001"]
ignore = ["STY"]
extend-exclude = ["generated/**"]
spark-version = "3.5"        # fixes that need a newer Spark are not offered
```

Test code usually calls actions on purpose: an assert like `df.count() > 0` or a
`collect()` on a small fixture is fine there. Turn the action rules off for tests:

```toml
[tool.sparklint.per-file-ignores]
"tests/**" = ["ACT"]
```

All options are listed in the [configuration docs](https://clusterbite.github.io/sparklint/configuration/).

### Suppressions

Suppress a finding with `# noqa: ACT002` or `# sparklint: ignore[ACT002]` on the line, or
`# sparklint: file-ignore[ACT002]` for a whole file. cylint's `# cy:ignore CY001` comments
are honoured too.

## Rules

Every rule, with its rationale, detection logic and examples, has a page on the
[documentation site](https://clusterbite.github.io/sparklint/rules/) (source:
[docs/RULES_CATALOG.md](https://github.com/clusterbite/sparklint/blob/main/docs/RULES_CATALOG.md)).
`sparklint rules` lists the rules of your installed version, and `sparklint rule <CODE>`
prints the details of one.

## CI and editors

### GitHub Actions

```yaml
# .github/workflows/sparklint.yml
name: sparklint
on: [push, pull_request]
jobs:
  sparklint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-python@v7
        with:
          python-version: "3.12"
      - run: pip install sparklint
      - run: sparklint check --output-format github .
```

For GitHub code scanning, write SARIF and upload it:

```yaml
      - run: sparklint check --output-format sarif --exit-zero . > sparklint.sarif
      - uses: github/codeql-action/upload-sarif@v4
        with:
          sarif_file: sparklint.sarif
```

### pre-commit

With the published wheel (fast, no Rust toolchain needed):

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: sparklint
        name: sparklint
        entry: sparklint check
        language: python
        additional_dependencies: ["sparklint==0.2.0"]
        types_or: [python, jupyter]
```

This repository also has a `.pre-commit-hooks.yaml`, so
`repo: https://github.com/clusterbite/sparklint` with `rev: v0.2.0` and `id: sparklint`
works as well, but pre-commit then builds sparklint from source, which needs a Rust
toolchain and takes a few minutes the first time.

### Editors

There is no language server yet. Run `sparklint check` from your editor's task runner,
or feed the `json` / `sarif` output to a plugin that reads them.

## Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](https://github.com/clusterbite/sparklint/blob/main/CONTRIBUTING.md). False
positives are bugs: please include a minimal snippet. For security issues see
[SECURITY.md](https://github.com/clusterbite/sparklint/blob/main/SECURITY.md). Design notes are in [docs/DESIGN.md](https://github.com/clusterbite/sparklint/blob/main/docs/DESIGN.md).

## License

Apache-2.0. See [LICENSE](https://github.com/clusterbite/sparklint/blob/main/LICENSE) and [NOTICE](https://github.com/clusterbite/sparklint/blob/main/NOTICE).

Built by [ClusterBite](https://clusterbite.com/?utm_source=github&utm_medium=readme&utm_campaign=sparklint),
which works on Databricks cost optimization.
