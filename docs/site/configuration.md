---
title: Configuration
description: Configure sparklint in pyproject.toml or sparklint.toml - rule selection, presets, tags, per-file ignores, severities and rule options.
---

# Configuration

Configuration goes in `[tool.sparklint]` in `pyproject.toml`, or at the top level of
`sparklint.toml` / `.sparklint.toml`. For each file, the closest configuration in its
directory or a parent directory wins; in one directory, `sparklint.toml` wins over
`pyproject.toml`. `--config FILE` uses one file for everything.

```toml
[tool.sparklint]
extend-select = ["ACT001"]
ignore = ["STY"]
extend-exclude = ["generated/**"]
spark-version = "3.5"

[tool.sparklint.per-file-ignores]
"tests/**" = ["ACT"]
```

In `sparklint.toml`, drop the `tool.sparklint` prefix (`[per-file-ignores]`).

## Selecting rules

A selector can be:

- a rule code: `ACT002`
- a prefix: `ACT` (a family), `ACT0`
- a rule name: `collect-unbounded`
- a tag: `tag:cost`
- a preset: `default`, `review`, `strict`, `cylint-compat`, `serverless`
- `ALL`

```bash
sparklint check --select ACT,PRF          # replaces the configured selection
sparklint check --extend-select ACT001    # adds to it
sparklint check --ignore STY              # turns rules off
sparklint check --select tag:cost
sparklint check --select review
```

The same keys work in the configuration (`select`, `extend-select`, `ignore`). The most
specific selector wins, so `select = ["ACT"]` with `ignore = ["ACT001"]` runs every ACT
rule except ACT001. Per-file ignores always win.

### Presets

- `default`: the rules marked *Default* in the [rule list](rules/index.md). Used when
  nothing is selected.
- `review`: every rule except style, including the info-level cost rules and the ACT001
  inventory of every Spark action. Meant for a one-off code review, not CI.
- `strict`: every rule.
- `cylint-compat`: cylint's default rule set, for [migrating](cylint.md).
- `serverless` (alias `uc-migration`): `default` plus every `DBX` rule (Unity Catalog,
  standard access mode, serverless, Spark Connect).

`preset = "review"` in the configuration is the same as `select = ["review"]`.

### Tags

Tags cut across families:

- `cost`: directly burns compute or driver time.
- `correctness`: wrong results or runtime failures.
- `connect`, `serverless`, `uc-standard`: breaks or misbehaves on that compute.
- `spark4`: deprecated or removed in Spark 4.
- `notebook`: only relevant in notebooks.
- `style`, `security`, `pipelines`.

The [rule list](rules/index.md#by-tag) shows which rules carry each tag.

### Severity

```toml
[tool.sparklint.severity]
ACT = "error"          # any selector -> error | warning | info
```

### Per-file ignores

Test code usually calls actions on purpose: an assert like `df.count() > 0` or a
`collect()` on a small fixture is fine there.

```toml
[tool.sparklint.per-file-ignores]
"tests/**" = ["ACT"]
"notebooks/scratch/*.py" = ["ALL"]
```

## All options

```toml
[tool.sparklint]
select = ["default"]            # or: preset = "review"
extend-select = ["ACT001"]
ignore = ["STY"]
exclude = ["build", "generated/**"]   # replaces the default excludes
extend-exclude = ["vendor/**"]        # adds to them
known-spark-globals = ["ss", "ctx:SparkContext"]
notebook = "auto"               # auto | on | off
heuristics = true
spark-version = "3.5"
collect-limit = 1000
cube-min-columns = 5
rollup-min-columns = 8
pipeline-fanout-min-flows = 3
distinct-aggregates-min = 3
zorder-max-columns = 3
broadcast-threshold-max = "1g"
functions-aliases = ["F", "sf"]
types-aliases = ["T"]
```

- **`exclude` / `extend-exclude`**: glob patterns of paths to skip. `exclude` replaces
  the default list (`.venv`, `build`, `__pycache__`, `site-packages`, …);
  `extend-exclude` adds to it.
- **`known-spark-globals`**: names that hold Spark objects without being assigned in the
  file, for jobs where the platform injects them. A bare name is a SparkSession;
  `name:Type` gives another type (`ctx:SparkContext`).
- **`notebook`**: `auto` treats Databricks source notebooks and `.ipynb` files as
  notebooks (the `spark`, `sc`, `display`, `dbutils` globals exist). `on` treats every
  file as a notebook, including plain `.py` files with `%pip` or `!cmd` lines. `off`
  never does. See [Notebooks](notebooks.md).
- **`heuristics`**: also use variable names (`df`, `*_df`, …) to guess DataFrames when
  nothing else says what a value is. Findings that rely on a guess are reported at lower
  confidence; rules that report errors need a certain type. Set `false` to rely only on
  what sparklint can prove.
- **`spark-version`**: your Spark version. Fixes that need a newer one are not offered
  (`isEmpty()` needs 3.3).
- **`collect-limit`**: the `N` in ACT002's unsafe fix `.limit(N).collect()`.
- **`cube-min-columns`** / **`rollup-min-columns`**: [PRF024](rules/PRF024.md) reports
  `cube()` / `rollup()` with at least this many columns. `0` turns it off.
- **`pipeline-fanout-min-flows`**: [DBX017](rules/DBX017.md) reports a pipeline source
  streamed by at least this many datasets. `0` turns it off.
- **`distinct-aggregates-min`**: [PRF026](rules/PRF026.md) reports an aggregation with
  at least this many distinct aggregates over different columns. `0` turns it off.
- **`zorder-max-columns`**: [IOD020](rules/IOD020.md) reports `ZORDER BY` with more
  columns than this. `0` turns it off.
- **`broadcast-threshold-max`**: [CFG007](rules/CFG007.md) reports
  `autoBroadcastJoinThreshold` at or above this size (`"1g"`, `"512m"`, bytes). `"0"`
  turns it off.
- **`functions-aliases`** / **`types-aliases`**: the accepted import aliases for
  `pyspark.sql.functions` and `pyspark.sql.types` ([STY001](rules/STY001.md)).
