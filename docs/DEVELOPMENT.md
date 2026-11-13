# sparklint — development

## Toolchain

Rust 1.97 or newer (edition 2024) and, for the wheel, [maturin](https://www.maturin.rs/).

## Layout

| Path | What |
|---|---|
| `crates/sparklint_source` | Loader: `.py`, Databricks source notebooks, `.ipynb` (ruff_notebook), cell map |
| `crates/sparklint_semantic` | Scopes/bindings, def-use, Spark type oracle, function summaries, `ProjectIndex` |
| `crates/sparklint_linter` | Registry (`spec/rules.json`), `Checker`, rules, noqa, config, fixes |
| `crates/sparklint` | CLI binary (`check`, `rule`, `rules`, `version`): `check.rs`, `discover.rs` (file discovery), `output.rs` (text / json / github / sarif), `rules_cmd.rs` |
| `python/sparklint` | Python package: `find_sparklint_bin()`, `python -m sparklint` |
| `spec/pyspark_api.json` | Generated PySpark API model (`scripts/gen_api.py`) |
| `spec/rules.json` | Rule metadata generated from `docs/RULES_CATALOG.md` (`scripts/gen_rules.py`) |
| `tests/fixtures/<CODE>/` | Rule fixtures (see below) |
| `docs/site/`, `mkdocs.yml` | Documentation site (see below) |

## Build, test, lint

```bash
cargo build                                   # debug binary: target/debug/sparklint
cargo test --workspace                        # unit + fixture + CLI end-to-end tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

### Rule fixtures

`crates/sparklint_linter/tests/fixtures.rs` lints every `tests/fixtures/<CODE>/*.py` and
`*.ipynb` with only `<CODE>` selected (each file as its own project, through
`sparklint_linter::lint_project`, the same index → lint path as `check`). A subdirectory
`tests/fixtures/<CODE>/<case>/` is one multi-file project: all its files are linted
together, module names relative to the subdirectory (`from readers import f` resolves to
`<case>/readers.py`):

- a line ending in `# expect: CODE` (comma-separated list allowed) must produce that
  diagnostic, reported on that line (the diagnostic's start line);
- no other line may produce it;
- if `<name>.py.fixed` exists, applying the *safe* fixes to `<name>.py` must give exactly
  that text;
- every implemented rule needs a fixtures directory with at least one positive expectation
  and at least one file without expectations (a pure negative).
- preset pass: every fixture is also linted with `ALL` and with the default preset; a
  rule's findings in its own directory must be the same as when it runs alone, except
  findings dropped because a higher-precedence rule (`overlap::PRECEDENCE`) fired at
  the same site. In `<CODE>/overlap.py` every `<CODE>` finding must be dropped that way
  under `ALL`, and under the default preset whenever it enables the rule that beats it,
  and each precedence pair must be exercised by some fixture.

Add a rule: implement its hook function(s) in `crates/sparklint_linter/src/rules/<family>.rs`,
add one `Rule::new("CODE").call(..)` line to that module's `RULES` (the rule table, see
DESIGN §3.4), and add `tests/fixtures/<CODE>/`. Nothing else lists rule codes: the
registry's `implemented` flag, the enabled check and the dispatch come from the table.

A rule about the value a Spark config key is set to (most `CFG` rules) is a row, not a
hook: add `Rule::new("CODE").conf(&Conf::new(&["spark.sql.someKey", ..], verdict))` to
`rules/conf.rs` `RULES` (keys in their usual casing; add `.table(&["delta.someProperty"],
table_verdict)` if the same setting can be a `TBLPROPERTIES` entry) and write
`fn verdict(s: &Setting) -> Option<Finding>` (`s.key` is the display name, `s.value` the
constant value text, `s.via` how it was written, `s.settings` the linter settings). Do
not call `writes()` or write a casing helper / finish hook: matching, the "set back later
in this file" downgrade to info and reporting are the module's. Add table tests on the
verdict in `conf.rs` (its `verdict()` / `sev()` / `table()` helpers look the row up by code)
plus the fixture directory.

Rule metadata lives in `docs/RULES_CATALOG.md`; regenerate `spec/rules.json` with
`python3 scripts/gen_rules.py > spec/rules.json`.

## CLI

```bash
sparklint check [PATHS...] [--select S,..] [--extend-select S,..] [--ignore S,..]
                [--output-format text|json|github|sarif] [--fix] [--unsafe-fixes]
                [--statistics] [--config FILE] [--exit-zero] [-q]
sparklint check - [--stdin-filename PATH] < file.py   # one file from stdin; with --fix the
                                                      # fixed source goes to stdout
sparklint rule ACT002            # explain a rule (code or kebab name)
sparklint rules [--format json] [--implemented]
sparklint version
```

Exit codes: 0 = no findings, 1 = findings, 2 = error (bad config/selector, unreadable or
missing path). Discovery honours `.gitignore` (also outside git repos), the configured
`exclude`/`extend-exclude`, and the default excludes (`.venv`, `build`, `__pycache__` …);
files named explicitly on the command line are always linted. Linting is two-phase and
parallel (rayon): phase A builds the cross-file function-summary index, phase B lints.

Selectors: codes (`ACT002`), prefixes (`ACT`, `ACT0`), kebab names
(`collect-unbounded`), `tag:<name>`, `ALL`, presets `default`, `review`, `strict`,
`cylint-compat`, `serverless`. The most specific selector wins; per-file ignores always win.

Configuration: `[tool.sparklint]` in `pyproject.toml`, or `sparklint.toml` /
`.sparklint.toml` (closest ancestor directory wins; in one directory `sparklint.toml` wins).

```toml
[tool.sparklint]
select = ["default"]            # or preset = "review"
extend-select = ["ACT001"]
ignore = ["STY"]
exclude = ["build", "generated/**"]   # replaces the default excludes; use extend-exclude to add
known-spark-globals = ["ss", "ctx:SparkContext"]
notebook = "auto"               # auto | on | off: Databricks notebook globals (spark, sc, display, dbutils); "on" also parses .py files with notebook magics (%pip, !cmd)
heuristics = true               # name heuristics (df, *_df, ...) at Likely confidence
spark-version = "3.5"           # gates fixes (isEmpty() needs >= 3.3)
collect-limit = 1000            # N in the ACT002 unsafe fix .limit(N).collect()
cube-min-columns = 5            # PRF024: cube() with >= N columns (2^N grouping sets); 0 = off
rollup-min-columns = 8          # PRF024: rollup() with >= N columns (N+1 sets); 0 = off
pipeline-fanout-min-flows = 3   # DBX017: source streamed by >= N pipeline datasets; 0 = off
distinct-aggregates-min = 3     # PRF026: >= N distinct aggregates over different columns in one aggregation; 0 = off
zorder-max-columns = 3          # IOD020: ZORDER BY with more than N columns; 0 = off
broadcast-threshold-max = "1g"  # CFG007: autoBroadcastJoinThreshold at or above this size; "0" = off
functions-aliases = ["F", "sf"]

[tool.sparklint.severity]
ACT = "error"

[tool.sparklint.per-file-ignores]
"tests/**" = ["ACT"]
```

Suppressions: `# noqa`, `# noqa: ACT002,PRF001`, `# sparklint: ignore[ACT002]`,
`# sparklint: file-ignore[ACT002]`, and cylint's `# cy:ignore CY001` (mapped via
RULES_CATALOG §4).

## Documentation site

The site at https://clusterbite.github.io/sparklint/ is MkDocs Material. Hand-written
pages are in `docs/site/`; `scripts/gen_docs.py` generates one page per rule from
`spec/rules.json`, the cylint mapping from `docs/RULES_CATALOG.md` §4, and the changelog
(generated files are git-ignored). To change a rule's page, edit its catalog entry and
regenerate `spec/rules.json`.

```bash
pip install -r docs/requirements.txt
python3 scripts/gen_docs.py && mkdocs serve      # http://127.0.0.1:8000
```

`.github/workflows/docs.yml` builds it with `--strict` on pull requests and deploys it to
GitHub Pages on every push to `main`.

## Packaging (maturin, like ruff)

Root `pyproject.toml`: `bindings = "bin"`, `manifest-path = "crates/sparklint/Cargo.toml"`,
`python-source = "python"`. The wheel ships the native `sparklint` binary in the scripts
directory plus the `sparklint` Python package (`find_sparklint_bin()`, `python -m sparklint`).
The source distribution carries the crates and `spec/*.json`, so `pip install` can build
it where no wheel exists.

```bash
maturin build --release -o target/wheels      # about a minute (fat LTO)
uv venv /tmp/slv && uv pip install --python /tmp/slv/bin/python target/wheels/sparklint-*.whl
/tmp/slv/bin/sparklint check path/
/tmp/slv/bin/python -m sparklint --version
```

## Comparing with cylint

Install cylint into a scratch venv and compare `cy lint -f json` with `sparklint check
--select ALL --ignore ACT001 --output-format json`, matching findings on (file, line, mapped
code via `noqa::CYLINT_MAP`).
