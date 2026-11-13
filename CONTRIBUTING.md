# Contributing to sparklint

Thanks for helping. Bug reports, false-positive reports, rule ideas and pull requests are
all welcome.

## Reporting a false positive or a missed finding

Open an issue with:

- the sparklint version (`sparklint --version`),
- a minimal snippet that reproduces it (a few lines of PySpark is ideal),
- the command you ran and the output you got / expected.

Please strip anything confidential (table names, paths, credentials) from the snippet.

## Development setup

You need Rust 1.97+ (edition 2024) and, for the wheel, [maturin](https://www.maturin.rs/).

```bash
cargo build                                            # debug binary: target/debug/sparklint
cargo test --workspace                                 # unit, fixture and CLI tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
maturin build --release -o target/wheels               # the wheel
```

CI runs the same commands; please run them before opening a pull request.

[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) describes the crate layout, the CLI, the
configuration and the fixture harness. [docs/DESIGN.md](docs/DESIGN.md) explains the
type oracle the rules rely on.

## Adding or changing a rule

1. Describe the rule in [docs/RULES_CATALOG.md](docs/RULES_CATALOG.md) (why it is slow or
   wrong, what is detected, false-positive risks, sources), then regenerate the metadata:
   `python3 scripts/gen_rules.py > spec/rules.json`.
2. Implement it as a hook in the matching module under `crates/sparklint_linter/src/rules/`
   and add one `Rule::new("CODE")...` line to that module's `RULES` table.
3. Add `tests/fixtures/<CODE>/positive.py` (each expected finding marked with
   `# expect: <CODE>` on the line where it starts) and `negative.py` (look-alikes that must
   not fire). Fixable rules also get `<name>.py.fixed` snapshots.

Rules should fire only when the receiver is known to be a Spark object; a false positive
is worse than a missed finding.

## Pull requests

- Keep changes focused; one rule or fix per pull request is easiest to review.
- Add or update tests for every behaviour change.
- Add a line under "Unreleased" in [CHANGELOG.md](CHANGELOG.md) for user-visible changes.

By contributing you agree that your contributions are licensed under the Apache License,
Version 2.0 (see [LICENSE](LICENSE)).
