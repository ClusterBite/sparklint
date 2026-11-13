---
title: CI and editors
description: Run sparklint in GitHub Actions, GitHub code scanning (SARIF) and pre-commit, and from your editor.
---

# CI and editors

## GitHub Actions

```yaml title=".github/workflows/sparklint.yml"
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

`--output-format github` turns findings into annotations on the pull request's changed
lines.

## GitHub code scanning (SARIF)

Write SARIF and upload it, so findings show up under the repository's *Security → Code
scanning* tab and on pull requests:

```yaml
      - run: sparklint check --output-format sarif --exit-zero . > sparklint.sarif
      - uses: github/codeql-action/upload-sarif@v4
        with:
          sarif_file: sparklint.sarif
```

Other tools that read SARIF 2.1.0 work the same way.

## pre-commit

With the published wheel (fast, no Rust toolchain needed):

```yaml title=".pre-commit-config.yaml"
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

The repository also has a `.pre-commit-hooks.yaml`, so
`repo: https://github.com/clusterbite/sparklint` with `rev: v0.2.0` and `id: sparklint`
works too, but pre-commit then builds sparklint from source, which needs a Rust toolchain
and takes a few minutes the first time.

## Adopting it on an existing codebase

1. Run `sparklint check --statistics` to see which rules fire most.
2. Turn off what you will not fix now with `ignore`, or silence directories with
   [`per-file-ignores`](configuration.md#per-file-ignores) (tests, scratch notebooks).
3. Make CI fail on new findings only in the code you touch, then remove ignores one
   rule at a time.

## Editors

There is no language server yet. Run `sparklint check` from your editor's task runner,
pipe the open buffer through `sparklint check - --stdin-filename <path>`, or feed the
`json` / `sarif` output to a plugin that reads them.
