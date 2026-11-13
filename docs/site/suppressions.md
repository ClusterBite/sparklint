---
title: Suppressions
description: Silence a sparklint finding on one line or in a whole file, with noqa, sparklint ignore comments or cylint's cy:ignore.
---

# Suppressions

Turn a rule off for one line:

```python
rows = small_lookup.collect()  # noqa: ACT002
rows = small_lookup.collect()  # sparklint: ignore[ACT002]
```

or for a whole file, anywhere in it:

```python
# sparklint: file-ignore[ACT002, PRF008]
```

- `# noqa` and `# sparklint: ignore` without codes silence every rule on the line;
  `# sparklint: file-ignore` without codes silences the file.
- Codes can be prefixes: `# noqa: ACT` silences every ACT rule on the line.
- A finding that spans several lines is silenced by a comment on its first or its last
  line.
- Keywords are case-insensitive.

## cylint comments

cylint's comments keep working: `# cy:ignore` silences the line and
`# cy:ignore CY001,CY012` silences the sparklint rules those codes map to (see
[Migrating from cylint](cylint.md#mapping)).

## Rather than a comment

For whole directories (tests, scratch notebooks), use
[`per-file-ignores`](configuration.md#per-file-ignores) instead of comments in every
file. To drop a rule everywhere, add it to `ignore`.
