#!/usr/bin/env python3
"""Generate spec/rules.json (rule metadata + docs) from docs/RULES_CATALOG.md.

Usage: python3 scripts/gen_rules.py > spec/rules.json

The Rust registry (crates/sparklint_linter/src/registry.rs) embeds this file with
include_str!; which rules are *implemented* is decided in Rust, not here.
"""
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
CATALOG = ROOT / "docs" / "RULES_CATALOG.md"

SEV = {"E": "error", "W": "warning", "I": "info"}
FAMILIES = {
    "ACT": "Actions & driver",
    "PRF": "Plan & shuffle performance",
    "UDF": "User-defined functions",
    "IOD": "I/O & Delta",
    "CFG": "Spark config",
    "STM": "Structured Streaming",
    "COR": "Correctness",
    "DBX": "Databricks platform & compatibility",
    "PDS": "pandas-on-Spark",
    "DEP": "Deprecated / removed APIs",
    "STY": "Style & naming",
}


def main() -> None:
    text = CATALOG.read_text(encoding="utf-8")
    rules = {}
    # Section 3 index table rows: | ACT001 | spark-action | I | off | T1 | none | cost |
    row = re.compile(
        r"^\|\s*([A-Z]{3}\d{3})\s*\|\s*([a-z0-9-]+)\s*\|\s*([EWI])\s*\|\s*([^|]+?)\s*\|"
        r"\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]*?)\s*\|"
        r"\s*$"
    )
    for line in text.splitlines():
        m = row.match(line)
        if not m:
            continue
        code, name, sev, on, ti, fix, tags = (m.group(i) for i in range(1, 8))
        on = on.strip()
        rules[code] = {
            "code": code,
            "name": name,
            "family": FAMILIES.get(code[:3], code[:3]),
            "severity": SEV[sev],
            "default_on": on == "on",
            "cylint_compat_on": on in ("on", "on*"),
            "type_info": ti.strip(),
            "fix": fix.strip(),
            "tags": [t.strip() for t in tags.split(",") if t.strip()],
            "message": "",
            "docs": "",
        }
    # Section 5 entries: "#### CODE `name`" until the next heading.
    sec5 = text.split("## 5. Rule details", 1)[1].split("\n## 6.", 1)[0]
    parts = re.split(r"^#### ([A-Z]{3}\d{3}) `[^`]+`\s*$", sec5, flags=re.M)
    for i in range(1, len(parts), 2):
        code, body = parts[i], parts[i + 1]
        body = re.split(r"^### ", body, flags=re.M)[0].strip()
        if code not in rules:
            continue
        rules[code]["docs"] = body
        m = re.search(r"\*\*Msg:\*\*\s*`([^`]+)`", body)
        if m:
            rules[code]["message"] = m.group(1)
        else:
            # short entries (STY005+): "style · info · T1 · none · palantir — text"
            first = body.splitlines()[0] if body else ""
            if "—" in first:
                rules[code]["message"] = first.split("—", 1)[1].strip()
    out = {"rules": [rules[k] for k in sorted(rules)]}
    json.dump(out, sys.stdout, indent=1, ensure_ascii=False)
    sys.stdout.write("\n")
    print(f"{len(rules)} rules", file=sys.stderr)


if __name__ == "__main__":
    main()
