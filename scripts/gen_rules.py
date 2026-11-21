#!/usr/bin/env python3
"""Generate spec/rules.json (rule metadata + docs) from docs/RULES_CATALOG.md and the
rule table's message variants.

Usage: python3 scripts/gen_rules.py > spec/rules.json

The Rust registry (crates/sparklint_linter/src/registry.rs) embeds this file with
include_str!; which rules are *implemented* is decided in Rust, not here.

An implemented rule's messages (id, message template, help) come from its `Msg`
declarations in Rust, dumped to spec/rule_messages.json by a test
(`UPDATE_SPEC=1 cargo test -p sparklint_linter --lib spec_rule_messages`); its catalog
entry keeps only prose and must not repeat them (`- **Msg:**` / `- **Help:**`). A
planned rule's message is read from its catalog `- **Msg:**` item.
"""
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
CATALOG = ROOT / "docs" / "RULES_CATALOG.md"
MESSAGES = [
    ROOT / "spec" / "rule_messages.json",
]

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


def msg_item(body: str):
    """The text of a catalog entry's `- **Msg:**` item (continuation lines included),
    or None when the entry has none."""
    lines = body.splitlines()
    for i, line in enumerate(lines):
        m = re.match(r"^- \*\*Msg:\*\*\s?(.*)$", line)
        if not m:
            continue
        out = [m.group(1).rstrip()]
        for cont in lines[i + 1:]:
            if cont.startswith("- **") or not cont.strip():
                break
            out.append(cont[2:].rstrip() if cont.startswith("  ") else cont.rstrip())
        return "\n".join(out).strip()
    return None


def load_messages():
    """{code: [{"id", "message", "help"}]} from the rule table dumps."""
    out = {}
    for path in MESSAGES:
        for code, variants in json.loads(path.read_text(encoding="utf-8")).items():
            out[code] = [{"id": v["id"], "message": v["message"], "help": v["help"]} for v in variants]
    return out


def main() -> None:
    text = CATALOG.read_text(encoding="utf-8")
    declared = load_messages()
    errors = []
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
            "messages": [],
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
        if code in declared:
            # the rule table owns the wording; the catalog must not repeat it
            for label in re.findall(r"^- \*\*(Msg[^*]*|Help[^*]*):\*\*", body, flags=re.M):
                errors.append(f"{code}: remove the catalog `- **{label}:**` item (the rule's Msg declarations own its text)")
            rules[code]["messages"] = declared[code]
            rules[code]["message"] = declared[code][0]["message"]
            continue
        # a message holding backticks is written as a ``double-backtick`` code span
        m = re.search(r"\*\*Msg:\*\*\s*(?:``\s?(.+?)\s?``|`([^`]+)`)", body)
        item = msg_item(body)
        if m and item is not None and item.startswith(m.group(0)[len("**Msg:**"):].lstrip()):
            rules[code]["message"] = m.group(1) or m.group(2)
        elif item:
            # several variants (`- **Msg:** one per case:` + a bullet list, or prose
            # around the code spans): the whole item, sub-bullets dedented
            rules[code]["message"] = item
        else:
            # short entries (STY005+): "style · info · T1 · none · palantir — text"
            first = body.splitlines()[0] if body else ""
            if "—" in first:
                rules[code]["message"] = first.split("—", 1)[1].strip()
    errors += [f"{code}: has message variants but no catalog rule" for code in sorted(set(declared) - set(rules))]
    if errors:
        sys.exit("gen_rules.py:\n  " + "\n  ".join(errors))
    out = {"rules": [rules[k] for k in sorted(rules)]}
    json.dump(out, sys.stdout, indent=1, ensure_ascii=False)
    sys.stdout.write("\n")
    print(f"{len(rules)} rules", file=sys.stderr)


if __name__ == "__main__":
    main()
