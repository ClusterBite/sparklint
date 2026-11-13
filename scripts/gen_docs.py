#!/usr/bin/env python3
"""Generate the documentation site's rule pages, cylint mapping and changelog.

Usage: python3 scripts/gen_docs.py [--out docs/site]

Reads spec/rules.json (one entry per rule this build implements), docs/RULES_CATALOG.md
(the cylint mapping table) and CHANGELOG.md, and writes into the MkDocs source dir:

  rules/index.md        every rule, grouped by family
  rules/<CODE>.md       one page per rule
  cylint.md             migrating from cylint (mapping table)
  changelog.md          a copy of CHANGELOG.md

The generated files are not committed (see .gitignore); the docs workflow runs this
before `mkdocs build`. Stdlib only.
"""
import argparse
import json
import pathlib
import re
import shutil

ROOT = pathlib.Path(__file__).resolve().parent.parent

FAMILY_ORDER = ["ACT", "PRF", "UDF", "IOD", "CFG", "STM", "COR", "DBX", "PDS", "DEP", "STY", "SYN"]
FAMILY_INTRO = {
    "ACT": "Spark actions and work on the driver: unbounded collects, actions in loops or only for logging.",
    "PRF": "Query plans and shuffles: loops that grow plans, joins, windows, caching, partitioning.",
    "UDF": "Python and pandas UDFs: cost and correctness.",
    "IOD": "Reads, writes, JDBC, Delta MERGE / OPTIMIZE / VACUUM, partitioning, overwrites.",
    "CFG": "Spark configuration values and how they are set.",
    "STM": "Structured Streaming: checkpoints, triggers, watermarks, foreachBatch.",
    "COR": "Code that silently gives wrong results or fails at runtime.",
    "DBX": "Databricks platform: Unity Catalog, standard access mode, serverless, Spark Connect, notebooks.",
    "PDS": "pandas-on-Spark (`pyspark.pandas`) traps.",
    "DEP": "APIs deprecated or removed in recent Spark versions.",
    "STY": "Style and naming. All off in the `default` preset.",
    "SYN": "Files that do not parse.",
}
TYPE_INFO = {
    "T0": "syntax and imports",
    "T1": "the type of the receiver",
    "T2": "dataflow inside a function",
    "T3": "cross-file analysis",
}
FIX_TEXT = {
    "none": "none (the message suggests what to do)",
    "safe": "safe, applied with `--fix`",
    "unsafe": "unsafe, applied with `--unsafe-fixes`",
    "safe/unsafe": "safe for some cases (`--fix`), unsafe for others (`--unsafe-fixes`)",
}
# Bullet labels in a rule's catalog entry -> section heading (None = not shown).
SECTIONS = [
    ("Why", "Why it matters"),
    ("Detect", "When it is reported"),
    ("Implemented", "Current behaviour"),
    ("FP", "False positives and limits"),
    ("Severity", "Severity"),
    ("Config", "Configuration"),
    ("Overlap", "Overlap with other rules"),
    ("Related", "Related"),
    ("Fix", "Fix"),
    ("Ex", "Example"),
]

SYN001 = {
    "code": "SYN001",
    "name": "syntax-error",
    "family": "Syntax",
    "severity": "error",
    "default_on": True,
    "cylint_compat_on": True,
    "type_info": "T0",
    "fix": "none",
    "tags": ["correctness"],
    "message": "The file could not be parsed; no rules were run on it.",
    "docs": (
        "syntax · error · on · T0 · none\n"
        "- **Why:** Rules need a syntax tree. A file the Python parser cannot read gets no other findings, "
        "so one syntax error can hide everything else in it.\n"
        "- **Detect:** The parser reports a syntax error. In notebooks (Databricks source notebooks and "
        "`.ipynb`) only the broken cell is skipped: the other cells are still linted, and `--fix` still "
        "fixes them. Notebook magics (`%pip`, `!cmd`, `x = !cmd`) are not syntax errors; in a plain `.py` "
        "file that uses them, set `notebook = \"on\"`."
    ),
}


def esc_cell(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ")


def parse_entry(docs: str):
    """Split a catalog entry into (header parts, [(label, text)])."""
    lines = docs.splitlines()
    header = [p.strip() for p in lines[0].split(" · ")] if lines and not lines[0].startswith("- **") else []
    body = lines[1:] if header else lines
    items = []
    for line in body:
        m = re.match(r"^- \*\*([^*]+?):\*\*\s?(.*)$", line)
        if m:
            items.append([m.group(1).strip(), m.group(2)])
        elif items:
            items[-1][1] += "\n" + line
    return header, items


def nest(text: str) -> str:
    """Catalog sub-bullets use 2-space indents; Python-Markdown needs 4."""
    out = []
    for line in text.strip("\n").splitlines():
        m = re.match(r"^( +)(- .*)$", line)
        if m:
            depth = max(1, len(m.group(1)) // 2)
            out.append("    " * (depth - 1) + m.group(2))
        else:
            out.append(line.strip())
    text = "\n".join(out).strip()
    # A label followed directly by a list ("**FP:**\n  - a\n  - b") starts with the list.
    return text


def code_span(s: str):
    """`x` -> x when s is exactly one backtick span, else None."""
    s = s.strip()
    m = re.fullmatch(r"(`+)(.+?)\1", s)
    return m.group(2) if m and "`" not in m.group(2) else None


def render_example(text: str) -> str:
    text = text.strip()
    if "\n" in text or " → " not in text:
        return nest(text)
    before, after = text.split(" → ", 1)
    b = code_span(before)
    if b is None:
        return text
    out = ["Before:", "", "```python", b, "```", ""]
    a = code_span(after)
    if a is not None:
        out += ["After:", "", "```python", a, "```"]
    else:
        m = re.match(r"^(`+)(.+?)\1(.*)$", after.strip())
        if m and "`" not in m.group(2) and m.group(3).strip():
            out += ["After:", "", "```python", m.group(2), "```", "", m.group(3).strip().lstrip(",;").strip()]
        else:
            out += ["After: " + after.strip()]
    return "\n".join(out)


def section_of(label: str):
    for key, title in SECTIONS:
        if label == key or label.startswith(key + " ") or label.startswith(key + "("):
            return key, title
    return None, label


def presets_of(r) -> str:
    on = []
    if r["default_on"]:
        on.append("`default`")
    if r["code"][:3] != "STY":
        on.append("`review`")
    on.append("`strict`")
    if r["cylint_compat_on"]:
        on.append("`cylint-compat`")
    if r["default_on"] or r["code"][:3] == "DBX":
        on.append("`serverless`")
    return ", ".join(on)


def rule_page(r, cy_for) -> str:
    header, items = parse_entry(r["docs"])
    sev = header[1] if len(header) > 1 else r["severity"]
    title = f"{r['code']}: {r['name']}"
    out = [
        "---",
        f"title: \"{title}\"",
        f"description: \"sparklint rule {r['code']} ({r['name']}): {esc_cell(r['message'])[:150].replace(chr(34), chr(39))}\"",
        "---",
        "",
        f"# {r['code']} `{r['name']}`",
        "",
        f"- **Family:** [{r['family']}](index.md#{r['code'][:3].lower()})",
        f"- **Severity:** {sev}",
        f"- **On in the default preset:** {'yes' if r['default_on'] else 'no'} (presets: {presets_of(r)})",
        f"- **Autofix:** {FIX_TEXT.get(r['fix'], r['fix'])}",
        f"- **Needs:** {TYPE_INFO.get(r['type_info'][:2], r['type_info'])}",
    ]
    if r["tags"]:
        out.append("- **Tags:** " + ", ".join(f"`{t}`" for t in r["tags"]))
    if cy_for.get(r["code"]):
        out.append("- **cylint:** " + ", ".join(f"`{c}`" for c in cy_for[r["code"]]) + " ([mapping](../cylint.md))")
    out += ["", "## Message", "", "```text", r["message"] or "(see below)", "```", ""]

    order = {key: i for i, (key, _) in enumerate(SECTIONS)}
    groups = {}
    for label, text in items:
        if label == "Msg":
            continue
        key, title_ = section_of(label)
        if key is None:
            key = label
        groups.setdefault(key, (title_, []))[1].append((label, text))
    for key in sorted(groups, key=lambda k: order.get(k, len(order))):
        title_, parts = groups[key]
        out += [f"## {title_}", ""]
        for label, text in parts:
            body = render_example(text) if key == "Ex" else nest(text)
            if key == "Fix" and label != "Fix":
                kind = label[len("Fix"):].strip().strip("()")
                out.append(f"**{kind[:1].upper() + kind[1:]}:**" + ("\n\n" if body.startswith("- ") else " ") + body)
            else:
                out.append(body)
            out.append("")
    out += [
        "## Suppressing it",
        "",
        f"Add `# noqa: {r['code']}` (or `# sparklint: ignore[{r['code']}]`) at the end of the reported "
        f"line, `# sparklint: file-ignore[{r['code']}]` for a whole file, or `ignore = [\"{r['code']}\"]` "
        "in the configuration. See [Suppressions](../suppressions.md).",
        "",
    ]
    return "\n".join(out)


def cylint_table(catalog: str):
    sec = catalog.split("## 4. cylint", 1)
    if len(sec) < 2:
        return [], ""
    sec = sec[1].split("\n## ", 1)[0]
    intro = []
    rows = []
    for line in sec.splitlines()[1:]:
        if line.startswith("| CY"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            rows.append(cells)
        elif not line.startswith("|") and line.strip() and line.strip() != "---":
            intro.append(line.strip())
    return rows, " ".join(intro)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate the sparklint docs site's rule pages.")
    ap.add_argument("--out", type=pathlib.Path, default=ROOT / "docs" / "site")
    args = ap.parse_args()
    out = args.out

    rules = json.loads((ROOT / "spec" / "rules.json").read_text(encoding="utf-8"))["rules"]
    rules = [r for r in rules if r["code"] != "SYN001"] + [SYN001]
    catalog = (ROOT / "docs" / "RULES_CATALOG.md").read_text(encoding="utf-8")
    cy_rows, cy_intro = cylint_table(catalog)
    codes = {r["code"] for r in rules}
    cy_for = {}
    for cells in cy_rows:
        for code in re.findall(r"[A-Z]{3}\d{3}", cells[2]):
            cy_for.setdefault(code, []).append(cells[0])

    rules_dir = out / "rules"
    if rules_dir.exists():
        shutil.rmtree(rules_dir)
    rules_dir.mkdir(parents=True)
    for r in rules:
        (rules_dir / f"{r['code']}.md").write_text(rule_page(r, cy_for), encoding="utf-8")

    # index
    by_fam = {}
    for r in rules:
        by_fam.setdefault(r["code"][:3], []).append(r)
    n_default = sum(r["default_on"] for r in rules)
    idx = [
        "---",
        "title: Rules",
        "description: Every sparklint rule for PySpark and Databricks code, grouped by family.",
        "hide:",
        "  - navigation",
        "---",
        "",
        "# Rules",
        "",
        f"sparklint has **{len(rules)} rules** in {len(by_fam)} families; **{n_default}** are on in the "
        "`default` preset. Codes are a three-letter family plus three digits and are never reused. "
        "`sparklint rules` lists the rules of your installed version and `sparklint rule <CODE>` prints "
        "one of them.",
        "",
        "Legend: **Default** = on in the `default` preset. **Fix** = `safe` (applied with `--fix`), "
        "`unsafe` (applied with `--unsafe-fixes`) or empty. Select rules by code, family, name, "
        "`tag:<name>` or preset; see [Configuration](../configuration.md#selecting-rules).",
        "",
    ]
    tags = {}
    for r in rules:
        for t in r["tags"]:
            tags.setdefault(t, []).append(r["code"])
    fams = [f for f in FAMILY_ORDER if f in by_fam] + sorted(f for f in by_fam if f not in FAMILY_ORDER)
    for fam in fams:
        rs = sorted(by_fam[fam], key=lambda r: r["code"])
        idx += [f"## {fam}: {rs[0]['family']} {{ #{fam.lower()} }}", ""]
        if fam in FAMILY_INTRO:
            idx += [FAMILY_INTRO[fam], ""]
        idx += ["| Code | Name | Message | Severity | Default | Fix |", "|---|---|---|---|---|---|"]
        for r in rs:
            fix = "" if r["fix"] == "none" else r["fix"]
            idx.append(
                f"| [{r['code']}]({r['code']}.md) | {r['name']} | {esc_cell(r['message'])} | "
                f"{r['severity']} | {'yes' if r['default_on'] else ''} | {fix} |"
            )
        idx.append("")
    idx += ["## By tag", "", "Select a tag with `--select tag:<name>`.", ""]
    for t in sorted(tags, key=lambda t: (-len(tags[t]), t)):
        links = ", ".join(f"[{c}]({c}.md)" for c in sorted(tags[t]))
        idx += [f"- `{t}` ({len(tags[t])}): {links}"]
    idx.append("")
    (rules_dir / "index.md").write_text("\n".join(idx), encoding="utf-8")

    # cylint migration
    cy = [
        "---",
        "title: Migrating from cylint",
        "description: How cylint's CY rules map to sparklint rule codes, and how to switch.",
        "---",
        "",
        "# Migrating from cylint",
        "",
        "sparklint covers every rule cylint implements, "
        "under new codes. The main difference is that sparklint knows which values are Spark "
        "objects, so `count()` on a list or a pandas DataFrame is not reported, and DataFrames passed "
        "into functions or returned from helpers are still recognised.",
        "",
        "## Switching",
        "",
        "1. `pip install sparklint`",
        "2. To start with the same rule set as cylint's defaults, use the `cylint-compat` preset:",
        "",
        "    ```toml",
        "    [tool.sparklint]",
        "    preset = \"cylint-compat\"",
        "    ```",
        "",
        "3. Keep your suppressions: `# cy:ignore CY001` comments are honoured and mapped to the "
        "sparklint codes below. New suppressions use `# noqa: ACT002`.",
        "4. Later, move to the `default` preset (the rules marked as on in the [rule list](rules/index.md)).",
        "",
        "## Mapping",
        "",
    ]
    if cy_intro:
        cy += [cy_intro, ""]
    cy += ["| cylint | cylint name | sparklint | What changes |", "|---|---|---|---|"]
    for cells in cy_rows:
        sp = re.sub(
            r"[A-Z]{3}\d{3}",
            lambda m: f"[{m.group(0)}](rules/{m.group(0)}.md)" if m.group(0) in codes else m.group(0),
            cells[2],
        )
        cy.append(f"| {cells[0]} | {cells[1]} | {sp} | {cells[4] if len(cells) > 4 else ''} |")
    cy.append("")
    (out / "cylint.md").write_text("\n".join(cy), encoding="utf-8")

    # changelog
    ch = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    (out / "changelog.md").write_text(
        "---\ntitle: Changelog\ndescription: Release notes for sparklint.\n---\n\n" + ch, encoding="utf-8"
    )
    print(f"wrote {len(rules)} rule pages, rules/index.md, cylint.md, changelog.md to {out}")


if __name__ == "__main__":
    main()
