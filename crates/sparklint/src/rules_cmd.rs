//! `sparklint rule <CODE>` and `sparklint rules`.

use std::io::Write;

use anyhow::bail;
use sparklint_linter::registry::{self, FixAvailability, RuleMeta};

use crate::{EXIT_CLEAN, ListFormat};

fn fix_str(f: FixAvailability) -> &'static str {
    match f {
        FixAvailability::None => "none",
        FixAvailability::Safe => "safe",
        FixAvailability::Unsafe => "unsafe",
        FixAvailability::Sometimes => "sometimes",
    }
}

fn status(r: &RuleMeta) -> &'static str {
    if r.implemented {
        "implemented"
    } else {
        "planned"
    }
}

pub fn explain(code: &str) -> anyhow::Result<u8> {
    let Some(r) = registry::get(code) else {
        bail!("unknown rule `{code}` (see `sparklint rules`)");
    };
    let mut out = std::io::stdout().lock();
    writeln!(out, "# {} ({})\n", r.name, r.code)?;
    writeln!(out, "Family:        {}", r.family)?;
    writeln!(out, "Severity:      {}", r.severity.as_str())?;
    writeln!(
        out,
        "Default:       {}",
        if r.default_on { "on" } else { "off" }
    )?;
    writeln!(out, "Status:        {}", status(r))?;
    writeln!(out, "Fix:           {}", fix_str(r.fix))?;
    writeln!(out, "Type info:     {}", r.type_info)?;
    if !r.tags.is_empty() {
        writeln!(out, "Tags:          {}", r.tags.join(", "))?;
    }
    let cy: Vec<&str> = sparklint_linter::noqa::CYLINT_MAP
        .iter()
        .filter(|(_, new)| *new == r.code)
        .map(|(cy, _)| *cy)
        .collect();
    if !cy.is_empty() {
        writeln!(out, "cylint:        {}", cy.join(", "))?;
    }
    writeln!(out, "\n## Message\n\n{}\n", r.message)?;
    if !r.docs.is_empty() {
        writeln!(out, "## Details\n\n{}", r.docs.trim_end())?;
    }
    Ok(EXIT_CLEAN)
}

pub fn list(format: ListFormat, implemented_only: bool) -> anyhow::Result<u8> {
    let rules: Vec<&RuleMeta> = registry::rules()
        .iter()
        .filter(|r| !implemented_only || r.implemented)
        .collect();
    let mut out = std::io::stdout().lock();
    match format {
        ListFormat::Json => {
            serde_json::to_writer_pretty(&mut out, &rules)?;
            writeln!(out)?;
        }
        ListFormat::Text => {
            let w = rules.iter().map(|r| r.name.len()).max().unwrap_or(10);
            for r in rules {
                writeln!(
                    out,
                    "{}  {:<w$}  {:<7}  {:<3}  {:<11}  {}",
                    r.code,
                    r.name,
                    r.severity.as_str(),
                    if r.default_on { "on" } else { "off" },
                    status(r),
                    if r.fix == FixAvailability::None {
                        ""
                    } else {
                        "fix"
                    },
                )?;
            }
        }
    }
    Ok(EXIT_CLEAN)
}
