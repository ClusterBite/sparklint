//! STY001 functions-alias.

use ruff_python_ast::Stmt;
use ruff_text_size::TextRange;
use sparklint_semantic::{BindingKind, MODULE_SCOPE};

use super::Rule;
use crate::checker::Checker;
use crate::diagnostic::{Diagnostic, Edit, Fix};

/// STY001 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[Rule::new("STY001").stmt(sty001_stmt)];

fn sty001_stmt(ch: &mut Checker, stmt: &Stmt) {
    if matches!(stmt, Stmt::Import(_) | Stmt::ImportFrom(_)) {
        functions_alias(ch, stmt);
    }
}

pub fn functions_alias(ch: &mut Checker, stmt: &Stmt) {
    // (alias node range, binding range, module kind, current alias, name range)
    let mut found: Vec<(
        TextRange,
        &'static str,
        Option<String>,
        TextRange,
        TextRange,
    )> = Vec::new();
    match stmt {
        Stmt::ImportFrom(i) if i.level == 0 => {
            let Some(m) = &i.module else { return };
            if m.as_str() != "pyspark.sql" {
                return;
            }
            for a in &i.names {
                let kind = match a.name.as_str() {
                    "functions" => "functions",
                    "types" => "types",
                    _ => continue,
                };
                let binding_range = a.asname.as_ref().map_or(a.range, |n| n.range);
                found.push((
                    a.range,
                    kind,
                    a.asname.as_ref().map(|n| n.as_str().to_string()),
                    binding_range,
                    a.name.range,
                ));
            }
        }
        Stmt::Import(i) => {
            for a in &i.names {
                let kind = match a.name.as_str() {
                    "pyspark.sql.functions" => "functions",
                    "pyspark.sql.types" => "types",
                    _ => continue,
                };
                // `import pyspark.sql.functions` (no alias) is used fully qualified: fine.
                let Some(asname) = &a.asname else { continue };
                found.push((
                    a.range,
                    kind,
                    Some(asname.as_str().to_string()),
                    asname.range,
                    a.name.range,
                ));
            }
        }
        _ => return,
    }
    for (alias_range, kind, current, binding_range, name_range) in found {
        let allowed = if kind == "functions" {
            &ch.settings.functions_aliases
        } else {
            &ch.settings.types_aliases
        };
        if current
            .as_ref()
            .is_some_and(|c| allowed.iter().any(|a| a == c))
        {
            continue;
        }
        let Some(preferred) = allowed.first().cloned() else {
            continue;
        };
        let shown = current.clone().unwrap_or_else(|| kind.to_string());
        let mut d = Diagnostic::new(
            "STY001",
            format!(
                "Import pyspark.sql.{kind} as {} (found `{shown}`).",
                allowed.join(" or ")
            ),
            alias_range,
        );
        if let Some(fix) = rename_fix(ch, binding_range, name_range, current.is_some(), &preferred)
        {
            d = d.with_fix(fix);
        }
        ch.report(d);
    }
}

fn rename_fix(
    ch: &Checker,
    binding_range: TextRange,
    name_range: TextRange,
    has_alias: bool,
    new: &str,
) -> Option<Fix> {
    let model = ch.model;
    // The new name must not already be bound in the module.
    if model
        .bindings()
        .iter()
        .any(|b| b.name == new && b.scope == MODULE_SCOPE)
    {
        return None;
    }
    let (id, _) = model
        .bindings()
        .iter()
        .enumerate()
        .find(|(_, b)| b.kind == BindingKind::Import && b.range == binding_range)?;
    let mut edits = vec![if has_alias {
        Edit::replace(binding_range, new)
    } else {
        Edit::insert(name_range.end(), format!(" as {new}"))
    }];
    for u in model.binding_uses(id as u32) {
        // Only rename uses that resolve to this import alone.
        if u.reaching.len() != 1 {
            return None;
        }
        edits.push(Edit::replace(u.range, new));
    }
    // Shadowing in nested scopes makes renaming unsafe.
    if model
        .bindings()
        .iter()
        .any(|b| b.name == new && b.scope != MODULE_SCOPE)
    {
        return None;
    }
    Some(Fix::safe(format!("Rename to `{new}`"), edits))
}
