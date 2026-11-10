//! Cross-file project index of function return types (DESIGN §3.2 item 7).
//!
//! Phase A builds a [`crate::SemanticModel`] for every file (in parallel, by the
//! caller) and collects [`crate::SemanticModel::exported_summaries`]; phase B lints
//! each file with the index as its [`SummaryProvider`]. Summaries are keyed by every
//! dotted suffix of the module name (`src.pkg.mod.f`, `pkg.mod.f`, `mod.f`), so
//! `from pkg.mod import f` resolves regardless of the source root. Conflicting
//! entries for the same key are dropped (ambiguous).

use std::path::{Component, Path};

use rustc_hash::FxHashMap;

use crate::model::SummaryProvider;
use crate::types::TypeInfo;

#[derive(Debug, Default, Clone)]
pub struct ProjectIndex {
    entries: FxHashMap<String, Option<TypeInfo>>,
}

impl ProjectIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add the exported summaries of one module.
    pub fn add_module(&mut self, module_name: &str, summaries: &[(String, TypeInfo)]) {
        let parts: Vec<&str> = module_name.split('.').filter(|s| !s.is_empty()).collect();
        for (path, ty) in summaries {
            for start in 0..parts.len() {
                let key = format!("{}.{}", parts[start..].join("."), path);
                self.insert(key, *ty);
            }
            if parts.is_empty() {
                self.insert(path.clone(), *ty);
            }
        }
    }

    fn insert(&mut self, key: String, ty: TypeInfo) {
        match self.entries.get_mut(&key) {
            None => {
                self.entries.insert(key, Some(ty));
            }
            Some(Some(prev)) if prev.ty == ty.ty => {
                *prev = prev.join(ty);
            }
            Some(slot) => *slot = None,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl SummaryProvider for ProjectIndex {
    fn function_return(&self, qualified: &str) -> Option<TypeInfo> {
        self.entries.get(qualified).copied().flatten()
    }
}

/// Dotted module name of `path` relative to `root` (`root/pkg/mod.py` → `pkg.mod`,
/// `pkg/__init__.py` → `pkg`).
pub fn module_name_for_path(root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    let mut parts: Vec<String> = rel
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect();
    if let Some(last) = parts.last_mut() {
        for ext in [".py", ".ipynb"] {
            if let Some(stem) = last.strip_suffix(ext) {
                *last = stem.to_string();
                break;
            }
        }
    }
    if parts.last().is_some_and(|l| l == "__init__") {
        parts.pop();
    }
    parts
        .into_iter()
        .map(|p| p.replace(['-', ' '], "_"))
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Ty;

    #[test]
    fn suffix_keys_and_conflicts() {
        let mut idx = ProjectIndex::new();
        idx.add_module(
            "src.pkg.io",
            &[("load".into(), TypeInfo::certain(Ty::DataFrame))],
        );
        idx.add_module("other.io", &[("load".into(), TypeInfo::certain(Ty::Str))]);
        assert_eq!(
            idx.function_return("pkg.io.load").unwrap().ty,
            Ty::DataFrame
        );
        assert_eq!(
            idx.function_return("src.pkg.io.load").unwrap().ty,
            Ty::DataFrame
        );
        // "io.load" is ambiguous
        assert!(idx.function_return("io.load").is_none());
    }

    #[test]
    fn module_names() {
        let root = Path::new("/r");
        assert_eq!(
            module_name_for_path(root, Path::new("/r/pkg/mod.py")),
            "pkg.mod"
        );
        assert_eq!(
            module_name_for_path(root, Path::new("/r/pkg/__init__.py")),
            "pkg"
        );
        assert_eq!(
            module_name_for_path(root, Path::new("/r/my-nb.ipynb")),
            "my_nb"
        );
    }
}
