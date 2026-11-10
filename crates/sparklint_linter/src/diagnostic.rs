//! Diagnostics and fixes.

use ruff_text_size::{TextRange, TextSize};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Error => "error",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "info" | "i" | "note" => Some(Severity::Info),
            "warning" | "warn" | "w" => Some(Severity::Warning),
            "error" | "e" => Some(Severity::Error),
            _ => None,
        }
    }
}

/// Whether a fix may be applied with `--fix` (safe) or only `--unsafe-fixes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Applicability {
    Unsafe,
    Safe,
}

/// A text replacement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Edit {
    pub range: TextRange,
    pub content: String,
}

impl Edit {
    pub fn replace(range: TextRange, content: impl Into<String>) -> Self {
        Self {
            range,
            content: content.into(),
        }
    }

    pub fn insert(at: TextSize, content: impl Into<String>) -> Self {
        Self {
            range: TextRange::empty(at),
            content: content.into(),
        }
    }

    pub fn delete(range: TextRange) -> Self {
        Self {
            range,
            content: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fix {
    pub edits: Vec<Edit>,
    pub applicability: Applicability,
    /// Short description (`Replace with df.isEmpty()`).
    pub message: String,
}

impl Fix {
    pub fn safe(message: impl Into<String>, edits: Vec<Edit>) -> Self {
        Self {
            edits,
            applicability: Applicability::Safe,
            message: message.into(),
        }
    }

    pub fn unsafe_(message: impl Into<String>, edits: Vec<Edit>) -> Self {
        Self {
            edits,
            applicability: Applicability::Unsafe,
            message: message.into(),
        }
    }

    pub fn min_start(&self) -> TextSize {
        self.edits
            .iter()
            .map(|e| e.range.start())
            .min()
            .unwrap_or_default()
    }
}

/// One finding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Diagnostic {
    pub code: &'static str,
    pub message: String,
    pub range: TextRange,
    pub severity: Severity,
    pub fix: Option<Fix>,
    pub help: Option<String>,
    /// The site the finding is about, when it differs from `range` (for example the
    /// `.collect()` call a driver-iteration finding reports on). Only used to apply
    /// rule precedence (`crate::overlap`); never printed.
    pub site: Option<TextRange>,
    /// Further overlap sites in the same file (a finding about a loop covers every
    /// write in it). Never printed.
    pub also_sites: Vec<TextRange>,
}

impl Diagnostic {
    /// A diagnostic with the rule's default severity.
    pub fn new(code: &'static str, message: impl Into<String>, range: TextRange) -> Self {
        let severity = crate::registry::get(code)
            .map(|r| r.severity)
            .unwrap_or(Severity::Warning);
        Self {
            code,
            message: message.into(),
            range,
            severity,
            fix: None,
            help: None,
            site: None,
            also_sites: Vec::new(),
        }
    }

    /// Set the overlap site (see [`Diagnostic::site`]).
    pub fn with_site(mut self, site: TextRange) -> Self {
        self.site = Some(site);
        self
    }

    /// The overlap site: [`Diagnostic::site`] or the reported range.
    pub fn site_range(&self) -> TextRange {
        self.site.unwrap_or(self.range)
    }

    /// Every overlap site: [`Self::site_range`] plus [`Diagnostic::also_sites`].
    pub fn sites(&self) -> impl Iterator<Item = TextRange> + '_ {
        std::iter::once(self.site_range()).chain(self.also_sites.iter().copied())
    }

    pub fn with_severity(mut self, s: Severity) -> Self {
        self.severity = s;
        self
    }

    pub fn with_fix(mut self, f: Fix) -> Self {
        self.fix = Some(f);
        self
    }

    pub fn with_help(mut self, h: impl Into<String>) -> Self {
        self.help = Some(h.into());
        self
    }

    pub fn name(&self) -> &'static str {
        crate::registry::get(self.code)
            .map(|r| r.name.as_str())
            .unwrap_or("")
    }
}
