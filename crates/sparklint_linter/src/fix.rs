//! Applying fixes to source text.

use ruff_text_size::{TextRange, TextSize};

use crate::diagnostic::{Applicability, Diagnostic};

/// Apply all applicable fixes that do not overlap; returns the new text and the
/// number of fixes applied (0 = unchanged).
pub fn apply_fixes(text: &str, diagnostics: &[Diagnostic], unsafe_fixes: bool) -> (String, usize) {
    let (out, applied, _) = apply_fixes_where(text, diagnostics, unsafe_fixes, &|_| true);
    (out, applied)
}

/// Offsets `(old, new)` at the start and end of every applied edit, in order.
pub type Markers = Vec<(TextSize, TextSize)>;

/// [`apply_fixes`] restricted to fixes whose every edit satisfies `allowed`; also
/// returns the edit markers (for writing notebook cells back).
pub fn apply_fixes_where(
    text: &str,
    diagnostics: &[Diagnostic],
    unsafe_fixes: bool,
    allowed: &dyn Fn(TextRange) -> bool,
) -> (String, usize, Markers) {
    let mut fixes: Vec<_> = diagnostics
        .iter()
        .filter_map(|d| d.fix.as_ref())
        .filter(|f| unsafe_fixes || f.applicability == Applicability::Safe)
        .filter(|f| !f.edits.is_empty())
        .filter(|f| f.edits.iter().all(|e| allowed(e.range)))
        .collect();
    fixes.sort_by_key(|f| (f.min_start(), f.edits.len()));
    let mut out = String::with_capacity(text.len());
    let mut last = TextSize::from(0);
    let mut applied = 0;
    let mut seen = Vec::new();
    let mut markers = Vec::new();
    for fix in fixes {
        if seen.contains(&fix) {
            continue;
        }
        let mut edits = fix.edits.clone();
        edits.sort_by_key(|e| (e.range.start(), e.range.end()));
        // Skip fixes overlapping an already-applied edit, or with self-overlapping edits.
        if edits[0].range.start() < last
            || edits
                .windows(2)
                .any(|w| w[1].range.start() < w[0].range.end())
            || edits
                .iter()
                .any(|e| usize::from(e.range.end()) > text.len())
        {
            continue;
        }
        for e in &edits {
            let start = usize::from(e.range.start());
            out.push_str(&text[usize::from(last)..start]);
            markers.push((e.range.start(), TextSize::of(out.as_str())));
            out.push_str(&e.content);
            markers.push((e.range.end(), TextSize::of(out.as_str())));
            last = e.range.end();
        }
        applied += 1;
        seen.push(fix);
    }
    out.push_str(&text[usize::from(last).min(text.len())..]);
    (out, applied, markers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::{Edit, Fix};
    use ruff_text_size::TextRange;

    fn d(fix: Fix) -> Diagnostic {
        Diagnostic::new("COR001", "m", TextRange::default()).with_fix(fix)
    }

    #[test]
    fn applies_non_overlapping_and_respects_safety() {
        let text = "abcdef";
        let r = |a: u32, b: u32| TextRange::new(a.into(), b.into());
        let diags = vec![
            d(Fix::safe("x", vec![Edit::replace(r(0, 1), "A")])),
            d(Fix::safe("y", vec![Edit::replace(r(0, 2), "ZZ")])), // overlaps
            d(Fix::unsafe_("z", vec![Edit::replace(r(4, 5), "E")])),
            d(Fix::safe("w", vec![Edit::insert(6.into(), "!")])),
        ];
        assert_eq!(apply_fixes(text, &diags, false), ("Abcdef!".to_string(), 2));
        assert_eq!(apply_fixes(text, &diags, true), ("AbcdEf!".to_string(), 3));
    }

    #[test]
    fn allowed_filter_and_markers() {
        let text = "abcdef";
        let r = |a: u32, b: u32| TextRange::new(a.into(), b.into());
        let diags = vec![
            d(Fix::safe("x", vec![Edit::replace(r(0, 1), "AA")])),
            // One of its edits is outside the allowed range: the whole fix is skipped.
            d(Fix::safe(
                "y",
                vec![Edit::replace(r(2, 3), "C"), Edit::replace(r(4, 6), "")],
            )),
            d(Fix::safe("z", vec![Edit::insert(3.into(), "!")])),
        ];
        let allowed = |e: TextRange| e.end() <= TextSize::from(4);
        let (out, applied, markers) = apply_fixes_where(text, &diags, false, &allowed);
        assert_eq!((out.as_str(), applied), ("AAbc!def", 2));
        let m = |a: u32, b: u32| (TextSize::from(a), TextSize::from(b));
        assert_eq!(markers, vec![m(0, 0), m(1, 2), m(3, 4), m(3, 5)]);
    }
}
