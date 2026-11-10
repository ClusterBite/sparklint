//! Rule overlap: one finding per site, under the most specific rule.
//!
//! Some rules describe the same problem at different specificity (`len(df.collect())`
//! is both an unbounded collect and a driver-side aggregate). Rules do not know about
//! each other: each reports what it sees, tagging the finding with its *site*
//! ([`Diagnostic::site_range`]: the `.collect()` / `.toPandas()` call, the loop, the
//! cached binding…). After all rules ran for a file, [`resolve`] drops every finding
//! of a lower-precedence rule whose site also carries a finding of a higher one.
//!
//! "Did the higher rule fire" is what counts: with the higher rule disabled (or not
//! firing at that site), the lower rule's finding stays. Suppression comments are
//! applied later, so a `# noqa`-ed higher finding still hides the lower one.

use ruff_text_size::TextRange;
use rustc_hash::FxHashSet;

use crate::diagnostic::Diagnostic;

/// `(higher, lower)`: a `lower` finding is dropped when a `higher` finding has the
/// same site.
pub const PRECEDENCE: &[(&str, &str)] = &[
    // collect-for-driver-compute > collect-unbounded / driver-row-iteration
    ("ACT009", "ACT002"),
    ("ACT009", "ACT004"),
    // driver-row-iteration > collect-unbounded / topandas-unbounded
    ("ACT004", "ACT002"),
    ("ACT004", "ACT003"),
    // withcolumn-in-loop > column-op-in-loop (site: the loop)
    ("PRF001", "PRF002"),
    // cache-single-use > cache-never-unpersisted (site: the .cache() call)
    ("PRF008", "PRF009"),
    // coalesce-one-before-write > repartition-before-write (site: the repartition(1))
    ("IOD003", "PRF010"),
    // write-in-loop > action-in-loop (site: the write call)
    ("IOD006", "ACT005"),
    // running-collect-list-window > global-window (site: the .over() call)
    ("PRF031", "PRF005"),
    // json-schema-inference-via-rdd > read-schema-inference (site: the .json() call)
    ("IOD016", "IOD001"),
    // udf-in-join-condition > or-join-condition / udf-in-filter (site: the join / filter call)
    ("UDF014", "PRF032"),
    ("UDF014", "UDF002"),
];

/// Whether `code` loses to some other rule in [`PRECEDENCE`].
fn is_lower(code: &str) -> bool {
    PRECEDENCE.iter().any(|(_, lo)| *lo == code)
}

/// Drop lower-precedence findings that share a site with a higher one (one file).
pub fn resolve(diagnostics: &mut Vec<Diagnostic>) {
    let mut keep = kept(diagnostics).into_iter();
    diagnostics.retain(|_| keep.next().unwrap_or(true));
}

/// Per diagnostic: whether [`resolve`] keeps it.
pub fn kept(diagnostics: &[Diagnostic]) -> Vec<bool> {
    if !diagnostics.iter().any(|d| is_lower(d.code)) {
        return vec![true; diagnostics.len()];
    }
    let higher: FxHashSet<(&'static str, TextRange)> = diagnostics
        .iter()
        .filter(|d| PRECEDENCE.iter().any(|(hi, _)| *hi == d.code))
        .flat_map(|d| d.sites().map(move |s| (d.code, s)))
        .collect();
    diagnostics
        .iter()
        .map(|d| {
            let site = d.site_range();
            !PRECEDENCE
                .iter()
                .any(|(hi, lo)| *lo == d.code && higher.contains(&(*hi, site)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(a: u32, b: u32) -> TextRange {
        TextRange::new(a.into(), b.into())
    }

    fn d(code: &'static str, range: TextRange, site: Option<TextRange>) -> Diagnostic {
        let d = Diagnostic::new(code, "m", range);
        match site {
            Some(s) => d.with_site(s),
            None => d,
        }
    }

    fn codes(v: &[Diagnostic]) -> Vec<&'static str> {
        v.iter().map(|d| d.code).collect()
    }

    #[test]
    fn lower_dropped_at_same_site_only() {
        let mut v = vec![
            d("ACT002", r(5, 12), Some(r(0, 14))),
            d("ACT009", r(0, 20), Some(r(0, 14))),
            d("ACT002", r(30, 40), Some(r(25, 42))), // other collect: kept
        ];
        resolve(&mut v);
        assert_eq!(codes(&v), ["ACT009", "ACT002"]);
        assert_eq!(v[1].range, r(30, 40));
    }

    #[test]
    fn lower_kept_without_higher() {
        let mut v = vec![d("ACT003", r(0, 9), Some(r(0, 12)))];
        resolve(&mut v);
        assert_eq!(codes(&v), ["ACT003"]);
        // An unrelated rule at the same site does not suppress.
        let mut v = vec![
            d("ACT003", r(0, 9), Some(r(0, 12))),
            d("ACT001", r(0, 12), None),
        ];
        resolve(&mut v);
        assert_eq!(codes(&v), ["ACT003", "ACT001"]);
    }

    #[test]
    fn chain_uses_fired_findings() {
        // ACT009 > ACT004 > ACT002 at one collect: only ACT009 stays, even though
        // ACT004 is itself dropped.
        let site = Some(r(10, 22));
        let mut v = vec![
            d("ACT002", r(13, 22), site),
            d("ACT004", r(10, 22), site),
            d("ACT009", r(0, 30), site),
        ];
        resolve(&mut v);
        assert_eq!(codes(&v), ["ACT009"]);
    }

    #[test]
    fn site_defaults_to_range() {
        let mut v = vec![d("PRF010", r(3, 18), None), d("IOD003", r(3, 18), None)];
        resolve(&mut v);
        assert_eq!(codes(&v), ["IOD003"]);
    }

    #[test]
    fn also_sites_count() {
        let mut hi = d("ACT009", r(0, 5), None);
        hi.also_sites = vec![r(10, 20), r(30, 40)];
        let mut v = vec![
            d("ACT002", r(12, 20), Some(r(10, 20))),
            d("ACT004", r(32, 40), Some(r(30, 40))),
            d("ACT002", r(50, 60), Some(r(50, 60))),
            hi,
        ];
        resolve(&mut v);
        assert_eq!(codes(&v), ["ACT002", "ACT009"]);
        assert_eq!(v[0].range, r(50, 60));
    }

    #[test]
    fn precedence_codes_are_implemented() {
        for (hi, lo) in PRECEDENCE {
            assert!(crate::rules::is_implemented(hi), "{hi}");
            assert!(crate::rules::is_implemented(lo), "{lo}");
            assert_ne!(hi, lo);
        }
    }
}
