//! "Is this frame small enough for X?" — one size verdict per value (DataFrames and
//! RDDs alike: `sc.parallelize([...])` is driver-built data like
//! `createDataFrame([...])`, and `df.rdd` keeps the frame's size facts).
//!
//! Rules never combine the raw size facts (`bounded`, `local`, `reduced`) themselves:
//! they ask [`TypeInfo::size`](crate::TypeInfo::size) /
//! [`SparkCall::size`](crate::SparkCall::size) one of the named questions below, so
//! every rule asking the same question gets the same answer.

use crate::facts::Facts;

/// What the oracle knows about how big a value is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Size {
    rows: Option<u64>,
    /// The row bound comes from a limit-like step / small origin, not only an aggregate.
    row_limited: bool,
    local: bool,
    reduced: bool,
}

impl Size {
    pub fn of(f: &Facts) -> Size {
        Size {
            rows: f.bounded,
            row_limited: f.row_limited,
            local: f.local,
            reduced: f.reduced,
        }
    }

    /// At most this many rows ([`crate::facts::Bounded`]; `u64::MAX` = a `limit(n)`
    /// with a non-literal `n`).
    pub fn rows(self) -> Option<u64> {
        self.rows
    }

    /// Has any row bound (literal or not).
    pub fn has_row_bound(self) -> bool {
        self.rows.is_some()
    }

    /// Exactly one row: an aggregate (`agg`, `count(*)` SQL, keyless `groupBy().agg`),
    /// `limit(1)`, `first()`.
    pub fn one_row(self) -> bool {
        self.rows == Some(1)
    }

    /// Driver-built data or catalog metadata ([`crate::facts::Local`]).
    pub fn local(self) -> bool {
        self.local
    }

    /// The whole value fits in driver memory: a row bound or driver-built / catalog
    /// metadata data. Collecting it (`collect`, `toPandas`, `[0]` on a collect,
    /// broadcasting it) is fine.
    pub fn fits_driver(self) -> bool {
        self.has_row_bound() || self.local
    }

    /// An action over it (`count()`, `display()`, a job per loop iteration) is a
    /// small job: it fits the driver.
    pub fn cheap_action(self) -> bool {
        self.fits_driver()
    }

    /// A preview of it (`show()`, `display()`) is cheap: driver-built data, catalog
    /// metadata, or a row bound from a limit-like step (`limit(10)`, SQL `LIMIT n`), so
    /// a preview left in the code is a deliberate small look, not a forgotten job. A
    /// bound that comes only from aggregating the frame (`agg(..)`, `describe()`,
    /// `summary()`, `SELECT count(*) FROM t`) is not: that preview scans everything.
    pub fn cheap_preview(self) -> bool {
        self.local || (self.has_row_bound() && self.row_limited)
    }

    /// A shuffle of it (`repartition(n)` before a write) is cheap and only sets the
    /// output file count: it fits the driver.
    pub fn cheap_shuffle(self) -> bool {
        self.fits_driver()
    }

    /// Small enough to be a broadcast / cross-join side (a broadcast collects it on
    /// the driver): it fits the driver.
    pub fn broadcastable(self) -> bool {
        self.fits_driver()
    }

    /// Probably a small output (a handful of files is right): fits the driver or went
    /// through a reducing step (filter, groupBy-agg, distinct, sample).
    pub fn small_output(self) -> bool {
        self.fits_driver() || self.reduced
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn size(rows: Option<u64>, local: bool, reduced: bool) -> Size {
        sized(rows, true, local, reduced)
    }

    fn sized(rows: Option<u64>, row_limited: bool, local: bool, reduced: bool) -> Size {
        Size::of(&Facts {
            bounded: rows,
            row_limited,
            local,
            reduced,
            ..Facts::NONE
        })
    }

    #[test]
    fn verdict_per_shape() {
        // shape, size, [one_row, fits_driver, cheap_action, cheap_preview,
        //               cheap_shuffle, broadcastable, small_output]
        for (shape, s, want) in [
            ("table", size(None, false, false), [false; 7]),
            (
                "filtered",
                size(None, false, true),
                [false, false, false, false, false, false, true],
            ),
            (
                "limit(5).agg(..)",
                size(Some(1), false, false),
                [true, true, true, true, true, true, true],
            ),
            // One row out, every row read: only the preview question says no.
            (
                "agg(..)",
                sized(Some(1), false, false, false),
                [true, true, true, false, true, true, true],
            ),
            (
                "describe()",
                sized(Some(10), false, false, false),
                [false, true, true, false, true, true, true],
            ),
            (
                "createDataFrame(pdf).agg(..)",
                sized(Some(1), false, true, false),
                [true, true, true, true, true, true, true],
            ),
            (
                "limit(10)",
                size(Some(10), false, false),
                [false, true, true, true, true, true, true],
            ),
            (
                "limit(n)",
                size(Some(u64::MAX), false, false),
                [false, true, true, true, true, true, true],
            ),
            (
                "createDataFrame(pdf)",
                size(None, true, false),
                [false, true, true, true, true, true, true],
            ),
            (
                "createDataFrame(pdf).limit(5)",
                size(Some(5), true, false),
                [false, true, true, true, true, true, true],
            ),
            // RDDs: the same facts, the same answers.
            (
                "sc.parallelize([a, b]).map(f)",
                size(Some(2), true, false),
                [false, true, true, true, true, true, true],
            ),
            (
                "sc.textFile(p).filter(f)",
                size(None, false, true),
                [false, false, false, false, false, false, true],
            ),
            (
                "createDataFrame(pdf).rdd",
                size(None, true, false),
                [false, true, true, true, true, true, true],
            ),
        ] {
            let got = [
                s.one_row(),
                s.fits_driver(),
                s.cheap_action(),
                s.cheap_preview(),
                s.cheap_shuffle(),
                s.broadcastable(),
                s.small_output(),
            ];
            assert_eq!(got, want, "{shape}");
        }
    }
}
