//! Code in a loop body that runs at most once per loop: statements followed, in
//! the same block, by an unconditional `break` or `return`.
//!
//! Retry loops (`for attempt in range(3): try: df.write...; break` /
//! `except: time.sleep(..)`) and first-match searches (`for t in tables: if
//! ok(t): return spark.table(t).count()`) only complete their work once: rules
//! that count "one job / commit per iteration" ask
//! [`crate::SemanticModel::repeating_loops_at`] instead of `loops_at`.
//!
//! A region is a run of statements `s[i..j]` of one block ending right before a
//! `break` / `return` at index `j`. It starts after the last statement that may
//! `continue` this loop (a `continue` inside an `except` handler is allowed: the
//! failed attempt is retried, the successful one leaves). Blocks of nested
//! `if` / `try` / `with` / `match` statements are searched too; nested loops,
//! functions and classes are not (they are their own loops / scopes).

use ruff_python_ast::{ExceptHandler, Stmt};
use ruff_text_size::{Ranged, TextRange};

/// How a once-only region leaves the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopExit {
    /// `break`: leaves this loop; enclosing loops still repeat.
    Break,
    /// `return`: leaves the function, so every enclosing loop too.
    Return,
}

/// Once-only regions of a loop body.
pub(crate) fn once_regions(body: &[Stmt]) -> Vec<(TextRange, LoopExit)> {
    let mut out = Vec::new();
    block(body, &mut out);
    out
}

fn block(stmts: &[Stmt], out: &mut Vec<(TextRange, LoopExit)>) {
    let mut start = 0;
    for (j, s) in stmts.iter().enumerate() {
        let exit = match s {
            Stmt::Break(_) => Some(LoopExit::Break),
            Stmt::Return(_) => Some(LoopExit::Return),
            _ => None,
        };
        if let Some(exit) = exit {
            // The jump itself is included: `return spark.table(t).count()` runs once.
            if start < j || exit == LoopExit::Return {
                out.push((TextRange::new(stmts[start].start(), s.end()), exit));
            }
            // Statements after the jump are unreachable.
            break;
        }
        if matches!(s, Stmt::Continue(_)) {
            break;
        }
        nested(s, out);
        if may_continue(s, false) {
            start = j + 1;
        }
    }
}

/// Search the blocks of a compound statement (not loops, defs or classes).
fn nested(s: &Stmt, out: &mut Vec<(TextRange, LoopExit)>) {
    match s {
        Stmt::If(i) => {
            block(&i.body, out);
            for c in &i.elif_else_clauses {
                block(&c.body, out);
            }
        }
        Stmt::Try(t) => {
            block(&t.body, out);
            for ExceptHandler::ExceptHandler(h) in &t.handlers {
                block(&h.body, out);
            }
            block(&t.orelse, out);
            block(&t.finalbody, out);
        }
        Stmt::With(w) => block(&w.body, out),
        Stmt::Match(m) => {
            for c in &m.cases {
                block(&c.body, out);
            }
        }
        _ => {}
    }
}

/// Whether `s` may `continue` the enclosing loop (outside an `except` handler).
fn may_continue(s: &Stmt, in_handler: bool) -> bool {
    let any = |b: &[Stmt], h: bool| b.iter().any(|s| may_continue(s, h));
    match s {
        Stmt::Continue(_) => !in_handler,
        Stmt::If(i) => {
            any(&i.body, in_handler) || i.elif_else_clauses.iter().any(|c| any(&c.body, in_handler))
        }
        Stmt::Try(t) => {
            any(&t.body, in_handler)
                || t.handlers
                    .iter()
                    .any(|ExceptHandler::ExceptHandler(h)| any(&h.body, true))
                || any(&t.orelse, in_handler)
                || any(&t.finalbody, in_handler)
        }
        Stmt::With(w) => any(&w.body, in_handler),
        Stmt::Match(m) => m.cases.iter().any(|c| any(&c.body, in_handler)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruff_python_parser::parse_module;

    /// Source of every once-only region of the first loop in `src`.
    fn regions(src: &str) -> Vec<(String, LoopExit)> {
        let parsed = parse_module(src).unwrap();
        let body = match &parsed.syntax().body[0] {
            Stmt::For(f) => &f.body,
            Stmt::While(w) => &w.body,
            _ => panic!("no loop"),
        };
        once_regions(body)
            .into_iter()
            .map(|(r, e)| (src[r].trim().to_string(), e))
            .collect()
    }

    #[test]
    fn once_regions_table() {
        use LoopExit::*;
        let cases: &[(&str, &[(&str, LoopExit)])] = &[
            // retry: break after the write inside the try body
            (
                "for a in r:\n    try:\n        w()\n        break\n    except E:\n        s()\n",
                &[("w()\n        break", Break)],
            ),
            // break after the try statement, handler continues: allowed
            (
                "for a in r:\n    try:\n        w()\n    except E:\n        continue\n    break\n",
                &[(
                    "try:\n        w()\n    except E:\n        continue\n    break",
                    Break,
                )],
            ),
            // first match then return (the returned expression runs once too)
            (
                "for t in ts:\n    if ok(t):\n        w(t)\n        return 1\n",
                &[("w(t)\n        return 1", Return)],
            ),
            (
                "for t in ts:\n    if ok(t):\n        return n(t)\n",
                &[("return n(t)", Return)],
            ),
            // conditional break: nothing runs once
            ("while True:\n    w()\n    if done():\n        break\n", &[]),
            // a continue before the break resets the start
            (
                "for a in r:\n    x()\n    if c:\n        continue\n    w()\n    break\n",
                &[("w()\n    break", Break)],
            ),
            // continue first: the break is unreachable
            ("for a in r:\n    continue\n    break\n", &[]),
            // nested loop bodies are not searched
            (
                "for a in r:\n    for b in s:\n        w()\n        break\n",
                &[],
            ),
            // no jump
            ("for a in r:\n    w()\n", &[]),
        ];
        for (src, want) in cases {
            let got = regions(src);
            let want: Vec<(String, LoopExit)> =
                want.iter().map(|(s, e)| (s.to_string(), *e)).collect();
            assert_eq!(got, want, "{src}");
        }
    }
}
