//! SQL text inside Python: the one reader of `spark.sql(...)` queries and of SQL
//! fragments (`F.expr("...")`, `df.filter("...")`).
//!
//! [`SemanticModel::spark_sql`] / [`SemanticModel::sql_query`] take the query of a
//! `sql()` call once: the argument (positional or `sqlQuery=`), a string literal or
//! f-string (interpolations become `{}`) or a name whose every binding holds the same
//! such text ([`SemanticModel::sql_text`]). [`SqlText`]
//! tokenizes it once — comments dropped, string literals and quoted identifiers
//! masked — and answers the questions rules ask: statement keyword, `SET k = v`,
//! `OPTIONS (..)` / `TBLPROPERTIES (..)` key/value lists, keywords and function calls
//! outside strings, a trailing `LIMIT n`, single-row queries, and the plan-shape
//! probes (`NOT IN (SELECT ..)`, `CUBE`/`ROLLUP` width, `ORDER BY rand()`, `SELECT *`,
//! join-condition equalities). It is a tokenizer, not a SQL parser: clause questions
//! (select list, `FROM`, `GROUP BY` items, `ORDER BY`, window specifications) and
//! condition questions (join keys, NULL comparisons, incremental `WHERE`s) are
//! answered from one shallow clause parse of the tokens ([`clauses`]: clauses,
//! windows, condition trees, assignment lists), built lazily on the first such
//! question.
//!
//! SQL text passed to Column / DataFrame calls (`F.expr`, `filter` / `where`,
//! `selectExpr`) and to `spark.sql` is found by ONE question,
//! [`SemanticModel::sql_sites`] ([`sites`]): rules ask it instead of matching call
//! shapes themselves.

mod clauses;
mod sites;

use std::cell::OnceCell;
use std::ops::Range;

use ruff_python_ast::{self as ast, Expr};

use crate::infer::call_arg;
use crate::model::SemanticModel;
use clauses::{ClauseKind, Clauses, Pred, PredKind, Window};
pub use sites::{SqlSite, SqlSiteKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Identifier or keyword (`SELECT`, `spark`, `t1`).
    Word,
    /// `10`, `1.5`, `10m`.
    Number,
    /// `'..'` / `".."` string literal.
    Str,
    /// `` `..` `` quoted identifier.
    Ident,
    /// Any other single character (`(`, `=`, `.`, `;`, `{`).
    Punct,
}

#[derive(Debug, Clone, Copy)]
struct Tok {
    kind: Kind,
    /// Byte span in the raw text (quotes included).
    start: usize,
    end: usize,
}

/// One SQL text, tokenized.
#[derive(Debug, Clone)]
pub struct SqlText {
    raw: String,
    /// `raw` uppercased, comments blanked and the contents of quoted strings and
    /// identifiers replaced by spaces (quotes kept); same byte length as `raw`.
    code: String,
    toks: Vec<Tok>,
    /// Byte spans of the bodies of `/*+ ... */` hint comments.
    hint_spans: Vec<(usize, usize)>,
    /// Query blocks, clauses and window specifications, parsed on first use.
    clauses: OnceCell<Clauses>,
}

/// `SET key = value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetPair {
    /// The key as written (backticks removed).
    pub key: String,
    /// Unquoted value text; `None` when it is not constant (an f-string hole).
    pub value: Option<String>,
}

/// What a SQL boolean condition says about join keys (PRF004).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlCond {
    /// Has an equality (any `=`, also against a literal).
    Equi,
    /// Only range / LIKE / BETWEEN predicates.
    NonEqui,
    Unknown,
}

/// One top-level conjunct of a SQL join condition (PRF032).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlConjunct {
    /// A column-to-column equality: a usable join key.
    Equi,
    /// A disjunction holding such an equality: no key from it.
    OrWithEq,
    /// Anything else (range predicate, literal filter).
    Other,
}

impl SqlText {
    pub fn new(raw: &str) -> Self {
        let b = raw.as_bytes();
        let mut code = String::with_capacity(raw.len());
        let mut toks = Vec::new();
        let mut hint_spans = Vec::new();
        let blank = |code: &mut String, s: &str| {
            for ch in s.chars() {
                if ch == '\n' {
                    code.push('\n');
                } else {
                    code.extend(std::iter::repeat_n(' ', ch.len_utf8()));
                }
            }
        };
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            let start = i;
            if c == b'-' && b.get(i + 1) == Some(&b'-') {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                blank(&mut code, &raw[start..i]);
                continue;
            }
            if c == b'/' && b.get(i + 1) == Some(&b'*') {
                i += 2;
                while i < b.len() && !(b[i] == b'*' && b.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                if b.get(start + 2) == Some(&b'+') {
                    hint_spans.push((start + 3, i.min(b.len())));
                }
                i = (i + 2).min(b.len());
                blank(&mut code, &raw[start..i]);
                continue;
            }
            if matches!(c, b'\'' | b'"' | b'`') {
                i += 1;
                loop {
                    if i >= b.len() {
                        break;
                    }
                    if c != b'`' && b[i] == b'\\' {
                        i = (i + 2).min(b.len());
                        continue;
                    }
                    if b[i] == c {
                        // `` `a``b` `` escapes a backtick inside a quoted identifier.
                        if c == b'`' && b.get(i + 1) == Some(&b'`') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                let closed = i < b.len();
                let body_end = i;
                if closed {
                    i += 1;
                }
                code.push(c as char);
                blank(&mut code, &raw[start + 1..body_end]);
                if closed {
                    code.push(c as char);
                }
                let kind = if c == b'`' { Kind::Ident } else { Kind::Str };
                toks.push(Tok {
                    kind,
                    start,
                    end: i,
                });
                continue;
            }
            let ch = raw[i..].chars().next().unwrap_or(' ');
            if ch.is_whitespace() {
                i += ch.len_utf8();
                code.push_str(&raw[start..i]);
                continue;
            }
            let word_char = |x: char| x.is_alphanumeric() || x == '_';
            let kind = if ch.is_ascii_digit() {
                // `10`, `1.5`, `10m`, `1e9`
                while let Some(x) = raw[i..].chars().next() {
                    let frac = x == '.' && raw[i + 1..].starts_with(|d: char| d.is_ascii_digit());
                    if !(word_char(x) || frac) {
                        break;
                    }
                    i += x.len_utf8();
                }
                Kind::Number
            } else if word_char(ch) || ch == '$' {
                while let Some(x) = raw[i..].chars().next() {
                    if !(word_char(x) || x == '$') {
                        break;
                    }
                    i += x.len_utf8();
                }
                Kind::Word
            } else {
                i += ch.len_utf8();
                Kind::Punct
            };
            code.push_str(&raw[start..i].to_ascii_uppercase());
            toks.push(Tok {
                kind,
                start,
                end: i,
            });
        }
        debug_assert_eq!(code.len(), raw.len());
        Self {
            raw: raw.to_string(),
            code,
            toks,
            hint_spans,
            clauses: OnceCell::new(),
        }
    }

    /// The clause structure of the text, parsed once.
    fn clauses(&self) -> &Clauses {
        self.clauses
            .get_or_init(|| Clauses::parse(&self.toks, &self.code))
    }

    /// Code view of tokens `r` (`""` for an empty range).
    fn code_of(&self, r: &Range<usize>) -> &str {
        match (self.toks.get(r.start), self.toks.get(r.end.wrapping_sub(1))) {
            (Some(a), Some(b)) if !r.is_empty() => &self.code[a.start..b.end],
            _ => "",
        }
    }

    /// Token `i` upper-cased when it is a word.
    fn word_at(&self, i: usize) -> Option<&str> {
        let t = self.toks.get(i)?;
        (t.kind == Kind::Word).then(|| self.upper(t))
    }

    fn punct_at(&self, i: usize, p: &str) -> bool {
        self.toks.get(i).is_some_and(|t| self.is_punct(t, p))
    }

    /// Token `i` starts a call of `rand(`/`randn(`/`random(`.
    fn random_call_at(&self, i: usize) -> bool {
        matches!(self.word_at(i), Some("RAND" | "RANDN" | "RANDOM")) && self.punct_at(i + 1, "(")
    }

    /// Token `i` opens `( [(..] SELECT|WITH`: a parenthesised subquery.
    fn opens_subquery(&self, i: usize) -> bool {
        let mut j = i;
        while self.punct_at(j, "(") {
            j += 1;
        }
        j > i && matches!(self.word_at(j), Some("SELECT" | "WITH"))
    }

    /// The text as written.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Uppercased text with comments removed and quoted strings/identifiers blanked
    /// (quotes kept), byte-for-byte aligned with [`Self::raw`]: keyword scans on it
    /// never match inside a string or a comment.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Whitespace-separated words of [`Self::code`] (trailing `;` dropped); a quoted
    /// string or identifier stays inside its word (`` `my db`.t `` is one word).
    pub fn words(&self) -> Vec<&str> {
        let mut out = Vec::new();
        let mut span: Option<(usize, usize)> = None;
        for t in self.statement_toks() {
            match span {
                Some((s, e)) if e == t.start => span = Some((s, t.end)),
                _ => {
                    if let Some((s, e)) = span {
                        out.push(&self.code[s..e]);
                    }
                    span = Some((t.start, t.end));
                }
            }
        }
        if let Some((s, e)) = span {
            out.push(&self.code[s..e]);
        }
        out
    }

    /// The statement's leading keyword, uppercased (`SELECT`, `SET`, `CACHE`, ...).
    pub fn keyword(&self) -> Option<&str> {
        let t = self.toks.first()?;
        (t.kind == Kind::Word).then(|| &self.code[t.start..t.end])
    }

    fn upper(&self, t: &Tok) -> &str {
        &self.code[t.start..t.end]
    }

    fn is_punct(&self, t: &Tok, p: &str) -> bool {
        t.kind == Kind::Punct && &self.raw[t.start..t.end] == p
    }

    /// Text of tokens `ts` as written, unquoted when it is one quoted token.
    fn text_of(&self, ts: &[Tok]) -> Option<String> {
        let (first, last) = (ts.first()?, ts.last()?);
        if ts.len() == 1 && matches!(first.kind, Kind::Str | Kind::Ident) {
            let s = &self.raw[first.start..first.end];
            let q = &s[..1];
            let inner = s[1..].strip_suffix(q).unwrap_or(&s[1..]);
            return Some(inner.trim().to_string());
        }
        Some(self.raw[first.start..last.end].trim().to_string())
    }

    /// Tokens without trailing `;`.
    fn statement_toks(&self) -> &[Tok] {
        let mut ts = &self.toks[..];
        while let Some((last, rest)) = ts.split_last() {
            if !self.is_punct(last, ";") {
                break;
            }
            ts = rest;
        }
        ts
    }

    /// `SET key = value` (Spark config); `None` for `SET`, `SET -v`, `SET key`,
    /// `SET VAR x = ..` and keys that are not constant.
    pub fn set_pair(&self) -> Option<SetPair> {
        if self.keyword() != Some("SET") {
            return None;
        }
        let ts = &self.statement_toks()[1..];
        let eq = ts.iter().position(|t| self.is_punct(t, "="))?;
        let key = self.text_of(&ts[..eq])?;
        if key.is_empty() || key.contains(char::is_whitespace) || key.contains(['{', '}']) {
            return None;
        }
        let value = match self.text_of(&ts[eq + 1..]) {
            Some(v) if v.contains("{}") => None,
            Some(v) => Some(v),
            None => Some(String::new()),
        };
        Some(SetPair { key, value })
    }

    /// Every `<keyword> ( k v, k = v, .. )` list in the statement (`OPTIONS`,
    /// `TBLPROPERTIES`), keys and values unquoted, as written.
    pub fn properties(&self, keyword: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let ts = &self.toks;
        for (i, t) in ts.iter().enumerate() {
            if t.kind != Kind::Word || !self.upper(t).eq_ignore_ascii_case(keyword) {
                continue;
            }
            if !ts.get(i + 1).is_some_and(|p| self.is_punct(p, "(")) {
                continue;
            }
            let mut depth = 0i32;
            let mut item: Vec<Tok> = Vec::new();
            for t in &ts[i + 2..] {
                let close = self.is_punct(t, ")") && depth == 0;
                if close || (self.is_punct(t, ",") && depth == 0) {
                    if let Some(kv) = self.property(&item) {
                        out.push(kv);
                    }
                    item.clear();
                    if close {
                        break;
                    }
                    continue;
                }
                if self.is_punct(t, "(") {
                    depth += 1;
                } else if self.is_punct(t, ")") {
                    depth -= 1;
                }
                item.push(*t);
            }
        }
        out
    }

    fn property(&self, item: &[Tok]) -> Option<(String, String)> {
        let (k, v) = match item.iter().position(|t| self.is_punct(t, "=")) {
            Some(eq) => (&item[..eq], &item[eq + 1..]),
            None if item.len() >= 2 => (&item[..1], &item[1..]),
            None => return None,
        };
        Some((self.text_of(k)?, self.text_of(v)?))
    }

    /// Whether `kw` occurs as a whole word outside strings and comments.
    pub fn has_keyword(&self, kw: &str) -> bool {
        let kw = kw.to_ascii_uppercase();
        self.code
            .match_indices(kw.as_str())
            .any(|(i, _)| is_word_at(&self.code, i, &kw))
    }

    /// Whether the text calls function `name` (`name(`, case-insensitive) outside
    /// strings and comments.
    pub fn calls(&self, name: &str) -> bool {
        let n = name.to_ascii_uppercase();
        self.code.match_indices(n.as_str()).any(|(i, _)| {
            is_word_at(&self.code, i, &n) && self.code[i + n.len()..].trim_start().starts_with('(')
        })
    }

    /// Identifier words outside strings and comments, as written: words (keywords and
    /// function names included) and quoted identifiers without their backticks; no
    /// numbers, no string contents. `upper(i.name) = 'x'` -> `upper`, `i`, `name`.
    pub fn identifiers(&self) -> Vec<String> {
        self.toks
            .iter()
            .filter_map(|t| match t.kind {
                Kind::Word => Some(self.raw[t.start..t.end].to_string()),
                Kind::Ident => {
                    let body = &self.raw[t.start + 1..t.end];
                    let body = body.strip_suffix('`').unwrap_or(body);
                    Some(body.replace("``", "`"))
                }
                _ => None,
            })
            .collect()
    }

    /// `n` if the query ends in a top-level `LIMIT <n>` (`SELECT 1 FROM t LIMIT 1`).
    /// An f-string placeholder count (`LIMIT {n}`) is a bound of unknown size,
    /// `u64::MAX`, like `df.limit(n)` with a non-literal `n`.
    pub fn trailing_limit(&self) -> Option<u64> {
        let ts = self.statement_toks();
        let is_limit = |kw: &Tok| kw.kind == Kind::Word && self.upper(kw) == "LIMIT";
        if let [.., kw, open, close] = ts
            && is_limit(kw)
            && self.is_punct(open, "{")
            && self.is_punct(close, "}")
            && open.end == close.start
        {
            return Some(u64::MAX);
        }
        let [.., kw, n] = ts else { return None };
        // A LIMIT inside a parenthesised subquery would end in `)`, so a trailing
        // `LIMIT n` pair is the outermost one.
        (n.kind == Kind::Number && is_limit(kw))
            .then(|| self.raw[n.start..n.end].parse().ok())
            .flatten()
    }

    /// Whether the query text restricts its rows anywhere (also inside a subquery):
    /// a `WHERE`, `LIMIT`, `SELECT [DISTINCT] TOP n` or `FETCH FIRST|NEXT` outside
    /// strings and comments. A JDBC `dbtable` / `query` subquery with one of these is
    /// treated as a deliberate, likely small extract.
    pub fn restricts_rows(&self) -> bool {
        let word = |t: &Tok| (t.kind == Kind::Word).then(|| self.upper(t));
        let ts = &self.toks;
        ts.iter()
            .any(|t| matches!(word(t), Some("WHERE" | "LIMIT")))
            || ts.windows(2).any(|p| {
                matches!(
                    (word(&p[0]), word(&p[1])),
                    (Some("FETCH"), Some("FIRST" | "NEXT"))
                )
            })
            // `TOP n` / `TOP (n)`: a column named `top` is not a row limit.
            || ts.windows(3).any(|p| {
                matches!(word(&p[0]), Some("SELECT" | "DISTINCT"))
                    && word(&p[1]) == Some("TOP")
                    && (p[2].kind == Kind::Number || self.is_punct(&p[2], "("))
            })
    }

    /// Whether the query is one row only because it aggregates the rows it reads: a
    /// [`Self::single_row`] `SELECT … FROM` (`SELECT count(*) FROM t`, also with a
    /// trailing `LIMIT`). Not `DESCRIBE DETAIL`, an `EXPLAIN` or a `SELECT` without
    /// `FROM`, which read no table rows.
    pub fn aggregates_table(&self) -> bool {
        self.word_at(0) == Some("SELECT") && self.has_keyword("FROM") && self.single_row()
    }

    /// Whether the query provably returns at most one row: `DESCRIBE DETAIL`, an
    /// `EXPLAIN` (one row of plan text), a `SELECT` without `FROM`, or a `SELECT` of only aggregates with no `GROUP BY`
    /// (`SELECT max(d) FROM t`).
    pub fn single_row(&self) -> bool {
        if self.words().starts_with(&["DESCRIBE", "DETAIL"]) || self.explain() {
            return true;
        }
        // A `SELECT` statement: `SELECT` and a space, then something.
        let [select, next, ..] = self.statement_toks() else {
            return false;
        };
        if self.word_at(0) != Some("SELECT") || next.start == select.end {
            return false;
        }
        let c = self.clauses();
        if c.queries
            .iter()
            .any(|q| q.clause(ClauseKind::GroupBy).is_some())
            || self
                .words()
                .iter()
                .any(|w| matches!(*w, "UNION" | "JOIN" | "LATERAL" | "EXPLODE"))
        {
            return false;
        }
        let top = c
            .queries
            .first()
            .filter(|q| q.select == Some(0) && q.clause(ClauseKind::From).is_some());
        let Some(list) = top.and_then(|q| q.clause(ClauseKind::Select)) else {
            return !self.has_keyword("FROM") && !self.has_keyword("WHERE");
        };
        // A window aggregate (`max(x) OVER (..)`) is one value per row.
        !self.has_keyword("OVER")
            && list
                .items
                .iter()
                .all(|it| is_aggregate_item(self.code_of(it)))
    }

    /// Whether the text, read as ONE select-list expression (a `selectExpr` argument),
    /// is a global aggregate: `min(d) AS lo`, `count(*)`, `variance(a - b)`. A window
    /// aggregate (`OVER`) or a list of several expressions is not.
    pub fn aggregate_expression(&self) -> bool {
        let items = &self.clauses().items;
        !self.has_keyword("OVER") && items.len() == 1 && is_aggregate_item(self.code_of(&items[0]))
    }

    /// Whether the statement is `EXPLAIN [EXTENDED|CODEGEN|COST|FORMATTED] <query>`:
    /// Spark only plans the inner query and returns its plan as one row of text.
    pub fn explain(&self) -> bool {
        self.words().first() == Some(&"EXPLAIN")
    }

    /// Whether the statement is a catalog metadata command whose result is sized by
    /// the catalog, not by table data: `SHOW TABLES` / `SHOW TBLPROPERTIES` / `SHOW
    /// COLUMNS` / `SHOW CATALOGS` ..., `DESCRIBE [TABLE|EXTENDED|SCHEMA|FUNCTION ..] x`.
    /// `SHOW PARTITIONS` and `DESCRIBE HISTORY` grow with the data / commit count and
    /// are not metadata here.
    pub fn catalog_metadata(&self) -> bool {
        let w = self.words();
        match w.as_slice() {
            ["SHOW", second, ..] => *second != "PARTITIONS",
            ["DESCRIBE" | "DESC", second, ..] => !matches!(*second, "HISTORY" | "QUERY"),
            _ => false,
        }
    }

    /// Whether the query contains `NOT IN (SELECT|WITH ...)`.
    pub fn not_in_subquery(&self) -> bool {
        let c = self.clauses();
        (0..self.toks.len())
            .filter(|&i| self.word_at(i) == Some("NOT"))
            .any(|i| {
                (self.word_at(i + 1) == Some("IN") && self.opens_subquery(i + 2))
                    // `NOT (x IN (SELECT ...))`: the same null-aware anti join.
                    || c.opened_at(i + 1).is_some_and(|g| self.whole_in_subquery(g))
            })
    }

    /// Group `g` holds exactly `<expr> IN (SELECT|WITH ...)`: its first own-level `IN`
    /// has a plain left side (no AND / OR / NOT) and a subquery that ends the group.
    fn whole_in_subquery(&self, g: usize) -> bool {
        let c = self.clauses();
        let grp = &c.groups[g];
        let Some(close) = grp.close else {
            return false;
        };
        let Some(k) =
            (grp.open + 1..close).find(|&i| c.level[i] == Some(g) && self.word_at(i) == Some("IN"))
        else {
            return false;
        };
        let lhs = grp.open + 1..k;
        !lhs.is_empty()
            && !lhs
                .into_iter()
                .any(|i| matches!(self.word_at(i), Some("AND" | "OR" | "NOT")))
            && self.opens_subquery(k + 1)
            && c.group_at(k + 1)
                .is_some_and(|h| h.close.map(|x| x + 1) == Some(close))
    }

    /// CUBE / ROLLUP groupings, one entry per `GROUP BY` clause that has any
    /// (`CUBE(..)`, `ROLLUP(..)`, `... WITH CUBE|ROLLUP`). Spark multiplies the
    /// grouping sets of every CUBE / ROLLUP / GROUPING SETS item of one clause
    /// (`GROUP BY CUBE(a, b), ROLLUP(c)` = 4 x 2 sets), so each entry carries the
    /// product as well as its widest single grouping.
    pub fn cube_groupings(&self) -> Vec<CubeGrouping> {
        let c = self.clauses();
        let closed = |i: usize| c.group_at(i).filter(|g| g.close.is_some());
        let mut out: Vec<(usize, CubeGrouping)> = Vec::new();
        for q in &c.queries {
            let Some(gb) = q.clause(ClauseKind::GroupBy) else {
                continue;
            };
            // (is_cube, columns) per CUBE / ROLLUP item; sets per GROUPING SETS item.
            let mut cubes: Vec<(bool, usize)> = Vec::new();
            let mut sets: Vec<u128> = Vec::new();
            for it in gb.items.iter().filter(|it| !it.is_empty()) {
                let i = it.start;
                match self.word_at(i) {
                    Some(w @ ("CUBE" | "ROLLUP")) => {
                        if let Some(g) = closed(i + 1) {
                            cubes.push((w == "CUBE", g.item_count()));
                        }
                    }
                    Some("GROUPING") if self.word_at(i + 1) == Some("SETS") => {
                        if let Some(g) = closed(i + 2) {
                            sets.push(g.item_count().max(1) as u128);
                        }
                    }
                    _ => {}
                }
            }
            // `GROUP BY a, b WITH CUBE|ROLLUP` groups by all the items before it.
            let with = gb.body.clone().find(|&i| {
                c.level[i] == q.scope
                    && self.word_at(i) == Some("WITH")
                    && matches!(self.word_at(i + 1), Some("CUBE" | "ROLLUP"))
            });
            if let Some(w) = with {
                let n = gb
                    .items
                    .iter()
                    .filter(|it| !it.is_empty() && it.start < w)
                    .count();
                cubes.push((self.word_at(w + 1) == Some("CUBE"), n));
            }
            let Some(&(is_cube, columns)) = cubes.first() else {
                continue;
            };
            let mut g = CubeGrouping {
                is_cube,
                columns,
                sets: 1,
                parts: 0,
            };
            for (is_cube, n) in cubes {
                if (is_cube && !g.is_cube) || (is_cube == g.is_cube && n > g.columns) {
                    g.is_cube = is_cube;
                    g.columns = n;
                }
                g.sets = g.sets.saturating_mul(grouping_sets(is_cube, n));
                g.parts += 1;
            }
            for s in sets {
                g.sets = g.sets.saturating_mul(s);
                g.parts += 1;
            }
            out.push((gb.kw, g));
        }
        out.sort_by_key(|(kw, _)| *kw);
        out.into_iter().map(|(_, g)| g).collect()
    }

    /// Distinct aggregates (`COUNT(DISTINCT a)`, `SUM(DISTINCT b, c)`, ...) per `SELECT`
    /// (a text without `SELECT`, e.g. an `F.expr` fragment, is one segment). Each
    /// aggregate is its argument list as a key: lower-cased, backticks and spaces
    /// removed, sorted, so `COUNT(DISTINCT `A`)` and `sum(distinct a)` share a key (Spark
    /// groups distinct aggregates by their argument set).
    pub fn distinct_aggregates(&self) -> Vec<Vec<String>> {
        let up = &self.code;
        let mut out: Vec<Vec<String>> = vec![Vec::new()];
        let mut seen_select = false;
        let mut i = 0;
        while i < up.len() {
            if !up.is_char_boundary(i) {
                i += 1;
                continue;
            }
            if is_word_at(up, i, "SELECT") {
                if seen_select || !out[0].is_empty() {
                    out.push(Vec::new());
                }
                seen_select = true;
                i += 6;
                continue;
            }
            if is_word_at(up, i, "DISTINCT") {
                let before = up[..i].trim_end();
                if let Some(b) = before.strip_suffix('(') {
                    let b = b.trim_end();
                    let fname = b
                        .rsplit(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                        .next()
                        .unwrap_or("");
                    if !fname.is_empty() && fname.as_bytes()[0].is_ascii_alphabetic() {
                        let open = before.len() - 1;
                        if let Some(body) = paren_body(up, open) {
                            let (s, e) = (i + 8, open + 1 + body.len());
                            if let Some(raw) = self.raw.get(s..e) {
                                let mut args: Vec<String> = split_top_commas(raw)
                                    .into_iter()
                                    .map(|a| {
                                        a.chars()
                                            .filter(|c| !c.is_whitespace() && *c != '`')
                                            .collect::<String>()
                                            .to_lowercase()
                                    })
                                    .filter(|a| !a.is_empty())
                                    .collect();
                                args.sort();
                                if !args.is_empty() {
                                    out.last_mut().expect("non-empty").push(args.join(","));
                                }
                            }
                        }
                    }
                }
                i += 8;
                continue;
            }
            i += 1;
        }
        out.retain(|v| !v.is_empty());
        out
    }

    /// Whether the query has a global `ORDER BY rand()` (leading sort key), outside
    /// window specifications (`OVER (PARTITION BY k ORDER BY rand())`).
    pub fn order_by_rand(&self) -> bool {
        self.clauses()
            .queries
            .iter()
            .flat_map(|q| &q.clauses)
            .any(|c| {
                c.kind == ClauseKind::OrderBy
                    && !c.body.is_empty()
                    && self.random_call_at(c.body.start)
            })
    }

    /// Running array aggregates, in text order: `collect_list(..)` / `collect_set(..)` /
    /// `array_agg(..)` (an optional `FILTER (WHERE ..)` between) `OVER (..)` with an
    /// inline window that has `ORDER BY` / `SORT BY` and either no frame (the default
    /// `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`) or a frame from `UNBOUNDED
    /// PRECEDING` (or at least [`RUNNING_FRAME_OFFSET`] rows back) to a finite end.
    /// Named windows (`OVER w`) and frames whose bounds are not plain (`INTERVAL ..`)
    /// are not followed.
    pub fn running_collects(&self) -> Vec<RunningCollect> {
        let c = self.clauses();
        c.windows
            .iter()
            .filter(|w| w.over.is_some() && c.groups[w.group].close.is_some())
            .filter_map(|w| {
                let name = self.word_at(w.func?)?;
                if !matches!(name, "COLLECT_LIST" | "COLLECT_SET" | "ARRAY_AGG") {
                    return None;
                }
                Some(RunningCollect {
                    name: name.to_ascii_lowercase(),
                    partitioned: self.running_window(w)?,
                })
            })
            .collect()
    }

    /// Comparisons with a `NULL` literal, in text order: `a = NULL`, `a == NULL`,
    /// `a != NULL`, `a <> NULL` (either operand order). They are NULL for every row
    /// (three-valued logic), so a filter on them keeps nothing. Not comparisons:
    /// the null-safe `<=>`, `IS [NOT] NULL`, assignments (`UPDATE .. SET a = NULL`,
    /// MERGE `UPDATE SET`, a `SET k = NULL` statement, `TBLPROPERTIES` / `OPTIONS`
    /// pairs) and named arguments (`=>`).
    pub fn null_comparisons(&self) -> Vec<NullComparison> {
        let n = self.toks.len();
        let null = |i: usize| self.word_at(i) == Some("NULL");
        self.clauses()
            .ops
            .iter()
            .filter(|r| !r.assignment)
            .filter_map(|r| {
                let (a, b) = (r.range.start, r.range.end);
                // `<lhs> op NULL` / `NULL op <rhs>`
                let with_null = (a > 0 && b < n && null(b)) || (a > 0 && null(a - 1) && b < n);
                let op = &self.raw[self.toks[a].start..self.toks[b - 1].end];
                (with_null && matches!(op, "=" | "==" | "!=" | "<>")).then(|| NullComparison {
                    op: op.to_string(),
                    negated: matches!(op, "!=" | "<>"),
                })
            })
            .collect()
    }

    /// `Some(partitioned)` when window `w` is ordered with a running frame (see
    /// [`Self::running_collects`]).
    fn running_window(&self, w: &Window) -> Option<bool> {
        w.order.as_ref()?;
        let partitioned = w.partition.is_some();
        let Some(f) = w.frame else {
            return Some(partitioned);
        };
        let end = self.clauses().groups[w.group].inner(self.toks.len()).end;
        // Words and numbers from the frame keyword on (`ROWS BETWEEN 5 PRECEDING ..`).
        let words: Vec<&str> = self.toks[f..end]
            .iter()
            .filter(|t| matches!(t.kind, Kind::Word | Kind::Number))
            .map(|t| self.upper(t))
            .collect();
        let (start, end) = if words.get(1) == Some(&"BETWEEN") {
            let (start, used) = frame_bound_at(&words, 2)?;
            if words.get(2 + used) != Some(&"AND") {
                return None;
            }
            (start, frame_bound_at(&words, 3 + used)?.0)
        } else {
            // `ROWS UNBOUNDED PRECEDING` = `.. BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
            (frame_bound_at(&words, 1)?.0, FrameBound::Finite)
        };
        (start == FrameBound::FromStart && end == FrameBound::Finite).then_some(partitioned)
    }

    /// Whether the text starts with a call of `rand(`/`randn(`/`random(` (a sort key).
    pub fn is_random_call(&self) -> bool {
        self.random_call_at(0)
    }

    /// Whether the statement rediscovers a table's partitions by listing its
    /// directories: `MSCK REPAIR TABLE t`, `REPAIR TABLE t [ADD|DROP|SYNC PARTITIONS]`,
    /// `ALTER TABLE t RECOVER PARTITIONS`. `REPAIR TABLE t SYNC METADATA` (Unity
    /// Catalog metadata sync for Delta/Iceberg) lists no partitions.
    pub fn repairs_partitions(&self) -> bool {
        let w = self.words();
        match w.as_slice() {
            ["MSCK", "REPAIR", "TABLE", _, rest @ ..] | ["REPAIR", "TABLE", _, rest @ ..] => {
                !matches!(rest, ["SYNC", "METADATA", ..])
            }
            ["ALTER", "TABLE", _, "RECOVER", "PARTITIONS", ..] => true,
            _ => false,
        }
    }

    /// Number of columns in the widest `ZORDER BY (a, b, ...)` list (Delta `OPTIMIZE`);
    /// the parentheses are optional (`ZORDER BY a, b`), keywords outside strings and
    /// comments only. `None` without a `ZORDER BY`.
    pub fn zorder_columns(&self) -> Option<usize> {
        let up = &self.code;
        let mut best: Option<usize> = None;
        for (i, _) in up.char_indices() {
            if !is_word_at(up, i, "ZORDER") {
                continue;
            }
            let rest = &up[i + 6..];
            let by = rest.len() - rest.trim_start().len();
            if !is_word_at(rest, by, "BY") {
                continue;
            }
            let after = i + 6 + by + 2;
            let tail = &up[after..];
            let ws = tail.len() - tail.trim_start().len();
            let n = if tail[ws..].starts_with('(') {
                match paren_body(up, after + ws) {
                    Some(body) => top_level_items(body),
                    None => continue,
                }
            } else {
                let end = tail.find(';').unwrap_or(tail.len());
                top_level_items(&tail[..end])
            };
            best = Some(best.map_or(n, |b| b.max(n)));
        }
        best
    }

    /// Names of the query hints in `/*+ NAME(args), NAME2 */` comments, upper-cased, in
    /// order. Ordinary comments, string literals and their look-alikes give nothing.
    pub fn hints(&self) -> Vec<String> {
        let mut out = Vec::new();
        for &(s, e) in &self.hint_spans {
            let mut depth = 0i32;
            let mut word = String::new();
            let flush = |w: &mut String, out: &mut Vec<String>| {
                if !w.is_empty() && !w.starts_with(|c: char| c.is_ascii_digit()) {
                    out.push(w.to_ascii_uppercase());
                }
                w.clear();
            };
            for ch in self.raw[s..e].chars() {
                match ch {
                    '(' => {
                        if depth == 0 {
                            flush(&mut word, &mut out);
                        }
                        depth += 1;
                    }
                    ')' => depth -= 1,
                    c if depth == 0 && (c.is_alphanumeric() || c == '_') => word.push(c),
                    _ if depth == 0 => flush(&mut word, &mut out),
                    _ => {}
                }
            }
            if depth == 0 {
                flush(&mut word, &mut out);
            }
        }
        out
    }

    /// `SELECT * FROM <table>` (not from a subquery or a CTE).
    ///
    /// Read on whitespace-separated words, not on the clause parse: `SELECT n.*,x`
    /// (no space) is not a star select here and `SELECT * except(a)` is one; the
    /// clause-based answer differs on such texts in the dogfood corpus.
    pub fn select_star(&self) -> bool {
        let toks = self.words();
        // `WITH cte AS (...) SELECT * FROM cte` / `SELECT * FROM (subquery)` select
        // from an already projected relation.
        if toks.first() == Some(&"WITH") {
            return false;
        }
        // A select list starting with `*` or `alias.*` (also `*, extra`, `DISTINCT *`);
        // `* EXCEPT (..)` prunes, so it is not a star select.
        let star = |w: &str| {
            let w = w.trim_end_matches(',');
            w == "*" || w.ends_with(".*")
        };
        toks.iter().enumerate().any(|(i, w)| {
            if !w.ends_with("SELECT") {
                return false;
            }
            let mut j = i + 1;
            if matches!(toks.get(j), Some(&("DISTINCT" | "ALL"))) {
                j += 1;
            }
            if !toks.get(j).is_some_and(|w| star(w))
                || matches!(toks.get(j + 1), Some(&("EXCEPT" | "REPLACE")))
            {
                return false;
            }
            let Some(k) = toks[j..].iter().position(|w| *w == "FROM").map(|k| k + j) else {
                return false;
            };
            // `SELECT * FROM (subquery)` reads an already projected relation.
            !toks.get(k + 1).is_some_and(|w| w.starts_with('('))
        })
    }

    /// Whether the query shuffles or windows: a join, aggregation (`GROUP BY`),
    /// `DISTINCT`, a window (`OVER`, `WINDOW` clause), `UNION` or `ORDER BY` (PRF006
    /// lineage cost). Read from the tokens and clauses: `INSERT OVERWRITE`, names
    /// such as `distinct_count` and `IS DISTINCT FROM` are none of these.
    pub fn is_expensive(&self) -> bool {
        let c = self.clauses();
        let words =
            |w: &'static str| (0..self.toks.len()).filter(move |&i| self.word_at(i) == Some(w));
        let joins = words("JOIN").next().is_some() || words("UNION").next().is_some();
        // `a IS [NOT] DISTINCT FROM b` compares.
        let distinct = words("DISTINCT").any(|i| {
            let before = |k: usize| i.checked_sub(k).and_then(|j| self.word_at(j));
            !(before(1) == Some("IS") || before(1) == Some("NOT") && before(2) == Some("IS"))
        });
        // `f(..) OVER (..)` or `OVER w`.
        let window = !c.windows.is_empty()
            || words("OVER").any(|i| self.toks.get(i + 1).is_some_and(|t| t.kind == Kind::Word));
        let clause = c.queries.iter().any(|q| {
            q.clauses.iter().any(|cl| {
                matches!(
                    cl.kind,
                    ClauseKind::GroupBy | ClauseKind::OrderBy | ClauseKind::Window
                )
            })
        });
        joins || distinct || window || clause
    }

    /// Classify a SQL boolean condition (`F.expr("a.k = b.k")`), read as one
    /// AND / OR / NOT tree of comparisons: `Equi` when some `=` / `==` / `<=>`
    /// compares two non-literal sides (also inside an OR or NOT); an equality
    /// against a literal (`c.region = 'eu'`) is a filter, not a key. `NonEqui` when
    /// the comparisons are `<`, `>`, `<=`, `>=`, `!=` / `<>`, `BETWEEN` or `LIKE`.
    /// Comparisons inside a call's arguments are not the condition's.
    pub fn cond(&self) -> SqlCond {
        let cmps = self.clauses().whole.comparisons();
        if cmps.iter().any(|c| c.key_equality()) {
            SqlCond::Equi
        } else if cmps.iter().any(|c| c.non_equi()) {
            SqlCond::NonEqui
        } else {
            SqlCond::Unknown
        }
    }

    /// Top-level conjuncts of a SQL join condition. OR binds looser than AND, so a
    /// top-level OR makes the whole text one disjunction; equalities against a
    /// literal (`c.region = 'eu'`) are filters, not keys.
    pub fn conjuncts(&self) -> Vec<SqlConjunct> {
        let mut out = Vec::new();
        self.push_conjuncts(&self.clauses().whole, &mut out);
        out
    }

    fn push_conjuncts(&self, p: &Pred, out: &mut Vec<SqlConjunct>) {
        match &p.kind {
            PredKind::Or(branches) => out.push(if self.common_or_key(branches) {
                SqlConjunct::Equi
            } else if self.has_key_equality(p) {
                SqlConjunct::OrWithEq
            } else {
                SqlConjunct::Other
            }),
            PredKind::And(xs) => xs.iter().for_each(|x| self.push_conjuncts(x, out)),
            _ => out.push(if self.has_key_equality(p) {
                SqlConjunct::Equi
            } else {
                SqlConjunct::Other
            }),
        }
    }

    /// Some atom of `p` is an equality of two non-literal sides.
    fn has_key_equality(&self, p: &Pred) -> bool {
        p.comparisons().iter().any(|c| c.key_equality())
    }

    /// A key equality `x = y` / `x <=> y` as one comparable key: spaces dropped,
    /// sides sorted (Catalyst compares `a = b` and `b = a` as the same expression).
    /// `None` for anything else, and for atoms holding quoted text (two different
    /// backticked names look alike in the masked code).
    fn eq_key(&self, p: &Pred) -> Option<String> {
        let PredKind::Atom(Some(c)) = &p.kind else {
            return None;
        };
        if !c.key_equality()
            || self.toks[p.range.clone()]
                .iter()
                .any(|t| matches!(t.kind, Kind::Str | Kind::Ident))
        {
            return None;
        }
        let side = |r: &Range<usize>| -> String {
            self.toks[r.clone()].iter().map(|t| self.upper(t)).collect()
        };
        let (l, r) = (side(&c.left), side(&c.right));
        let op = if c.kind == clauses::CmpOp::NullSafeEq {
            "<=>"
        } else {
            "="
        };
        let (a, b) = if l <= r { (l, r) } else { (r, l) };
        Some(format!("{a}{op}{b}"))
    }

    /// A key equality ANDed into every branch of a disjunction: Catalyst's
    /// BooleanSimplification factors `(k AND a) OR (k AND b)` into `k AND (a OR b)`,
    /// so such an OR still gives the join a key.
    fn common_or_key(&self, branches: &[Pred]) -> bool {
        let keys = |b: &Pred| -> Vec<String> {
            b.conjuncts()
                .iter()
                .filter_map(|x| self.eq_key(x))
                .collect()
        };
        let mut it = branches.iter();
        let Some(first) = it.next() else {
            return false;
        };
        let mut common = keys(first);
        for b in it {
            let k = keys(b);
            common.retain(|x| k.contains(x));
        }
        !common.is_empty()
    }
}

// What a statement reads (loop verdicts: is a table named per item?).
impl SqlText {
    /// Tables and paths the statement reads: the relation
    /// names after `FROM` / `JOIN` (and `USING` in a `MERGE`), comma-joined
    /// relations included, in order. Names are unquoted (backticks removed) and
    /// lower-cased; `{}` stands for an f-string hole. `delta.`/`parquet.`… path
    /// relations (`` delta.`/mnt/x` ``) yield the path as written. Subqueries, CTE
    /// names, table-valued functions (`range(10)`, `read_files(..)`) and `FROM`
    /// inside function calls (`EXTRACT(year FROM d)`) are skipped; the target of a
    /// `DELETE FROM` is a write, not a read.
    pub fn source_tables(&self) -> Vec<String> {
        let ts = self.statement_toks();
        let ctes = self.cte_names();
        let kw = self.keyword();
        let mut out: Vec<String> = Vec::new();
        // Paren stack: whether each open paren holds a query.
        let mut stack: Vec<bool> = Vec::new();
        let mut first_from = true;
        let mut i = 0;
        while i < ts.len() {
            let t = &ts[i];
            if self.is_punct(t, "(") {
                let query = ts.get(i + 1).is_some_and(|n| {
                    n.kind == Kind::Word && matches!(self.upper(n), "SELECT" | "WITH" | "FROM")
                });
                stack.push(query);
                i += 1;
                continue;
            }
            if self.is_punct(t, ")") {
                stack.pop();
                i += 1;
                continue;
            }
            let in_query = stack.last().is_none_or(|q| *q);
            let word = (t.kind == Kind::Word).then(|| self.upper(t));
            let starts = match word {
                Some("FROM") => {
                    let skip = first_from && kw == Some("DELETE");
                    first_from = false;
                    !skip
                }
                Some("JOIN") => true,
                Some("USING") => kw == Some("MERGE") && stack.is_empty(),
                _ => false,
            };
            if !(starts && in_query) {
                i += 1;
                continue;
            }
            i += 1;
            // One or more comma-separated relations.
            loop {
                // A subquery: the walk goes on inside it.
                if ts.get(i).is_some_and(|p| self.is_punct(p, "(")) {
                    break;
                }
                let Some((name, next)) = self.relation_at(ts, i, true) else {
                    break;
                };
                i = next;
                if let Some(n) = name
                    && !ctes.contains(&n)
                {
                    out.push(n);
                }
                // Skip an alias (`t AS x`, `t x`), then continue on a top-level comma.
                while let Some(a) = ts.get(i) {
                    let alias = a.kind == Kind::Ident
                        || (a.kind == Kind::Word
                            && (self.upper(a) == "AS" || !is_clause_word(self.upper(a))));
                    if !alias {
                        break;
                    }
                    i += 1;
                }
                if ts.get(i).is_some_and(|c| self.is_punct(c, ",")) {
                    i += 1;
                    continue;
                }
                break;
            }
        }
        out
    }

    /// Names defined by `WITH name AS (..)` (upper-cased words lower-cased).
    fn cte_names(&self) -> Vec<String> {
        let ts = self.statement_toks();
        let mut out = Vec::new();
        for i in 0..ts.len() {
            let (Some(prev), Some(as_), Some(open)) = (
                i.checked_sub(1).and_then(|p| ts.get(p)),
                ts.get(i + 1),
                ts.get(i + 2),
            ) else {
                continue;
            };
            let prev_ok = (prev.kind == Kind::Word
                && matches!(self.upper(prev), "WITH" | "RECURSIVE"))
                || self.is_punct(prev, ",");
            if prev_ok
                && matches!(ts[i].kind, Kind::Word | Kind::Ident)
                && as_.kind == Kind::Word
                && self.upper(as_) == "AS"
                && self.is_punct(open, "(")
            {
                out.push(self.ident_text(&ts[i]).to_ascii_lowercase());
            }
        }
        out
    }

    /// Text of one word / quoted identifier token, backticks removed.
    fn ident_text(&self, t: &Tok) -> String {
        let s = &self.raw[t.start..t.end];
        if t.kind == Kind::Ident {
            s.trim_matches('`').to_string()
        } else {
            s.to_string()
        }
    }

    /// A relation name starting at token `i`: adjacent words, quoted identifiers,
    /// dots and `{}` holes. Returns `(Some(name), next)`, `(None, next)` for a
    /// subquery or table-valued function (skipped), or `None` when no relation
    /// starts there.
    /// `tvf`: a `(` after the name opens table-function arguments (reads), not a
    /// column list (`INSERT INTO t (a, b)`).
    fn relation_at(&self, ts: &[Tok], i: usize, tvf: bool) -> Option<(Option<String>, usize)> {
        let first = ts.get(i)?;
        if self.is_punct(first, "(") {
            // Subquery: skip to its closing paren.
            let mut depth = 0i32;
            let mut j = i;
            while j < ts.len() {
                if self.is_punct(&ts[j], "(") {
                    depth += 1;
                } else if self.is_punct(&ts[j], ")") {
                    depth -= 1;
                    if depth == 0 {
                        return Some((None, j + 1));
                    }
                }
                j += 1;
            }
            return Some((None, j));
        }
        let part = |t: &Tok| {
            matches!(t.kind, Kind::Word | Kind::Ident)
                || self.is_punct(t, ".")
                || self.is_punct(t, "{")
                || self.is_punct(t, "}")
        };
        if !part(first) || (first.kind == Kind::Word && is_clause_word(self.upper(first))) {
            return None;
        }
        let mut j = i + 1;
        while j < ts.len() && ts[j].start == ts[j - 1].end && part(&ts[j]) {
            j += 1;
        }
        let toks = &ts[i..j];
        if tvf && ts.get(j).is_some_and(|t| self.is_punct(t, "(")) {
            // A table-valued function: skip its arguments.
            let (_, next) = self.relation_at(ts, j, true)?;
            return Some((None, next));
        }
        // `delta.`/path`` → the path.
        if let [fmt, dot, path] = toks
            && fmt.kind == Kind::Word
            && self.is_punct(dot, ".")
            && path.kind == Kind::Ident
        {
            let p = self.ident_text(path);
            if p.contains('/') || p.contains(':') {
                return Some((Some(p), j));
            }
        }
        let name: String = toks.iter().map(|t| self.ident_text(t)).collect();
        Some((Some(name.to_ascii_lowercase()), j))
    }
}

/// Clause keywords that end a relation name / alias in `FROM a x, b`.
fn is_clause_word(w: &str) -> bool {
    matches!(
        w,
        "WHERE"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "OUTER"
            | "CROSS"
            | "SEMI"
            | "ANTI"
            | "NATURAL"
            | "LATERAL"
            | "ON"
            | "USING"
            | "GROUP"
            | "ORDER"
            | "HAVING"
            | "LIMIT"
            | "UNION"
            | "EXCEPT"
            | "MINUS"
            | "INTERSECT"
            | "WINDOW"
            | "SELECT"
            | "FROM"
            | "WHEN"
            | "SET"
            | "VALUES"
            | "PARTITION"
            | "REPLACE"
            | "CLUSTER"
            | "DISTRIBUTE"
            | "SORT"
            | "QUALIFY"
            | "PIVOT"
            | "UNPIVOT"
            | "TABLESAMPLE"
            | "VERSION"
            | "TIMESTAMP"
            | "FOR"
            | "AS"
            | "WITH"
            | "OPTIONS"
            | "TBLPROPERTIES"
    )
}

/// One `GROUP BY` clause's CUBE / ROLLUP groupings (see `SqlText::cube_groupings`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CubeGrouping {
    /// The widest single grouping is a CUBE (a CUBE beats any ROLLUP).
    pub is_cube: bool,
    /// Columns of that widest grouping.
    pub columns: usize,
    /// Grouping sets of the whole clause: the product over its CUBE / ROLLUP /
    /// GROUPING SETS items (saturating).
    pub sets: u128,
    /// Number of CUBE / ROLLUP / GROUPING SETS items in the clause.
    pub parts: usize,
}

/// A comparison with a `NULL` literal found by [`SqlText::null_comparisons`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullComparison {
    /// The operator as written (`=`, `==`, `!=`, `<>`).
    pub op: String,
    /// `!=` / `<>`.
    pub negated: bool,
}

/// A running array aggregate found by [`SqlText::running_collects`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningCollect {
    /// Function name, lower-cased (`collect_list`).
    pub name: String,
    /// The window has `PARTITION BY` / `DISTRIBUTE BY`.
    pub partitioned: bool,
}

/// A window frame that reaches back at least this many rows from the current row
/// covers the partition start in practice: it is read as a running frame.
pub const RUNNING_FRAME_OFFSET: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrameBound {
    /// `UNBOUNDED PRECEDING` or `n PRECEDING` with n >= [`RUNNING_FRAME_OFFSET`].
    FromStart,
    /// `UNBOUNDED FOLLOWING`.
    ToEnd,
    /// `CURRENT ROW`, `n PRECEDING`, `n FOLLOWING`.
    Finite,
}

/// One frame bound at `w[i..]` → (bound, words used).
fn frame_bound_at(w: &[&str], i: usize) -> Option<(FrameBound, usize)> {
    match (w.get(i).copied()?, w.get(i + 1).copied()?) {
        ("UNBOUNDED", "PRECEDING") => Some((FrameBound::FromStart, 2)),
        ("UNBOUNDED", "FOLLOWING") => Some((FrameBound::ToEnd, 2)),
        ("CURRENT", "ROW") => Some((FrameBound::Finite, 2)),
        (n, dir @ ("PRECEDING" | "FOLLOWING")) => {
            let n: u64 = n.parse().ok()?;
            let back = dir == "PRECEDING" && n >= RUNNING_FRAME_OFFSET;
            Some((
                if back {
                    FrameBound::FromStart
                } else {
                    FrameBound::Finite
                },
                2,
            ))
        }
        _ => None,
    }
}

/// Grouping sets of one `CUBE(n)` (2^n) or `ROLLUP(n)` (n + 1), saturating.
pub fn grouping_sets(is_cube: bool, n: usize) -> u128 {
    if is_cube {
        u32::try_from(n)
            .ok()
            .and_then(|n| 1u128.checked_shl(n))
            .filter(|_| n < 128)
            .unwrap_or(u128::MAX)
    } else {
        n as u128 + 1
    }
}

/// Whether `w` occurs at byte `i` of `up` as a whole word.
fn is_word_at(up: &str, i: usize, w: &str) -> bool {
    let b = up.as_bytes();
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    up.get(i..).is_some_and(|r| r.starts_with(w))
        && (i == 0 || !ident(b[i - 1]))
        && b.get(i + w.len()).is_none_or(|c| !ident(*c))
}

/// SQL aggregate functions: one value per group.
const SQL_AGGREGATES: &[&str] = &[
    "COUNT",
    "MAX",
    "MIN",
    "SUM",
    "AVG",
    "MEAN",
    "ANY_VALUE",
    "FIRST",
    "LAST",
    "COUNT_IF",
    "BOOL_AND",
    "BOOL_OR",
    "EVERY",
    "MAX_BY",
    "MIN_BY",
    "APPROX_COUNT_DISTINCT",
    "COLLECT_SET",
    "COLLECT_LIST",
    "ARRAY_AGG",
    "PERCENTILE",
    "PERCENTILE_APPROX",
    "APPROX_PERCENTILE",
    "MEDIAN",
    "MODE",
    "STDDEV",
    "STDDEV_SAMP",
    "STDDEV_POP",
    "VARIANCE",
    "VAR_SAMP",
    "VAR_POP",
    "CORR",
    "COVAR_POP",
    "COVAR_SAMP",
    "SKEWNESS",
    "KURTOSIS",
];

/// Whether one select-list item of the upper-cased code view starts with an aggregate
/// call (`MAX(x)`, `COUNT(DISTINCT k) AS n`).
fn is_aggregate_item(it: &str) -> bool {
    let it = it.trim().trim_start_matches("DISTINCT ").trim();
    SQL_AGGREGATES
        .iter()
        .any(|a| is_word_at(it, 0, a) && it[a.len()..].trim_start().starts_with('('))
}

fn split_top_commas(s: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let (mut d, mut start) = (0i32, 0usize);
    for (j, c) in s.char_indices() {
        match c {
            '(' => d += 1,
            ')' => d -= 1,
            ',' if d == 0 => {
                items.push(&s[start..j]);
                start = j + 1;
            }
            _ => {}
        }
    }
    items.push(&s[start..]);
    items
}

/// Number of top-level comma-separated items in `s` (quotes and parentheses aware).
fn top_level_items(s: &str) -> usize {
    let (mut depth, mut items, mut quote, mut any) = (0i32, 0usize, None::<char>, false);
    for ch in s.chars() {
        if let Some(q) = quote {
            if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' | '`' => {
                quote = Some(ch);
                any = true;
            }
            '(' => {
                depth += 1;
                any = true;
            }
            ')' => depth -= 1,
            ',' if depth == 0 => {
                if any {
                    items += 1;
                }
                any = false;
            }
            c if !c.is_whitespace() => any = true,
            _ => {}
        }
    }
    items + usize::from(any)
}

/// Text inside the parentheses that open at byte `open` of `s` (`s[open] == '('`).
fn paren_body(s: &str, open: usize) -> Option<&str> {
    let mut depth = 0i32;
    let mut quote = None::<char>;
    for (i, ch) in s[open..].char_indices() {
        if let Some(q) = quote {
            if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&s[open + 1..open + i]);
                }
            }
            _ => {}
        }
    }
    None
}

impl SemanticModel<'_> {
    /// Static text of a string literal / f-string (holes become `{}`), or of a name
    /// whose every reaching binding holds the same such text ([`Self::sql_text`]).
    pub fn resolve_text(&self, e: &Expr) -> Option<String> {
        self.sql_text(e).map(|(t, _)| t)
    }

    /// The statement of a `sql(sqlQuery, ...)` call, resolved (see
    /// [`Self::resolve_text`]), `EXPLAIN` included. The caller checks the receiver.
    /// Only the result's shape (row bound) is read from it; rules about what the
    /// query RUNS use [`Self::sql_query`].
    pub fn sql_statement(&self, c: &ast::ExprCall) -> Option<SqlText> {
        let q = call_arg(c, 0, "sqlQuery")?;
        self.resolve_text(q).map(|t| SqlText::new(&t))
    }

    /// The query a `sql(sqlQuery, ...)` call executes ([`Self::sql_statement`]);
    /// `None` for an `EXPLAIN`, which only plans its query (nothing is scanned,
    /// sorted or written). The caller checks the receiver.
    pub fn sql_query(&self, c: &ast::ExprCall) -> Option<SqlText> {
        self.sql_statement(c).filter(|q| !q.explain())
    }

    /// `spark.sql(...)` on a session: its query.
    pub fn spark_sql(&self, expr: &Expr) -> Option<SqlText> {
        let sc = self.spark_call_named(expr, &["sql"])?;
        if !sc.class().is_session() {
            return None;
        }
        self.sql_query(sc.call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(s: &str) -> SqlText {
        SqlText::new(s)
    }

    #[test]
    fn masking() {
        let t = q("select 'a -- b' /* c */, `x y` -- d\nfrom t");
        assert_eq!(t.code().len(), t.raw().len());
        assert_eq!(t.code(), "SELECT '      '        , `   `     \nFROM T");
        assert_eq!(q("SELECT 'é' FROM t").code(), "SELECT '  ' FROM T");
        assert_eq!(q("x = 'it\\'s' AND y").code(), "X = '     ' AND Y");
        assert_eq!(q("'open").code(), "'    ");
        assert_eq!(q("/* only").code(), "       ");
    }

    #[test]
    fn identifiers() {
        let cases: &[(&str, &[&str])] = &[
            ("upper(i.name) = 'x'", &["upper", "i", "name"]),
            ("a + 1 > b2 -- c\n", &["a", "b2"]),
            (
                "`my col` = \"lit\" AND `a``b`.c",
                &["my col", "AND", "a`b", "c"],
            ),
            ("`open", &["open"]),
            ("{} > x", &["x"]),
            ("", &[]),
        ];
        for (s, want) in cases {
            assert_eq!(q(s).identifiers(), *want, "{s}");
        }
    }

    #[test]
    fn keywords_and_calls() {
        let t = q("  -- note\n select a from t");
        assert_eq!(t.keyword(), Some("SELECT"));
        assert_eq!(q("'x'").keyword(), None);
        assert!(q("SELECT a FROM t JOIN u").has_keyword("join"));
        assert!(!q("SELECT 'a join b' FROM t -- join\n").has_keyword("JOIN"));
        assert!(!q("SELECT joined FROM t").has_keyword("JOIN"));
        assert!(q("SELECT input_file_name () FROM t").calls("input_file_name"));
        assert!(q("my_udf(x) > 1").calls("MY_UDF"));
        assert!(!q("note = 'my_udf(x)'").calls("my_udf"));
        assert!(!q("not_my_udf(x)").calls("my_udf"));
        assert!(!q("my_udf_col > 1").calls("my_udf"));
        let t = q("  show tables in sales; ");
        assert_eq!(t.words(), ["SHOW", "TABLES", "IN", "SALES"]);
        let t = q("SHOW TABLES IN `my db`.x -- c\n;");
        assert_eq!(t.words(), ["SHOW", "TABLES", "IN", "`     `.X"]);
        let t = q("SELECT * FROM (SELECT 1)");
        assert_eq!(t.words(), ["SELECT", "*", "FROM", "(SELECT", "1)"]);
    }

    #[test]
    fn set_pairs() {
        let s = |x: &str| q(x).set_pair().map(|p| (p.key, p.value));
        let some = |k: &str, v: &str| Some((k.to_string(), Some(v.to_string())));
        assert_eq!(
            s("SET spark.sql.adaptive.enabled = false"),
            some("spark.sql.adaptive.enabled", "false")
        );
        assert_eq!(
            s("set spark.sql.autoBroadcastJoinThreshold=-1;"),
            some("spark.sql.autoBroadcastJoinThreshold", "-1")
        );
        assert_eq!(s("SET k = 'x'"), some("k", "x"));
        assert_eq!(s("SET `k.x` = \"10m\" -- why\n"), some("k.x", "10m"));
        assert_eq!(s("/* c */ SET k=v"), some("k", "v"));
        assert_eq!(s("SET k = {}"), Some(("k".into(), None)));
        assert_eq!(s("SELECT 1"), None);
        assert_eq!(s("SET"), None);
        assert_eq!(s("SET -v"), None);
        assert_eq!(s("SET spark.executor.memory"), None);
        assert_eq!(s("SET {} = false"), None);
        assert_eq!(s("SET VAR v = 1"), None);
        assert_eq!(s("SELECT 'SET k = v'"), None);
    }

    #[test]
    fn property_lists() {
        let p = |x: &str, kw: &str| q(x).properties(kw);
        let kv = |k: &str, v: &str| (k.to_string(), v.to_string());
        assert_eq!(
            p(
                "ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false')",
                "TBLPROPERTIES"
            ),
            vec![kv("delta.autoOptimize.optimizeWrite", "false")]
        );
        assert_eq!(
            p(
                "CREATE TABLE t (a INT) USING DELTA tblproperties (\n  delta.autoOptimize.autoCompact = false, 'x'='FALSE')",
                "TBLPROPERTIES"
            ),
            vec![
                kv("delta.autoOptimize.autoCompact", "false"),
                kv("x", "FALSE")
            ]
        );
        assert_eq!(
            p(
                "CACHE TABLE t OPTIONS ('storageLevel' 'MEMORY_AND_DISK_2')",
                "OPTIONS"
            ),
            vec![kv("storageLevel", "MEMORY_AND_DISK_2")]
        );
        assert!(p("SHOW TBLPROPERTIES t", "TBLPROPERTIES").is_empty());
        assert!(p("SELECT 'TBLPROPERTIES (a = false)' FROM t", "TBLPROPERTIES").is_empty());
    }

    #[test]
    fn trailing_limit() {
        let l = |s: &str| q(s).trailing_limit();
        assert_eq!(l("SELECT 1 FROM t LIMIT 1"), Some(1));
        assert_eq!(l("select * from t\n limit 10 ;\n"), Some(10));
        assert_eq!(l("SELECT * FROM t LIMIT 10 -- first ten\n"), Some(10));
        assert_eq!(l("SELECT * FROM (SELECT * FROM t LIMIT 5) s"), None);
        assert_eq!(l("SELECT * FROM t"), None);
        // f-string placeholder count: a bound of unknown size.
        assert_eq!(l("SELECT * FROM t LIMIT {}"), Some(u64::MAX));
        assert_eq!(l("SELECT * FROM t\nLIMIT {} ;"), Some(u64::MAX));
        assert_eq!(l("SELECT * FROM (SELECT * FROM t LIMIT {}) s"), None);
        assert_eq!(l("SELECT * FROM t WHERE x = {}"), None);
        assert_eq!(l("SELECT * FROM t LIMIT '{}'"), None);
        assert_eq!(l("SELECT * FROM t -- LIMIT 1"), None);
        assert_eq!(l("SELECT * FROM t WHERE x = 'LIMIT 1'"), None);
    }

    #[test]
    fn restricts_rows() {
        let r = |s: &str| q(s).restricts_rows();
        assert!(r("(select * from orders where\n x > 1) t"));
        assert!(r("(SELECT * FROM t LIMIT 5) q"));
        assert!(r("(SELECT TOP 10 * FROM t) q"));
        assert!(r("(select distinct top 10 id from t) q"));
        assert!(r("SELECT * FROM t ORDER BY id FETCH FIRST 10 ROWS ONLY"));
        assert!(r("select * from t\twhere id = 1"));
        assert!(!r("orders"));
        assert!(!r("where_table"));
        assert!(!r("(select * from t where_x) q"));
        assert!(!r("(select * from t -- where\n) q"));
        assert!(!r("(select * from t where_col, note) q"));
        assert!(!r("(select note from t) q /* limit 1 */"));
        assert!(!r("(select ' limit 1 ' from t) q"));
        assert!(!r("(select top from t) q"));
        assert!(!r("select * from orders"));
    }

    #[test]
    fn single_row_queries() {
        let s = |x: &str| q(x).single_row();
        assert!(s("SELECT max(usage_date) FROM cat.sch.t"));
        assert!(s("select count(*) as n, max(x) from t where y > 1"));
        assert!(s("SELECT current_catalog()"));
        assert!(s("DESCRIBE DETAIL `a`.`b`"));
        assert!(s("-- latest\nSELECT max(d) FROM t"));
        assert!(!s("SELECT max(x) FROM t GROUP BY k"));
        assert!(!s("SELECT k, max(x) FROM t"));
        assert!(!s("SELECT * FROM t"));
        assert!(!s(
            "SELECT count(*) FROM a JOIN b ON a.k = b.k GROUP BY a.k"
        ));
        assert!(!s("DESCRIBE HISTORY t"));
        assert!(!s("SELECT maximum(x) FROM t"));
        assert!(s("SELECT var_samp(x), stddev_pop(y) FROM t"));
        assert!(!s("SELECT max(x) OVER (PARTITION BY k) FROM t"));
        assert!(s("explain cost select * from view"));
        assert!(s("EXPLAIN FORMATTED SELECT * FROM big ORDER BY rand()"));
    }

    #[test]
    fn aggregates_table_queries() {
        let a = |x: &str| q(x).aggregates_table();
        assert!(a("SELECT max(usage_date) FROM cat.sch.t"));
        assert!(a("select count(*) as n from t where y > 1 limit 1"));
        assert!(!a("SELECT current_catalog()"));
        assert!(!a("DESCRIBE DETAIL `a`.`b`"));
        assert!(!a("EXPLAIN SELECT count(*) FROM t"));
        assert!(!a("SELECT * FROM t LIMIT 10"));
        assert!(!a("SELECT max(x) FROM t GROUP BY k"));
    }

    #[test]
    fn explain_statements() {
        let e = |x: &str| q(x).explain();
        assert!(e("EXPLAIN SELECT * FROM t"));
        assert!(e("explain extended select a from t"));
        assert!(e("-- plan\n EXPLAIN CODEGEN SELECT 1;"));
        assert!(!e("SELECT 'EXPLAIN' FROM t"));
        assert!(!e("SELECT explain FROM t"));
        assert!(!e("DESCRIBE QUERY SELECT * FROM t"));
    }

    #[test]
    fn aggregate_select_expressions() {
        let a = |x: &str| q(x).aggregate_expression();
        assert!(a("min(post_date) as min_post_date"));
        assert!(a("variance(label-prediction)"));
        assert!(a("COUNT(DISTINCT k) n"));
        assert!(a("max(order_id)"));
        assert!(a("max({})"));
        assert!(!a("max(x) over (partition by k)"));
        assert!(!a("min(a), b"));
        assert!(!a("k"));
        assert!(!a("maximum(x)"));
        assert!(!a("'max(x)'"));
        assert!(!a(""));
    }

    #[test]
    fn catalog_metadata_commands() {
        let m = |x: &str| q(x).catalog_metadata();
        assert!(m("DESCRIBE TABLE EXTENDED cat.sch.t"));
        assert!(m("describe `a`.`b`"));
        assert!(m("DESC FORMATTED t"));
        assert!(m("SHOW TBLPROPERTIES t"));
        assert!(m("show tables in sales like 'x*'"));
        assert!(m("SHOW CATALOGS"));
        assert!(m("SHOW COLUMNS IN t"));
        assert!(m("-- props\nSHOW TBLPROPERTIES t;"));
        assert!(!m("SHOW PARTITIONS t"));
        assert!(!m("DESCRIBE HISTORY t"));
        assert!(!m("DESCRIBE QUERY SELECT * FROM t"));
        assert!(!m("SELECT * FROM t"));
        assert!(!m("SELECT 'SHOW TABLES' FROM t"));
        assert!(!m("SHOW"));
    }

    #[test]
    fn not_in_subquery() {
        let f = |s: &str| q(s).not_in_subquery();
        assert!(f("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)"));
        assert!(f("select * from a where id not in\n  ( select id from b)"));
        assert!(f(
            "SELECT * FROM a WHERE (x, y) NOT IN ((SELECT x, y FROM b))"
        ));
        assert!(f(
            "SELECT * FROM a WHERE k NOT IN (WITH c AS (SELECT k FROM b) SELECT k FROM c)"
        ));
        assert!(!f("SELECT * FROM a WHERE id NOT IN (1, 2, 3)"));
        assert!(!f("SELECT * FROM a WHERE id IN (SELECT id FROM b)"));
        assert!(!f(
            "SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id)"
        ));
        assert!(!f("SELECT 'x NOT IN (SELECT y)' AS note FROM t"));
        assert!(!f("SELECT a FROM t -- id NOT IN (SELECT id FROM b)\n"));
        assert!(!f("SELECT a /* NOT IN (SELECT 1) */ FROM t"));
        assert!(!f("SELECT cannot_in FROM t WHERE knot IN (SELECT 1)"));
        assert!(!f("SELECT * FROM a WHERE id NOT IN ('SELECT')"));
        assert!(!f("SELECT 'é' FROM t WHERE x NOT IN (2)"));
        // `NOT (x IN (subquery))` is the same anti join.
        assert!(f("SELECT * FROM a WHERE NOT (id IN (SELECT id FROM b))"));
        assert!(f(
            "select * from a where not ( (a.x, a.y) in ((select x, y from b)) )"
        ));
        assert!(!f("SELECT * FROM a WHERE NOT (id IN (1, 2))"));
        assert!(!f(
            "SELECT * FROM a WHERE NOT (id IN (SELECT id FROM b) AND x > 1)"
        ));
        assert!(!f(
            "SELECT * FROM a WHERE NOT (x > 1 AND id IN (SELECT id FROM b))"
        ));
        assert!(!f("SELECT * FROM a WHERE NOT (EXISTS (SELECT 1 FROM b))"));
    }

    #[test]
    fn distinct_aggregates() {
        let d = |s: &str| q(s).distinct_aggregates();
        assert_eq!(
            d(
                "SELECT k, COUNT(DISTINCT a), count(distinct `B`), SUM(DISTINCT a), approx_count_distinct(c) FROM t GROUP BY k"
            ),
            vec![vec!["a".to_string(), "b".to_string(), "a".to_string()]]
        );
        assert_eq!(d("count(distinct b, a)"), vec![vec!["a,b".to_string()]]);
        assert_eq!(
            d("SELECT count(DISTINCT x) FROM (SELECT count(distinct y), count(distinct z) FROM t)"),
            vec![
                vec!["x".to_string()],
                vec!["y".to_string(), "z".to_string()]
            ]
        );
        assert!(d("SELECT DISTINCT a, b FROM t").is_empty());
        assert!(d("SELECT 'count(distinct a)' FROM t -- count(distinct b)\n").is_empty());
        assert!(d("SELECT count(a) FROM t").is_empty());
        assert_eq!(
            d("SELECT count(distinct lower(a)), count(distinct é) FROM t"),
            vec![vec!["lower(a)".to_string(), "é".to_string()]]
        );
    }

    #[test]
    fn running_collects() {
        let r = |s: &str| {
            q(s).running_collects()
                .into_iter()
                .map(|c| (c.name, c.partitioned))
                .collect::<Vec<_>>()
        };
        let one = |n: &str, p: bool| vec![(n.to_string(), p)];
        // default frame of an ordered window
        assert_eq!(
            r("collect_list(v) over (partition by k order by ts)"),
            one("collect_list", true)
        );
        assert_eq!(
            r("SELECT k, COLLECT_SET(v) OVER (ORDER BY ts) AS s FROM t"),
            one("collect_set", false)
        );
        assert_eq!(
            r(
                "array_agg(v) over (distribute by k sort by ts rows between unbounded preceding and current row)"
            ),
            one("array_agg", true)
        );
        assert_eq!(
            r(
                "collect_list(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"
            ),
            one("collect_list", true)
        );
        assert_eq!(
            r("collect_list(v) over (partition by k order by ts range unbounded preceding)"),
            one("collect_list", true)
        );
        assert_eq!(
            r(
                "collect_list(v) over (partition by k order by ts rows between 5000000 preceding and current row)"
            ),
            one("collect_list", true)
        );
        assert_eq!(
            r("collect_list(v) filter (where v > 0) over (partition by k order by ts)"),
            one("collect_list", true)
        );
        assert_eq!(
            r(
                "select collect_set(a) over (order by x), collect_list(b) over (partition by k order by y) from t"
            ),
            vec![
                ("collect_set".to_string(), false),
                ("collect_list".to_string(), true)
            ]
        );
        // not running
        let none: Vec<(String, bool)> = Vec::new();
        assert_eq!(r("collect_list(v) over (partition by k)"), none);
        assert_eq!(
            r(
                "collect_list(v) over (partition by k order by ts rows between unbounded preceding and unbounded following)"
            ),
            none
        );
        assert_eq!(
            r(
                "collect_list(v) over (partition by k order by ts rows between 10 preceding and current row)"
            ),
            none
        );
        assert_eq!(
            r(
                "collect_list(v) over (partition by k order by ts rows between current row and unbounded following)"
            ),
            none
        );
        assert_eq!(
            r(
                "collect_list(v) over (partition by k order by ts range between interval '7' day preceding and current row)"
            ),
            none
        );
        assert_eq!(r("collect_list(v) over w"), none);
        assert_eq!(
            r("SELECT k, collect_list(v) FROM t GROUP BY k ORDER BY k"),
            none
        );
        assert_eq!(r("select 'collect_list(v) over (order by ts)' as s"), none);
        assert_eq!(r("my_collect_list(v) over (order by ts)"), none);
        assert_eq!(r("sum(v) over (partition by k order by ts)"), none);
    }

    #[test]
    fn cube_groupings() {
        let w = |s: &str| {
            q(s).cube_groupings()
                .into_iter()
                .map(|g| (g.is_cube, g.columns, g.sets, g.parts))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            w("SELECT 1 FROM t GROUP BY CUBE(a, b, c)"),
            vec![(true, 3, 8, 1)]
        );
        assert_eq!(
            w("select 1 from t group by cube ((a, b), f(c, d), 'x,y')"),
            vec![(true, 3, 8, 1)]
        );
        assert_eq!(
            w("SELECT 1 FROM t GROUP BY a, b, c, d WITH ROLLUP"),
            vec![(false, 4, 5, 1)]
        );
        // Mixed groupings multiply: 3 x 4 sets, the widest single one is the CUBE.
        assert_eq!(
            w("SELECT a FROM t GROUP BY a, ROLLUP(b, c), CUBE(d, e)"),
            vec![(true, 2, 12, 2)]
        );
        assert_eq!(
            w("SELECT 1 FROM t GROUP BY CUBE(a, b, c), CUBE(d, e, f)"),
            vec![(true, 3, 64, 2)]
        );
        assert_eq!(
            w("SELECT 1 FROM t GROUP BY CUBE(a, b), GROUPING SETS ((c), (d), ())"),
            vec![(true, 2, 12, 2)]
        );
        // GROUPING SETS alone is the explicit form: nothing to report.
        assert!(w("SELECT 1 FROM t GROUP BY GROUPING SETS ((a, b), (a), ())").is_empty());
        // One entry per GROUP BY clause (a subquery's clause is separate).
        assert_eq!(
            w("SELECT 1 FROM (SELECT a FROM t GROUP BY CUBE(a, b)) GROUP BY ROLLUP(c, d, e)"),
            vec![(true, 2, 4, 1), (false, 3, 4, 1)]
        );
        assert!(w("SELECT cube(a, b) FROM t GROUP BY a").is_empty());
        assert!(w("SELECT * FROM cube WHERE x = 1").is_empty());
        assert_eq!(
            w("SELECT a FROM t GROUP BY a WITH CUBE"),
            vec![(true, 1, 2, 1)]
        );
        assert!(w("SELECT 'GROUP BY CUBE(a, b)' FROM t").is_empty());
        assert_eq!(grouping_sets(true, 200), u128::MAX);
        assert_eq!(grouping_sets(true, 127), 1u128 << 127);
    }

    #[test]
    fn null_comparisons() {
        let f = |s: &str| {
            q(s).null_comparisons()
                .into_iter()
                .map(|n| (n.op, n.negated))
                .collect::<Vec<_>>()
        };
        let eq = || ("=".to_string(), false);
        assert_eq!(f("SELECT * FROM t WHERE a = NULL"), vec![eq()]);
        assert_eq!(f("a = null"), vec![eq()]);
        assert_eq!(f("NULL = a"), vec![eq()]);
        assert_eq!(f("a == NULL"), vec![("==".to_string(), false)]);
        assert_eq!(f("a != NULL"), vec![("!=".to_string(), true)]);
        assert_eq!(f("a<>NULL"), vec![("<>".to_string(), true)]);
        assert_eq!(f("CASE WHEN a = NULL THEN 1 END"), vec![eq()]);
        assert_eq!(
            f("SELECT a FROM t JOIN s ON t.k = s.k AND s.x = NULL"),
            vec![eq()]
        );
        assert_eq!(f("SELECT a = NULL AS flag FROM t"), vec![eq()]);
        assert_eq!(f("NOT (a = NULL)"), vec![eq()]);
        assert_eq!(f("coalesce(a = NULL, false)"), vec![eq()]);
        // The WHERE of an UPDATE compares; its SET assigns.
        assert_eq!(
            f("UPDATE t SET a = NULL, b = NULL WHERE c = NULL"),
            vec![eq()]
        );
        assert_eq!(
            f("UPDATE t SET a = CASE WHEN b = NULL THEN 1 END"),
            vec![eq()]
        );
        assert_eq!(
            f(
                "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED AND s.d = NULL THEN UPDATE SET t.x = NULL"
            ),
            vec![eq()]
        );
        for s in [
            "a <=> NULL",
            "a IS NULL",
            "a IS NOT NULL",
            "a >= NULL AND b <= NULL",
            "SET spark.sql.x = NULL",
            "UPDATE t SET a = NULL",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.x = NULL, t.y = NULL",
            "ALTER TABLE t SET TBLPROPERTIES ('k' = NULL)",
            "CREATE TABLE t USING delta OPTIONS (path = NULL)",
            "f(a => NULL)",
            "SELECT 'a = NULL' FROM t",
            "SELECT a FROM t -- WHERE a = NULL\n",
            "a = NULLIF(b, 0)",
            "a = `NULL`",
        ] {
            assert!(f(s).is_empty(), "{s}");
        }
    }

    #[test]
    fn order_by_rand() {
        let f = |s: &str| q(s).order_by_rand();
        assert!(f("SELECT * FROM t ORDER BY rand() LIMIT 10"));
        assert!(f("select * from t order by\n  RANDOM ( ) limit 5"));
        assert!(f("SELECT * FROM t ORDER BY randn(42), id"));
        assert!(f(
            "SELECT * FROM (SELECT * FROM t ORDER BY rand() LIMIT 10) s"
        ));
        assert!(f(
            "WITH s AS (SELECT * FROM t ORDER BY rand() LIMIT 100) SELECT * FROM s"
        ));
        assert!(!f("SELECT * FROM t ORDER BY id, rand()"));
        assert!(!f(
            "SELECT *, row_number() OVER (PARTITION BY k ORDER BY rand()) rn FROM t"
        ));
        assert!(!f(
            "SELECT *, rank() OVER w FROM t WINDOW w AS (PARTITION BY k ORDER BY rand())"
        ));
        assert!(!f("SELECT * FROM t SORT BY rand()"));
        assert!(!f("SELECT * FROM t DISTRIBUTE BY rand()"));
        assert!(!f("SELECT * FROM t ORDER BY random_score"));
        assert!(!f("SELECT 'ORDER BY rand()' AS note FROM t"));
        assert!(!f("SELECT a FROM t -- ORDER BY rand()\n"));
        assert!(!f("SELECT * FROM t WHERE rand() < 0.1 ORDER BY id"));
        assert!(q(" rand() ").is_random_call());
        assert!(!q("rand_col").is_random_call());
    }

    #[test]
    fn select_star_and_cost() {
        assert!(q("SELECT * FROM t").select_star());
        assert!(q("select *\n from db.t where x = 1").select_star());
        assert!(!q("SELECT * FROM (SELECT a FROM t)").select_star());
        assert!(!q("WITH c AS (SELECT a FROM t) SELECT * FROM c").select_star());
        assert!(!q("SELECT a FROM t").select_star());
        assert!(q("/* all */ SELECT * FROM t").select_star());
        assert!(q("SELECT *, 1 AS one FROM t").select_star());
        assert!(q("SELECT * , x FROM t").select_star());
        assert!(q("SELECT t.* FROM t").select_star());
        assert!(q("SELECT a.*, b.x FROM a JOIN b ON a.k = b.k").select_star());
        assert!(q("SELECT DISTINCT * FROM t").select_star());
        assert!(q("SELECT a FROM t UNION ALL SELECT * FROM u").select_star());
        assert!(!q("SELECT * EXCEPT (b) FROM t").select_star());
        assert!(!q("SELECT count(*) FROM t").select_star());
        assert!(!q("SELECT a, b.* FROM t").select_star());
        assert!(!q("SELECT *").select_star());
        assert!(q("SELECT a FROM t JOIN u ON t.k = u.k").is_expensive());
        assert!(q("SELECT k, count(*) FROM t GROUP BY k").is_expensive());
        assert!(!q("SELECT a FROM t WHERE note = 'x join y'").is_expensive());
        assert!(!q("SELECT a FROM t -- order by a\n").is_expensive());
        // Read from tokens and clauses, not substrings.
        assert!(q("SELECT a, row_number() OVER (ORDER BY a) FROM t").is_expensive());
        assert!(q("SELECT rank() OVER w FROM t WINDOW w AS (PARTITION BY k)").is_expensive());
        assert!(q("SELECT DISTINCT a FROM t").is_expensive());
        assert!(q("SELECT a FROM t UNION SELECT a FROM u").is_expensive());
        assert!(q("SELECT a FROM t\nGROUP\n  BY a").is_expensive());
        assert!(q("SELECT * FROM t ORDER BY a").is_expensive());
        assert!(!q("INSERT OVERWRITE t SELECT * FROM s").is_expensive());
        assert!(!q("SELECT distinct_count, reunion_id, window_start FROM t").is_expensive());
        assert!(!q("SELECT * FROM t WHERE a IS NOT DISTINCT FROM b").is_expensive());
    }

    #[test]
    fn zorder_column_counts() {
        let z = |s: &str| q(s).zorder_columns();
        assert_eq!(z("OPTIMIZE t ZORDER BY (a, b, c, d)"), Some(4));
        assert_eq!(z("optimize t where dt = '1' zorder by (a)"), Some(1));
        assert_eq!(z("OPTIMIZE t ZORDER BY a, b, c"), Some(3));
        assert_eq!(z("OPTIMIZE t ZORDER BY a, b;"), Some(2));
        assert_eq!(z("OPTIMIZE t ZORDER BY (`a,b`, c, d, e, f)"), Some(5));
        assert_eq!(z("OPTIMIZE t ZORDER BY (a, b) -- , c, d, e\n"), Some(2));
        assert_eq!(
            z("OPTIMIZE t ZORDER\n  BY (\n a,\n b,\n c,\n d,\n e\n)"),
            Some(5)
        );
        assert_eq!(z("OPTIMIZE t"), None);
        assert_eq!(z("SELECT 'ZORDER BY (a, b, c, d)'"), None);
        assert_eq!(z("SELECT zorder_by FROM t"), None);
        assert_eq!(z("OPTIMIZE t ZORDER BY (a, b"), None);
    }

    #[test]
    fn hint_names() {
        let h = |s: &str| q(s).hints();
        assert_eq!(
            h("SELECT /*+ SHUFFLE_REPLICATE_NL(b) */ * FROM a JOIN b ON a.k = b.k"),
            vec!["SHUFFLE_REPLICATE_NL"]
        );
        assert_eq!(
            h("select /*+ broadcast(b), shuffle_replicate_nl(c, d) */ 1"),
            vec!["BROADCAST", "SHUFFLE_REPLICATE_NL"]
        );
        assert_eq!(
            h("SELECT /*+ REPARTITION(8, k) COALESCE(2) */ 1"),
            vec!["REPARTITION", "COALESCE"]
        );
        assert!(h("SELECT /* SHUFFLE_REPLICATE_NL(b) */ 1").is_empty());
        assert!(h("SELECT '/*+ SHUFFLE_REPLICATE_NL(b) */' FROM t").is_empty());
        assert!(h("SELECT 1 -- /*+ SHUFFLE_REPLICATE_NL(b) */").is_empty());
        assert!(h("SELECT shuffle_replicate_nl FROM t").is_empty());
        assert_eq!(h("SELECT /*+ MERGE(a) "), vec!["MERGE"]);
    }

    #[test]
    fn partition_repairs() {
        let r = |s: &str| q(s).repairs_partitions();
        assert!(r("MSCK REPAIR TABLE db.events"));
        assert!(r("msck repair table `my db`.`t`;"));
        assert!(r("REPAIR TABLE t SYNC PARTITIONS"));
        assert!(r("REPAIR TABLE t ADD PARTITIONS"));
        assert!(r("  -- refresh\nREPAIR TABLE {}"));
        assert!(r("ALTER TABLE sales RECOVER PARTITIONS"));
        assert!(!r("REPAIR TABLE t SYNC METADATA"));
        assert!(!r(
            "ALTER TABLE t ADD IF NOT EXISTS PARTITION (dt='2024-01-01')"
        ));
        assert!(!r("SELECT 'MSCK REPAIR TABLE t'"));
        assert!(!r("SELECT * FROM msck_repair"));
        assert!(!r("MSCK REPAIR TABLE"));
        assert!(!r("ALTER TABLE t DROP PARTITION (dt='x')"));
    }

    /// One table of condition shapes read through the condition tree: (text, cond,
    /// conjuncts, NULL comparisons). Statements (MERGE WHEN, UPDATE SET) only ask
    /// for NULL comparisons: cond / conjuncts read a condition fragment.
    #[test]
    fn predicate_table() {
        use SqlCond::{Equi, NonEqui, Unknown};
        use SqlConjunct::{Equi as E, OrWithEq as Or, Other as O};
        type Case<'a> = (&'a str, Option<SqlCond>, Option<&'a [SqlConjunct]>, usize);
        let cases: &[Case] = &[
            ("a.ts BETWEEN b.lo AND b.hi", Some(NonEqui), Some(&[O]), 0),
            (
                "a.ts NOT BETWEEN 1 AND 2 AND a.k = b.k",
                Some(Equi),
                Some(&[O, E]),
                0,
            ),
            (
                "(a.k = b.k OR (a.j = b.j AND a.x > 1)) AND c = 'x'",
                Some(Equi),
                Some(&[Or, O]),
                0,
            ),
            ("NOT (a.k = b.k)", Some(Equi), Some(&[E]), 0),
            ("a.k == b.k", Some(Equi), Some(&[E]), 0),
            (
                "a.k = 1 AND b.k = -2.5 AND c = TRUE",
                Some(Unknown),
                Some(&[O, O, O]),
                0,
            ),
            (
                "a.name LIKE b.pattern OR a.x <> b.y",
                Some(NonEqui),
                Some(&[O]),
                0,
            ),
            (
                "a.k IN (SELECT k FROM t) AND a.v IS NOT NULL",
                Some(Unknown),
                Some(&[O, O]),
                0,
            ),
            ("coalesce(a.k = b.k, false)", Some(Unknown), Some(&[O]), 0),
            ("a.x = NULL OR NULL <> b.y", Some(NonEqui), Some(&[O]), 2),
            (
                "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED AND s.d = NULL THEN UPDATE SET t.x = NULL",
                None,
                None,
                1,
            ),
            (
                "UPDATE t SET a = NULL, b = NULL WHERE c = NULL",
                None,
                None,
                1,
            ),
        ];
        for (text, cond, conj, nulls) in cases {
            let t = q(text);
            if let Some(c) = cond {
                assert_eq!(t.cond(), *c, "cond {text}");
            }
            if let Some(c) = conj {
                assert_eq!(t.conjuncts(), *c, "conjuncts {text}");
            }
            assert_eq!(t.null_comparisons().len(), *nulls, "nulls {text}");
        }
    }

    #[test]
    fn join_conditions() {
        assert_eq!(q("a.k = b.k").cond(), SqlCond::Equi);
        assert_eq!(q("a.k <=> b.k").cond(), SqlCond::Equi);
        assert_eq!(q("a.ts BETWEEN b.lo AND b.hi").cond(), SqlCond::NonEqui);
        assert_eq!(q("a.ts >= b.lo").cond(), SqlCond::NonEqui);
        assert_eq!(q("a.name LIKE b.pat").cond(), SqlCond::NonEqui);
        assert_eq!(q("a.ts < b.ts AND a.note != '='").cond(), SqlCond::NonEqui);
        assert_eq!(q("f(a, b)").cond(), SqlCond::Unknown);
        // literal equalities are filters, not keys
        assert_eq!(
            q("ts BETWEEN start AND end AND kind = 'x'").cond(),
            SqlCond::NonEqui
        );
        assert_eq!(q("ts > start AND kind = \"x\"").cond(), SqlCond::NonEqui);
        assert_eq!(q("kind = 'x' AND n = 1").cond(), SqlCond::Unknown);
        assert_eq!(q("a.k = b.k AND kind = 'x'").cond(), SqlCond::Equi);
        assert_eq!(q("a.k = b.k OR a.j = b.j").cond(), SqlCond::Equi);
        assert_eq!(q("a.id != b.id").cond(), SqlCond::NonEqui);
        assert_eq!(q("a.id <> b.id").cond(), SqlCond::NonEqui);
        assert_eq!(q("a.k <=> NULL").cond(), SqlCond::Unknown);
        use SqlConjunct::*;
        let c = |s: &str| q(s).conjuncts();
        assert_eq!(c("a.k = b.k"), vec![Equi]);
        assert_eq!(c("a.k = b.k OR a.j = b.j"), vec![OrWithEq]);
        assert_eq!(
            c("a.x = b.x AND (a.k = b.k OR a.j = b.j)"),
            vec![Equi, OrWithEq]
        );
        // OR binds looser than AND.
        assert_eq!(c("a.x = b.x AND a.y = b.y OR a.j = b.j"), vec![OrWithEq]);
        assert_eq!(c("a.k = b.k OR c.region = 'eu'"), vec![OrWithEq]);
        assert_eq!(c("c.region = 'eu' OR c.region = 'us'"), vec![Other]);
        assert_eq!(
            c("(a.k = b.k or a.j = b.j) and c.flag = true"),
            vec![OrWithEq, Other]
        );
        // BETWEEN keeps its AND: one conjunct.
        assert_eq!(c("a.ts BETWEEN b.lo AND b.hi"), vec![Other]);
        assert_eq!(
            c("a.ts BETWEEN b.lo AND b.hi AND a.k = b.k"),
            vec![Other, Equi]
        );
        // A key equality ANDed into every OR branch is factored out by Catalyst.
        assert_eq!(
            c("(a.k = b.k AND a.x = 1) OR (a.k = b.k AND a.y = 2)"),
            vec![Equi]
        );
        assert_eq!(
            c("a.k = b.k AND a.x > b.x OR b.k = a.k AND a.y IS NULL"),
            vec![Equi]
        );
        assert_eq!(c("(a.k <=> b.k AND a.x = 1) OR (b.k <=> a.k)"), vec![Equi]);
        assert_eq!(
            c("(a.k = b.k AND a.x = 1) OR (a.j = b.j AND a.y = 2) OR (a.k = b.k)"),
            vec![OrWithEq]
        );
        // Not the same key: `=` vs `<=>`, a literal, a range, masked quoted names.
        assert_eq!(
            c("(a.k = b.k AND a.x = 1) OR (a.k <=> b.k)"),
            vec![OrWithEq]
        );
        assert_eq!(
            c("(a.f = 1 AND a.k = b.k) OR (a.f = 1 AND a.j = b.j)"),
            vec![OrWithEq]
        );
        assert_eq!(
            c("(a.k >= b.k AND a.j = b.j) OR (a.k >= b.k)"),
            vec![OrWithEq]
        );
        assert_eq!(
            c("(`x` = `y` AND a.j = b.j) OR (`p` = `q`)"),
            vec![OrWithEq]
        );
    }
}
