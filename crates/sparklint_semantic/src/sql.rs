//! SQL text inside Python: the one reader of `spark.sql(...)` queries and of SQL
//! fragments (`F.expr("...")`, `df.filter("...")`).
//!
//! [`SemanticModel::spark_sql`] / [`SemanticModel::sql_query`] take the query of a
//! `sql()` call once: the argument (positional or `sqlQuery=`), a string literal or
//! f-string (interpolations become `{}`) or a name bound once to one. [`SqlText`]
//! tokenizes it once — comments dropped, string literals and quoted identifiers
//! masked — and answers the questions rules ask: statement keyword, `SET k = v`,
//! `OPTIONS (..)` / `TBLPROPERTIES (..)` key/value lists, keywords and function calls
//! outside strings, a trailing `LIMIT n`, single-row queries, and the plan-shape
//! probes (`NOT IN (SELECT ..)`, `CUBE`/`ROLLUP` width, `ORDER BY rand()`, `SELECT *`,
//! join-condition equalities). It is a tokenizer, not a SQL parser.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::Ranged;

use crate::infer::{call_arg, static_text};
use crate::model::SemanticModel;

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
        }
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

    /// `n` if the query ends in a top-level `LIMIT <n>` (`SELECT 1 FROM t LIMIT 1`).
    pub fn trailing_limit(&self) -> Option<u64> {
        let ts = self.statement_toks();
        let [.., kw, n] = ts else { return None };
        // A LIMIT inside a parenthesised subquery would end in `)`, so a trailing
        // `LIMIT n` pair is the outermost one.
        (n.kind == Kind::Number && kw.kind == Kind::Word && self.upper(kw) == "LIMIT")
            .then(|| self.raw[n.start..n.end].parse().ok())
            .flatten()
    }

    /// Whether the query provably returns at most one row: `DESCRIBE DETAIL`, an
    /// `EXPLAIN` (one row of plan text), a `SELECT` without `FROM`, or a `SELECT` of only aggregates with no `GROUP BY`
    /// (`SELECT max(d) FROM t`).
    pub fn single_row(&self) -> bool {
        let up = self.code.trim().trim_end_matches(';').trim();
        if self.words().starts_with(&["DESCRIBE", "DETAIL"]) || self.explain() {
            return true;
        }
        let Some(rest) = up.strip_prefix("SELECT") else {
            return false;
        };
        if !rest.starts_with(char::is_whitespace) {
            return false;
        }
        let words: Vec<&str> = up.split_whitespace().collect();
        if words.windows(2).any(|w| w == ["GROUP", "BY"])
            || words
                .iter()
                .any(|w| matches!(*w, "UNION" | "JOIN" | "LATERAL" | "EXPLODE"))
        {
            return false;
        }
        // Select list up to the first top-level FROM.
        let mut depth = 0i32;
        let bytes = rest.as_bytes();
        let mut end = rest.len();
        for (i, c) in bytes.iter().enumerate() {
            match c {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'F' if depth == 0 && is_word_at(rest, i, "FROM") => {
                    end = i;
                    break;
                }
                _ => {}
            }
        }
        if end == rest.len() {
            return !self.has_keyword("FROM") && !self.has_keyword("WHERE");
        }
        // A window aggregate (`max(x) OVER (..)`) is one value per row.
        !self.has_keyword("OVER")
            && split_top_commas(&rest[..end])
                .iter()
                .all(|it| is_aggregate_item(it))
    }

    /// Whether the text, read as ONE select-list expression (a `selectExpr` argument),
    /// is a global aggregate: `min(d) AS lo`, `count(*)`, `variance(a - b)`. A window
    /// aggregate (`OVER`) or a list of several expressions is not.
    pub fn aggregate_expression(&self) -> bool {
        let items = split_top_commas(&self.code);
        !self.has_keyword("OVER") && items.len() == 1 && is_aggregate_item(items[0])
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
        let up = &self.code;
        for (i, _) in up.match_indices("NOT") {
            if !is_word_at(up, i, "NOT") {
                continue;
            }
            let rest = up[i + 3..].trim_start();
            if !is_word_at(rest, 0, "IN") {
                continue;
            }
            let mut body = rest[2..].trim_start();
            if !body.starts_with('(') {
                continue;
            }
            while let Some(b) = body.strip_prefix('(') {
                body = b.trim_start();
            }
            if is_word_at(body, 0, "SELECT") || is_word_at(body, 0, "WITH") {
                return true;
            }
        }
        false
    }

    /// Widest `CUBE(..)` / `ROLLUP(..)` / `... WITH CUBE|ROLLUP` grouping:
    /// (is_cube, number of grouping expressions). CUBE wins over ROLLUP.
    pub fn cube_width(&self) -> Option<(bool, usize)> {
        let up = &self.code;
        let mut best: Option<(bool, usize)> = None;
        let mut keep = |is_cube: bool, n: usize| {
            let better = match best {
                None => true,
                Some((bc, bn)) => (is_cube && !bc) || (is_cube == bc && n > bn),
            };
            if better {
                best = Some((is_cube, n));
            }
        };
        for (i, _) in up.char_indices() {
            for (w, is_cube) in [("CUBE", true), ("ROLLUP", false)] {
                if !is_word_at(up, i, w) {
                    continue;
                }
                let after = i + w.len();
                let rest = &up[after..];
                let ws = rest.len() - rest.trim_start().len();
                // `WITH CUBE` / `WITH ROLLUP` after a plain GROUP BY list.
                let before = up[..i].trim_end();
                if before.ends_with("WITH") && is_word_at(up, before.len() - 4, "WITH") {
                    if let Some(start) = group_by_before(up, before.len() - 4) {
                        keep(is_cube, top_level_items(&up[start..before.len() - 4]));
                    }
                    continue;
                }
                if rest[ws..].starts_with('(')
                    && group_by_before(up, i).is_some()
                    && let Some(body) = paren_body(up, after + ws)
                {
                    keep(is_cube, top_level_items(body));
                }
            }
        }
        best
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
        let up = &self.code;
        for (i, _) in up.match_indices("ORDER") {
            if !is_word_at(up, i, "ORDER") {
                continue;
            }
            let rest = up[i + 5..].trim_start();
            if !is_word_at(rest, 0, "BY") {
                continue;
            }
            if random_call_prefix(rest[2..].trim_start()) && !in_window_spec(up, i) {
                return true;
            }
        }
        false
    }

    /// Whether the text starts with a call of `rand(`/`randn(`/`random(` (a sort key).
    pub fn is_random_call(&self) -> bool {
        random_call_prefix(self.code.trim_start())
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
    pub fn select_star(&self) -> bool {
        let toks = self.words();
        // `WITH cte AS (...) SELECT * FROM cte` / `SELECT * FROM (subquery)` select
        // from an already projected relation.
        if toks.first() == Some(&"WITH") {
            return false;
        }
        toks.windows(4).any(|w| {
            w[0].ends_with("SELECT") && w[1] == "*" && w[2] == "FROM" && !w[3].starts_with('(')
        }) || toks
            .windows(3)
            .last()
            .is_some_and(|w| w[0].ends_with("SELECT") && w[1] == "*" && w[2] == "FROM")
    }

    /// Whether the query shuffles or windows: a join, aggregation, DISTINCT, window,
    /// UNION or sort (PRF006 lineage cost).
    pub fn is_expensive(&self) -> bool {
        let s = &self.code;
        [
            " JOIN ", "GROUP BY", "DISTINCT", " OVER", "UNION", "ORDER BY", "WINDOW",
        ]
        .iter()
        .any(|k| s.contains(k))
            || s.split_whitespace().any(|w| w == "JOIN")
    }

    /// Classify a SQL boolean condition (`F.expr("a.k = b.k")`): any `=` / `<=>` is an
    /// equality (also `c.region = 'eu'`: use [`Self::conjuncts`] when literal filters
    /// matter).
    pub fn cond(&self) -> SqlCond {
        cond_of(&self.code)
    }

    /// Top-level conjuncts of a SQL join condition. OR binds looser than AND, so a
    /// top-level OR makes the whole text one disjunction; equalities against a
    /// literal (`c.region = 'eu'`) are filters, not keys.
    pub fn conjuncts(&self) -> Vec<SqlConjunct> {
        let mut out = Vec::new();
        conjuncts_of(&self.code, &mut out);
        out
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

/// Start of the last `GROUP BY` before byte `end`, if no `SELECT`/`FROM` follows it.
fn group_by_before(up: &str, end: usize) -> Option<usize> {
    let head = &up[..end];
    let mut words: Vec<(usize, &str)> = Vec::new();
    let mut start = None;
    for (i, c) in head
        .char_indices()
        .chain(std::iter::once((head.len(), ' ')))
    {
        let ident = c.is_ascii_alphanumeric() || c == '_';
        match (ident, start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                words.push((s, &head[s..i]));
                start = None;
            }
            _ => {}
        }
    }
    let gb = words
        .windows(2)
        .rposition(|w| w[0].1 == "GROUP" && w[1].1 == "BY")?;
    if words[gb + 2..]
        .iter()
        .any(|(_, w)| matches!(*w, "SELECT" | "FROM" | "WHERE" | "HAVING"))
    {
        return None;
    }
    Some(words[gb + 1].0 + 2)
}

/// Uppercased SQL starting with a call to `rand(`/`randn(`/`random(`.
fn random_call_prefix(up: &str) -> bool {
    ["RANDOM", "RANDN", "RAND"]
        .iter()
        .any(|w| is_word_at(up, 0, w) && up[w.len()..].trim_start().starts_with('('))
}

/// Whether byte `at` sits inside parentheses that hold no `SELECT` before it: a window
/// specification (`OVER (...)`, `WINDOW w AS (...)`) rather than a subquery.
fn in_window_spec(up: &str, at: usize) -> bool {
    let b = up.as_bytes();
    let mut depth = 0i32;
    for p in (0..at).rev() {
        match b[p] {
            b')' => depth += 1,
            b'(' if depth > 0 => depth -= 1,
            b'(' => {
                return !up[p + 1..at]
                    .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .any(|w| w == "SELECT");
            }
            _ => {}
        }
    }
    false
}

fn cond_of(t: &str) -> SqlCond {
    let b = t.as_bytes();
    for (i, ch) in b.iter().enumerate() {
        if *ch == b'=' {
            let prev = if i > 0 { b[i - 1] } else { b' ' };
            if !matches!(prev, b'<' | b'>' | b'!') {
                return SqlCond::Equi;
            }
        }
    }
    if t.contains("<=>") {
        return SqlCond::Equi;
    }
    if t.contains('<') || t.contains('>') || t.contains("BETWEEN") || t.contains(" LIKE ") {
        SqlCond::NonEqui
    } else {
        SqlCond::Unknown
    }
}

/// Split `s` on `sep` (uppercase, whole word) outside parentheses and quotes.
fn split_top<'a>(s: &'a str, sep: &str) -> Vec<&'a str> {
    let b = s.as_bytes();
    let sep = sep.as_bytes();
    let mut out = Vec::new();
    let (mut depth, mut quote, mut start, mut i) = (0i32, None::<u8>, 0usize, 0usize);
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
        } else if c == b'\'' || c == b'"' || c == b'`' {
            quote = Some(c);
        } else if c == b'(' {
            depth += 1;
        } else if c == b')' {
            depth -= 1;
        } else if depth == 0
            && b[i..].starts_with(sep)
            && (i == 0 || !b[i - 1].is_ascii_alphanumeric() && b[i - 1] != b'_')
            && b.get(i + sep.len())
                .is_none_or(|n| !n.is_ascii_alphanumeric() && *n != b'_')
        {
            out.push(&s[start..i]);
            i += sep.len();
            start = i;
            continue;
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

fn strip_parens(s: &str) -> &str {
    let mut t = s.trim();
    while t.len() >= 2 && t.starts_with('(') && t.ends_with(')') {
        // Only strip when the outer pair encloses the whole text.
        let inner = &t[1..t.len() - 1];
        let mut depth = 0i32;
        let balanced = inner.bytes().all(|c| {
            if c == b'(' {
                depth += 1;
            } else if c == b')' {
                depth -= 1;
            }
            depth >= 0
        });
        if !balanced {
            break;
        }
        t = inner.trim();
    }
    t
}

/// `col = <literal>` (either order): a filter, not a join key.
fn literal_eq(p: &str) -> bool {
    let lit = |s: &str| {
        let s = s.trim();
        (s.len() >= 2 && (s.starts_with('\'') && s.ends_with('\'')))
            || s.parse::<f64>().is_ok()
            || matches!(s, "TRUE" | "FALSE" | "NULL")
    };
    let t = p.replace("<=>", "=").replace("==", "=");
    let parts: Vec<&str> = t.split('=').collect();
    parts.len() == 2 && (lit(parts[0]) || lit(parts[1]))
}

/// Any non-literal equality (atoms split on top-level AND/OR, recursing into parentheses).
fn any_eq(s: &str) -> bool {
    let p = strip_parens(s);
    let ors = split_top(p, "OR");
    if ors.len() > 1 {
        return ors.iter().any(|b| any_eq(b));
    }
    let ands = split_top(p, "AND");
    if ands.len() > 1 {
        return ands.iter().any(|b| any_eq(b));
    }
    if p != s.trim() {
        return any_eq(p);
    }
    cond_of(p) == SqlCond::Equi && !literal_eq(p)
}

fn conjuncts_of(s: &str, out: &mut Vec<SqlConjunct>) {
    let s = strip_parens(s);
    // OR binds looser than AND: a top-level OR makes the whole text one disjunction.
    if split_top(s, "OR").len() > 1 {
        out.push(if any_eq(s) {
            SqlConjunct::OrWithEq
        } else {
            SqlConjunct::Other
        });
        return;
    }
    let ands = split_top(s, "AND");
    if ands.len() == 1 {
        out.push(if any_eq(s) {
            SqlConjunct::Equi
        } else {
            SqlConjunct::Other
        });
        return;
    }
    // `x BETWEEN a AND b` splits into two pieces; harmless (neither holds an `=`).
    for piece in ands {
        conjuncts_of(piece, out);
    }
}

impl SemanticModel<'_> {
    /// Static text of a string literal / f-string (holes become `{}`), or of a name
    /// bound once to one.
    pub fn resolve_text(&self, e: &Expr) -> Option<String> {
        if let Some(t) = static_text(e) {
            return Some(t);
        }
        if let Expr::Name(n) = e
            && let Some(b) = self.single_binding(n)
            && let Some(v) = self.binding(b).value
            && v.range() != e.range()
        {
            return static_text(v);
        }
        None
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
        assert_eq!(l("SELECT * FROM t LIMIT {}"), None);
        assert_eq!(l("SELECT * FROM t -- LIMIT 1"), None);
        assert_eq!(l("SELECT * FROM t WHERE x = 'LIMIT 1'"), None);
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
    fn cube_width() {
        let w = |s: &str| q(s).cube_width();
        assert_eq!(w("SELECT 1 FROM t GROUP BY CUBE(a, b, c)"), Some((true, 3)));
        assert_eq!(
            w("select 1 from t group by cube ((a, b), f(c, d), 'x,y')"),
            Some((true, 3))
        );
        assert_eq!(
            w("SELECT 1 FROM t GROUP BY a, b, c, d WITH ROLLUP"),
            Some((false, 4))
        );
        assert_eq!(
            w("SELECT a FROM t GROUP BY a, ROLLUP(b, c), CUBE(d, e)"),
            Some((true, 2))
        );
        assert_eq!(w("SELECT cube(a, b) FROM t GROUP BY a"), None);
        assert_eq!(w("SELECT * FROM cube WHERE x = 1"), None);
        assert_eq!(w("SELECT a FROM t GROUP BY a WITH CUBE"), Some((true, 1)));
        assert_eq!(w("SELECT 'GROUP BY CUBE(a, b)' FROM t"), None);
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
        assert!(q("SELECT a FROM t JOIN u ON t.k = u.k").is_expensive());
        assert!(q("SELECT k, count(*) FROM t GROUP BY k").is_expensive());
        assert!(!q("SELECT a FROM t WHERE note = 'x join y'").is_expensive());
        assert!(!q("SELECT a FROM t -- order by a\n").is_expensive());
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

    #[test]
    fn join_conditions() {
        assert_eq!(q("a.k = b.k").cond(), SqlCond::Equi);
        assert_eq!(q("a.k <=> b.k").cond(), SqlCond::Equi);
        assert_eq!(q("a.ts BETWEEN b.lo AND b.hi").cond(), SqlCond::NonEqui);
        assert_eq!(q("a.ts >= b.lo").cond(), SqlCond::NonEqui);
        assert_eq!(q("a.name LIKE b.pat").cond(), SqlCond::NonEqui);
        assert_eq!(q("a.ts < b.ts AND a.note != '='").cond(), SqlCond::NonEqui);
        assert_eq!(q("f(a, b)").cond(), SqlCond::Unknown);
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
        assert_eq!(c("a.ts BETWEEN b.lo AND b.hi"), vec![Other, Other]);
    }
}
