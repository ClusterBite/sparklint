//! The clause structure of one SQL text, parsed once from its tokens
//! ([`super::SqlText`] builds it lazily on the first clause question).
//!
//! The parse is shallow and never fails: parenthesised groups, the query blocks of
//! every (sub)query, each block's clauses (`SELECT` list, `FROM`, `WHERE`, `GROUP BY`,
//! `HAVING`, `QUALIFY`, `WINDOW`, `ORDER BY` / `SORT BY` / `CLUSTER BY` /
//! `DISTRIBUTE BY`, `LIMIT`, `OFFSET`) as token ranges, and the window
//! specifications (`f(..) OVER (..)`, `WINDOW w AS (..)`) with their `PARTITION BY`,
//! `ORDER BY` and frame. Comments are not tokens and string literals / quoted
//! identifiers are single tokens, so nothing inside them is ever a clause.
//!
//! Conditions are part of the parse: every `WHERE` / `HAVING` / `QUALIFY` / `ON` /
//! `WHEN` body and the whole text (a condition fragment) become a [`Pred`] tree of
//! AND / OR / NOT over atoms with their comparison ([`Cmp`]: left, operator, right,
//! literal sides; `BETWEEN .. AND ..` kept whole), and every comparison-operator run is
//! marked when it assigns ([`OpRun::assignment`]: `SET` clauses, property lists).
//!
//! A *scope* is the top level or a group that holds a query (a `SELECT` at its own
//! level, or it starts with `WITH` / `FROM` / a query group). Clause keywords count
//! only at a scope's own paren level: `EXTRACT(year FROM d)` and the `ORDER BY` of a
//! window specification belong to no query. A scope splits into blocks at each
//! further `SELECT` and at set operators (`UNION`, `EXCEPT`, ...) and `;`.

use std::ops::Range;

use super::{Kind, Tok};

/// One parenthesised group `( .. )`.
#[derive(Debug, Clone)]
pub(super) struct Group {
    /// Token index of the `(`.
    pub open: usize,
    /// Token index of the matching `)`; `None` when the group is never closed.
    pub close: Option<usize>,
    /// The group holds a query (it is a scope).
    pub query: bool,
    /// Its tokens split on its own-level commas (at least one range, maybe empty).
    pub items: Vec<Range<usize>>,
}

impl Group {
    /// Tokens strictly inside the parentheses.
    pub fn inner(&self, len: usize) -> Range<usize> {
        self.open + 1..self.close.unwrap_or(len)
    }

    /// Number of non-empty comma-separated items (`CUBE(a, (b, c))` = 2).
    pub fn item_count(&self) -> usize {
        self.items.iter().filter(|r| !r.is_empty()).count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClauseKind {
    Select,
    From,
    Where,
    GroupBy,
    Having,
    Qualify,
    Window,
    OrderBy,
    SortBy,
    ClusterBy,
    DistributeBy,
    Limit,
    Offset,
}

/// One clause of a query block.
#[derive(Debug, Clone)]
pub(super) struct Clause {
    pub kind: ClauseKind,
    /// Token index of the clause's first keyword (`GROUP` of `GROUP BY`).
    pub kw: usize,
    /// Tokens after the keyword(s) up to the next clause of the block (nested groups
    /// included).
    pub body: Range<usize>,
    /// `body` split on the block's own-level commas (at least one range, maybe empty).
    pub items: Vec<Range<usize>>,
}

/// One query block: a `SELECT ..` (with whatever leads it, e.g. `WITH ..` or
/// `INSERT INTO t`) up to a set operator, `;`, a further `SELECT` or the end of its
/// scope.
#[derive(Debug, Clone)]
pub(super) struct Query {
    /// The group holding the block (`None` = top level).
    pub scope: Option<usize>,
    pub tokens: Range<usize>,
    /// Token index of its `SELECT`.
    pub select: Option<usize>,
    /// In text order.
    pub clauses: Vec<Clause>,
}

impl Query {
    /// The block's first clause of kind `k`.
    pub fn clause(&self, k: ClauseKind) -> Option<&Clause> {
        self.clauses.iter().find(|c| c.kind == k)
    }
}

/// One window specification: `f(..) [FILTER (..)] OVER (..)` or a `WINDOW w AS (..)`
/// definition.
#[derive(Debug, Clone)]
pub(super) struct Window {
    /// Token index of `OVER` (`None` for a `WINDOW` clause definition).
    pub over: Option<usize>,
    /// Token index of the windowed function's name (`collect_list` in
    /// `collect_list(v) OVER (..)`).
    pub func: Option<usize>,
    /// The group holding the specification.
    pub group: usize,
    /// Tokens after `PARTITION BY` / `DISTRIBUTE BY`.
    pub partition: Option<Range<usize>>,
    /// Tokens after `ORDER BY` / `SORT BY`.
    pub order: Option<Range<usize>>,
    /// Token index of the frame keyword (`ROWS` / `RANGE`).
    pub frame: Option<usize>,
}

/// A boolean condition (a `WHERE` / `ON` / `HAVING` / `QUALIFY` / `WHEN` body, or a
/// condition fragment such as a join condition): AND / OR / NOT over atoms, parsed
/// once from the tokens. OR binds looser than AND; enclosing parentheses are
/// stripped at every node.
#[derive(Debug, Clone)]
pub(super) struct Pred {
    /// The condition's tokens, enclosing parentheses stripped.
    pub range: Range<usize>,
    pub kind: PredKind,
}

#[derive(Debug, Clone)]
pub(super) enum PredKind {
    Or(Vec<Pred>),
    And(Vec<Pred>),
    Not(Box<Pred>),
    /// No own-level AND / OR: a comparison (`cmp`), or another predicate (a call,
    /// a column, `EXISTS (..)`).
    Atom(Option<Cmp>),
}

/// The own-level comparison of an atom: `left op right` (token ranges).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Cmp {
    pub left: Range<usize>,
    /// The operator tokens (`<=>`, `NOT BETWEEN`, `IS NOT`).
    pub op: Range<usize>,
    pub kind: CmpOp,
    /// `x BETWEEN lo AND hi`: `lo AND hi`.
    pub right: Range<usize>,
    /// Which side is one literal (`'eu'`, `-1`, `TRUE`, `NULL`).
    pub left_literal: bool,
    pub right_literal: bool,
}

impl Cmp {
    /// `a = b` / `a <=> b` with two non-literal sides: a join key, not a filter.
    pub fn key_equality(&self) -> bool {
        matches!(self.kind, CmpOp::Eq | CmpOp::NullSafeEq)
            && !self.left.is_empty()
            && !self.right.is_empty()
            && !self.left_literal
            && !self.right_literal
    }

    /// A range / inequality / pattern comparison (`<`, `!=`, `BETWEEN`, `LIKE`).
    pub fn non_equi(&self) -> bool {
        matches!(
            self.kind,
            CmpOp::Ne
                | CmpOp::Lt
                | CmpOp::Le
                | CmpOp::Gt
                | CmpOp::Ge
                | CmpOp::Between
                | CmpOp::Like
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CmpOp {
    /// `=` / `==`.
    Eq,
    /// `<=>`.
    NullSafeEq,
    /// `!=` / `<>`.
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Between,
    /// `LIKE` / `ILIKE` / `RLIKE` / `REGEXP`.
    Like,
    In,
    Is,
    /// Any other run of `=`, `!`, `<`, `>` (`=>`).
    Other,
}

impl Pred {
    /// The comparisons of every atom (through AND / OR / NOT).
    pub fn comparisons(&self) -> Vec<&Cmp> {
        let mut out = Vec::new();
        self.walk_cmps(&mut out);
        out
    }

    fn walk_cmps<'a>(&'a self, out: &mut Vec<&'a Cmp>) {
        match &self.kind {
            PredKind::Or(xs) | PredKind::And(xs) => xs.iter().for_each(|x| x.walk_cmps(out)),
            PredKind::Not(x) => x.walk_cmps(out),
            PredKind::Atom(c) => out.extend(c),
        }
    }

    /// The node's own-level AND operands (itself when it is no AND).
    pub fn conjuncts(&self) -> &[Pred] {
        match &self.kind {
            PredKind::And(xs) => xs,
            _ => std::slice::from_ref(self),
        }
    }
}

/// Where a condition sits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CondKind {
    Where,
    Having,
    Qualify,
    /// `JOIN .. ON` / `MERGE .. USING .. ON`.
    On,
    /// `CASE WHEN c THEN` / `WHEN [NOT] MATCHED AND c THEN`.
    When,
}

/// One condition body of the text.
#[derive(Debug, Clone)]
pub(super) struct Condition {
    // Not every build reads `kind` / `pred`.
    #[allow(dead_code)]
    pub kind: CondKind,
    pub body: Range<usize>,
    #[allow(dead_code)]
    pub pred: Pred,
}

/// One run of comparison-operator characters (`=`, `!`, `<`, `>`, adjacent tokens).
#[derive(Debug, Clone)]
pub(super) struct OpRun {
    pub range: Range<usize>,
    /// The run is in an assignment list: a `SET` clause (`UPDATE .. SET a = 1`, MERGE
    /// `UPDATE SET`, the `SET k = v` statement) or a property list
    /// (`TBLPROPERTIES (..)`, `OPTIONS (..)`), up to the next condition keyword.
    pub assignment: bool,
}

/// Groups that hold `key = value` assignments.
const PROPERTY_LISTS: &[&str] = &[
    "TBLPROPERTIES",
    "OPTIONS",
    "PROPERTIES",
    "SERDEPROPERTIES",
    "DBPROPERTIES",
    "TAGS",
];

/// Words that end an assignment list (the expression is a condition again).
const CONDITION_WORDS: &[&str] = &[
    "WHEN", "WHERE", "ON", "HAVING", "QUALIFY", "AND", "OR", "NOT", "SELECT", "CASE", "THEN",
    "ELSE", "IF",
];

/// Words that end a join's `ON` condition.
const JOIN_WORDS: &[&str] = &[
    "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "ANTI", "SEMI", "LATERAL",
];

fn is_op_char(v: &View, i: usize) -> bool {
    ["=", "!", "<", ">"].iter().any(|p| v.is_punct(i, p))
}

/// Tokens `i` and `i + 1` touch (no space between).
fn touching(v: &View, i: usize) -> bool {
    matches!((v.toks.get(i), v.toks.get(i + 1)), (Some(a), Some(b)) if a.end == b.start)
}

/// `r` without enclosing parentheses: `((a = b))` → `a = b`; `(a) AND (b)` stays.
fn strip(v: &View, mut r: Range<usize>) -> Range<usize> {
    while r.len() >= 2 && v.is_punct(r.start, "(") && v.is_punct(r.end - 1, ")") {
        let inner = r.start + 1..r.end - 1;
        let mut depth = 0i32;
        let enclosed = inner.clone().all(|i| {
            if v.is_punct(i, "(") {
                depth += 1;
            } else if v.is_punct(i, ")") {
                depth -= 1;
            }
            depth >= 0
        });
        if !enclosed {
            break;
        }
        r = inner;
    }
    r
}

/// Split `r` on the word `w` outside parentheses (an unmatched `)` ends splitting).
/// The `AND` of `x BETWEEN lo AND hi` belongs to the BETWEEN.
fn split_word(v: &View, r: Range<usize>, w: &str) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let (mut depth, mut start, mut between) = (0i32, r.start, false);
    for i in r.clone() {
        if v.is_punct(i, "(") {
            depth += 1;
        } else if v.is_punct(i, ")") {
            depth -= 1;
        } else if depth == 0 && v.is_word(i, "BETWEEN") {
            between = true;
        } else if depth == 0 && v.is_word(i, w) {
            if w == "AND" && std::mem::take(&mut between) {
                continue;
            }
            out.push(start..i);
            start = i + 1;
        }
    }
    out.push(start..r.end.max(start));
    out
}

/// Parse the condition in tokens `r`.
fn pred(v: &View, r: Range<usize>) -> Pred {
    let r = strip(v, r);
    for w in ["OR", "AND"] {
        let parts = split_word(v, r.clone(), w);
        if parts.len() > 1 {
            let xs = parts.into_iter().map(|p| pred(v, p)).collect();
            return Pred {
                range: r,
                kind: if w == "OR" {
                    PredKind::Or(xs)
                } else {
                    PredKind::And(xs)
                },
            };
        }
    }
    if r.len() > 1 && v.is_word(r.start, "NOT") {
        return Pred {
            kind: PredKind::Not(Box::new(pred(v, r.start + 1..r.end))),
            range: r,
        };
    }
    Pred {
        kind: PredKind::Atom(cmp(v, r.clone())),
        range: r,
    }
}

/// Tokens `r` are one literal: a string, a (signed) number, `TRUE` / `FALSE` / `NULL`.
fn literal(v: &View, r: &Range<usize>) -> bool {
    let number = |i: usize| v.toks.get(i).is_some_and(|t| t.kind == Kind::Number);
    match r.len() {
        1 => {
            v.toks[r.start].kind == Kind::Str
                || number(r.start)
                || matches!(v.word(r.start), Some("TRUE" | "FALSE" | "NULL"))
        }
        2 => (v.is_punct(r.start, "-") || v.is_punct(r.start, "+")) && number(r.start + 1),
        _ => false,
    }
}

/// The first own-level comparison operator of atom `r`.
fn cmp(v: &View, r: Range<usize>) -> Option<Cmp> {
    let mut depth = 0i32;
    let mut i = r.start;
    while i < r.end {
        if v.is_punct(i, "(") {
            depth += 1;
        } else if v.is_punct(i, ")") {
            depth -= 1;
        } else if depth == 0 {
            let at = |op: Range<usize>, kind| {
                let (left, right) = (r.start..op.start, op.end..r.end);
                Some(Cmp {
                    left_literal: literal(v, &left),
                    right_literal: literal(v, &right),
                    left,
                    right,
                    op,
                    kind,
                })
            };
            // `NOT BETWEEN` / `NOT LIKE` / `NOT IN`: the NOT is part of the operator.
            let not = |j: usize| {
                if j > r.start && v.is_word(j - 1, "NOT") {
                    j - 1
                } else {
                    j
                }
            };
            if is_op_char(v, i) {
                let mut end = i + 1;
                while end < r.end && is_op_char(v, end) && touching(v, end - 1) {
                    end += 1;
                }
                let text: String = (i..end)
                    .map(|k| &v.code[v.toks[k].start..v.toks[k].end])
                    .collect();
                let kind = match text.as_str() {
                    "=" | "==" => CmpOp::Eq,
                    "<=>" => CmpOp::NullSafeEq,
                    "!=" | "<>" => CmpOp::Ne,
                    "<" => CmpOp::Lt,
                    "<=" => CmpOp::Le,
                    ">" => CmpOp::Gt,
                    ">=" => CmpOp::Ge,
                    _ => CmpOp::Other,
                };
                return at(i..end, kind);
            }
            match v.word(i) {
                Some("BETWEEN") => return at(not(i)..i + 1, CmpOp::Between),
                Some("LIKE" | "ILIKE" | "RLIKE" | "REGEXP") => {
                    return at(not(i)..i + 1, CmpOp::Like);
                }
                Some("IN") => return at(not(i)..i + 1, CmpOp::In),
                Some("IS") => {
                    let end = if v.is_word(i + 1, "NOT") {
                        i + 2
                    } else {
                        i + 1
                    };
                    return at(i..end.min(r.end), CmpOp::Is);
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// The parsed clause structure ([`Clauses::parse`]).
#[derive(Debug, Clone, Default)]
pub(super) struct Clauses {
    pub groups: Vec<Group>,
    /// Innermost group around each token (`(`/`)` belong to the group around them).
    pub level: Vec<Option<usize>>,
    /// The group a `(` token opens.
    opens: Vec<Option<usize>>,
    /// The group a `)` token closes.
    closes: Vec<Option<usize>>,
    /// Query blocks, in text order.
    pub queries: Vec<Query>,
    /// Window specifications, in text order.
    pub windows: Vec<Window>,
    /// The whole text split on its top-level commas (at least one range): the items
    /// of a select-list fragment (`selectExpr("..")`).
    pub items: Vec<Range<usize>>,
    /// The whole text read as one condition (a join condition, a `filter("..")`
    /// fragment).
    pub whole: Pred,
    /// The condition bodies (`WHERE`, `HAVING`, `QUALIFY`, `ON`, `WHEN`), in text order.
    pub conditions: Vec<Condition>,
    /// Every comparison-operator run, in text order.
    pub ops: Vec<OpRun>,
}

impl Default for Pred {
    fn default() -> Self {
        Pred {
            range: 0..0,
            kind: PredKind::Atom(None),
        }
    }
}

/// Token accessors over the upper-cased code view.
struct View<'a> {
    toks: &'a [Tok],
    code: &'a str,
}

impl View<'_> {
    fn word(&self, i: usize) -> Option<&str> {
        let t = self.toks.get(i)?;
        (t.kind == Kind::Word).then(|| &self.code[t.start..t.end])
    }

    fn is_word(&self, i: usize, w: &str) -> bool {
        self.word(i) == Some(w)
    }

    fn is_punct(&self, i: usize, p: &str) -> bool {
        self.toks
            .get(i)
            .is_some_and(|t| t.kind == Kind::Punct && &self.code[t.start..t.end] == p)
    }

    /// The clause keyword starting at token `i` and how many tokens it spans.
    fn clause_at(&self, i: usize) -> Option<(ClauseKind, usize)> {
        let by = |k| self.is_word(i + 1, "BY").then_some((k, 2));
        match self.word(i)? {
            "SELECT" => Some((ClauseKind::Select, 1)),
            // `a IS DISTINCT FROM b` is a comparison.
            "FROM" if !(i > 0 && self.is_word(i - 1, "DISTINCT")) => Some((ClauseKind::From, 1)),
            "WHERE" => Some((ClauseKind::Where, 1)),
            "HAVING" => Some((ClauseKind::Having, 1)),
            "QUALIFY" => Some((ClauseKind::Qualify, 1)),
            "GROUP" => by(ClauseKind::GroupBy),
            "ORDER" => by(ClauseKind::OrderBy),
            "SORT" => by(ClauseKind::SortBy),
            "CLUSTER" => by(ClauseKind::ClusterBy),
            "DISTRIBUTE" => by(ClauseKind::DistributeBy),
            // `WINDOW w AS (..)`; `window(ts, '5 minutes')` is a function.
            "WINDOW"
                if matches!(self.toks.get(i + 1), Some(t) if matches!(t.kind, Kind::Word | Kind::Ident))
                    && self.is_word(i + 2, "AS") =>
            {
                Some((ClauseKind::Window, 1))
            }
            // A count follows: a column named `limit` is no clause.
            "LIMIT"
                if self.toks.get(i + 1).is_some_and(|t| t.kind == Kind::Number)
                    || self.is_punct(i + 1, "{")
                    || self.is_punct(i + 1, "(")
                    || self.is_word(i + 1, "ALL") =>
            {
                Some((ClauseKind::Limit, 1))
            }
            "OFFSET"
                if self.toks.get(i + 1).is_some_and(|t| t.kind == Kind::Number)
                    || self.is_punct(i + 1, "{") =>
            {
                Some((ClauseKind::Offset, 1))
            }
            _ => None,
        }
    }

    /// A set operator ends the block (`SELECT * EXCEPT (a)` is a column list).
    fn set_op_at(&self, i: usize) -> bool {
        match self.word(i) {
            Some("UNION" | "INTERSECT" | "MINUS") => true,
            Some("EXCEPT") => !(i > 0 && self.is_punct(i - 1, "*")),
            _ => false,
        }
    }
}

impl Clauses {
    /// Parse `toks` (spans into the upper-cased, masked `code`).
    pub fn parse(toks: &[Tok], code: &str) -> Self {
        let v = View { toks, code };
        let n = toks.len();
        let mut c = Clauses {
            level: vec![None; n],
            opens: vec![None; n],
            closes: vec![None; n],
            ..Default::default()
        };
        // Groups and the innermost group of every token.
        let mut stack: Vec<usize> = Vec::new();
        for i in 0..n {
            c.level[i] = stack.last().copied();
            if v.is_punct(i, "(") {
                c.opens[i] = Some(c.groups.len());
                stack.push(c.groups.len());
                c.groups.push(Group {
                    open: i,
                    close: None,
                    query: false,
                    items: Vec::new(),
                });
            } else if v.is_punct(i, ")")
                && let Some(g) = stack.pop()
            {
                c.groups[g].close = Some(i);
                c.closes[i] = Some(g);
                c.level[i] = stack.last().copied();
            }
        }
        // Which groups hold a query: children open after their parents, so walk back.
        let mut own_select = vec![false; c.groups.len()];
        for (i, level) in c.level.iter().enumerate().take(n) {
            if let Some(g) = *level
                && v.is_word(i, "SELECT")
            {
                own_select[g] = true;
            }
        }
        for g in (0..c.groups.len()).rev() {
            let first = c.groups[g].open + 1;
            let starts_query = c.level.get(first) == Some(&Some(g))
                && (v.is_word(first, "WITH")
                    || v.is_word(first, "FROM")
                    || c.opens[first].is_some_and(|h| c.groups[h].query));
            c.groups[g].query = own_select[g] || starts_query;
        }
        for g in 0..c.groups.len() {
            let inner = c.groups[g].inner(n);
            c.groups[g].items = c.split(&v, inner, Some(g));
        }
        c.items = c.split(&v, 0..n, None);
        // Query blocks of every scope.
        let scopes = std::iter::once(None)
            .chain((0..c.groups.len()).filter(|&g| c.groups[g].query).map(Some));
        let mut queries = Vec::new();
        for scope in scopes {
            let range = scope.map_or(0..n, |g| c.groups[g].inner(n));
            c.blocks(&v, scope, range, &mut queries);
        }
        queries.sort_by_key(|q| q.tokens.start);
        c.queries = queries;
        // Window specifications.
        for g in 0..c.groups.len() {
            if let Some(w) = c.window(&v, g) {
                c.windows.push(w);
            }
        }
        c.whole = pred(&v, 0..n);
        c.ops = c.op_runs(&v);
        c.conditions = c.condition_sites(&v);
        c
    }

    /// Every run of comparison-operator characters, marked when it sits in an
    /// assignment list (see [`OpRun::assignment`]).
    fn op_runs(&self, v: &View) -> Vec<OpRun> {
        let n = v.toks.len();
        // Per paren level: inside an assignment list so far.
        let mut top = false;
        let mut state = vec![false; self.groups.len()];
        let mut assign = vec![false; n];
        for (i, slot) in assign.iter_mut().enumerate() {
            let st = match self.level[i] {
                None => &mut top,
                Some(g) => &mut state[g],
            };
            *slot = *st;
            if v.is_word(i, "SET") {
                *st = true;
            } else if v.is_punct(i, ";") || v.word(i).is_some_and(|w| CONDITION_WORDS.contains(&w))
            {
                *st = false;
            }
            if let Some(g) = self.opens[i] {
                state[g] = i > 0 && v.word(i - 1).is_some_and(|w| PROPERTY_LISTS.contains(&w));
            }
        }
        let mut out = Vec::new();
        let mut i = 0;
        while i < n {
            if !is_op_char(v, i) {
                i += 1;
                continue;
            }
            let start = i;
            i += 1;
            while i < n && is_op_char(v, i) && touching(v, i - 1) {
                i += 1;
            }
            out.push(OpRun {
                range: start..i,
                assignment: assign[start],
            });
        }
        out
    }

    /// The condition bodies of the text: `WHERE` / `HAVING` / `QUALIFY` clauses, a
    /// join's (or MERGE's) `ON`, and `WHEN .. THEN` (CASE, MERGE `WHEN MATCHED AND ..`).
    fn condition_sites(&self, v: &View) -> Vec<Condition> {
        let n = v.toks.len();
        let mut out = Vec::new();
        let mut push = |kind, body: Range<usize>| {
            out.push(Condition {
                kind,
                pred: pred(v, body.clone()),
                body,
            });
        };
        for q in &self.queries {
            for cl in &q.clauses {
                let kind = match cl.kind {
                    ClauseKind::Where => CondKind::Where,
                    ClauseKind::Having => CondKind::Having,
                    ClauseKind::Qualify => CondKind::Qualify,
                    _ => continue,
                };
                push(kind, cl.body.clone());
            }
        }
        for i in 0..n {
            let lvl = self.level[i];
            let (lo, hi) = lvl.map_or((0, n), |g| {
                let r = self.groups[g].inner(n);
                (r.start, r.end)
            });
            let own = |j: usize| self.level[j] == lvl;
            if v.is_word(i, "ON") {
                // JOIN / MERGE .. USING earlier in the same clause.
                let joined = (lo..i)
                    .rev()
                    .filter(|&j| own(j))
                    .take_while(|&j| {
                        v.clause_at(j).is_none()
                            && !v.is_word(j, "ON")
                            && !v.is_punct(j, ",")
                            && !v.is_punct(j, ";")
                            && !v.set_op_at(j)
                    })
                    .any(|j| v.is_word(j, "JOIN") || v.is_word(j, "USING"));
                if !joined {
                    continue;
                }
                let end = (i + 1..hi)
                    .find(|&j| {
                        own(j)
                            && (v.clause_at(j).is_some()
                                || v.set_op_at(j)
                                || v.is_punct(j, ",")
                                || v.is_punct(j, ";")
                                || v.is_word(j, "WHEN")
                                || v.is_word(j, "ON")
                                || v.word(j).is_some_and(|w| JOIN_WORDS.contains(&w))
                                    && !v.is_punct(j + 1, "("))
                    })
                    .unwrap_or(hi);
                push(CondKind::On, i + 1..end);
            } else if v.is_word(i, "WHEN") {
                let Some(then) = (i + 1..hi).find(|&j| own(j) && v.is_word(j, "THEN")) else {
                    continue;
                };
                let mut s = i + 1;
                // MERGE `WHEN [NOT] MATCHED [BY SOURCE|TARGET] [AND cond] THEN`
                let matched = if v.is_word(s, "NOT") && v.is_word(s + 1, "MATCHED") {
                    s += 2;
                    true
                } else if v.is_word(s, "MATCHED") {
                    s += 1;
                    true
                } else {
                    false
                };
                if matched {
                    if v.is_word(s, "BY") {
                        s += 2;
                    }
                    if s < then && v.is_word(s, "AND") {
                        s += 1;
                    } else {
                        continue;
                    }
                }
                push(CondKind::When, s.min(then)..then);
            }
        }
        out.sort_by_key(|c| c.body.start);
        out
    }

    /// The index of the group whose `(` is token `i`.
    pub fn opened_at(&self, i: usize) -> Option<usize> {
        self.opens.get(i).copied().flatten()
    }

    /// The group whose `(` is token `i`.
    pub fn group_at(&self, i: usize) -> Option<&Group> {
        self.opened_at(i).map(|g| &self.groups[g])
    }

    /// Split `range` on the commas at `scope`'s own level (at least one range).
    fn split(&self, v: &View, range: Range<usize>, scope: Option<usize>) -> Vec<Range<usize>> {
        let mut out = Vec::new();
        let mut start = range.start;
        for i in range.clone() {
            if self.level[i] == scope && v.is_punct(i, ",") {
                out.push(start..i);
                start = i + 1;
            }
        }
        out.push(start..range.end.max(start));
        out
    }

    /// Split the own-level tokens of one scope into query blocks and clauses.
    fn blocks(&self, v: &View, scope: Option<usize>, range: Range<usize>, out: &mut Vec<Query>) {
        let new = |start| Query {
            scope,
            tokens: start..start,
            select: None,
            clauses: Vec::new(),
        };
        let mut cur = new(range.start);
        let finish = |mut q: Query, end: usize, out: &mut Vec<Query>| {
            q.tokens.end = end;
            if let Some(last) = q.clauses.last_mut() {
                last.body.end = end;
            }
            if q.select.is_some() || !q.clauses.is_empty() {
                out.push(q);
            }
        };
        let mut i = range.start;
        while i < range.end {
            if self.level[i] != scope {
                i += 1;
                continue;
            }
            if v.set_op_at(i) || v.is_punct(i, ";") {
                finish(std::mem::replace(&mut cur, new(i + 1)), i, out);
                i += 1;
                continue;
            }
            if let Some((kind, len)) = v.clause_at(i) {
                if kind == ClauseKind::Select {
                    if cur.select.is_some() {
                        finish(std::mem::replace(&mut cur, new(i)), i, out);
                    }
                    cur.select = Some(i);
                }
                if let Some(last) = cur.clauses.last_mut() {
                    last.body.end = i;
                }
                cur.clauses.push(Clause {
                    kind,
                    kw: i,
                    body: i + len..i + len,
                    items: Vec::new(),
                });
                i += len;
                continue;
            }
            i += 1;
        }
        let end = range.end;
        finish(cur, end, out);
        // Clause items, now that the bodies are known.
        for q in out.iter_mut().filter(|q| q.scope == scope) {
            for cl in &mut q.clauses {
                cl.items = self.split(v, cl.body.clone(), scope);
            }
        }
    }

    /// Group `g` as a window specification: after `OVER`, or `AS` inside a `WINDOW`
    /// clause.
    fn window(&self, v: &View, g: usize) -> Option<Window> {
        let open = self.groups[g].open;
        let prev = open.checked_sub(1)?;
        let over = v.is_word(prev, "OVER");
        let named = v.is_word(prev, "AS")
            && self.queries.iter().any(|q| {
                q.scope == self.level[open]
                    && q.clauses
                        .iter()
                        .any(|c| c.kind == ClauseKind::Window && c.body.contains(&open))
            });
        if !(over || named) {
            return None;
        }
        // `name(..) [FILTER (..)] OVER (..)`
        let func = over.then(|| self.call_before(v, prev)).flatten();
        let mut w = Window {
            over: over.then_some(prev),
            func,
            group: g,
            partition: None,
            order: None,
            frame: None,
        };
        let inner = self.groups[g].inner(v.toks.len());
        // (slot, start) of the open PARTITION / ORDER part.
        let mut open_part: Option<(bool, usize)> = None;
        let close = |w: &mut Window, part: Option<(bool, usize)>, end: usize| match part {
            Some((true, s)) => w.partition = Some(s..end),
            Some((false, s)) => w.order = Some(s..end),
            None => {}
        };
        let mut i = inner.start;
        while i < inner.end {
            if self.level[i] != Some(g) {
                i += 1;
                continue;
            }
            let by = v.is_word(i + 1, "BY");
            let part = match v.word(i) {
                Some("PARTITION" | "DISTRIBUTE") if by => Some(true),
                Some("ORDER" | "SORT") if by => Some(false),
                Some("ROWS" | "RANGE") => None,
                _ => {
                    i += 1;
                    continue;
                }
            };
            close(&mut w, open_part.take(), i);
            match part {
                Some(slot) => {
                    open_part = Some((slot, i + 2));
                    i += 2;
                }
                None => {
                    w.frame.get_or_insert(i);
                    i += 1;
                }
            }
        }
        close(&mut w, open_part, inner.end);
        Some(w)
    }

    /// The function name of the call that ends at token `end` (exclusive of the
    /// `OVER` at `end`): `name(..)` or `name(..) FILTER (..)`.
    fn call_before(&self, v: &View, end: usize) -> Option<usize> {
        let args = |close: usize| close.checked_sub(1).and_then(|c| self.closes[c]);
        let mut g = args(end)?;
        let mut before = self.groups[g].open.checked_sub(1)?;
        if v.is_word(before, "FILTER") {
            g = args(before)?;
            before = self.groups[g].open.checked_sub(1)?;
        }
        v.word(before).map(|_| before)
    }
}

#[cfg(test)]
mod tests {
    use super::super::SqlText;
    use super::*;

    /// One line per query block (`depth: KIND body | KIND body`) and per window
    /// (`OVER f: PARTITION .. | ORDER .. | FRAME ..`), bodies as code text.
    fn render(s: &str) -> Vec<String> {
        let t = SqlText::new(s);
        let c = t.clauses();
        let text = |r: &Range<usize>| -> String {
            if r.is_empty() {
                return String::new();
            }
            t.code[t.toks[r.start].start..t.toks[r.end - 1].end].to_string()
        };
        let depth = |mut g: Option<usize>| {
            let mut d = 0;
            while let Some(x) = g {
                d += 1;
                g = c.level[c.groups[x].open];
            }
            d
        };
        let mut out: Vec<String> = c
            .queries
            .iter()
            .map(|q| {
                let parts: Vec<String> = q
                    .clauses
                    .iter()
                    .map(|cl| {
                        let items: Vec<String> = cl.items.iter().map(text).collect();
                        format!("{:?} {}", cl.kind, items.join(", "))
                    })
                    .collect();
                format!("{}: {}", depth(q.scope), parts.join(" | "))
            })
            .collect();
        for w in &c.windows {
            let mut parts = vec![match (w.over, w.func) {
                (Some(_), Some(f)) => format!("OVER {}", text(&(f..f + 1))),
                (Some(_), None) => "OVER".to_string(),
                (None, _) => "WINDOW".to_string(),
            }];
            if let Some(p) = &w.partition {
                parts.push(format!("PARTITION {}", text(p)));
            }
            if let Some(o) = &w.order {
                parts.push(format!("ORDER {}", text(o)));
            }
            if let Some(f) = w.frame {
                parts.push(format!("FRAME {}", text(&(f..f + 1))));
            }
            out.push(parts.join(" | "));
        }
        out
    }

    /// A condition tree as text: `OR(..)`, `AND(..)`, `NOT(..)`, comparisons as
    /// `left [op] right` (a literal side marked `lit:`), other atoms as code text.
    fn tree(t: &SqlText, p: &Pred) -> String {
        let text = |r: &Range<usize>| -> String {
            if r.is_empty() {
                return String::new();
            }
            t.code[t.toks[r.start].start..t.toks[r.end - 1].end].to_string()
        };
        let list = |name: &str, xs: &[Pred]| {
            let parts: Vec<String> = xs.iter().map(|x| tree(t, x)).collect();
            format!("{name}({})", parts.join(", "))
        };
        match &p.kind {
            PredKind::Or(xs) => list("OR", xs),
            PredKind::And(xs) => list("AND", xs),
            PredKind::Not(x) => format!("NOT({})", tree(t, x)),
            PredKind::Atom(None) => text(&p.range),
            PredKind::Atom(Some(c)) => {
                let side = |r: &Range<usize>, lit: bool| {
                    format!("{}{}", if lit { "lit:" } else { "" }, text(r))
                };
                format!(
                    "{} [{}] {}",
                    side(&c.left, c.left_literal),
                    text(&c.op),
                    side(&c.right, c.right_literal)
                )
            }
        }
    }

    /// The condition bodies (`Kind: tree`) of a text, then its runs of operators
    /// marked `=:` when they assign.
    fn conds(s: &str) -> Vec<String> {
        let t = SqlText::new(s);
        let c = t.clauses();
        c.conditions
            .iter()
            .map(|x| format!("{:?}: {}", x.kind, tree(&t, &x.pred)))
            .collect()
    }

    #[test]
    fn condition_trees() {
        let w = |s: &str| {
            let t = SqlText::new(s);
            tree(&t, &t.clauses().whole)
        };
        assert_eq!(w("a.k = b.k"), "A.K [=] B.K");
        assert_eq!(
            w("(a.k = b.k AND x > 1) OR NOT (y <=> 'q')"),
            "OR(AND(A.K [=] B.K, X [>] lit:1), NOT(Y [<=>] lit:' '))"
        );
        assert_eq!(
            w("((a = 1)) and (b != c or d like 'x%')"),
            "AND(A [=] lit:1, OR(B [!=] C, D [LIKE] lit:'  '))"
        );
        assert_eq!(
            w("a.id NOT IN (SELECT id FROM t) AND b IS NOT NULL"),
            "AND(A.ID [NOT IN] (SELECT ID FROM T), B [IS NOT] lit:NULL)"
        );
        // Calls and nested comparisons stay inside their atom.
        assert_eq!(
            w("coalesce(a = b, false) OR f(x)"),
            "OR(COALESCE(A = B, FALSE), F(X))"
        );
        // BETWEEN keeps its AND; the next AND splits again.
        assert_eq!(
            w("a.ts BETWEEN b.lo AND b.hi"),
            "A.TS [BETWEEN] B.LO AND B.HI"
        );
        assert_eq!(
            w("x NOT BETWEEN 1 AND 5 AND y = -2 AND (z BETWEEN a AND b OR q)"),
            "AND(X [NOT BETWEEN] 1 AND 5, Y [=] lit:-2, OR(Z [BETWEEN] A AND B, Q))"
        );
        assert_eq!(w(""), "");
    }

    #[test]
    fn condition_sites() {
        assert_eq!(
            conds(
                "SELECT a FROM t JOIN u ON t.k = u.k AND u.x > 1 LEFT JOIN v ON v.k = t.k WHERE a = 1 GROUP BY a HAVING count(*) > 2"
            ),
            [
                "On: AND(T.K [=] U.K, U.X [>] lit:1)",
                "On: V.K [=] T.K",
                "Where: A [=] lit:1",
                "Having: COUNT(*) [>] lit:2",
            ]
        );
        assert_eq!(
            conds(
                "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED AND s.d = 1 THEN UPDATE SET t.x = s.x WHEN NOT MATCHED THEN INSERT *"
            ),
            ["On: T.ID [=] S.ID", "When: S.D [=] lit:1"]
        );
        assert_eq!(
            conds(
                "SELECT CASE WHEN a > 1 THEN 'x' END FROM t WHERE k IN (SELECT k FROM u WHERE v < 0)"
            ),
            [
                "When: A [>] lit:1",
                "Where: K [IN] (SELECT K FROM U WHERE V < 0)",
                "Where: V [<] lit:0",
            ]
        );
        // ON without a join is no condition; a function named `left` is no join.
        assert!(conds("GRANT SELECT ON TABLE t TO u").is_empty());
        assert_eq!(
            conds("SELECT * FROM a JOIN b ON left(a.k, 2) = b.k"),
            ["On: LEFT(A.K, 2) [=] B.K"]
        );
    }

    #[test]
    fn assignment_runs() {
        let runs = |s: &str| -> Vec<String> {
            let t = SqlText::new(s);
            t.clauses()
                .ops
                .iter()
                .map(|r| {
                    let op = &t.code[t.toks[r.range.start].start..t.toks[r.range.end - 1].end];
                    format!("{}{op}", if r.assignment { "set" } else { "" })
                })
                .collect()
        };
        assert_eq!(
            runs("UPDATE t SET a = 1, b = CASE WHEN c = 2 THEN 3 END WHERE d <> 4"),
            ["set=", "set=", "=", "<>"]
        );
        assert_eq!(runs("ALTER TABLE t SET TBLPROPERTIES ('k' = 1)"), ["set="]);
        assert_eq!(
            runs("CREATE TABLE t USING delta OPTIONS (path = 'x') AS SELECT a == b FROM s"),
            ["set=", "=="]
        );
        assert_eq!(runs("SET x = 1; SELECT a >= b"), ["set=", ">="]);
    }

    #[test]
    fn simple_and_strings_comments() {
        assert_eq!(
            render("select a, f(b, c) as x from t where y = 'from z' -- group by q\n"),
            ["0: Select A, F(B, C) AS X | From T | Where Y = '      '"]
        );
        assert_eq!(
            render("SELECT /* FROM x */ a FROM `from` WHERE b IS DISTINCT FROM c"),
            ["0: Select A | From `    ` | Where B IS DISTINCT FROM C"]
        );
        // Function-call FROM is not a clause.
        assert_eq!(
            render("SELECT extract(year FROM d), trim(BOTH ' ' FROM s) FROM t"),
            ["0: Select EXTRACT(YEAR FROM D), TRIM(BOTH ' ' FROM S) | From T"]
        );
        // A fragment without SELECT has no query block.
        assert!(render("a.k = b.k AND x > 1").is_empty());
        assert_eq!(render(""), Vec::<String>::new());
    }

    #[test]
    fn nesting_ctes_and_subqueries() {
        assert_eq!(
            render(
                "WITH c AS (SELECT k FROM raw GROUP BY k) SELECT * FROM c WHERE k IN (SELECT k FROM d)"
            ),
            [
                "0: Select * | From C | Where K IN (SELECT K FROM D)",
                "1: Select K | From RAW | GroupBy K",
                "1: Select K | From D",
            ]
        );
        assert_eq!(
            render("SELECT count(*) FROM (SELECT a FROM t ORDER BY a LIMIT 5) s"),
            [
                "0: Select COUNT(*) | From (SELECT A FROM T ORDER BY A LIMIT 5) S",
                "1: Select A | From T | OrderBy A | Limit 5",
            ]
        );
        // FROM-first subquery and a query group inside a group.
        assert_eq!(
            render("SELECT * FROM ((SELECT a FROM t) ORDER BY rand())"),
            [
                "0: Select * | From ((SELECT A FROM T) ORDER BY RAND())",
                "1: OrderBy RAND()",
                "2: Select A | From T",
            ]
        );
        // Unclosed groups still parse.
        assert_eq!(
            render("SELECT a FROM (SELECT b FROM t"),
            [
                "0: Select A | From (SELECT B FROM T",
                "1: Select B | From T"
            ]
        );
    }

    #[test]
    fn set_operations_and_statements() {
        assert_eq!(
            render("SELECT a FROM t UNION ALL SELECT b FROM u ORDER BY 1 LIMIT 10;"),
            [
                "0: Select A | From T",
                "0: Select B | From U | OrderBy 1 | Limit 10",
            ]
        );
        assert_eq!(
            render("SELECT * EXCEPT (b) FROM t EXCEPT SELECT * FROM u"),
            ["0: Select * EXCEPT (B) | From T", "0: Select * | From U"]
        );
        assert_eq!(
            render("INSERT INTO t SELECT a FROM s; DELETE FROM t WHERE k = 1"),
            ["0: Select A | From S", "0: From T | Where K = 1"]
        );
        // A column named `limit` / `window` is no clause.
        assert_eq!(
            render("SELECT limit, window FROM t LIMIT {}"),
            ["0: Select LIMIT, WINDOW | From T | Limit {}"]
        );
    }

    #[test]
    fn grouping_clauses() {
        assert_eq!(
            render("SELECT a FROM t GROUP BY a, ROLLUP(b, c), CUBE(d, e) HAVING count(*) > 1"),
            ["0: Select A | From T | GroupBy A, ROLLUP(B, C), CUBE(D, E) | Having COUNT(*) > 1"]
        );
        assert_eq!(
            render("SELECT 1 FROM t GROUP BY GROUPING SETS ((a, b), (a), ())"),
            ["0: Select 1 | From T | GroupBy GROUPING SETS ((A, B), (A), ())"]
        );
        assert_eq!(
            render("SELECT a FROM t GROUP BY a, b WITH ROLLUP ORDER BY a"),
            ["0: Select A | From T | GroupBy A, B WITH ROLLUP | OrderBy A"]
        );
        let t = SqlText::new("CUBE((a, b), f(c, d), 'x,y', )");
        assert_eq!(t.clauses().group_at(1).map(Group::item_count), Some(3));
    }

    #[test]
    fn window_specs() {
        assert_eq!(
            render(
                "SELECT collect_list(v) FILTER (WHERE v > 0) OVER (PARTITION BY k, j ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
            ),
            [
                "0: Select COLLECT_LIST(V) FILTER (WHERE V > 0) OVER (PARTITION BY K, J ORDER BY TS ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) | From T",
                "OVER COLLECT_LIST | PARTITION K, J | ORDER TS | FRAME ROWS",
            ]
        );
        // The window's ORDER BY is not the query's.
        assert_eq!(
            render(
                "SELECT rank() OVER w FROM t WINDOW w AS (DISTRIBUTE BY k SORT BY rand()) ORDER BY 1"
            ),
            [
                "0: Select RANK() OVER W | From T | Window W AS (DISTRIBUTE BY K SORT BY RAND()) | OrderBy 1",
                "WINDOW | PARTITION K | ORDER RAND()",
            ]
        );
        assert_eq!(render("max(x) over ()"), ["OVER MAX"]);
        // `window(ts, ..)` is a function, not a WINDOW clause.
        assert_eq!(
            render("SELECT window(ts, '5 minutes') FROM t GROUP BY window(ts, '5 minutes')"),
            ["0: Select WINDOW(TS, '         ') | From T | GroupBy WINDOW(TS, '         ')"]
        );
    }
}
