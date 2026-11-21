//! COR003 bitwise-comparison-precedence, COR007 discarded-transformation,
//! COR010 union-by-position, COR016 regex-escape.

use ruff_python_ast::{self as ast, AnyNodeRef, Expr, Operator, Stmt};
use ruff_text_size::{Ranged, TextRange, TextSize};
use sparklint_semantic::call_run::run_of_ty;
use sparklint_semantic::literal::{Escape, Reader, escapes, pieces};
use sparklint_semantic::transform;
use sparklint_semantic::{Confidence, Ty};

use super::Rule;
use super::describe;
use super::helpers::*;
use super::msg::Msg;
use crate::checker::Checker;
use crate::diagnostic::{Edit, Fix, Severity};
use crate::fix_text::FixText;
use crate::msg;

/// COR003/007/010/016 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("COR003").expr(cor003_expr).msgs(&[COR003]),
    Rule::new("COR007")
        .stmt(cor007_stmt)
        .msgs(&[COR007, COR007_UNRUN]),
    Rule::new("COR010")
        .call(union_by_position)
        .msgs(&[COR010, COR010_REDUCE]),
    Rule::new("COR016")
        .call(suspicious_regex_escape)
        .expr(cor016_expr)
        .msgs(&[COR016_INVALID, COR016_REGEX, COR016_GROUP]),
];

const COR003: Msg = Msg::new(
    "COR003",
    "precedence",
    "& / | bind tighter than comparisons: `a > 1 & b < 2` parses as `a > (1 & b) < 2` — parenthesise each comparison.",
)
.help("(F.col(\"a\") > 1) & (F.col(\"b\") < 2)");
const COR007: Msg = Msg::new(
    "COR007",
    "discarded",
    "Result of {call} is discarded — DataFrames are immutable; assign it.",
)
.help("df = df.<transformation>(...)");
const COR007_UNRUN: Msg = Msg::new(
    "COR007",
    "never-runs",
    "{call} sets up {what} that never runs — nothing happens until {finish} is called.",
)
.help("Finish the chain in the same statement, e.g. df.write.mode(\"overwrite\").saveAsTable(\"t\"), or remove it.");
const COR010: Msg = Msg::new(
    "COR010",
    "union",
    ".{method}() matches columns by position — a different column order silently puts values in the wrong columns; use unionByName().",
)
.help("a.unionByName(b) (allowMissingColumns=True when one side lacks columns), or select the same column list on both sides first.");
const COR010_REDUCE: Msg = Msg::new(
    "COR010",
    "reduce-union",
    "reduce(DataFrame.union, …) matches columns by position — use DataFrame.unionByName.",
)
.help("reduce(DataFrame.unionByName, frames)");
const COR016_INVALID: Msg = Msg::new(
    "COR016",
    "invalid-escape",
    "Invalid escape sequence \"{escape}\" in a non-raw string — a SyntaxWarning today, an error in future Python; use r\"...\".",
)
.help("Prefix the literal with r (r\"\\d+\"): a raw string keeps every backslash for the regex engine.");
const COR016_REGEX: Msg = Msg::new(
    "COR016",
    "regex-control-character",
    "\"{escape}\" in a regex passed as a non-raw string is a control character, not a regex escape — use r\"...\".",
)
.help("r\"\\bword\\b\": in a raw string \\b reaches the regex engine as a word boundary; in a plain string it is a backspace.");
const COR016_GROUP: Msg = Msg::new(
    "COR016",
    "regex-group-reference",
    "\"{escape}\" in a regex passed as a non-raw string is the control character {char}, not a group reference — use r\"...\".",
)
.help("r\"\\1\" (or \"\\\\g<1>\" in a replacement): only a raw string or a doubled backslash passes the group number to the regex engine.");

fn cor003_expr(ch: &mut Checker, expr: &Expr) {
    if let Expr::Compare(c) = expr {
        bitwise_comparison_precedence(ch, c, expr);
    }
}

fn cor007_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::Expr(e) = stmt
        && !expects_exception(ch, stmt)
    {
        discarded_transformation(ch, &e.value);
    }
}

/// `stmt` sits directly in a `try:` body with an `except` handler or in a
/// `with pytest.raises(..)` / `with self.assertRaises(..)` block: Classic Spark
/// analyses a transformation eagerly, so a discarded `df.select("c")` there is a
/// deliberate probe for the error (missing column, bad argument).
fn expects_exception(ch: &Checker, stmt: &Stmt) -> bool {
    ch.run_at(stmt.range()).is_probe()
}

fn cor016_expr(ch: &mut Checker, expr: &Expr) {
    if matches!(
        expr,
        Expr::StringLiteral(_) | Expr::BytesLiteral(_) | Expr::FString(_)
    ) {
        invalid_escape(ch, expr);
    }
}

// ------------------------------------------------------------------ COR003

/// Whether `e` (an operand of `parent`, a comparison or a bitwise operation) is an
/// unparenthesised `a & b` / `a | b`.
fn bare_bitwise<'e>(
    ch: &Checker,
    e: &'e Expr,
    parent: impl Into<AnyNodeRef<'e>>,
) -> Option<&'e ast::ExprBinOp> {
    let Expr::BinOp(b) = e else { return None };
    if !matches!(b.op, Operator::BitAnd | Operator::BitOr) {
        return None;
    }
    // Parenthesised in the source (comments and line breaks inside the pair
    // included): `a > (1 & b)` is deliberate.
    (!ch.fix_text().is_parenthesized(e, parent)).then_some(b)
}

/// A Column, also when the type is only inferred from usage (a parameter frame's
/// `frame.a`): the shape `x > 1 & y < 2` is a bug for any non-scalar operand.
fn column(ch: &Checker, e: &Expr) -> bool {
    ch.model.ty_of(e).is_column()
}

pub fn bitwise_comparison_precedence(ch: &mut Checker, c: &ast::ExprCompare, expr: &Expr) {
    let operands: Vec<&Expr> = c.operands.iter().collect();
    let bitwise: Vec<Option<&ast::ExprBinOp>> =
        operands.iter().map(|o| bare_bitwise(ch, o, c)).collect();
    if bitwise.iter().all(Option::is_none) {
        return;
    }
    // A Column inside the `&` / `|`: `a > 1 & b` computes `1 & b`, never meant.
    let column_in_bitwise = bitwise
        .iter()
        .flatten()
        .any(|b| column(ch, &b.left) || column(ch, &b.right));
    // A chained comparison over a Column calls bool() on a Column and raises. A single
    // comparison against `x | y` of non-Columns (`F.col("mode") == READ | WRITE`) is
    // the integer-flag idiom and does what it says.
    let chained_over_column = operands.len() > 2 && operands.iter().any(|o| column(ch, o));
    if !column_in_bitwise && !chained_over_column {
        return;
    }
    let mut d = msg!(COR003, expr.range());
    if let Some(edits) = regroup(ch, c, &bitwise) {
        d = d.with_fix(Fix::safe("Parenthesise the comparisons", edits));
    }
    ch.report(d);
}

/// Parenthesise `o0 op0 o1 op1 o2 …` split at the bare `&`/`|` operands. Only
/// inserts parentheses: operands, comments and line breaks stay as written (an
/// operand already in its own parentheses keeps them; the new pair goes around it).
fn regroup(
    ch: &Checker,
    c: &ast::ExprCompare,
    bitwise: &[Option<&ast::ExprBinOp>],
) -> Option<Vec<Edit>> {
    let ft = ch.fix_text();
    let nested = |e: &Expr, b: &ast::ExprBinOp| bare_bitwise(ch, e, b).is_some();
    // Each group is the (start, end, atoms) of a run of comparison atoms.
    let mut groups: Vec<(TextSize, TextSize, usize)> = Vec::new();
    let mut cur: Option<(TextSize, TextSize, usize)> = None;
    let push = |cur: &mut Option<(TextSize, TextSize, usize)>, r: TextRange| {
        *cur = Some(match *cur {
            None => (r.start(), r.end(), 1),
            Some((s, _, n)) => (s, r.end(), n + 1),
        });
    };
    for (i, o) in c.operands.iter().enumerate() {
        match bitwise[i] {
            Some(b) => {
                if nested(&b.left, b) || nested(&b.right, b) {
                    return None;
                }
                push(&mut cur, ft.own_range(&b.left, b));
                groups.extend(cur.take());
                push(&mut cur, ft.own_range(&b.right, b));
            }
            None => push(&mut cur, ft.own_range(o, c)),
        }
    }
    groups.extend(cur.take());
    let edits: Vec<Edit> = groups
        .into_iter()
        .filter(|&(_, _, n)| n > 1)
        .flat_map(|(s, e, _)| FixText::wrap_range(TextRange::new(s, e)))
        .collect();
    (!edits.is_empty()).then_some(edits)
}

// ------------------------------------------------------------------ COR007

pub fn discarded_transformation(ch: &mut Checker, value: &Expr) {
    let sev = if ch.is_notebook() {
        Severity::Warning
    } else {
        Severity::Error
    };
    if let Some(u) = ch.model.unrun_builder(value) {
        let call = describe::call(ch, value);
        ch.report(
            msg!(
                COR007_UNRUN,
                method_range(value),
                call = call,
                what = u.what(),
                finish = u.finish()
            )
            .with_severity(sev),
        );
        return;
    }
    let Some(sc) = ch.model.spark_call(value) else {
        return;
    };
    let (c, m) = (sc.call, sc.method);
    // `checkpoint(eager=False)` / `localCheckpoint(eager=False)` only mark the frame
    // they return: discarded, nothing is checkpointed.
    let lazy_checkpoint = transform::checkpoint_runs_now(&sc) == Some(false);
    if (transform::of_call(&sc).is_some_and(|r| r.side_effect()) && !lazy_checkpoint)
        || c.arguments
            .keywords
            .iter()
            .any(|k| k.arg.as_ref().is_some_and(|a| a.as_str() == "inplace"))
    {
        return;
    }
    let rt = sc.receiver_ty;
    if !matches!(
        rt.ty,
        Ty::DataFrame | Ty::GroupedData | Ty::NaFunctions | Ty::StatFunctions
    ) {
        return;
    }
    if run_of_ty(rt.ty, m).is_some() && !lazy_checkpoint {
        return;
    }
    let out = ch.model.ty_of(value);
    if !matches!(out.ty, Ty::DataFrame | Ty::GroupedData) {
        return;
    }
    // Heuristic receivers (a name that merely looks like a DataFrame) need a
    // DataFrame-only method as evidence: pandas has drop/fillna/rename too.
    if rt.confidence == Confidence::Likely && !sparklint_semantic::evidence::is_dataframe_only(m) {
        return;
    }
    let call = describe::call(ch, value);
    ch.report(msg!(COR007, method_range(value), call).with_severity(sev));
}

// ------------------------------------------------------------------ COR010

pub fn union_by_position(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    if let Some(q) = ch.model.qualified_name(&c.func)
        && q == "functools.reduce"
        && let Some(f) = c.arguments.args.first()
        && let Some(fq) = ch.model.qualified_name(f)
        && matches!(
            fq.as_str(),
            "pyspark.sql.DataFrame.union" | "pyspark.sql.DataFrame.unionAll"
        )
    {
        ch.report(msg!(COR010_REDUCE, f.range()));
        return;
    }
    let Some(sc) = ch.model.spark_call_named(expr, &["union", "unionAll"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let (recv, m) = (sc.receiver, sc.method);
    let Some(other) = sc.arg(0, "other") else {
        return;
    };
    if !ch.model.is_dataframe(other) && !ch.model.ty_of(other).is_unknown() {
        return;
    }
    // Both sides have the same columns in the same order by construction: the same
    // frame after rows-only steps, the same column list selected, or one side
    // re-selected with the other's `.columns` (the oracle's columns fact).
    if ch
        .model
        .frame_columns(recv)
        .agrees(ch.model.frame_columns(other))
    {
        return;
    }
    let mut d = msg!(COR010, method_range(expr), method = m);
    if let Expr::Attribute(a) = &*c.func {
        d = d.with_fix(Fix::unsafe_(
            "Replace with `unionByName`",
            vec![Edit::replace(a.attr.range, "unionByName")],
        ));
    }
    ch.report(d);
}

// ------------------------------------------------------------------ COR016

/// Offset of the first invalid escape (`\d`) in a non-raw literal body.
fn first_invalid(body: &str, bytes: bool) -> Option<usize> {
    escapes(body, bytes).find(|e| !e.valid).map(|e| e.at)
}

/// How the regex engine reads a string argument.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RegexRole {
    /// A Python `re` pattern.
    Python,
    /// A Spark (Java) pattern.
    Java,
    /// An `re.sub` / `re.subn` replacement template.
    PythonRepl,
}

/// Spark/`re` callables: their regex arguments (keyword or positional) and how
/// the engine reads each.
fn regex_args<'e>(ch: &Checker, c: &'e ast::ExprCall, expr: &Expr) -> Vec<(RegexRole, &'e Expr)> {
    let args = &c.arguments;
    let one = |role, kw, i| {
        args.find_argument_value(kw, i)
            .map(|a| (role, a))
            .into_iter()
            .collect::<Vec<_>>()
    };
    if let Some(q) = ch.model.qualified_name(&c.func) {
        if let Some(f) = q.strip_prefix("pyspark.sql.functions.") {
            return match f {
                "regexp_replace" | "regexp_extract" | "split" => one(RegexRole::Java, "pattern", 1),
                "regexp_extract_all" | "regexp_like" | "regexp" | "rlike" | "regexp_count"
                | "regexp_substr" | "regexp_instr" => one(RegexRole::Java, "regexp", 1),
                _ => Vec::new(),
            };
        }
        if let Some(f) = q.strip_prefix("re.") {
            let mut out = match f {
                "compile" | "match" | "search" | "fullmatch" | "sub" | "subn" | "split"
                | "findall" | "finditer" => one(RegexRole::Python, "pattern", 0),
                _ => Vec::new(),
            };
            if matches!(f, "sub" | "subn") {
                out.extend(one(RegexRole::PythonRepl, "repl", 1));
            }
            return out;
        }
    }
    if let Some(sc) = ch.model.spark_call_named(expr, &["rlike", "regexp"])
        && sc.class() == Ty::Column
    {
        return one(RegexRole::Java, "other", 0);
    }
    Vec::new()
}

/// A valid escape that means something else to the regex engine than the
/// character Python puts in the string.
enum Suspicious {
    /// `\b` (word boundary vs backspace); `\v` in a Java pattern (vertical
    /// whitespace class vs one vertical tab).
    Control,
    /// `\1`..`\77` (group reference vs an octal character).
    Group(u32),
}

/// First suspicious escape of a non-raw literal body for `role`: (offset, length, kind).
fn suspicious_escape(body: &str, role: RegexRole) -> Option<(usize, usize, Suspicious)> {
    let b = body.as_bytes();
    let octal = |i: usize| b.get(i).is_some_and(|d| (b'0'..=b'7').contains(d));
    for Escape { at: i, next: n, .. } in escapes(body, false) {
        match n {
            b'b' if role != RegexRole::PythonRepl => return Some((i, 2, Suspicious::Control)),
            b'v' if role == RegexRole::Java => return Some((i, 2, Suspicious::Control)),
            // Three octal digits are an octal escape to the regex engine as well.
            b'1'..=b'7' if !(octal(i + 2) && octal(i + 3)) => {
                let len = if octal(i + 2) { 3 } else { 2 };
                let value = u32::from_str_radix(&body[i + 1..i + len], 8).unwrap_or(0);
                return Some((i, len, Suspicious::Group(value)));
            }
            _ => {}
        }
    }
    None
}

fn report_invalid(
    ch: &mut Checker,
    body_start: TextSize,
    body: &str,
    at: usize,
    fix_at: Option<TextSize>,
) {
    let esc_start = body_start + TextSize::from(at as u32);
    let shown: String = body[at..].chars().take(2).collect();
    let range = TextRange::new(esc_start, esc_start + TextSize::from(shown.len() as u32));
    let mut d = msg!(COR016_INVALID, range, escape = shown);
    if let Some(at) = fix_at {
        d = d.with_fix(Fix::safe(
            "Make it a raw string",
            vec![Edit::insert(at, "r")],
        ));
    }
    ch.report(d);
}

/// Every non-raw string / bytes / f-string literal piece with an invalid escape
/// (`"\d"`); fixed with an `r` prefix when that keeps the value.
pub fn invalid_escape(ch: &mut Checker, expr: &Expr) {
    let source = ch.source.text.as_str();
    for p in pieces(expr) {
        if !p.is_text() || p.prefix.raw {
            continue;
        }
        let body = p.text(source);
        let Some(at) = first_invalid(body, p.prefix.bytes) else {
            continue;
        };
        let fix_at = p.raw_prefix_at(source, Reader::Python);
        report_invalid(ch, p.range.start(), body, at, fix_at);
    }
}

/// Valid-but-wrong escapes (`"\b"` = backspace, `"\1"` = chr(1)) in a regex argument;
/// fixed (unsafe) with an `r` prefix when the regex engine then reads every escape as
/// written. One finding per f-string (never fixed: the prefix is shared).
pub fn suspicious_regex_escape(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let source = ch.source.text.as_str();
    for (role, arg) in regex_args(ch, c, expr) {
        let mut fstring_reported = false;
        for p in pieces(arg) {
            if !p.is_text() || p.prefix.raw || (p.prefix.fstring && fstring_reported) {
                continue;
            }
            let body = p.text(source);
            if first_invalid(body, p.prefix.bytes).is_some() {
                continue; // reported by invalid_escape
            }
            let Some(found) = suspicious_escape(body, role) else {
                continue;
            };
            fstring_reported |= p.prefix.fstring;
            let fix_at = p.raw_prefix_at(source, Reader::Regex);
            report_suspicious(ch, p.range.start(), body, found, fix_at);
        }
    }
}

fn report_suspicious(
    ch: &mut Checker,
    body_start: TextSize,
    body: &str,
    (at, len, kind): (usize, usize, Suspicious),
    fix_at: Option<TextSize>,
) {
    let esc_start = body_start + TextSize::from(at as u32);
    let shown = &body[at..at + len];
    let range = TextRange::new(esc_start, esc_start + TextSize::from(len as u32));
    let mut d = match kind {
        Suspicious::Control => msg!(COR016_REGEX, range, escape = shown),
        Suspicious::Group(v) => {
            let char = format!("\\x{v:02x}");
            msg!(COR016_GROUP, range, escape = shown, char = char)
        }
    };
    if let Some(at) = fix_at {
        d = d.with_fix(Fix::unsafe_(
            "Make it a raw string",
            vec![Edit::insert(at, "r")],
        ));
    }
    ch.report(d);
}
