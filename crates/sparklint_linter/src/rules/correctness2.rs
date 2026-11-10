//! COR003 bitwise-comparison-precedence, COR007 discarded-transformation,
//! COR010 union-by-position, COR016 regex-escape.

use ruff_python_ast::{self as ast, AnyStringFlags, Expr, Operator, Stmt, StringFlags};
use ruff_text_size::{Ranged, TextRange, TextSize};
use sparklint_semantic::api::{DATAFRAME_ONLY_METHODS, action_kind};
use sparklint_semantic::{Confidence, Ty};

use super::Rule;
use super::actions::method_range;
use super::helpers::*;
use crate::checker::Checker;
use crate::diagnostic::{Diagnostic, Edit, Fix, Severity};

/// COR003/007/010/016 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[
    Rule::new("COR003").expr(cor003_expr),
    Rule::new("COR007").stmt(cor007_stmt),
    Rule::new("COR010").call(union_by_position),
    Rule::new("COR016")
        .call(suspicious_regex_escape)
        .expr(cor016_expr),
];

fn cor003_expr(ch: &mut Checker, expr: &Expr) {
    if let Expr::Compare(c) = expr {
        bitwise_comparison_precedence(ch, c, expr);
    }
}

fn cor007_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::Expr(e) = stmt {
        discarded_transformation(ch, &e.value);
    }
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

/// Whether `e` (an operand of a comparison) is an unparenthesised `a & b` / `a | b`.
fn bare_bitwise<'e>(ch: &Checker, e: &'e Expr) -> Option<&'e ast::ExprBinOp> {
    let Expr::BinOp(b) = e else { return None };
    if !matches!(b.op, Operator::BitAnd | Operator::BitOr) {
        return None;
    }
    // Parenthesised in the source: `a > (1 & b)` is deliberate.
    let text = &ch.source.text;
    let before = text[..usize::from(e.start())].trim_end();
    let after = text[usize::from(e.end())..].trim_start();
    if before.ends_with('(') && after.starts_with(')') {
        return None;
    }
    Some(b)
}

fn certain_column(ch: &Checker, e: &Expr) -> bool {
    let t = ch.model.ty_of(e);
    t.is_column() && t.is_certain()
}

pub fn bitwise_comparison_precedence(ch: &mut Checker, c: &ast::ExprCompare, expr: &Expr) {
    let operands: Vec<&Expr> = c.operands.iter().collect();
    let bitwise: Vec<Option<&ast::ExprBinOp>> =
        operands.iter().map(|o| bare_bitwise(ch, o)).collect();
    if bitwise.iter().all(Option::is_none) {
        return;
    }
    // A Column must be involved (otherwise it is integer/bool arithmetic).
    let involves_column = operands.iter().any(|o| certain_column(ch, o))
        || bitwise
            .iter()
            .flatten()
            .any(|b| certain_column(ch, &b.left) || certain_column(ch, &b.right));
    if !involves_column {
        return;
    }
    let mut d = Diagnostic::new(
        "COR003",
        "& / | bind tighter than comparisons: `a > 1 & b < 2` parses as `a > (1 & b) < 2` — parenthesise each comparison.",
        expr.range(),
    );
    if let Some(text) = regroup(ch, c, &operands, &bitwise) {
        d = d.with_fix(Fix::safe(
            "Parenthesise the comparisons",
            vec![Edit::replace(expr.range(), text)],
        ));
    }
    ch.report(d);
}

/// Rebuild `o0 op0 o1 op1 o2 …` splitting at the bare `&`/`|` operands.
fn regroup(
    ch: &Checker,
    c: &ast::ExprCompare,
    operands: &[&Expr],
    bitwise: &[Option<&ast::ExprBinOp>],
) -> Option<String> {
    let src = |e: &Expr| ch.slice(e.range()).to_string();
    let nested = |e: &Expr| bare_bitwise(ch, e).is_some();
    let mut out = String::new();
    let mut group: Vec<String> = Vec::new();
    let close = |group: &mut Vec<String>, out: &mut String| {
        let g = group.join(" ");
        if group.len() == 1 {
            out.push_str(&g);
        } else {
            out.push('(');
            out.push_str(&g);
            out.push(')');
        }
        group.clear();
    };
    for (i, o) in operands.iter().enumerate() {
        if i > 0 {
            group.push(c.ops[i - 1].as_str().to_string());
        }
        match bitwise[i] {
            Some(b) => {
                if nested(&b.left) || nested(&b.right) {
                    return None;
                }
                group.push(src(&b.left));
                close(&mut group, &mut out);
                out.push_str(if b.op == Operator::BitAnd {
                    " & "
                } else {
                    " | "
                });
                group.push(src(&b.right));
            }
            None => group.push(src(o)),
        }
    }
    close(&mut group, &mut out);
    (!out.contains('\n')).then_some(out)
}

// ------------------------------------------------------------------ COR007

/// Methods whose call has an effect even when the result is discarded.
const EFFECTFUL: &[&str] = &[
    "cache",
    "persist",
    "unpersist",
    "checkpoint",
    "localCheckpoint",
    "createOrReplaceTempView",
    "createTempView",
    "createGlobalTempView",
    "createOrReplaceGlobalTempView",
    "registerTempTable",
    "printSchema",
    "explain",
    "show",
    "display",
    "foreach",
    "foreachPartition",
    "writeTo",
    "observe",
    "hint",
];

pub fn discarded_transformation(ch: &mut Checker, value: &Expr) {
    let Some(sc) = ch.model.spark_call(value) else {
        return;
    };
    let (c, recv, m) = (sc.call, sc.receiver, sc.method);
    if EFFECTFUL.contains(&m)
        || c.arguments
            .keywords
            .iter()
            .any(|k| k.arg.as_ref().is_some_and(|a| a.as_str() == "inplace"))
    {
        return;
    }
    let rt = sc.receiver_ty;
    if !matches!(rt.ty, Ty::DataFrame | Ty::GroupedData | Ty::NaFunctions) {
        return;
    }
    if action_kind(rt.ty, m).is_some() {
        return;
    }
    let out = ch.model.ty_of(value);
    if !matches!(out.ty, Ty::DataFrame | Ty::GroupedData) {
        return;
    }
    // Heuristic receivers (a name that merely looks like a DataFrame) need a
    // DataFrame-only method as evidence: pandas has drop/fillna/rename too.
    if rt.confidence == Confidence::Likely && !DATAFRAME_ONLY_METHODS.contains(&m) {
        return;
    }
    let sev = if ch.is_notebook() {
        Severity::Warning
    } else {
        Severity::Error
    };
    ch.report(
        Diagnostic::new(
            "COR007",
            format!(
                "Result of {}.{m}() is discarded — DataFrames are immutable; assign it.",
                short(ch, recv)
            ),
            method_range(value),
        )
        .with_severity(sev)
        .with_help("df = df.<transformation>(...)"),
    );
}

// ------------------------------------------------------------------ COR010

fn same_select(ch: &Checker, a: &Expr, b: &Expr) -> bool {
    match (method_call(a), method_call(b)) {
        (Some((ca, _, "select")), Some((cb, _, "select"))) => {
            ch.slice(ca.arguments.range) == ch.slice(cb.arguments.range)
                && ca.arguments.args.iter().all(|x| x.is_string_literal_expr())
        }
        _ => false,
    }
}

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
        ch.report(Diagnostic::new(
            "COR010",
            "reduce(DataFrame.union, …) matches columns by position — use DataFrame.unionByName.",
            f.range(),
        ));
        return;
    }
    let Some(sc) = ch.model.spark_call_named(expr, &["union", "unionAll"]) else {
        return;
    };
    if !sc.is_dataframe() {
        return;
    }
    let (recv, m) = (sc.receiver, sc.method);
    let Some(other) = c.arguments.args.first() else {
        return;
    };
    if !ch.model.is_dataframe(other) && !ch.model.ty_of(other).is_unknown() {
        return;
    }
    let mut d = Diagnostic::new(
        "COR010",
        format!(
            ".{m}() matches columns by position — a different column order silently puts values in the wrong columns; use unionByName()."
        ),
        method_range(expr),
    );
    if same_select(ch, recv, other) {
        d = d.with_severity(Severity::Info);
    }
    if let Expr::Attribute(a) = &*c.func {
        d = d.with_fix(Fix::unsafe_(
            "Replace with `unionByName`",
            vec![Edit::replace(a.attr.range, "unionByName")],
        ));
    }
    ch.report(d);
}

// ------------------------------------------------------------------ COR016

/// Escape scan of one literal's body: (offset of first invalid escape, any valid
/// escape, suspicious control escapes \b \a \f \v \0).
struct EscapeScan {
    invalid: Option<usize>,
    valid: bool,
    suspicious: Option<usize>,
}

fn scan_escapes(body: &str, bytes: bool) -> EscapeScan {
    let b = body.as_bytes();
    let mut i = 0;
    let mut out = EscapeScan {
        invalid: None,
        valid: false,
        suspicious: None,
    };
    while i < b.len() {
        if b[i] != b'\\' {
            i += 1;
            continue;
        }
        let Some(&n) = b.get(i + 1) else { break };
        let valid = match n {
            b'\n'
            | b'\r'
            | b'\\'
            | b'\''
            | b'"'
            | b'a'
            | b'b'
            | b'f'
            | b'n'
            | b'r'
            | b't'
            | b'v'
            | b'x'
            | b'0'..=b'7' => true,
            b'N' | b'u' | b'U' => !bytes,
            _ => false,
        };
        if valid {
            out.valid = true;
            if matches!(n, b'a' | b'b' | b'f' | b'v' | b'0') && out.suspicious.is_none() {
                out.suspicious = Some(i);
            }
        } else if out.invalid.is_none() {
            out.invalid = Some(i);
        }
        i += 2;
    }
    out
}

/// Spark/`re` callables whose given argument is a regex pattern.
fn regex_arg_index(ch: &Checker, c: &ast::ExprCall, expr: &Expr) -> Option<usize> {
    if let Some(q) = ch.model.qualified_name(&c.func) {
        if let Some(f) = q.strip_prefix("pyspark.sql.functions.") {
            return match f {
                "regexp_replace" | "regexp_extract" | "regexp_extract_all" | "split"
                | "regexp_like" | "regexp" | "rlike" | "regexp_count" | "regexp_substr"
                | "regexp_instr" => Some(1),
                _ => None,
            };
        }
        if let Some(f) = q.strip_prefix("re.") {
            return matches!(
                f,
                "compile"
                    | "match"
                    | "search"
                    | "fullmatch"
                    | "sub"
                    | "subn"
                    | "split"
                    | "findall"
                    | "finditer"
            )
            .then_some(0);
        }
    }
    if let Some(sc) = ch.model.spark_call_named(expr, &["rlike", "regexp"])
        && sc.class() == Ty::Column
    {
        return Some(0);
    }
    None
}

struct Part {
    range: TextRange,
    flags: AnyStringFlags,
}

fn string_parts(e: &Expr) -> Vec<Part> {
    match e {
        Expr::StringLiteral(s) => s
            .value
            .iter()
            .map(|p| Part {
                range: p.range,
                flags: p.flags.as_any_string_flags(),
            })
            .collect(),
        Expr::BytesLiteral(s) => s
            .value
            .iter()
            .map(|p| Part {
                range: p.range,
                flags: p.flags.as_any_string_flags(),
            })
            .collect(),
        _ => Vec::new(),
    }
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
    let mut d = Diagnostic::new(
        "COR016",
        format!(
            "Invalid escape sequence \"{shown}\" in a non-raw string — a SyntaxWarning today, an error in future Python; use r\"...\"."
        ),
        TextRange::new(esc_start, esc_start + TextSize::from(shown.len() as u32)),
    );
    if let Some(at) = fix_at {
        d = d.with_fix(Fix::safe(
            "Make it a raw string",
            vec![Edit::insert(at, "r")],
        ));
    }
    ch.report(d);
}

/// Every non-raw string / bytes / f-string literal with an invalid escape (`"\d"`).
pub fn invalid_escape(ch: &mut Checker, expr: &Expr) {
    let mut parts = string_parts(expr);
    if let Expr::FString(f) = expr {
        for part in f.value.iter() {
            match part {
                ast::FStringPartRef::Literal(l) => parts.push(Part {
                    range: l.range,
                    flags: l.flags.as_any_string_flags(),
                }),
                ast::FStringPartRef::FString(fs) => {
                    if fs.flags.prefix().is_raw() {
                        continue;
                    }
                    for el in &fs.elements {
                        if let ast::InterpolatedStringElement::Literal(l) = el {
                            let body = ch.slice(l.range);
                            if let Some(at) = scan_escapes(body, false).invalid {
                                report_invalid(ch, l.range.start(), body, at, None);
                            }
                        }
                    }
                }
            }
        }
    }
    let bytes = matches!(expr, Expr::BytesLiteral(_));
    for part in parts {
        if part.flags.prefix().is_raw() {
            continue;
        }
        let open = part.flags.opener_len();
        let close = part.flags.closer_len();
        if part.range.len() < open + close {
            continue;
        }
        let body_range = TextRange::new(part.range.start() + open, part.range.end() - close);
        let body = ch.slice(body_range);
        let scan = scan_escapes(body, bytes);
        let Some(at) = scan.invalid else { continue };
        // A raw prefix keeps the value only when no valid escape is present.
        let prefix = part.flags.prefix().as_str();
        let fix_at = (!scan.valid && !prefix.contains(['u', 'U']) && !body.ends_with('\\'))
            .then_some(part.range.start());
        report_invalid(ch, body_range.start(), body, at, fix_at);
    }
}

/// Valid-but-wrong escapes (`"\b"` = backspace) in a regex argument.
pub fn suspicious_regex_escape(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let Some(i) = regex_arg_index(ch, c, expr) else {
        return;
    };
    let Some(arg) = c.arguments.args.get(i) else {
        return;
    };
    for part in string_parts(arg) {
        if part.flags.prefix().is_raw() {
            continue;
        }
        let open = part.flags.opener_len();
        let close = part.flags.closer_len();
        if part.range.len() < open + close {
            continue;
        }
        let body_range = TextRange::new(part.range.start() + open, part.range.end() - close);
        let body = ch.slice(body_range);
        let scan = scan_escapes(body, false);
        if scan.invalid.is_some() {
            continue; // reported by invalid_escape
        }
        let Some(at) = scan.suspicious else { continue };
        let esc_start = body_range.start() + TextSize::from(at as u32);
        let shown: String = body[at..].chars().take(2).collect();
        let mut d = Diagnostic::new(
            "COR016",
            format!(
                "\"{shown}\" in a regex passed as a non-raw string is a control character, not a regex escape — use r\"...\"."
            ),
            TextRange::new(esc_start, esc_start + TextSize::from(shown.len() as u32)),
        );
        let prefix = part.flags.prefix().as_str();
        if !prefix.contains(['u', 'U']) && !body.contains("\\\\") && !body.ends_with('\\') {
            d = d.with_fix(Fix::unsafe_(
                "Make it a raw string",
                vec![Edit::insert(part.range.start(), "r")],
            ));
        }
        ch.report(d);
    }
}
