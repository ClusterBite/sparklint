//! UDF001 udf-has-builtin.

use ruff_python_ast::{self as ast, CmpOp, Expr, Operator, Stmt};
use ruff_text_size::{Ranged, TextRange};
use sparklint_semantic::Ty;

use super::Rule;
use super::helpers::*;
use crate::checker::Checker;
use crate::diagnostic::Diagnostic;

/// UDF001 (see [`super::Rule`]).
pub const RULES: &[Rule] = &[Rule::new("UDF001").stmt(udf001_stmt).call(udf_call)];

fn udf001_stmt(ch: &mut Checker, stmt: &Stmt) {
    if let Stmt::FunctionDef(f) = stmt {
        udf_decorator(ch, f);
    }
}

pub(crate) fn is_udf_factory(ch: &Checker, func: &Expr) -> bool {
    if matches!(
        ch.model.qualified_name(func).as_deref(),
        Some("pyspark.sql.functions.udf")
    ) {
        return true;
    }
    // An unbound `udf` name (snippets/notebooks that rely on star imports).
    ch.settings.heuristics
        && matches!(func, Expr::Name(n) if n.id.as_str() == "udf"
            && ch.model.use_site(n).is_some_and(|u| u.reaching.is_empty()))
}

/// `F.udf(fn_or_lambda, ...)`, `udf(lambda ...)`, `spark.udf.register("n", fn, ...)`.
pub fn udf_call(ch: &mut Checker, c: &ast::ExprCall, expr: &Expr) {
    let target = if is_udf_factory(ch, &c.func) {
        c.arguments.args.first().or_else(|| {
            c.arguments
                .keywords
                .iter()
                .find(|k| k.arg.as_ref().is_some_and(|a| a.as_str() == "f"))
                .map(|k| &k.value)
        })
    } else if let Some(sc) = ch.model.spark_call_named(expr, &["register"])
        && sc.class() == Ty::UdfRegistration
    {
        c.arguments.args.get(1)
    } else {
        None
    };
    let Some(target) = target else { return };
    let (params, body): (Vec<String>, &Expr) = match target {
        Expr::Lambda(l) => {
            let params = l
                .parameters
                .as_ref()
                .map(|p| {
                    p.iter_non_variadic_params()
                        .map(|p| p.parameter.name.as_str().to_string())
                        .collect()
                })
                .unwrap_or_default();
            (params, &l.body)
        }
        Expr::Name(n) => {
            let Some(b) = ch.model.single_binding(n) else {
                return;
            };
            let b = ch.model.binding(b);
            let Some(def) = ch.fn_defs.get(&b.range.start()).copied() else {
                return;
            };
            match single_return(def) {
                Some((p, e)) => (p, e),
                None => return,
            }
        }
        _ => return,
    };
    if let Some(builtin) = builtin_equivalent(ch, body, &params) {
        report(ch, builtin, expr.range());
    }
}

/// `@udf`, `@udf("string")`, `@F.udf(returnType=...)` on a def.
pub fn udf_decorator(ch: &mut Checker, f: &ast::StmtFunctionDef) {
    let Some(dec) = f.decorator_list.iter().find(|d| {
        let callee = match &d.expression {
            Expr::Call(c) => &*c.func,
            other => other,
        };
        is_udf_factory(ch, callee)
    }) else {
        return;
    };
    let Some((params, body)) = single_return(f) else {
        return;
    };
    if let Some(builtin) = builtin_equivalent(ch, body, &params) {
        report(
            ch,
            builtin,
            TextRange::new(dec.range.start(), f.name.range.end()),
        );
    }
}

fn report(ch: &mut Checker, builtin: &str, range: TextRange) {
    ch.report(
        Diagnostic::new(
            "UDF001",
            format!("UDF body is equivalent to built-in {builtin} — drop the UDF."),
            range,
        )
        .with_help("Python UDFs serialise every row JVM→Python→JVM and block Catalyst/Photon; the built-in runs natively (note: built-ins return NULL for NULL input)."),
    );
}

/// Parameters and the single returned expression of a def (docstring allowed).
fn single_return(f: &ast::StmtFunctionDef) -> Option<(Vec<String>, &Expr)> {
    let mut body = f.body.iter().filter(|s| {
        !matches!(s, Stmt::Expr(e) if e.value.is_string_literal_expr())
            && !matches!(s, Stmt::Pass(_))
    });
    let first = body.next()?;
    if body.next().is_some() {
        return None;
    }
    let Stmt::Return(r) = first else { return None };
    let params = f
        .parameters
        .iter_non_variadic_params()
        .map(|p| p.parameter.name.as_str().to_string())
        .collect();
    Some((params, r.value.as_deref()?))
}

struct Ctx<'c, 'a> {
    ch: &'c Checker<'a>,
    params: &'c [String],
}

impl Ctx<'_, '_> {
    fn is_param(&self, e: &Expr) -> bool {
        matches!(e, Expr::Name(n) if self.params.iter().any(|p| p == n.id.as_str()))
    }

    fn is_literal(e: &Expr) -> bool {
        matches!(
            e,
            Expr::StringLiteral(_)
                | Expr::NumberLiteral(_)
                | Expr::BooleanLiteral(_)
                | Expr::NoneLiteral(_)
        ) || matches!(e, Expr::UnaryOp(u) if matches!(&*u.operand, Expr::NumberLiteral(_)))
    }

    fn simple(&self, e: &Expr) -> bool {
        self.is_param(e) || Self::is_literal(e)
    }

    fn simple_args(&self, c: &ast::ExprCall) -> bool {
        c.arguments.args.iter().all(|a| self.simple(a))
            && c.arguments.keywords.iter().all(|k| self.simple(&k.value))
    }

    fn mentions_param(&self, e: &Expr) -> bool {
        let mut found = false;
        walk_exprs(e, &mut |x| {
            if self.is_param(x) {
                found = true;
            }
        });
        found
    }

    /// Arithmetic over params and numeric literals.
    fn arith(&self, e: &Expr) -> bool {
        match e {
            Expr::BinOp(b) => {
                matches!(
                    b.op,
                    Operator::Add
                        | Operator::Sub
                        | Operator::Mult
                        | Operator::Div
                        | Operator::Mod
                        | Operator::FloorDiv
                        | Operator::Pow
                ) && self.arith(&b.left)
                    && self.arith(&b.right)
            }
            Expr::UnaryOp(u) => self.arith(&u.operand),
            Expr::NumberLiteral(_) => true,
            other => self.is_param(other),
        }
    }

    fn qualified(&self, e: &Expr) -> Option<String> {
        self.ch.model.qualified_name(e)
    }
}

/// The built-in Spark function a UDF body is equivalent to, if any.
pub fn builtin_equivalent(ch: &Checker, body: &Expr, params: &[String]) -> Option<&'static str> {
    if params.is_empty() {
        return None;
    }
    let cx = Ctx { ch, params };
    equivalent(&cx, body)
}

fn equivalent(cx: &Ctx, e: &Expr) -> Option<&'static str> {
    match e {
        Expr::Call(c) => {
            // str methods on a parameter
            if let Expr::Attribute(a) = &*c.func {
                if cx.is_param(&a.value) && cx.simple_args(c) {
                    let f = match a.attr.as_str() {
                        "lower" | "casefold" => "F.lower()",
                        "upper" => "F.upper()",
                        "strip" => "F.trim()",
                        "lstrip" => "F.ltrim()",
                        "rstrip" => "F.rtrim()",
                        "title" | "capitalize" => "F.initcap()",
                        "startswith" => "Column.startswith()",
                        "endswith" => "Column.endswith()",
                        "replace" => "F.replace() / F.regexp_replace()",
                        "split" => "F.split()",
                        "zfill" => "F.lpad()",
                        "find" | "index" => "F.instr()",
                        "strftime" => "F.date_format()",
                        _ => return None,
                    };
                    return Some(f);
                }
                // hashlib.md5(x.encode()).hexdigest()
                if a.attr.as_str() == "hexdigest"
                    && let Expr::Call(inner) = &*a.value
                    && let Some(q) = cx.qualified(&inner.func)
                    && cx.mentions_param(&a.value)
                {
                    return match q.as_str() {
                        "hashlib.md5" => Some("F.md5()"),
                        "hashlib.sha1" => Some("F.sha1()"),
                        "hashlib.sha256" | "hashlib.sha512" | "hashlib.sha224"
                        | "hashlib.sha384" => Some("F.sha2()"),
                        _ => None,
                    };
                }
            }
            let q = cx.qualified(&c.func)?;
            let args_ok = cx.simple_args(c) && c.arguments.args.iter().any(|a| cx.is_param(a));
            match q.as_str() {
                "builtins.len" if args_ok => Some("F.length()"),
                "builtins.abs" if args_ok => Some("F.abs()"),
                "builtins.round" if args_ok => Some("F.round()"),
                "builtins.str" if args_ok => Some("Column.cast(\"string\")"),
                "builtins.int" if args_ok => Some("Column.cast(\"int\")"),
                "builtins.float" if args_ok => Some("Column.cast(\"double\")"),
                "builtins.bool" if args_ok => Some("Column.cast(\"boolean\")"),
                "math.sqrt" if args_ok => Some("F.sqrt()"),
                "math.floor" if args_ok => Some("F.floor()"),
                "math.ceil" if args_ok => Some("F.ceil()"),
                "math.log" | "math.log10" | "math.log2" if args_ok => Some("F.log()"),
                "math.exp" if args_ok => Some("F.exp()"),
                "math.pow" if args_ok => Some("F.pow()"),
                "math.fabs" if args_ok => Some("F.abs()"),
                "re.sub" if args_ok => Some("F.regexp_replace()"),
                "re.match" | "re.search" | "re.fullmatch" if args_ok => Some("Column.rlike()"),
                "re.findall" if args_ok => Some("F.regexp_extract_all()"),
                "datetime.datetime.strptime" | "datetime.strptime" if args_ok => {
                    Some("F.to_timestamp()")
                }
                _ => None,
            }
        }
        Expr::Subscript(s) => {
            if cx.is_param(&s.value) && s.slice.is_slice_expr() {
                return Some("F.substring()");
            }
            if let Expr::Call(c) = &*s.value
                && cx.qualified(&c.func).as_deref() == Some("json.loads")
                && c.arguments.args.first().is_some_and(|a| cx.is_param(a))
                && Ctx::is_literal(&s.slice)
            {
                return Some("F.get_json_object() / F.from_json()");
            }
            None
        }
        Expr::Compare(c) if c.ops.len() == 1 => {
            let (l, r) = (&c.operands[0], &c.operands[1]);
            match c.ops[0] {
                CmpOp::Is | CmpOp::IsNot if cx.is_param(l) && r.is_none_literal_expr() => {
                    Some(if c.ops[0] == CmpOp::Is {
                        "Column.isNull()"
                    } else {
                        "Column.isNotNull()"
                    })
                }
                CmpOp::In | CmpOp::NotIn
                    if cx.is_param(l)
                        && matches!(r, Expr::List(_) | Expr::Tuple(_) | Expr::Set(_)) =>
                {
                    Some("Column.isin()")
                }
                CmpOp::Eq | CmpOp::NotEq | CmpOp::Gt | CmpOp::GtE | CmpOp::Lt | CmpOp::LtE
                    if (cx.arith(l) && cx.arith(r))
                        && (cx.mentions_param(l) || cx.mentions_param(r)) =>
                {
                    Some("Column comparison operators")
                }
                _ => None,
            }
        }
        Expr::If(i) => {
            // `x if x is not None else d` → coalesce
            if let Expr::Compare(t) = &*i.test
                && t.ops.len() == 1
                && cx.is_param(&t.operands[0])
                && t.operands[1].is_none_literal_expr()
            {
                let p = &t.operands[0];
                let same = |e: &Expr| {
                    cx.is_param(e)
                        && e.as_name_expr().map(|n| &n.id) == p.as_name_expr().map(|n| &n.id)
                };
                match t.ops[0] {
                    CmpOp::IsNot if same(&i.body) && cx.simple(&i.orelse) => {
                        return Some("F.coalesce()");
                    }
                    CmpOp::Is if same(&i.orelse) && cx.simple(&i.body) => {
                        return Some("F.coalesce()");
                    }
                    CmpOp::IsNot if i.orelse.is_none_literal_expr() => {
                        return equivalent(cx, &i.body);
                    }
                    CmpOp::Is if i.body.is_none_literal_expr() => return equivalent(cx, &i.orelse),
                    _ => {}
                }
            }
            // null guard `x.upper() if x else None`
            if cx.is_param(&i.test) && i.orelse.is_none_literal_expr() {
                return equivalent(cx, &i.body);
            }
            let simple_or_arith = |e: &Expr| cx.simple(e) || cx.arith(e);
            let test_ok = match &*i.test {
                Expr::Compare(t) => t.operands.iter().all(simple_or_arith),
                other => cx.is_param(other),
            };
            if test_ok && simple_or_arith(&i.body) && simple_or_arith(&i.orelse) {
                return Some("F.when().otherwise()");
            }
            None
        }
        Expr::BinOp(b) => {
            if cx.arith(e) && cx.mentions_param(e) {
                return Some("Column arithmetic (+ - * /)");
            }
            if b.op == Operator::Add
                && cx.simple(&b.left)
                && cx.simple(&b.right)
                && (cx.is_param(&b.left) || cx.is_param(&b.right))
            {
                return Some("F.concat()");
            }
            None
        }
        _ => None,
    }
}
