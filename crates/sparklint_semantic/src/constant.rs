//! "Which constant does this expression hold?": the one constant reader.
//!
//! [`SemanticModel::constant`] answers a typed [`Const`] (`Str` / `Int` / `Num` /
//! `Bool` / `None`) for a literal, an integer expression (`10 * 1024`, `2 ** 30`,
//! `1 << 4`, `n // 2`, unary minus) or a name, following EVERY binding that reaches the
//! name through the values module ([`SemanticModel::hop`]): branches that agree are a
//! constant (`if a: N = 8 else: N = 2 * 4` is 8), branches that disagree, or a path
//! without a value (a parameter, a loop target), are no constant. One depth guard
//! ([`CONST_DEPTH`]) bounds the walk: every expression node and name hop counts one.
//!
//! The typed accessors say how a caller reads the answer: [`Const::as_str`],
//! [`Const::as_int`], [`Const::as_num`] (ints too), [`Const::as_bool`] (Python `True` /
//! `False`), [`Const::as_flag`] (a Spark option: `True` or `"true"`, any case) and
//! [`Const::option_text`] (a Spark option's text: `True` → `"true"`, `8` → `"8"`).
//!
//! `None` is a value of its own: an argument that is `None` on every path is "not
//! given", which is how pyspark treats it (`schema=None`, `insertInto(t, overwrite=None)`
//! fall back to the default). [`SemanticModel::given_arg`] /
//! [`crate::SparkCall::given`] read an argument that way.
//!
//! [`literal`] reads only what is written at the expression (no names, no arithmetic):
//! for questions about the code's shape (a subscript key written as a literal, a
//! comparison operand), never for an argument's value.

use ruff_python_ast::{self as ast, Expr, Number, Operator, UnaryOp};

use crate::infer::call_arg;
use crate::model::SemanticModel;
use crate::values::Hops;

/// How deep constant following goes: every expression node and name hop counts one.
pub const CONST_DEPTH: u32 = 8;

/// A constant value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Const<'e> {
    Str(&'e str),
    Int(i128),
    Num(f64),
    Bool(bool),
    None,
}

impl<'e> Const<'e> {
    /// The string.
    pub fn as_str(self) -> Option<&'e str> {
        match self {
            Const::Str(s) => Some(s),
            _ => None,
        }
    }

    /// The integer.
    pub fn as_int(self) -> Option<i128> {
        match self {
            Const::Int(i) => Some(i),
            _ => None,
        }
    }

    /// The number (an integer or a float).
    pub fn as_num(self) -> Option<f64> {
        match self {
            Const::Int(i) => Some(i as f64),
            Const::Num(f) => Some(f),
            _ => None,
        }
    }

    /// A Python `True` / `False`.
    pub fn as_bool(self) -> Option<bool> {
        match self {
            Const::Bool(b) => Some(b),
            _ => None,
        }
    }

    /// A Spark option flag: `True` / `False`, or the strings `"true"` / `"false"` in any
    /// case (Spark reads option values as text).
    pub fn as_flag(self) -> Option<bool> {
        match self {
            Const::Bool(b) => Some(b),
            Const::Str(s) if s.eq_ignore_ascii_case("true") => Some(true),
            Const::Str(s) if s.eq_ignore_ascii_case("false") => Some(false),
            _ => None,
        }
    }

    /// Python truthiness (`if x:`): `True`, a non-zero number, a non-empty string.
    pub fn truthy(self) -> bool {
        match self {
            Const::Bool(b) => b,
            Const::Int(i) => i != 0,
            Const::Num(f) => f != 0.0,
            Const::Str(s) => !s.is_empty(),
            Const::None => false,
        }
    }

    /// `None`.
    pub fn is_none(self) -> bool {
        self == Const::None
    }

    /// A Spark option's text: the string, `True` → `"true"`, an integer's digits. Floats
    /// and `None` have no text Spark and Python agree on.
    pub fn option_text(self) -> Option<String> {
        match self {
            Const::Str(s) => Some(s.to_string()),
            Const::Bool(b) => Some(b.to_string()),
            Const::Int(i) => Some(i.to_string()),
            Const::Num(_) | Const::None => None,
        }
    }
}

/// The literal written at `e`: a string, number, `True` / `False`, `None`, or a signed
/// number (`-1`, `+0.5`). No names, no arithmetic: see the module docs.
pub fn literal(e: &Expr) -> Option<Const<'_>> {
    match e {
        Expr::StringLiteral(s) => Some(Const::Str(s.value.to_str())),
        Expr::NumberLiteral(n) => number(n),
        Expr::BooleanLiteral(b) => Some(Const::Bool(b.value)),
        Expr::NoneLiteral(_) => Some(Const::None),
        Expr::UnaryOp(u) if matches!(u.op, UnaryOp::USub | UnaryOp::UAdd) => {
            signed(u.op, literal(&u.operand)?)
        }
        _ => None,
    }
}

/// String literal value (not f-strings): `literal(e)` as a string.
pub fn str_literal(e: &Expr) -> Option<&str> {
    literal(e)?.as_str()
}

/// Integer literal value (`8`, `-1`): `literal(e)` as an integer in the i64 range.
pub fn int_literal(e: &Expr) -> Option<i64> {
    literal(e)?.as_int().and_then(|i| i64::try_from(i).ok())
}

fn number(n: &ast::ExprNumberLiteral) -> Option<Const<'_>> {
    match &n.value {
        Number::Int(i) => i.as_i64().map(|v| Const::Int(i128::from(v))),
        Number::Float(f) => Some(Const::Num(*f)),
        Number::Complex { .. } => None,
    }
}

fn signed(op: UnaryOp, c: Const<'_>) -> Option<Const<'_>> {
    let minus = op == UnaryOp::USub;
    match c {
        Const::Int(i) if minus => i.checked_neg().map(Const::Int),
        Const::Num(f) if minus => Some(Const::Num(-f)),
        Const::Int(_) | Const::Num(_) => Some(c),
        _ => None,
    }
}

/// Two paths' constants as one: equal values, or an int and a float of the same
/// number (`0` / `0.0`: the float, so it reads as a number but not as an int).
fn agree<'e>(a: Const<'e>, b: Const<'e>) -> Option<Const<'e>> {
    match (a, b) {
        _ if a == b => Some(a),
        (Const::Int(i), Const::Num(f)) | (Const::Num(f), Const::Int(i)) if i as f64 == f => {
            Some(Const::Num(f))
        }
        _ => None,
    }
}

fn arith(op: Operator, l: i128, r: i128) -> Option<i128> {
    match op {
        Operator::Mult => l.checked_mul(r),
        Operator::Add => l.checked_add(r),
        Operator::Sub => l.checked_sub(r),
        Operator::Pow => u32::try_from(r).ok().and_then(|r| l.checked_pow(r)),
        Operator::LShift => u32::try_from(r)
            .ok()
            .filter(|r| *r < 100)
            .and_then(|r| l.checked_mul(1i128 << r)),
        Operator::FloorDiv if r != 0 => Some(l.div_euclid(r)),
        _ => None,
    }
}

impl<'a> SemanticModel<'a> {
    /// The constant `e` holds on every path (see the module docs); `None` when it is no
    /// constant or the paths disagree.
    pub fn constant<'e>(&self, e: &'e Expr) -> Option<Const<'e>>
    where
        'a: 'e,
    {
        self.constant_at(e, 0)
    }

    fn constant_at<'e>(&self, e: &'e Expr, depth: u32) -> Option<Const<'e>>
    where
        'a: 'e,
    {
        if depth > CONST_DEPTH {
            return None;
        }
        match e {
            Expr::StringLiteral(_)
            | Expr::NumberLiteral(_)
            | Expr::BooleanLiteral(_)
            | Expr::NoneLiteral(_) => literal(e),
            Expr::UnaryOp(u) if matches!(u.op, UnaryOp::USub | UnaryOp::UAdd) => {
                signed(u.op, self.constant_at(&u.operand, depth + 1)?)
            }
            Expr::BinOp(b) => {
                let l = self.constant_at(&b.left, depth + 1)?.as_int()?;
                let r = self.constant_at(&b.right, depth + 1)?.as_int()?;
                arith(b.op, l, r).map(Const::Int)
            }
            Expr::Name(_) => {
                let mut acc: Option<Const<'e>> = None;
                for v in self.const_bindings(e)? {
                    let x = self.constant_at(v, depth + 1)?;
                    acc = Some(match acc {
                        None => x,
                        Some(a) => agree(a, x)?,
                    });
                }
                acc
            }
            _ => None,
        }
    }

    /// The values of every binding that reaches a name (`None` when it is no name, no
    /// binding reaches it, or one of them has no value: a parameter, a loop target, ...).
    pub(crate) fn const_bindings(&self, e: &Expr) -> Option<Vec<&'a Expr>> {
        // CONST_DEPTH bounds the whole walk (names and operators); one hop per call.
        let (reached, _) = self.hop(e, Hops::DEFAULT)?;
        reached.iter().map(|r| r.value).collect()
    }

    /// A string constant: `constant(e)` as a string.
    pub fn const_str<'e>(&self, e: &'e Expr) -> Option<&'e str>
    where
        'a: 'e,
    {
        self.constant(e)?.as_str()
    }

    /// An integer constant: `constant(e)` as an integer.
    pub fn const_int(&self, e: &Expr) -> Option<i128> {
        self.constant(e)?.as_int()
    }

    /// `e` as an argument value as given: `None` when it is `None` on every path (pyspark
    /// treats an explicit `None` as the parameter's default).
    pub fn given<'e>(&self, e: &'e Expr) -> Option<&'e Expr>
    where
        'a: 'e,
    {
        (!self.constant(e).is_some_and(Const::is_none)).then_some(e)
    }

    /// Argument `kw=`, else positional argument `i` of `call` ([`call_arg`]), as given:
    /// `None` when it is missing or `None` on every path.
    pub fn given_arg<'e>(&self, call: &'e ast::ExprCall, i: usize, kw: &str) -> Option<&'e Expr>
    where
        'a: 'e,
    {
        self.given(call_arg(call, i, kw)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSummaries, SemanticOptions, build_model};
    use ruff_python_ast::Stmt;
    use ruff_text_size::TextSize;

    /// `constant()` of the last statement's expression, and `literal()` of it.
    fn read(body: &str) -> (Option<String>, Option<String>) {
        let m = ruff_python_parser::parse_module(body)
            .expect("valid test source")
            .into_syntax();
        let model = build_model(
            &m,
            TextSize::of(body),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let Some(Stmt::Expr(e)) = m.body.last() else {
            panic!("last statement is no expression: {body}");
        };
        let show = |c: Option<Const>| c.map(|c| format!("{c:?}"));
        (show(model.constant(&e.value)), show(literal(&e.value)))
    }

    /// (source, constant, literal) for every value shape.
    #[test]
    fn value_shapes() {
        let rows: &[(&str, Option<&str>, Option<&str>)] = &[
            // literals
            ("'k'", Some("Str(\"k\")"), Some("Str(\"k\")")),
            ("'a' 'b'", Some("Str(\"ab\")"), Some("Str(\"ab\")")),
            ("8", Some("Int(8)"), Some("Int(8)")),
            ("-1", Some("Int(-1)"), Some("Int(-1)")),
            ("+3", Some("Int(3)"), Some("Int(3)")),
            ("0.5", Some("Num(0.5)"), Some("Num(0.5)")),
            ("-0.0", Some("Num(-0.0)"), Some("Num(-0.0)")),
            ("True", Some("Bool(true)"), Some("Bool(true)")),
            ("False", Some("Bool(false)"), Some("Bool(false)")),
            ("None", Some("None"), Some("None")),
            ("1j", None, None),
            ("f'{x}'", None, None),
            ("-'a'", None, None),
            // int arithmetic (not for the literal reader)
            ("10 * 1024", Some("Int(10240)"), None),
            ("2 ** 10", Some("Int(1024)"), None),
            ("1 << 4", Some("Int(16)"), None),
            ("7 // 2", Some("Int(3)"), None),
            ("-(2 * 3)", Some("Int(-6)"), None),
            ("1 / 2", None, None),
            ("0.5 * 2", None, None),
            ("2 ** 200", None, None),
            ("1 // 0", None, None),
            // names: every reaching binding, agree or unknown
            ("K = 'id'\nK", Some("Str(\"id\")"), None),
            ("N = 2 ** 10\nM = N * 4\nM", Some("Int(4096)"), None),
            ("B = True\nB", Some("Bool(true)"), None),
            ("X = None\nX", Some("None"), None),
            (
                "if c:\n    N = 8\nelse:\n    N = 2 * 4\nN",
                Some("Int(8)"),
                None,
            ),
            ("if c:\n    N = 4\nelse:\n    N = 8\nN", None, None),
            ("if c:\n    X = None\nelse:\n    X = 1\nX", None, None),
            (
                "if c:\n    X = 8\nelse:\n    X = 8.0\nX",
                Some("Num(8.0)"),
                None,
            ),
            (
                "if c:\n    X = 0.0\nelse:\n    X = 0\nX",
                Some("Num(0.0)"),
                None,
            ),
            ("if c:\n    X = 8\nelse:\n    X = 8.5\nX", None, None),
            ("if c:\n    X = '8'\nelse:\n    X = 8\nX", None, None),
            (
                "def f(N=4):\n    if c:\n        N = 4\n    return N\nf",
                None,
                None,
            ),
            ("n = len(xs)\nn", None, None),
            ("for n in [1, 2]:\n    pass\nn", None, None),
            ("x", None, None),
        ];
        for (src, constant, lit) in rows {
            let want = (constant.map(str::to_string), lit.map(str::to_string));
            assert_eq!(read(src), want, "{src}");
        }
    }

    #[test]
    fn depth_is_bounded() {
        let mut body = String::from("A0 = 1\n");
        for k in 1..=CONST_DEPTH + 1 {
            body.push_str(&format!("A{k} = A{}\n", k - 1));
        }
        body.push_str(&format!("A{}\n", CONST_DEPTH + 1));
        assert_eq!(read(&body).0, None);
        assert_eq!(read("A0 = 1\nA1 = A0\nA1\n").0, Some("Int(1)".into()));
    }

    #[test]
    fn accessors() {
        assert_eq!(Const::Int(8).as_num(), Some(8.0));
        assert_eq!(Const::Num(8.0).as_int(), None);
        assert_eq!(Const::Str("TRUE").as_flag(), Some(true));
        assert_eq!(Const::Str("False").as_flag(), Some(false));
        assert_eq!(Const::Str("yes").as_flag(), None);
        assert_eq!(Const::Str("true").as_bool(), None);
        assert_eq!(Const::Bool(true).option_text().as_deref(), Some("true"));
        assert_eq!(Const::Int(-3).option_text().as_deref(), Some("-3"));
        assert_eq!(Const::Num(0.5).option_text(), None);
        assert_eq!(Const::None.option_text(), None);
        assert!(Const::None.is_none());
        assert!(Const::Str("false").truthy());
        assert!(!Const::Str("").truthy());
        assert!(!Const::Int(0).truthy());
        assert!(Const::Num(0.5).truthy());
        assert!(!Const::None.truthy());
        assert_eq!(
            int_literal(
                &ruff_python_parser::parse_expression("2 ** 70")
                    .unwrap()
                    .into_expr()
            ),
            None
        );
    }
}
