//! "Which columns does this expression read?": the one answer rules ask instead of
//! matching `col(..)` calls, string arguments, `df.a` attributes and column lists
//! themselves.
//!
//! - [`column_name`]: the column ONE expression names (`"a"`,
//!   `pyspark.sql.functions.col("a")` / `column("a")`, `df.a`, `df["a"]`).
//! - [`ColumnRefs::of`]: every column name an expression mentions (string literals,
//!   DataFrame attributes, the identifiers of `F.expr` SQL text), whether it is a star
//!   (`"*"`, `"t.*"`), and whether it reads something that cannot be named (a Column
//!   or str variable, an f-string hole, unreadable SQL text).
//! - [`column_args`]: the column arguments of a call taking `*cols`: arguments as
//!   written, a list or tuple argument and a starred literal list unpacked.
//!
//! Names are over-inclusive on purpose (the rules use them to prove a column is NOT
//! read): a dotted name contributes itself and every part (`"a.b"` -> `a.b`, `a`,
//! `b`), SQL text every identifier word (keywords and function names included).

use ruff_python_ast::Expr;
use ruff_python_ast::visitor::{self, Visitor};

use crate::constant::str_literal;
use crate::infer::call_arg;
use crate::model::SemanticModel;
use crate::sql::{SqlSite, SqlSiteKind, SqlText};

/// The column `e` names as written: `"a"`, `F.col("a")` / `F.column("a")` (the
/// `pyspark.sql.functions` ones, `col=` too), `df.a`, `df["a"]` -> `a`.
pub fn column_name(model: &SemanticModel, e: &Expr) -> Option<String> {
    match e {
        Expr::StringLiteral(s) => Some(s.value.to_str().to_string()),
        Expr::Call(c)
            if model.is_spark_function(&c.func, "col")
                || model.is_spark_function(&c.func, "column") =>
        {
            let args = &c.arguments;
            if args.args.len() + args.keywords.len() != 1 {
                return None;
            }
            call_arg(c, 0, "col")
                .and_then(str_literal)
                .map(str::to_string)
        }
        // `df.a` (the oracle types it a Column: not an API member, not `_private`).
        Expr::Attribute(a)
            if model.ty_of(&a.value).is_dataframe() && model.ty_of(e).is_column() =>
        {
            Some(a.attr.as_str().to_string())
        }
        Expr::Subscript(s) if model.ty_of(&s.value).is_dataframe() => {
            str_literal(&s.slice).map(str::to_string)
        }
        _ => None,
    }
}

/// Every column name an expression mentions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnRefs {
    /// Names as found (duplicates kept, in source order).
    pub names: Vec<String>,
    /// The expression itself is a star: `"*"` / `"t.*"` (as a string, `F.col`,
    /// `df[..]`); `t` of `t.*` is in `names`.
    pub star: bool,
    /// It also reads something that cannot be named (a Column / str variable, an
    /// f-string hole, SQL text that cannot be read).
    pub unknown: bool,
}

impl ColumnRefs {
    /// The names `e` mentions, walking every sub-expression (not into lambdas, not
    /// into `F.lit(..)` values).
    pub fn of<'a, 'e>(model: &SemanticModel<'a>, e: &'e Expr) -> Self
    where
        'a: 'e,
    {
        let mut out = ColumnRefs {
            star: column_name(model, e).is_some_and(|n| n == "*" || n.ends_with(".*")),
            ..ColumnRefs::default()
        };
        Walk {
            model,
            out: &mut out,
        }
        .visit_expr(e);
        out
    }

    /// The names a SQL site's text mentions; unknown when the text cannot be read or
    /// was built from f-string holes.
    pub fn of_site(site: &SqlSite) -> Self {
        match &site.text {
            Some(t) => {
                let mut r = Self::of_sql(t);
                r.unknown = !site.holes.is_empty();
                r
            }
            None => ColumnRefs {
                unknown: true,
                ..ColumnRefs::default()
            },
        }
    }

    /// The identifiers of a SQL expression ([`SqlText::identifiers`]).
    pub fn of_sql(text: &SqlText) -> Self {
        ColumnRefs {
            names: text.identifiers(),
            ..ColumnRefs::default()
        }
    }

    /// Whether `name` is among the names.
    pub fn contains(&self, name: &str) -> bool {
        self.names.iter().any(|n| n == name)
    }

    /// A column name as written: itself and, when dotted, each part (backticks
    /// dropped, a trailing `.*` / a bare `*` contribute only the qualifier).
    fn add_name(&mut self, n: &str) {
        let n = n.trim_end_matches(".*");
        if n.is_empty() || n == "*" {
            return;
        }
        let plain = n.replace('`', "");
        if plain.contains('.') {
            self.names.extend(
                plain
                    .split('.')
                    .filter(|p| !p.is_empty())
                    .map(str::to_string),
            );
        }
        self.names.push(plain);
    }
}

struct Walk<'m, 'a, 'o> {
    model: &'m SemanticModel<'a>,
    out: &'o mut ColumnRefs,
}

impl<'e, 'a: 'e> Visitor<'e> for Walk<'_, 'a, '_> {
    fn visit_expr(&mut self, e: &'e Expr) {
        let model = self.model;
        match e {
            Expr::Lambda(_) => return,
            Expr::StringLiteral(s) => {
                self.out.add_name(s.value.to_str());
                return;
            }
            Expr::FString(_) => {
                // An f-string column name: its holes could name anything.
                self.out.unknown = true;
                return;
            }
            Expr::Call(c)
                if model.is_spark_function(&c.func, "lit")
                    || model.is_spark_function(&c.func, "typedLit") =>
            {
                return; // a value, not a column
            }
            Expr::Call(_) => {
                if let Some(site) = model
                    .sql_sites(e)
                    .into_iter()
                    .find(|s| s.kind == SqlSiteKind::Expression)
                {
                    let r = ColumnRefs::of_site(&site);
                    self.out.names.extend(r.names);
                    self.out.unknown |= r.unknown;
                    return;
                }
            }
            Expr::Attribute(_) => {
                if let Some(n) = column_name(model, e) {
                    self.out.add_name(&n);
                    return;
                }
            }
            Expr::Name(_) if !named_object(model, e) => {
                self.out.unknown = true;
                return;
            }
            _ => {}
        }
        visitor::walk_expr(self, e);
    }
}

/// A module, an imported object or a non-Column Spark object (`F`, `df`, `spark`):
/// a name that does not stand for a column.
fn named_object(model: &SemanticModel, e: &Expr) -> bool {
    let t = model.ty_of(e);
    model.qualified_name(e).is_some() || (t.ty.is_spark() && !t.is_column())
}

/// The column arguments of a call taking `*cols`.
#[derive(Debug, Clone, Default)]
pub struct ColumnArgs<'e> {
    /// One expression per column argument, containers unpacked.
    pub exprs: Vec<&'e Expr>,
    /// A `*starred` argument that is not a literal list / tuple: more columns than
    /// `exprs` may be passed.
    pub unknown: bool,
}

/// Unpacks the column arguments `args`: `f("a", "b")`, `f(["a", "b"])`,
/// `f(("a", "b"))`, `f(*["a", "b"])` all give `"a"`, `"b"`; `f(*cols)` is unknown.
pub fn column_args<'e>(args: impl IntoIterator<Item = &'e Expr>) -> ColumnArgs<'e> {
    let mut out = ColumnArgs::default();
    for a in args {
        let inner = match a {
            Expr::Starred(s) => match &*s.value {
                v @ (Expr::List(_) | Expr::Tuple(_)) => v,
                _ => {
                    out.unknown = true;
                    continue;
                }
            },
            a => a,
        };
        match inner {
            Expr::List(l) => out.exprs.extend(l.elts.iter()),
            Expr::Tuple(t) => out.exprs.extend(t.elts.iter()),
            other => out.exprs.push(other),
        }
    }
    out
}

/// `<frame>.columns` on a DataFrame, or an unfiltered comprehension over it
/// (`[F.col(c) for c in df.columns]`): every column of the frame.
pub fn all_columns(model: &SemanticModel, e: &Expr) -> bool {
    let frame_columns = |e: &Expr| matches!(e, Expr::Attribute(a) if a.attr.as_str() == "columns" && model.is_dataframe(&a.value));
    let gens = match e {
        Expr::ListComp(lc) => &lc.generators,
        Expr::Generator(g) => &g.generators,
        _ => return frame_columns(e),
    };
    gens.len() == 1 && gens[0].ifs.is_empty() && frame_columns(&gens[0].iter)
}

/// `name` occurs as a plain name somewhere in `e` (not inside lambdas).
pub fn mentions_name(e: &Expr, name: &str) -> bool {
    struct M<'n> {
        name: &'n str,
        found: bool,
    }
    impl<'e> Visitor<'e> for M<'_> {
        fn visit_expr(&mut self, e: &'e Expr) {
            match e {
                Expr::Name(n) if n.id.as_str() == self.name => self.found = true,
                Expr::Lambda(_) => {}
                _ => visitor::walk_expr(self, e),
            }
        }
    }
    let mut m = M { name, found: false };
    m.visit_expr(e);
    m.found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSummaries, SemanticOptions, build_model};
    use ruff_python_ast::ModModule;
    use ruff_text_size::{Ranged, TextSize};

    const PRELUDE: &str = "from pyspark.sql import SparkSession\n\
        import pyspark.sql.functions as F\n\
        spark = SparkSession.builder.getOrCreate()\n\
        df = spark.table(\"t\")\n\
        c = F.col(\"z\")\n\
        q = \"k > 1\"\n\
        def col(x):\n    return x\n";

    fn parse(src: &str) -> ModModule {
        ruff_python_parser::parse_module(src)
            .expect("valid test source")
            .into_syntax()
    }

    /// Runs `f` on the expression of the last statement of PRELUDE + `body`.
    fn with_expr<R>(body: &str, f: impl for<'x> FnOnce(&SemanticModel<'x>, &'x Expr) -> R) -> R {
        let src = format!("{PRELUDE}{body}\n");
        let m = parse(&src);
        let model = build_model(
            &m,
            TextSize::of(src.as_str()),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let Some(ruff_python_ast::Stmt::Expr(s)) = m.body.last() else {
            panic!("last statement is not an expression: {body}");
        };
        f(&model, &s.value)
    }

    #[test]
    fn column_name_per_shape() {
        let cases: &[(&str, Option<&str>)] = &[
            ("\"a\"", Some("a")),
            ("\"a.b\"", Some("a.b")),
            ("F.col(\"a\")", Some("a")),
            ("F.column(\"a\")", Some("a")),
            ("F.col(col=\"a\")", Some("a")),
            ("F.col(\"*\")", Some("*")),
            // a local `col` helper is not pyspark's
            ("col(\"a\")", None),
            ("F.col(\"a\", \"b\")", None),
            ("F.col(c)", None),
            ("df.a", Some("a")),
            ("df[\"a\"]", Some("a")),
            ("df[\"*\"]", Some("*")),
            ("df[0]", None),
            // DataFrame API members and private attributes are not columns
            ("df.columns", None),
            ("df._jdf", None),
            ("c", None),
            ("F.upper(\"a\")", None),
            ("f\"a{1}\"", None),
        ];
        for (src, want) in cases {
            with_expr(src, |m, e| {
                assert_eq!(column_name(m, e).as_deref(), *want, "{src}");
            });
        }
    }

    #[test]
    fn refs_per_shape() {
        // (source, names, star, unknown)
        let cases: &[(&str, &[&str], bool, bool)] = &[
            ("F.col(\"a\") > 1", &["a"], false, false),
            ("F.col(\"a.b\") > 1", &["a", "b", "a.b"], false, false),
            ("F.col(\"`a.b`\")", &["a", "b", "a.b"], false, false),
            ("df.a == df[\"b\"]", &["a", "b"], false, false),
            // the SQL text of F.expr: identifier words, strings and numbers left out
            (
                "F.expr(\"upper(i.name) = 'x' AND n > 1\")",
                &["upper", "i", "name", "AND", "n"],
                false,
                false,
            ),
            ("F.expr(q)", &["k"], false, false),
            ("F.expr(f\"{c} > 1\")", &[], false, true),
            ("F.expr(x)", &[], false, true),
            ("\"id\"", &["id"], false, false),
            // a Column variable or an f-string name: unknown
            ("c > 1", &[], false, true),
            ("F.col(f\"{x}_id\")", &[], false, true),
            // modules and Spark objects are not columns; F.lit values are not names
            ("F.lit(\"x\") + df.a", &["a"], false, false),
            (
                "F.when(F.col(\"a\") > 1, F.lit(\"y\"))",
                &["a"],
                false,
                false,
            ),
            // lambdas are not entered
            (
                "F.transform(\"xs\", lambda x: x + y)",
                &["xs"],
                false,
                false,
            ),
            // stars: the expression itself only
            ("\"*\"", &[], true, false),
            ("F.col(\"t.*\")", &["t"], true, false),
            ("df[\"*\"]", &[], true, false),
            ("F.count(\"*\")", &[], false, false),
        ];
        for (src, names, star, unknown) in cases {
            with_expr(src, |m, e| {
                let r = ColumnRefs::of(m, e);
                let mut got: Vec<&str> = r.names.iter().map(String::as_str).collect();
                let mut want = names.to_vec();
                got.sort_unstable();
                want.sort_unstable();
                assert_eq!(got, want, "{src}");
                assert_eq!(r.star, *star, "star {src}");
                assert_eq!(r.unknown, *unknown, "unknown {src}");
            });
        }
    }

    #[test]
    fn refs_of_sql_text() {
        let r = ColumnRefs::of_sql(&SqlText::new("upper(i.name) = 'x' AND `my col` > 1"));
        assert_eq!(r.names, ["upper", "i", "name", "AND", "my col"]);
        assert!(!r.unknown && !r.star);
        assert!(r.contains("i"));
        assert!(!r.contains("x"));
    }

    #[test]
    fn column_args_per_shape() {
        // (call source, argument sources, unknown)
        let cases: &[(&str, &[&str], bool)] = &[
            ("f(\"a\", \"b\")", &["\"a\"", "\"b\""], false),
            ("f([\"a\", \"b\"])", &["\"a\"", "\"b\""], false),
            ("f((\"a\", \"b\"))", &["\"a\"", "\"b\""], false),
            ("f(*[\"a\", \"b\"])", &["\"a\"", "\"b\""], false),
            ("f(*(\"a\",))", &["\"a\""], false),
            ("f([\"a\"], \"b\")", &["\"a\"", "\"b\""], false),
            ("f(*cols)", &[], true),
            ("f(\"a\", *cols)", &["\"a\""], true),
            ("f(cols)", &["cols"], false),
        ];
        for (src, want, unknown) in cases {
            let full = format!("{PRELUDE}{src}\n");
            with_expr(src, |_, e| {
                let Expr::Call(c) = e else { panic!("{src}") };
                let got = column_args(c.arguments.args.iter());
                let texts: Vec<&str> = got
                    .exprs
                    .iter()
                    .map(|x| &full[usize::from(x.start())..usize::from(x.end())])
                    .collect();
                assert_eq!(texts, *want, "{src}");
                assert_eq!(got.unknown, *unknown, "unknown {src}");
            });
        }
    }

    #[test]
    fn all_columns_per_shape() {
        let cases = [
            ("df.columns", true),
            ("[F.col(x) for x in df.columns]", true),
            ("(F.col(x) for x in df.columns)", true),
            ("[F.col(x) for x in df.columns if x != \"id\"]", false),
            ("[x for x in df.columns for y in df.columns]", false),
            ("pdf.columns", false),
            ("df.schema", false),
        ];
        for (src, want) in cases {
            with_expr(src, |m, e| assert_eq!(all_columns(m, e), want, "{src}"));
        }
    }

    #[test]
    fn mentions_name_per_shape() {
        let cases = [
            ("F.col(v) + 1", "v", true),
            ("F.col(\"v\")", "v", false),
            ("df.v", "v", false),
            ("F.transform(\"xs\", lambda x: v)", "v", false),
            ("[v for _ in range(2)]", "v", true),
        ];
        for (src, name, want) in cases {
            with_expr(src, |_, e| {
                assert_eq!(mentions_name(e, name), want, "{src}")
            });
        }
    }
}
