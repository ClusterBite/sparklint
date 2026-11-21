//! "Which columns, in which order, does this frame have?" — the model side of the
//! [`FrameColumns`](crate::facts::FrameColumns) fact.
//!
//! The fact row says what each DataFrame step does to a frame's columns (rows-only
//! steps keep them, a `select` of a readable list sets them, anything else forgets
//! them) and how two paths merge. A frame whose columns nobody spelled out (a table
//! read, a parameter, a helper's result) is identified by where it came from: the
//! expression that made it, the bindings a name reaches, or the dotted text of a plain
//! attribute chain (`self.df`). [`SemanticModel::frame_columns`] answers with that
//! identity, so two sides agree exactly when they have the same columns in the same
//! order by construction. The oracle asks [`SemanticModel::receiver_columns`] before
//! each DataFrame step, so a step that keeps the columns keeps the receiver's identity.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::{Ranged, TextRange};

use crate::facts::{ColumnList, Columns, Fact, FrameColumns, Origin};
use crate::model::{BindingKind, SemanticModel};

/// How far names are followed to the frame they hold.
const NAME_HOPS: u32 = crate::values::NAME_HOPS;

impl SemanticModel<'_> {
    /// The columns of the frame `e` evaluates to: [`Columns::Known`] for a `select` of a
    /// readable list, otherwise [`Columns::SameAs`] the frame it was derived from by
    /// rows-only steps (its own origin when nothing is known). Never
    /// [`Columns::Unknown`].
    pub fn frame_columns(&self, e: &Expr) -> Columns {
        self.columns_at(e, None, 0)
    }

    /// [`Self::frame_columns`] of a step's receiver whose oracle facts are at hand
    /// (`known` = its `columns` fact): no second inference of the receiver.
    pub(crate) fn receiver_columns(&self, e: &Expr, known: Columns) -> Columns {
        self.columns_at(e, Some(known), 0)
    }

    fn columns_at(&self, e: &Expr, known: Option<Columns>, hops: u32) -> Columns {
        if let Expr::Name(n) = e {
            return self.name_columns(n, hops);
        }
        if let Some(path) = plain_path(e) {
            return Columns::SameAs(Origin::path(path));
        }
        match known.unwrap_or_else(|| self.ty_of(e).facts.columns) {
            Columns::Unknown => Columns::SameAs(Origin::Expr(e.range())),
            c => c,
        }
    }

    /// A name: what every reaching binding holds (a plain `name = value` followed to its
    /// value, anything else unknown), merged by the fact row as at an if/else; when the
    /// bindings do not agree, the name itself, reached by those bindings.
    fn name_columns(&self, n: &ast::ExprName, hops: u32) -> Columns {
        let reaching = self.reaching(n);
        if reaching.is_empty() {
            return Columns::SameAs(Origin::Expr(n.range));
        }
        if hops < NAME_HOPS {
            let merged = reaching
                .iter()
                .map(|b| match self.plain_value(*b) {
                    Some(v) if v.range() != n.range => {
                        self.columns_at(v, Some(self.binding(*b).ty.facts.columns), hops + 1)
                    }
                    _ => Columns::Unknown,
                })
                .reduce(FrameColumns::merge)
                .unwrap_or_default();
            if merged != Columns::Unknown {
                return merged;
            }
        }
        let ranges: Vec<TextRange> = reaching.iter().map(|b| self.binding(*b).range).collect();
        Columns::SameAs(Origin::bindings(n.id.as_str(), &ranges))
    }

    /// The value of a plain `name = value` (or `name: T = value`) binding: not tuple
    /// unpacking, not a chained or augmented assignment.
    fn plain_value(&self, id: crate::model::BindingId) -> Option<&Expr> {
        let b = self.binding(id);
        if !matches!(b.kind, BindingKind::Assign | BindingKind::AnnAssign) {
            return None;
        }
        let v = b.value?;
        (b.stmt_range.start() == b.range.start()
            && v.range().end() == b.stmt_range.end()
            && v.start() > b.range.end())
        .then_some(v)
    }

    /// The columns a `select(..)` call picks, when its arguments say: string literals
    /// (also one list / tuple literal of them), one name / `*name` holding the list, or
    /// another frame's `.columns` / `*frame.columns`.
    pub(crate) fn select_columns(&self, c: &ast::ExprCall) -> Option<Columns> {
        if !c.arguments.keywords.is_empty() || c.arguments.args.is_empty() {
            return None;
        }
        let strs = |items: &[Expr]| -> Option<Columns> {
            let names: Option<Vec<&str>> = items
                .iter()
                .map(|x| x.as_string_literal_expr().map(|s| s.value.to_str()))
                .collect();
            names.map(|n| Columns::Known(ColumnList::names(n)))
        };
        if let Some(k) = strs(&c.arguments.args) {
            return Some(k);
        }
        let [arg] = &*c.arguments.args else {
            return None;
        };
        let arg = match arg {
            Expr::Starred(s) => &*s.value,
            a => a,
        };
        match arg {
            Expr::List(l) => strs(&l.elts),
            Expr::Tuple(t) => strs(&t.elts),
            Expr::Name(n) => {
                let r = self.reaching(n);
                let ranges: Vec<TextRange> = r.iter().map(|b| self.binding(*b).range).collect();
                (!r.is_empty())
                    .then(|| Columns::Known(ColumnList::variable(n.id.as_str(), &ranges)))
            }
            Expr::Attribute(a)
                if a.attr.as_str() == "columns"
                    && (a.value.is_name_expr() || plain_path(&a.value).is_some()) =>
            {
                Some(self.frame_columns(&a.value))
            }
            _ => None,
        }
    }
}

/// The parts of an attribute chain with no calls (`self.df` -> `self`, `df`); `None`
/// for anything else (a plain name included).
fn plain_path(e: &Expr) -> Option<Vec<&str>> {
    let Expr::Attribute(a) = e else {
        return None;
    };
    let mut parts = vec![a.attr.as_str()];
    let mut cur = &*a.value;
    loop {
        match cur {
            Expr::Attribute(x) => {
                parts.push(x.attr.as_str());
                cur = &x.value;
            }
            Expr::Name(n) => {
                parts.push(n.id.as_str());
                parts.reverse();
                return Some(parts);
            }
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{NoSummaries, SemanticOptions, build_model};
    use ruff_python_ast::Expr;
    use ruff_text_size::{Ranged, TextSize};

    /// Each `union` line: do both sides have the same columns in the same order?
    #[test]
    fn union_sides() {
        let src = r#"
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
a = spark.table("a")
b = spark.table("b")
cols = ["x", "y"]
flag = cols[0] == "x"
if flag:
    m = a.filter("x > 1")
else:
    m = a.limit(3)
if flag:
    n = a.filter("x > 1")
else:
    n = a.withColumn("z", F.lit(1))
u, v = a.select(cols), b.select(cols)
h = a
class Job:
    def __init__(self):
        self.df = spark.table("t")
    def run(self):
        return self.df.filter("x > 1").union(self.df.limit(5))
a.union(a)
a.filter("x > 1").union(a.where("x < 0").cache())
a.select(cols).union(b.select(*cols))
a.select("x", "y").union(b.select(["x", "y"]))
a.select("x", "y").union(b.select("y", "x"))
a.union(b.select(a.columns))
a.join(b, "id", "left_anti").union(a)
a.join(b, "id", "inner").union(a)
a.unionByName(b).union(a)
a.unionByName(b, allowMissingColumns=True).union(a)
a.intersect(b).union(a.dropna())
a.withColumn("z", F.lit(1)).union(a)
a.union(b)
m.union(a)
n.union(a)
u.union(v)
h.union(a)
a[["x", "y"]].union(a)
spark.table("a").union(spark.table("a"))
"#;
        let want = [
            true, true, true, true, false, true, true, false, true, false, true, false, false,
            true, false, false, true, false, false,
        ];
        let m = ruff_python_parser::parse_module(src)
            .expect("valid test source")
            .into_syntax();
        let model = build_model(
            &m,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let unions: Vec<&Expr> = m
            .body
            .iter()
            .filter_map(|s| s.as_expr_stmt().map(|e| &*e.value))
            .collect();
        assert_eq!(unions.len(), want.len());
        for (e, want) in unions.iter().zip(want) {
            let sc = model.spark_call(e).expect("a union call");
            let other = sc.arg(0, "other").expect("other side");
            let got = model
                .frame_columns(sc.receiver)
                .agrees(model.frame_columns(other));
            assert_eq!(got, want, "{:?}", &src[e.range()]);
        }
        // An attribute chain with no calls is the same frame twice (in a method).
        let class_union = m
            .body
            .iter()
            .find_map(|s| s.as_class_def_stmt())
            .and_then(|c| c.body[1].as_function_def_stmt())
            .and_then(|f| f.body[0].as_return_stmt())
            .and_then(|r| r.value.as_deref())
            .expect("return in Job.run");
        let sc = model.spark_call(class_union).expect("union");
        let other = sc.arg(0, "other").unwrap();
        assert!(
            model
                .frame_columns(sc.receiver)
                .agrees(model.frame_columns(other))
        );
    }
}
