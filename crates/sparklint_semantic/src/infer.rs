//! The type oracle: `ty_of(expr)` and `qualified_name(expr)`.

use ruff_python_ast::{self as ast, Expr, Number, Operator, UnaryOp};
use ruff_text_size::Ranged;

use crate::api::{self, DATAFRAME_ONLY_METHODS, SESSION_ONLY_ATTRS, canonical_qualified};
use crate::model::{BindingKind, SemanticModel};
use crate::types::{Confidence, Elem, Facts, Ty, TypeInfo};

/// Bound under which `spark.range(n)` counts as a small, bounded DataFrame.
const SMALL_RANGE: i64 = 10_000;

/// pandas-on-Spark DataFrame/Series methods that return a frame/series of the same
/// kind. Deliberately excludes `groupby`/`rolling`/`expanding` (different objects),
/// `head`/`tail` (bounded results) and reductions.
const PS_FRAME_PRESERVING: &[&str] = &[
    "abs",
    "add",
    "sub",
    "mul",
    "div",
    "truediv",
    "floordiv",
    "mod",
    "pow",
    "radd",
    "rsub",
    "rmul",
    "rdiv",
    "rtruediv",
    "astype",
    "copy",
    "drop",
    "dropna",
    "drop_duplicates",
    "fillna",
    "rename",
    "replace",
    "reset_index",
    "set_index",
    "sort_values",
    "sort_index",
    "assign",
    "where",
    "mask",
    "round",
    "clip",
    "merge",
    "join",
    "query",
    "isin",
    "isna",
    "isnull",
    "notna",
    "notnull",
    "to_frame",
    "rank",
    "shift",
    "diff",
    "pct_change",
    "cumsum",
    "cumprod",
    "cummax",
    "cummin",
];

/// Python builtins we resolve as `builtins.<name>`.
pub(crate) const BUILTINS: &[&str] = &[
    "print",
    "len",
    "list",
    "dict",
    "set",
    "tuple",
    "str",
    "int",
    "float",
    "bool",
    "sum",
    "min",
    "max",
    "sorted",
    "any",
    "all",
    "enumerate",
    "zip",
    "range",
    "map",
    "filter",
    "iter",
    "next",
    "open",
    "isinstance",
    "getattr",
    "setattr",
    "hasattr",
    "abs",
    "round",
    "repr",
    "type",
    "super",
    "reversed",
    "frozenset",
    "object",
    "Exception",
    "ValueError",
    "id",
    "hash",
    "divmod",
    "pow",
    "format",
    "vars",
    "dir",
    "input",
    "exit",
];

/// Integer literal value of an expression (`10`, `-1`).
pub fn int_literal(e: &Expr) -> Option<i64> {
    match e {
        Expr::NumberLiteral(n) => match &n.value {
            Number::Int(i) => i.as_i64(),
            _ => None,
        },
        Expr::UnaryOp(u) if u.op == UnaryOp::USub => int_literal(&u.operand).map(|v| -v),
        _ => None,
    }
}

/// String literal value (not f-strings).
pub fn str_literal(e: &Expr) -> Option<&str> {
    match e {
        Expr::StringLiteral(s) => Some(s.value.to_str()),
        _ => None,
    }
}

/// Static text of a string literal or f-string (interpolations become `{}`).
pub fn static_text(e: &Expr) -> Option<String> {
    match e {
        Expr::StringLiteral(s) => Some(s.value.to_str().to_string()),
        Expr::FString(f) => {
            let mut out = String::new();
            for part in f.value.iter() {
                match part {
                    ast::FStringPartRef::Literal(l) => out.push_str(&l.value),
                    ast::FStringPartRef::FString(fs) => {
                        for el in &fs.elements {
                            match el {
                                ast::InterpolatedStringElement::Literal(l) => {
                                    out.push_str(&l.value)
                                }
                                ast::InterpolatedStringElement::Interpolation(_) => {
                                    out.push_str("{}")
                                }
                            }
                        }
                    }
                }
            }
            Some(out)
        }
        _ => None,
    }
}

/// Positional argument `i` or keyword `kw` of a call.
pub fn call_arg<'e>(call: &'e ast::ExprCall, i: usize, kw: &str) -> Option<&'e Expr> {
    if let Some(k) = call
        .arguments
        .keywords
        .iter()
        .find(|k| k.arg.as_ref().is_some_and(|a| a.as_str() == kw))
    {
        return Some(&k.value);
    }
    let args = &call.arguments.args;
    if args.iter().take(i + 1).any(|a| a.is_starred_expr()) {
        return None;
    }
    args.get(i)
}

/// Whether a name matches the DataFrame naming heuristic (`df`, `*_df`, `df_*`, `sdf`).
pub fn is_dataframe_name(name: &str) -> bool {
    let n = name.trim_start_matches('_');
    n == "df"
        || n == "sdf"
        || n.ends_with("_df")
        || n.ends_with("_sdf")
        || n.starts_with("df_")
        || n == "dataframe"
}

/// Whether a name matches the SparkSession naming heuristic (`spark`, `spark_session`).
pub fn is_session_name(name: &str) -> bool {
    let n = name.trim_start_matches('_');
    n == "spark" || n == "spark_session" || n == "sparkSession" || n.ends_with("_spark")
}

/// Whether a name looks like a logger (`logger`, `log`, `LOGGER`, `app_logger`, …).
/// Unlike cylint this is not a substring match (`catalog` is not a logger).
pub fn is_logger_name(name: &str) -> bool {
    let n = name.trim_start_matches('_').to_ascii_lowercase();
    n == "log" || n == "logger" || n == "logging" || n.ends_with("_logger") || n.ends_with("_log")
}

impl<'a> SemanticModel<'a> {
    /// The canonical qualified name an expression refers to, if it resolves to an
    /// import (or a builtin), e.g. `F.col` → `pyspark.sql.functions.col`.
    pub fn qualified_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Name(n) => {
                let use_site = self.use_site(n)?;
                if use_site.reaching.is_empty() {
                    let name = n.id.as_str();
                    if BUILTINS.contains(&name) {
                        return Some(format!("builtins.{name}"));
                    }
                    if matches!(name, "display" | "displayHTML") {
                        return Some(format!("databricks.{name}"));
                    }
                    if self.star_functions && api::api().function(name).is_some() {
                        return Some(format!("pyspark.sql.functions.{name}"));
                    }
                    return None;
                }
                let mut q: Option<&String> = None;
                for b in &use_site.reaching {
                    let bq = self.binding(*b).qualified.as_ref()?;
                    match q {
                        None => q = Some(bq),
                        Some(prev) if prev == bq => {}
                        Some(_) => return None,
                    }
                }
                q.cloned()
            }
            Expr::Attribute(a) => {
                let base = self.qualified_name(&a.value)?;
                Some(canonical_qualified(&format!("{base}.{}", a.attr.as_str())))
            }
            _ => None,
        }
    }

    /// Whether the expression resolves to `pyspark.sql.functions.<name>`.
    pub fn is_spark_function(&self, expr: &Expr, name: &str) -> bool {
        self.qualified_name(expr)
            .is_some_and(|q| q.strip_prefix("pyspark.sql.functions.") == Some(name))
    }

    /// The type of an expression.
    pub fn ty_of(&self, expr: &Expr) -> TypeInfo {
        match expr {
            Expr::Name(n) => self.ty_of_name(n),
            Expr::Attribute(a) => self.ty_of_attribute(a, expr),
            Expr::Call(c) => self.ty_of_call(c),
            Expr::Subscript(s) => self.ty_of_subscript(s),
            Expr::Compare(c) => {
                // Identity / membership tests are plain Python bools even on Columns.
                if c.ops.iter().all(|op| {
                    matches!(
                        op,
                        ast::CmpOp::Is | ast::CmpOp::IsNot | ast::CmpOp::In | ast::CmpOp::NotIn
                    )
                }) {
                    return TypeInfo::certain(Ty::Bool);
                }
                // `psdf["x"] > 0` is an element-wise pandas-on-Spark mask.
                if let [lhs, rhs] = &*c.operands {
                    let l = self.ty_of(lhs);
                    if l.ty == Ty::PandasOnSparkFrame
                        && matches!(
                            self.ty_of(rhs).ty,
                            Ty::Int | Ty::Float | Ty::Str | Ty::Bool | Ty::PandasOnSparkFrame
                        )
                    {
                        return TypeInfo::certain(Ty::PandasOnSparkFrame)
                            .with_confidence(l.confidence);
                    }
                }
                let mut col: Option<TypeInfo> = None;
                let mut local: Option<TypeInfo> = None;
                for op in c.operands.iter() {
                    let t = self.ty_of(op);
                    if t.is_column() {
                        col = Some(col.map_or(t, |c| c.join(t)));
                    } else if t.ty.is_local_data() {
                        local = Some(t);
                    }
                }
                // `pdf["x"] > 0` is an element-wise mask, still a local data value.
                col.map(|c| TypeInfo::certain(Ty::Column).with_confidence(c.confidence))
                    .or(local.map(non_spark))
                    .unwrap_or_else(|| TypeInfo::likely(Ty::Bool))
            }
            Expr::BinOp(b) => {
                let l = self.ty_of(&b.left);
                let r = self.ty_of(&b.right);
                if l.is_column() || r.is_column() {
                    let conf = if l.is_column() && r.is_column() {
                        l.confidence.max(r.confidence)
                    } else if l.is_column() {
                        l.confidence
                    } else {
                        r.confidence
                    };
                    return TypeInfo::certain(Ty::Column).with_confidence(conf);
                }
                // pandas-on-Spark arithmetic: frame op frame / frame op number.
                let ps = |t: &TypeInfo| t.ty == Ty::PandasOnSparkFrame;
                let num = |t: &TypeInfo| matches!(t.ty, Ty::Int | Ty::Float | Ty::Bool);
                if (ps(&l) && (ps(&r) || num(&r))) || (ps(&r) && num(&l)) {
                    let conf = match (ps(&l), ps(&r)) {
                        (true, true) => l.confidence.min(r.confidence),
                        (true, false) => l.confidence,
                        _ => r.confidence,
                    };
                    return TypeInfo::certain(Ty::PandasOnSparkFrame).with_confidence(conf);
                }
                // pandas / numpy arithmetic stays a local data value.
                if l.ty.is_local_data() {
                    return non_spark(l);
                }
                if r.ty.is_local_data() {
                    return non_spark(r);
                }
                match (l.ty, r.ty, b.op) {
                    (Ty::Str, _, Operator::Add | Operator::Mod) | (_, Ty::Str, Operator::Add) => {
                        TypeInfo::likely(Ty::Str)
                    }
                    (Ty::Int, Ty::Int, Operator::Div) => TypeInfo::likely(Ty::Float),
                    (Ty::Int, Ty::Int, _) => TypeInfo::likely(Ty::Int),
                    (Ty::Float | Ty::Int, Ty::Float | Ty::Int, _) => TypeInfo::likely(Ty::Float),
                    (Ty::List(e), Ty::List(e2), Operator::Add) => {
                        TypeInfo::likely(Ty::List(if e == e2 { e } else { Elem::Unknown }))
                    }
                    _ => TypeInfo::UNKNOWN,
                }
            }
            Expr::UnaryOp(u) => {
                let t = self.ty_of(&u.operand);
                match u.op {
                    UnaryOp::Not => TypeInfo::likely(Ty::Bool),
                    _ if t.is_column() => t,
                    _ if t.ty.is_local_data() => non_spark(t),
                    UnaryOp::USub | UnaryOp::UAdd if matches!(t.ty, Ty::Int | Ty::Float) => t,
                    _ => TypeInfo::UNKNOWN,
                }
            }
            Expr::BoolOp(b) => b
                .values
                .iter()
                .map(|v| self.ty_of(v))
                .reduce(TypeInfo::join)
                .unwrap_or(TypeInfo::UNKNOWN),
            Expr::If(i) => self.ty_of(&i.body).join(self.ty_of(&i.orelse)),
            Expr::Named(n) => self.ty_of(&n.value),
            Expr::Starred(s) => self.ty_of(&s.value),
            Expr::StringLiteral(_) | Expr::FString(_) => TypeInfo::certain(Ty::Str),
            Expr::BytesLiteral(_) => TypeInfo::certain(Ty::Object),
            Expr::NumberLiteral(n) => match n.value {
                Number::Int(_) => TypeInfo::certain(Ty::Int),
                _ => TypeInfo::certain(Ty::Float),
            },
            Expr::BooleanLiteral(_) => TypeInfo::certain(Ty::Bool),
            Expr::NoneLiteral(_) => TypeInfo::certain(Ty::NoneTy),
            Expr::List(l) => TypeInfo::certain(Ty::List(self.elem_of_elts(&l.elts))),
            Expr::ListComp(l) => TypeInfo::certain(Ty::List(self.ty_of(&l.elt).ty.elem())),
            Expr::Generator(g) => TypeInfo::certain(Ty::List(self.ty_of(&g.elt).ty.elem())),
            Expr::Tuple(_) => TypeInfo::certain(Ty::Tuple),
            Expr::Dict(_) | Expr::DictComp(_) => TypeInfo::certain(Ty::Dict),
            Expr::Set(_) | Expr::SetComp(_) => TypeInfo::certain(Ty::Set),
            Expr::Lambda(_) => TypeInfo::certain(Ty::Function),
            _ => TypeInfo::UNKNOWN,
        }
    }

    fn elem_of_elts(&self, elts: &[Expr]) -> Elem {
        let mut elem: Option<Elem> = None;
        for e in elts {
            let t = self.ty_of(e).ty.elem();
            match elem {
                None => elem = Some(t),
                Some(p) if p == t => {}
                Some(_) => return Elem::Unknown,
            }
        }
        elem.unwrap_or(Elem::Unknown)
    }

    fn ty_of_name(&self, n: &ast::ExprName) -> TypeInfo {
        let Some(use_site) = self.use_site(n) else {
            return TypeInfo::UNKNOWN;
        };
        let name = n.id.as_str();
        let t = if use_site.reaching.is_empty() {
            self.unbound_type(name)
        } else {
            use_site
                .reaching
                .iter()
                .map(|b| self.binding(*b).ty)
                .reduce(TypeInfo::join)
                .unwrap_or(TypeInfo::UNKNOWN)
        };
        if t.is_unknown() {
            return self.name_heuristic(name);
        }
        t
    }

    pub(crate) fn name_heuristic(&self, name: &str) -> TypeInfo {
        if !self.opts.heuristics {
            return TypeInfo::UNKNOWN;
        }
        if is_dataframe_name(name) {
            TypeInfo::likely(Ty::DataFrame)
        } else if is_session_name(name) {
            TypeInfo::likely(Ty::SparkSession)
        } else if is_logger_name(name) {
            TypeInfo::likely(Ty::Logger)
        } else {
            TypeInfo::UNKNOWN
        }
    }

    /// Type of a name with no binding in any enclosing scope (globals/builtins).
    pub(crate) fn unbound_type(&self, name: &str) -> TypeInfo {
        if let Some((_, t)) = self.opts.known_globals.iter().find(|(n, _)| n == name) {
            return TypeInfo::certain(*t);
        }
        let nb = match name {
            "spark" => Some(Ty::SparkSession),
            "sc" => Some(Ty::SparkContext),
            "sqlContext" => Some(Ty::SqlContext),
            "dbutils" => Some(Ty::DbUtils),
            _ => None,
        };
        if let Some(t) = nb {
            if self.opts.is_notebook {
                return TypeInfo::certain(t);
            }
            if self.opts.heuristics {
                return TypeInfo::likely(t);
            }
        }
        if BUILTINS.contains(&name) || matches!(name, "display" | "displayHTML") {
            return TypeInfo::certain(Ty::Function);
        }
        TypeInfo::UNKNOWN
    }

    fn ty_of_attribute(&self, a: &ast::ExprAttribute, expr: &Expr) -> TypeInfo {
        let attr = a.attr.as_str();
        let q = self.qualified_name(expr);
        if let Some(q) = &q
            && let Some(t) = api::qualified_value_type(q)
        {
            return TypeInfo::certain(t);
        }
        let base = self.ty_of(&a.value);
        if let Some(q) = &q
            && !(base.ty.is_spark() || matches!(base.ty, Ty::Instance(_) | Ty::DbUtils))
        {
            if q.starts_with("pyspark.sql.functions.") {
                return TypeInfo::certain(Ty::Function);
            }
            return TypeInfo::certain(Ty::Object);
        }
        match base.ty {
            Ty::Instance(c) => self.class_attr(c, attr).unwrap_or(TypeInfo::UNKNOWN),
            Ty::DbUtils => TypeInfo::likely(Ty::DbUtils),
            // `pdf.values`, `pdf.loc`, `pdf.T`, `arr.shape`: still pandas / numpy.
            t if t.is_local_data() => non_spark(base),
            t if t.is_spark() => match api::api().method_of(t, attr) {
                Some(m) if m.property => {
                    let mut out = TypeInfo::certain(m.returns).with_confidence(base.confidence);
                    if t == Ty::DataFrame {
                        out.facts = Facts {
                            cached: false,
                            ..base.facts
                        };
                    }
                    if t == Ty::SparkSession && attr == "readStream" {
                        out.facts.streaming = true;
                    }
                    out
                }
                Some(_) => TypeInfo::certain(Ty::Function),
                // `df.colname` is a Column; private attributes (`self._jdf`,
                // `df._cached_schema`) are not.
                None if t == Ty::DataFrame && !attr.starts_with('_') => {
                    TypeInfo::certain(Ty::Column).with_confidence(base.confidence)
                }
                // `psdf.iloc`, `psdf.values`, `psdf.colname`: pandas-on-Spark objects the
                // rules do not model. `psdf.spark` and private attributes
                // (`psdf._internal.spark_frame`) are the doors back to Spark, so they
                // stay unknown.
                None if t == Ty::PandasOnSparkFrame
                    && attr != "spark"
                    && !attr.starts_with('_') =>
                {
                    non_spark(base)
                }
                None => TypeInfo::UNKNOWN,
            },
            _ => TypeInfo::UNKNOWN,
        }
    }

    fn class_attr(&self, class: u32, attr: &str) -> Option<TypeInfo> {
        let key = (class, attr.to_string());
        self.class_attrs
            .get(&key)
            .or_else(|| self.prior.class_attrs.get(&key))
            .copied()
    }

    fn ty_of_subscript(&self, s: &ast::ExprSubscript) -> TypeInfo {
        let v = self.ty_of(&s.value);
        match v.ty {
            Ty::DataFrame => match &*s.slice {
                Expr::List(_) | Expr::Tuple(_) => v,
                Expr::Slice(_) => TypeInfo::UNKNOWN,
                _ => TypeInfo::certain(Ty::Column).with_confidence(v.confidence),
            },
            // `psdf["x"]` / `psdf[["a", "b"]]` / `psdf[psdf["x"] > 0]`: still pandas-on-Spark.
            // Integer / slice / unknown keys (positional or label lookups) are not modelled.
            Ty::PandasOnSparkFrame => {
                let key = &*s.slice;
                let frame_key = matches!(key, Expr::StringLiteral(_) | Expr::List(_))
                    || self.ty_of(key).ty == Ty::PandasOnSparkFrame;
                if frame_key {
                    TypeInfo::certain(Ty::PandasOnSparkFrame).with_confidence(v.confidence)
                } else {
                    non_spark(v)
                }
            }
            Ty::List(e) => {
                if s.slice.is_slice_expr() {
                    v
                } else {
                    let mut t = TypeInfo::certain(Ty::from_elem(e)).with_confidence(v.confidence);
                    if e == Elem::Unknown {
                        t = TypeInfo::UNKNOWN;
                    }
                    t.facts.from_spark = v.facts.from_spark;
                    t
                }
            }
            // `pdf["x"]`, `pdf[mask]`, `pdf.loc[...]`, `arr[0]`: still pandas / numpy.
            t if t.is_local_data() => non_spark(v),
            Ty::Dict | Ty::Row | Ty::Tuple => TypeInfo::UNKNOWN,
            _ => TypeInfo::UNKNOWN,
        }
    }

    /// Return type of a call to a function defined in this module (by its scope).
    fn local_function_return(&self, scope: u32) -> Option<TypeInfo> {
        if let Some(f) = self.function_summary(scope)
            && (f.annotated_return.is_some() || f.is_udf)
        {
            return Some(if f.is_udf {
                TypeInfo::certain(Ty::Column)
            } else {
                f.return_type()
            });
        }
        self.prior.func_returns.get(&scope).copied().or_else(|| {
            self.function_summary(scope).and_then(|f| {
                // Function bodies are walked after the module body: until a `return`
                // was seen the summary says "returns None", which is only true once
                // the body is done (then the first pass recorded it in `prior`).
                let t = f.return_type();
                (f.has_return_value && !t.is_unknown()).then_some(t)
            })
        })
    }

    fn ty_of_call(&self, c: &ast::ExprCall) -> TypeInfo {
        let func = &*c.func;
        // Calls of local functions / classes (through the binding).
        if let Expr::Name(n) = func
            && let [b] = self.reaching(n)
        {
            let binding = self.binding(*b);
            match (binding.kind, binding.def_scope) {
                (BindingKind::FunctionDef, Some(s)) => {
                    if binding.ty.ty == Ty::Udf {
                        return TypeInfo::certain(Ty::Column);
                    }
                    return self.local_function_return(s).unwrap_or(TypeInfo::UNKNOWN);
                }
                (BindingKind::ClassDef, Some(s)) => return TypeInfo::certain(Ty::Instance(s)),
                _ => {}
            }
        }
        if let Some(q) = self.qualified_name(func) {
            if let Some(t) = self.call_of_qualified(&q, c) {
                return t;
            }
            if let Some(t) = self.external.function_return(&q) {
                return t;
            }
        }
        match func {
            Expr::Attribute(a) => {
                let recv = self.ty_of(&a.value);
                // Py4J handles (`self._jdf.writeStream()`, `df._jdf.union(...)`) mirror
                // the DataFrame API: name evidence must not turn them into DataFrames.
                if recv.is_unknown()
                    && matches!(&*a.value, Expr::Attribute(inner) if inner.attr.as_str().starts_with("_j"))
                {
                    return TypeInfo::UNKNOWN;
                }
                self.method_call(recv, a.attr.as_str(), c)
            }
            _ => {
                let callee = self.ty_of(func);
                match callee.ty {
                    Ty::Udf => TypeInfo::certain(Ty::Column),
                    _ => TypeInfo::UNKNOWN,
                }
            }
        }
    }

    fn call_of_qualified(&self, q: &str, c: &ast::ExprCall) -> Option<TypeInfo> {
        if let Some(f) = q.strip_prefix("pyspark.sql.functions.") {
            return Some(match f {
                "udf" | "pandas_udf" | "arrow_udf" => TypeInfo::certain(Ty::Udf),
                "broadcast" => c
                    .arguments
                    .args
                    .first()
                    .map(|a| self.ty_of(a))
                    .filter(|t| t.is_dataframe())
                    .unwrap_or(TypeInfo::likely(Ty::DataFrame)),
                _ => match api::api().function(f) {
                    Some(Ty::Unknown) => TypeInfo::UNKNOWN,
                    Some(t) => TypeInfo::certain(t),
                    None => TypeInfo::likely(Ty::Column),
                },
            });
        }
        if let Some(m) = q.strip_prefix("pyspark.sql.Window.") {
            let facts = window_facts(Facts::default(), m, c);
            return Some(TypeInfo::certain(Ty::WindowSpec).with_facts(facts));
        }
        let t = match q {
            "pyspark.sql.SparkSession.getActiveSession"
            | "pyspark.sql.SparkSession.active"
            | "pyspark.sql.SparkSession"
            | "databricks.connect.DatabricksSession.getActiveSession" => Ty::SparkSession,
            "pyspark.SparkContext" | "pyspark.SparkContext.getOrCreate" => Ty::SparkContext,
            "pyspark.sql.SQLContext"
            | "pyspark.sql.SQLContext.getOrCreate"
            | "pyspark.sql.HiveContext" => Ty::SqlContext,
            "pyspark.sql.Row" => Ty::Row,
            "pyspark.sql.Column" => Ty::Column,
            "delta.tables.DeltaTable.forName"
            | "delta.tables.DeltaTable.forPath"
            | "delta.tables.DeltaTable.convertToDelta"
            | "delta.DeltaTable.forName"
            | "delta.DeltaTable.forPath" => Ty::DeltaTable,
            "logging.getLogger"
            | "structlog.get_logger"
            | "structlog.getLogger"
            | "logging.LoggerAdapter"
            | "logging.Logger" => Ty::Logger,
            "builtins.len" | "builtins.int" | "builtins.hash" | "builtins.id" => Ty::Int,
            "builtins.str" | "builtins.repr" | "builtins.format" => Ty::Str,
            "builtins.float" => Ty::Float,
            "builtins.bool"
            | "builtins.isinstance"
            | "builtins.hasattr"
            | "builtins.any"
            | "builtins.all" => Ty::Bool,
            "builtins.dict" => Ty::Dict,
            "builtins.set" | "builtins.frozenset" => Ty::Set,
            "builtins.tuple" => Ty::Tuple,
            "builtins.print" => Ty::NoneTy,
            "builtins.list" | "builtins.sorted" | "builtins.reversed" => {
                let inner = c.arguments.args.first().map(|a| self.ty_of(a));
                let mut t = TypeInfo::certain(Ty::List(match inner.map(|t| t.ty) {
                    Some(Ty::List(e)) => e,
                    _ => Elem::Unknown,
                }));
                if let Some(i) = inner {
                    t.facts.from_spark = i.facts.from_spark;
                    t.facts.bounded = i.facts.bounded;
                }
                return Some(t);
            }
            "builtins.range" | "builtins.enumerate" | "builtins.zip" | "builtins.map"
            | "builtins.filter" => Ty::List(Elem::Unknown),
            "functools.reduce" => return Some(self.reduce_type(c)),
            _ => {
                if let Some(rest) = q.strip_prefix("pandas.") {
                    if rest == "DataFrame"
                        || rest.starts_with("read_")
                        || rest == "concat"
                        || rest == "merge"
                    {
                        return Some(TypeInfo::certain(Ty::PandasFrame));
                    }
                    // `pd.Series(..)`, `pd.json_normalize(..)`, `pd.to_datetime(..)`, …:
                    // no pandas function returns a Spark object.
                    return Some(TypeInfo::certain(Ty::NonSpark));
                }
                // numpy and polars values are driver-side data too.
                if q.starts_with("numpy.") || q.starts_with("polars.") {
                    return Some(TypeInfo::certain(Ty::NonSpark));
                }
                if let Some(rest) = q.strip_prefix("pyspark.pandas.") {
                    if rest == "DataFrame"
                        || rest == "Series"
                        || rest.starts_with("read_")
                        || matches!(rest, "concat" | "from_pandas" | "range" | "sql")
                    {
                        return Some(TypeInfo::certain(Ty::PandasOnSparkFrame));
                    }
                    return None;
                }
                if q.starts_with("pyspark.sql.types.") {
                    return Some(TypeInfo::certain(Ty::DataType));
                }
                return None;
            }
        };
        Some(TypeInfo::certain(t))
    }

    /// `functools.reduce(DataFrame.unionByName, dfs)` / `reduce(lambda a, b: …, xs, df)`.
    fn reduce_type(&self, c: &ast::ExprCall) -> TypeInfo {
        let args = &c.arguments.args;
        if let Some(init) = args.get(2) {
            let t = self.ty_of(init);
            if t.is_dataframe() {
                return TypeInfo {
                    facts: Facts::default(),
                    ..t
                };
            }
        }
        if let Some(f) = args.first()
            && let Some(q) = self.qualified_name(f)
            && q.starts_with("pyspark.sql.DataFrame.")
        {
            return TypeInfo::certain(Ty::DataFrame);
        }
        if let Some(seq) = args.get(1)
            && let Ty::List(Elem::DataFrame) = self.ty_of(seq).ty
        {
            return TypeInfo::likely(Ty::DataFrame);
        }
        TypeInfo::UNKNOWN
    }

    /// Result of `recv.method(...)`.
    pub(crate) fn method_call(&self, recv: TypeInfo, m: &str, c: &ast::ExprCall) -> TypeInfo {
        let mut recv = recv;
        if recv.is_unknown() {
            if !self.opts.heuristics {
                return TypeInfo::UNKNOWN;
            }
            if DATAFRAME_ONLY_METHODS.contains(&m) {
                // `dg.DataGenerator(..).withIdOutput().withColumn(..)`: a builder of
                // another library that mirrors the DataFrame API is not a DataFrame.
                if let Expr::Attribute(a) = &*c.func
                    && self.foreign_object_chain(&a.value)
                {
                    return TypeInfo::UNKNOWN;
                }
                recv = TypeInfo::likely(Ty::DataFrame);
            } else if SESSION_ONLY_ATTRS.contains(&m) && m == "createDataFrame" {
                recv = TypeInfo::likely(Ty::SparkSession);
            } else {
                return TypeInfo::UNKNOWN;
            }
        }
        let conf = recv.confidence;
        match recv.ty {
            Ty::Instance(cls) => {
                let Some(scope) = self.class_methods.get(&(cls, m.to_string())) else {
                    return TypeInfo::UNKNOWN;
                };
                self.local_function_return(*scope)
                    .unwrap_or(TypeInfo::UNKNOWN)
            }
            Ty::Logger => TypeInfo::certain(Ty::NoneTy),
            Ty::Udf => TypeInfo::certain(Ty::Udf),
            Ty::DataFrame => self.dataframe_method(recv, m, c),
            Ty::SessionBuilder => match m {
                "getOrCreate" | "create" => {
                    TypeInfo::certain(Ty::SparkSession).with_confidence(conf)
                }
                _ => recv,
            },
            Ty::SparkSession | Ty::SqlContext => self.session_method(recv, m, c),
            Ty::WindowSpec => {
                let facts = window_facts(recv.facts, m, c);
                TypeInfo::certain(Ty::WindowSpec)
                    .with_confidence(conf)
                    .with_facts(facts)
            }
            Ty::GroupedData => match api::api().method_of(Ty::GroupedData, m) {
                Some(mi) => {
                    let mut t = TypeInfo::certain(mi.returns).with_confidence(conf);
                    t.facts = Facts {
                        bounded: recv.facts.bounded,
                        reduced: mi.returns == Ty::DataFrame,
                        streaming: recv.facts.streaming,
                        ..Facts::default()
                    };
                    t
                }
                None => TypeInfo::UNKNOWN,
            },
            Ty::DataFrameReader | Ty::DataStreamReader => match api::api().method_of(recv.ty, m) {
                Some(mi) => {
                    let mut t = TypeInfo::certain(mi.returns).with_confidence(conf);
                    t.facts.streaming = recv.ty == Ty::DataStreamReader || recv.facts.streaming;
                    t
                }
                None => TypeInfo::UNKNOWN,
            },
            Ty::DbUtils => TypeInfo::likely(Ty::DbUtils),
            // `pdf.join(..)`, `pdf.groupby(..).sum()`, `arr.reshape(..)`: whatever a
            // pandas / numpy method returns, it is not a Spark object. (An unknown
            // result bound to `pd_df` would be taken for a Spark DataFrame by its name.)
            Ty::PandasFrame | Ty::NonSpark => non_spark(recv),
            Ty::PandasOnSparkFrame => match api::api().method_of(Ty::PandasOnSparkFrame, m) {
                Some(mi) => TypeInfo::certain(mi.returns).with_confidence(conf),
                // Methods that return another frame/series of the same kind.
                None if PS_FRAME_PRESERVING.contains(&m) => recv,
                // Private methods are pyspark's own plumbing (they hand out Spark frames).
                None if m.starts_with('_') => TypeInfo::UNKNOWN,
                // Anything else (`groupby`, `rolling`, `head`, reductions, ...) is some
                // pandas-on-Spark object the rules do not model: not a `pyspark.sql`
                // DataFrame, whatever the variable is called.
                None => non_spark(recv),
            },
            Ty::List(_) | Ty::Str | Ty::Dict | Ty::Set | Ty::Tuple | Ty::Int | Ty::Float => {
                match (recv.ty, m) {
                    (Ty::Str, "join" | "format" | "lower" | "upper" | "strip" | "replace") => {
                        TypeInfo::certain(Ty::Str)
                    }
                    (Ty::Str, "split") => TypeInfo::certain(Ty::List(Elem::Str)),
                    (Ty::List(_) | Ty::Str, "count" | "index") => TypeInfo::certain(Ty::Int),
                    (Ty::List(_), "copy") => recv,
                    (Ty::Set, "union" | "intersection" | "difference") => {
                        TypeInfo::certain(Ty::Set)
                    }
                    _ => TypeInfo::UNKNOWN,
                }
            }
            t if t.is_spark() => match api::api().method_of(t, m) {
                Some(mi) => {
                    let mut out = TypeInfo::certain(mi.returns).with_confidence(conf);
                    if mi.returns == Ty::Rdd || mi.returns == Ty::DataFrame {
                        out.facts.bounded = recv.facts.bounded;
                    }
                    if t == Ty::Rdd && matches!(mi.returns, Ty::List(_)) {
                        out.facts.from_spark = true;
                        out.facts.bounded = match m {
                            "take" | "takeOrdered" | "top" | "takeSample" => c
                                .arguments
                                .args
                                .first()
                                .and_then(int_literal)
                                .map(|n| n.max(0) as u64),
                            _ => recv.facts.bounded,
                        };
                    }
                    out
                }
                None => TypeInfo::UNKNOWN,
            },
            _ => TypeInfo::UNKNOWN,
        }
    }

    fn session_method(&self, recv: TypeInfo, m: &str, c: &ast::ExprCall) -> TypeInfo {
        let conf = recv.confidence;
        match m {
            "createDataFrame" => {
                let mut t = TypeInfo::certain(Ty::DataFrame).with_confidence(conf);
                if let Some(data) = call_arg(c, 0, "data") {
                    match data {
                        Expr::List(l) => t.facts.bounded = Some(l.elts.len() as u64),
                        Expr::Tuple(l) => t.facts.bounded = Some(l.elts.len() as u64),
                        _ => {}
                    }
                }
                t
            }
            "range" => {
                let mut t = TypeInfo::certain(Ty::DataFrame).with_confidence(conf);
                let args = &c.arguments.args;
                let n = match (
                    args.first().and_then(int_literal),
                    args.get(1).and_then(int_literal),
                ) {
                    (Some(s), Some(e)) => Some(e - s),
                    (Some(n), None) if args.len() == 1 => Some(n),
                    _ => None,
                };
                if let Some(n) = n
                    && (0..=SMALL_RANGE).contains(&n)
                {
                    t.facts.bounded = Some(n as u64);
                }
                t
            }
            "sql" => {
                let mut t = TypeInfo::certain(Ty::DataFrame).with_confidence(conf);
                if let Some(q) = self.sql_query(c) {
                    if let Some(n) = q.trailing_limit() {
                        t.facts.bounded = Some(n);
                    } else if q.single_row() {
                        t.facts.bounded = Some(1);
                    }
                }
                t
            }
            _ => match api::api().method_of(Ty::SparkSession, m) {
                Some(mi) => TypeInfo::certain(mi.returns).with_confidence(conf),
                None => TypeInfo::UNKNOWN,
            },
        }
    }

    fn dataframe_method(&self, recv: TypeInfo, m: &str, c: &ast::ExprCall) -> TypeInfo {
        let conf = recv.confidence;
        let args = &c.arguments.args;
        let mut facts = recv.facts;
        facts.cached = false;
        let rows = |ty: Ty, bounded: Option<u64>| {
            let mut t = TypeInfo::certain(ty).with_confidence(conf);
            t.facts.from_spark = true;
            t.facts.bounded = bounded;
            t
        };
        match m {
            "collect" | "toLocalIterator" => return rows(Ty::List(Elem::Row), recv.facts.bounded),
            "take" | "tail" => {
                let n = args.first().and_then(int_literal).map(|n| n.max(0) as u64);
                return rows(Ty::List(Elem::Row), n.or(recv.facts.bounded));
            }
            "head" => {
                return match args.first().or_else(|| call_arg(c, 0, "n")) {
                    None => rows(Ty::Row, Some(1)),
                    Some(a) => rows(
                        Ty::List(Elem::Row),
                        int_literal(a)
                            .map(|n| n.max(0) as u64)
                            .or(recv.facts.bounded),
                    ),
                };
            }
            "first" => return rows(Ty::Row, Some(1)),
            "toPandas" => return rows(Ty::PandasFrame, recv.facts.bounded),
            "transform" => {
                if let Some(Expr::Name(n)) = args.first()
                    && let [b] = self.reaching(n)
                    && let Some(s) = self.binding(*b).def_scope
                    && let Some(t) = self.local_function_return(s)
                    && t.is_dataframe()
                {
                    return t;
                }
                return TypeInfo::certain(Ty::DataFrame)
                    .with_confidence(conf)
                    .with_facts(Facts {
                        streaming: facts.streaming,
                        ..Facts::default()
                    });
            }
            _ => {}
        }
        let Some(mi) = api::api().method_of(Ty::DataFrame, m) else {
            return TypeInfo::UNKNOWN;
        };
        if mi.returns != Ty::DataFrame {
            let mut t = TypeInfo::certain(mi.returns).with_confidence(conf);
            if matches!(
                mi.returns,
                Ty::GroupedData
                    | Ty::DataFrameWriter
                    | Ty::DataFrameWriterV2
                    | Ty::DataStreamWriter
            ) {
                t.facts.bounded = recv.facts.bounded;
                t.facts.streaming = recv.facts.streaming;
            }
            if let Ty::List(Elem::DataFrame) = mi.returns {
                t.facts.bounded = recv.facts.bounded;
            }
            return t;
        }
        match m {
            "limit" => {
                let n = args
                    .first()
                    .or_else(|| call_arg(c, 0, "num"))
                    .map(|a| int_literal(a).map_or(u64::MAX, |n| n.max(0) as u64))
                    .unwrap_or(u64::MAX);
                facts.bounded = Some(facts.bounded.map_or(n, |b| b.min(n)));
            }
            "agg" => facts.bounded = Some(1),
            "select" if !args.is_empty() && args.iter().all(|a| self.is_aggregate_expr(a)) => {
                facts.bounded = Some(1);
            }
            "describe" | "summary" => facts.bounded = Some(facts.bounded.map_or(10, |b| b.min(10))),
            "filter"
            | "where"
            | "distinct"
            | "dropDuplicates"
            | "drop_duplicates"
            | "sample"
            | "sampleBy"
            | "dropna"
            | "intersect"
            | "intersectAll"
            | "subtract"
            | "exceptAll"
            | "dropDuplicatesWithinWatermark" => facts.reduced = true,
            "cache" | "persist" | "checkpoint" | "localCheckpoint" => facts.cached = true,
            "join" | "crossJoin" | "union" | "unionAll" | "unionByName" | "lateralJoin" => {
                let other = args.first().map(|a| self.ty_of(a));
                facts.bounded = match (facts.bounded, other.and_then(|o| o.facts.bounded)) {
                    (Some(a), Some(b)) if m.starts_with("union") => Some(a.saturating_add(b)),
                    (Some(a), Some(b)) => Some(a.saturating_mul(b)),
                    _ => None,
                };
                facts.reduced = false;
            }
            _ => {}
        }
        TypeInfo::certain(Ty::DataFrame)
            .with_confidence(conf)
            .with_facts(facts)
    }

    /// Whether `e` is an instance of a class imported from a non-Spark library, or a
    /// method chain on one (`dg.DataGenerator(..).withIdOutput()`), or a name bound only
    /// to such values. The class is recognised by its import and its CapWords name; a
    /// lower-case function of the same library (`lib.load(spark)`) may well return a
    /// DataFrame and is not one.
    fn foreign_object_chain(&self, e: &Expr) -> bool {
        self.foreign_chain(e, &mut Vec::new()) == Some(true)
    }

    /// `Some(true)`: rooted at a foreign class instance, `Some(false)`: rooted at
    /// something else, `None`: a binding cycle (`x = x.withColumn(..)` in a loop).
    fn foreign_chain(&self, e: &Expr, seen: &mut Vec<u32>) -> Option<bool> {
        let mut e = e;
        loop {
            match e {
                Expr::Call(c) => {
                    if let Some(q) = self.qualified_name(&c.func) {
                        return Some(is_foreign_class(&q));
                    }
                    // Builder-style methods return the builder; what any other method
                    // (`.build()`, `.load()`) returns is unknown and may be a DataFrame.
                    match &*c.func {
                        Expr::Attribute(a) if is_fluent_method(a.attr.as_str()) => e = &a.value,
                        _ => return Some(false),
                    }
                }
                Expr::Name(n) => {
                    let reaching = self.reaching(n);
                    if reaching.is_empty() || seen.len() > 32 {
                        return Some(false);
                    }
                    let mut any = None;
                    for b in reaching {
                        if seen.contains(b) {
                            continue;
                        }
                        seen.push(*b);
                        let binding = self.binding(*b);
                        if !matches!(binding.kind, BindingKind::Assign | BindingKind::AnnAssign) {
                            return Some(false);
                        }
                        match binding.value.map(|v| self.foreign_chain(v, seen)) {
                            Some(Some(true)) => any = Some(true),
                            Some(None) => {}
                            _ => return Some(false),
                        }
                    }
                    return any;
                }
                _ => return Some(false),
            }
        }
    }

    /// `F.max("x")`, `F.count("*").alias("n")`, `F.sum(c).cast("long")`: a global
    /// aggregate (one row when every select item is one).
    pub fn is_aggregate_expr(&self, e: &Expr) -> bool {
        const AGGS: &[&str] = &[
            "count",
            "countDistinct",
            "count_distinct",
            "approx_count_distinct",
            "sum",
            "sum_distinct",
            "avg",
            "mean",
            "min",
            "max",
            "first",
            "last",
            "any_value",
            "max_by",
            "min_by",
            "collect_list",
            "collect_set",
            "stddev",
            "stddev_samp",
            "stddev_pop",
            "variance",
            "var_samp",
            "var_pop",
            "percentile_approx",
            "percentile",
            "median",
            "mode",
            "bool_and",
            "bool_or",
            "every",
            "some",
            "count_if",
            "corr",
            "covar_pop",
            "covar_samp",
            "skewness",
            "kurtosis",
            "product",
        ];
        let mut e = e;
        loop {
            if let Expr::BinOp(b) = e {
                let lit = |x: &Expr| x.is_number_literal_expr() || x.is_boolean_literal_expr();
                let (l, r) = (
                    self.is_aggregate_expr(&b.left),
                    self.is_aggregate_expr(&b.right),
                );
                return (l || r) && (l || lit(&b.left)) && (r || lit(&b.right));
            }
            if let Expr::UnaryOp(u) = e {
                e = &u.operand;
                continue;
            }
            let Expr::Call(c) = e else { return false };
            if let Expr::Attribute(a) = &*c.func
                && matches!(a.attr.as_str(), "alias" | "cast" | "name" | "astype")
                && self.ty_of(&a.value).is_column()
            {
                e = &a.value;
                continue;
            }
            return AGGS.iter().any(|f| self.is_spark_function(&c.func, f));
        }
    }

    /// Element type when iterating over `iter` (`for x in iter`).
    pub fn iter_elem_type(&self, iter: &Expr) -> TypeInfo {
        let t = self.ty_of(iter);
        match t.ty {
            Ty::List(e) => {
                if e == Elem::Unknown {
                    return TypeInfo::UNKNOWN;
                }
                let mut out = TypeInfo::certain(Ty::from_elem(e)).with_confidence(t.confidence);
                out.facts.from_spark = t.facts.from_spark;
                out
            }
            Ty::PandasFrame => TypeInfo::certain(Ty::Str),
            // Rows of an ndarray, chunks of `np.array_split(pdf, n)`, `iterrows()` pairs.
            Ty::NonSpark => non_spark(t),
            Ty::Str => TypeInfo::certain(Ty::Str),
            Ty::Dict | Ty::Set => TypeInfo::UNKNOWN,
            _ => TypeInfo::UNKNOWN,
        }
    }

    /// The confidence-aware check rules use for "is a DataFrame".
    pub fn is_dataframe(&self, expr: &Expr) -> bool {
        self.ty_of(expr).is_dataframe()
    }

    pub fn is_column(&self, expr: &Expr) -> bool {
        self.ty_of(expr).is_column()
    }

    /// Strongest confidence that `expr` is a DataFrame, if it is one.
    pub fn dataframe_confidence(&self, expr: &Expr) -> Option<Confidence> {
        let t = self.ty_of(expr);
        t.is_dataframe().then_some(t.confidence)
    }

    /// Range-based helper for rules: the source range of an expression.
    pub fn range_of(expr: &Expr) -> ruff_text_size::TextRange {
        expr.range()
    }
}

/// The local data value derived from `from` (a pandas / numpy / pandas-on-Spark
/// value): [`Ty::NonSpark`] with the confidence of its origin and no facts.
fn non_spark(from: TypeInfo) -> TypeInfo {
    TypeInfo::certain(Ty::NonSpark).with_confidence(from.confidence)
}

/// A class of a library outside the Spark universe, by its qualified import name:
/// `dbldatagen.DataGenerator`, `tempo.TSDF`. Classes follow the CapWords convention;
/// `pyspark`, `delta`, `dlt` and Databricks Connect classes are Spark's own.
fn is_foreign_class(q: &str) -> bool {
    const SPARK_ROOTS: &[&str] = &["pyspark", "delta", "dlt", "databricks.connect", "builtins"];
    if SPARK_ROOTS
        .iter()
        .any(|r| q == *r || q.strip_prefix(r).is_some_and(|rest| rest.starts_with('.')))
    {
        return false;
    }
    let Some((_, class)) = q.rsplit_once('.') else {
        return false;
    };
    class.starts_with(|c: char| c.is_ascii_uppercase())
}

/// A method that, on a builder object, conventionally returns the builder itself:
/// `withX(..)`, `option(..)`, and the DataFrame-only names a builder may mirror.
fn is_fluent_method(m: &str) -> bool {
    m.starts_with("with")
        || matches!(m, "option" | "options")
        || DATAFRAME_ONLY_METHODS.contains(&m)
}

/// WindowSpec fact transitions for `Window.<m>(…)` / `spec.<m>(…)`.
fn window_facts(mut facts: Facts, m: &str, c: &ast::ExprCall) -> Facts {
    match m {
        "partitionBy" => {
            if !c.arguments.args.is_empty() || !c.arguments.keywords.is_empty() {
                let only_empty_list = c.arguments.args.len() == 1
                    && matches!(&c.arguments.args[0], Expr::List(l) if l.elts.is_empty());
                if !only_empty_list {
                    facts.win_partitioned = true;
                }
            }
        }
        "orderBy" => facts.win_ordered = true,
        "rowsBetween" | "rangeBetween" => {
            facts.win_framed = true;
            let start = ast_arg(c, 0, "start");
            let end = ast_arg(c, 1, "end");
            facts.win_frame_growing = start
                .is_some_and(|s| frame_bound(s) == Some(Bound::Unbounded))
                && end.is_some_and(|e| frame_bound(e) == Some(Bound::Finite));
        }
        _ => {}
    }
    facts
}

fn ast_arg<'a>(c: &'a ast::ExprCall, pos: usize, kw: &str) -> Option<&'a Expr> {
    c.arguments.args.get(pos).or_else(|| {
        c.arguments
            .keywords
            .iter()
            .find(|k| k.arg.as_ref().is_some_and(|a| a.as_str() == kw))
            .map(|k| &k.value)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bound {
    Unbounded,
    Finite,
}

/// A window frame bound: `Window.unboundedPreceding/Following`, `±sys.maxsize`
/// (pyspark maps |n| ≥ sys.maxsize to unbounded) and huge int literals are unbounded;
/// `Window.currentRow` and other int literals are finite. Anything else is unknown.
fn frame_bound(e: &Expr) -> Option<Bound> {
    const HUGE: u64 = 9_223_372_036_854_775_807; // sys.maxsize on 64-bit
    match e {
        Expr::Attribute(a) => match a.attr.as_str() {
            "unboundedPreceding" | "unboundedFollowing" | "maxsize" => Some(Bound::Unbounded),
            "currentRow" => Some(Bound::Finite),
            _ => None,
        },
        Expr::NumberLiteral(n) => match &n.value {
            Number::Int(i) => Some(match i.as_u64() {
                Some(v) if v < HUGE => Bound::Finite,
                _ => Bound::Unbounded,
            }),
            _ => None,
        },
        Expr::UnaryOp(u) if matches!(u.op, UnaryOp::USub | UnaryOp::UAdd) => {
            frame_bound(&u.operand)
        }
        // `-sys.maxsize - 1`
        Expr::BinOp(b) if matches!(b.op, Operator::Sub | Operator::Add) => {
            match (frame_bound(&b.left), frame_bound(&b.right)) {
                (Some(Bound::Unbounded), Some(Bound::Finite)) => Some(Bound::Unbounded),
                (Some(Bound::Finite), Some(Bound::Finite)) => Some(Bound::Finite),
                _ => None,
            }
        }
        _ => None,
    }
}
