//! Builds a [`SemanticModel`] from a parsed module: one forward walk per scope,
//! function bodies deferred to the end of their enclosing scope (Python call-time
//! semantics), loops visited twice to propagate loop-carried bindings, if/try
//! branches merged as reaching-definition sets. The whole walk runs twice so that
//! function summaries from the first pass inform calls in the second.

use std::collections::VecDeque;

use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_ast::{self as ast, Expr, ExprContext, Stmt};
use ruff_text_size::{Ranged, TextRange, TextSize};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::api::{self, canonical_qualified};
use crate::evidence::{self, Evidence};
use crate::model::*;
use crate::types::{Elem, Ty, TypeInfo};

type Env = FxHashMap<String, Vec<BindingId>>;

/// Build the semantic model of a module.
pub fn build_model<'a>(
    module: &'a ast::ModModule,
    source_len: TextSize,
    opts: SemanticOptions,
    external: &'a dyn SummaryProvider,
) -> SemanticModel<'a> {
    // Pass 1: compute summaries.
    let mut pass1 = Walker::run(module, source_len, opts.clone(), external, Prior::default());
    // Usage evidence needs every use of a binding, so it is read off pass 1. A value
    // with a pandas-only use must not be guessed a DataFrame in the summaries either:
    // when there is one, pass 1 runs again knowing it.
    let pandas_used = pandas_used_bindings(&pass1);
    if !pandas_used.is_empty() {
        let prior = Prior {
            pandas_used: pandas_used.clone(),
            ..Prior::default()
        };
        pass1 = Walker::run(module, source_len, opts.clone(), external, prior);
    }
    let mut prior = Prior::default();
    for f in &pass1.functions {
        let t = f.return_type();
        if !t.is_unknown() {
            prior.func_returns.insert(f.scope, t);
        }
    }
    prior.class_attrs = pass1.class_attrs.clone();
    prior.arg_evidence = pass1.arg_evidence.clone();
    prior.pandas_used = pandas_used;
    drop(pass1);
    // Pass 2: the final model.
    Walker::run(module, source_len, opts, external, prior)
}

struct Walker<'a> {
    m: SemanticModel<'a>,
    envs: Vec<Env>,
    globals: Vec<FxHashSet<String>>,
    scope: ScopeId,
    deferred: VecDeque<(&'a ast::StmtFunctionDef, ScopeId)>,
    deferred_seen: FxHashSet<TextSize>,
    scope_by_range: FxHashMap<(TextSize, TextSize, u8), ScopeId>,
    binding_by_key: FxHashMap<(TextSize, u8), BindingId>,
    /// Every binding of a name in a scope, regardless of reachability. Used for
    /// closures: a nested function body sees whichever enclosing binding is live at
    /// call time, even if the enclosing scope's fall-through path lost it (e.g. an
    /// import inside an `except` handler that ends in `return`).
    all_bindings: FxHashMap<(ScopeId, String), Vec<BindingId>>,
    loop_by_range: FxHashSet<TextRange>,
    branch_by_range: FxHashSet<TextRange>,
    loop_nesting: u32,
    /// Pending attribute context for the next Name visited: (attr, called).
    pending_attr: Option<(String, bool)>,
    /// Function summary index for the function body currently being walked.
    current_function: Option<usize>,
    module_name: Option<String>,
}

fn kind_tag(k: BindingKind) -> u8 {
    k as u8
}

impl<'a> Walker<'a> {
    fn run(
        module: &'a ast::ModModule,
        source_len: TextSize,
        opts: SemanticOptions,
        external: &'a dyn SummaryProvider,
        prior: Prior,
    ) -> SemanticModel<'a> {
        let module_name = opts.module_name.clone();
        let mut m = SemanticModel::empty(opts, external);
        m.prior = prior;
        m.scopes.push(Scope {
            kind: ScopeKind::Module,
            parent: None,
            range: TextRange::new(TextSize::from(0), source_len),
            name: String::new(),
        });
        let mut w = Walker {
            m,
            envs: vec![Env::default()],
            globals: vec![FxHashSet::default()],
            scope: MODULE_SCOPE,
            deferred: VecDeque::new(),
            deferred_seen: FxHashSet::default(),
            scope_by_range: FxHashMap::default(),
            binding_by_key: FxHashMap::default(),
            all_bindings: FxHashMap::default(),
            loop_by_range: FxHashSet::default(),
            branch_by_range: FxHashSet::default(),
            loop_nesting: 0,
            pending_attr: None,
            current_function: None,
            module_name,
        };
        w.visit_body(&module.body);
        while let Some((def, scope)) = w.deferred.pop_front() {
            w.visit_function_body(def, scope);
        }
        w.finish()
    }

    fn finish(mut self) -> SemanticModel<'a> {
        let mut per_binding: Vec<Vec<TextSize>> = vec![Vec::new(); self.m.bindings.len()];
        let mut offsets: Vec<&TextSize> = self.m.uses.keys().collect();
        offsets.sort();
        for off in offsets {
            if let Some(u) = self.m.uses.get(off) {
                for b in &u.reaching {
                    if let Some(v) = per_binding.get_mut(*b as usize) {
                        v.push(*off);
                    }
                }
            }
        }
        self.m.binding_uses = per_binding;
        self.m
    }

    // ---------------------------------------------------------------- scopes

    fn new_scope(&mut self, kind: ScopeKind, range: TextRange, name: &str) -> ScopeId {
        let key = (range.start(), range.end(), kind as u8);
        if let Some(id) = self.scope_by_range.get(&key) {
            self.envs[*id as usize].clear();
            return *id;
        }
        let id = self.m.scopes.len() as ScopeId;
        self.m.scopes.push(Scope {
            kind,
            parent: Some(self.scope),
            range,
            name: name.to_string(),
        });
        self.envs.push(Env::default());
        self.globals.push(FxHashSet::default());
        self.scope_by_range.insert(key, id);
        id
    }

    fn lookup(&self, name: &str) -> Vec<BindingId> {
        let mut s = Some(self.scope);
        let mut first = true;
        while let Some(id) = s {
            let sc = &self.m.scopes[id as usize];
            if first || sc.kind != ScopeKind::Class {
                if self.globals[id as usize].contains(name) {
                    return self.envs[0].get(name).cloned().unwrap_or_default();
                }
                if let Some(b) = self.envs[id as usize].get(name) {
                    return b.clone();
                }
                if !first && let Some(all) = self.all_bindings.get(&(id, name.to_string())) {
                    return all.clone();
                }
            }
            first = false;
            s = sc.parent;
        }
        Vec::new()
    }

    fn type_of_reaching(&self, reaching: &[BindingId], name: &str) -> TypeInfo {
        if reaching.is_empty() {
            let t = self.m.unbound_type(name);
            return if t.is_unknown() {
                self.m.name_heuristic(name, reaching)
            } else {
                t
            };
        }
        let t = reaching
            .iter()
            .map(|b| self.m.bindings[*b as usize].ty)
            .reduce(TypeInfo::join)
            .unwrap_or(TypeInfo::UNKNOWN);
        if t.is_unknown() {
            self.m.name_heuristic(name, reaching)
        } else {
            t
        }
    }

    fn target_scope(&self, name: &str) -> ScopeId {
        if self.globals[self.scope as usize].contains(name) {
            MODULE_SCOPE
        } else {
            self.scope
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_binding(
        &mut self,
        name: &str,
        kind: BindingKind,
        range: TextRange,
        stmt_range: TextRange,
        value: Option<&'a Expr>,
        qualified: Option<String>,
        ty: TypeInfo,
        def_scope: Option<ScopeId>,
    ) -> BindingId {
        let scope = self.target_scope(name);
        let key = (range.start(), kind_tag(kind));
        let id = if let Some(id) = self.binding_by_key.get(&key).copied() {
            let b = &mut self.m.bindings[id as usize];
            b.ty = ty;
            b.qualified = qualified;
            b.value = value;
            id
        } else {
            let id = self.m.bindings.len() as BindingId;
            self.m.bindings.push(Binding {
                name: name.to_string(),
                scope,
                kind,
                range,
                stmt_range,
                value,
                qualified,
                ty,
                def_scope,
            });
            self.binding_by_key.insert(key, id);
            self.all_bindings
                .entry((scope, name.to_string()))
                .or_default()
                .push(id);
            id
        };
        self.envs[scope as usize].insert(name.to_string(), vec![id]);
        id
    }

    // ---------------------------------------------------------------- env merging

    fn snapshot(&self) -> Env {
        self.envs[self.scope as usize].clone()
    }

    fn restore(&mut self, env: Env) {
        self.envs[self.scope as usize] = env;
    }

    fn merge(a: &Env, b: &Env) -> Env {
        let mut out = a.clone();
        for (k, v) in b {
            let e = out.entry(k.clone()).or_default();
            for id in v {
                if !e.contains(id) {
                    e.push(*id);
                }
            }
            e.sort_unstable();
        }
        out
    }

    fn terminates(body: &[Stmt]) -> bool {
        matches!(
            body.last(),
            Some(Stmt::Return(_) | Stmt::Raise(_) | Stmt::Continue(_) | Stmt::Break(_))
        )
    }

    /// Visit alternative branches starting from the same env and merge the results.
    fn visit_branches(&mut self, branches: &[&'a [Stmt]], has_else: bool) {
        let before = self.snapshot();
        let mut merged: Option<Env> = if has_else { None } else { Some(before.clone()) };
        for body in branches {
            self.restore(before.clone());
            self.visit_body(body);
            if Self::terminates(body) {
                continue;
            }
            let env = self.snapshot();
            merged = Some(match merged {
                None => env,
                Some(m) => Self::merge(&m, &env),
            });
        }
        self.restore(merged.unwrap_or(before));
    }

    // ---------------------------------------------------------------- loops

    fn push_loop(
        &mut self,
        kind: LoopKind,
        range: TextRange,
        body: TextRange,
        iter: Option<&Expr>,
    ) {
        if !self.loop_by_range.insert(range) {
            return;
        }
        let func_scope = self.func_scope_of(self.scope);
        let literal_len = iter.and_then(|i| match i {
            Expr::List(l) => Some(l.elts.len()),
            Expr::Tuple(t) => Some(t.elts.len()),
            Expr::Set(s) => Some(s.elts.len()),
            _ => None,
        });
        self.m.loops.push(LoopInfo {
            kind,
            range,
            body,
            iter: iter.map(Ranged::range),
            func_scope,
            literal_len,
        });
    }

    fn push_branch(&mut self, range: TextRange, arms: &[&'a [Stmt]]) {
        if !self.branch_by_range.insert(range) {
            return;
        }
        let arms = arms
            .iter()
            .filter_map(|b| match (b.first(), b.last()) {
                (Some(f), Some(l)) => Some(TextRange::new(f.start(), l.end())),
                _ => None,
            })
            .collect();
        self.m.branches.push(BranchInfo { range, arms });
    }

    fn func_scope_of(&self, mut s: ScopeId) -> ScopeId {
        loop {
            let sc = &self.m.scopes[s as usize];
            match sc.kind {
                ScopeKind::Module | ScopeKind::Function | ScopeKind::Lambda => return s,
                _ => match sc.parent {
                    Some(p) => s = p,
                    None => return MODULE_SCOPE,
                },
            }
        }
    }

    fn visit_loop_body(
        &mut self,
        test: Option<&'a Expr>,
        body: &'a [Stmt],
        rebind: impl Fn(&mut Self),
    ) {
        let env0 = self.snapshot();
        if let Some(t) = test {
            self.visit_expr(t);
        }
        rebind(self);
        self.visit_body(body);
        let mut env_end = self.snapshot();
        if self.loop_nesting < 3 {
            // Second pass with loop-carried bindings.
            self.restore(Self::merge(&env0, &env_end));
            self.loop_nesting += 1;
            if let Some(t) = test {
                self.visit_expr(t);
            }
            rebind(self);
            self.visit_body(body);
            self.loop_nesting -= 1;
            env_end = self.snapshot();
        }
        self.restore(Self::merge(&env0, &env_end));
    }

    // ---------------------------------------------------------------- targets

    fn bind_target(
        &mut self,
        target: &'a Expr,
        value: Option<&'a Expr>,
        ti: TypeInfo,
        kind: BindingKind,
        stmt_range: TextRange,
    ) {
        match target {
            Expr::Name(n) => {
                let qualified = value.and_then(|v| match v {
                    Expr::Name(_) | Expr::Attribute(_) => self.m.qualified_name(v),
                    _ => None,
                });
                let def_scope = value.and_then(|v| match v {
                    Expr::Name(vn) => match self.m.reaching(vn) {
                        [b] => self.m.bindings[*b as usize].def_scope,
                        _ => None,
                    },
                    _ => None,
                });
                let kind = if def_scope.is_some() && kind == BindingKind::Assign {
                    // alias of a function/class keeps call semantics
                    value
                        .and_then(|v| v.as_name_expr())
                        .and_then(|vn| self.m.single_binding(vn))
                        .map(|b| self.m.bindings[b as usize].kind)
                        .unwrap_or(kind)
                } else {
                    kind
                };
                self.add_binding(
                    n.id.as_str(),
                    kind,
                    n.range,
                    stmt_range,
                    value,
                    qualified,
                    ti,
                    def_scope,
                );
            }
            Expr::Tuple(ast::ExprTuple { elts, .. }) | Expr::List(ast::ExprList { elts, .. }) => {
                let literal = value.and_then(|v| match v {
                    Expr::Tuple(t) if t.elts.len() == elts.len() => Some(&t.elts),
                    Expr::List(l) if l.elts.len() == elts.len() => Some(&l.elts),
                    _ => None,
                });
                for (i, elt) in elts.iter().enumerate() {
                    let (v, t) = match literal {
                        Some(vals) => (Some(&vals[i]), self.m.ty_of(&vals[i])),
                        None => match ti.ty {
                            Ty::List(e) if e != Elem::Unknown => {
                                let mut t = TypeInfo::certain(Ty::from_elem(e))
                                    .with_confidence(ti.confidence);
                                t.facts.bounded = ti.facts.bounded;
                                (None, t)
                            }
                            _ => (None, TypeInfo::UNKNOWN),
                        },
                    };
                    self.bind_target(elt, v, t, kind, stmt_range);
                }
            }
            Expr::Starred(s) => self.bind_target(
                &s.value,
                None,
                TypeInfo::certain(Ty::List(Elem::Unknown)),
                kind,
                stmt_range,
            ),
            Expr::Attribute(a) => {
                // self.x = value inside a method
                self.visit_expr(&a.value);
                if let Expr::Name(base) = &*a.value
                    && let [b] = self.m.reaching(base)
                    && let Ty::Instance(cls) = self.m.bindings[*b as usize].ty.ty
                {
                    let key = (cls, a.attr.as_str().to_string());
                    let e = self.m.class_attrs.entry(key).or_insert(ti);
                    *e = e.join(ti);
                }
            }
            Expr::Subscript(s) => {
                self.visit_expr(&s.value);
                self.visit_expr(&s.slice);
            }
            _ => self.visit_expr(target),
        }
    }

    /// Bind a `for` / comprehension target with element typing (enumerate, zip).
    fn bind_loop_target(
        &mut self,
        target: &'a Expr,
        iter: &'a Expr,
        kind: BindingKind,
        stmt_range: TextRange,
    ) {
        if let (Expr::Tuple(t), Expr::Call(c)) = (target, iter) {
            let q = self.m.qualified_name(&c.func);
            if q.as_deref() == Some("builtins.enumerate")
                && t.elts.len() == 2
                && let Some(inner) = c.arguments.args.first()
            {
                let elem = self.m.iter_elem_type(inner);
                self.bind_target(
                    &t.elts[0],
                    None,
                    TypeInfo::certain(Ty::Int),
                    kind,
                    stmt_range,
                );
                self.bind_target(&t.elts[1], None, elem, kind, stmt_range);
                return;
            }
            if q.as_deref() == Some("builtins.zip") && t.elts.len() == c.arguments.args.len() {
                for (e, a) in t.elts.iter().zip(c.arguments.args.iter()) {
                    let elem = self.m.iter_elem_type(a);
                    self.bind_target(e, None, elem, kind, stmt_range);
                }
                return;
            }
        }
        let elem = self.m.iter_elem_type(iter);
        self.bind_target(target, None, elem, kind, stmt_range);
    }

    // ---------------------------------------------------------------- annotations

    fn qualified_of_annotation_name(&self, e: &Expr) -> Option<String> {
        match e {
            Expr::Name(n) => {
                let reaching = self.lookup(n.id.as_str());
                if reaching.is_empty() {
                    let s = n.id.as_str();
                    return if crate::infer::BUILTINS.contains(&s) || s == "None" {
                        Some(format!("builtins.{s}"))
                    } else {
                        None
                    };
                }
                let mut q: Option<String> = None;
                for b in reaching {
                    let bq = self.m.bindings[b as usize].qualified.clone()?;
                    if q.as_ref().is_some_and(|p| *p != bq) {
                        return None;
                    }
                    q = Some(bq);
                }
                q
            }
            Expr::Attribute(a) => {
                let base = self.qualified_of_annotation_name(&a.value)?;
                Some(canonical_qualified(&format!("{base}.{}", a.attr.as_str())))
            }
            _ => None,
        }
    }

    fn resolve_annotation(&self, e: &Expr) -> Option<TypeInfo> {
        match e {
            Expr::StringLiteral(s) => {
                let parsed = ruff_python_parser::parse_expression(s.value.to_str()).ok()?;
                self.resolve_annotation(parsed.expr())
            }
            Expr::NoneLiteral(_) => Some(TypeInfo::certain(Ty::NoneTy)),
            Expr::Name(_) | Expr::Attribute(_) => {
                if let Some(q) = self.qualified_of_annotation_name(e) {
                    if q.starts_with("py4j.") {
                        // JVM handles (`self._jdf: JavaObject`) are never Spark values.
                        return Some(TypeInfo::certain(Ty::Object));
                    }
                    return api::annotation_type(&q).map(TypeInfo::certain);
                }
                // Unresolved bare names (forward refs without import).
                let last = match e {
                    Expr::Name(n) => n.id.as_str(),
                    Expr::Attribute(a) => a.attr.as_str(),
                    _ => return None,
                };
                let t = match last {
                    "DataFrame" if matches!(e, Expr::Name(_)) => Ty::DataFrame,
                    "SparkSession" => Ty::SparkSession,
                    "Column" => Ty::Column,
                    "WindowSpec" => Ty::WindowSpec,
                    "GroupedData" => Ty::GroupedData,
                    "JavaObject" | "JavaMember" | "JavaClass" | "JVMView" | "JavaGateway" => {
                        return Some(TypeInfo::certain(Ty::Object));
                    }
                    _ => return None,
                };
                Some(TypeInfo::likely(t))
            }
            Expr::Subscript(s) => {
                let base = self
                    .qualified_of_annotation_name(&s.value)
                    .unwrap_or_else(|| match &*s.value {
                        Expr::Name(n) => format!("typing.{}", n.id.as_str()),
                        _ => String::new(),
                    });
                let last = base.rsplit('.').next().unwrap_or("");
                match last {
                    "Optional" | "Annotated" | "Final" | "ClassVar" => {
                        let inner = match &*s.slice {
                            Expr::Tuple(t) => t.elts.first()?,
                            other => other,
                        };
                        self.resolve_annotation(inner)
                    }
                    "Union" => {
                        let Expr::Tuple(t) = &*s.slice else {
                            return self.resolve_annotation(&s.slice);
                        };
                        let tys: Vec<TypeInfo> = t
                            .elts
                            .iter()
                            .filter_map(|x| self.resolve_annotation(x))
                            .filter(|t| t.ty != Ty::NoneTy)
                            .collect();
                        match tys.as_slice() {
                            [one] => Some(*one),
                            _ => None,
                        }
                    }
                    "list" | "List" | "Sequence" | "Iterable" | "Iterator" | "Generator"
                    | "Collection" | "MutableSequence" => {
                        let inner = match &*s.slice {
                            Expr::Tuple(t) => t.elts.first(),
                            other => Some(other),
                        };
                        let elem = inner
                            .and_then(|i| self.resolve_annotation(i))
                            .map(|t| t.ty.elem())
                            .unwrap_or(Elem::Unknown);
                        Some(TypeInfo::certain(Ty::List(elem)))
                    }
                    "dict" | "Dict" | "Mapping" | "MutableMapping" => {
                        Some(TypeInfo::certain(Ty::Dict))
                    }
                    "tuple" | "Tuple" => Some(TypeInfo::certain(Ty::Tuple)),
                    "set" | "Set" | "FrozenSet" | "frozenset" => Some(TypeInfo::certain(Ty::Set)),
                    _ => None,
                }
            }
            Expr::BinOp(b) if b.op == ast::Operator::BitOr => {
                let l = self.resolve_annotation(&b.left);
                let r = self.resolve_annotation(&b.right);
                match (l, r) {
                    (Some(l), Some(r)) if l.ty == Ty::NoneTy => Some(r),
                    (Some(l), Some(r)) if r.ty == Ty::NoneTy => Some(l),
                    (Some(l), Some(r)) if l.ty == r.ty => Some(l),
                    (Some(l), None) if l.ty != Ty::NoneTy => None,
                    _ => None,
                }
            }
            _ => None,
        }
    }

    // ---------------------------------------------------------------- functions

    fn visit_function_def(&mut self, f: &'a ast::StmtFunctionDef, stmt: &'a Stmt) {
        let mut is_udf = false;
        let mut is_static = false;
        let mut is_classmethod = false;
        for d in &f.decorator_list {
            self.visit_expr(&d.expression);
            let callee = match &d.expression {
                Expr::Call(c) => &*c.func,
                other => other,
            };
            match self.m.qualified_name(callee).as_deref() {
                Some("pyspark.sql.functions.udf" | "pyspark.sql.functions.pandas_udf") => {
                    is_udf = true
                }
                Some("builtins.staticmethod") => is_static = true,
                Some("builtins.classmethod") => is_classmethod = true,
                _ => {}
            }
            if let Expr::Name(n) = callee {
                match n.id.as_str() {
                    "staticmethod" => is_static = true,
                    "classmethod" => is_classmethod = true,
                    _ => {}
                }
            }
        }
        // Defaults and annotations are evaluated in the enclosing scope.
        let params = &f.parameters;
        for p in params.iter_non_variadic_params() {
            if let Some(d) = &p.default {
                self.visit_expr(d);
            }
            if let Some(a) = &p.parameter.annotation {
                self.visit_annotation_expr(a);
            }
        }
        for p in [&params.vararg, &params.kwarg].into_iter().flatten() {
            if let Some(a) = &p.annotation {
                self.visit_annotation_expr(a);
            }
        }
        if let Some(r) = &f.returns {
            self.visit_annotation_expr(r);
        }
        let body_range = match (f.body.first(), f.body.last()) {
            (Some(first), Some(last)) => TextRange::new(first.start(), last.end()),
            _ => f.range,
        };
        let parent_kind = self.m.scopes[self.scope as usize].kind;
        let class_scope = (parent_kind == ScopeKind::Class).then_some(self.scope);
        let path = match parent_kind {
            ScopeKind::Module => Some(f.name.as_str().to_string()),
            ScopeKind::Class => {
                let cls = &self.m.scopes[self.scope as usize];
                if cls.parent == Some(MODULE_SCOPE) {
                    Some(format!("{}.{}", cls.name, f.name.as_str()))
                } else {
                    None
                }
            }
            _ => None,
        };
        let fscope = self.new_scope(ScopeKind::Function, body_range, f.name.as_str());
        if let Some(cls) = class_scope {
            self.m
                .class_methods
                .insert((cls, f.name.as_str().to_string()), fscope);
        }
        let annotated_return = f.returns.as_ref().and_then(|r| self.resolve_annotation(r));
        let idx = if let Some(i) = self.m.func_by_scope.get(&fscope) {
            *i
        } else {
            let i = self.m.functions.len();
            self.m.functions.push(FunctionSummary {
                name: f.name.as_str().to_string(),
                path,
                scope: fscope,
                params: Vec::new(),
                returns: TypeInfo::UNKNOWN,
                has_return_value: false,
                annotated_return,
                is_udf,
                range: f.range,
            });
            self.m.func_by_scope.insert(fscope, i);
            i
        };
        let _ = idx;
        let ty = if is_udf {
            TypeInfo::certain(Ty::Udf)
        } else {
            TypeInfo::certain(Ty::Function)
        };
        self.add_binding(
            f.name.as_str(),
            BindingKind::FunctionDef,
            f.name.range,
            stmt.range(),
            None,
            None,
            ty,
            Some(fscope),
        );
        if self.deferred_seen.insert(f.range.start()) {
            self.deferred.push_back((f, fscope));
        }
        // stash method kind on the scope name for self typing
        if is_static || is_classmethod {
            self.m.scopes[fscope as usize].name = format!(
                "{}{}",
                if is_static { "@static:" } else { "@class:" },
                f.name.as_str()
            );
        }
    }

    fn visit_annotation_expr(&mut self, e: &'a Expr) {
        // Record uses of names in (non-string) annotations.
        self.visit_expr(e);
    }

    fn visit_function_body(&mut self, f: &'a ast::StmtFunctionDef, fscope: ScopeId) {
        let saved_scope = self.scope;
        let saved_fn = self.current_function;
        let saved_nesting = self.loop_nesting;
        self.scope = fscope;
        self.loop_nesting = 0;
        self.current_function = self.m.func_by_scope.get(&fscope).copied();
        if let Some(i) = self.current_function {
            self.m.functions[i].returns = TypeInfo::UNKNOWN;
            self.m.functions[i].has_return_value = false;
        }
        let parent = self.m.scopes[fscope as usize]
            .parent
            .unwrap_or(MODULE_SCOPE);
        let parent_is_class = self.m.scopes[parent as usize].kind == ScopeKind::Class;
        let is_plain_method =
            parent_is_class && !self.m.scopes[fscope as usize].name.starts_with('@');

        let param_names: Vec<&str> = f
            .parameters
            .iter_non_variadic_params()
            .map(|p| p.parameter.name.as_str())
            .collect();
        let evidence = param_evidence(&f.body, &param_names);
        let arg_evidence = self
            .m
            .prior
            .arg_evidence
            .get(&fscope)
            .cloned()
            .unwrap_or_default();
        let mut summary_params = Vec::new();
        for (i, p) in f.parameters.iter_non_variadic_params().enumerate() {
            let name = p.parameter.name.as_str();
            let mut ti = p
                .parameter
                .annotation
                .as_ref()
                .and_then(|a| self.resolve_annotation(a))
                .unwrap_or(TypeInfo::UNKNOWN);
            if ti.is_unknown() && i == 0 && is_plain_method {
                ti = TypeInfo::certain(Ty::Instance(parent));
            }
            if ti.is_unknown()
                && let Some(d) = &p.default
            {
                let dt = self.m.ty_of(d);
                if dt.ty != Ty::NoneTy {
                    ti = dt;
                }
            }
            if ti.is_unknown()
                && let Some(t) = evidence.get(name)
            {
                ti = TypeInfo::likely(*t);
            }
            if ti.is_unknown() {
                let arg_index = if is_plain_method {
                    i.checked_sub(1)
                } else {
                    Some(i)
                };
                if let Some(t) = arg_index.and_then(|ai| arg_evidence.get(ai))
                    && t.ty.is_spark()
                {
                    ti = TypeInfo::likely(t.ty);
                }
            }
            summary_params.push((name.to_string(), ti));
            self.add_binding(
                name,
                BindingKind::Param,
                p.parameter.name.range,
                p.range,
                None,
                None,
                ti,
                None,
            );
        }
        for p in [&f.parameters.vararg, &f.parameters.kwarg]
            .into_iter()
            .flatten()
        {
            let ti = if f
                .parameters
                .vararg
                .as_ref()
                .is_some_and(|v| std::ptr::eq(&**v, &**p))
            {
                TypeInfo::certain(Ty::Tuple)
            } else {
                TypeInfo::certain(Ty::Dict)
            };
            self.add_binding(
                p.name.as_str(),
                BindingKind::Param,
                p.name.range,
                p.range,
                None,
                None,
                ti,
                None,
            );
        }
        if let Some(i) = self.current_function {
            self.m.functions[i].params = summary_params;
        }
        self.visit_body(&f.body);
        if contains_yield(&f.body)
            && let Some(i) = self.current_function
        {
            self.m.functions[i].returns = TypeInfo::UNKNOWN;
            self.m.functions[i].has_return_value = true;
        }
        self.scope = saved_scope;
        self.current_function = saved_fn;
        self.loop_nesting = saved_nesting;
    }

    // ---------------------------------------------------------------- imports

    fn resolve_relative(&self, level: u32, module: Option<&str>) -> String {
        let mut base: Vec<&str> = self
            .module_name
            .as_deref()
            .map(|m| m.split('.').collect())
            .unwrap_or_default();
        for _ in 0..level {
            base.pop();
        }
        let mut s = base.join(".");
        if let Some(m) = module {
            if !s.is_empty() {
                s.push('.');
            }
            s.push_str(m);
        }
        s
    }

    fn visit_import(&mut self, stmt: &'a Stmt) {
        match stmt {
            Stmt::Import(i) => {
                for alias in &i.names {
                    let full = alias.name.as_str();
                    let (local, q) = match &alias.asname {
                        Some(a) => (a.as_str().to_string(), canonical_qualified(full)),
                        None => {
                            let first = full.split('.').next().unwrap_or(full);
                            (first.to_string(), first.to_string())
                        }
                    };
                    let range = alias.asname.as_ref().map_or(alias.range, |a| a.range);
                    let ty = api::qualified_value_type(&q)
                        .map(TypeInfo::certain)
                        .unwrap_or(TypeInfo::certain(Ty::Object));
                    self.add_binding(
                        &local,
                        BindingKind::Import,
                        range,
                        stmt.range(),
                        None,
                        Some(q),
                        ty,
                        None,
                    );
                }
            }
            Stmt::ImportFrom(i) => {
                let module = if i.level > 0 {
                    self.resolve_relative(i.level, i.module.as_ref().map(|m| m.as_str()))
                } else {
                    i.module
                        .as_ref()
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default()
                };
                for alias in &i.names {
                    let name = alias.name.as_str();
                    if name == "*" {
                        if canonical_qualified(&module) == "pyspark.sql.functions" {
                            self.m.star_functions = true;
                        }
                        continue;
                    }
                    let local = alias.asname.as_ref().map_or(name, |a| a.as_str());
                    let q = canonical_qualified(&format!("{module}.{name}"));
                    let range = alias.asname.as_ref().map_or(alias.range, |a| a.range);
                    let ty = api::qualified_value_type(&q)
                        .map(TypeInfo::certain)
                        .unwrap_or(TypeInfo::certain(Ty::Object));
                    self.add_binding(
                        local,
                        BindingKind::Import,
                        range,
                        stmt.range(),
                        None,
                        Some(q),
                        ty,
                        None,
                    );
                }
            }
            _ => {}
        }
    }

    // ---------------------------------------------------------------- statements

    fn visit_statement(&mut self, stmt: &'a Stmt) {
        match stmt {
            Stmt::FunctionDef(f) => self.visit_function_def(f, stmt),
            Stmt::ClassDef(c) => {
                for d in &c.decorator_list {
                    self.visit_expr(&d.expression);
                }
                if let Some(args) = &c.arguments {
                    for a in &args.args {
                        self.visit_expr(a);
                    }
                    for k in &args.keywords {
                        self.visit_expr(&k.value);
                    }
                }
                let cscope = self.new_scope(ScopeKind::Class, c.range, c.name.as_str());
                self.add_binding(
                    c.name.as_str(),
                    BindingKind::ClassDef,
                    c.name.range,
                    stmt.range(),
                    None,
                    None,
                    TypeInfo::certain(Ty::Object),
                    Some(cscope),
                );
                let saved = self.scope;
                self.scope = cscope;
                self.visit_body(&c.body);
                self.scope = saved;
            }
            Stmt::Return(r) => {
                let t = match &r.value {
                    Some(v) => {
                        self.visit_expr(v);
                        Some(self.m.ty_of(v))
                    }
                    None => None,
                };
                if let Some(i) = self.current_function
                    && self.func_scope_of(self.scope) == self.m.functions[i].scope
                {
                    let f = &mut self.m.functions[i];
                    let t = t.unwrap_or(TypeInfo::certain(Ty::NoneTy));
                    if f.has_return_value {
                        f.returns = f.returns.join(t);
                    } else {
                        f.returns = t;
                        f.has_return_value = true;
                    }
                }
            }
            Stmt::Delete(d) => {
                for t in &d.targets {
                    match t {
                        Expr::Name(n) => {
                            let s = self.target_scope(n.id.as_str());
                            self.envs[s as usize].remove(n.id.as_str());
                        }
                        other => self.visit_expr(other),
                    }
                }
            }
            Stmt::Assign(a) => {
                self.visit_expr(&a.value);
                let ti = self.m.ty_of(&a.value);
                for t in &a.targets {
                    self.bind_target(t, Some(&a.value), ti, BindingKind::Assign, stmt.range());
                }
            }
            Stmt::AugAssign(a) => {
                self.visit_expr(&a.value);
                if let Expr::Name(n) = &*a.target {
                    let reaching = self.lookup(n.id.as_str());
                    let old = self.type_of_reaching(&reaching, n.id.as_str());
                    // The target is also read.
                    self.m.uses.insert(
                        n.range.start(),
                        UseSite {
                            range: n.range,
                            scope: self.scope,
                            reaching,
                            attr: None,
                            called: false,
                        },
                    );
                    let new_t = self.m.ty_of(&a.value);
                    let t = if old.is_column() || new_t.is_column() {
                        TypeInfo::certain(Ty::Column)
                    } else {
                        old
                    };
                    self.add_binding(
                        n.id.as_str(),
                        BindingKind::AugAssign,
                        TextRange::new(n.range.start(), n.range.start()),
                        stmt.range(),
                        None,
                        None,
                        t,
                        None,
                    );
                } else {
                    self.visit_expr(&a.target);
                }
            }
            Stmt::AnnAssign(a) => {
                self.visit_annotation_expr(&a.annotation);
                let ann = self.resolve_annotation(&a.annotation);
                if let Some(v) = &a.value {
                    self.visit_expr(v);
                    let vt = self.m.ty_of(v);
                    let ti = match ann {
                        Some(t) if !t.is_unknown() => {
                            if t.ty == vt.ty {
                                TypeInfo {
                                    facts: vt.facts,
                                    ..t
                                }
                            } else {
                                t
                            }
                        }
                        _ => vt,
                    };
                    self.bind_target(&a.target, Some(v), ti, BindingKind::AnnAssign, stmt.range());
                } else if let Expr::Name(_) = &*a.target {
                    // bare annotation: no binding
                } else {
                    self.visit_expr(&a.target);
                }
            }
            Stmt::TypeAlias(t) => self.visit_expr(&t.value),
            Stmt::For(f) => {
                self.visit_expr(&f.iter);
                let body_range = match (f.body.first(), f.body.last()) {
                    (Some(a), Some(b)) => TextRange::new(f.target.start(), b.end().max(a.end())),
                    _ => f.range,
                };
                self.push_loop(LoopKind::For, f.range, body_range, Some(&f.iter));
                let target: &'a Expr = &f.target;
                let iter: &'a Expr = &f.iter;
                let srange = stmt.range();
                self.visit_loop_body(None, &f.body, move |w| {
                    w.bind_loop_target(target, iter, BindingKind::LoopVar, srange)
                });
                self.visit_body(&f.orelse);
            }
            Stmt::While(wh) => {
                let body_end = wh.body.last().map_or(wh.range.end(), |b| b.end());
                self.push_loop(
                    LoopKind::While,
                    wh.range,
                    TextRange::new(wh.test.start(), body_end),
                    None,
                );
                self.visit_loop_body(Some(&wh.test), &wh.body, |_| {});
                self.visit_body(&wh.orelse);
            }
            Stmt::If(i) => {
                self.visit_expr(&i.test);
                let mut branches: Vec<&'a [Stmt]> = vec![&i.body];
                let mut has_else = false;
                for clause in &i.elif_else_clauses {
                    if let Some(t) = &clause.test {
                        self.visit_expr(t);
                    } else {
                        has_else = true;
                    }
                    branches.push(&clause.body);
                }
                self.push_branch(i.range, &branches);
                self.visit_branches(&branches, has_else);
            }
            Stmt::With(w) => {
                for item in &w.items {
                    self.visit_expr(&item.context_expr);
                    if let Some(v) = &item.optional_vars {
                        let t = self.m.ty_of(&item.context_expr);
                        self.bind_target(
                            v,
                            Some(&item.context_expr),
                            t,
                            BindingKind::WithTarget,
                            stmt.range(),
                        );
                    }
                }
                self.visit_body(&w.body);
            }
            Stmt::Match(mt) => {
                self.visit_expr(&mt.subject);
                let mut branches: Vec<&'a [Stmt]> = Vec::new();
                for case in &mt.cases {
                    if let Some(g) = &case.guard {
                        self.visit_expr(g);
                    }
                    branches.push(&case.body);
                }
                self.push_branch(mt.range, &branches);
                self.visit_branches(&branches, false);
            }
            Stmt::Raise(r) => {
                if let Some(e) = &r.exc {
                    self.visit_expr(e);
                }
                if let Some(c) = &r.cause {
                    self.visit_expr(c);
                }
            }
            Stmt::Try(t) => {
                let before = self.snapshot();
                self.visit_body(&t.body);
                let after_body = self.snapshot();
                let mut merged = after_body.clone();
                let handler_start = Self::merge(&before, &after_body);
                for h in &t.handlers {
                    let ast::ExceptHandler::ExceptHandler(h) = h;
                    self.restore(handler_start.clone());
                    if let Some(ty) = &h.type_ {
                        self.visit_expr(ty);
                    }
                    if let Some(name) = &h.name {
                        self.add_binding(
                            name.as_str(),
                            BindingKind::ExceptVar,
                            name.range,
                            h.range,
                            None,
                            None,
                            TypeInfo::certain(Ty::Object),
                            None,
                        );
                    }
                    self.visit_body(&h.body);
                    if !Self::terminates(&h.body) {
                        merged = Self::merge(&merged, &self.snapshot());
                    }
                }
                self.restore(after_body);
                self.visit_body(&t.orelse);
                let after_else = self.snapshot();
                let merged = Self::merge(&merged, &after_else);
                self.restore(merged);
                self.visit_body(&t.finalbody);
            }
            Stmt::Assert(a) => {
                self.visit_expr(&a.test);
                if let Some(m) = &a.msg {
                    self.visit_expr(m);
                }
            }
            Stmt::Import(_) | Stmt::ImportFrom(_) => self.visit_import(stmt),
            Stmt::Global(g) => {
                for n in &g.names {
                    self.globals[self.scope as usize].insert(n.as_str().to_string());
                }
            }
            Stmt::Nonlocal(_) => {}
            Stmt::Expr(e) => {
                self.visit_expr(&e.value);
                self.cache_marker(&e.value, stmt);
            }
            Stmt::Pass(_) | Stmt::Break(_) | Stmt::Continue(_) | Stmt::IpyEscapeCommand(_) => {}
        }
    }

    /// `df.cache()` / `df.persist()` / `df.unpersist()` as a statement mutates `df`.
    fn cache_marker(&mut self, value: &'a Expr, stmt: &'a Stmt) {
        let Expr::Call(c) = value else { return };
        let Expr::Attribute(a) = &*c.func else { return };
        let Expr::Name(n) = &*a.value else { return };
        let method = a.attr.as_str();
        if !matches!(method, "cache" | "persist" | "unpersist") {
            return;
        }
        let t = self.m.ty_of(&a.value);
        if !t.is_dataframe() {
            return;
        }
        let mut nt = t;
        nt.facts.cached = method != "unpersist";
        self.add_binding(
            n.id.as_str(),
            BindingKind::CacheMarker,
            n.range,
            stmt.range(),
            Some(value),
            None,
            nt,
            None,
        );
    }

    fn visit_comprehension_expr(
        &mut self,
        expr: &'a Expr,
        generators: &'a [ast::Comprehension],
        elts: &[&'a Expr],
    ) {
        let Some(first) = generators.first() else {
            for e in elts {
                self.visit_expr(e);
            }
            return;
        };
        self.visit_expr(&first.iter);
        let cscope = self.new_scope(ScopeKind::Comprehension, expr.range(), "<comprehension>");
        self.push_loop(
            LoopKind::Comprehension,
            expr.range(),
            expr.range(),
            Some(&first.iter),
        );
        let saved = self.scope;
        self.scope = cscope;
        for (i, g) in generators.iter().enumerate() {
            if i > 0 {
                self.visit_expr(&g.iter);
            }
            self.bind_loop_target(
                &g.target,
                &g.iter,
                BindingKind::ComprehensionVar,
                expr.range(),
            );
            for cond in &g.ifs {
                self.visit_expr(cond);
            }
        }
        for e in elts {
            self.visit_expr(e);
        }
        self.scope = saved;
    }

    fn record_call_evidence(&mut self, c: &'a ast::ExprCall) {
        let Expr::Name(n) = &*c.func else { return };
        let [b] = self.m.reaching(n) else { return };
        let b = &self.m.bindings[*b as usize];
        let Some(def_scope) = b.def_scope else { return };
        let fscope = match b.kind {
            BindingKind::FunctionDef => def_scope,
            BindingKind::ClassDef => {
                match self
                    .m
                    .class_methods
                    .get(&(def_scope, "__init__".to_string()))
                {
                    Some(s) => *s,
                    None => return,
                }
            }
            _ => return,
        };
        let tys: Vec<TypeInfo> = c.arguments.args.iter().map(|a| self.m.ty_of(a)).collect();
        let e = self.m.arg_evidence.entry(fscope).or_default();
        for (i, t) in tys.into_iter().enumerate() {
            if i >= e.len() {
                e.push(t);
            } else {
                e[i] = e[i].join(t);
            }
        }
    }
}

impl<'a> Visitor<'a> for Walker<'a> {
    fn visit_stmt(&mut self, stmt: &'a Stmt) {
        self.visit_statement(stmt);
    }

    fn visit_expr(&mut self, expr: &'a Expr) {
        match expr {
            Expr::Name(n) => {
                let pending = self.pending_attr.take();
                if n.ctx == ExprContext::Load {
                    let reaching = self.lookup(n.id.as_str());
                    let (attr, called) = match pending {
                        Some((a, c)) => (Some(a), c),
                        None => (None, false),
                    };
                    self.m.uses.insert(
                        n.range.start(),
                        UseSite {
                            range: n.range,
                            scope: self.scope,
                            reaching,
                            attr,
                            called,
                        },
                    );
                }
            }
            Expr::Attribute(a) => {
                let called =
                    matches!(self.pending_attr.take(), Some((ref s, true)) if s == "\0call");
                if let Expr::Name(_) = &*a.value {
                    self.pending_attr = Some((a.attr.as_str().to_string(), called));
                }
                self.visit_expr(&a.value);
                self.pending_attr = None;
            }
            Expr::Call(c) => {
                if let Expr::Attribute(_) = &*c.func {
                    self.pending_attr = Some(("\0call".to_string(), true));
                }
                self.visit_expr(&c.func);
                self.pending_attr = None;
                for a in &c.arguments.args {
                    self.visit_expr(a);
                }
                for k in &c.arguments.keywords {
                    self.visit_expr(&k.value);
                }
                self.record_call_evidence(c);
            }
            Expr::Lambda(l) => {
                if let Some(p) = &l.parameters {
                    for d in p
                        .iter_non_variadic_params()
                        .filter_map(|p| p.default.as_deref())
                    {
                        self.visit_expr(d);
                    }
                }
                let lscope = self.new_scope(ScopeKind::Lambda, l.body.range(), "<lambda>");
                let saved = self.scope;
                self.scope = lscope;
                if let Some(p) = &l.parameters {
                    for p in p.iter_non_variadic_params() {
                        self.add_binding(
                            p.parameter.name.as_str(),
                            BindingKind::Param,
                            p.parameter.name.range,
                            p.range,
                            None,
                            None,
                            TypeInfo::UNKNOWN,
                            None,
                        );
                    }
                    for v in [&p.vararg, &p.kwarg].into_iter().flatten() {
                        self.add_binding(
                            v.name.as_str(),
                            BindingKind::Param,
                            v.name.range,
                            v.range,
                            None,
                            None,
                            TypeInfo::UNKNOWN,
                            None,
                        );
                    }
                }
                self.visit_expr(&l.body);
                self.scope = saved;
            }
            Expr::ListComp(c) => self.visit_comprehension_expr(expr, &c.generators, &[&c.elt]),
            Expr::SetComp(c) => self.visit_comprehension_expr(expr, &c.generators, &[&c.elt]),
            Expr::Generator(c) => self.visit_comprehension_expr(expr, &c.generators, &[&c.elt]),
            Expr::DictComp(c) => {
                let mut elts: Vec<&'a Expr> = Vec::new();
                if let Some(k) = &c.key {
                    elts.push(k);
                }
                elts.push(&c.value);
                self.visit_comprehension_expr(expr, &c.generators, &elts)
            }
            Expr::Named(n) => {
                self.visit_expr(&n.value);
                let t = self.m.ty_of(&n.value);
                // Walrus binds in the enclosing non-comprehension scope.
                let saved = self.scope;
                while self.m.scopes[self.scope as usize].kind == ScopeKind::Comprehension {
                    self.scope = self.m.scopes[self.scope as usize]
                        .parent
                        .unwrap_or(MODULE_SCOPE);
                }
                self.bind_target(
                    &n.target,
                    Some(&n.value),
                    t,
                    BindingKind::Walrus,
                    expr.range(),
                );
                self.scope = saved;
            }
            _ => {
                self.pending_attr = None;
                visitor::walk_expr(self, expr)
            }
        }
    }
}

/// Usage evidence for untyped parameters: `p.withColumn(...)` ⇒ DataFrame,
/// `p.read` / `p.createDataFrame(...)` ⇒ SparkSession, `p.iloc` ⇒ pandas; mixed
/// evidence is none ([`evidence::Evidence::verdict`]).
fn param_evidence(body: &[Stmt], params: &[&str]) -> FxHashMap<String, Ty> {
    struct Ev<'p> {
        params: &'p [&'p str],
        found: FxHashMap<String, Evidence>,
    }
    impl Ev<'_> {
        /// `x.attr` / `x.attr(..)` with `x` a parameter.
        fn param_use(&mut self, a: &ast::ExprAttribute, call: Option<&ast::ExprCall>) {
            if let Expr::Name(n) = &*a.value
                && self.params.contains(&n.id.as_str())
            {
                let u = evidence::of_use(a.attr.as_str(), call);
                if u.is_some() {
                    self.found.entry(n.id.to_string()).or_default().add(u);
                }
            }
        }
    }
    impl<'b> Visitor<'b> for Ev<'_> {
        fn visit_stmt(&mut self, stmt: &'b Stmt) {
            // do not descend into nested defs (their params shadow)
            if matches!(stmt, Stmt::FunctionDef(_) | Stmt::ClassDef(_)) {
                return;
            }
            visitor::walk_stmt(self, stmt);
        }
        fn visit_expr(&mut self, expr: &'b Expr) {
            match expr {
                // A called attribute: the call shape is part of the evidence.
                Expr::Call(c) if matches!(&*c.func, Expr::Attribute(_)) => {
                    let Expr::Attribute(a) = &*c.func else {
                        unreachable!()
                    };
                    self.param_use(a, Some(c));
                    self.visit_expr(&a.value);
                    for arg in &*c.arguments.args {
                        self.visit_expr(arg);
                    }
                    for k in &*c.arguments.keywords {
                        self.visit_expr(&k.value);
                    }
                }
                Expr::Attribute(a) => {
                    self.param_use(a, None);
                    visitor::walk_expr(self, expr);
                }
                _ => visitor::walk_expr(self, expr),
            }
        }
    }
    let mut ev = Ev {
        params,
        found: FxHashMap::default(),
    };
    for s in body {
        ev.visit_stmt(s);
    }
    ev.found
        .into_iter()
        .filter_map(|(name, e)| e.verdict().map(|t| (name, t)))
        .collect()
}

/// Bindings with a pandas-only use (`x.iloc`, `x.shape`, `x.sort_values(..)`) →
/// whether the uses say pandas frame (no Spark evidence beside it).
fn pandas_used_bindings(m: &SemanticModel) -> FxHashMap<TextRange, bool> {
    (0..m.bindings().len() as BindingId)
        .filter_map(|b| {
            let e = Evidence::of_attrs(m.binding_uses(b).filter_map(|u| u.attr.as_deref()));
            e.has_pandas()
                .then(|| (m.binding(b).range, e.verdict() == Some(Ty::PandasFrame)))
        })
        .collect()
}

fn contains_yield(body: &[Stmt]) -> bool {
    struct Y(bool);
    impl<'b> Visitor<'b> for Y {
        fn visit_stmt(&mut self, stmt: &'b Stmt) {
            if matches!(stmt, Stmt::FunctionDef(_) | Stmt::ClassDef(_)) {
                return;
            }
            visitor::walk_stmt(self, stmt);
        }
        fn visit_expr(&mut self, expr: &'b Expr) {
            if matches!(expr, Expr::Yield(_) | Expr::YieldFrom(_)) {
                self.0 = true;
            }
            if !matches!(expr, Expr::Lambda(_)) {
                visitor::walk_expr(self, expr);
            }
        }
    }
    let mut y = Y(false);
    for s in body {
        y.visit_stmt(s);
    }
    y.0
}
