//! Public data structures of the semantic model and its query API.

use ruff_python_ast::{Expr, ExprName};
use ruff_text_size::{Ranged, TextRange, TextSize};
use rustc_hash::FxHashMap;

use crate::types::{Ty, TypeInfo};

pub type ScopeId = u32;
pub type BindingId = u32;

/// The module scope always has id 0.
pub const MODULE_SCOPE: ScopeId = 0;

/// Options that influence inference.
#[derive(Debug, Clone)]
pub struct SemanticOptions {
    /// Enable the Databricks notebook globals (`spark`, `sc`, `sqlContext`, `dbutils`,
    /// `display`) as `Certain`.
    pub is_notebook: bool,
    /// Enable name/usage heuristics (`Confidence::Likely`).
    pub heuristics: bool,
    /// Extra globals: name → type (e.g. `spark` → SparkSession for job code).
    pub known_globals: Vec<(String, Ty)>,
    /// Dotted module name of the file (`pkg.mod`), used for relative imports and to
    /// key cross-file summaries.
    pub module_name: Option<String>,
}

impl Default for SemanticOptions {
    fn default() -> Self {
        Self {
            is_notebook: false,
            heuristics: true,
            known_globals: Vec::new(),
            module_name: None,
        }
    }
}

/// Source of function return types defined in *other* files (the project index).
pub trait SummaryProvider: Sync {
    /// Return type of the function with the given canonical qualified name
    /// (`pkg.mod.func` or `pkg.mod.Class.method`).
    fn function_return(&self, qualified: &str) -> Option<TypeInfo>;
}

/// A provider with no cross-file knowledge.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSummaries;

impl SummaryProvider for NoSummaries {
    fn function_return(&self, _qualified: &str) -> Option<TypeInfo> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    Module,
    Class,
    Function,
    Lambda,
    Comprehension,
}

#[derive(Debug, Clone)]
pub struct Scope {
    pub kind: ScopeKind,
    pub parent: Option<ScopeId>,
    /// Range in which names of this scope are visible (function: body).
    pub range: TextRange,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BindingKind {
    Import,
    Assign,
    AugAssign,
    AnnAssign,
    Param,
    LoopVar,
    ComprehensionVar,
    WithTarget,
    Walrus,
    FunctionDef,
    ClassDef,
    ExceptVar,
    /// Synthetic: a bare `x.cache()` / `x.persist()` / `x.unpersist()` statement
    /// re-binds `x` with updated facts.
    CacheMarker,
}

#[derive(Debug, Clone)]
pub struct Binding<'a> {
    pub name: String,
    pub scope: ScopeId,
    pub kind: BindingKind,
    /// Range of the binding target (the name).
    pub range: TextRange,
    /// Range of the binding statement.
    pub stmt_range: TextRange,
    /// The assigned value expression, if any.
    pub value: Option<&'a Expr>,
    /// Qualified name for imports and aliases (`W = Window`).
    pub qualified: Option<String>,
    pub ty: TypeInfo,
    /// For `FunctionDef`/`ClassDef`: the scope the definition creates.
    pub def_scope: Option<ScopeId>,
}

/// One read of a name.
#[derive(Debug, Clone)]
pub struct UseSite {
    pub range: TextRange,
    pub scope: ScopeId,
    /// Reaching bindings (empty = unbound/global/builtin).
    pub reaching: Vec<BindingId>,
    /// If the use is the receiver of an attribute access `x.attr`: the attribute name.
    pub attr: Option<String>,
    /// … and whether that attribute is called (`x.attr(...)`).
    pub called: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LoopKind {
    For,
    While,
    Comprehension,
}

#[derive(Debug, Clone)]
pub struct LoopInfo {
    pub kind: LoopKind,
    /// Whole statement / comprehension expression.
    pub range: TextRange,
    /// Region executed once per iteration.
    pub body: TextRange,
    /// The iterable (evaluated once) for `for` loops and the first comprehension
    /// generator; excluded from `body`.
    pub iter: Option<TextRange>,
    /// Innermost function/lambda/module scope containing the loop.
    pub func_scope: ScopeId,
    /// Number of elements when the iterable is a literal list/tuple/set.
    pub literal_len: Option<usize>,
}

impl LoopInfo {
    pub fn contains_in_body(&self, offset: TextSize) -> bool {
        self.body.contains(offset) && !self.iter.is_some_and(|r| r.contains(offset))
    }
}

/// An `if`/`elif`/`else` (or `match`) statement: its mutually exclusive arms.
#[derive(Debug, Clone)]
pub struct BranchInfo {
    /// Whole statement.
    pub range: TextRange,
    /// One range per arm (the arm's body), in source order.
    pub arms: Vec<TextRange>,
}

/// One step of a branch path: (index into [`SemanticModel::branches`], arm index).
pub type BranchStep = (u32, u32);

/// Per-function summary (DESIGN §3.2 item 6).
#[derive(Debug, Clone)]
pub struct FunctionSummary {
    pub name: String,
    /// Module-local path (`func`, `Class.method`); `None` for nested functions.
    pub path: Option<String>,
    pub scope: ScopeId,
    pub params: Vec<(String, TypeInfo)>,
    pub returns: TypeInfo,
    pub has_return_value: bool,
    pub annotated_return: Option<TypeInfo>,
    pub is_udf: bool,
    pub range: TextRange,
}

impl FunctionSummary {
    /// The effective return type.
    pub fn return_type(&self) -> TypeInfo {
        if let Some(a) = self.annotated_return {
            return a;
        }
        if self.has_return_value {
            self.returns
        } else {
            TypeInfo::certain(Ty::NoneTy)
        }
    }
}

/// Knowledge carried from the first inference pass into the second.
#[derive(Debug, Clone, Default)]
pub(crate) struct Prior {
    pub func_returns: FxHashMap<ScopeId, TypeInfo>,
    pub class_attrs: FxHashMap<(ScopeId, String), TypeInfo>,
    pub arg_evidence: FxHashMap<ScopeId, Vec<TypeInfo>>,
}

/// The semantic model of one module: scopes, bindings, uses, loops and summaries,
/// plus the type oracle (`ty_of`, `qualified_name`, `in_loop`, …).
pub struct SemanticModel<'a> {
    pub(crate) opts: SemanticOptions,
    pub(crate) scopes: Vec<Scope>,
    pub(crate) bindings: Vec<Binding<'a>>,
    pub(crate) uses: FxHashMap<TextSize, UseSite>,
    pub(crate) binding_uses: Vec<Vec<TextSize>>,
    pub(crate) loops: Vec<LoopInfo>,
    pub(crate) branches: Vec<BranchInfo>,
    pub(crate) functions: Vec<FunctionSummary>,
    pub(crate) func_by_scope: FxHashMap<ScopeId, usize>,
    pub(crate) class_attrs: FxHashMap<(ScopeId, String), TypeInfo>,
    pub(crate) class_methods: FxHashMap<(ScopeId, String), ScopeId>,
    pub(crate) arg_evidence: FxHashMap<ScopeId, Vec<TypeInfo>>,
    pub(crate) prior: Prior,
    pub(crate) external: &'a dyn SummaryProvider,
    /// `from pyspark.sql.functions import *` seen.
    pub(crate) star_functions: bool,
}

impl<'a> SemanticModel<'a> {
    pub(crate) fn empty(opts: SemanticOptions, external: &'a dyn SummaryProvider) -> Self {
        Self {
            opts,
            scopes: Vec::new(),
            bindings: Vec::new(),
            uses: FxHashMap::default(),
            binding_uses: Vec::new(),
            loops: Vec::new(),
            branches: Vec::new(),
            functions: Vec::new(),
            func_by_scope: FxHashMap::default(),
            class_attrs: FxHashMap::default(),
            class_methods: FxHashMap::default(),
            arg_evidence: FxHashMap::default(),
            prior: Prior::default(),
            external,
            star_functions: false,
        }
    }

    pub fn options(&self) -> &SemanticOptions {
        &self.opts
    }

    pub fn is_notebook(&self) -> bool {
        self.opts.is_notebook
    }

    pub fn heuristics(&self) -> bool {
        self.opts.heuristics
    }

    pub fn scopes(&self) -> &[Scope] {
        &self.scopes
    }

    pub fn scope(&self, id: ScopeId) -> &Scope {
        &self.scopes[id as usize]
    }

    pub fn bindings(&self) -> &[Binding<'a>] {
        &self.bindings
    }

    pub fn binding(&self, id: BindingId) -> &Binding<'a> {
        &self.bindings[id as usize]
    }

    pub fn loops(&self) -> &[LoopInfo] {
        &self.loops
    }

    pub fn functions(&self) -> &[FunctionSummary] {
        &self.functions
    }

    /// Summary of the function whose body scope is `scope`.
    pub fn function_summary(&self, scope: ScopeId) -> Option<&FunctionSummary> {
        self.func_by_scope
            .get(&scope)
            .and_then(|i| self.functions.get(*i))
    }

    /// The body scope of method `name` of the class whose scope is `class`.
    pub fn class_method(&self, class: ScopeId, name: &str) -> Option<ScopeId> {
        self.class_methods.get(&(class, name.to_string())).copied()
    }

    /// The use record of a `Name` load expression.
    pub fn use_site(&self, name: &ExprName) -> Option<&UseSite> {
        self.uses.get(&name.range.start())
    }

    /// Bindings reaching a `Name` load (def-use).
    pub fn reaching(&self, name: &ExprName) -> &[BindingId] {
        self.use_site(name)
            .map(|u| u.reaching.as_slice())
            .unwrap_or(&[])
    }

    /// The single binding reaching a name load, if exactly one does.
    pub fn single_binding(&self, name: &ExprName) -> Option<BindingId> {
        match self.reaching(name) {
            [b] => Some(*b),
            _ => None,
        }
    }

    /// All uses (name loads) of a binding, in source order.
    pub fn binding_uses(&self, id: BindingId) -> impl Iterator<Item = &UseSite> {
        self.binding_uses
            .get(id as usize)
            .into_iter()
            .flatten()
            .filter_map(|o| self.uses.get(o))
    }

    /// The binding created by a `Name` *store* target at `range`.
    pub fn binding_at(&self, target: TextRange) -> Option<BindingId> {
        self.bindings
            .iter()
            .position(|b| b.range == target && b.kind != BindingKind::CacheMarker)
            .map(|i| i as BindingId)
    }

    /// Innermost scope of any kind containing `offset`.
    pub fn scope_at(&self, offset: TextSize) -> ScopeId {
        let mut best = MODULE_SCOPE;
        let mut best_len = u32::MAX;
        for (i, s) in self.scopes.iter().enumerate().skip(1) {
            if s.range.contains_inclusive(offset) {
                let len = u32::from(s.range.len());
                if len < best_len {
                    best = i as ScopeId;
                    best_len = len;
                }
            }
        }
        best
    }

    /// Innermost function/lambda/module scope containing `offset`.
    pub fn function_scope_at(&self, offset: TextSize) -> ScopeId {
        let mut s = self.scope_at(offset);
        loop {
            let sc = self.scope(s);
            match sc.kind {
                ScopeKind::Module | ScopeKind::Function | ScopeKind::Lambda => return s,
                _ => match sc.parent {
                    Some(p) => s = p,
                    None => return MODULE_SCOPE,
                },
            }
        }
    }

    /// Loops (within the same function) whose body contains `offset`, outermost first.
    pub fn loops_at(&self, offset: TextSize) -> Vec<&LoopInfo> {
        let fs = self.function_scope_at(offset);
        let mut v: Vec<&LoopInfo> = self
            .loops
            .iter()
            .filter(|l| l.func_scope == fs && l.contains_in_body(offset))
            .collect();
        v.sort_by_key(|l| (l.range.start(), std::cmp::Reverse(l.range.end())));
        v.dedup_by_key(|l| l.range);
        v
    }

    /// Innermost enclosing loop of the node (same function).
    pub fn enclosing_loop<T: Ranged>(&self, node: &T) -> Option<&LoopInfo> {
        self.loops_at(node.start()).into_iter().last()
    }

    /// Whether the node is lexically inside a loop body of its own function.
    pub fn in_loop<T: Ranged>(&self, node: &T) -> bool {
        !self.loops_at(node.start()).is_empty()
    }

    /// Loop depth at an offset (same function).
    pub fn loop_depth(&self, offset: TextSize) -> usize {
        self.loops_at(offset).len()
    }

    /// Whether `offset` is inside a loop that does not also contain `outer`
    /// (i.e. code at `offset` runs repeatedly relative to `outer`).
    pub fn in_loop_not_containing(&self, offset: TextSize, outer: TextSize) -> bool {
        self.loops_at(offset)
            .iter()
            .any(|l| !l.contains_in_body(outer) && !l.range.contains(outer))
    }

    pub fn branches(&self) -> &[BranchInfo] {
        &self.branches
    }

    /// The `if`/`match` arms (same function) containing `offset`, outermost first.
    pub fn branch_path(&self, offset: TextSize) -> Vec<BranchStep> {
        let fs = self.function_scope_at(offset);
        let mut v: Vec<(TextRange, BranchStep)> = self
            .branches
            .iter()
            .enumerate()
            .filter_map(|(i, b)| {
                let arm = b.arms.iter().position(|a| a.contains_inclusive(offset))?;
                (self.function_scope_at(b.range.start()) == fs)
                    .then_some((b.range, (i as u32, arm as u32)))
            })
            .collect();
        v.sort_by_key(|(r, _)| (r.start(), std::cmp::Reverse(r.end())));
        v.into_iter().map(|(_, s)| s).collect()
    }

    /// Whether code at `a` and at `b` can never both run in one pass through their
    /// function (they sit in different arms of the same `if`/`match`).
    pub fn mutually_exclusive(&self, a: TextSize, b: TextSize) -> bool {
        let pa = self.branch_path(a);
        let pb = self.branch_path(b);
        pa.iter()
            .any(|(bi, arm)| pb.iter().any(|(bj, arm2)| bi == bj && arm != arm2))
    }

    /// Largest total weight of items that can all run on one execution path, where
    /// each item carries its [`branch_path`](Self::branch_path). Items in different
    /// arms of one branch are alternatives (the heavier arm counts); everything else
    /// adds up.
    pub fn max_path_weight(items: &[(Vec<BranchStep>, u32)]) -> u32 {
        type Item<'p> = (&'p [BranchStep], u32);
        type Arms<'p> = Vec<(u32, Vec<Item<'p>>)>;
        fn go(items: &[Item<'_>]) -> u32 {
            let mut total = 0u32;
            // branch index -> arm -> items below it
            let mut groups: Vec<(u32, Arms<'_>)> = Vec::new();
            for (path, w) in items {
                match path.split_first() {
                    None => total = total.saturating_add(*w),
                    Some(((b, arm), rest)) => {
                        let g = match groups.iter_mut().position(|(gb, _)| gb == b) {
                            Some(i) => &mut groups[i].1,
                            None => {
                                groups.push((*b, Vec::new()));
                                &mut groups.last_mut().expect("just pushed").1
                            }
                        };
                        match g.iter_mut().position(|(a, _)| a == arm) {
                            Some(i) => g[i].1.push((rest, *w)),
                            None => g.push((*arm, vec![(rest, *w)])),
                        }
                    }
                }
            }
            for (_, arms) in groups {
                let best = arms.iter().map(|(_, its)| go(its)).max().unwrap_or(0);
                total = total.saturating_add(best);
            }
            total
        }
        let v: Vec<(&[BranchStep], u32)> = items.iter().map(|(p, w)| (p.as_slice(), *w)).collect();
        go(&v)
    }

    /// Exported summaries for the project index: (local path, return type).
    pub fn exported_summaries(&self) -> Vec<(String, TypeInfo)> {
        self.functions
            .iter()
            .filter_map(|f| {
                let p = f.path.clone()?;
                let r = f.return_type();
                (!r.is_unknown()).then_some((p, r))
            })
            .collect()
    }
}
