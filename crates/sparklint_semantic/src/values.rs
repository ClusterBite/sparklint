//! "What does this name / iterable hold?": the one place that follows a name back to
//! the value(s) it was bound to.
//!
//! A lookup is driven by ONE policy, [`Hops`]: every binding that reaches a name counts
//! (an if/else-bound name holds two values), a binding holds its value only when the name
//! really has it ([`SemanticModel::held_value`]: assignments, annotated assignments,
//! walrus, literal unpacking, cache markers), and at most [`NAME_HOPS`] names are
//! followed in a row. Every lookup has the same cycle guard: a binding whose value is the
//! use itself (`x = x` in a loop) holds no known value; longer cycles (`x = x + 1`,
//! `a = b; b = a`) end on the hop budget.
//!
//! ```text
//! model.hop(e, h)        // one name hop: the value of every reaching binding (Reached)
//! model.hop_one(e, h)    // exactly one reaching binding with a held value
//! model.value_of(e, h)   // follow names bound once while possible; where it stopped
//! model.values_of(e, h)  // every path's final value (None: some path holds unknown)
//! model.known_values(e, h) / dicts_of(e, h)  // the paths that hold a value + complete
//! model.items_of(e, h)   // literal list/tuple/set items (spreads followed), dict keys,
//!                        // `.items()` pairs, `.keys()` / `.values()`
//! model.for_items(b, h)  // the loop target + constant items a for-loop binding takes
//! ```
//!
//! Callers that walk method chains between names (`w = df.write; w.mode(..)`) keep
//! their walk and spend the same budget through [`SemanticModel::hop_one`]; a later
//! question ("does this depend on the loop variable / a parameter?") reuses a [`Hops`]
//! the same way.

use ruff_python_ast::{self as ast, Expr};
use ruff_text_size::Ranged;

use crate::model::{BindingId, BindingKind, SemanticModel};

/// How many names a lookup follows in a row (`a = b; b = c; c = [..]`).
pub const NAME_HOPS: u32 = 4;

/// Container methods that put a value into the receiver in place (`rows.append(x)`).
pub const CONTAINER_MUTATORS: &[&str] = &[
    "append",
    "extend",
    "add",
    "update",
    "insert",
    "appendleft",
    "setdefault",
];

/// The hop policy: every binding that reaches a name counts (an if/else-bound name
/// holds two values), a binding holds its value only when the name really has it
/// (`x = v`, `x: T = v`, `(x := v)`, `a, b = 1, 2` → `b` holds `2`, a cache marker;
/// not `with v as x`), and at most [`NAME_HOPS`] names are followed in a row. The
/// value is a budget cursor: walkers that follow names between other steps (method
/// chains, nested conditions) carry it along and spend it through
/// [`SemanticModel::hop`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hops {
    left: u32,
}

impl Hops {
    /// A fresh budget.
    pub const DEFAULT: Hops = Hops { left: NAME_HOPS };

    /// Names that may still be followed.
    pub const fn left(&self) -> u32 {
        self.left
    }

    fn spent(self) -> Self {
        Hops {
            left: self.left - 1,
        }
    }
}

impl Default for Hops {
    fn default() -> Self {
        Hops::DEFAULT
    }
}

/// One binding reached by a name hop and the value it holds (`None`: a parameter, a
/// loop target, an import, a cycle, a binding the guard refuses).
#[derive(Debug, Clone, Copy)]
pub struct Reached<'a> {
    pub binding: BindingId,
    pub value: Option<&'a Expr>,
}

/// One item an iterable yields.
#[derive(Debug, Clone, Copy)]
pub enum Item<'e> {
    /// A list / tuple / set element, a dict key or value.
    Value(&'e Expr),
    /// A `(key, value)` pair of `d.items()`.
    Pair(&'e Expr, &'e Expr),
}

impl<'e> Item<'e> {
    /// The item as a `(k, v)` pair: a `.items()` pair or a 2-tuple literal.
    pub fn pair(&self) -> Option<(&'e Expr, &'e Expr)> {
        match *self {
            Item::Pair(k, v) => Some((k, v)),
            Item::Value(Expr::Tuple(t)) if t.elts.len() == 2 => Some((&t.elts[0], &t.elts[1])),
            Item::Value(_) => None,
        }
    }
}

/// The items of a constant iterable.
#[derive(Debug, Clone, Default)]
pub struct Items<'e> {
    pub items: Vec<Item<'e>>,
    /// Every item is known (no spread or path that could not be followed).
    pub complete: bool,
}

impl<'a> SemanticModel<'a> {
    /// The value binding `b` holds: what an assignment, annotated assignment, walrus,
    /// unpacking of a literal (`a, b = 1, 2`) or cache marker gave the name. `None` for
    /// parameters, loop / with / except targets, imports, definitions and unpacking
    /// of anything but a literal of the same length.
    pub fn held_value(&self, b: BindingId) -> Option<&'a Expr> {
        let binding = self.binding(b);
        match binding.kind {
            BindingKind::Assign
            | BindingKind::AnnAssign
            | BindingKind::Walrus
            | BindingKind::CacheMarker => binding.value,
            _ => None,
        }
    }

    /// Whether the container binding `b` holds is filled in place later: a
    /// [`CONTAINER_MUTATORS`] call on the name (`rows.append(x)`) or an item
    /// assignment (`d[k] = v`, `d[k] += v`). Its literal value is then only the
    /// start (`rows = []`), not what the name holds.
    pub fn mutated(&self, b: BindingId) -> bool {
        self.binding_uses(b).any(|u| {
            (u.called
                && u.attr
                    .as_deref()
                    .is_some_and(|a| CONTAINER_MUTATORS.contains(&a)))
                || self.item_stores.contains(&u.range.start())
        })
    }

    /// One name hop: every binding that reaches `e` (a name) with the value it holds.
    /// `None` when `e` is no name, the budget is spent or no binding reaches it.
    pub fn hop(&self, e: &Expr, h: Hops) -> Option<(Vec<Reached<'a>>, Hops)> {
        let Expr::Name(n) = e else { return None };
        if h.left == 0 {
            return None;
        }
        let reaching = self.reaching(n);
        if reaching.is_empty() {
            return None;
        }
        let out = reaching
            .iter()
            .map(|&b| Reached {
                binding: b,
                value: self.held_value(b).filter(|v| v.range() != n.range),
            })
            .collect();
        Some((out, h.spent()))
    }

    /// One name hop to the value of the only binding reaching `e`.
    pub fn hop_one(&self, e: &Expr, h: Hops) -> Option<(&'a Expr, Hops)> {
        match self.hop(e, h)? {
            (r, h) if r.len() == 1 => Some((r[0].value?, h)),
            _ => None,
        }
    }

    /// `e` followed through names while exactly one binding with a held value reaches
    /// them: the expression where it stopped (a name when it could not go on).
    pub fn value_of<'e>(&self, e: &'e Expr, h: Hops) -> &'e Expr
    where
        'a: 'e,
    {
        self.value_hops(e, h).0
    }

    /// [`Self::value_of`] plus the budget left there.
    pub fn value_hops<'e>(&self, mut e: &'e Expr, mut h: Hops) -> (&'e Expr, Hops)
    where
        'a: 'e,
    {
        while let Some((v, h2)) = self.hop_one(e, h) {
            e = v;
            h = h2;
        }
        (e, h)
    }

    /// The final value on every path through names (`e` itself when it is no name);
    /// `None` when some path ends in a name that holds no known value.
    pub fn values_of<'e>(&self, e: &'e Expr, h: Hops) -> Option<Vec<&'e Expr>>
    where
        'a: 'e,
    {
        let (values, complete) = self.known_values(e, h);
        complete.then(|| values.into_iter().map(|(v, _)| v).collect())
    }

    /// The final values of the paths through names that end in a held value, with
    /// the budget left there, and whether every path did.
    pub fn known_values<'e>(&self, e: &'e Expr, h: Hops) -> (Vec<(&'e Expr, Hops)>, bool)
    where
        'a: 'e,
    {
        let mut out = Vec::new();
        let complete = self.values_into(e, h, &mut out);
        (out, complete)
    }

    fn values_into<'e>(&self, e: &'e Expr, h: Hops, out: &mut Vec<(&'e Expr, Hops)>) -> bool
    where
        'a: 'e,
    {
        if !matches!(e, Expr::Name(_)) {
            out.push((e, h));
            return true;
        }
        let Some((reached, h)) = self.hop(e, h) else {
            return false;
        };
        let mut complete = true;
        for r in reached {
            complete &= r.value.is_some_and(|v| self.values_into(v, h, out));
        }
        complete
    }

    /// The dict literals `e` holds on the paths that hold one, and whether every path
    /// did (`config(map=SETTINGS)` with `SETTINGS` bound in an if/else: both dicts).
    pub fn dicts_of<'e>(&self, e: &'e Expr, h: Hops) -> (Vec<&'e ast::ExprDict>, bool)
    where
        'a: 'e,
    {
        let (values, mut complete) = self.known_values(e, h);
        let mut out = Vec::new();
        for (v, _) in values {
            match v {
                Expr::Dict(d) => out.push(d),
                _ => complete = false,
            }
        }
        (out, complete)
    }

    /// The items iterating `e` yields, when they are written out: list / tuple / set
    /// literals (`*spread` items followed), dict literals (keys), `d.items()` (pairs),
    /// `d.keys()`, `d.values()`, through names under `h`, on every path that holds
    /// one. `None` when no path does; `complete` is false when some path does not or
    /// an item is not written out (an unknown `*spread`, a `**` entry).
    pub fn items_of<'e>(&self, e: &'e Expr, h: Hops) -> Option<Items<'e>>
    where
        'a: 'e,
    {
        let mut items = Items {
            items: Vec::new(),
            complete: true,
        };
        self.items_into(e, h, &mut items).then_some(items)
    }

    fn items_into<'e>(&self, e: &'e Expr, h: Hops, out: &mut Items<'e>) -> bool
    where
        'a: 'e,
    {
        let (values, complete) = self.known_values(e, h);
        out.complete &= complete;
        let mut any = false;
        // Spreads and `.items()` receivers get the budget left on their path.
        for (v, inner) in values {
            let ok = self.iterable_into(v, inner, out);
            out.complete &= ok;
            any |= ok;
        }
        any
    }

    /// The items of one value (no name): false when it is no written-out iterable.
    fn iterable_into<'e>(&self, v: &'e Expr, inner: Hops, out: &mut Items<'e>) -> bool
    where
        'a: 'e,
    {
        match v {
            Expr::List(ast::ExprList { elts, .. })
            | Expr::Tuple(ast::ExprTuple { elts, .. })
            | Expr::Set(ast::ExprSet { elts, .. }) => {
                for x in elts {
                    match x {
                        Expr::Starred(st) => {
                            if !self.items_into(&st.value, inner, out) {
                                out.complete = false;
                            }
                        }
                        x => out.items.push(Item::Value(x)),
                    }
                }
            }
            Expr::Dict(d) => {
                for it in &d.items {
                    match &it.key {
                        Some(k) => out.items.push(Item::Value(k)),
                        None => out.complete = false,
                    }
                }
            }
            Expr::Call(c) if c.arguments.is_empty() => {
                let Expr::Attribute(a) = &*c.func else {
                    return false;
                };
                let method = a.attr.as_str();
                if !matches!(method, "items" | "keys" | "values") {
                    return false;
                }
                let (dicts, complete) = self.dicts_of(&a.value, inner);
                if dicts.is_empty() {
                    return false;
                }
                out.complete &= complete;
                for d in dicts {
                    for it in &d.items {
                        let Some(k) = &it.key else {
                            out.complete = false;
                            continue;
                        };
                        out.items.push(match method {
                            "items" => Item::Pair(k, &it.value),
                            "keys" => Item::Value(k),
                            _ => Item::Value(&it.value),
                        });
                    }
                }
            }
            _ => return false,
        }
        true
    }

    /// For a `for` loop variable binding: the loop's whole target and the items its
    /// iterable yields (see [`Self::items_of`]).
    pub fn for_items(&self, b: BindingId, h: Hops) -> Option<(&'a Expr, Items<'a>)> {
        let (target, iter) = self.for_loop_of(b)?;
        Some((target, self.items_of(iter, h)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoSummaries, SemanticOptions};
    use ruff_python_ast::visitor::{self, Visitor};
    use ruff_text_size::TextSize;

    struct LastName<'a> {
        name: &'a str,
        found: Option<&'a Expr>,
    }
    impl<'a> Visitor<'a> for LastName<'a> {
        fn visit_expr(&mut self, e: &'a Expr) {
            if let Expr::Name(n) = e
                && n.id.as_str() == self.name
                && n.ctx.is_load()
            {
                self.found = Some(e);
            }
            visitor::walk_expr(self, e);
        }
    }

    /// Builds the model of `src` and calls `f` with the last load of `name`.
    fn at<R>(src: &str, name: &str, f: impl FnOnce(&SemanticModel, &Expr) -> R) -> R {
        let module = ruff_python_parser::parse_module(src)
            .expect("valid source")
            .into_syntax();
        let model = crate::build_model(
            &module,
            TextSize::of(src),
            SemanticOptions::default(),
            &NoSummaries,
        );
        let mut v = LastName { name, found: None };
        v.visit_body(&module.body);
        f(&model, v.found.expect("name used"))
    }

    fn text(src: &str, e: &Expr) -> String {
        src[usize::from(e.start())..usize::from(e.end())].to_string()
    }

    /// (source, name, value_of, values_of, items_of texts)
    type Case = (&'static str, &'static str, &'static str, Texts, Texts);
    type Texts = Option<&'static [&'static str]>;

    /// Binding shapes → what `value_of` / `values_of` / `items_of` answer.
    #[test]
    fn binding_shapes() {
        let h = Hops::DEFAULT;
        let cases: &[Case] = &[
            (
                "x = [1, 2]\nx",
                "x",
                "[1, 2]",
                Some(&["[1, 2]"]),
                Some(&["1", "2"]),
            ),
            (
                "a = b = (1,)\na",
                "a",
                "(1,)",
                Some(&["(1,)"]),
                Some(&["1"]),
            ),
            ("a, b = 1, 2\nb", "b", "2", Some(&["2"]), None),
            ("a, b = f()\nb", "b", "b", None, None),
            ("y = 3\nx = y\nx", "x", "3", Some(&["3"]), None),
            (
                "if c:\n    d = {'a': 1}\nelse:\n    d = {'b': 2}\nd",
                "d",
                "d",
                Some(&["{'a': 1}", "{'b': 2}"]),
                Some(&["'a'", "'b'"]),
            ),
            ("def f(p):\n    p", "p", "p", None, None),
            ("for i in [1, 2]:\n    i", "i", "i", None, None),
            (
                "x = 0\nwhile c:\n    x = x + 1\nx",
                "x",
                "x",
                Some(&["0", "x + 1"]),
                None,
            ),
            (
                "while c:\n    x = x + 1",
                "x",
                "x + 1",
                Some(&["x + 1"]),
                None,
            ),
            ("while c:\n    x = x", "x", "x", None, None),
            ("with open(p) as fh:\n    fh", "fh", "fh", None, None),
            (
                "a, b = [1], [2]\nb",
                "b",
                "[2]",
                Some(&["[2]"]),
                Some(&["2"]),
            ),
            (
                "if c:\n    L = [1]\nelse:\n    L = f()\nL",
                "L",
                "L",
                Some(&["[1]", "f()"]),
                Some(&["1"]),
            ),
            (
                "A = [1]\nB = [*A, 2]\nB",
                "B",
                "[*A, 2]",
                Some(&["[*A, 2]"]),
                Some(&["1", "2"]),
            ),
            (
                "D = {'k': 'v'}\nP = D.items()\nP",
                "P",
                "D.items()",
                Some(&["D.items()"]),
                Some(&["'k':'v'"]),
            ),
            (
                "a = 1\nb = a\nc = b\nd = c\ne = d\nf = e\nf",
                "f",
                "b",
                None,
                None,
            ),
        ];
        for (src, name, value, values, items) in cases {
            at(src, name, |m, e| {
                assert_eq!(text(src, m.value_of(e, h)), *value, "value_of {src:?}");
                let vs = m
                    .values_of(e, h)
                    .map(|v| v.iter().map(|x| text(src, x)).collect::<Vec<_>>());
                assert_eq!(
                    vs.as_deref(),
                    values
                        .map(|v| v.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                        .as_deref(),
                    "values_of {src:?}"
                );
                let it = m.items_of(e, h).map(|i| {
                    i.items
                        .iter()
                        .map(|it| match it {
                            Item::Value(v) => text(src, v),
                            Item::Pair(k, v) => format!("{}:{}", text(src, k), text(src, v)),
                        })
                        .collect::<Vec<_>>()
                });
                assert_eq!(
                    it.as_deref(),
                    items
                        .map(|v| v.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                        .as_deref(),
                    "items_of {src:?}"
                );
            });
        }
    }

    #[test]
    fn items_complete() {
        let src = "if c:\n    L = [1]\nelse:\n    L = f()\nL";
        at(src, "L", |m, e| {
            assert!(
                !m.items_of(e, Hops::DEFAULT)
                    .expect("one known path")
                    .complete
            );
        });
        let src = "L = [1, 2]\nL";
        at(src, "L", |m, e| {
            assert!(m.items_of(e, Hops::DEFAULT).expect("literal").complete);
        });
    }

    #[test]
    fn for_items_pairs() {
        let src = "D = {'a': 1, **E}\nfor k, v in D.items():\n    k";
        at(src, "k", |m, e| {
            let Expr::Name(n) = e else { unreachable!() };
            let b = m.single_binding(n).expect("loop binding");
            let (target, items) = m.for_items(b, Hops::DEFAULT).expect("constant items");
            assert_eq!(text(src, target), "k, v");
            assert!(!items.complete);
            let pairs: Vec<_> = items
                .items
                .iter()
                .filter_map(|i| i.pair())
                .map(|(k, v)| (text(src, k), text(src, v)))
                .collect();
            assert_eq!(pairs, [("'a'".to_string(), "1".to_string())]);
        });
    }
}
