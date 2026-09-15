//! Minimal jq-style JSON query engine.
//!
//! Two execution modes:
//!
//! 1. **Path query** (backward-compatible): `JsonQuery::parse(".data.t[]").execute(&v)`
//!    walks operators `Match/Access/Iter/Parent/Select` and returns a `Vec<Value>`.
//!    For zero-copy access use `pick` (handles `Match/Access/Iter/Parent`; not
//!    `Select` because it allocates a fresh object).
//! 2. **Assignment / bindings** (new): `JsonQuery::parse("from = sub_secs(ts(), interval())")`
//!    with top-level `name = expr` pairs evaluates to a single `Value::Object`.
//!    `execute` returns `vec![Value::Object({...})]` in that case.
//!
//! Built-in functions (`now`, `ts`, `interval`, `attr`, `sub_secs`, `add_secs`,
//! `add`, `sub`, `mul`, `div`, `int`, `float`, `str`) only resolve inside the
//! assignment mode — they read from a context object `{ts, interval, now,
//! payload, attributes}`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub enum Operator {
    Match(String),
    Access(usize),
    Iter,
    Select(Vec<String>),
    /// Climb one level to the element's parent (path token `^`).
    Parent,
    /// Numeric literal parsed from the query.
    LiteralInt(i64),
    /// Float literal parsed from the query.
    LiteralFloat(f64),
    /// String literal parsed from the query (`"abc"` or `'abc'`).
    LiteralStr(String),
    /// Built-in function call. Args are pre-parsed `JsonQuery`s so they can
    /// reference other calls, literals, or path lookups.
    Call(String, Vec<JsonQuery>),
    /// `name = expr` at top level — push into the object being built.
    Assign(String, Box<JsonQuery>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonQuery {
    operators: Vec<Operator>,
}

/// One element in flight during a query: the current value plus the chain of
/// ancestors it was reached through (root first, immediate parent last).
struct Frame<'a, V> {
    val: &'a V,
    parents: Vec<&'a V>,
}

impl<'a, V> Frame<'a, V> {
    fn child(&self, val: &'a V) -> Self {
        let mut parents = self.parents.clone();
        parents.push(self.val);
        Self { val, parents }
    }

    fn parent(&self) -> Option<Self> {
        let mut parents = self.parents.clone();
        let val = parents.pop()?;
        Some(Self { val, parents })
    }
}

/// Returns true when the operator list looks like top-level assignments:
/// every operator is `Assign` and at least one exists.
fn is_assignment(ops: &[Operator]) -> bool {
    !ops.is_empty() && ops.iter().all(|op| matches!(op, Operator::Assign(_, _)))
}

impl JsonQuery {
    pub fn new(operators: Vec<Operator>) -> Self {
        Self { operators }
    }

    /// Parse a jq-style path or an assignment expression.
    ///
    /// Path syntax (legacy): `.field.sub[0][]` etc.
    /// Assignment syntax: `name = expr, name = expr, ...` where each `expr`
    /// is itself a path or a built-in call (`ts()`, `sub_secs(a, b)`, `42`,
    /// `"abc"`, `payload.x`). The first non-assignment operator terminates
    /// the assignment list.
    pub fn parse(path: &str) -> Result<Self, Error> {
        let mut operators = Vec::new();
        let mut chars = path.chars().peekable();

        // Track whether we have seen at least one `name = expr` and whether
        // we are still in "assignment mode" at the top level.
        let mut had_assign = false;
        let mut in_assign_mode = true;

        while let Some(c) = chars.next() {
            if !in_assign_mode {
                // Fall through to path parsing.
                if c == '.' {
                    continue;
                }
                parse_path_token(c, &mut chars, &mut operators)?;
                continue;
            }

            // Assignment mode at top level: read `ident = expr`.
            if c.is_whitespace() || c == ',' {
                continue;
            }

            if c == '.' {
                // Path begins — abandon assignment mode, push operator.
                in_assign_mode = false;
                parse_path_token('.', &mut chars, &mut operators)?;
                // The dot itself is consumed; continue the inner path parsing
                // for the following character via re-entering the loop with
                // a synthetic recursion. Simplest: just call the helper for
                // `.X` style by re-processing the next char.
                continue;
            }

            // Try to read identifier for assignment LHS.
            if c.is_alphabetic() || c == '_' {
                let mut ident = String::new();
                ident.push(c);
                while let Some(&next) = chars.peek() {
                    if next.is_alphanumeric() || next == '_' {
                        ident.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                // Skip whitespace.
                while let Some(&next) = chars.peek() {
                    if next.is_whitespace() {
                        chars.next();
                    } else {
                        break;
                    }
                }
                if chars.peek() == Some(&'=') && chars.clone().nth(1) != Some('=') {
                    chars.next(); // consume '='
                    let expr = parse_expr(&mut chars)?;
                    operators.push(Operator::Assign(ident, Box::new(expr)));
                    had_assign = true;
                    continue;
                }
                // Not an assignment — this looks like a function call
                // (e.g. `ts()`, `sub_secs(a, b)`) or a literal/lone ident.
                // If followed by `(` it's a Call; otherwise treat the ident
                // as the start of a path.
                if chars.peek() == Some(&'(') {
                    chars.next(); // consume '('
                    let args = parse_call_args(&mut chars)?;
                    operators.push(Operator::Call(ident, args));
                    // After a top-level call, we are no longer in assignment
                    // mode (the call is its own expression).
                    in_assign_mode = false;
                    continue;
                }
                // Lone ident without `(`: enter path mode treating ident as
                // a top-level field.
                in_assign_mode = false;
                operators.push(Operator::Match(ident));
                continue;
            }

            // Numeric literal at top level (e.g. `x = 42` RHS already handled;
            // here we are at top level and see a number directly).
            if c.is_ascii_digit() || (c == '-' && chars.peek().is_some_and(|n| n.is_ascii_digit()))
            {
                let mut lit = String::new();
                lit.push(c);
                while let Some(&next) = chars.peek() {
                    if next.is_ascii_digit() || next == '.' {
                        lit.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                operators.push(parse_numeric_literal(&lit));
                in_assign_mode = false;
                continue;
            }

            // String literal at top level.
            if c == '"' || c == '\'' {
                let quote = c;
                let mut lit = String::new();
                for next in chars.by_ref() {
                    if next == quote {
                        break;
                    }
                    lit.push(next);
                }
                operators.push(Operator::LiteralStr(lit));
                in_assign_mode = false;
                continue;
            }

            // Anything else at top level falls back to path parsing.
            in_assign_mode = false;
            parse_path_token(c, &mut chars, &mut operators)?;
        }

        // If we parsed an assignment and never escaped, that's fine.
        let _ = had_assign;
        Ok(Self { operators })
    }

    /// Public entry: returns `Vec<Value>`. In assignment mode the result is
    /// always a single `Value::Object` (wrapped in a 1-element vec).
    /// Pure path queries (`Match`/`Access`/`Iter`/`Parent`) are answered
    /// zero-copy via `pick`. Queries containing operators that build
    /// fresh values (`Select`) fall back to the owned evaluation path.
    pub fn execute(&self, data: &Value) -> Vec<Value> {
        if is_assignment(&self.operators) {
            return vec![self.eval(data)];
        }
        if self.has_select() {
            return self.execute_owned(data);
        }
        self.pick(data).into_iter().cloned().collect()
    }

    /// True when any operator in the chain needs to construct a fresh
    /// value (e.g. `Select`) rather than borrow through the input.
    fn has_select(&self) -> bool {
        self.operators
            .iter()
            .any(|op| matches!(op, Operator::Select(_)))
    }

    /// Owned fallback for path queries that include `Select` (or other
    /// non-borrowing operators). Mirrors `pick`'s walk but emits owned
    /// values at every step.
    fn execute_owned(&self, data: &Value) -> Vec<Value> {
        let mut scratch: Vec<Value> = vec![data.clone()];
        for op in &self.operators {
            let mut next: Vec<Value> = Vec::new();
            for val in &scratch {
                match op {
                    Operator::Match(field) => {
                        if let Some(v) = val.get(field) {
                            next.push(v.clone());
                        }
                    }
                    Operator::Access(idx) => {
                        if let Value::Array(arr) = val
                            && let Some(v) = arr.get(*idx)
                        {
                            next.push(v.clone());
                        }
                    }
                    Operator::Iter => match val {
                        Value::Array(arr) => {
                            for v in arr {
                                next.push(v.clone());
                            }
                        }
                        Value::Object(map) => {
                            for v in map.values() {
                                next.push(v.clone());
                            }
                        }
                        _ => {}
                    },
                    Operator::Select(keep) => {
                        if let Some(obj) = val.as_object() {
                            let mut out = serde_json::Map::new();
                            for key in keep {
                                if let Some(v) = obj.get(key) {
                                    out.insert(key.clone(), v.clone());
                                }
                            }
                            next.push(Value::Object(out));
                        }
                    }
                    _ => {}
                }
            }
            scratch = next;
            if scratch.is_empty() {
                break;
            }
        }
        scratch
    }

    /// Walk path operators and return zero-copy borrowed references.
    ///
    /// Handles the borrowing subset of path operators (`Match`, `Access`,
    /// `Iter`, `Parent`). `Select` is excluded — it builds a fresh object
    /// and cannot yield a `&'a Value` borrowed from `data`; callers that
    /// need it must use `execute` and accept the clone.
    pub fn pick<'a>(&self, data: &'a Value) -> Vec<&'a Value> {
        let mut collection = vec![Frame {
            val: data,
            parents: Vec::new(),
        }];

        for op in &self.operators {
            let mut next_collection = Vec::new();
            for item in collection {
                match op {
                    Operator::Match(field) => {
                        if let Some(v) = item.val.get(field) {
                            next_collection.push(item.child(v));
                        }
                    }
                    Operator::Access(index) => {
                        if let Some(v) = item.val.get(*index) {
                            next_collection.push(item.child(v));
                        } else if let Some(v) = item.val.get(index.to_string()) {
                            next_collection.push(item.child(v));
                        }
                    }
                    Operator::Iter => {
                        if let Some(arr) = item.val.as_array() {
                            for v in arr {
                                next_collection.push(item.child(v));
                            }
                        } else if let Some(obj) = item.val.as_object() {
                            for v in obj.values() {
                                next_collection.push(item.child(v));
                            }
                        } else {
                            println!(
                                "Warning: Cannot iterate over non-array/object value: {:?}",
                                item.val
                            );
                        }
                    }
                    Operator::Parent => {
                        if let Some(p) = item.parent() {
                            next_collection.push(p);
                        }
                    }
                    // `Select` and literal/Call operators are not legal in
                    // pure path queries routed through `pick` — assignment
                    // queries use `eval` instead.
                    _ => {}
                }
            }
            collection = next_collection;
            if collection.is_empty() {
                break;
            }
        }
        collection.into_iter().map(|f| f.val).collect()
    }

    /// Internal — evaluate assignment-style query on the given context.
    /// Returns the resulting `Value::Object`. Public callers should use
    /// `execute` instead.
    fn eval(&self, ctx: &Value) -> Value {
        let mut out = serde_json::Map::new();
        for op in &self.operators {
            if let Operator::Assign(name, expr) = op {
                let val = self.eval_expr(expr, ctx);
                if let Some(v) = val.into_iter().next() {
                    out.insert(name.clone(), v);
                }
            }
        }
        Value::Object(out)
    }

    /// Evaluate a single expression (not necessarily a top-level assignment).
    /// Returns a `Vec<Value>` of produced values.
    fn eval_expr(&self, expr: &JsonQuery, ctx: &Value) -> Vec<Value> {
        // Literal / Call operators produce values independently from ctx.
        for op in &expr.operators {
            match op {
                Operator::Call(name, args) => {
                    if let Some(value) = self.call_builtin(name, args, ctx) {
                        return vec![value];
                    }
                }
                Operator::LiteralInt(i) => return vec![Value::from(*i)],
                Operator::LiteralFloat(f) => return vec![Value::from(*f)],
                Operator::LiteralStr(s) => return vec![Value::from(s.clone())],
                _ => {}
            }
        }
        // Path-style operators: walk the full chain starting from ctx,
        // chaining each operator onto the output of the previous one.
        let mut scratch = vec![ctx.clone()];
        for op in &expr.operators {
            let mut next = Vec::new();
            for val in &scratch {
                match op {
                    Operator::Match(field) => {
                        if let Some(v) = val.get(field) {
                            next.push(v.clone());
                        }
                    }
                    Operator::Access(idx) => {
                        if let Some(v) = val.get(*idx) {
                            next.push(v.clone());
                        } else if let Some(v) = val.get(idx.to_string()) {
                            next.push(v.clone());
                        }
                    }
                    Operator::Iter => {
                        if let Some(arr) = val.as_array() {
                            for v in arr {
                                next.push(v.clone());
                            }
                        } else if let Some(obj) = val.as_object() {
                            for v in obj.values() {
                                next.push(v.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            scratch = next;
            if scratch.is_empty() {
                break;
            }
        }
        scratch
    }

    /// Dispatch a built-in call. Returns `None` on error / unknown function.
    fn call_builtin(&self, name: &str, args: &[JsonQuery], ctx: &Value) -> Option<Value> {
        let eval_arg = |idx: usize| -> Option<Value> {
            let a = args.get(idx)?;
            self.eval_expr(a, ctx).into_iter().next()
        };

        match name {
            "now" => require_arity(name, args.len(), 0)
                .ok()
                .map(|_| ctx.get("now").cloned().unwrap_or(Value::Null)),
            "ts" => require_arity(name, args.len(), 0)
                .ok()
                .map(|_| ctx.get("ts").cloned().unwrap_or(Value::Null)),
            "interval" => require_arity(name, args.len(), 0)
                .ok()
                .map(|_| ctx.get("interval").cloned().unwrap_or(Value::Null)),
            "attr" => {
                require_arity(name, args.len(), 1).ok()?;
                let key = eval_arg(0)?.as_str()?.to_string();
                let attrs = ctx.get("attributes").and_then(|v| v.as_object())?;
                Some(attrs.get(&key).cloned().unwrap_or(Value::Null))
            }
            "sub_secs" => {
                require_arity(name, args.len(), 2).ok()?;
                let a = to_i64(&eval_arg(0)?)?;
                let b = to_i64(&eval_arg(1)?)?;
                Some(Value::from(a - b))
            }
            "add_secs" => {
                require_arity(name, args.len(), 2).ok()?;
                let a = to_i64(&eval_arg(0)?)?;
                let b = to_i64(&eval_arg(1)?)?;
                Some(Value::from(a + b))
            }
            "add" => binary_arith(name, args, ctx, self, |a, b| a + b),
            "sub" => binary_arith(name, args, ctx, self, |a, b| a - b),
            "mul" => binary_arith(name, args, ctx, self, |a, b| a * b),
            "div" => {
                require_arity(name, args.len(), 2).ok()?;
                let a = eval_arg(0)?;
                let b = eval_arg(1)?;
                let (af, bf) = (to_f64(&a)?, to_f64(&b)?);
                if bf == 0.0 {
                    return None;
                }
                Some(Value::from(af / bf))
            }
            "int" => {
                require_arity(name, args.len(), 1).ok()?;
                Some(Value::from(to_i64(&eval_arg(0)?)?))
            }
            "float" => {
                require_arity(name, args.len(), 1).ok()?;
                Some(Value::from(to_f64(&eval_arg(0)?)))
            }
            "str" => {
                require_arity(name, args.len(), 1).ok()?;
                Some(Value::from(value_to_string(&eval_arg(0)?)))
            }
            _ => None,
        }
    }
}

fn require_arity(name: &str, got: usize, want: usize) -> Result<(), Error> {
    if got != want {
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!("builtin `{name}` expects {want} args, got {got}"),
        ))
    } else {
        Ok(())
    }
}

fn to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64().or_else(|| n.as_i64().map(|i| i as f64)),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn binary_arith<F>(
    name: &str,
    args: &[JsonQuery],
    ctx: &Value,
    self_: &JsonQuery,
    f: F,
) -> Option<Value>
where
    F: Fn(f64, f64) -> f64,
{
    require_arity(name, args.len(), 2).ok()?;
    let a = self_.eval_expr(&args[0], ctx).into_iter().next()?;
    let b = self_.eval_expr(&args[1], ctx).into_iter().next()?;
    let af = to_f64(&a)?;
    let bf = to_f64(&b)?;
    Some(Value::from(f(af, bf)))
}

fn parse_numeric_literal(s: &str) -> Operator {
    if s.contains('.') {
        Operator::LiteralFloat(s.parse::<f64>().unwrap_or(0.0))
    } else {
        Operator::LiteralInt(s.parse::<i64>().unwrap_or(0))
    }
}

/// Parse a path token — used for both the legacy path mode and for parsing
/// RHS expressions in assignment mode that contain path tokens.
fn parse_path_token(
    c: char,
    chars: &mut std::iter::Peekable<std::str::Chars>,
    operators: &mut Vec<Operator>,
) -> Result<(), Error> {
    match c {
        '.' => Ok(()),
        '^' => {
            operators.push(Operator::Parent);
            Ok(())
        }
        '[' => {
            let mut content = String::new();
            let mut found_close = false;
            while let Some(&next) = chars.peek() {
                if next == ']' {
                    chars.next();
                    found_close = true;
                    break;
                }
                content.push(chars.next().unwrap());
            }
            if !found_close {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Missing closing bracket ']'",
                ));
            }
            if content.is_empty() {
                operators.push(Operator::Iter);
            } else if let Ok(index) = content.parse::<usize>() {
                operators.push(Operator::Access(index));
            } else {
                let clean_field = content.trim_matches(|c| c == '"' || c == '\'');
                operators.push(Operator::Match(clean_field.to_string()));
            }
            Ok(())
        }
        _ => {
            let mut field = String::new();
            field.push(c);
            while let Some(&next) = chars.peek() {
                if next == '.' || next == '[' {
                    break;
                }
                field.push(chars.next().unwrap());
            }
            if let Ok(index) = field.parse::<usize>() {
                operators.push(Operator::Access(index));
            } else {
                operators.push(Operator::Match(field));
            }
            Ok(())
        }
    }
}

/// Parse the RHS expression of an assignment. Stops at top-level `,` or
/// end-of-input.
fn parse_expr(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<JsonQuery, Error> {
    let mut operators = Vec::new();
    let mut in_path = false;
    while let Some(&c) = chars.peek() {
        if c == ',' || c == ')' {
            if c == ',' {
                chars.next();
            }
            break;
        }
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        let c = chars.next().unwrap();
        // Detect a top-level call: ident followed by `(`.
        if !in_path && (c.is_alphabetic() || c == '_') {
            let mut ident = String::new();
            ident.push(c);
            while let Some(&next) = chars.peek() {
                if next.is_alphanumeric() || next == '_' {
                    ident.push(chars.next().unwrap());
                } else {
                    break;
                }
            }
            if chars.peek() == Some(&'(') {
                chars.next(); // consume '('
                let args = parse_call_args(chars)?;
                operators.push(Operator::Call(ident, args));
                in_path = true;
                continue;
            }
            // Lone ident — treat as Match against the context.
            operators.push(Operator::Match(ident));
            in_path = true;
            continue;
        }
        if !in_path
            && (c.is_ascii_digit()
                || (c == '-' && chars.peek().is_some_and(|n| n.is_ascii_digit())))
        {
            let mut lit = String::new();
            lit.push(c);
            while let Some(&next) = chars.peek() {
                if next.is_ascii_digit() || next == '.' {
                    lit.push(chars.next().unwrap());
                } else {
                    break;
                }
            }
            operators.push(parse_numeric_literal(&lit));
            in_path = true;
            continue;
        }
        if !in_path && (c == '"' || c == '\'') {
            let quote = c;
            let mut lit = String::new();
            for next in chars.by_ref() {
                if next == quote {
                    break;
                }
                lit.push(next);
            }
            operators.push(Operator::LiteralStr(lit));
            in_path = true;
            continue;
        }
        parse_path_token(c, chars, &mut operators)?;
        in_path = true;
    }
    Ok(JsonQuery { operators })
}

/// Parse comma-separated argument list inside a `(` ... `)`.
/// Consumes the closing `)`.
fn parse_call_args(
    chars: &mut std::iter::Peekable<std::str::Chars>,
) -> Result<Vec<JsonQuery>, Error> {
    let mut args = Vec::new();
    loop {
        // Skip whitespace and commas between args.
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() || c == ',' {
                chars.next();
            } else {
                break;
            }
        }
        if chars.peek() == Some(&')') {
            chars.next();
            return Ok(args);
        }
        let arg = parse_expr(chars)?;
        args.push(arg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Instant;

    fn generate_large_data(size: usize) -> Value {
        let mut t = Vec::with_capacity(size);
        let mut c = Vec::with_capacity(size);
        for i in 0..size {
            t.push(1600000000 + i);
            c.push(120.5 + (i as f64 * 0.1));
        }
        json!({ "data": { "t": t, "c": c } })
    }

    // ──────────────────────────────────────────────────────────────────
    // Backward-compat tests (path queries)
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn run_benchmark() {
        let size = 10_000;
        let data = generate_large_data(size);
        let path_t = "data.t[]";

        let query = JsonQuery::parse(path_t).unwrap();
        let start = Instant::now();
        let results = query.execute(&data);
        let exec_duration = start.elapsed();

        println!(
            "\n🚀 Zero-copy extraction ({} elements): {:?}",
            size, exec_duration
        );
        println!("📊 Average per element: {:?}", exec_duration / size as u32);

        assert_eq!(results.len(), size);
    }

    #[test]
    fn test_basic_path() {
        let data = json!({"stock": {"symbol": "VND", "price": 15000}});
        let query = JsonQuery::parse("stock.symbol").unwrap();
        let result = query.execute(&data);
        assert_eq!(result, vec![json!("VND")]);
    }

    #[test]
    fn test_array_access() {
        let data = json!({"prices": [100, 200, 300]});
        let query = JsonQuery::parse("prices[1]").unwrap();
        let result = query.execute(&data);
        assert_eq!(result, vec![json!(200)]);
    }

    #[test]
    fn test_binance_style() {
        let data = json!([[161000, "20.5"], [162000, "21.0"]]);
        let query = JsonQuery::parse("[].0").unwrap();
        let result = query.execute(&data);
        assert_eq!(result, vec![json!(161000), json!(162000)]);
    }

    #[test]
    fn test_nested_iter_match() {
        let data = json!({
            "items": [
                {"t": 1, "c": 10.5},
                {"t": 2, "c": 11.0}
            ]
        });
        let query = JsonQuery::parse("items[].c").unwrap();
        let result = query.execute(&data);
        assert_eq!(result, vec![json!(10.5), json!(11.0)]);
    }

    #[test]
    fn test_deep_mixed_nesting() {
        let data = json!({
            "markets": [
                {
                    "stocks": [
                        {"symbol": "FPT", "history": [10, 11]},
                        {"symbol": "VNM", "history": [20, 21]}
                    ]
                }
            ]
        });
        let query = JsonQuery::parse("markets[].stocks[].history[1]").unwrap();
        let result = query.execute(&data);
        assert_eq!(result, vec![json!(11), json!(21)]);
    }

    #[test]
    fn test_asymmetric_json() {
        let data = json!([
            {"info": {"price": 100}},
            {"error": "not found"},
            {"info": {"price": 200}}
        ]);
        let query = JsonQuery::parse("[].info.price").unwrap();
        let result = query.execute(&data);
        assert_eq!(result, vec![json!(100), json!(200)]);
    }

    #[test]
    fn test_invalid_path() {
        let query = JsonQuery::parse("data[missing");
        assert!(query.is_err());
    }

    #[test]
    fn test_index_after_dot() {
        let data = json!([["a", "b"], ["c", "d"]]);
        let query = JsonQuery::parse("[].1").unwrap();
        let result = query.execute(&data);
        assert_eq!(result, vec![json!("b"), json!("d")]);
    }

    #[test]
    fn test_bracket_with_quotes() {
        let data = json!({"complex-key": {"val": 42}});
        let query = JsonQuery::parse("[\"complex-key\"].val").unwrap();
        let result = query.execute(&data);
        assert_eq!(result, vec![json!(42)]);
    }

    #[test]
    fn test_get_root_array() {
        let data = json!([1, 2, 3, 4, 5]);
        let query_root = JsonQuery::parse("").unwrap();
        let result_root = query_root.execute(&data);
        assert_eq!(result_root.len(), 1);
        assert_eq!(result_root[0], data);
        assert!(result_root[0].is_array());
    }

    #[test]
    fn test_get_array_elements_directly() {
        let data = json!([1, 2, 3]);
        let query_iter = JsonQuery::parse("[]").unwrap();
        let result_iter = query_iter.execute(&data);
        let json_output = serde_json::to_string_pretty(&query_iter.operators).unwrap();
        println!("--- JSON Representation of Operators ---");
        println!("{}", json_output);
        println!("---------------------------------------");
        assert_eq!(result_iter.len(), 3);
        assert_eq!(result_iter[0], json!(1));
        assert_eq!(result_iter[1], json!(2));
        assert_eq!(result_iter[2], json!(3));
    }

    #[test]
    fn test_parent_climb_to_sibling_field() {
        let data = json!({
            "series": [
                {"metric": {"instance": "a:1"}, "values": [[1, "1"], [2, "2"]]},
                {"metric": {"instance": "b:2"}, "values": [[3, "3"]]}
            ]
        });
        let query = JsonQuery::parse("series[].values[].^.^.metric.instance").unwrap();
        assert_eq!(
            query.execute(&data),
            vec![json!("a:1"), json!("a:1"), json!("b:2")]
        );
    }

    #[test]
    fn test_single_parent_from_array_element() {
        let data = json!({"points": [[1, "x"], [2, "y"]]});
        let query = JsonQuery::parse("points[].^.len").unwrap();
        assert!(query.execute(&data).is_empty());
        let query2 = JsonQuery::parse("points[].^").unwrap();
        let result = query2.execute(&data);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], json!([[1, "x"], [2, "y"]]));
    }

    #[test]
    fn test_parent_at_root_is_noop() {
        let data = json!({"a": 1});
        let query = JsonQuery::parse("^.a").unwrap();
        assert!(query.execute(&data).is_empty());
    }

    #[test]
    fn test_select_operator_manually() {
        let data = json!({
            "Data": [
                {
                    "currencyName": "US DOLLAR",
                    "currencyCode": "USD",
                    "cash": "26108.00",
                    "sell": "26368.00"
                },
                {
                    "currencyName": "EURO",
                    "currencyCode": "EUR",
                    "cash": "30017.75",
                    "sell": "31600.24"
                }
            ]
        });

        let query = JsonQuery::new(vec![
            Operator::Match("Data".to_string()),
            Operator::Iter,
            Operator::Select(vec!["currencyCode".to_string(), "sell".to_string()]),
        ]);

        let results = query.execute(&data);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["currencyCode"], "USD");
        assert_eq!(results[0]["sell"], "26368.00");
        assert_eq!(results[1]["currencyCode"], "EUR");
        assert_eq!(results[1]["sell"], "31600.24");
        assert!(results[0].get("currencyName").is_none());

        println!(
            "Result JSON of {}: {}",
            serde_json::to_string(&query).unwrap(),
            serde_json::to_string_pretty(&results).unwrap(),
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // Literal parsing tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_parse_int_literal() {
        let q = JsonQuery::parse("123").unwrap();
        assert_eq!(q.operators, vec![Operator::LiteralInt(123)]);
    }

    #[test]
    fn test_parse_float_literal() {
        let q = JsonQuery::parse("1.5").unwrap();
        assert_eq!(q.operators, vec![Operator::LiteralFloat(1.5)]);
    }

    #[test]
    fn test_parse_string_literal() {
        let q = JsonQuery::parse("\"hi\"").unwrap();
        assert_eq!(q.operators, vec![Operator::LiteralStr("hi".to_string())]);
    }

    // ──────────────────────────────────────────────────────────────────
    // Built-in execution tests (via `execute` with assignment queries)
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_builtin_now() {
        let q = JsonQuery::parse("x = now()").unwrap();
        let out = q.execute(&json!({"now": 1000}));
        assert_eq!(out, vec![json!({"x": 1000})]);
    }

    #[test]
    fn test_builtin_ts() {
        let q = JsonQuery::parse("x = ts()").unwrap();
        let out = q.execute(&json!({"ts": 2000}));
        assert_eq!(out, vec![json!({"x": 2000})]);
    }

    #[test]
    fn test_builtin_interval() {
        let q = JsonQuery::parse("x = interval()").unwrap();
        let out = q.execute(&json!({"interval": 60}));
        assert_eq!(out, vec![json!({"x": 60})]);
    }

    #[test]
    fn test_builtin_attr() {
        let q = JsonQuery::parse("x = attr(\"region\")").unwrap();
        let out = q.execute(&json!({"attributes": {"region": "us"}}));
        assert_eq!(out, vec![json!({"x": "us"})]);
    }

    #[test]
    fn test_builtin_sub_secs() {
        let q = JsonQuery::parse("x = sub_secs(100, 30)").unwrap();
        let out = q.execute(&json!({}));
        assert_eq!(out, vec![json!({"x": 70})]);
    }

    #[test]
    fn test_builtin_add_secs() {
        let q = JsonQuery::parse("x = add_secs(100, 30)").unwrap();
        let out = q.execute(&json!({}));
        assert_eq!(out, vec![json!({"x": 130})]);
    }

    #[test]
    fn test_builtin_arith() {
        let q1 = JsonQuery::parse("x = add(1.5, 2.5)").unwrap();
        assert_eq!(q1.execute(&json!({})), vec![json!({"x": 4.0})]);
        let q2 = JsonQuery::parse("x = sub(10, 3)").unwrap();
        assert_eq!(q2.execute(&json!({})), vec![json!({"x": 7.0})]);
        let q3 = JsonQuery::parse("x = mul(3, 4)").unwrap();
        assert_eq!(q3.execute(&json!({})), vec![json!({"x": 12.0})]);
        let q4 = JsonQuery::parse("x = div(10, 4)").unwrap();
        assert_eq!(q4.execute(&json!({})), vec![json!({"x": 2.5})]);
    }

    #[test]
    fn test_builtin_casts() {
        let q1 = JsonQuery::parse("x = int(\"42\")").unwrap();
        assert_eq!(q1.execute(&json!({})), vec![json!({"x": 42})]);
        let q2 = JsonQuery::parse("x = float(3)").unwrap();
        assert_eq!(q2.execute(&json!({})), vec![json!({"x": 3.0})]);
        let q3 = JsonQuery::parse("x = str(123)").unwrap();
        assert_eq!(q3.execute(&json!({})), vec![json!({"x": "123"})]);
    }

    #[test]
    fn test_builtin_nested() {
        let q = JsonQuery::parse("x = sub_secs(add_secs(ts(), 30), interval())").unwrap();
        let out = q.execute(&json!({"ts": 1000, "interval": 60}));
        assert_eq!(out, vec![json!({"x": 970})]);
    }

    // ──────────────────────────────────────────────────────────────────
    // Assignment / bindings tests
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_assignment_single() {
        let q = JsonQuery::parse("x = 42").unwrap();
        assert_eq!(q.execute(&json!({})), vec![json!({"x": 42})]);
    }

    #[test]
    fn test_assignment_multi() {
        let q = JsonQuery::parse("from = sub_secs(ts(), interval()), to = ts()").unwrap();
        let out = q.execute(&json!({"ts": 1000, "interval": 60}));
        assert_eq!(out, vec![json!({"from": 940, "to": 1000})]);
    }

    #[test]
    fn test_assignment_payload_path() {
        let q = JsonQuery::parse("meta = payload.region").unwrap();
        let out = q.execute(&json!({"payload": {"region": "us"}}));
        assert_eq!(out, vec![json!({"meta": "us"})]);
    }

    #[test]
    fn test_assignment_missing_value_skipped() {
        let q = JsonQuery::parse("x = sub_secs(ts(), interval())").unwrap();
        let out = q.execute(&json!({"ts": 1000}));
        // interval missing → builtin returns None → value not inserted.
        assert_eq!(out, vec![json!({})]);
    }

    // ──────────────────────────────────────────────────────────────────
    // Error cases
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_unknown_builtin_returns_none() {
        let q = JsonQuery::parse("x = nonexistent()").unwrap();
        let out = q.execute(&json!({}));
        assert_eq!(out, vec![json!({})]);
    }

    #[test]
    fn test_wrong_arity_returns_none() {
        let q = JsonQuery::parse("x = sub_secs(1)").unwrap();
        let out = q.execute(&json!({}));
        assert_eq!(out, vec![json!({})]);
    }

    // ──────────────────────────────────────────────────────────────────
    // `eval` is internal — sanity-check it via the public `execute`.
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn test_assignment_returns_single_object_vec() {
        let q = JsonQuery::parse("a = 1, b = 2").unwrap();
        let out = q.execute(&json!({}));
        assert_eq!(out.len(), 1);
        assert!(out[0].is_object());
    }

    #[test]
    fn test_legacy_path_returns_vec_of_values() {
        let data = json!([1, 2, 3]);
        let q = JsonQuery::parse("[]").unwrap();
        let out = q.execute(&data);
        assert_eq!(out.len(), 3);
    }
}
