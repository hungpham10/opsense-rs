//! Value casting shared by the declarative extractors (`json_2_json`, the
//! HTTP node's jq field mapping, ...): turn whatever JSON a query picked out
//! into the declared target type.

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CastType {
    U64,
    U32,
    I64,
    I32,
    F64,
    F32,
    String,
    Bool,
}

/// Cast `val` to `target_type`; `None` means "cannot cast" — callers decide
/// whether that skips the item or the whole batch.
#[must_use]
pub fn cast_value(val: Value, target_type: &CastType) -> Option<Value> {
    match target_type {
        CastType::U64 => match val {
            Value::Number(n) => n.as_u64().map(|v| Value::Number(Number::from(v))),
            Value::String(s) => s
                .trim()
                .parse::<u64>()
                .ok()
                .map(|v| Value::Number(Number::from(v))),
            Value::Bool(b) => Some(Value::Number(Number::from(if b { 1u64 } else { 0u64 }))),
            _ => None,
        },
        CastType::U32 => match val {
            Value::Number(n) => n
                .as_u64()
                .and_then(|v| u32::try_from(v).ok())
                .map(|v| Value::Number(Number::from(v))),
            Value::String(s) => s
                .trim()
                .parse::<u32>()
                .ok()
                .map(|v| Value::Number(Number::from(v))),
            Value::Bool(b) => Some(Value::Number(Number::from(if b { 1u32 } else { 0u32 }))),
            _ => None,
        },
        CastType::I64 => match val {
            Value::Number(n) => n.as_i64().map(|v| Value::Number(Number::from(v))),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .ok()
                .map(|v| Value::Number(Number::from(v))),
            Value::Bool(b) => Some(Value::Number(Number::from(if b { 1i64 } else { -0i64 }))),
            _ => None,
        },
        CastType::I32 => match val {
            Value::Number(n) => n
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(|v| Value::Number(Number::from(v))),
            Value::String(s) => s
                .trim()
                .parse::<i32>()
                .ok()
                .map(|v| Value::Number(Number::from(v))),
            Value::Bool(b) => Some(Value::Number(Number::from(if b { 1i32 } else { 0i32 }))),
            _ => None,
        },
        CastType::F64 => match val {
            Value::Number(n) => n.as_f64().map(|v| {
                Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .ok()
                .and_then(|v| Number::from_f64(v).map(Value::Number)),
            Value::Bool(b) => Number::from_f64(if b { 1.0 } else { 0.0 }).map(Value::Number),
            _ => None,
        },
        CastType::F32 => match val {
            Value::Number(n) => n.as_f64().map(|v| {
                Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }),
            Value::String(s) => s
                .trim()
                .parse::<f32>()
                .ok()
                .and_then(|v| Number::from_f64(v as f64).map(Value::Number)),
            Value::Bool(b) => Number::from_f64(if b { 1.0 } else { 0.0 }).map(Value::Number),
            _ => None,
        },
        CastType::String => match val {
            Value::String(s) => Some(Value::String(s)),
            Value::Number(n) => Some(Value::String(n.to_string())),
            Value::Bool(b) => Some(Value::String(b.to_string())),
            Value::Null => Some(Value::String("null".to_string())),
            _ => Some(Value::String(val.to_string())),
        },
        CastType::Bool => match val {
            Value::Bool(b) => Some(Value::Bool(b)),
            Value::String(s) => match s.trim().to_lowercase().as_str() {
                "true" | "1" | "on" | "yes" => Some(Value::Bool(true)),
                "false" | "0" | "off" | "no" => Some(Value::Bool(false)),
                _ => None,
            },
            Value::Number(n) => Some(Value::Bool(n.as_f64().is_some_and(|v| v != 0.0))),
            Value::Null => Some(Value::Bool(false)),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn casts_between_common_types() {
        assert_eq!(cast_value(json!("32.5"), &CastType::F64), Some(json!(32.5)));
        assert_eq!(
            cast_value(json!("1700000000"), &CastType::I64).unwrap(),
            json!(1_700_000_000i64)
        );
        assert_eq!(cast_value(json!(1), &CastType::String), Some(json!("1")));
        assert_eq!(cast_value(json!("yes"), &CastType::Bool), Some(json!(true)));
        assert_eq!(cast_value(json!(null), &CastType::F64), None);
    }
}
