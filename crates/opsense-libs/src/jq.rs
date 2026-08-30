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
    /// Climb one level to the element's parent (path token `^`). Requires
    /// ancestry tracking in `pick`/`execute`, enabling e.g.
    /// `series[].values[].^.^.metric` to reach sibling fields of the array
    /// an item was iterated from.
    Parent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonQuery {
    operators: Vec<Operator>,
}

/// One element in flight during a query: the current value plus the chain of
/// ancestors it was reached through (root first, immediate parent last).
/// `Operator::Parent` pops that chain; every descent pushes the container it
/// matched inside.
struct Frame<V> {
    val: V,
    parents: Vec<V>,
}

impl<V: Clone> Frame<V> {
    fn child(&self, val: V) -> Self {
        let mut parents = self.parents.clone();
        parents.push(self.val.clone());
        Self { val, parents }
    }

    fn parent(&self) -> Option<Self> {
        let mut parents = self.parents.clone();
        let val = parents.pop()?;
        Some(Self { val, parents })
    }
}

impl JsonQuery {
    pub fn new(operators: Vec<Operator>) -> Self {
        Self { operators }
    }

    pub fn parse(path: &str) -> Result<Self, Error> {
        let mut operators = Vec::new();
        let mut chars = path.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '.' => continue,
                '^' => operators.push(Operator::Parent),
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
                }
            }
        }
        Ok(Self { operators })
    }

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

    pub fn execute(&self, data: &Value) -> Vec<Value> {
        let mut collection = vec![Frame {
            val: data.clone(),
            parents: Vec::new(),
        }];

        for op in &self.operators {
            let mut next_collection = Vec::new();
            for item in collection {
                match op {
                    Operator::Match(field) => {
                        if let Some(v) = item.val.get(field) {
                            next_collection.push(item.child(v.clone()));
                        }
                    }
                    Operator::Access(index) => {
                        if let Some(v) = item.val.get(*index) {
                            next_collection.push(item.child(v.clone()));
                        } else if let Some(v) = item.val.get(index.to_string()) {
                            next_collection.push(item.child(v.clone()));
                        }
                    }
                    Operator::Iter => {
                        if let Some(arr) = item.val.as_array() {
                            for v in arr {
                                next_collection.push(item.child(v.clone()));
                            }
                        } else if let Some(obj) = item.val.as_object() {
                            for v in obj.values() {
                                next_collection.push(item.child(v.clone()));
                            }
                        }
                    }
                    // CHỖ NÀY HẾT LỖI: Vì chúng ta trả về Value sở hữu (Owned)
                    Operator::Select(fields) => {
                        let mut new_obj = serde_json::Map::new();
                        for field in fields {
                            if let Some(v) = item.val.get(field) {
                                new_obj.insert(field.clone(), v.clone());
                            }
                        }
                        next_collection.push(Frame {
                            val: Value::Object(new_obj),
                            parents: item.parents,
                        });
                    }
                    Operator::Parent => {
                        if let Some(p) = item.parent() {
                            next_collection.push(p);
                        }
                    }
                }
            }
            collection = next_collection;
            if collection.is_empty() {
                break;
            }
        }
        collection.into_iter().map(|f| f.val).collect()
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

        // Cách 1: Path rỗng (Duyệt hết vòng lặp operators rỗng)
        // Kết quả sẽ trả về chính mảng data trong Vec
        let query_root = JsonQuery::parse("").unwrap();
        let result_root = query_root.execute(&data);

        assert_eq!(result_root.len(), 1);
        assert_eq!(result_root[0], data);
        assert!(result_root[0].is_array());
    }

    #[test]
    fn test_get_array_elements_directly() {
        let data = json!([1, 2, 3]);

        // Cách 2: Sử dụng toán tử Iter "[]" ngay từ đầu
        // Kết quả sẽ trả về Vec chứa tham chiếu đến từng phần tử bên trong
        let query_iter = JsonQuery::parse("[]").unwrap();
        let result_iter = query_iter.execute(&data);

        // Chuyển Vec<Operator> thành chuỗi JSON
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
        // Từ từng phần tử của `values`, `^^` climb qua mảng values về series cha
        let query = JsonQuery::parse("series[].values[].^.^.metric.instance").unwrap();
        assert_eq!(
            query.execute(&data),
            vec![json!("a:1"), json!("a:1"), json!("b:2")]
        );
    }

    #[test]
    fn test_single_parent_from_array_element() {
        // `^` một lần từ phần tử mảng con về đúng mảng chứa nó
        let data = json!({"points": [[1, "x"], [2, "y"]]});
        let query = JsonQuery::parse("points[].^.len").unwrap();
        assert!(query.execute(&data).is_empty()); // mảng không có field "len"
        let query2 = JsonQuery::parse("points[].^").unwrap();
        let result = query2.execute(&data);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], json!([[1, "x"], [2, "y"]]));
    }

    #[test]
    fn test_parent_at_root_is_noop() {
        let data = json!({"a": 1});
        let query = JsonQuery::parse("^.a").unwrap();
        // `^` tại root không có cha để climb → collection rỗng, path chết
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
}
