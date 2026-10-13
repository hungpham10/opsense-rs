//! Runtime component registry — bổ sung cho typetag, không thay thế.
//!
//! typetag (`Component` trait) đăng ký lúc **link**; registry này đăng ký lúc
//! **chạy**. `pipeline_from_config` thử registry trước, rồi mới fallback typetag.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use super::{Component, IoError as Error};

/// Dựng component từ JSON config. `type_name` là key (vd `"xt_pnl"`).
/// Trả `Err` khi type đã đăng ký nhưng config hỏng — để lỗi chỉ đúng node đó,
/// không làm sập toàn pipeline.
pub type ComponentFactory =
    fn(type_name: &str, cfg: serde_json::Value) -> Result<Box<dyn Component>, std::io::Error>;

#[derive(Default)]
pub struct ComponentRegistry {
    factories: HashMap<&'static str, ComponentFactory>,
}

impl ComponentRegistry {
    /// Đăng ký một factory cho type_name.
    /// Panic nếu type_name đã tồn tại (tránh ghi đè im lặng).
    pub fn register(&mut self, type_name: &'static str, factory: ComponentFactory) -> &mut Self {
        if self.factories.contains_key(type_name) {
            panic!("component type `{type_name}` đã được đăng ký trong registry");
        }
        self.factories.insert(type_name, factory);
        self
    }

    /// Thử dựng component. Trả:
    /// - `Some(Ok(comp))`  : type đã đăng ký, config hợp lệ
    /// - `Some(Err(e))`    : type đã đăng ký, config hỏng (chỉ node này fail)
    /// - `None`            : type chưa đăng ký → caller fallback typetag
    pub fn build(
        &self,
        type_name: &str,
        cfg: serde_json::Value,
    ) -> Option<Result<Box<dyn Component>, Error>> {
        self.factories.get(type_name).map(|f| f(type_name, cfg))
    }

    /// Liệt kê type đã đăng ký (cho `validate` / debug).
    pub fn types(&self) -> Vec<&'static str> {
        self.factories.keys().copied().collect()
    }
}

/// Registry toàn cục — dùng cho built-in + plugin. `OnceLock` đảm bảo init idempotent.
fn global() -> &'static Mutex<ComponentRegistry> {
    static ONCE: OnceLock<Mutex<ComponentRegistry>> = OnceLock::new();
    ONCE.get_or_init(|| Mutex::new(ComponentRegistry::default()))
}

/// Đăng ký vào registry toàn cục — gọi từ component crate, host, hoặc module .so.
pub fn register(type_name: &'static str, factory: super::ComponentFactory) {
    global().lock().unwrap().register(type_name, factory);
}

/// Thử dựng component từ registry toàn cục.
pub fn build(type_name: &str, cfg: serde_json::Value) -> Option<Result<Box<dyn Component>, Error>> {
    global().lock().unwrap().build(type_name, cfg)
}

/// Liệt kê type đã đăng ký (cho `validate` / debug).
pub fn types() -> Vec<&'static str> {
    global().lock().unwrap().types()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Factory không cần dựng component thật — chỉ cần một `Component` type là
    /// đủ cho chữ ký, nên ta trả `Err` và test tập trung vào logic registry.
    fn failing_factory(_name: &str, _cfg: serde_json::Value) -> Result<Box<dyn Component>, Error> {
        Err(Error::other("boom"))
    }

    #[test]
    fn build_unregistered_returns_none() {
        let reg = ComponentRegistry::default();
        assert!(reg.build("khong_co", serde_json::json!({})).is_none());
    }

    #[test]
    fn build_propagates_factory_error() {
        let mut reg = ComponentRegistry::default();
        reg.register("fail_type", failing_factory);
        // Đã đăng ký → `Some`, nhưng factory lỗi → `Err` (chỉ node này chết).
        assert!(
            reg.build("fail_type", serde_json::json!({}))
                .unwrap()
                .is_err()
        );
        // Lỗi không lan sang type khác chưa đăng ký.
        assert!(reg.build("type_khac", serde_json::json!({})).is_none());
    }

    #[test]
    #[should_panic(expected = "đã được đăng ký")]
    fn register_twice_panics() {
        let mut reg = ComponentRegistry::default();
        reg.register("dup", failing_factory);
        reg.register("dup", failing_factory);
    }

    #[test]
    fn types_lists_registered() {
        let mut reg = ComponentRegistry::default();
        reg.register("a", failing_factory);
        assert_eq!(reg.types(), vec!["a"]);
    }
}
