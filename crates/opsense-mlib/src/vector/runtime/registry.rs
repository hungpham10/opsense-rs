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
    pub fn build(&self, type_name: &str, cfg: serde_json::Value)
        -> Option<Result<Box<dyn Component>, Error>>
    {
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
pub fn build(type_name: &str, cfg: serde_json::Value)
    -> Option<Result<Box<dyn Component>, Error>>
{
    global().lock().unwrap().build(type_name, cfg)
}

/// Liệt kê type đã đăng ký (cho `validate` / debug).
pub fn types() -> Vec<&'static str> {
    global().lock().unwrap().types()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::runtime::{Component, Message, Outbound};
    compile_error!("TEST MODULE COMPILED");  // This should cause a compile error if the module is compiled
    
    // Dummy component để test registry
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct DummyComponent {
        id: String,
        value: i32,
    }

    impl crate::vector::runtime::Identify for DummyComponent {
        fn id(&self) -> String { self.id.clone() }
        fn get_inputs(&self) -> Option<&Vec<String>> { None }
        fn clone_arc(&self) -> std::sync::Arc<dyn Component> {
            std::sync::Arc::new(self.clone())
        }
        fn as_any(&self) -> &dyn std::any::Any { self }
        fn component_type(&self) -> crate::vector::runtime::ComponentType { crate::vector::runtime::ComponentType::Transform }
        fn is_terminal(&self) -> bool { false }
        fn compare(&self, other: &dyn Component) -> bool {
            other.as_any().downcast_ref::<Self>().map(|o| o == self).unwrap_or(false)
        }
    }

    // Required by typetag for Component trait
    impl DummyComponent {
        fn typetag_name() -> &'static str { "test_dummy" }
        
        fn typetag_deserialize<'de, D>(_deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::de::Deserializer<'de>,
        {
            // For test purposes, we don't actually deserialize from JSON
            // The registry builds components via factory functions, not deserialization
            Err(serde::de::Error::custom("DummyComponent should not be deserialized via typetag in tests"))
        }
    }

    #[async_trait::async_trait]
    impl Component for DummyComponent {
        async fn run(
            &self,
            _id: usize,
            _rx: &mut tokio::sync::mpsc::Receiver<Message>,
            _tx: Outbound,
        ) -> Result<(), std::io::Error> { Ok(()) }
    }

    fn dummy_factory(_name: &str, cfg: serde_json::Value) -> Result<Box<dyn Component>, std::io::Error> {
        let value = cfg.get("value").and_then(|v| v.as_i64()).unwrap_or(42) as i32;
        Ok(Box::new(DummyComponent { id: "test".into(), value }))
    }

    #[test]
    fn register_then_build() {
        let mut reg = ComponentRegistry::default();
        reg.register("test_dummy", dummy_factory);
        let result = reg.build("test_dummy", serde_json::json!({ "value": 123 }));
        assert!(result.is_some());
        let comp = result.unwrap().unwrap();
        let dummy = comp.as_any().downcast_ref::<DummyComponent>().unwrap();
        assert_eq!(dummy.value, 123);
    }

    #[test]
    fn unregistered_returns_none() {
        let reg = ComponentRegistry::default();
        assert!(reg.build("does_not_exist", serde_json::json!({})).is_none());
    }

    #[test]
    fn build_error_is_scoped() {
        let mut reg = ComponentRegistry::default();
        reg.register("fail_type", |_name, _cfg| Err(std::io::Error::new(std::io::ErrorKind::Other, "boom")));
        let result = reg.build("fail_type", serde_json::json!({}));
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
        // Unregistered vẫn trả None, không bị ảnh hưởng
        assert!(reg.build("other_type", serde_json::json!({})).is_none());
    }
}