//! Per-call config lookups: `attr(name)` / `attrs()`.

use opsense_macros::rhai_func;
use rhai::{Dynamic, Map};
use std::collections::BTreeMap;
use std::sync::Arc;

mod inner {
    use super::*;

    #[rhai_func]
    fn attr(name: &str, attributes: Arc<BTreeMap<String, String>>) -> Dynamic {
        attributes
            .get(name)
            .map(|v| Dynamic::from(v.clone()))
            .unwrap_or(Dynamic::UNIT)
    }

    #[rhai_func]
    fn attrs(attributes: Arc<BTreeMap<String, String>>) -> Dynamic {
        let mut map = Map::new();
        for (k, v) in attributes.iter() {
            map.insert(k.clone().into(), Dynamic::from(v.clone()));
        }
        Dynamic::from(map)
    }
}

/// Register `attr` and `attrs` functions with the given attributes.
/// Note: this is a function, not using the generated register, because
/// attributes are per-call.
pub fn register(eng: &mut rhai::Engine, attributes: BTreeMap<String, String>) {
    let attrs = Arc::new(attributes);

    // attr(name) -> value or ()
    let attrs_clone = attrs.clone();
    eng.register_fn("attr", move |name: &str| -> Dynamic {
        attrs_clone
            .get(name)
            .map(|v| Dynamic::from(v.clone()))
            .unwrap_or(Dynamic::UNIT)
    });

    // attrs() -> Map
    let attrs_clone = attrs.clone();
    eng.register_fn("attrs", move || -> Dynamic {
        let mut map = Map::new();
        for (k, v) in attrs_clone.iter() {
            map.insert(k.clone().into(), Dynamic::from(v.clone()));
        }
        Dynamic::from(map)
    });
}