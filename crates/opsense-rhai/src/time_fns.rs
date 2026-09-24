//! Time helper functions for Rhai scripts.

use opsense_macros::rhai_func;

mod inner {
    use super::*;

    #[rhai_func]
    fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }
}

/// Register all time functions with the given engine.
pub fn register(engine: &mut rhai::Engine) {
    // The #[rhai_func] macro submits functions to inventory
    for f in inventory::iter::<crate::rhai_collect::RhaiFreeFn> {
        (f.register)(engine);
    }
}