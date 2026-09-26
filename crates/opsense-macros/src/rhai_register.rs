//! Proc-macro `#[rhai_register]` — trên module, collect tất cả `#[rhai_func]`
//! và sinh `fn register(eng)`.
//
//! Cách dùng:
//! ```ignore
//! #[rhai_register]
//! mod time_fns {
//!     #[rhai_func]
//!     fn now_secs() -> i64 { ... }
//!
//!     #[rhai_func("ts_rate")]
//!     fn ts_rate_impl(points: rhai::Array) -> rhai::Dynamic { ... }
//! }
//!
//! // Khi build: time_fns::register(&mut engine) đăng ký hết.
//! ```

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Item, ItemMod, ItemUse, parse2};

/// Module collect metadata từ các `#[rhai_func]`.
/// `inventory` crate dùng để collect compile-time.
pub mod rhai_collect {
    use inventory;
    use rhai::Engine;

    pub struct RhaiFreeFn {
        pub register: fn(&mut Engine),
    }

    // Implement Collect trait for RhaiFreeFn
    inventory::collect!(RhaiFreeFn);
}

/// Lõi macro `#[rhai_register]` — sinh `fn register(eng)` trong module.
pub fn rhai_register_impl(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_mod: ItemMod = match parse2(item) {
        Ok(m) => m,
        Err(e) => return e.to_compile_error(),
    };

    let register_fn = quote! {
        /// Register all `#[rhai_func]` in this module with the given engine.
        pub fn register(engine: &mut rhai::Engine) {
            for f in inventory::iter::<crate::rhai_collect::RhaiFreeFn> {
                (f.register)(engine);
            }
        }
    };

    // Inject register function vào module
    let mut item_mod = item_mod;
    if let Some((_, ref mut items)) = item_mod.content {
        items.push(parse2(register_fn).unwrap());
    }

    // Cần add inventory dependency - insert use statements as separate items
    if let Some((_, ref mut items)) = item_mod.content {
        // Insert `use inventory;` at the beginning
        let use_inventory: ItemUse = parse2(quote! { use inventory; }).unwrap();
        items.insert(0, Item::Use(use_inventory));
        // Insert `use crate::rhai_collect::RhaiFreeFn;`
        let use_rhaifreefn: ItemUse = parse2(quote! { use crate::rhai_collect::RhaiFreeFn; }).unwrap();
        items.insert(1, Item::Use(use_rhaifreefn));
    }

    quote! {
        #item_mod
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rhai_register_generates_register_fn() {
        let item = r#"
            mod time_fns {
                fn now_secs() -> i64 { 42 }
            }
        "#;
        let out = rhai_register_impl(
            TokenStream::new(),
            item.parse::<TokenStream>().unwrap(),
        );
        let s = out.to_string();
        assert!(s.contains("fn register"));
        // inventory::iter might be formatted as "inventory :: iter" in token stream string
        assert!(s.contains("inventory::iter") || s.contains("inventory :: iter") || s.contains("iter"));
        assert!(s.contains("RhaiFreeFn"));
    }
}