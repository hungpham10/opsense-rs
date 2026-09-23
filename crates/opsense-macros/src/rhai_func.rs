//! Proc-macro `#[rhai_func]` — đăng ký free function vào Rhai engine.
//!
//! Dùng trên `fn` bất kỳ. Sẽ được collect bởi `#[rhai_register]` module
//! để sinh `fn register(eng)`.
//
//! Cách dùng:
//! ```ignore
//! #[rhai_func]                                    // tên script = tên fn
//! fn now_secs() -> i64 { ... }
//!
//! #[rhai_func("ts_rate")]                         // override tên script
//! fn ts_rate_impl(points: rhai::Array) -> rhai::Dynamic { ... }
//! ```

use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemFn, LitStr, parse::{Parse, ParseStream}};

/// Tham số đọc từ attr `#[rhai_func(...)]`.
#[derive(Default)]
struct RhaiFuncAttr {
    /// Tên script (optional, default = tên Rust fn).
    name: Option<String>,
}

impl Parse for RhaiFuncAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut out = Self::default();
        if !input.is_empty() {
            let lit: LitStr = input.parse()?;
            out.name = Some(lit.value());
        }
        Ok(out)
    }
}

/// Lõi macro `#[rhai_func]` — attribute macro trên function.
///
/// Sinh metadata để `#[rhai_register]` collect. Không thay đổi function gốc.
pub fn rhai_func_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let parsed = match syn::parse2::<RhaiFuncAttr>(attr) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error(),
    };

    let mut item_fn = match syn::parse2::<ItemFn>(item) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    // Tên script = attr.name hoặc tên function
    let script_name = parsed.name.unwrap_or_else(|| item_fn.sig.ident.to_string());
    let fn_ident = &item_fn.sig.ident;

    // Sinh wrapper registration code
    let wrapper_name = quote::format_ident!("__rhai_register_{}", fn_ident);
    let wrapper_fn = quote! {
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #wrapper_name(engine: &mut rhai::Engine) {
            engine.register_fn(#script_name, #fn_ident);
        }

        // Submit vào inventory
        inventory::submit! {
            crate::rhai_collect::RhaiFreeFn {
                register: #wrapper_name,
            }
        }
    };

    // Trả về function gốc + wrapper
    quote! {
        #item_fn
        #wrapper_fn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rhai_func_default_name() {
        let item = r#"
            fn now_secs() -> i64 {
                42
            }
        "#;
        let out = rhai_func_impl(
            TokenStream::new(),
            item.parse::<TokenStream>().unwrap(),
        );
        let s = out.to_string();
        assert!(s.contains("fn now_secs"));
        assert!(s.contains("__rhai_register_now_secs"));
        // Check for inventory submission (either macro name or type name)
        assert!(s.contains("inventory::submit") || s.contains("RhaiFreeFn"));
    }

    #[test]
    fn rhai_func_custom_name() {
        let item = r#"
            fn ts_rate_impl(points: rhai::Array) -> rhai::Dynamic {
                rhai::Dynamic::UNIT
            }
        "#;
        let out = rhai_func_impl(
            "\"ts_rate\"".parse::<TokenStream>().unwrap(),
            item.parse::<TokenStream>().unwrap(),
        );
        let s = out.to_string();
        assert!(s.contains("fn ts_rate_impl"));
        assert!(s.contains("__rhai_register_ts_rate_impl"));
        assert!(s.contains("\"ts_rate\""));
    }
}