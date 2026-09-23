//! Proc-macro `#[rhai]` system — khai binding Rhai cho struct & free function.
//!
//! Gồm 3 macro:
//! - `#[rhai_class(...)]` — cho struct (trước gọi là `#[rhai]`)
//! - `#[rhai_func(...)]` — cho free function
//! - `#[rhai_register]` — trên module, collect & sinh `fn register(eng)`
//!
//! Cũng re-export các macro từ `configurable_component`:
//! `#[source]`, `#[sink]`, `#[transform]`, `#[input]`, `#[output]`

mod configurable_component;
mod rhai_class;
mod rhai_func;
mod rhai_register;

use configurable_component::{configurable_component_impl, ComponentType};
use rhai_class::rhai_class_impl;
use rhai_func::rhai_func_impl;
use rhai_register::rhai_register_impl;

use proc_macro::TokenStream;

// ── Re-export configurable_component macros ──

#[proc_macro_attribute]
pub fn sink(attr: TokenStream, item: TokenStream) -> TokenStream {
    configurable_component_impl(attr, item, ComponentType::Sink, vec!["id", "inputs"])
}

#[proc_macro_attribute]
pub fn source(attr: TokenStream, item: TokenStream) -> TokenStream {
    configurable_component_impl(attr, item, ComponentType::Source, vec!["id"])
}

#[proc_macro_attribute]
pub fn transform(attr: TokenStream, item: TokenStream) -> TokenStream {
    configurable_component_impl(attr, item, ComponentType::Transform, vec!["id", "inputs"])
}

#[proc_macro_attribute]
pub fn input(attr: TokenStream, item: TokenStream) -> TokenStream {
    configurable_component_impl(attr, item, ComponentType::Input, vec!["id"])
}

#[proc_macro_attribute]
pub fn output(attr: TokenStream, item: TokenStream) -> TokenStream {
    configurable_component_impl(attr, item, ComponentType::Output, vec!["id", "inputs"])
}

// ── Rhai macros ──

/// Khai binding Rhai cho **struct** — sinh đúng MỘT block registration
/// (constructor bắt buộc + toàn bộ accessor), hết duplicate-panic / dead API.
///
/// ```ignore
/// #[rhai_class(
///     constructor = "transition_analysis",
///     accessors("num_buckets", "total_from", "has_transitions_from", "grid")
/// )]
/// pub struct TransitionAnalysis { /* ... */ }
/// ```
#[proc_macro_attribute]
pub fn rhai_class(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr2 = proc_macro2::TokenStream::from(attr);
    let item2 = proc_macro2::TokenStream::from(item);
    proc_macro::TokenStream::from(rhai_class_impl(attr2, item2))
}

/// Đăng ký **free function** vào Rhai engine.
///
/// Dùng trên `fn` bất kỳ — sẽ được collect bởi `#[rhai_register]` module.
///
/// ```ignore
/// #[rhai_func]                                    // tên script = tên fn
/// fn now_secs() -> i64 { ... }
///
/// #[rhai_func("ts_rate")]                         // override tên script
/// fn ts_rate_impl(points: rhai::Array) -> rhai::Dynamic { ... }
/// ```
#[proc_macro_attribute]
pub fn rhai_func(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr2 = proc_macro2::TokenStream::from(attr);
    let item2 = proc_macro2::TokenStream::from(item);
    proc_macro::TokenStream::from(rhai_func_impl(attr2, item2))
}

/// Trên module: collect tất cả `#[rhai_func]` bên trong và sinh `fn register(eng)`.
///
/// ```ignore
/// #[rhai_register]
/// mod time_fns {
///     #[rhai_func]
///     fn now_secs() -> i64 { ... }
/// }
///
/// // Khi build: time_fns::register(&mut engine) đăng ký hết.
/// ```
#[proc_macro_attribute]
pub fn rhai_register(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr2 = proc_macro2::TokenStream::from(attr);
    let item2 = proc_macro2::TokenStream::from(item);
    proc_macro::TokenStream::from(rhai_register_impl(attr2, item2))
}