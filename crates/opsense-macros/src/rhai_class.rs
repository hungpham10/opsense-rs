//! Proc-macro `#[rhai_class]` — khai binding Rhai theo kiểu typetag.
//!
//! Nhân bản pattern `configurable_component.rs`: đọc struct, validate
//! constructor ở compile-time, sinh **một block registration duy nhất**
//! (constructor + toàn bộ accessor). Mục tiêu 3 blocker gốc của
//! `opsense-rhai/src/tools.rs`:
//!   1. Dead API  → constructor bắt buộc + đủ accessor → script luôn tạo
//!      được instance và đọc được dữ liệu.
//!   2. Dup-panic → một block duy nhất → hết đăng ký trùng khi build engine.
//!   3. Thiếu accessor → list `accessors(...)` khai tường minh trong attr.
//!
//! Cách dùng:
//! ```ignore
//! #[rhai_class(
//!     constructor = "transition_analysis", // tên hàm script (Rust method: new)
//!     accessors("num_buckets", "total_from", "has_transitions_from", "grid")
//! )]
//! pub struct TransitionAnalysis { /* ... */ }
//! ```
//!
//! Generated code tham chiếu hợp đồng `crate::script::RhaiBindings` +
//! `rhai::Engine` — crate dùng resolve tại Phase 2 (`opsense-components`).
//! Constructor không có → `Self::new` không resolve được → **compile error ở
//! phía người dùng** (enforcement ngay khi build crates).

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Ident, LitStr, Meta, Token,
    parse::{Parse, ParseStream, Parser},
    punctuated::Punctuated,
    ExprClosure,
};

/// Tham số đọc từ attr `#[rhai(...)]`.
struct RhaiAttr {
    /// Tên hàm script của constructor (default: `"new"`).
    constructor: String,
    /// Tự động sinh constructor từ `Default::default()` nếu struct có `#[derive(Default)]`.
    default_constructor: bool,
    /// Danh sách tên hàm accessor (tên script = tên Rust method) hoặc
    /// mapping `"name" -> closure` cho accessor phức tạp.
    accessors: Vec<AccessorSpec>,
}

#[derive(Debug)]
enum AccessorSpec {
    /// Bare name → `engine.register_fn("name", Self::name)`
    FnRef(LitStr),
    /// `"name" -> |...| ...` closure → `engine.register_fn("name", closure)`
    Closure(LitStr, ExprClosure),
}

impl Default for RhaiAttr {
    fn default() -> Self {
        Self {
            constructor: "new".to_string(),
            default_constructor: false,
            accessors: Vec::new(),
        }
    }
}

impl Parse for AccessorSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: LitStr = input.parse()?;
        if input.peek(Token![->]) {
            input.parse::<Token![->]>()?;
            let closure: ExprClosure = input.parse()?;
            Ok(AccessorSpec::Closure(name, closure))
        } else {
            Ok(AccessorSpec::FnRef(name))
        }
    }
}

/// Parse attr `#[rhai(...)]` sử dụng syn.
fn parse_rhai_attr(attr: TokenStream) -> syn::Result<RhaiAttr> {
    let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
    let metas = parser.parse2(attr)?;

    let mut out = RhaiAttr::default();

    for meta in metas {
        match meta {
            Meta::NameValue(nv) if nv.path.is_ident("constructor") => {
                if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(s), .. }) = nv.value {
                    out.constructor = s.value();
                } else {
                    return Err(syn::Error::new_spanned(
                        nv.value,
                        "`constructor` cần dạng `constructor = \"tên_hàm\"`",
                    ));
                }
            }
            Meta::NameValue(nv) if nv.path.is_ident("default_constructor") => {
                if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Bool(b), .. }) = nv.value {
                    out.default_constructor = b.value();
                } else {
                    return Err(syn::Error::new_spanned(
                        nv.value,
                        "`default_constructor` cần dạng `default_constructor = true/false`",
                    ));
                }
            }
            Meta::List(list) if list.path.is_ident("accessors") => {
                let content = list.parse_args_with(
                    Punctuated::<AccessorSpec, Token![,]>::parse_terminated,
                )?;
                out.accessors = content.into_iter().collect();
                if out.accessors.is_empty() {
                    return Err(syn::Error::new_spanned(
                        list,
                        "`accessors(...)` không được rỗng — liệt kê ít nhất 1 method",
                    ));
                }
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    meta,
                    "không hiểu attr `#[rhai(...)]` — chỉ hỗ trợ \
                      `constructor = \"name\"` và/hoặc `accessors(\"a\", \"b\")` / `accessors(\"a\" -> closure, ...)`",
                ));
            }
        }
    }

    if out.constructor.is_empty() && !out.default_constructor {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "`constructor` không được rỗng — khai constructor = \"tên_hàm\" hoặc bật default_constructor = true",
        ));
    }

    Ok(out)
}

/// Lõi macro `#[rhai_class]` — hàm thuần token-transform (test được như hàm thường).
pub fn rhai_class_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = match syn::parse2::<DeriveInput>(item.clone()) {
        Ok(input) => input,
        Err(e) => return e.to_compile_error(),
    };
    let name = &input.ident;

    // 1. Chỉ struct (không enum/union) — y như configurable_component.
    match &input.data {
        Data::Struct(_) => {}
        _ => {
            return syn::Error::new(
                input.ident.span(),
                "`#[rhai_class]` chỉ áp dụng lên struct (bindings là dữ liệu + accessor)",
            )
            .to_compile_error();
        }
    }

    // 2. Parse attr → `constructor` script-name + list `accessors`.
    let parsed = match parse_rhai_attr(attr) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error(),
    };

    // 2b. Validate default_constructor: struct must have #[derive(Default)]
    if parsed.default_constructor {
        let has_default = input.attrs.iter().any(|attr| {
            attr.path().is_ident("derive") && attr.meta.require_list().is_ok_and(|list| {
                list.tokens.to_string().contains("Default")
            })
        });
        if !has_default {
            return syn::Error::new(
                name.span(),
                "`default_constructor = true` yêu cầu struct có `#[derive(Default)]`",
            )
            .to_compile_error();
        }
    }

    // 3. Xây registration statements.
    let constructor_lit = LitStr::new(&parsed.constructor, name.span());
    let constructor_ident = Ident::new(&parsed.constructor, name.span());

    let mut accessor_stmts = Vec::new();

    // Thêm default constructor nếu được bật
    if parsed.default_constructor {
        let default_constructor_name = if parsed.constructor.is_empty() {
            "new".to_string()
        } else {
            parsed.constructor.clone()
        };
        let default_constructor_lit = LitStr::new(&default_constructor_name, name.span());
        accessor_stmts.insert(0, quote! {
            engine.register_fn(#default_constructor_lit, || -> #name { Self::default() });
        });
    }

    for spec in parsed.accessors {
        match spec {
            AccessorSpec::FnRef(lit) => {
                let id = Ident::new(&lit.value(), lit.span());
                accessor_stmts.push(quote! {
                    engine.register_fn(#lit, Self::#id);
                });
            }
            AccessorSpec::Closure(lit, closure) => {
                accessor_stmts.push(quote! {
                    engine.register_fn(#lit, #closure);
                });
            }
        }
    }

    // 4. Sinh MỘT block registration duy nhất KÈM struct gốc.
    //
    // Hợp đồng với `crate::script::RhaiBindings` (Phase 2 opsense-components):
    //   trait RhaiBindings {
    //       fn register(engine: &mut rhai::Engine);
    //   }
    let impl_block = if parsed.default_constructor {
        quote! {
            #[automatically_derived]
            impl crate::script::RhaiBindings for #name {
                fn register(engine: &mut rhai::Engine) {
                    // Một block duy nhất — không bao giờ duplicate-panic.
                    engine.register_type::<#name>();
                    #(#accessor_stmts)*
                }
            }
        }
    } else {
        quote! {
            #[automatically_derived]
            impl crate::script::RhaiBindings for #name {
                fn register(engine: &mut rhai::Engine) {
                    // Một block duy nhất — không bao giờ duplicate-panic.
                    engine.register_type::<#name>();
                    #(#accessor_stmts)*
                    engine.register_fn(#constructor_lit, Self::#constructor_ident);
                }
            }
        }
    };

    // Trả về struct gốc + impl block
    quote! {
        #item
        #impl_block
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// Struct giả lập kiểu `TransitionAnalysis` trong opsense-mlib.
    const SAMPLE: &str = r#"
        pub struct TransitionAnalysis {
            id: String,
            points: Vec<f64>,
        }
    "#;

    fn call(attr: &str, item: &str) -> String {
        let attr_ts = TokenStream::from_str(attr).unwrap();
        let item_ts = TokenStream::from_str(item).unwrap();
        rhai_class_impl(attr_ts, item_ts).to_string()
    }

    /// Bỏ khoảng trắng để so sánh token một cách bền vững (Display của
    /// TokenStream chèn khoảng trắng quanh `::`, `.` v.v.).
    fn norm(s: &str) -> String {
        s.chars().filter(|c| !c.is_whitespace()).collect()
    }

    fn has(needle: &str, hay: &str) -> bool {
        norm(hay).contains(&norm(needle))
    }

    /// 1. Attr đầy đủ → đúng MỘT block đăng ký: register_type 1 lần,
    ///    constructor + đủ accessor. Không duplicate.
    #[test]
    fn sinh_mot_block_dang_ky_day_du() {
        let s = call(
            r#"constructor = "transition_analysis", accessors("num_buckets", "has_transitions_from")"#,
            SAMPLE,
        );
        assert_eq!(norm(&s).matches("implcrate::script::RhaiBindingsforTransitionAnalysis").count(), 1);
        assert_eq!(norm(&s).matches("register_type::<TransitionAnalysis>").count(), 1);
        assert!(has("engine.register_fn(\"transition_analysis\", Self::transition_analysis)", &s));
        assert!(has("engine.register_fn(\"num_buckets\", Self::num_buckets)", &s));
        assert!(has("engine.register_fn(\"has_transitions_from\", Self::has_transitions_from)", &s));
        // Không có accessor nào bị trùng.
        assert_eq!(norm(&s).matches("register_fn(\"num_buckets\"").count(), 1);
    }

    /// 2. Constructor mặc định `new` khi attr chỉ có accessors.
    #[test]
    fn sinh_constructor_mac_dinh_new() {
        let s = call(r#"accessors("grid")"#, SAMPLE);
        assert!(has("engine.register_fn(\"new\", Self::new)", &s));
        assert!(has("engine.register_fn(\"grid\", Self::grid)", &s));
    }

    /// 3. Attr rỗng `#[rhai_class]` → block tối thiểu: type + constructor `new`.
    #[test]
    fn sinh_block_toi_thieu_attr_rong() {
        let s = call("", SAMPLE);
        assert!(has("engine.register_type::<TransitionAnalysis>", &s));
        assert!(has("engine.register_fn(\"new\", Self::new)", &s));
    }

    /// 4. Struct (không enum) là bắt buộc — enum truyền vào phải báo lỗi.
    #[test]
    fn tu_choi_enum() {
        let s = call("", "pub enum NotStruct { A, B }");
        assert!(has("chỉ áp dụng lên struct", &s));
        assert!(has("compile_error", &s));
    }

    /// 5. Constructor rỗng → error khai báo.
    #[test]
    fn constructor_rong_bao_loi() {
        let s = call(r#"constructor = """#, SAMPLE);
        eprintln!("=== CTOR ERR OUTPUT ===\n{s}\n=== END ===");
        assert!(has("constructor` không được rỗng", &s));
        assert!(has("compile_error", &s));
    }

    /// 6. Attr không hiểu (garbage) → error.
    #[test]
    fn attr_garbage_bao_loi() {
        let s = call("garbage", SAMPLE);
        eprintln!("=== GARBAGE ERR OUTPUT ===\n{s}\n=== END ===");
        assert!(has("không hiểu attr", &s));
        assert!(has("compile_error", &s));
    }

    /// 7. `accessors()` rỗng → error.
    #[test]
    fn accessors_rong_bao_loi() {
        let s = call("accessors()", SAMPLE);
        assert!(has("`accessors(...)` không được rỗng", &s));
        assert!(has("compile_error", &s));
    }

    /// 8. Closure accessor được sinh đúng.
    #[test]
    fn closure_accessor_sinh_dung() {
        let s = call(
            r#"accessors("num_cells" -> |g: &mut Self| -> i64 { g.num_cells() as i64 })"#,
            SAMPLE,
        );
        assert!(has("engine.register_fn(\"num_cells\", |g: &mut Self| -> i64 { g.num_cells() as i64 })", &s));
    }

    /// 9. Default constructor từ `#[derive(Default)]`.
    #[test]
    fn default_constructor_tu_derive_default() {
        const SAMPLE_DEFAULT: &str = r#"
            #[derive(Default)]
            pub struct GridConfig {
                step: f64,
                max_bit: usize,
            }
        "#;
        let s = call(
            r#"default_constructor = true, accessors("step")"#,
            SAMPLE_DEFAULT,
        );
        assert!(has("engine.register_fn(\"new\", || -> GridConfig { Self::default() })", &s));
        assert!(has("engine.register_fn(\"step\", Self::step)", &s));
        // Không register explicit constructor
        assert!(!has("engine.register_fn(\"new\", Self::new)", &s));
    }

    /// 10. Default constructor error khi thiếu #[derive(Default)].
    #[test]
    fn default_constructor_loi_thieu_derive() {
        let s = call(r#"default_constructor = true"#, SAMPLE);
        eprintln!("=== OUTPUT ===\n{s}\n=== END ===");
        // Check for key substrings that appear in the compile_error message
        assert!(has("yêu cầu struct có", &s));
        assert!(has("derive(Default)", &s));
        assert!(has("compile_error", &s));
    }
}