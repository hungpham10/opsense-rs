use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashSet;
use syn::{
    Data, DeriveInput, Meta, Token, parse_macro_input, punctuated::Punctuated, spanned::Spanned,
};

pub enum ComponentType {
    Source,
    Sink,
    Transform,
    Input,
    Output,
}

fn to_snake_case(s: &str) -> String {
    let mut snake = String::new();
    let chars: Vec<char> = s.chars().collect();

    for i in 0..chars.len() {
        let ch = chars[i];

        if i > 0 {
            let prev = chars[i - 1];
            let is_caps_boundary = ch.is_uppercase();
            let is_numeric_boundary = (prev.is_numeric() && ch.is_alphabetic())
                || (prev.is_alphabetic() && ch.is_numeric());

            if (is_caps_boundary || is_numeric_boundary) && prev != '_' && ch != '_' {
                snake.push('_');
            }
        }

        if ch.is_uppercase() {
            snake.push(ch.to_ascii_lowercase());
        } else {
            snake.push(ch);
        }
    }
    snake
}

pub fn configurable_component_impl(
    attr: TokenStream,
    item: TokenStream,
    kind: ComponentType,
    requirements: Vec<&str>,
) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;

    // 1. Parse Attribute Arguments (e.g., derive(Clone), exclude(Debug, PartialEq),
    //    sea_orm)
    let mut extra_derives = Vec::new();
    let mut excluded_derives = HashSet::new();
    let mut sea_orm_opt_in = false;

    if !attr.is_empty() {
        let attr_args =
            parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);

        for meta in attr_args {
            if meta.path().is_ident("derive") {
                if let Meta::List(list) = meta {
                    let nested = list
                        .parse_args_with(Punctuated::<syn::Path, Token![,]>::parse_terminated)
                        .expect("Failed to parse derive content");
                    for path in nested {
                        extra_derives.push(path);
                    }
                }
            } else if meta.path().is_ident("exclude")
                && let Meta::List(list) = meta
            {
                let nested = list
                    .parse_args_with(Punctuated::<syn::Path, Token![,]>::parse_terminated)
                    .expect("Failed to parse exclude content");
                for path in nested {
                    if let Some(ident) = path.get_ident() {
                        excluded_derives.insert(ident.to_string());
                    }
                }
            } else if meta.path().is_ident("sea_orm") {
                // Opt-in via attribute (e.g. `#[sink(sea_orm)]`) rather than a
                // macro-crate feature flag: a `cfg!(feature = ...)` inside a
                // proc-macro is evaluated against the *macro* crate, not the
                // caller, so it cannot correctly gate caller output.
                sea_orm_opt_in = true;
            }
        }
    }

    // 2. Validate Required Fields
    let fields = if let Data::Struct(ref data) = input.data {
        &data.fields
    } else {
        return syn::Error::new(input.span(), "Struct is required to use with this macro")
            .to_compile_error()
            .into();
    };

    let existing_fields = fields
        .iter()
        .filter_map(|f| f.ident.as_ref().map(|id| id.to_string()))
        .collect::<HashSet<_>>();

    let missing_fields = requirements
        .iter()
        .filter(|&&req| !existing_fields.contains(req))
        .map(|&req| req.to_string())
        .collect::<Vec<_>>();

    if !missing_fields.is_empty() {
        return syn::Error::new(
            name.span(),
            format!(
                "Struct '{}' missing required fields: [{}]",
                name,
                missing_fields.join(", ")
            ),
        )
        .to_compile_error()
        .into();
    }

    // 3. Generate Output
    let component_name_str = to_snake_case(&(name.to_string()));
    let macro_name = quote::format_ident!("impl_{}", component_name_str);

    let mut raw_base_derives = vec![
        ("Serialize", quote! { serde::Serialize }),
        ("Deserialize", quote! { serde::Deserialize }),
        ("Clone", quote! { std::clone::Clone }),
        ("Debug", quote! { std::fmt::Debug }),
        ("PartialEq", quote! { std::cmp::PartialEq }),
    ];

    if sea_orm_opt_in {
        raw_base_derives.push(("FromQueryResult", quote! { sea_orm::FromQueryResult }));
    }

    let mut base_derives = Vec::new();
    for (name_str, derive_tokens) in raw_base_derives {
        if !excluded_derives.contains(name_str) {
            base_derives.push(derive_tokens);
        }
    }

    let extra_attributes = quote! {};

    for extra in extra_derives {
        let extra_str = quote! { #extra }.to_string();

        let is_duplicated = extra_str.contains("Serialize")
            || extra_str.contains("Deserialize")
            || extra_str.contains("Clone")
            || extra_str.contains("Debug")
            || extra_str.contains("PartialEq")
            || (sea_orm_opt_in && extra_str.contains("FromQueryResult"));

        let is_excluded = extra
            .get_ident()
            .is_some_and(|id| excluded_derives.contains(&id.to_string()));

        if !is_duplicated && !is_excluded {
            base_derives.push(quote! { #extra });
        }
    }

    let (ident_impl, type_enum) = match kind {
        ComponentType::Source => (
            quote! {
                fn get_inputs(&self) -> std::option::Option<&std::vec::Vec<std::string::String>> { None }
            },
            quote! { crate::vector::runtime::ComponentType::Source },
        ),
        ComponentType::Sink => (
            quote! {
                fn get_inputs(&self) -> std::option::Option<&std::vec::Vec<std::string::String>> { Some(&self.inputs) }
            },
            quote! { crate::vector::runtime::ComponentType::Sink },
        ),
        ComponentType::Transform => (
            quote! {
                fn get_inputs(&self) -> std::option::Option<&std::vec::Vec<std::string::String>> { Some(&self.inputs) }
            },
            quote! { crate::vector::runtime::ComponentType::Transform },
        ),
        ComponentType::Input => (
            quote! {
                fn get_inputs(&self) -> std::option::Option<&std::vec::Vec<std::string::String>> { None }
            },
            quote! { crate::vector::runtime::ComponentType::Input },
        ),
        ComponentType::Output => (
            quote! {
                fn get_inputs(&self) -> std::option::Option<&std::vec::Vec<std::string::String>> { Some(&self.inputs) }
            },
            quote! { crate::vector::runtime::ComponentType::Output },
        ),
    };

    let output = quote! {
        #[derive(#(#base_derives),*)]
        #[serde(deny_unknown_fields)]
        #extra_attributes
        #input

        impl Identify for #name {
            fn id(&self) -> std::string::String {
                self.id.clone()
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            #ident_impl

            fn component_type(&self) -> crate::vector::runtime::ComponentType {
                #type_enum
            }

            fn compare(&self, other: &dyn Component) -> bool {
                if let Some(other_concrete) = other.as_any().downcast_ref::<#name>() {
                    self == other_concrete
                } else {
                    false
                }
            }

            // Cập nhật hàm clone() mới trả về Arc theo đúng signature của trait Identify
            fn clone_arc(&self) -> std::sync::Arc<dyn Component> {
                std::sync::Arc::new(std::clone::Clone::clone(self))
            }
        }

        #[macro_export]
        macro_rules! #macro_name {
            ($($tt:tt)*) => {
                #[typetag::serde(name = #component_name_str)]
                #[async_trait::async_trait]
                impl Component for #name {
                    $($tt)*
                }
            };
        }
    };

    TokenStream::from(output)
}
