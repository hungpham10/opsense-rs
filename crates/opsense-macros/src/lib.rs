mod configurable_component;

use configurable_component::*;
use proc_macro::TokenStream;

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
