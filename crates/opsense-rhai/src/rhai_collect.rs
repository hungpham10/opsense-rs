//! Rhai function collection using inventory.
//!
//! This module defines the `RhaiFreeFn` type that is used to collect
//! free functions registered with `#[rhai_func]` via the `inventory` crate.

use rhai::Engine;

pub struct RhaiFreeFn {
    pub register: fn(&mut Engine),
}

inventory::collect!(RhaiFreeFn);