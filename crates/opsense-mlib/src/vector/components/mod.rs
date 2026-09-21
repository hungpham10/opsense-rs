pub mod capture;
pub mod clock;
pub mod input;
pub mod null;
pub mod output;
pub mod print;

#[cfg(feature = "json")]
pub mod file;

mod converters;
pub use capture::CaptureSink;
pub use converters::{WebSocketClient, WebSocketPolling};

pub fn used() {}
