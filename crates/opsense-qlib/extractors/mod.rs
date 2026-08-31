//! Feature extractors — transform raw candle data into model input vectors.
//!
//! Each extractor implements the [`Extractor`] trait defined in the parent module.
//! Extractors are composable: a model can chain multiple extractors to build its
//! full feature vector.

pub mod ohlcv;

pub use ohlcv::OhlcvExtractor;
