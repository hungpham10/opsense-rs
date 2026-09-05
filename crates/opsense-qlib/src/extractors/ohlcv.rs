//! OHLCV feature extractor.
//!
//! Splits a candle slice into four time-aligned `Vec<f64>` arrays:
//! `[closes, highs, lows, prev_closes]`.  Each array is padded to
//! [`OhlcvExtractor::window`] by repeating the first value so that ONNX
//! models with fixed-size weight tensors receive the expected shape.

use std::io::Error;

use serde::{Deserialize, Serialize};

use schemas::CandleStick;

use crate::qlib::Extractor;

/// Extracts `[closes, highs, lows, prev_closes]` from a candle slice.
///
/// Each array is **padded** to `window` elements by inserting copies of the
/// first value at the front.  This guarantees the ONNX model always sees
/// the expected `[1, window]` shape even when fewer candles are available.
#[derive(Debug, Serialize, Deserialize)]
pub struct OhlcvExtractor {
    /// Target length of each output array.
    pub window: usize,
}

#[typetag::serde]
impl Extractor for OhlcvExtractor {
    fn name(&self) -> &str {
        "ohlcv"
    }

    fn extract(&self, candles: &[CandleStick]) -> Result<Vec<Vec<f64>>, Error> {
        if candles.is_empty() {
            return Err(Error::other("OhlcvExtractor: empty candle slice"));
        }

        let take = self.window.min(candles.len());
        let slice = candles.len() - take;

        let mut closes: Vec<f64> = candles[slice..].iter().map(|c| c.c).collect();
        let mut highs: Vec<f64> = candles[slice..].iter().map(|c| c.h).collect();
        let mut lows: Vec<f64> = candles[slice..].iter().map(|c| c.l).collect();
        let first_close = closes[0];

        // prev_closes = [first, closes[0..take-1]] (shifted by 1)
        let mut prev_closes: Vec<f64> = vec![first_close];
        prev_closes.extend(&closes[..take.saturating_sub(1)]);

        // Pad each array to self.window
        let pad = |v: &mut Vec<f64>, val: f64| {
            while v.len() < self.window {
                v.insert(0, val);
            }
        };
        let c0 = closes[0];
        let h0 = highs[0];
        let l0 = lows[0];
        let p0 = prev_closes[0];
        pad(&mut closes, c0);
        pad(&mut highs, h0);
        pad(&mut lows, l0);
        pad(&mut prev_closes, p0);

        Ok(vec![closes, highs, lows, prev_closes])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_candle(c: f64, h: f64, l: f64) -> CandleStick {
        CandleStick {
            t: 0,
            c,
            h,
            l,
            o: 0.0,
            v: 0.0,
        }
    }

    #[test]
    fn test_extract_pads_to_window() {
        let candles: Vec<_> = (0..10)
            .map(|i| mk_candle(100.0 + i as f64, 101.0 + i as f64, 99.0 + i as f64))
            .collect();
        let ex = OhlcvExtractor { window: 20 };
        let result = ex.extract(&candles).expect("extract");
        assert_eq!(result.len(), 4); // closes, highs, lows, prev_closes
        for arr in &result {
            assert_eq!(arr.len(), 20);
        }
        // First 10 entries should be padded (all same = closes[0])
        assert_eq!(result[0][0], result[0][9]); // padded region
        assert_eq!(result[0][9], result[0][10]); // boundary
    }

    #[test]
    fn test_extract_exact_window_no_padding() {
        let candles: Vec<_> = (0..50)
            .map(|i| mk_candle(100.0 + i as f64, 101.0 + i as f64, 99.0 + i as f64))
            .collect();
        let ex = OhlcvExtractor { window: 50 };
        let result = ex.extract(&candles).expect("extract");
        for arr in &result {
            assert_eq!(arr.len(), 50);
        }
        assert!((result[0][0] - 100.0).abs() < 1e-6);
        assert!((result[0][49] - 149.0).abs() < 1e-6);
    }

    #[test]
    fn test_prev_closes_shifted() {
        let candles: Vec<_> = (0..5)
            .map(|i| mk_candle(10.0 + i as f64, 11.0 + i as f64, 9.0 + i as f64))
            .collect();
        let ex = OhlcvExtractor { window: 5 };
        let result = ex.extract(&candles).expect("extract");
        let prev = &result[3]; // prev_closes
        // prev_closes[0] should equal closes[0] (no prior candle)
        assert!((prev[0] - result[0][0]).abs() < 1e-6);
        // prev_closes[i] should equal closes[i-1]
        for i in 1..5 {
            assert!((prev[i] - result[0][i - 1]).abs() < 1e-6);
        }
    }

    #[test]
    fn test_empty_candles_returns_error() {
        let ex = OhlcvExtractor { window: 10 };
        assert!(ex.extract(&[]).is_err());
    }
}
