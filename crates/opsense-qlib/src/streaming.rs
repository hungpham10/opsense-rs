//! # StreamingPortfolio — Portfolio chạy realtime theo luồng candle
//!
//! Khác với `Portfolio::forward` (backtest trên mảng candle đã có), struct
//! này giữ toàn bộ state giữa các lần gọi `on_candle`:
//! - `orders` / `history` — lệnh đang mở / đã đóng
//! - `plan` — vec [`TradingGrid`] từ lần rebuild gần nhất
//! - `candle_seq` — thứ tự nến toàn cục (cho T+N settlement)
//! - `candles` — lịch sử nến (rolling) để strategy rebuild
//!
//! Mỗi `on_candle` = một bước của vòng `forward` gốc: check exit → notify
//! `Closed`, rebuild nếu tới hạn → notify `Rebuilt`, evaluate entry → notify
//! `Placed`/`Rejected`. Tái sử dụng `Portfolio::check_order_exit` /
//! `evaluate_grid_entries` để backtest và realtime dùng chung một codepath.

use std::collections::VecDeque;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::candle::CandleStick;
use crate::grid::TradingGrid;
use crate::portfolio::{Order, OrderType, Portfolio};
use crate::{Calendar, Fee, GridSnapshot, OrderEvent, Strategy};

/// Cap lịch sử candle giữ trong RAM cho rebuild. Lookback lớn hơn số này
/// sẽ bị rebuild từ chối (strategy tự quyết định error/ignore).
const HISTORY_CAP: usize = 2048;

/// Engine giao dịch streaming giữ state giữa các candle. `Serialize` giữ
/// config/params; state chạy (`candles`/`orders`/`plan`/...) là runtime-only,
/// không serialize — serialize chỉ dùng để khai báo component.
#[derive(Clone, Serialize, Deserialize)]
pub struct StreamingPortfolio {
    strategy: Arc<dyn Strategy + Sync + Send>,
    fee: Arc<dyn Fee + Sync + Send>,

    kelly_fraction: f64,
    base_capital: f64,
    settlement_candles: u64,

    /// Lịch sử nến rolling — feed cho `Strategy::rebuild`.
    #[serde(skip, default)]
    candles: VecDeque<CandleStick>,

    #[serde(skip, default)]
    orders: Vec<Order>,
    #[serde(skip, default)]
    history: Vec<Order>,
    #[serde(skip, default)]
    plan: Vec<TradingGrid>,
    #[serde(skip, default)]
    review_at: u64,
    #[serde(skip, default)]
    candle_seq: u64,
    #[serde(skip, default)]
    candle_ts: i64,
}

impl std::fmt::Debug for StreamingPortfolio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingPortfolio")
            .field("kelly_fraction", &self.kelly_fraction)
            .field("base_capital", &self.base_capital)
            .field("settlement_candles", &self.settlement_candles)
            .field("open_orders", &self.orders.len())
            .field("closed_orders", &self.history.len())
            .field("grids", &self.plan.len())
            .field("candle_seq", &self.candle_seq)
            .finish()
    }
}

impl PartialEq for StreamingPortfolio {
    fn eq(&self, other: &Self) -> bool {
        self.kelly_fraction == other.kelly_fraction
            && self.base_capital == other.base_capital
            && self.settlement_candles == other.settlement_candles
    }
}

impl StreamingPortfolio {
    pub fn new(
        strategy: Arc<dyn Strategy + Sync + Send>,
        fee: Arc<dyn Fee + Sync + Send>,
        calendar: Arc<dyn Calendar + Sync + Send>,
        kelly_fraction: f64,
        base_capital: f64,
        settlement_candles: u64,
    ) -> Self {
        let settlement = if settlement_candles > 0 {
            settlement_candles
        } else {
            calendar.settlement_candles()
        };
        Self {
            strategy,
            fee,
            kelly_fraction,
            base_capital,
            settlement_candles: settlement,
            candles: VecDeque::with_capacity(HISTORY_CAP),
            orders: Vec::new(),
            history: Vec::new(),
            plan: Vec::new(),
            review_at: 0,
            candle_seq: 0,
            candle_ts: 0,
        }
    }

    pub fn open_orders(&self) -> &[Order] {
        &self.orders
    }

    pub fn closed_orders(&self) -> &[Order] {
        &self.history
    }

    pub fn grids(&self) -> &[TradingGrid] {
        &self.plan
    }

    /// Feed một candle đã đóng vào engine. Trả về các [`OrderEvent`] phát
    /// sinh (Closed → Rebuilt → Placed/Rejected), đúng thứ tự của forward.
    pub async fn on_candle(&mut self, candle: &CandleStick) -> Result<Vec<OrderEvent>, Error> {
        if !candle.t.is_positive() || !candle.c.is_finite() || candle.c <= 0.0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "candle must have positive ts and finite close",
            ));
        }
        if candle.t <= self.candle_ts {
            return Ok(vec![]); // nến cũ/trùng — bỏ qua (idempotent theo ts)
        }

        self.candle_seq += 1;
        let current_seq = self.candle_seq;
        let ts = candle.t.max(0) as u64;
        let fee_rate = self.fee.rate();
        let mut events = Vec::new();

        self.push_history(*candle);

        // 1. Check exits trên các lệnh đang mở → Closed
        let prev_hist_len = self.history.len();
        self.orders.retain_mut(|order| {
            if let Some((exit_price, pnl_pct)) =
                Portfolio::check_order_exit(order, candle, fee_rate, current_seq)
            {
                order.exit_price = Some(exit_price);
                order.pnl_pct = Some(pnl_pct);
                if let Some(grid) = self.plan.get_mut(order.grid_index) {
                    grid.record_trade_outcome(
                        order.level_index,
                        order.dtype == OrderType::Long,
                        pnl_pct,
                    );
                }
                self.history.push(*order);
                false
            } else {
                true
            }
        });
        for order in &self.history[prev_hist_len..] {
            events.push(OrderEvent::Closed { ts, order: *order });
        }

        // 2. Rebuild nếu tới hạn — lịch sử nến cục bộ feed thẳng vào strategy
        if self.review_at <= ts {
            let (next_review, new_plan) = self.rebuild(ts).await?;
            self.review_at = next_review;
            if !new_plan.is_empty() {
                self.plan = new_plan;
            }
            events.push(OrderEvent::Rebuilt {
                ts,
                grids: self
                    .plan
                    .iter()
                    .map(|g| GridSnapshot {
                        levels: g.levels().to_vec(),
                    })
                    .collect(),
            });
        }

        // 3. Evaluate entries trên plan mới/này → Placed / Rejected
        let unlock_seq = current_seq + self.settlement_candles;
        events.extend(Portfolio::evaluate_grid_entries(
            usize::try_from(current_seq).unwrap_or(usize::MAX),
            candle,
            self.plan.as_slice(),
            &mut self.orders,
            fee_rate,
            self.kelly_fraction,
            self.base_capital,
            unlock_seq,
        ));

        self.candle_ts = candle.t;
        Ok(events)
    }

    fn push_history(&mut self, candle: CandleStick) {
        if self.candles.len() == HISTORY_CAP {
            self.candles.pop_front();
        }
        self.candles.push_back(candle);
    }

    /// Force-close mọi lệnh đang mở tại giá close của nến cuối — dùng khi
    /// kết thúc stream/shutdown, tương đương forward dừng giữa chừng.
    /// Trả về các [`OrderEvent::Closed`] phát sinh.
    pub fn flush(&mut self) -> Vec<OrderEvent> {
        let (ts, close) = self
            .candles
            .back()
            .map_or((0, 0.0), |c| (c.t.max(0) as u64, c.c));
        let fee_rate = self.fee.rate();
        let mut events = Vec::new();

        for mut order in std::mem::take(&mut self.orders) {
            let pnl_pct = match order.dtype {
                OrderType::Long if order.entry_price > 0.0 => {
                    (close - order.entry_price) / order.entry_price
                }
                OrderType::Short if order.entry_price > 0.0 => {
                    (order.entry_price - close) / order.entry_price
                }
                _ => 0.0,
            } - 2.0 * fee_rate;
            order.exit_price = Some(close);
            order.pnl_pct = Some(pnl_pct);
            if let Some(grid) = self.plan.get_mut(order.grid_index) {
                grid.record_trade_outcome(
                    order.level_index,
                    order.dtype == OrderType::Long,
                    pnl_pct,
                );
            }
            self.history.push(order);
            events.push(OrderEvent::Closed { ts, order });
        }
        events
    }

    /// Gọi `Strategy::rebuild` với lịch sử nến streaming.
    /// Trả về `(next_review_ts, plan mới)`.
    async fn rebuild(&mut self, ts: u64) -> Result<(u64, Vec<TradingGrid>), Error> {
        let next_review = self.strategy.next(ts).await;
        let fetch_from = ts.saturating_sub(self.rebuild_lookback_secs());
        let params = self.strategy.init();

        let history: Vec<CandleStick> = self.candles.iter().copied().collect();
        let slice_start = history.partition_point(|c| (c.t.max(0) as u64) < fetch_from);
        // Strategy fetch [from, ts) — không gồm nến hiện tại
        let available = history[slice_start..]
            .iter()
            .filter(|c| (c.t.max(0) as u64) < ts)
            .count();
        let enough_data = available >= 10;

        let plan = self
            .strategy
            .rebuild(
                ts,
                self.plan.as_slice(),
                &mut move |from, to| {
                    let snapshot: Vec<CandleStick> = history
                        .iter()
                        .filter(|c| (c.t.max(0) as u64) >= from && (c.t.max(0) as u64) < to)
                        .copied()
                        .collect();
                    Box::pin(async move { Ok(snapshot) })
                },
                &move |id: usize| params.get(id).copied().unwrap_or(0.0),
            )
            .await
            .or_else(|error| {
                // Rebuild fail (chưa đủ data, market closed...) → giữ plan cũ
                if enough_data {
                    Err(error)
                } else {
                    Ok(Vec::new())
                }
            })?;

        Ok((next_review, plan))
    }

    fn rebuild_lookback_secs(&self) -> u64 {
        // Ưu tiên lookback của strategy nếu nó expose qua init() (grid: param 4)
        let params = self.strategy.init();
        if params.len() > 4 && params[4] > 0.0 {
            return params[4] as u64;
        }
        // Mặc định: đủ nến cho analysis grid — 2 ngày theo resolution rebuild
        2 * 24 * 3600
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calendar::CryptoCalendar;
    use crate::fee::SimpleFixedFee;
    use crate::strategies::GridStrategy;

    fn engine(settlement: u64) -> StreamingPortfolio {
        StreamingPortfolio::new(
            Arc::new(GridStrategy::new(4, 0.01, 10.0, 2 * 24 * 3600, 900, 300)),
            Arc::new(SimpleFixedFee::new(0.0005)),
            Arc::new(CryptoCalendar),
            0.25,
            1_000.0,
            settlement,
        )
    }

    fn candle(step: usize, price: f64) -> CandleStick {
        CandleStick::new(
            1_700_000_000 + step as i64 * 300,
            price,
            price + 2.0,
            price - 2.0,
            price + 0.1,
            10.0,
        )
    }

    async fn feed(prices: impl IntoIterator<Item = f64>) -> StreamingPortfolio {
        let mut pf = engine(0);
        for (step, p) in prices.into_iter().enumerate() {
            pf.on_candle(&candle(step, p)).await.unwrap();
        }
        pf
    }

    #[tokio::test]
    async fn rejects_invalid_candles() {
        let mut pf = engine(0);
        for bad in [
            CandleStick::new(0, 100.0, 101.0, 99.0, 100.0, 1.0),
            CandleStick::new(1_700_000_000, 100.0, 101.0, 99.0, f64::NAN, 1.0),
            CandleStick::new(1_700_000_000, 100.0, 101.0, 99.0, 0.0, 1.0),
        ] {
            let error = pf.on_candle(&bad).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidData);
        }
    }

    #[tokio::test]
    async fn duplicate_and_out_of_order_candles_are_idempotent() {
        let mut pf = engine(0);
        pf.on_candle(&candle(0, 100.0)).await.unwrap();
        let seq = pf.candle_seq;
        // Trùng ts và ts lùi — cả hai phải bị bỏ qua, seq không tăng.
        assert!(pf.on_candle(&candle(0, 100.0)).await.unwrap().is_empty());
        assert!(
            pf.on_candle(&CandleStick::new(
                1_699_999_999,
                100.0,
                101.0,
                99.0,
                100.0,
                1.0
            ))
            .await
            .unwrap()
            .is_empty()
        );
        assert_eq!(pf.candle_seq, seq);
        assert_eq!(pf.candles.len(), 1);
    }

    #[tokio::test]
    async fn first_candles_rebuild_without_enough_history_keeps_empty_plan() {
        // Chưa đủ 10 nến → rebuild trả plan rỗng, không error, không đặt lệnh.
        let mut pf = engine(0);
        for step in 0..5 {
            let events = pf.on_candle(&candle(step, 100.0)).await.unwrap();
            assert!(
                events
                    .iter()
                    .all(|e| !matches!(e, OrderEvent::Placed { .. }))
            );
        }
        assert!(pf.plan.is_empty());
    }

    #[tokio::test]
    async fn wave_market_places_and_closes_orders() {
        let prices = (0..200).map(|i| 100.0 + (i % 40) as f64);
        let mut pf = feed(prices).await;
        let closed = pf.flush();
        assert!(pf.open_orders().is_empty(), "flush phải đóng hết lệnh mở");
        assert!(!pf.closed_orders().is_empty());
        assert!(
            closed
                .iter()
                .all(|e| matches!(e, OrderEvent::Closed { .. }))
        );
        // Flush lần 2 phải là no-op.
        assert!(pf.flush().is_empty());
    }

    #[tokio::test]
    async fn settlement_blocks_same_candle_exit() {
        // T+2: lệnh mở ở nến 100 phải không được đóng bởi nến 101 dù chạm SL.
        let mut pf = engine(2);
        // Nạp đủ history để rebuild tạo plan quanh vùng giá 100.
        for step in 0..30 {
            pf.on_candle(&candle(step, 100.0 - step as f64))
                .await
                .unwrap();
        }
        assert!(!pf.plan.is_empty(), "sau 10+ nến plan phải được build");
        if let Some(order) = pf.open_orders().first() {
            let crash = CandleStick::new(
                pf.candle_ts + 300,
                order.entry_price,
                order.entry_price + 1.0,
                order.sl_price - 10.0,
                order.sl_price - 5.0,
                10.0,
            );
            pf.on_candle(&crash).await.unwrap();
            // T+2 chặn: 1 nến mới < 2 nến chờ → chưa được đóng.
            assert_eq!(pf.open_orders().len(), 1, "lệnh phải còn mở vì T+N chưa đủ");
        }
    }

    #[tokio::test]
    async fn serialization_roundtrips_config_without_runtime_state() {
        let pf = engine(3);
        let json = serde_json::to_string(&pf).unwrap();
        let back: StreamingPortfolio = serde_json::from_str(&json).unwrap();
        assert_eq!(back.settlement_candles, 3);
        assert_eq!(back.kelly_fraction, pf.kelly_fraction);
        // State runtime phải reset, không theo config.
        assert!(back.candles.is_empty() && back.orders.is_empty() && back.plan.is_empty());
    }
}
