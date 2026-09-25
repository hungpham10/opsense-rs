// grid.rhai — Binance grid strategy.
//
// Node nhận message từ BA input edges và branch theo `trigger()` (native —
// đọc `payload.trigger` trước, `payload.src` sau; xem
// crates/opsense-rhai/src/transform.rs):
//
//   trigger() == "tick"   → realtime aggTrade từ json_2_json (payload có
//     `ts` tính bằng **ms** — map từ `E`): gộp giá vào candle hiện tại
//     (bucket theo resolution, giây) trong station của chính node. Script
//     trả về 5 obs OHLCV của bucket đó; transform tự ghi vào own station →
//     trạng thái candle giữ qua station, không cần global.
//   trigger() khác        → snapshot: `station_candles` trên history (klines)
//     + candle live (own) → merge → `grid_fit` + `transition_analysis` →
//     một summary obs (`labels.kind = "snapshot"`) ước lượng bước giá tiếp
//     theo xu hướng grid: `last + (up_prob - down_prob) * grid_step`.
//
// Params (all optional, sane defaults):
//   history_source  = "history"   — station chứa candles klines (http candles)
//   own_station     = "grid"      — station của node (candle tick + snapshot)
//   symbol          = "BTCUSDT"
//   resolution      = "1m"
//   history_secs    = 3600        — cửa sổ history đọc cho fit
//   live_window_secs = 300        — cửa sổ candle live đọc từ own station

fn process(observations) {
    let hist_src = "history";
    if is_def_var("param_history_source") && type_of(param_history_source) != "()" {
        hist_src = param_history_source;
    }
    let own = "grid";
    if is_def_var("param_own_station") && type_of(param_own_station) != "()" {
        own = param_own_station;
    }
    let symbol = "BTCUSDT";
    if is_def_var("param_symbol") && type_of(param_symbol) != "()" {
        symbol = param_symbol;
    }
    let resolution = "1m";
    if is_def_var("param_resolution") && type_of(param_resolution) != "()" {
        resolution = param_resolution;
    }
    let hw = 3600;
    if is_def_var("param_history_secs") && type_of(param_history_secs) != "()" {
        hw = param_history_secs.to_int();
    }
    let lw = 300;
    if is_def_var("param_live_window_secs") && type_of(param_live_window_secs) != "()" {
        lw = param_live_window_secs.to_int();
    }

    // `params.mode`:
    //   "analysis" (mặc định) — chỉ dựng AnalysisGrid + TransitionAnalysis và
    //     phát snapshot; không đặt lệnh.
    //   "trading" — thêm bước đặt lệnh: candle VỪA ĐÓNG chạy qua kernel
    //     `portfolio_feed` (cùng kernel backtest dùng), lệnh ghi thành
    //     observation `signal = "order"` trong own station.
    let mode = "analysis";
    if is_def_var("param_mode") && type_of(param_mode) != "()" {
        mode = param_mode;
    }

    if trigger() == "tick" {
        return tick_accumulate(observations, own, symbol, resolution);
    }

    // Clock ping (trigger "") hoặc klines mới (trigger "history") → recompute.
    let out = snapshot(hist_src, own, symbol, resolution, hw, lw);

    if mode == "trading" {
        out += trade(hist_src, own, symbol, resolution, hw);
    }
    out
}

// ── trading: chạy kernel đặt lệnh cho candle vừa đóng ─────────────────────
//
// Station là state duy nhất: `portfolio_feed` tự tái dựng session từ các
// observation `signal = "order"` + cursor `labels.kind = "trading_step"` trong
// own station, nên script không giữ gì cả và restart không mất lệnh.

fn trade(hist_src, own, symbol, resolution, hw) {
    let now = now_secs();
    let from = now - hw;
    let step = bucket_secs(resolution);

    // Series phân tích = kline history + candle live; cùng bucket thì live thắng
    // (nến đang tích cực phản ánh giá thực tế, không phải nến đã đóng của API).
    let hist = station_candles(hist_src, from, now, resolution);
    let live = station_candles(own, from, now, resolution);
    if type_of(hist) == "()" && type_of(live) == "()" { return []; }
    let series = merge_candles(empty_if_missing(hist), empty_if_missing(live));
    // ≥ 10 nến để AnalysisGrid dựng được plan.
    if series.len() < 10 { return []; }

    // Chỉ giao trên nến ĐÃ ĐÓNG: bucket + step <= now.
    let closed = last_closed(series, now, step);
    if type_of(closed) == "()" { return []; }

    // Lệnh/cursor đều nằm ở own station.
    let state = station_query(own, from, now);
    if type_of(state) == "()" { return []; }
    if already_traded(state, closed["t"].to_int()) { return []; }

    portfolio_feed(series, state, closed, trading_cfg(), symbol)
}

/// `()` (station chưa có) → mảng rỗng để merge.
fn empty_if_missing(candles) {
    if type_of(candles) == "()" { [] } else { candles }
}

/// Nến cuối đã đóng (bucket + step <= now), hoặc `()` nếu tất cả còn đang mở.
fn last_closed(candles, now, step) {
    let i = candles.len() - 1;
    while i >= 0 {
        let c = candles[i];
        if c["t"].to_int() + step <= now { return c; }
        i -= 1;
    }
    ()
}

/// Cursor `labels.kind = "trading_step"` lưu nến đã chạy → không chạy lại nến đó.
fn already_traded(state, last_ts) {
    for o in state {
        if o["labels"]["kind"] == "trading_step" && o["ts"] >= last_ts { return true; }
    }
    false
}

/// Độ dài bucket (giây) theo resolution.
fn bucket_secs(resolution) {
    if resolution == "1" || resolution == "1m" { return 60; }
    if resolution == "5" || resolution == "5m" { return 300; }
    if resolution == "15" || resolution == "15m" { return 900; }
    if resolution == "1H" || resolution == "60" { return 3600; }
    if resolution == "1D" { return 86400; }
    60
}

/// Cấu hình engine lấy từ `params` của node (đổi chiến lược = đổi TOML).
fn trading_cfg() {
    let resolution = "1m";
    if is_def_var("param_resolution") && type_of(param_resolution) != "()" {
        resolution = param_resolution;
    }
    let calendar = "crypto";
    if is_def_var("param_calendar") && type_of(param_calendar) != "()" {
        calendar = param_calendar;
    }
    let strategy = "grid";
    if is_def_var("param_strategy") && type_of(param_strategy) != "()" {
        strategy = param_strategy;
    }
    let grid_levels = 5;
    if is_def_var("param_grid_levels") && type_of(param_grid_levels) != "()" {
        grid_levels = param_grid_levels.to_int();
    }
    let sl_pct = 0.008;
    if is_def_var("param_sl_pct") && type_of(param_sl_pct) != "()" {
        sl_pct = param_sl_pct.to_float();
    }
    let smoothing_k = 10.0;
    if is_def_var("param_smoothing_k") && type_of(param_smoothing_k) != "()" {
        smoothing_k = param_smoothing_k.to_float();
    }
    let lookback_secs = 2 * 24 * 3600;
    if is_def_var("param_lookback_secs") && type_of(param_lookback_secs) != "()" {
        lookback_secs = param_lookback_secs.to_int();
    }
    let review_interval_secs = 900;
    if is_def_var("param_review_interval_secs") && type_of(param_review_interval_secs) != "()" {
        review_interval_secs = param_review_interval_secs.to_int();
    }
    let trading_candle_secs = 60;
    if is_def_var("param_trading_candle_secs") && type_of(param_trading_candle_secs) != "()" {
        trading_candle_secs = param_trading_candle_secs.to_int();
    }
    let fee_rate = 0.0005;
    if is_def_var("param_fee_rate") && type_of(param_fee_rate) != "()" {
        fee_rate = param_fee_rate.to_float();
    }
    let kelly_fraction = 0.25;
    if is_def_var("param_kelly_fraction") && type_of(param_kelly_fraction) != "()" {
        kelly_fraction = param_kelly_fraction.to_float();
    }
    let base_capital = 100000.0;
    if is_def_var("param_base_capital") && type_of(param_base_capital) != "()" {
        base_capital = param_base_capital.to_float();
    }
    let settlement_candles = 0;
    if is_def_var("param_settlement_candles") && type_of(param_settlement_candles) != "()" {
        settlement_candles = param_settlement_candles.to_int();
    }

    #{
        resolution: resolution,
        calendar: calendar,
        strategy: strategy,
        grid_levels: grid_levels,
        sl_pct: sl_pct,
        smoothing_k: smoothing_k,
        lookback_secs: lookback_secs,
        review_interval_secs: review_interval_secs,
        trading_candle_secs: trading_candle_secs,
        fee_rate: fee_rate,
        kelly_fraction: kelly_fraction,
        base_capital: base_capital,
        settlement_candles: settlement_candles
    }
}

// ── tick: gộp trade price vào candle bucket ────────────────────────────────

fn tick_accumulate(observations, own, symbol, resolution) {
    let out = [];
    for o in observations {
        let ms = o["ts"];
        if type_of(ms) == "()" { continue; }
        if type_of(o["value"]) == "()" { continue; }
        let price = o["value"].to_float();

        // Binance klines/aggTrade ts là ms; bucket theo phút (resolution 1m).
        let bucket = (ms / 1000) / 60 * 60;
        let cur = station_candles(own, bucket - 60, bucket + 60, resolution);
        let c = candle_at(cur, bucket);
        let next = merge_price(c, price);
        out += candle_rows(bucket, next, symbol, resolution);
    }
    out
}

// Candle hiện tại cho bucket trong danh sách candles (hoặc candle rỗng).
fn candle_at(candles, bucket) {
    for c in candles {
        if c["t"] == bucket { return c; }
    }
    #{ t: bucket, o: 0.0, h: 0.0, l: 0.0, c: 0.0, v: 0.0 }
}

// Gộp một tick price vào candle: o giữ giá đầu, h/l = max/min, c = giá mới,
// v = số tick gộp (volume xấp xỉ bằng số trade đã gộp).
fn merge_price(c, price) {
    let o = if c["o"].to_float() > 0.0 { c["o"].to_float() } else { price };
    let h = if c["h"].to_float() > 0.0 && c["h"].to_float() > price { c["h"].to_float() } else { price };
    let l = if c["l"].to_float() > 0.0 && c["l"].to_float() < price { c["l"].to_float() } else { price };
    let v = c["v"].to_float() + 1.0;
    #{ o: o, h: h, l: l, c: price, v: v }
}

// 5 obs OHLCV cho một candle, theo đúng convention station
// (ts = giây, `labels.field` ∈ o/h/l/c/v, `labels.resolution`).
fn candle_rows(bucket, c, symbol, resolution) {
    let rows = [];
    rows.push(candle_obs(bucket, "o", c["o"].to_float(), symbol, resolution));
    rows.push(candle_obs(bucket, "h", c["h"].to_float(), symbol, resolution));
    rows.push(candle_obs(bucket, "l", c["l"].to_float(), symbol, resolution));
    rows.push(candle_obs(bucket, "c", c["c"].to_float(), symbol, resolution));
    rows.push(candle_obs(bucket, "v", c["v"].to_float(), symbol, resolution));
    rows
}

fn candle_obs(ts, field, value, symbol, resolution) {
    #{
        ts: ts,
        metric_id: symbol,
        kind: "metric",
        signal: "raw",
        value: value,
        labels: #{ field: field, resolution: resolution }
    }
}

// ── snapshot: history + live → grid/transition → summary obs ──────────────

fn snapshot(hist_src, own, symbol, resolution, hw, lw) {
    let now = now_secs();

    let hist = station_candles(hist_src, now - hw, now, resolution);
    if type_of(hist) == "()" { return []; }
    let live = station_candles(own, now - lw, now, resolution);
    if type_of(live) == "()" { return []; }

    // Live candle thắng history cùng bucket (bucket đang mở = giá realtime).
    let candles = sort_by_t(merge_candles(hist, live));
    if candles.len() < 2 { return []; }

    let points = [];
    for c in candles {
        points.push(#{ ts: c["t"], value: c["c"].to_float() });
    }

    let vals = [];
    for p in points { vals.push(p["value"]); }
    let v_min = array_min(vals);
    let v_max = array_max(vals);
    if v_max - v_min <= 0.0 { return []; }
    let v_last = vals[vals.len() - 1].to_float();

    let labels = #{
        kind: "snapshot",
        method: "",
        cells: "0",
        grid_step: "0",
        grid_cell: "0",
        up_prob: "0",
        down_prob: "0",
        stay_prob: "0",
        live_candles: live.len().to_string()
    };

    let predicted = v_last;
    let g = grid_fit(points, v_min, v_max, 8);
    if type_of(g) != "()" {
        labels.grid_step = grid_step(g).to_string();
        labels.grid_cell = grid_cell(g, v_last).to_string();
        labels.cells = num_cells(g).to_string();

        let interval = interval_secs_of(points);
        if interval > 0 {
            let ta = transition_analysis(g, points, interval);
            if type_of(ta) != "()" {
                labels.up_prob = up_probability(ta).to_string();
                labels.down_prob = down_probability(ta).to_string();
                labels.stay_prob = stay_probability(ta).to_string();
                labels.method = "grid-transition";
                // Bước giá kỳ vọng theo xu hướng grid (một cell).
                return [
                    #{ ts: candles[candles.len() - 1]["t"], metric_id: symbol, kind: "metric",
                       signal: "summary",
                       value: v_last + (up_probability(ta) - down_probability(ta)) * grid_step(g),
                       labels: labels }
                ];
            }
        }
        if labels.method == "" {
            labels.method = "grid-window";
        }
    }
    if labels.method == "" {
        labels.method = "steady";
    }

    [ #{ ts: candles[candles.len() - 1]["t"], metric_id: symbol, kind: "metric",
        signal: "summary", value: predicted, labels: labels } ]
}

/// Merge: live candle thắng history cùng bucket, bucket live-only được thêm
/// cuối; kết quả sort theo `t` ở caller (`sort_by_t`).
fn merge_candles(hist, live) {
    let out = [];
    for c in hist {
        let hit = find_candle(live, c["t"]);
        if type_of(hit) != "()" { out.push(hit); } else { out.push(c); }
    }
    for c in live {
        if type_of(find_candle(hist, c["t"])) == "()" { out.push(c); }
    }
    out
}

/// Candle có `t` == bucket, hoặc `()` nếu không có.
fn find_candle(candles, bucket) {
    for c in candles {
        if c["t"] == bucket { return c; }
    }
    ()
}

/// Sắp xếp candles theo `t` tăng dần (chèn — dữ liệu nhỏ).
fn sort_by_t(candles) {
    let out = [];
    for c in candles {
        let t = c["t"];
        let i = 0;
        while i < out.len() && out[i]["t"] <= t {
            i = i + 1;
        }
        if i < out.len() { out.insert(i, c); } else { out.push(c); }
    }
    out
}

/// Khoảng lấy mẫu trung bình (giây), 0 nếu không đủ 2 điểm.
fn interval_secs_of(points) {
    let n = points.len();
    if n < 2 { return 0; }
    let t0 = points[0]["ts"];
    let t1 = points[n - 1]["ts"];
    (t1 - t0) / (n - 1)
}

fn array_min(vals) {
    if vals.len() == 0 { return 0.0; }
    let m = vals[0].to_float();
    for x in vals { if x.to_float() < m { m = x.to_float(); } }
    m
}

fn array_max(vals) {
    if vals.len() == 0 { return 0.0; }
    let m = vals[0].to_float();
    for x in vals { if x.to_float() > m { m = x.to_float(); } }
    m
}