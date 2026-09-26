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
// own station, nên script không giữ gì cả.
//
// **"Restart không mất lệnh" chỉ đúng khi backend có persist.** Config này
// để `backend = "memory"` (v1 runtime-only) ⇒ state chỉ nằm trong RAM process,
// restart là mất sạch lệnh. Muốn restart-safe thì đổi sang `parquet`/`sqlite`.
// Đừng đọc comment cũ "restart không mất lệnh" là đúng với mọi backend.

fn trade(hist_src, own, symbol, resolution, hw) {
    let now = now_secs();
    let from = now - hw;
    let step = bucket_secs(resolution);

    // Kline history cho bucket đã đóng + candle live cho bucket đang mở; xem
    // `merge_candles` để biết vì sao không để live đè lên nến đã đóng.
    let hist = station_candles(hist_src, from, now, resolution);
    let live = station_candles(own, from, now, resolution);
    if type_of(hist) == "()" && type_of(live) == "()" { return []; }
    let series = merge_candles(empty_if_missing(hist), empty_if_missing(live), now, step);
    // ≥ 10 nến để AnalysisGrid dựng được plan.
    if series.len() < 10 { return []; }

    // Chỉ giao trên nến ĐÃ ĐÓNG: bucket + step <= now.
    let closed = last_closed(series, now, step);
    if type_of(closed) == "()" { return []; }

    // Lệnh/cursor đều nằm ở own station — nhưng **phải lọc**, không kéo cả
    // station. Own station lấp đầy tick aggTrade (5 obs/tick, vài nghìn
    // tick/phút); `station_query` không lọc dựng map cho từng observation và
    // `max_map_size` của Rhai là trần *engine-wide* (100_000) nên vài chục
    // nghìn obs là đủ vượt ⇒ cả batch script bị bỏ với
    // "Size of object map too large", node im bặt, không đặt được lệnh nào.
    // Đo thật: 26_680 obs trong 1 giờ đã vỡ. Kernel chỉ cần lệnh + cursor.
    // `station_query` trả `()` khi station chưa tồn tại (node vừa dựng) — nên
    // gộp qua `empty_if_missing` thay vì cộng thẳng, nếu không sẽ lỗi script.
    let state = [];
    state += empty_if_missing(station_query(own, from, now, "order"));
    state += empty_if_missing(station_query(own, from, now, "summary", "trading_step"));
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
    let strategy = "rhai";
    if is_def_var("param_strategy") && type_of(param_strategy) != "()" {
        strategy = param_strategy;
    }
    // Mặc định 4, khớp `trading.grid_levels` trong config.toml (contract test
    // `config_file_contract` chốt hai chỗ này phải bằng nhau). 4 vì mỗi ô là
    // một `TradingGrid` riêng: mốc đầu không có TP dưới, mốc cuối không có TP
    // trên ⇒ chỉ `levels - 2` mốc dùng được thật. 2 mốc là 0 mốc dùng được.
    let grid_levels = 4;
    if is_def_var("param_grid_levels") && type_of(param_grid_levels) != "()" {
        grid_levels = param_grid_levels.to_int();
    }
    let sl_pct = 0.008;
    if is_def_var("param_sl_pct") && type_of(param_sl_pct) != "()" {
        sl_pct = param_sl_pct.to_float();
    }
    let grid_min_trades = 3;
    if is_def_var("param_grid_min_trades") && type_of(param_grid_min_trades) != "()" {
        grid_min_trades = param_grid_min_trades.to_int();
    }
    let grid_weight_sharpness = 4.0;
    if is_def_var("param_grid_weight_sharpness") && type_of(param_grid_weight_sharpness) != "()" {
        grid_weight_sharpness = param_grid_weight_sharpness.to_float();
    }
    let grid_max_bit = 20;
    if is_def_var("param_grid_max_bit") && type_of(param_grid_max_bit) != "()" {
        grid_max_bit = param_grid_max_bit.to_int();
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

    let cfg = #{
        resolution: resolution,
        calendar: calendar,
        strategy: strategy,
        grid_levels: grid_levels,
        sl_pct: sl_pct,
        grid_min_trades: grid_min_trades,
        grid_weight_sharpness: grid_weight_sharpness,
        grid_max_bit: grid_max_bit,
        lookback_secs: lookback_secs,
        review_interval_secs: review_interval_secs,
        trading_candle_secs: trading_candle_secs,
        fee_rate: fee_rate,
        kelly_fraction: kelly_fraction,
        base_capital: base_capital,
        settlement_candles: settlement_candles
    };

    // `strategy = "dag"`: genome DAG build riêng ở Python truyền vào qua
    // `params.dag` (ops + nodes + weights). `Graph` tự emit ONNX rồi chạy
    // bằng tract — cầu nối giữa model ngoài và kernel.
    if is_def_var("param_dag") && type_of(param_dag) != "()" {
        cfg.dag = param_dag;
    }
    cfg
}

// ── Strategy: `fn rebuild` — dựng plan lưới lệnh ───────────────────────────
//
// Kernel (`Strategy::rebuild`) gọi hàm này mỗi `review_interval_secs`:
// nhận nến do kernel fetch, trả plan dạng dữ liệu. Cùng ý `Graph` (DAG) —
// chiến lược là **genome khai báo được**, không phải class Rust phải sửa rồi
// compile lại. Xem `opsense-rhai::ScriptStrategy` + `opsense-qlib::plan`.
//
//   candles = [#{ t, o, h, l, c, v }, …]   nến trong cửa sổ lookback
//   prev    = [#{ long_win, long_lost, short_win, short_lost }, …]
//            thống kê lệnh đã đóng của plan cũ (kernel giữ, không mất khi
//            script dựng lại plan)
//   params  = #{ grid_levels, sl_pct, lookback_secs, min_trades,
//                weight_sharpness, max_bit, level_edge_amp, … }
//
// Trả: một plan cho **mỗi** ô của AnalysisGrid (kernel ghép mỗi plan thành một
// lưới lệnh riêng trong khoảng giá của ô đó).
//
// Quy trình: sieve tìm số ô (AnalysisGrid) → xác suất chuyển trạng thái
// (TransitionAnalysis) → win-prob mô hình theo ô, lệch dần theo bậc (bậc ngoài
// gần giá hiện tại thì drift càng mạnh) → nếu đủ lệnh đã đóng thì tin tỉ lệ
// thắng thực tế.

fn rebuild(candles, prev, params) {
    // Knob thiếu → dùng default (script phải chạy được khi config không set hết,
    // ví dụ node chỉ truyền 3 knob trong test live).
    let levels = knob(params, "grid_levels", 4.0).to_int();
    let max_bit = knob(params, "max_bit", 20.0).to_int();
    let min_trades = knob(params, "min_trades", 3.0).to_int();
    let amp = knob(params, "level_edge_amp", 0.10).to_float();
    let sharpness = knob(params, "weight_sharpness", 4.0).to_float();

    // Khoảng giá quan sát + interval trung vị (transition cần step thời gian).
    let lo = candles[0].l;
    let hi = candles[0].h;
    let closes = [];
    let points = [];
    for k in candles {
        if k.l < lo { lo = k.l; }
        if k.h > hi { hi = k.h; }
        closes += [k.c];
        points += [#{ ts: k.t, value: k.c }];
    }
    if candles.len() < 2 || hi <= lo { return []; }

    let interval = median_gap(candles);

    let g = grid_fit_values(closes, lo, hi, max_bit);
    if type_of(g) == "()" { return []; }
    let t = transition_analysis(g, points, interval);
    if type_of(t) == "()" { return []; }

    let ups = up_probabilities(t);
    let downs = down_probabilities(t);
    let step = grid_step(g);
    let n_cells = num_cells(g);

    // Bỏ ô nào không đủ rộng để mọi mốc trong ô còn lãi sau phí sàn.
    //
    // Vì sao **bỏ ô** mà không rút bớt số mốc: mỗi ô là một `TradingGrid`
    // riêng, nên trong lưới của ô đó mốc đầu không có TP phía dưới
    // (`tp_below(0)` trả chính nó) và mốc cuối không có TP phía trên. Rút xuống
    // 2 mốc làm **mọi** mốc chết, còn 3 mốc chỉ còn 1 mốc dùng được — nên
    // `max_levels_for_profit` là cách sai (đã thử, làm hỏng test). Cách đúng là
    // giữ nguyên `grid_levels` mốc và loại ô không đủ rộng.
    //
    // Bề rộng ô là `step`; `levels` mốc ⇒ `levels − 1` khoảng. Đo thật trên
    // Binance: `step` ~ $25, mốc cách nhau $6.28, còn phí đòi ≥ $168 ở
    // `fee_rate = 0.001` ⇒ mọi entry của mọi ô đều bị kernel lo (`placed=0`).
    let fee = knob(params, "fee_rate", 0.001).to_float();
    // Biên an toàn 1.2: kernel so **nghiêm** (`expected_profit_pct > 2 × fee`),
    // nên bước đúng bằng ngưỡng cũng bị lo.
    let spacing_floor = min_profitable_step(fee, (lo + hi) / 2.0) * 1.2;

    let plans = [];
    for cell in 0..n_cells {
        let cell_lo = lo + cell * step;
        let up = pick(ups, cell);
        let down = pick(downs, cell);
        let total = up + down;
        // Drift = (up - down) / (up + down) trong [-1, 1].
        let drift = if total > 0.0 { (up - down) / total } else { 0.0 };

        // Ô quá hẹp: bỏ hẳn thay vì dựng lưới mà không lệnh nào lãi.
        if type_of(spacing_floor) != "()" && levels >= 2 {
            let spacing = step / (levels - 1);
            if spacing < spacing_floor { continue; }
        }

        // Lưới lệnh trong ô: K bậc đều, level đầu = đáy ô.
        let lv = [];
        for j in 0..levels { lv += [cell_lo + step * j / levels]; }
        let long_win = [];
        let short_win = [];
        let stats = if cell < prev.len() { prev[cell] } else { #{ } };

        for j in 0..levels {
            // t = 0 ở bậc dưới, 1 ở bậc trên: bậc càng xa giá hiện tại càng
            // nhiều drift ⇒ long khó thắng hơn, short dễ thắng hơn (và ngược lại).
            let x = j / (levels - 1);
            let model_long = clamp01(0.5 + drift + (1.0 - x) * amp);
            let model_short = clamp01(0.5 - drift + x * amp);

            let lw = wins(stats, "long_win", j);
            let ll = losses(stats, "long_lost", j);
            let sw = wins(stats, "short_win", j);
            let sl = losses(stats, "short_lost", j);

            // Đủ mẫu thì tin thực tế, chưa đủ thì tin mô hình.
            long_win += [if lw + ll >= min_trades { clamp01(lw / (lw + ll)) } else { model_long }];
            short_win += [if sw + sl >= min_trades { clamp01(sw / (sw + sl)) } else { model_short }];
        }

        plans += [#{ levels: lv,
                     sl_pct: params.sl_pct,
                     weight_sharpness: sharpness,
                     long_win: long_win,
                     short_win: short_win }];
    }
    plans
}

/// Đọc knob từ `params`, thiếu thì trả `default` — để `fn rebuild` chạy được
/// với config tối giểu (thiếu `level_edge_amp` chẳng hạn phải ra 0.10 chứ không
/// phải `()` làm hỏng cả lần rebuild).
fn knob(params, name, fallback) {
    if type_of(params[name]) == "()" { fallback } else { params[name] }
}

/// Median gap (giây) giữa các nến — dùng làm interval cho transition.
fn median_gap(candles) {
    let gaps = [];
    for i in 1..candles.len() { gaps += [candles[i].t - candles[i - 1].t]; }
    if gaps.len() == 0 { return 60; }
    gaps.sort();
    gaps[gaps.len() / 2]
}

/// Phần tử thứ `i` của mảng, thiếu → 0.0.
fn pick(arr, i) {
    if i < arr.len() { arr[i] } else { 0.0 }
}

/// Số lần thắng ở bậc `j` (thiếu → 0).
fn wins(stats, key, j) {
    if type_of(stats) == "()" { return 0; }
    if type_of(stats[key]) == "()" { return 0; }
    if j >= stats[key].len() { return 0; }
    stats[key][j]
}

/// Số lần thua ở bậc `j` (thiếu → 0).
fn losses(stats, key, j) {
    if type_of(stats) == "()" { return 0; }
    if type_of(stats[key]) == "()" { return 0; }
    if j >= stats[key].len() { return 0; }
    stats[key][j]
}

fn clamp01(x) {
    if x < 0.25 { 0.25 } else if x > 0.75 { 0.75 } else { x }
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

    // Kline history cho bucket đã đóng + candle live cho bucket đang mở.
    let step = bucket_secs(resolution);
    let candles = sort_by_t(merge_candles(hist, live, now, step));
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
/// Ghép kline (history) + candle live, quy tắc theo trạng thái bucket:
///
/// - bucket **đã đóng** (`t + step <= now`): giữ nến kline của API. Đây là
///   dữ liệu cuối cùng của cả phút. Trước đây live đè lên mọi bucket và đó là
///   lý do không có lệnh nào: nến live dựng từ tick 10s chỉ có vài phép nên
///   biên ~1 cent, thay cho biên thật ~$30 — vùng `[l, h]` bé bằng 1/1000 nên
///   không mốc lưới nào rơi vào ⇒ `grids_touched=1` mà `placed=0 rejected=0`.
/// - bucket **đang mở** (`t + step > now`): lấy live vì nến kline chưa có.
/// - bucket đóng chỉ có ở live (API thiếu nến): vẫn lấy, để không mất dữ liệu.
///
/// `merge_candles` cần `now` + `step` vì không tự biết bucket nào đã đóng.
fn merge_candles(hist, live, now, step) {
    // Map thay vì quét tuyến tính: `find_candle` từng là O(hist × live) và với
    // 960 nến là ~921k phép — chạm trần `set_max_operations(1_000_000)` nên
    // script chết "Too many operations", không dựng được lưới nào. Khoá map là
    // chuỗi vì Rhai chỉ cho khoá `Identifier`.
    let hist_t = #{};
    for c in hist { hist_t[c["t"].to_string()] = true; }

    let out = [];
    for c in hist {
        // Nhường bucket đang mở cho live, nếu không sẽ có hai nến cùng `t`.
        if c["t"].to_int() + step > now { continue; }
        out.push(c);
    }
    for c in live {
        let t = c["t"].to_int();
        if t + step > now { out.push(c); }
        else if !hist_t.contains(c["t"].to_string()) { out.push(c); }
    }
    out
}

/// Sắp xếp candles theo `t` tăng dần (chèn — dữ liệu nhỏ).
fn sort_by_t(candles) {
    let out = [];
    for c in candles {
        let t = c["t"];
        // Dò từ CUỐI về trước, không phải từ đầu: dữ liệu vào đã gần tăng
        // dần (kline rồi live) nên vị trí chèn thường là cuối ⇒ O(1) mỗi
        // phần tử. Dò từ đầu là O(n²) và với 960 nến thì vượt trần phép.
        let i = out.len();
        while i > 0 && out[i - 1]["t"] > t { i = i - 1; }
        if i == out.len() { out.push(c); } else { out.insert(i, c); }
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