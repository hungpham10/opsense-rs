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
//   own_station     = "grid"      — station của node (lệnh + snapshot)
//   live_source     = "tick-candle" — station chứa nến gom từ tick
//   symbol          = "BTCUSDT"
//   resolution      = "1m"
//   history_secs    = 3600        — cửa sổ history đọc cho fit
//   live_window_secs = 300        — cửa sổ candle live đọc từ live_source

fn process(observations) {
    let hist_src = "history";
    if is_def_var("param_history_source") && type_of(param_history_source) != "()" {
        hist_src = param_history_source;
    }
    let own = "grid";
    if is_def_var("param_own_station") && type_of(param_own_station) != "()" {
        own = param_own_station;
    }
    // Station chứa nến live. Tách khỏi `own` vì nến giờ do node `tick_2_candle`
    // gom từ tick và ghi vào station **của node đó**, không phải station của
    // node `grid`. Trước đây nến live nằm ở `own` vì chính script tự gộp.
    // Rỗng ⇒ lùi về `own` để cấu hình cũ không hỏng.
    let live_src = own;
    if is_def_var("param_live_source") && type_of(param_live_source) != "()" {
        live_src = param_live_source;
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
        // Nhánh tick không làm gì: node `tick_2_candle` đã gom nến và tự ghi khi
        // nến đóng. Script chỉ cần **không** ghi đè lên nến đó.
        return [];
    }

    // ── Tách hai nhịp: REVIEW (dựng lưới phân tích) và APPLY (đặt lệnh) ──
    //
    // Trước đây mỗi message đến — kể cả heartbeat rỗng của `tick_2_candle` —
    // đều chạy đủ `snapshot()` + `trade()`, tức ~7,5 lần/giây theo tần suất
    // aggTrade. Đo được: `snapshot()` ~55 ms/lần, trong đó `sort_by_t` 27 ms,
    // `merge_candles` 13,6 ms, dựng `points` 6,5 ms; còn kernel
    // (`grid_fit` 0,19 ms + `transition_analysis` 0,25 ms) gần như miễn phí.
    // Tức phần tốn là **thao tác mảng trong interpreter Rhai**, lặp lại vô
    // ích — đốt trọn một core mà không sinh thêm lệnh nào.
    //
    // Nay:
    //   REVIEW — dựng `grid_fit` + `transition_analysis`, nhịp `review_secs`
    //            (mặc định 1 giờ). Đây mới là việc đắt.
    //   APPLY  — chỉ đẩy nến vừa đóng vào lưới **đã có**, một lần mỗi nến.
    let step = bucket_secs(resolution);
    let now = now_secs();

    // Nến bucket `b` đóng khi `b + step <= now` ⇒ nến đã đóng gần nhất có
    // `ts = ((now - step) / step) * step`. Tính từ đồng hồ, **không** cần đọc
    // station — nên nhịp apply rẻ ngay ở cổng chặn đầu tiên.
    let closed_ts = ((now - step) / step) * step;

    // Marker do chính script ghi, khác `trading_step` của kernel (cái đó mang
    // ts của nến THỰC SỰ đóng, nên lệch với `closed_ts` khi API chậm — dùng
    // nó làm nhịp thì lúc dữ liệu trễ sẽ chạy lại nặng mỗi heartbeat).
    let marks = empty_if_missing(station_query(own, now - hw, now, "summary", "apply_step"));
    marks += empty_if_missing(station_query(own, now - hw, now, "summary", "review_step"));

    // `state` cho kernel: lệnh đã đóng + cursor T+N + plan đang dùng. Đọc ở
    // đây một lần rồi truyền xuống `trade` — trước đây `trade()` tự đọc, và
    // đọc sau khi đã merge/sort nên tốn phần nặng nhất trước khi biết mình có
    // cần làm việc hay không. Cả ba query này rẻ: ~0,2 ms mỗi cái.
    //
    // Plan (`kind = "plan"`) cũng là `signal = "summary"` nhưng **khác kind**,
    // nên hai query kia không lấy được nó. Mà plan là thứ `portfolio_feed` cần
    // để dựng lưới: `State::from_observations` đọc `session.plan` từ đúng
    // `state` này, thiếu nó thì plan rỗng ở **mọi** lời gọi.
    let state = [];
    state += empty_if_missing(station_query(own, now - hw, now, "order"));
    state += empty_if_missing(station_query(own, now - hw, now, "summary", "trading_step"));
    state += empty_if_missing(station_query(own, now - hw, now, "summary", "plan"));

    // ── Risk Governor: tính streak/fail-rate từ state để giảm nhiệt khi thua liên tiếp ──
    //
    // Chỉ chạy khi mode = "trading". Governor đọc lệnh đóng từ `state` (signal=order, status=closed),
    // tính streak thua liên tiếp + fail rate trong cửa sổ risk_window, rồi ra:
    //   risk_scale ∈ [risk_size_floor, 1]  — nhân vào base_capital
    //   apply_skip ≥ 1                       — thưa nhịp apply (1 lệnh / apply_skip nến)
    //   paused = true                        — dừng đặt lệnh mới (có timeout auto-recover)
    // Cả ba đều reset ngay khi có 1 lệnh thắng.
    let risk_enable = true;
    if is_def_var("param_risk_enable") && type_of(param_risk_enable) != "()" {
        risk_enable = param_risk_enable;
    }
    let risk_window = 10;
    if is_def_var("param_risk_window") && type_of(param_risk_window) != "()" {
        risk_window = param_risk_window.to_int();
    }
    let risk_size_step = 0.2;
    if is_def_var("param_risk_size_step") && type_of(param_risk_size_step) != "()" {
        risk_size_step = param_risk_size_step.to_float();
    }
    let risk_size_floor = 0.15;
    if is_def_var("param_risk_size_floor") && type_of(param_risk_size_floor) != "()" {
        risk_size_floor = param_risk_size_floor.to_float();
    }
    let risk_rate_ref = 0.5;
    if is_def_var("param_risk_rate_ref") && type_of(param_risk_rate_ref) != "()" {
        risk_rate_ref = param_risk_rate_ref.to_float();
    }
    let risk_rate_penalty = 0.5;
    if is_def_var("param_risk_rate_penalty") && type_of(param_risk_rate_penalty) != "()" {
        risk_rate_penalty = param_risk_rate_penalty.to_float();
    }
    let risk_soft_streak = 2;
    if is_def_var("param_risk_soft_streak") && type_of(param_risk_soft_streak) != "()" {
        risk_soft_streak = param_risk_soft_streak.to_int();
    }
    let risk_skip_cap = 5;
    if is_def_var("param_risk_skip_cap") && type_of(param_risk_skip_cap) != "()" {
        risk_skip_cap = param_risk_skip_cap.to_int();
    }
    let risk_hard_streak = 6;
    if is_def_var("param_risk_hard_streak") && type_of(param_risk_hard_streak) != "()" {
        risk_hard_streak = param_risk_hard_streak.to_int();
    }
    let risk_pause_secs = 900;
    if is_def_var("param_risk_pause_secs") && type_of(param_risk_pause_secs) != "()" {
        risk_pause_secs = param_risk_pause_secs.to_int();
    }
    let risk_model_win = 0.5;
    if is_def_var("param_risk_model_win") && type_of(param_risk_model_win) != "()" {
        risk_model_win = param_risk_model_win.to_float();
    }
    let risk_sl_widen_max = 1.5;
    if is_def_var("param_risk_sl_widen_max") && type_of(param_risk_sl_widen_max) != "()" {
        risk_sl_widen_max = param_risk_sl_widen_max.to_float();
    }

    // Tính governor ngay sau khi đọc state, trước gate do_apply
    let gov = #{};
    if risk_enable && mode == "trading" {
        gov = risk_governor(state, now, risk_window, risk_size_step, risk_size_floor,
                           risk_rate_ref, risk_rate_penalty, risk_soft_streak, risk_skip_cap,
                           risk_hard_streak, risk_pause_secs, risk_model_win, risk_sl_widen_max);
    } else {
        gov = #{ risk_scale: 1.0, apply_skip: 1, paused: false, streak: 0, fail_rate: 0.0,
                 sl_widen: 1.0, last_fail_ts: 0 };
    }

    let do_apply = mode == "trading" && !seen_step(marks, "apply_step", closed_ts)
        && !gov.paused
        && (gov.apply_skip <= 1 || (closed_ts / step) % gov.apply_skip == 0);

    let review_secs = 3600;
    if is_def_var("param_review_interval_secs") && type_of(param_review_interval_secs) != "()" {
        review_secs = param_review_interval_secs.to_int();
    }
    let do_review = !seen_step(marks, "review_step", now - review_secs);

    if !do_apply && !do_review { return []; }

    // Tới đây là clock ping (trigger "") hoặc klines mới (trigger "history").
    // Nhánh tick **không** gộp nến ở đây: node `tick_2_candle` làm việc đó và
    // giữ nến đang mở trong RAM (script stateless, gộp lại mỗi batch thì nến
    // phụ thuộc thứ tự batch và hỏng khi batch thiếu).
    // Merge + sort **một lần** rồi cho cả hai nhánh dùng chung. Trước đây
    // `snapshot()` và `trade()` tự đọc station và tự merge/sort riêng ⇒ mỗi
    // message làm trọng hai lần, đắt gấp đôi mà ra cùng một dữ liệu.
    let series = build_series(hist_src, live_src, resolution, hw, lw, now, step);
    if type_of(series) == "()" { return []; }

    let out = [];

    // Phát metrics risk/perf mỗi nhịp apply_due (kể cả paused/skip) để Grafana không thấy NaN.
    // Chỉ khi mode=trading và có nến đóng mới.
    let apply_due = mode == "trading" && !seen_step(marks, "apply_step", closed_ts);
    if apply_due {
        let closed_for_metrics = last_closed(series.candles, now, step);
        let closed_price = if type_of(closed_for_metrics) != "()" { closed_for_metrics.c.to_float() } else { 0.0 };
        out += risk_metrics(gov, symbol);
        out += perf_metrics(state, symbol, closed_price);
    }

    if do_review {
        out += snapshot(series, symbol);
        out += trend_probe(series.candles, symbol);
        out += trend_slope(series.candles, symbol);
        // Marker review: ghi kèm để lần sau biết đã review tới lúc nào, kể
        // cả khi `snapshot()` trả rỗng (thiếu dữ liệu) — nếu không thì thiếu
        // dữ liệu sẽ thử lại mỗi heartbeat.
        out += step_marker(now, symbol, "review_step");
    }
    if do_apply {
        let closed = last_closed(series.candles, now, step);
        if type_of(closed) != "()" {
            // Map `cfg` cho `portfolio_feed` **phải** dựng ở đây, trong thân
            // `process`. Runtime nạp `params` thành global `param_<name>` rồi
            // `call_fn("process")`, nên hàm con KHÔNG nhìn thấy: `is_def_var`
            // trong `trading_cfg()` trả `false` cho mọi param ⇒ nó luôn rơi về
            // số hardcode trong thân hàm. Lần này `min_rr` 0,5 trong config chết
            // âm thầm nên TP rơi về bậc kề. Xem `trading_cfg` bị bỏ.
            let cfg_calendar = "crypto";
            if is_def_var("param_calendar") && type_of(param_calendar) != "()" {
                cfg_calendar = param_calendar;
            }
            let cfg_strategy = "rhai";
            if is_def_var("param_strategy") && type_of(param_strategy) != "()" {
                cfg_strategy = param_strategy;
            }
            let cfg_grid_levels = 4;
            if is_def_var("param_grid_levels") && type_of(param_grid_levels) != "()" {
                cfg_grid_levels = param_grid_levels.to_int();
            }
            let cfg_sl_pct = 0.008;
            if is_def_var("param_sl_pct") && type_of(param_sl_pct) != "()" {
                cfg_sl_pct = param_sl_pct.to_float();
            }
            let cfg_min_trades = 3;
            if is_def_var("param_grid_min_trades") && type_of(param_grid_min_trades) != "()" {
                cfg_min_trades = param_grid_min_trades.to_int();
            }
            let cfg_sharpness = 4.0;
            if is_def_var("param_grid_weight_sharpness") && type_of(param_grid_weight_sharpness) != "()" {
                cfg_sharpness = param_grid_weight_sharpness.to_float();
            }
            let cfg_max_bit = 20;
            if is_def_var("param_grid_max_bit") && type_of(param_grid_max_bit) != "()" {
                cfg_max_bit = param_grid_max_bit.to_int();
            }
            let cfg_lookback = 2 * 24 * 3600;
            if is_def_var("param_lookback_secs") && type_of(param_lookback_secs) != "()" {
                cfg_lookback = param_lookback_secs.to_int();
            }
            let cfg_review = 900;
            if is_def_var("param_review_interval_secs") && type_of(param_review_interval_secs) != "()" {
                cfg_review = param_review_interval_secs.to_int();
            }
            let cfg_fee = 0.0002;
            if is_def_var("param_fee_rate") && type_of(param_fee_rate) != "()" {
                cfg_fee = param_fee_rate.to_float();
            }
            let cfg_kelly = 0.25;
            if is_def_var("param_kelly_fraction") && type_of(param_kelly_fraction) != "()" {
                cfg_kelly = param_kelly_fraction.to_float();
            }
            let cfg_capital = 100000.0;
            if is_def_var("param_base_capital") && type_of(param_base_capital) != "()" {
                cfg_capital = param_base_capital.to_float();
            }
            // Áp dụng risk governor: giảm base_capital theo risk_scale
            cfg_capital = cfg_capital * gov.risk_scale;
            let cfg_settlement = 0;
            if is_def_var("param_settlement_candles") && type_of(param_settlement_candles) != "()" {
                cfg_settlement = param_settlement_candles.to_int();
            }
            let cfg_min_rr = 0.0;
            if is_def_var("param_min_rr") && type_of(param_min_rr) != "()" {
                cfg_min_rr = param_min_rr.to_float();
            }
            // Phân bổ vốn theo xu hướng — `rebuild` đọc hai key này. Phải đặt
            // vào `cfg` ở đây vì hàm con KHÔNG thấy global `param_*` (xem
            // comment `trading_cfg` bị bỏ ở trên).
            let cfg_trend_bias_max = 0.0;
            if is_def_var("param_trend_bias_max") && type_of(param_trend_bias_max) != "()" {
                cfg_trend_bias_max = param_trend_bias_max.to_float();
            }
            let cfg_trend_er_gate = 0.15;
            if is_def_var("param_trend_er_gate") && type_of(param_trend_er_gate) != "()" {
                cfg_trend_er_gate = param_trend_er_gate.to_float();
            }

            let cfg = #{
                resolution: resolution,
                calendar: cfg_calendar,
                strategy: cfg_strategy,
                grid_levels: cfg_grid_levels,
                sl_pct: cfg_sl_pct,
                grid_min_trades: cfg_min_trades,
                grid_weight_sharpness: cfg_sharpness,
                grid_max_bit: cfg_max_bit,
                lookback_secs: cfg_lookback,
                review_interval_secs: cfg_review,
                fee_rate: cfg_fee,
                kelly_fraction: cfg_kelly,
                base_capital: cfg_capital,
                settlement_candles: cfg_settlement,
                min_rr: cfg_min_rr,
                trend_bias_max: cfg_trend_bias_max,
                trend_er_gate: cfg_trend_er_gate,
                // Risk governor params cho rebuild (SL widening)
                sl_levels: 1.0 * gov.sl_widen,
                tp_levels: 2.0 * gov.sl_widen,
                risk_sl_widen_max: risk_sl_widen_max
            };
            if is_def_var("param_dag") && type_of(param_dag) != "()" {
                cfg.dag = param_dag;
            }

            out += trade(series, state, closed, symbol, cfg);
            out += step_marker(closed_ts, symbol, "apply_step");
        }
    }
    out
}

/// Có marker `kind` nào đã ghi ở `ts >= ts_min` không ⇒ nhịp này đã chạy.
fn seen_step(state, kind, ts_min) {
    for o in state {
        if o["labels"]["kind"] == kind && o["ts"] >= ts_min { return true; }
    }
    false
}

/// Marker nhịp cho chính script (khác `trading_step` của kernel). `ts` = mốc
/// đã xử lý, nên `seen_step` so `>=` là so "đã gồm mốc này chưa".
fn step_marker(ts, symbol, kind) {
    [#{ ts: ts, metric_id: symbol, kind: "metric", signal: "summary", value: 0,
        labels: #{ kind: kind } }]
}

/// ============================================================================
/// RISK GOVERNOR — giảm nhiệt khi thua liên tiếp
/// ============================================================================
///
/// Ba cơ chế giảm nhiệt (reset ngay khi có 1 lệnh thắng):
/// 1. Kích thước:   base_capital × risk_scale,  risk_scale ∈ [risk_size_floor, 1]
/// 2. Xác suất apply: apply_skip ≥ 1 (1 lệnh / apply_skip nến), pause cứng có timeout
/// 3. Nới SL (rebuild): sl_levels/tp_levels nhân gov.sl_widen ∈ [1, risk_sl_widen_max]
///
/// Tín hiệu: quét `state` (signal=order, status=closed) trong cửa sổ risk_window.
/// Thiếu dữ liệu ⇒ streak=0 ⇒ hành vi y nguyên bản cũ.

/// Trích win/loss từ state (chỉ lệnh đóng: signal=order, status=closed).
/// Trả #{ wins: int, losses: int, last_fail_ts: int }.
fn closed_outcomes(state) {
    let wins = 0;
    let losses = 0;
    let last_fail_ts = 0;
    for o in state {
        if o["signal"] == "order" && o["labels"]["status"] == "closed" {
            let pnl = o["labels"]["pnl_pct"];
            if type_of(pnl) != "()" && pnl.to_float() < 0.0 {
                losses += 1;
                if o["ts"].to_int() > last_fail_ts { last_fail_ts = o["ts"].to_int(); }
            } else {
                wins += 1;
            }
        }
    }
    #{ wins: wins, losses: losses, last_fail_ts: last_fail_ts }
}

/// Tính streak thua liên tiếp (đếm ngược từ mới nhất, dừng khi gặp thắng).
/// Giới hạn trong risk_window lệnh gần nhất.
fn fail_streak(state, risk_window) {
    let streak = 0;
    let counted = 0;
    for o in state {
        if counted >= risk_window { break; }
        if o["signal"] == "order" && o["labels"]["status"] == "closed" {
            counted += 1;
            let pnl = o["labels"]["pnl_pct"];
            if type_of(pnl) != "()" && pnl.to_float() < 0.0 {
                streak += 1;
            } else {
                break; // gặp thắng ⇒ reset streak
            }
        }
    }
    streak
}

/// Tính fail rate trong cửa sổ risk_window.
fn fail_rate(state, risk_window) {
    let total = 0;
    let losses = 0;
    for o in state {
        if total >= risk_window { break; }
        if o["signal"] == "order" && o["labels"]["status"] == "closed" {
            total += 1;
            let pnl = o["labels"]["pnl_pct"];
            if type_of(pnl) != "()" && pnl.to_float() < 0.0 { losses += 1; }
        }
    }
    if total > 0 { losses.to_float() / total.to_float() } else { 0.0 }
}

/// Governor chính: trả map gồm risk_scale, apply_skip, paused, streak, fail_rate, sl_widen, last_fail_ts.
fn risk_governor(state, now, risk_window, risk_size_step, risk_size_floor,
                 risk_rate_ref, risk_rate_penalty, risk_soft_streak, risk_skip_cap,
                 risk_hard_streak, risk_pause_secs, risk_model_win, risk_sl_widen_max) {
    let outcomes = closed_outcomes(state);
    let streak = fail_streak(state, risk_window);
    let rate = fail_rate(state, risk_window);

    // risk_scale = max(floor, (1 - size_step*streak) * (1 - rate_penalty*max(0, rate - rate_ref)))
    let scale1 = if streak > 0 { 1.0 - risk_size_step * streak.to_float() } else { 1.0 };
    let scale2 = if rate > risk_rate_ref { 1.0 - risk_rate_penalty * (rate - risk_rate_ref) } else { 1.0 };
    let risk_scale = (scale1 * scale2).max(risk_size_floor);

    // apply_skip: thưa nhịp khi streak > soft_streak, tối đa skip_cap
    let apply_skip = if streak > risk_soft_streak {
        (1 + streak - risk_soft_streak).min(risk_skip_cap)
    } else { 1 };

    // paused: dừng hẳn khi streak >= hard_streak, nhưng có timeout auto-recover
    let paused = streak >= risk_hard_streak && (now - outcomes.last_fail_ts) < risk_pause_secs;

    // sl_widen: proxy toàn cục từ deficit win rate (rebuild áp per-cell từ prev)
    // deficit = max(0, (model_win - real_win) / model_win)  ; real_win = wins/(wins+losses)
    let total_closed = outcomes.wins + outcomes.losses;
    let real_win = if total_closed > 0 { outcomes.wins.to_float() / total_closed.to_float() } else { risk_model_win };
    let deficit = if risk_model_win > 0.0 { ((risk_model_win - real_win) / risk_model_win).max(0.0) } else { 0.0 };
    let sl_widen = (1.0 + deficit * (risk_sl_widen_max - 1.0)).min(risk_sl_widen_max);

    #{ risk_scale: risk_scale, apply_skip: apply_skip, paused: paused,
       streak: streak, fail_rate: rate, sl_widen: sl_widen, last_fail_ts: outcomes.last_fail_ts }
}

/// ============================================================================
/// METRICS EMISSION — phát obs cho Prometheus / Grafana
/// ============================================================================
///
/// Tất cả dùng `ts = now_secs()` (KHÔNG dùng closed_ts) để không bị gate 120s chặn.
/// Label chỉ giữ `kind` ổn định — số nằm ở `value` để tránh cardinality.

/// Phát metrics risk governor (mỗi nhịp apply_due, kể cả khi paused/skip).
fn risk_metrics(gov, symbol) {
    let now = now_secs();
    let out = [];
    out += [#{ ts: now, metric_id: "grid_risk_scale", kind: "metric", signal: "summary",
               value: gov.risk_scale, labels: #{ kind: "risk_state" } }];
    out += [#{ ts: now, metric_id: "grid_risk_loss_streak", kind: "metric", signal: "summary",
               value: gov.streak.to_float(), labels: #{ kind: "risk_state" } }];
    out += [#{ ts: now, metric_id: "grid_risk_fail_rate", kind: "metric", signal: "summary",
               value: gov.fail_rate, labels: #{ kind: "risk_state" } }];
    out += [#{ ts: now, metric_id: "grid_risk_apply_skip", kind: "metric", signal: "summary",
               value: gov.apply_skip.to_float(), labels: #{ kind: "risk_state" } }];
    out += [#{ ts: now, metric_id: "grid_risk_paused", kind: "metric", signal: "summary",
               value: if gov.paused { 1.0 } else { 0.0 }, labels: #{ kind: "risk_state" } }];
    out += [#{ ts: now, metric_id: "grid_risk_sl_widen", kind: "metric", signal: "summary",
               value: gov.sl_widen, labels: #{ kind: "risk_state" } }];
    out
}

/// Phát metrics hiệu năng chiến lược (window từ state).
fn perf_metrics(state, symbol, closed_price) {
    let now = now_secs();
    let outcomes = closed_outcomes(state);
    let total = outcomes.wins + outcomes.losses;
    let win_rate = if total > 0 { outcomes.wins.to_float() / total.to_float() } else { 0.0 };
    let open_notional = 0.0;
    let open_count = 0;
    let pnl_last = 0.0;
    for o in state {
        if o["signal"] == "order" {
            if o["labels"]["status"] == "open" {
                open_count += 1;
                let sz = o["labels"]["size"];
                if type_of(sz) != "()" { open_notional += sz.to_float(); }
            } else if o["labels"]["status"] == "closed" && type_of(o["labels"]["pnl_pct"]) != "()" {
                pnl_last = o["labels"]["pnl_pct"].to_float(); // lệnh gần nhất (state đã sort)
            }
        }
    }
    let out = [];
    out += [#{ ts: now, metric_id: "grid_win_rate", kind: "metric", signal: "summary",
               value: win_rate, labels: #{ kind: "perf" } }];
    out += [#{ ts: now, metric_id: "grid_trades_closed", kind: "metric", signal: "summary",
               value: total.to_float(), labels: #{ kind: "perf" } }];
    out += [#{ ts: now, metric_id: "grid_trades_open", kind: "metric", signal: "summary",
               value: open_count.to_float(), labels: #{ kind: "perf" } }];
    out += [#{ ts: now, metric_id: "grid_open_notional", kind: "metric", signal: "summary",
               value: open_notional, labels: #{ kind: "perf" } }];
    out += [#{ ts: now, metric_id: "grid_pnl_pct_last", kind: "metric", signal: "summary",
               value: pnl_last, labels: #{ kind: "perf" } }];
    out += [#{ ts: now, metric_id: "grid_price_last", kind: "metric", signal: "summary",
               value: closed_price, labels: #{ kind: "perf" } }];
    out
}

/// ============================================================================
/// GRID METRICS — hình học lưới phân tích (nhịp REVIEW)
/// ============================================================================
///
/// Trước đây toàn bộ số này chỉ bị `.to_string()` đổ vào label của obs
/// `snapshot`, mà `conf/*.toml` lại có `exclude_kinds = ["snapshot", ...]`
/// ⇒ Grafana không thấy gì về lưới.
///
/// Vì sao phát scalar riêng thay vì bỏ `snapshot` khỏi `exclude_kinds`:
/// `grid_step`/`up_prob`/`down_prob`/`grid_cell` đổi mỗi review ⇒ nếu để ở label
/// thì mỗi lần đổi sinh một series MỚI trên Grafana Cloud (bill theo series-hour).
/// Scalar mang `kind = "grid_state"` (không nằm trong `exclude_kinds`) nên mỗi
/// metric là **một** series cố định.
///
/// ⚠ Đây là lưới **phân tích** — `grid_fit(points, v_min, v_max, 8)` trong
/// `snapshot()`, `max_bit = 8`, KHÔNG có sàn `min_step_frac`. KHÔNG phải lưới
/// lệnh do `rebuild()` dựng bằng sieve. Nhịp: 1 mẫu / `review_interval_secs`.

/// Biên và bậc của lưới phân tích: số ô, bước một ô (giá), ô đang đứng của giá.
fn grid_geometry(cells, step_price, cell_now, lo, hi, symbol) {
    let now = now_secs();
    let out = [];
    out += [#{ ts: now, metric_id: "grid_cells", kind: "metric", signal: "summary",
               value: cells.to_float(), labels: #{ kind: "grid_state" } }];
    out += [#{ ts: now, metric_id: "grid_step_price", kind: "metric", signal: "summary",
               value: step_price, labels: #{ kind: "grid_state" } }];
    out += [#{ ts: now, metric_id: "grid_cell_now", kind: "metric", signal: "summary",
               value: cell_now.to_float(), labels: #{ kind: "grid_state" } }];
    out += [#{ ts: now, metric_id: "grid_lo", kind: "metric", signal: "summary",
               value: lo, labels: #{ kind: "grid_state" } }];
    out += [#{ ts: now, metric_id: "grid_hi", kind: "metric", signal: "summary",
               value: hi, labels: #{ kind: "grid_state" } }];
    out
}

/// Xác suất chuyển trạng thái trong một bước của lưới.
/// `stay_prob` cao ⇒ giá đi ngang, lệnh giới hạn khớp đều; `up/down` cao ⇒ đi
/// một chiều, lệnh giới hạn khó khớp và `grid_open_notional` sẽ đứng.
fn grid_transition(up_p, down_p, stay_p, symbol) {
    let now = now_secs();
    let out = [];
    out += [#{ ts: now, metric_id: "grid_up_prob", kind: "metric", signal: "summary",
               value: up_p, labels: #{ kind: "grid_state" } }];
    out += [#{ ts: now, metric_id: "grid_down_prob", kind: "metric", signal: "summary",
               value: down_p, labels: #{ kind: "grid_state" } }];
    out += [#{ ts: now, metric_id: "grid_stay_prob", kind: "metric", signal: "summary",
               value: stay_p, labels: #{ kind: "grid_state" } }];
    out
}

/// Probe xu hướng — metric_id riêng để Grafana có tên sạch (opsense_grid_trend_er).
/// Bỏ n/er/slope/net/tv khỏi label — số nằm ở value các metric riêng nếu cần.
fn trend_probe(candles, symbol) {
    let closes = [];
    for k in candles { closes += [k.c]; }
    let n = closes.len();
    if n < 3 { return []; }
    let net = closes[n - 1].to_float() - closes[0].to_float();
    let tv = 0.0;
    for k in 1..n { tv += (closes[k].to_float() - closes[k - 1].to_float()).abs(); }
    let er = if tv > 0.0 { net.abs() / tv } else { 0.0 };
    let slope = if tv > 0.0 { net / tv } else { 0.0 };
    [#{ ts: now_secs(), metric_id: "grid_trend_er", kind: "metric", signal: "summary", value: er,
        labels: #{ kind: "trend_probe" } }]
}

/// Probe trend slope (tùy chọn, emit riêng nếu cần).
fn trend_slope(candles, symbol) {
    let closes = [];
    for k in candles { closes += [k.c]; }
    let n = closes.len();
    if n < 3 { return []; }
    let net = closes[n - 1].to_float() - closes[0].to_float();
    let tv = 0.0;
    for k in 1..n { tv += (closes[k].to_float() - closes[k - 1].to_float()).abs(); }
    let slope = if tv > 0.0 { net / tv } else { 0.0 };
    [#{ ts: now_secs(), metric_id: "grid_trend_slope", kind: "metric", signal: "summary", value: slope,
        labels: #{ kind: "trend_probe" } }]
}

/// Đọc nến history + live, merge và sort — **một lần** cho mọi nhánh.
/// Trả `()` khi thiếu dữ liệu để dựng được chuỗi nến.
fn build_series(hist_src, live_src, resolution, hw, lw, now, step) {
    let hist = station_candles(hist_src, now - hw, now, resolution);
    if type_of(hist) == "()" { return (); }
    let live = station_candles(live_src, now - lw, now, resolution);
    if type_of(live) == "()" { return (); }
    let candles = sort_by_t(merge_candles(hist, live, now, step));
    if candles.len() < 2 { return (); }
    #{ now: now, step: step, live: live, candles: candles }
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

// Nhánh APPLY: nến đã đóng đi qua kernel trên lưới **đã có** (plan do
// `fn rebuild` dựng mỗi `review_interval_secs`). Không dựng lại lưới ở đây.
fn trade(series, state, closed, symbol, cfg) {
    let candles = series.candles;
    // ≥ 10 nến để AnalysisGrid dựng được plan.
    if candles.len() < 10 { return []; }

    // `state` (lệnh + cursor + plan) đã đọc ở `process` và truyền vào — đọc
    // sớm ở đó vì rẻ, và đọc *trước* khi merge/sort để lúc nến chưa tới không
    // phải làm việc nặng vô ích.
    if already_traded(state, closed["t"].to_int()) { return []; }

    portfolio_feed(candles, state, closed, cfg, symbol)
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
    // ── Hình học rủi ro, đo bằng BƯỚC LƯỚI chứ không bằng % giá ──────────────
    //
    // Vì sao đổi từ `sl_pct` sang `sl_levels`/`tp_levels`: kernel tính
    // `sl_long(j) = levels[j] * (1 - sl_pct)` (`qlib/grid.rs:326`), tức risk
    // **bám theo giá**, còn TP thì bám theo lưới (`reward` = 1 bước). Hai bên
    // lệch nhau nên RR = step/(entry*sl_pct) trôi theo biến động: đo được
    // RR 0,51 lúc lưới co, 1,02 lúc lưới nở — cùng một cấu hình.
    //
    // `sl_levels` = k (SL cách entry k bước), `tp_levels` = m (TP cách entry m
    // bước) ⇒ `RR = m/k` **hằng số**, không đổi khi sieve co/nới lưới, và
    // `win_ceiling` cũng hằng theo vì risk đã bám đúng bước.
    //
    // Mặc định k = 1,0 ⇒ RR = 2,0 và hòa vốn ~54%. **Không** xuống dưới ~0,31:
    // SL phải lớn hơn phí trọn vòng `2·fee·mid ≈ 34` (tức > 0,31 bước), nếu
    // không thì trả phí nhiều hơn cả khoản lỗ cực hạn.
    //
    // **Vì sao 0,5 → 1,0** (đo trên lệnh thật o27–o33):
    //
    //   k=0,5 ⇒ SL = 54,7 pts, TP = 109,6 pts. Nhưng nhiễu nến 1 phút đủ để
    //   đi ngược 54 pts trong ~38 phút: o33 vào short ở 85189,26, giá nhảy
    //   chạm đúng SL 85243,57 rồi **lại đi xuống tiếp**. Tức SL nằm **trong
    //   vùng nhiễu** ⇒ đoán đúng hướng vẫn bị quét stop.
    //
    //   Tỉ lệ thắng rơi từ 8/10 (80%, o17–o26) xuống 3/7 (43%, o27–o33).
    //
    //   RR **sau phí** ở k=0,5 chỉ còn (109,6−34)/(54,7+34) ≈ 0,85 < 1: mỗi
    //   lệnh thắng thực chất vẫn lỗ nhẹ, nên tỉ lệ thắng phải cao mới gồng,
    //   mà SL hẹp thì không gồng nổi.
    //
    //   k=1,0 đặt SL **đúng 1 bước lưới** (109,6 pts): nằm ngoài nhiễu thường
    //   lệnh mà vẫn nằm trong lưới (ngoài biên là mục tiêu khi k lớn hơn nữa),
    //   và RR gross giữ nguyên 2,0 vì m cũng là 2 bước.
    //
    // Đánh đổi: rủi ro mỗi lệnh **gấp đôi**, nên `kelly_fraction` và
    // `base_capital` phải xem lại — cùng `win_p` thì size sẽ tự điều chỉnh
    // theo `b = 1/sl_pct` (rộng hơn ⇒ size nhỏ hơn).
    //
    // `tp_levels` chỉ đổi được TP khi kernel chọn mốc xa hơn mốc kề, tức là
    // cần `min_rr` ở config **bằng** `tp_levels`. Kernel quét `tp_for_rr` và
    // lấy bậc kề đầu tiên có `reward/risk >= min_rr`, nên:
    //   min_rr <= 1.0 ⇒ TP bậc kề (m = 1), `sl_levels` là knob duy nhất chỉnh RR
    //   1.0 < min_rr <= 2.0 ⇒ TP 2 bậc, và bậc không còn đủ mốc phía kia thì
    //                          lệnh bị TỪ CHỐI, không rơi về bậc kề
    //
    // ⚠ Hai knob này ở HAI FILE KHÁC NHAU (`grid.local.toml` vs `.rhai`) và đã
    // lệch âm thầm một lần: `tp_levels = 2.0` mặc định ở đây, còn config để
    // `min_rr = 0.12` ⇒ TP rơi về bậc kề (RR 1.0) trong khi `win_ceiling` bên
    // dưới vẫn giả định 2 bước. Hệ quả: trần `win_p` tính trên nền RR 2.0,
    // thực tế là RR 1.0. Đổi một bên thì phải đổi cả bên kia.
    let sl_levels = knob(params, "sl_levels", 1.0).to_float();
    let tp_levels = knob(params, "tp_levels", 2.0).to_float();
    // Mật độ đặt lệnh. 0 = tắt, dùng lại `weight_sharpness` cũ.
    let occ_beta = knob(params, "edge_density_beta", 1.0).to_int().max(0);
    // `fee` đọc MỘT lần ở đây và dùng lại cho cả sàn sieve lẫn `spacing_floor`.
    // Trước đây có hai chỗ đọc với default khác nhau (0.001 ở `spacing_floor`),
    // nên đổi phí ở config mà quên một chỗ thì hai bên lệch nhau im lặng.
    // Default 0.0002 = maker của Binance USDⓈ-M Futures VIP 0.
    let fee = knob(params, "fee_rate", 0.0002).to_float();

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

    // ── Hướng + độ mạnh xu hướng (Kaufman Efficiency Ratio) ────────────────
    //
    // KHÔNG dùng `up/down_probabilities` cho việc này: đó là P(rời ô | hướng),
    // mà ở ô đỉnh thì P(down) > P(up) **bất kể xu hướng là gì** vì giá không
    // lên được cao hơn. Tổng hợp lại nó đo **hồi quy về trung bình**, không
    // đo xu hướng — và lệch dấu trong thị trường tăng mạnh.
    //
    // ER đo trên `closes` (cửa sổ lookback sẵn có):
    //   net  = c[-1] - c[0]        hướng
    //   tv   = Σ |Δc|              tổng biến độ (gồm cả nhiễu hai chiều)
    //   er   = |net| / tv          độ mạnh ∈ [0,1]: bao nhiêu phần biến độ là
    //                             có hướng. Slope lớn mà ER thấp = nhiễu trộn
    //                             ngược nhau ⇒ không nên tin hướng.
    //
    // ER thực tế của BTC 1m trên cửa sổ 16h chỉ quanh 0,05–0,30, nên `er_gate`
    // phải **đo** trên dữ liệu thật, không lấy số từ tài liệu chỉ báo.
    let n_c = closes.len();
    let net = closes[n_c - 1].to_float() - closes[0].to_float();
    let tv = 0.0;
    for k in 1..n_c { tv += (closes[k].to_float() - closes[k - 1].to_float()).abs(); }
    let er = if tv > 0.0 { net.abs() / tv } else { 0.0 };
    let slope = if tv > 0.0 { net / tv } else { 0.0 };

    // `trend_bias_max = 0` ⇒ tắt hoàn toàn, hành vi y như cũ (weights đều
    // trong ô). Đây là mặc định CỐ Ý: deploy script không được đổi hành vi
    // tiền thật, muốn bật thì đặt `trend_bias_max` ở config.
    let bias_max = knob(params, "trend_bias_max", 0.0).to_float();
    let er_gate = knob(params, "trend_er_gate", 0.15).to_float();
    // `er_gate = 0` nghĩa là "không chặn" — dùng để ĐO ER thật qua log trước khi
    // chọn ngưỡng. Nhưng phải chặn trước khi chia, không thì `slope / 0` ra
    // Infinity và `bias` thành NaN ⇒ `alloc_long` hỏng, plan rỗng.
    let no_gate = er_gate <= 0.0;
    let bias = if bias_max <= 0.0 || (!no_gate && er < er_gate) {
        0.0
    } else {
        let d = slope / (if no_gate { 1.0 } else { er_gate });
        (if d > 1.0 { 1.0 } else if d < -1.0 { -1.0 } else { d }) * bias_max
    };
    // Yếu hơn `er_gate` ⇒ `bias = 0` ⇒ về đúng 50/50. Đặt hướng khi chưa đủ
    // độ mạnh là đoán mò có tiền.
    let alloc_long = 0.5 + bias;
    if bias_max > 0.0 {
        print("grid: xu huong er=" + er + " (gate " + er_gate + ") net=" + net
            + " tv=" + tv + " => long " + alloc_long + " / short " + (1.0 - alloc_long));
    }

    let interval = median_gap(candles);

    // Sàn kích thước ô cho sieve, theo tỉ lệ giá: `step >= sàn × giá`.
    //
    // Vì sao: sieve **không biết phí**, nên nó ra lưới mịn nhất dữ liệu chịu
    // được. Đo được trên chính strategy này (cửa sổ 1h, `grid_levels = 4`):
    // sieve ra `step = $125.93` ⇒ mốc cách nhau $31.48, trong khi phí trọn vòng
    // đòi $33.87 ⇒ **8/8 entry bị lo** mà không có log gì cả. Cùng code đó với
    // cửa sổ 16h thì ra 4 ô rộng và vào được lệnh.
    //
    // Công thức `2 × fee_rate × levels`: mốc cách nhau `step / levels`, nên
    // muốn mốc ≥ `min_profitable_step = 2 × fee × giá` thì `step` phải ≥
    // `2 × fee × levels × giá`. Đúng bằng ngưỡng cũng không chắc (biên an toàn
    // của `spacing_floor` bên dưới là ×1.2) nên lấy ×1.2 cho chắc.
    let levels_f = levels.to_float();
    let min_step_frac = 2.0 * fee * levels_f * 1.2;
    let min_step_frac = if is_def_var("param_grid_min_step_frac")
        && type_of(param_grid_min_step_frac) != "()" {
        param_grid_min_step_frac.to_float()
    } else {
        min_step_frac
    };

    let g = grid_fit_values_cfg(closes, lo, hi, max_bit, min_step_frac);
    if type_of(g) == "()" { return []; }

    // Cửa sổ hẹp hơn cả sàn thì sieve dừng ngay ở level 0 và trả lỏi 1 ô —
    // mọi mốc vẫn quá gần để bù phí. Nói ra, đừng trả `[]` im lặng: "không có
    // lệnh" và "không dựng được lưới có lãi" là hai chuyện khác nhau.
    let step = grid_step(g);
    let floor_usd = min_step_frac * (lo + hi) / 2.0;
    if step < floor_usd - 0.0000001 {
        print("grid: cửa sổ hẹp hơn phí — step " + step + " < sàn " + floor_usd
            + " (biên độ " + (hi - lo) + ", giá " + ((lo + hi) / 2.0)
            + ", fee " + fee + ", levels " + levels_f
            + "). Không dựng được lưới nào đủ rộng để bù phí ⇒ không đặt được lệnh.");
        return [];
    }
    let t = transition_analysis(g, points, interval);
    if type_of(t) == "()" { return []; }

    let ups = up_probabilities(t);
    let downs = down_probabilities(t);
    let n_cells = num_cells(g);

    // ── Mật độ đặt lệnh: NGHỊCH ĐẢO occupancy ──────────────────────────────
    //
    // Ý tưởng: giá dày nhất ở giữa vùng, nên đặt lệnh dồn vào giữa là đánh vào
    // chỗ không có lợi thế. Đặt ở mép — nơi giá hiếm — thì mỗi lần vào lệnh là
    // một lần giá bị kéo căng, và SL nằm ngoài biên nên hiếm khi bị chọc.
    //
    // Dùng occupancy **thực nghiệm** (`grid_occupancy` = số interval bắt nguồn
    // từ mỗi ô) thay vì ép một hình chuông: giá BTC có fat tails và đổi hình
    // dạng theo regime, nên `exp(β·x²)` chỉ đúng khi phân phối đã chuẩn hoá —
    // còn occupancy thì tự bám dữ liệu thật.
    //
    // Cố ý KHÔNG dùng `up_probabilities` cho việc này: đó là P(đi lên | rời ô),
    // là xác suất *hướng*, khác hẳn P(giá ở ô) mà ta cần.
    let occ_tot = [];
    // Trọng số ô chuẩn hoá TOÀN CỤC (max = 1). Chuẩn hoá trong từng ô sẽ
    // triệt tiêu đúng cái khác biệt cần giữ: mọi mốc trong một ô có cùng
    // occupancy, mà kernel lại cộng `weight` vào `win_p` (`portfolio.rs:1020`)
    // nên khác biệt phải nằm GIỮA các ô mới có tác dụng.
    let occ_w = [];
    if occ_beta > 0 {
        let occ = grid_occupancy(g, points, interval);
        for cell in 0..n_cells {
            let seen = 0.0;
            for bucket in occ {
                if cell < bucket.len() { seen += bucket[cell].to_float(); }
            }
            // +1 để ô không có dữ liệu khỏi bị chia 0, và để w luôn tương đối.
            occ_tot += [seen + 1.0];
        }
        let mx = 0.0;
        for c in 0..n_cells {
            let v = 1.0;
            for b in 0..occ_beta { v *= 1.0 / occ_tot[c]; }
            occ_w += [v];
            if v > mx { mx = v; }
        }
        if mx > 0.0 {
            for c in 0..n_cells { occ_w[c] = occ_w[c] / mx; }
        }
    }

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
    // Biên an toàn 1.2: kernel so **nghiêm** (`expected_profit_pct > 2 × fee`),
    // nên bước đúng bằng ngưỡng cũng bị lo.
    let spacing_floor = min_profitable_step(fee, (lo + hi) / 2.0) * 1.2;

    // ── Trần `win_p`: kinh tế, không phải độ tin cậy ─────────────────────
    //
    // Số bước TP do `min_rr` ở config quyết định (xem `tp_for_rr`): TP thật là
    // bậc **ĐẦU TIÊN** đủ `reward/risk >= min_rr`, nên `reward` phải bằng
    // `spacing * tp_levels`. Nếu config để `min_rr` nhỏ hơn `tp_levels` thì
    // trần này tính trên nền RR dở hơn thực tế, tức đặt lệnh ở mức không hòa
    // vốn được — đã xảy ra một lần (`min_rr = 0.12` vs `tp_levels = 2.0`).
    //
    // Vì sao phải tính thay vì để hằng 0,75: đo thật trên Binance cho
    // `reward = 0,271%`, `sl_pct = 0,8%`, `fee = 0,02%/phía` ⇒ hòa vốn cần
    // **78,5%**. Trần 0,75 nằm dưới đó ⇒ **E < 0 ở mọi tham số hợp lệ**, và
    // không số liệu nào sửa được vì kernel tự chặn trước khi số liệu tới.
    let mid = (lo + hi) / 2.0;
    // Mốc dựng bằng `cell_lo + step * j / levels` ⇒ khoảng cách thật giữa hai
    // mốc là `step / levels`. Dùng `step / (levels - 1)` (như `spacing_floor`
    // bên dưới) sẽ **phóng đại** TP, tức làm ngưỡng hòa vốn thấp hơn thật —
    // sai theo hướng nguy hiểm: báo rẻ hơn để đặt lệnh.
    let gap = if levels >= 2 { step / levels_f } else { 0.0 };
    let spacing_frac = gap / mid;
    // Risk tính theo BƯỚC (`k` bước), không theo % giá — đây là chỗ RR và
    // ngưỡng hòa vốn trở nên bất biến. `sl_levels` hằng 0 thì rơi về
    // `sl_pct` cũ để tắt cơ chế mà không phải sửa config.
    let sl_pct = if gap > 0.0 { sl_levels * gap / mid } else { knob(params, "sl_pct", 0.008).to_float() };
    // `reward` phải là `tp_levels` bước chứ không phải 1 bước, nếu không
    // ngưỡng hòa vốn báo cao hơn thật và kernel đặt lệnh ở mức cần thiết.
    let cap = win_ceiling(spacing_frac * tp_levels, sl_pct, fee);
    if type_of(cap) == "()" {
        // Mốc cuối không có TP phía trên ⇒ `reward = 0` ⇒ không tỉ lệ thắng nào
        // hòa vốn. Vẫn dựng lưới (mốc khác vẫn dùng được), nhưng nói ra.
        print("grid: TP bằng 0 (levels < 2) — không tính được ngưỡng hòa vốn, \
              dùng trần độ-tin-cậy 0,75. Mọi mốc sẽ lỗ về mặt kỳ vọng.");
    } else if cap > win_conf_cap() {
        // Trần kinh tế vượt trần độ-tin-cậy: nói ra, vì đây là thứ quyết định
        // chiến lược có thể lãi hay không, và trước đây bị giấu im lặng.
        print("grid: ngưỡng hòa vốn " + cap + " > trần độ-tin-cậy 0,75 (TP "
            + spacing_frac + ", SL " + sl_pct + ", phí " + fee
            + ") ⇒ đặt trần win_p ở ngưỡng hòa vốn. Ở trần 0,75 cũ hệ thống \
              luôn âm dù dữ liệu tốt.");
    }

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
            let model_long = clamp_win(0.5 + drift + (1.0 - x) * amp, cap);
            let model_short = clamp_win(0.5 - drift + x * amp, cap);

            let lw = wins(stats, "long_win", j);
            let ll = losses(stats, "long_lost", j);
            let sw = wins(stats, "short_win", j);
            let sl = losses(stats, "short_lost", j);

            // Đủ mẫu thì tin thực tế, chưa đủ thì tin mô hình.
            long_win += [if lw + ll >= min_trades { clamp_win(lw / (lw + ll), cap) } else { model_long }];
            short_win += [if sw + sl >= min_trades { clamp_win(sw / (sw + sl), cap) } else { model_short }];
        }

        // SL của ô này: `k` bước tính từ **giữa ô**. Vì các mốc trong ô lệch
        // giữa ô không quá `step/2`, risk mỗi mốc sai số ≤ ~0,2% so với `k`×
        // spacing — chấp nhận được, và đổi lại RR thành hằng số.
        let cell_mid = cell_lo + step / 2.0;
        let cell_sl_pct = if gap > 0.0 { sl_levels * gap / cell_mid } else { params.sl_pct };

        // Trọng số theo nghịch đảo occupancy: ô giá hiếm ⇒ nhân lớn. Ô trung
        // tâm (giá dày) ⇒ nhân nhỏ. Chuẩn hoá cho tổng = 1 như `weights_normal`
        // để so sánh được với hình dạng cũ.
        let w = [];
        if occ_w.len() == n_cells && occ_w[cell] > 0.0 {
            // Chia đều trong ô để tổng của ô bằng `occ_w[cell]` (≤ 1): giữ được
            // thang tương đối giữa các ô mà vẫn không vượt 1.
            let per = occ_w[cell] / levels.to_float();
            // Tỉ lệ long/short theo xu hướng: scale TỪNG BẬC, giữ nguyên tổng
            // của ô. Cơ chế: `weights` nhân vào `win_p` (portfolio.rs:1020) rồi
            // `win_p` vào Kelly ⇒ `size ∝ weight`. Không cần sửa kernel.
            //
            // ⚠ Chiều phải theo **đúng** quy tắc kernel: `is_long = level <=
            // (levels[0] + levels[last]) / 2` (portfolio.rs:1016). KHÔNG dùng
            // `cell_mid`: với 4 bậc, kernel lấy `cell_lo + 3·step/8` còn
            // `cell_mid = cell_lo + step/2` — lệch nửa bước, đúng giữa hai bậc
            // giữa ô, nên dùng nhầm là gán trọng số sang sai chiều.
            let half = (lv[0] + lv[levels - 1]) / 2.0;
            let lv_f = levels.to_float();
            let n_l = 0;
            let n_s = 0;
            for j in 0..levels {
                if lv[j] <= half { n_l += 1; } else { n_s += 1; }
            }
            for j in 0..levels {
                let a = if lv[j] <= half && n_l > 0 {
                    lv_f * alloc_long / n_l.to_float()
                } else if n_s > 0 {
                    lv_f * (1.0 - alloc_long) / n_s.to_float()
                } else {
                    1.0
                };
                w += [per * a];
            }
        }

        let cell_plan = #{ levels: lv,
                           sl_pct: cell_sl_pct,
                           weight_sharpness: sharpness,
                           long_win: long_win,
                           short_win: short_win };
        // `with_weights` (`qlib/grid.rs:345`) **im lặng bỏ qua** nếu số phần tử
        // lệch số level — nên chỉ đặt khi đúng độ dài, còn lại để plan.rs
        // fallback về `weight_sharpness` như cũ.
        if w.len() == levels { cell_plan.weights = w; }
        plans += [cell_plan];
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

/// Sàn độ tin cậy: không tin tỉ lệ thắng nào thấp hơn 25%.
fn win_floor() { 0.25 }

/// Trần độ tin cậy: 75% — con số này là **ràng buộc độ tin cậy** ("đừng tin
/// mô hình hay mẫu nhỏ quá đà"), KHÔNG phải ràng buộc kinh tế.
///
/// Trần **kinh tế** là ngưỡng hòa vốn, tính ra ở [`win_ceiling`] theo TP/SL/phí
/// thật của lưới đang dựng. Trộn hai thứ độc lập vào một hằng 0,75 là lỗi đã
/// gây hậu quả: ngưỡng hòa vốn đo được là 78,5% > 75% ⇒ hệ thống **luôn âm**
/// dù dữ liệu thật tốt đến đâu.
fn win_conf_cap() { 0.75 }

/// Chặn tỉ lệ thắng trong `[win_floor(), cap]` — `cap` do [`win_ceiling`] trả về.
fn clamp_win(x, cap) {
    if type_of(cap) == "()" { return win_floor(); }
    let lo = win_floor();
    if x < lo { lo } else if x > cap { cap } else { x }
}

/// Trần thật cho `win_p`: `max(trần độ-tin-cậy, ngưỡng hòa vốn)`.
///
/// Ngưỡng hòa vốn phải **được phép** vượt lên trên 0,75, nếu không thì kernel
/// tự chặn mình ở mức không thể hòa vốn và không cách nào sửa bằng dữ liệu.
///
/// `reward` = TP thô (tỉ lệ giá), `risk` = `sl_pct`, `fee` = phí mỗi phía.
fn win_ceiling(reward, risk, fee) {
    let cap = win_conf_cap();
    let be = breakeven_win_p(reward, risk, fee);
    if type_of(be) == "()" { return cap; }
    if be > 1.0 { return 1.0; }
    if be > cap { be } else { cap }
}

// ── tick: KHÔNG gộp nến ở đây nữa ──────────────────────────────────────────
//
// Việc gom tick thành nến do node `tick_2_candle` lo
// (`crates/opsense-components/src/converters/tick2candle.rs`): nó giữ nến đang
// mở trong RAM và chỉ ghi nến khi nến đóng.
//
// Lý do bỏ khỏi script, không phải vì thừa:
//
// - **Script stateless.** Bản cũ phải `station_query` station của chính node
//   rồi tự gộp lại từ đầu mỗi batch, nên nến tích luỹ phụ thuộc thứ tự batch;
//   batch thiếu thì nến sai.
// - **Nến đóng là dữ liệu cuối.** Bản cũ ghi nến **đang mở** liên tục, và
//   kernel phải tự loại bằng `last_closed` trong script. Nay node chỉ ghi nến
//   đã đóng, nên invariant "chỉ nến đóng mới tới kernel" nằm ở đúng một chỗ.
// - **Lỗi nến là lỗi dữ liệu, không phải lỗi chiến lược** — nên log ở component
//   chứ không ở strategy.

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

// Nhánh REVIEW: dựng lại lưới phân tích + xác suất chuyển trạng thái, nhịp
// riêng (`review_interval_secs`). Nhận `series` dựng sẵn — không tự đọc
// station, vì `process()` đã merge/sort một lần cho cả hai nhánh.
//
// Trả về: obs `snapshot` (giá dự đoán + grid ở **label**) + obs `grid_*` scalar
// (`grid_geometry` / `grid_transition`) — obs `snapshot` bị `exclude_kinds` loại
// khỏi Prometheus, nên muốn Grafana thấy lưới thì các scalar kia mới là đường
// sống. Cả hai nhánh return đều phải gồm `geo`.
fn snapshot(series, symbol) {
    let now = series.now;
    let live = series.live;
    let candles = series.candles;
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
    // Metric scalar của lưới — xem `grid_geometry`/`grid_transition`.
    let geo = [];
    if type_of(g) != "()" {
        let n_cells = num_cells(g);
        let step_price = grid_step(g);
        let cell_now = grid_cell(g, v_last);
        labels.grid_step = step_price.to_string();
        labels.grid_cell = cell_now.to_string();
        labels.cells = n_cells.to_string();
        geo += grid_geometry(n_cells, step_price, cell_now, v_min, v_max, symbol);

        let interval = interval_secs_of(points);
        if interval > 0 {
            let ta = transition_analysis(g, points, interval);
            if type_of(ta) != "()" {
                let up_p = up_probability(ta);
                let down_p = down_probability(ta);
                let stay_p = stay_probability(ta);
                labels.up_prob = up_p.to_string();
                labels.down_prob = down_p.to_string();
                labels.stay_prob = stay_p.to_string();
                labels.method = "grid-transition";
                geo += grid_transition(up_p, down_p, stay_p, symbol);
                // Bước giá kỳ vọng theo xu hướng grid (một cell).
                let main = #{ ts: candles[candles.len() - 1]["t"], metric_id: symbol,
                             kind: "metric", signal: "summary",
                             value: v_last + (up_p - down_p) * step_price, labels: labels };
                let out = [main];
                out += geo;
                return out;
            }
        }
        if labels.method == "" {
            labels.method = "grid-window";
        }
    }
    if labels.method == "" {
        labels.method = "steady";
    }

    let out = [ #{ ts: candles[candles.len() - 1]["t"], metric_id: symbol, kind: "metric",
                  signal: "summary", value: predicted, labels: labels } ];
    out += geo;
    out
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
