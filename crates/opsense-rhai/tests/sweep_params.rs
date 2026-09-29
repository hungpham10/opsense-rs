//! Quét `sl_pct` × `grid_levels` của `grid.rhai` trên **nến BTC thật**, tách
//! train/test.
//!
//! # Vì sao cần
//!
//! Lập luận toán nói cấu trúc hiện tại lỗ ở mọi mức `win_p` hệ thống đạt được
//! (xem PR #36). Nhưng toán không biết `win_p` thật. Test này **đo**, chạy
//! pipeline thật (`ScriptStrategy::rebuild` + `Portfolio::forward`) trên nến
//! thật và đếm lệnh đóng với `pnl_pct` do kernel ghi.
//!
//! # Tách train/test
//!
//! `btc_1h_train.csv` (699 nến, 29 ngày) để **chọn** tham số. `btc_1h_test.csv`
//! (300 nến, 12 ngày sau đó) để **kiểm** tham số đã chọn. Chia theo thời gian,
//! không ngẫu nhiên — random split rò rỉ thông tin tương lai vào tập chọn.
//!
//! Đọc tham số chỉ trên train rồi báo cả hai cột. Nếu chỉ báo train thì mọi
//! thứ đều lên và test trở nên vô dụng.
//!
//! # Dữ liệu
//!
//! BTCUSDT 1H, lấy từ Binance public `/klines` (999 nến đã đóng ≈ 41,6 ngày).
//! Cố định trong repo nên test không cần network và chạy lặp lại cho ra cùng
//! kết quả — nếu đổi dữ liệu mỗi lần chạy thì mọi lần tăng/giảm đều vô nghĩa.
//!
//! # Đây là gì, không phải gì
//!
//! **Có**: công cụ loại bộ tham số tệ, trên dữ liệu thật, có test set.
//!
//! **Không có**: khẳng định hiệu quả ngoài đời. 1H không có microstructure,
//! không có slippage thật, không có sàn nào khác. Số ra đủ để **loại**, chưa đủ
//! để **chọn** bộ tốt để đi thật — đó còn cần dữ liệu tháng và nhiều cặp.
//!
//! # Chạy
//!
//! ```bash
//! cargo test -p opsense-rhai --test sweep_params -- --ignored --nocapture
//! ```

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use opsense_qlib::{
    CandleStick, DataLoader, Portfolio, PortfolioConfig, Session, SharpeScore, SimpleFixedFee,
    TradingGrid,
};
use opsense_rhai::{ScriptSource, ScriptStrategy};

const RESOLUTION: &str = "1h";
/// Nhịp dựng lại plan. **Không** dùng 900 như production: sweep chạy 6 bộ
/// tham số × 2 tập nến, và mỗi lần `rebuild` chạy sieve + transition analysis
/// trên cả cửa sổ 700 nến.
///
/// ```
/// review = 900s   →  train 2.796 + test 1.200 rebuild × 6 bộ = 23.976 lần
/// review = 86400s →  train    29 + test    12 rebuild × 6 bộ =    250 lần
/// ```
///
/// Chênh 96 lần; vòng CI đầu chạy hơn 40 phút chưa xong vì lý do này.
///
/// Đổi khác production ở chỗ lưới được dựng lại ít thường xuyên hơn. Đây là
/// đánh đổi có ý thức: sweep tìm **hình học rào** nào đứng vững, mà hình học
/// rào do `grid_fit_values_cfg` quyết từ dữ liệu chứ không phụ thuộc nhịp
/// rebuild. Số lệnh đóng mỗi bộ sẽ ít hơn production, nên **đừng so P&L
/// tuyệt đối** với lịch sử live — chỉ so *giữa các bộ tham số*, cùng một nhịp.
const REVIEW: u64 = 86_400;
const FEE: f64 = 0.0002;
const KELLY: f64 = 0.25;
/// `lookback_secs` của production (`config.toml:199`) = 48 giờ. **Phải khớp**:
/// lần chạy đầu tôi đặt bằng cả tập nến (~30 ngày) và sieve ra ô rộng 9%, bước
/// lưới 4,5% thay vì 0,27% — tức đo một chiến lược khác hẳn, không phải chiến
/// lược đang chạy.
const LOOKBACK_SECS: f64 = 172_800.0;
const CAPITAL: f64 = 100_000.0;

/// Bộ tham số quét. `sl_pct` × `grid_levels` là hai thứ quyết định hình học
/// rào TP/SL, tức hai thứ quyết định lãi-thua.
const SL_PCTS: &[f64] = &[0.002, 0.004, 0.008];
/// 4 là `grid_levels` của production; 3 và 5 để xem lân cận.
const LEVELS: &[i64] = &[3, 4, 5];

fn data(name: &str) -> Vec<CandleStick> {
    let p: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name);
    let txt = std::fs::read_to_string(&p)
        .unwrap_or_else(|e| panic!("đọc {}: {e}", p.display()));
    let mut out = Vec::new();
    for (i, line) in txt.lines().enumerate() {
        if i == 0 {
            continue; // header
        }
        let f: Vec<&str> = line.split(',').collect();
        assert_eq!(f.len(), 5, "dòng {} của {}: {}", i + 1, name, line);
        let g = |k: usize| f[k].parse::<f64>().expect("số trong fixture");
        out.push(CandleStick {
            t: f[0].parse::<i64>().expect("ts trong fixture"),
            o: g(1),
            h: g(2),
            l: g(3),
            c: g(4),
            v: 1.0,
        });
    }
    assert!(out.len() > 100, "{} chỉ có {} nến", name, out.len());
    out
}

fn strategy(levels: i64) -> ScriptStrategy {
    ScriptStrategy::new(
        ScriptSource::File(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../strategies/binance/grid.rhai"),
        ),
        REVIEW,
    )
    .with_knob("min_trades", 3.into())
    .with_knob("weight_sharpness", 4.0.into())
    .with_knob("max_bit", 20.into())
    .with_knob("fee_rate", FEE.into())
    .with_knob("grid_levels", levels.into())
}

/// Tham số mà kernel đọc theo **chỉ số**, không phải theo tên — xem
/// `ScriptStrategy::rebuild` (`param(0..4)`).
///
/// `param(4)` = `lookback_secs`: phải **phủ hết tập nến**, vì script đọc lịch sử
/// theo đồng hồ thật. Đặt 0 thì cửa sổ lấy nến rỗng và
/// `strategy script cần ≥ 10 nến, có 0` — lỗi này lần đầu tôi đặt sai, đã dính
/// và ghi lại đây.
fn params(i: usize, sl_pct: f64, levels: i64, lookback: f64) -> f64 {
    [KELLY, CAPITAL, levels as f64, sl_pct, lookback][i]
}

struct NoLoader;

#[async_trait::async_trait]
impl DataLoader for NoLoader {
    async fn range(
        &self,
        _f: u64,
        _t: u64,
        _r: &str,
    ) -> Result<Vec<CandleStick>, std::io::Error> {
        Ok(Vec::new())
    }
}

fn slice_fetch(
    data: Vec<CandleStick>,
) -> impl FnMut(
    u64,
    u64,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<CandleStick>, std::io::Error>> + Send>,
> {
    move |from, to| {
        let out: Vec<CandleStick> = data
            .iter()
            .copied()
            .filter(|c| c.t >= 0 && (c.t as u64) >= from && (c.t as u64) < to)
            .collect();
        Box::pin(async move { Ok(out) })
    }
}

/// Kết quả một lượt chạy trên một tập nến.
#[derive(Clone)]
struct Run {
    sl_pct: f64,
    levels: i64,
    cells: usize,
    placed: usize,
    closed: usize,
    tp: usize,
    sl: usize,
    pnl_usd: f64,
    /// Độ dài đường giá trong lưới ⇒ 1 bước lưới bằng bao nhiêu phần trăm.
    /// Tỉ lệ thắng thực tế phụ thuộc vào đây, nên phải báo chứ không giả định.
    step_frac: f64,
    breakeven: f64,
}

async fn run(data: &[CandleStick], sl_pct: f64, levels: i64) -> Run {
    let pf = Portfolio::new(
        Arc::new(NoLoader),
        Arc::new(strategy(levels)),
        Arc::new(SimpleFixedFee::new(FEE)),
        Arc::new(SharpeScore),
        Arc::new(opsense_qlib::CryptoCalendar),
        PortfolioConfig {
            resolution_for_test: RESOLUTION.into(),
            resolution_for_rebuild: RESOLUTION.into(),
            settlement_candles: 0,
            cache_enabled: false,
        },
    )
    .expect("Portfolio nhận strategy script");

    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let mut notify = move |e: opsense_qlib::OrderEvent| {
        sink.lock().expect("lock events").push(e);
        Box::pin(async { Ok(()) })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'static>,
            >
    };

    // `rebuild` chạy theo `REVIEW` giây, mỗi lần lấy cửa sổ
    // `[now - lookback, now]`. Cửa sổ phải phủ **toàn bộ** tập nến, nếu không
    // lượt đầu tiên không thấy nến nào.
    let lookback = LOOKBACK_SECS;

    let mut session = Session::new();
    session.next_ts = data[0].t as u64;
    // Con trỏ `next_ts` luôn lệch **+1 giây** so với mốc nến (calendar cộng
    // `resolution` vào chính con trỏ trước đó), còn `fetch` lọc `t < to`.
    //
    // Nên nếu `review_at` rơi **đúng** lên mốc nến thì cửa sổ
    // `[next_ts, review_at)` không chứa nến nào ⇒ `forward` trả `Ok(false)` và
    // replay **dừng im** ở giữa chừng. Đo được: dừng đúng ở nến thứ 240.
    //
    // Lệch 61 giây: vẫn nằm trong giờ đó (nến kế tiếp +1h) nhưng không trùng
    // mốc nến, nên `fetch` lấy được nến và replay chạy hết.
    //
    // Các lần `review_at` sau do `next(current) = current + REVIEW` sinh ra thì
    // tự mang lệch +1 giây rồi, không cần can thiệp.
    const BOUNDARY_OFFSET: i64 = 61;
    // `forward` xử lý **một nến mỗi lần gọi** (trả `Ok(false)` khi hết
    // nến mới) — phải lặp, không gọi một lần là xong. Gọi một lần thì chỉ có
    // nến đầu, `review_at` ở t0+10 ngày không bao giờ tới, plan rỗng, và test
    // "xanh" với 0 ô 0 lệnh — dấu hiệu tệ hơn đỏ.
    let mut guard = 0usize;
    // Khởi động ấm: `rebuild` cần **lịch sử phía trước** mốc hiện tại, mà
    // `slice_fetch` lọc `t < to` — tức chính nến đang xét cũng không tính.
    //
    // `Session::new()` để `review_at = 0` ⇒ rebuild ngay ở nến đầu tiên, cửa
    // sổ `[t0 − lookback, t0)` rỗng ⇒ `strategy script cần ≥ 10 nến, có 0`.
    // Fix `lookback_secs` trước đó **không** giải được: cửa sổ rộng hay hẹp thì
    // vẫn không có nến nào phía trước `t0`.
    //
    // Ở production nơi này không hỏng vì node `history` đã có sẵn dữ liệu
    // trước khi node `grid` chạy. Replay thì không có gì trước nến đầu tiên,
    // nên phải tự chờ tích lũy.
    //
    // Chờ tích lũy **nhiều hơn `lookback_secs`** rồi mới rebuild, để lượt
    // rebuild đầu đã có đủ cửa sổ 48 giờ như production — nếu không thì lượng
    // đầu dựng lưới trên cửa sổ ngắn hơn, khác hẳn hành vi thật.
    // Chờ 48 giờ + 24 giờ dư trước lượt rebuild đầu.
    const WARMUP_SECS: u64 = 72 * 3600;
    session.review_at = data[0].t as u64 + WARMUP_SECS + BOUNDARY_OFFSET as u64;
    // `forward` trả `Ok(false)` khi **cửa sổ hiện tại không có nến mới** — và
    // khi đó nó vẫn đẩy `next_ts` theo calendar, nghĩa là "gọi lại ở lượt sau",
    // KHÔNG phải "hết dữ liệu". Dừng ở lần `false` đầu tiên cắt replay cụt.
    //
    // Đo được: `review_at` nằm giữa hai mốc nến (`t0+240h+61`), nên sau khi ăn
    // nến `t0+240h` thì cửa sổ `[next_ts, review_at)` rỗng ⇒ `false`, con trỏ
    // mới nhích qua `review_at` ⇒ rebuild ở lượt kế. Phải gọi tiếp.
    //
    // Dừng theo **số nến đã xử lý**, không theo mã trả về. Chặn trên bằng trần
    // để lỗi con trỏ không tiến lộ ra thành vòng lặp vô hạn.
    let max_calls = data.len() * 8 + 64;
    while (session.candle_seq as usize) < data.len() {
        assert!(
            guard < max_calls,
            "forward gọi {} lần mà mới xử lý {}/{} nến — con trỏ không tiến",
            guard,
            session.candle_seq,
            data.len()
        );
        pf.forward(
            &mut session,
            &|i: usize| params(i, sl_pct, levels, lookback),
            &mut slice_fetch(data.to_vec()),
            &mut slice_fetch(data.to_vec()),
            &mut notify,
        )
        .await
        .expect("forward trên nến thật");
        guard += 1;
    }
    assert_eq!(
        session.candle_seq as usize,
        data.len(),
        "forward phải đi hết {} nến, mới chỉ đi {} nến",
        data.len(),
        session.candle_seq
    );

    let mut tp = 0usize;
    let mut sl = 0usize;
    let mut pnl_usd = 0.0f64;
    for o in &session.history {
        let p = o.pnl_pct.unwrap_or(0.0);
        if p >= 0.0 {
            tp += 1;
        } else {
            sl += 1;
        }
        pnl_usd += o.size * p;
    }

    // Bước lưới từ chính plan mà kernel dùng, không giả định.
    let (step_frac, breakeven) = match session.plan.first() {
        None => (0.0, f64::NAN),
        Some(g) => {
            let lv = g.levels();
            let mid = (lv[0] + lv[lv.len() - 1]) / 2.0;
            // Mốc dựng `cell_lo + step·j/levels` ⇒ khoảng cách thật là step/levels.
            let step_usd = (lv[lv.len() - 1] - lv[0]) / (lv.len() - 1).max(1) as f64;
            let frac = step_usd / mid;
            (frac, TradingGrid::breakeven_win_p(frac, g.stoploss_pct(), FEE))
        }
    };

    Run {
        sl_pct,
        levels,
        cells: session.plan.len(),
        placed: session.history.len() + session.orders.len(),
        closed: session.history.len(),
        tp,
        sl,
        pnl_usd,
        step_frac,
        breakeven,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "TEST ĐỂ CHẠY TAY — quét trên nến thật; có test set nhưng 1 cặp 1 thang, \
            chưa đủ để kết luận hiệu quả ngoài đời"]
async fn sweep_sl_pct_and_levels_on_real_candles() {
    let train = data("btc_1h_train.csv");
    let test = data("btc_1h_test.csv");
    println!(
        "train {} nến, test {} nến (test nằm SAU train — không trùng khoảng)",
        train.len(),
        test.len()
    );

    let mut rows: Vec<(Run, Run)> = Vec::new();
    for &sl_pct in SL_PCTS {
        for &levels in LEVELS {
            let t = run(&train, sl_pct, levels).await;
            // Tham số **đã chọn trên train**; chạy nguyên vẹn trên test.
            let v = run(&test, sl_pct, levels).await;
            rows.push((t, v));
        }
    }

    println!("\n{:<8} {:<7} | {:<26} | {:<26}", "sl_pct", "levels", "TRAIN (chọn)", "TEST (kiểm)");
    println!("{}", "-".repeat(96));
    for (t, v) in &rows {
        let f = |r: &Run| {
            format!(
                "{:>2}ô/{:>3}v {:>3}đ {:>2}/{:<2} {:+8.2}$",
                r.cells,
                r.placed,
                r.closed,
                r.tp,
                r.sl,
                r.pnl_usd
            )
        };
        println!("{:<8.3} {:<7} | {:<26} | {:<26}", t.sl_pct, t.levels, f(t), f(v));
    }

    println!("\nNgưỡng hòa vốn và bước lưới (lấy từ plan thật của lượt TRAIN):");
    for (t, _) in &rows {
        println!(
            "  sl_pct={:.3} levels={:<2} bước lưới {:.4}%  hòa vốn cần win_p {:.2}%",
            t.sl_pct,
            t.levels,
            t.step_frac * 100.0,
            t.breakeven * 100.0
        );
    }

    // Bảng xếp hạng để đọc nhanh: chọn trên TRAIN, rồi xem TEST có giữ được không.
    let mut by_train: Vec<(Run, Run)> = rows.to_vec();
    by_train.sort_by(|a, b| b.0.pnl_usd.partial_cmp(&a.0.pnl_usd).unwrap());
    println!("\n--- xếp theo P&L TRAIN ---");
    for (t, v) in &by_train {
        println!(
            "  sl_pct={:.3} levels={:<2}  train {:+8.2}$ ({}đ)  →  test {:+8.2}$ ({}đ)",
            t.sl_pct, t.levels, t.pnl_usd, t.closed, v.pnl_usd, v.closed
        );
    }

    let best_train = by_train[0].0.pnl_usd;
    let best_test = by_train[0].1.pnl_usd;
    println!(
        "\nBộ tốt nhất trên TRAIN mang sang TEST: {:+.2}$ → {:+.2}$",
        best_train, best_test
    );
    if best_test < 0.0 {
        println!(
            "  ⇒ Dấu đổi: tốt nhất trên TRAIN lỗ trên TEST. Đây là dấu hiệu khớp nhiễu\n\
             (overfit), không phải tham số dở."
        );
    }
    println!(
        "\nLưu ý: 1 cặp, 1 thang, ~{:.0} ngày. Đủ để LOẠI, chưa đủ để CHỌN để đi thật.",
        (train.len() + test.len()) as f64 / 24.0
    );
}
