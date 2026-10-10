//! Native tool library registered onto every sandboxed Rhai engine.
//!
//! One entrypoint — [`register_all`] — installs every script-facing function,
//! grouped by concern:
//!
//! - [`time_fns`] – `now_secs()`
//! - [`ts_ops`] – time-series operators (`ts_rate`, `ts_moving_avg`,
//!   `ts_resample`, `ts_quantile`, `ts_p95`, `ts_p99`, `ts_delta`,
//!   `ts_pct_change`)
//! - [`attributes`] – per-call config lookups: `attr(name)` / `attrs()`
//! - [`orders`] – realtime grid trading: `portfolio_feed`
//! - `AnalysisGrid` / `TransitionAnalysis` – constructor + accessors, **khai tại
//!   nguồn** bằng `#[rhai_class]` trong `opsense-mlib` (`grid.rs`,
//!   `transition.rs`) và được gọi qua trait `RhaiBindings` ở đây
//!
//! Vì sao gọi qua trait thay vì `eng.register_fn` tay: danh sách accessor nằm
//! cạnh type nó phục vụ. Thêm/bớt accessor chỉ sửa một chỗ, không thể xảy ra
//! chuyện script thấy hàm trong khai báo mà runtime không đăng ký (hoặc ngược
//! lại) — trước đây hai danh sách tồn tại song song nên lệch là chết lúc chạy.
//!
//! Còn lại phải đăng ký tay là các binding **per-call** (`attributes`,
//! `station`): chúng capture state (attributes lấy từ config, station lấy từ
//! `Context` + tokio handle) nên không dùng được `#[rhai_func]` — xem
//! `attributes::register`.

use opsense_mlib::grid::AnalysisGrid;
use opsense_mlib::script::RhaiBindings;
use opsense_mlib::trend::TrendAnalysis;
use opsense_mlib::transition::TransitionAnalysis;

use crate::capacity::CapacityForecast;

/// Overload `to_float`/`to_int` cho **string**.
///
/// `Observation.labels` là `BTreeMap<String, String>` — mọi label (`pnl_pct`,
/// `size`, `sl`, `tp` …) về bản chất là chuỗi. Script đọc chúng rồi gọi
/// `.to_float()` như số, và Rhai mặc định **không** có overload này ⇒ script
/// chết ngay ở dòng đầu tiên có lệnh đóng (`Function not found: to_float
/// (&str)`), kéo theo cả batch metrics trong `perf_metrics`/`risk_governor`
/// không bao giờ được phát. Parse thủ công ở script là cái nhất quyết: có
/// hàng chục chỗ đọc label, và chỗ nào quên là hỏng âm thầm.
fn register_label_casts(eng: &mut rhai::Engine) {
    eng.register_fn("to_float", |s: &str| -> f64 {
        s.trim().parse::<f64>().unwrap_or(0.0)
    });
    eng.register_fn("to_int", |s: &str| -> i64 {
        s.trim().parse::<i64>().unwrap_or(0)
    });
}

/// Install every script-facing native function.
pub fn register_all(eng: &mut rhai::Engine, attributes: std::collections::BTreeMap<String, String>) {
    // Custom types + constructors + accessors: khai báo ở `opsense-mlib`.
    // Thứ tự quan trọng — `register_type` phải chạy trước khi script tạo
    // instance (macro đặt `register_type` trước accessor trong cùng block).
    AnalysisGrid::register(eng);
    TransitionAnalysis::register(eng);
    TrendAnalysis::register(eng);
    CapacityForecast::register(eng);

    // Free functions qua macro (`#[rhai_func]` + `inventory`).
    crate::time_fns::register(eng);
    crate::ts_ops::register(eng);

    register_label_casts(eng);

    // Per-call state: không macro được (xem module doc).
    crate::attributes::register(eng, attributes);

    // Realtime grid trading: `portfolio_feed` — kernel chạy trên Session tái
    // dựng từ observation order trong station (xem `orders`).
    crate::orders::register(eng);
}

/// Binding cho engine **strategy** (script `fn rebuild`, xem
/// [`crate::strategy`]): cùng bộ phân tích như `process` nhưng cắt bỏ phần
/// có state.
///
/// Không đăng ký `portfolio_feed`/`station`: strategy chỉ *dựng plan* từ nến
/// kernel đưa vào, không được gọi ngược vào kernel (nếu không là đệ quy
/// `rebuild` → `portfolio_feed` → `rebuild` …).
pub(crate) fn register_strategy_tools(eng: &mut rhai::Engine) {
    AnalysisGrid::register(eng);
    TransitionAnalysis::register(eng);
    TrendAnalysis::register(eng);
    CapacityForecast::register(eng);
    register_label_casts(eng);
    crate::time_fns::register(eng);
    crate::ts_ops::register(eng);
    crate::attributes::register(eng, std::collections::BTreeMap::new());
}
