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
use opsense_mlib::transition::TransitionAnalysis;

/// Install every script-facing native function.
pub fn register_all(eng: &mut rhai::Engine, attributes: std::collections::BTreeMap<String, String>) {
    // Custom types + constructors + accessors: khai báo ở `opsense-mlib`.
    // Thứ tự quan trọng — `register_type` phải chạy trước khi script tạo
    // instance (macro đặt `register_type` trước accessor trong cùng block).
    AnalysisGrid::register(eng);
    TransitionAnalysis::register(eng);

    // Free functions qua macro (`#[rhai_func]` + `inventory`).
    crate::time_fns::register(eng);
    crate::ts_ops::register(eng);

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
    crate::time_fns::register(eng);
    crate::ts_ops::register(eng);
    crate::attributes::register(eng, std::collections::BTreeMap::new());
}
