# Hướng dẫn viết component mới trong Opsense

> Crate nơi component sống: **`crates/opsense-components`** — đã nối thẳng vào
> serve qua `use opsense_components as _;` và healthy trong CI. Thêm component
> mới = thêm **một** struct + `#[transform]` (hoặc `#[sink]`/`#[source]`), khai
> trong `config.toml` của strategy, test theo **run → chờ → đọc → timeout**.
> Không cần sửa `serve`/`main`/CI matrix.

---

## 1. Bắt chước component đang chạy

Template chuẩn: `crates/opsense-components/src/processor.rs` (node
`processor_transform`). Cấu trúc macro sinh (`opsense-macros` +
`configurable_component.rs`) gồm 4 bước — **viết struct là đủ, macro làm phần
còn lại**:

| Bước | Do macro `#[transform]` | Bạn tự viết |
|---|---|---|
| 1. Validate field bắt buộc | thiếu `id`/`inputs` → compile error | — |
| 2. Sinh derives | `Serialize/Deserialize/Clone/Debug/PartialEq` + `deny_unknown_fields` | — |
| 3. Sinh `impl Identify` | `component_type()` + `clone_arc()` | — |
| 4. Đăng ký typetag | `#[typetag::serde(name=…)]` + registry toàn cục | —

Nếu component muốn script Rhai gọi được hàm của nó → thêm macro **`#[rhai]`**
(cùng crate `opsense-macros`, xem §4) — macro sinh **một block** `register_type`
+ `register_fn` (constructor bắt buộc + đủ accessor), **không có duplicate-panic**.

## 2. Cấu trúc tối thiểu (bắt chước `processor.rs`)

```rust
use opsense_components::prelude::{ComponentDeclaration, InputDeclaration};
use opsense_macros::transform;
use opsense_mlib::some_lib::MyAnalysis;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Component mới — transform observations.
#[transform]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MyTransform {
    pub id: String,
    pub inputs: Vec<InputDeclaration>,
    pub config: MyConfig,          // field component riêng, đọc từ config.toml
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MyConfig {
    pub threshold: f64,
}

impl MyTransform {
    pub fn new(id: String, inputs: Vec<InputDeclaration>, config: MyConfig) -> Self {
        Self { id, inputs, config }
    }
}
```

Khai trong `config.toml` của strategy:

```toml
[[pipeline.components]]
type = "my_transform"
id = "my-stage"
inputs = ["prev-node"]
config = { threshold = 0.5 }
```

> `type = "my_transform"` chính là tên typetag macro sinh từ tên struct. Serve
> tự nhận qua registry — **không sửa gì ở serve**.

## 3. Nếu component cần script-chạy-được (đính Rhai bindings)

Thay vì `register_fn!` rải rác gây duplicate, gắn **`#[rhai]`** lên struct
analysis (cùng crate `opsense-macros`). Macro đọc constructor + method, sinh:

```
register_type::<MyAnalysis>();
register_fn!("my_analysis", MyAnalysis::new /* constructor bắt buộc */);
register_fn!("num_buckets" | "total_from" | "has_transitions_from" | … , accessor);
```

Tất cả trong **một block** → hết panic trùng, hết "dead binding" (thiếu
constructor/accessor → compile error ngay). Script gọi:

```rhai
let a = my_analysis(grid, points, 300);   // constructor — luôn có
let n = num_buckets(a);                   // accessor — đủ
```

## 4. Test end-to-end (bắt buộc với mọi component mới)

Quy trình chuẩn — **run → chờ → đọc dữ liệu → timeout:**

1. Thêm node vào `config.toml` của một strategy (xem §2 hoặc `docs/RHAI.md`
   §4). Chạy `opsense serve` với config đó.
2. **Chờ một khoảng thời gian** đủ để pipeline chạy vài tick (vd 15–30s) —
   đừng đọc ngay, dữ liệu cần thời gian để vào cửa sổ.
3. **Đọc dữ liệu qua GraphQL** (`queryTimeseries` cho station_id của node) —
   xác nhận observation do component sinh ra đã vào trạm.
4. **Không có → báo timeout + xử lý:** kiểm tra server healthy, station đã
   đăng ký chưa (node có bật `station = true` hoặc có
   `timeseries_station_transform` sau node không), log warn lỗi script (retry
   tự heal). **Không panic ngầm** — timeout là hành vi kỳ vọng, log cảnh báo
   rồi xử lý ở batch kế.

Pattern test tham khảo: `crates/opsense-components/tests/*_pipeline.rs` +
`crates/opsense/tests/common/mod.rs` (`ensure_serve`/`ensure_pipeline` — chế độ
integration panic, local skip).

## 5. Checklist khi thêm component mới

- [ ] Struct có `#[transform]`/`#[sink]`/`#[source]` + `#[serde(deny_unknown_fields)]`.
- [ ] Có `impl new(...)` cho mọi type cần script tạo (constructor bắt buộc).
- [ ] Nếu cần script gọi → gắn `#[rhai]` (một block duy nhất, không đăng ký
      tay ở nơi khác).
- [ ] Khai node trong `config.toml` → chạy `opsense validate` → pass.
- [ ] Test run → chờ → đọc GraphQL → timeout+handle — xem §4.
- [ ] Cập nhật `docs/RHAI.md` (nếu thêm hàm script mới) hoặc `docs/GUIDE.md`
      (nếu thêm component pipeline).
