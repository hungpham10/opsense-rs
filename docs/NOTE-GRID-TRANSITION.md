# NOTE — tích hợp grid/transition Rhai vào `opsense-components`

> Ngày: 2026-09-23. Trạng thái: **bản ghi chú chính thức** — viết trước khi
> code, theo quyết định pivot: **KHÔNG hồi sinh `opsense-rhai` orphan, tích
> hợp thẳng vào `opsense-components`** (crate đã nối sẵn serve qua
> `use opsense_components as _;`). Bản này là **note đồng hành**: audit, quyết
> định, kế hoạch, checklist, thiết kế macro — để bất kỳ ai đọc lại đều biết vì
> sao repo có hình dạng này.

---

## 1. Bối cảnh

Câu hỏi gốc: `tools.rs` có khai `AnalysisGrid` + `TransitionAnalysis` và các
hàm của chúng để script Rhai gọi được không, và có test được end-to-end qua
`strategies/` không.

Audit cho kết quả 3 mức:
- **Đã khai:** cả hai type + gần như toàn bộ method accessor đều được
  `register_type`/`register_fn` trong `opsense-rhai/src/tools.rs`.
- **Executor không chạy được:** crate `opsense-rhai` là **orphan** — không có
  workspace dep nào link nó, `rhai` không có trong `[workspace.dependencies]`,
  path dep `opsense-mlib` trỏ sai, và nó tham chiếu `opsense_core::script`
  (module đã bị xoá trong đợt refactor; tôi đã phục hồi về
  `opsense-core/src/script.rs`).
- **API mang tính "chết" từ góc nhìn script:**
  1. `TransitionAnalysis` **không có constructor** được đăng ký → script
     không bao giờ tạo được instance → toàn bộ method API của nó vô dụng.
  2. Block registration `TransitionAnalysis` **copy-paste 4 lần** (tools.rs 4
     chỗ) → Rhai có thể panic khi build engine vì đăng ký trùng.
  3. Thiếu 3 accessor: `num_buckets`, `total_from`/`has_transitions_from`.
- **serve + CI hiện tại chỉ link `opsense-components`** — crate đang healthy,
  có sẵn cơ chế khai component trong `config.toml` (`processor.rs`,
  `station`, `signal`).

## 2. Quyết định (thay đổi hướng đi)

> **KHÔNG** hồi sinh + wire `opsense-rhai` nguyên crate vào workspace/serve.
> **Tích hợp thẳng grid/transition/engine Rhai vào `opsense-components`** —
> crate đã nối sẵn serve, healthy trong CI, có sẵn cơ chế khai component trong
> `config.toml` (giống hệt `processor.rs`).

Chia nhỏ việc theo 3 phần độc lập — **mỗi phần test riêng được**:

### A. Component `rhai_transform` trong `opsense-components`
- Người dùng khai component trong `config.toml` (cơ chế như `processor.rs`).
- Component đọc script `.rhai` từ path/kèm inline, chạy qua engine Rhai
  (tích hợp thẳng vào crate), output observations mới.
- **Test:** khai trong `config.toml` → chạy serve → chờ → đọc dữ liệu (GraphQL
  query) → không có → báo **timeout** + xử lý (retry/heal).

### B. Macro `#[rhai]` trong `opsense-macros` (xem §4)
- Đăng ký grid + transition **một block duy nhất** (hết duplicate-panic).
- Constructor bắt buộc (hết "dead API").
- Accessor sinh từ method của struct (hết thiếu).
- Đăng ký qua typetag → serve/CI **không cần sửa gì** (đã link components).

### C. Level-3 — được phép trễ, không phải đợt này: sandbox plugin §7,
pattern/catalog catalog-level (giữ nguyên trong note, không cam kết CI).

## 3. Kế hoạch (checklist)

### Phase 1 — macro `#[rhai]` + bindings lên `opsense-components` structs

#### Macro `#[rhai]` — ĐÃ XONG ✅ (commit tách riêng, CI xanh)
- [x] Nhân bản pattern `configurable_component.rs` →
  `crates/opsense-macros/src/rhai_bindings.rs` (`rhai_bindings_impl` là hàm
  thuần token-transform, test được như hàm thường — không cần runtime engine).
- [x] `pub fn rhai` export từ `opsense-macros/src/lib.rs` (bridge
  `proc_macro::TokenStream` ↔ `proc_macro2::TokenStream`).
- [x] Attr `#[rhai(constructor = "…", accessors("a", "b"))]` — validate
  constructor bắt buộc (rỗng/thiếu `=`/không hiểu ⇒ compile-error).
- [x] Sinh **MỘT** block `register_type_auto_name` + `register_fn`
  (constructor + toàn bộ accessor) → hết 4× duplicate-panic, hết "dead binding".
- [x] 7 unit test (`cargo test -p opsense-macros`) — 1 block duy nhất,
  constructor default `new`, attr rỗng, enum bị từ chối, error constructor /
  accessors / garbage. Toàn bộ test pass.
- [x] Contract sinh ra: `impl crate::script::RhaiBindings` ({`fn
  register(engine: &mut rhai::Engine)`}) — crate dùng resolve tại Phase 2.

#### Bindings lên structs (grid/transition) — CHƯA (đợt sau, sau khi `opsense-components` có `script::RhaiBindings` + `rhai::Engine`)
`AnalysisGrid` (analysis grid) — đã đủ accessor → chỉ cần one-block.
`TransitionAnalysis` — bổ sung constructor `transition_analysis(grid, points,
interval_secs)` reusing `parse_points` + sinh accessor: `num_buckets`,
`total_from`, `has_transitions_from`, `grid`.

### Phase 2 — wiring serve (tối thiểu, không đổi CI matrix)
- [x] `opsense-components` đã link serve (`use opsense_components as _;`).
- [x] `opsense-macros` đã có sẵn 6 proc-macro + lõi `configurable_component_impl`
      (đã có thêm `#[rhai]` — Phase 1).
- [ ] `opsense-components` có `crate::script` (`RhaiBindings` trait + registry
  typetag) để macro `#[rhai]` resolve; đính `#[rhai]` lên `TransitionAnalysis`
  + `AnalysisGrid`.
- [ ] `rhai_transform` node nhận script từ `config.toml` (pattern `processor.rs`)
  + engine build đăng ký block bindings.

### Phase 3 — test end-to-end (run → chờ → đọc → timeout)
1. Chiến lược mới với node `rhai_transform` + script `.rhai` gọi grid/transition
   (bổ sung vào `strategies/`, pattern các strategy CI hiện có).
2. **Chạy serve** → **chờ 1 khoảng thời gian** (vd 15–30s) cho pipeline vài tick.
3. **Đọc dữ liệu** qua GraphQL (`queryTimeseries`) — check exit code.
4. **Không có → báo timeout + xử lý:** log warn, kiểm tra serve healthy,
   station đã đăng ký chưa, script có lỗi không; retry ở tick kế — không
   panic ngầm; hết thời gian → trả timeout xử lý.

### Phase 4 — tài liệu
- [x] `docs/NOTE-GRID-TRANSITION.md` (bản này) — note đồng hành.
- [x] Viết lại `docs/RHAI.md` — hợp đồng script khớp API macro sinh.
- [x] Hướng dẫn viết component mới — bắt chước `processor.rs` (`docs/COMPONENT.md`).

## 4. Thiết kế macro `#[rhai]` (đã xác minh khả thi)

Đọc `opsense-macros/src/configurable_component.rs` (dòng 43–268) → pattern
macro đang chạy gồm: đọc attr → validate required fields → sinh derives →
`#[typetag::serde]` + `impl Identify`. **Một `#[rhai]` macro làm y hệt** nhưng
sinh registry Rhai thay vì component vector:

| Bước | `#[transform]` (đang dùng) | `#[rhai]` (sẽ thêm) |
|---|---|---|
| Đọc attr | `ComponentType::Transform` + `vec!["id","inputs"]` | loại binding + field bắt buộc |
| Validate | thiếu field → compile error | thiếu constructor → compile error |
| Sinh derives | `Serialize/Deserialize/...` + `deny_unknown_fields` | `register_type` + `register_fn` 1 block |
| Typetag | `#[typetag::serde(name=…)]` + `impl Identify` | registry Rhai → serve tự nhận |

API bindings (nguồn: `opsense-mlib` `grid.rs`/`transition.rs`):
- `grid_fit`/`grid_fit_values` (constructor), `num_cells`/`num_lines`/
  `grid_step`/`grid_cell`/`grid_crossings`/`grid_occupancy`/`grid_ranges`.
- `transition_analysis` (constructor phải đăng ký), `num_buckets`/`num_cells`/
  `interval_secs`/`intervals`/`interval_cells`/`dwell_times`/`mean_dwell`/
  `max_dwell`/`transitions`/`has_transitions_from`/`total_from`/
  `down`/`up`/`stay_probability`/`...ities`.

Script tham chiếu: `scripts/disk_spike_check.rhai` (baseline + spike).

---
