# NOTE — lệnh cần ACID ⇒ thêm backend quan hệ cho `TimeseriesStorage`

> Ngày: 2026-09-28. Ghi chú thiết kế, **chưa** code.
>
> Liên quan: [`NOTE-GRID-TRANSITION.md`](./NOTE-GRID-TRANSITION.md) (lệnh + cursor
> T+N trong station), tầng cluster đang dựng (`opsense-mlib::gossip` / `raft`).

---

## 1. Vấn đề

Lệnh hôm nay là **observation trong station** (`signal = "order"`, labels
`order_id, dtype, grid, level, size, sl, tp, status, pnl_pct, candle_seq` —
`crates/opsense-rhai/src/orders.rs:45-53`). Tức nó đi qua `TimeseriesStorage`.

Ba backend có sẵn: `InMemoryStorage`, `LakehouseStorage` (parquet + S3),
`RedisStorage`, `SqliteStorage` (SQLite, file của `Search`).

**Không backend nào có transaction nghiệp vụ.** Cụ thể với parquet + S3, đo từ
code:

- `LakehouseStorage::append` (`storage/parquet.rs:1644`) chỉ ghi thêm một entry
  WAL rồi flush ra **file batch mới** (`ts/blk=<id>/batch-<millis>.parquet`).
  Append tức **tạo object mới** ⇒ không thể có unique constraint, không thể
  cập nhật "lệnh này đã gửi sàn" một cách nguyên tử, và không thể chặn hai
  tiến trình ghi trùng.
- S3 không có transaction nhiều object, và việc list/overwrite có tính
  eventually-consistent ⇒ cả "đọc lại sau khi ghi" cũng không chắc.

Hệ quả trực tiếp: **không có chỗ nào để nói "lệnh này đã có, đừng ghi lần
nữa"**. Đây là rủi ro mất tiền thật, không phải rủi ro dữ liệu.

## 2. Quyết định

Lệnh nằm trên **backend quan hệ** (Postgres trước; MySQL nếu deployment nào
cần). Không tách riêng bảng lệnh — đặt station `orders` lên backend quan hệ là
xong, nhờ vậy `queryTimeseries`, MCP tool `opsense_orders`, audit… **không đổi
đường đọc**.

"Chỉ master ra lệnh" là quy tắc **chi phí/simplification** (một node nói chuyện
với sàn), không phải điều kiện đúng nghiệm: nếu ghi bằng khoá nghiệp vụ +
`ON CONFLICT DO NOTHING`, và lệnh gửi sàn được kéo bằng
`SELECT … FOR UPDATE SKIP LOCKED`, thì **hai master cùng chạy vẫn không đặt
trùng** — DB tự phân quyền. ⇒ Phần lệnh **không cần chờ Raft**; Raft sau đó
chỉ còn việc điều phối tính toán, nơi dedup khó hơn vì kết quả không nằm trong
một hàng DB.

## 3. Khoá nghiệp vụ — chưa chốt, và `order_id` hiện tại **không dùng được**

`orders.rs:545` sinh id kiểu `format!("o{ts}-{next_id}")`, với `next_id` lấy từ
`tail_num(&id) + 1` tức **đếm trên lịch sử station** (dòng 408-409). Nên
`order_id` thuộc **vị trí**, không thuộc ý định: mất station rồi chạy lại trên
cùng nến thì cùng một ý định ra id khác ⇒ khoá theo `order_id` là khoá theo
sự tình cờ và lệnh trùng lọt qua.

Ứng viên có cơ sở từ code sẵn có: **`(strategy, grid_index, level_index,
candle_seq)`**

- `open_orders` đã khoá lệnh đang mở theo `(grid_index, level_index)`
  (`orders.rs:359`, "đúng khoá mà `open_orders` dùng để chặn đặt trùng").
- `candle_seq` là thứ tự nến toàn cục, **không reset khi rebuild**
  (`opsense-qlib/src/session.rs:53`).

⇒ `(cell, level, candle)` xác định **ý định** và không đổi khi chạy lại hay
restart.

**Cần chốt trước khi viết unique index:** có hợp lệ một trường hợp *cùng 4 thành
phần đó mà vẫn phải ra hai lệnh* không (tách lô, retry lệnh bị từ chối)? Nếu có
thì phải thêm thành phần vào khoá.

## 4. Chỗ thiếu trong API (đây là việc chính)

Trait `TimeseriesStorage` (`storage.rs:121`) có:

```rust
async fn append(&self, series: &[u8], timestamp: u64, value: &[u8]) -> Result<()>
```

Không có chỗ cho khoá nghiệp vụ, nên **không thể** diễn đạt "ghi nếu chưa có".
Đề xuất: thêm method có điều kiện

```rust
/// Ghi nếu `key` chưa tồn tại. `Ok(false)` = đã có, không ghi.
async fn append_unique(&self, series: &[u8], ts: u64, value: &[u8], key: &[u8]) -> Result<bool>
```

- Chỉ backend quan hệ override; backend khác trả `StorageError` "không hỗ trợ"
  (không được **âm thầm** ghi thường — lúc đó lệnh trùng quay lại).
- Ràng buộc `UNIQUE(key)` nằm ở tầng DB, chỗ ACID có ý nghĩa.

## 5. Hệ quả phải chốt

Đặt `orders` lên Postgres ⇒ nó **không còn nằm trong lake parquet/S3** như hiện
tại. Nghĩa là lịch sử lệnh lâu dài cần đường riêng. Có thể chấp nhận (lệnh là
dữ liệu nhỏ, khác bản chất với candle), nhưng đây là quyết định chứ không phải
hệ quả miễn phí.

## 6. Checklist

- [ ] Chốt khoá nghiệp vụ (§3).
- [ ] Thêm `append_unique` vào `TimeseriesStorage` + test cho **cả** backend cũ
      (phải trả lỗi "không hỗ trợ", không được fallback).
- [ ] Backend Postgres (`sqlx`): bảng + `UNIQUE(key)` + `append_unique`.
- [ ] Chuyển station `orders` sang backend Postgres; chạy lại
      `integration_mcp_config` (tool `opsense_orders`) + e2e binance.
- [ ] Test **hai tiến trình cùng ghi** ⇒ còn đúng một lệnh (đây là thứ cần bảo
      vệ nhiều nhất).
- [ ] Chốt đường lịch sử lệnh dài hạn (§5).
