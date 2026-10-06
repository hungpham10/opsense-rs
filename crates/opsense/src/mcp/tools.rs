//! MCP tool implementations. Each tool = 1 GraphQL round-trip via `OpsenseClient`.
//!
//! Returns `Result<String, String>` — `String` impls `IntoContents` so the
//! `#[tool]` macro auto-wraps it as a `CallToolResult::success`. The `Err`
//! arm is rendered as a text error message (still success transport-wise;
//! MCP doesn't distinguish).

use crate::client::OpsenseClient;
use crate::client::graphql::ComponentInput;

fn json_dump<T: serde::Serialize>(v: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(v)
}

pub async fn status(client: &OpsenseClient) -> Result<String, String> {
    client
        .status()
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|s| json_dump(&s).map_err(|e| format!("{e}")))
}

pub async fn attributes(client: &OpsenseClient) -> Result<String, String> {
    client
        .attributes()
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|m| json_dump(&m).map_err(|e| format!("{e}")))
}

/// Cấu hình **đang chạy** (`params`, `script_path`, …). Bỏ `id` → tất cả node.
///
/// Đọc trước khi sửa: `opsense_reload` nhận danh sách node đầy đủ, nên không
/// đọc thì sửa một param cũng phải dựng lại cả pipeline.
pub async fn get_config(client: &OpsenseClient, id: Option<&str>) -> Result<String, String> {
    client
        .components(id)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|c| json_dump(&c).map_err(|e| format!("{e}")))
}

pub async fn set_attribute(
    client: &OpsenseClient,
    name: &str,
    value: &str,
) -> Result<String, String> {
    client
        .set_attribute(name, value)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|r| json_dump(&r).map_err(|e| format!("{e}")))
}

pub async fn remove_attribute(client: &OpsenseClient, name: &str) -> Result<String, String> {
    client
        .remove_attribute(name)
        .await
        .map(|removed| format!("removed={removed}"))
        .map_err(|e| format!("{e:#}"))
}

/// Xoá sạch **một** station (RAM + storage).
///
/// Node sở hữu station **vẫn chạy** và ghi lại dữ liệu mới tới, nên đây là
/// "xoá sạch rồi để nó tự lấp lại" chứ không phải xoá vĩnh viễn.
pub async fn clear_station(client: &OpsenseClient, id: &str) -> Result<String, String> {
    client
        .clear_station(id)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|r| render_clear_result(r, false))
}

/// Xoá sạch mọi station. `confirm` bắt buộc bằng `true` vì mất cả lệnh đang
/// mở, cursor T+N và toàn bộ lịch sử của process.
pub async fn clear_all_stations(
    client: &OpsenseClient,
    confirm: bool,
) -> Result<String, String> {
    if !confirm {
        return Err(
            "clear_all_stations xoá MẌT VÀNG dữ liệu của process (lệnh đang mở, \
             cursor T+N, plan, lịch sử station) — node vẫn chạy và sẽ ghi lại từ \
             dữ liệu mới tới. Gọi lại với confirm = true nếu bạn thật sự muốn."
                .to_string(),
        );
    }
    client
        .clear_all_stations()
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|r| render_clear_result(r, true))
}

/// Gộp `cleared`/`failed` thành bản tóm tắt.
///
/// Liệt kê từng station khi có **nhiều** cái, hoặc khi `bulk` bật (lệnh xoá
/// tất cả thì caller cần biết đã xoá đúng những gì). Một lệnh đơn lẻ thì chỉ
/// một dòng là đủ.
fn render_clear_result(
    r: crate::client::graphql::ClearStationResult,
    bulk: bool,
) -> Result<String, String> {
    let mut out = String::new();
    if bulk || r.cleared.len() > 1 {
        for c in &r.cleared {
            out.push_str(&format!("- {} ({}): cleared\n", c.id, c.kind));
        }
    } else if let Some(c) = r.cleared.first() {
        out.push_str(&format!("- {} ({}): cleared\n", c.id, c.kind));
    }
    if !r.failed.is_empty() {
        out.push_str("FAILED:\n");
        for f in &r.failed {
            out.push_str(&format!("- {f}\n"));
        }
    }
    out.push_str(&format!("cleared={} failed={}", r.cleared.len(), r.failed.len()));
    Ok(out)
}

pub async fn query_timeseries(
    client: &OpsenseClient,
    node: &str,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
    limit: Option<i64>,
    signal: Option<&str>,
    label_kind: Option<&str>,
    status: Option<&str>,
) -> Result<String, String> {
    client
        .query_station(node, from_ts, to_ts, limit, signal, label_kind, status)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|c| json_dump(&c).map_err(|e| format!("{e}")))
}

/// State giao dịch trong một station: lệnh (`signal = "order"`) **và** cursor T+N
/// (`labels.kind = "trading_step"`).
///
/// Cursor cố ý **không** lọc theo `signal` được: nó là `signal = "summary"`, nên
/// lọc `signal = "order"` sẽ âm thầm rơi mất cursor — tức mất đúng thứ agent
/// cần để biết "T+N đã chạy tới nến nào". Vì vậy lấy cả hai loại rồi lọc ở đây.
/// `status` chỉ áp cho lệnh (cursor không có status) và **đã** lọc server-side
/// qua tham số `status` của `queryTimeseries` — trước đây phải kéo hết về mới
/// lọc được, nên `limit` cắt cụt trước khi lọc.
/// Lệnh + cursor T+N của một station.
///
/// `status` lọc **sau** khi đã gộp theo `order_id` (xem bước 2 bên dưới): station
/// append-only nên bản `open` vẫn còn sau khi lệnh đóng. Lọc ở server *trước*
/// sẽ trả lệnh đã đóng là đang mở.
pub async fn orders(
    client: &OpsenseClient,
    node: &str,
    status: Option<&str>,
    from_ts: Option<i64>,
    to_ts: Option<i64>,
) -> Result<String, String> {
    // Cố tình **không** truyền `status` xuống server — xem bước 3.
    let raw = query_timeseries(client, node, from_ts, to_ts, Some(2000), None, None, None).await?;
    let mut v: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| format!("query result: {e}"))?;
    let order_filter = status.map(|s| s.to_string());
    if let Some(obs) = v
        .get_mut("observations")
        .and_then(|o| o.as_array_mut())
    {
        // 1. Giữ cursor T+N và lệnh, bỏ quan sát không liên quan.
        obs.retain(|o| {
            let kind = o.get("labels").and_then(|l| l.get("kind")).and_then(|k| k.as_str());
            let signal = o.get("signal").and_then(|s| s.as_str());
            if kind == Some("trading_step") {
                return true; // cursor T+N — không có `status`
            }
            signal == Some("order")
        });

        // 2. Gộp lệnh theo `order_id`, lấy bản ghi **mới nhất**.
        //
        // Station append-only: đóng lệnh là observation **mới** cùng
        // `order_id`, bản `open` cũ **vẫn còn** với `status = "open"`. Lọc
        // `status` ở server *trước* bước này là sai: nó thấy bản `open` cũ và
        // trả về một lệnh **đã đóng** là đang mở.
        //
        // Đã đo: lọc thô cho 2 lệnh mở (647 + 2103 USD) trong khi thật chỉ còn
        // 1 (2103 USD) ⇒ vị thế bị double-count.
        let mut latest: std::collections::HashMap<String, (i64, usize)> = Default::default();
        for (i, o) in obs.iter().enumerate() {
            let Some(id) = o
                .get("labels")
                .and_then(|l| l.get("order_id"))
                .and_then(|v| v.as_str())
            else {
                continue; // cursor T+N — không có `order_id`
            };
            let ts = o.get("ts").and_then(|t| t.as_i64()).unwrap_or(0);
            match latest.get(id) {
                // `>=` để bản ghi sau cùng thắng khi trùng `ts`.
                Some(&(prev_ts, _)) if prev_ts > ts => {}
                _ => {
                    latest.insert(id.to_string(), (ts, i));
                }
            }
        }
        let keep: std::collections::HashSet<usize> =
            latest.values().map(|(_, i)| *i).collect();
        let mut i = 0;
        obs.retain(|_| {
            let k = keep.contains(&i);
            i += 1;
            k
        });

        // 3. Lọc `status` **sau** khi đã gộp — lúc này mới đúng nghĩa.
        if let Some(want) = &order_filter {
            obs.retain(|o| {
                let kind = o.get("labels").and_then(|l| l.get("kind")).and_then(|k| k.as_str());
                if kind == Some("trading_step") {
                    return true; // cursor không có `status`
                }
                o.get("labels")
                    .and_then(|l| l.get("status"))
                    .and_then(|s| s.as_str())
                    == Some(want.as_str())
            });
        }
    }
    json_dump(&v).map_err(|e| format!("{e}"))
}

/// Sửa **một** thành phần của một node (vd `params.sl_pct` → `0.02`).
///
/// Đường sửa mặc định: `opsense_reload` nhận danh sách node **đầy đủ**, thiếu một
/// node là mất node đó. Ở đây server tự đọc cấu hình hiện tại, patch đúng một chỗ,
/// validate lại toàn bộ rồi mới reload.
pub async fn set_param(
    client: &OpsenseClient,
    id: &str,
    path: &str,
    value: &str,
) -> Result<String, String> {
    client
        .patch_component(id, path, value)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|r| json_dump(&r).map_err(|e| format!("{e}")))
}

/// `components_json` is a JSON array of component objects. Each element:
/// `{ "type": "...", "id": "...", "config": {...}, "inputs": [...] }`.
pub async fn reload_from_json(
    client: &OpsenseClient,
    components_json: &str,
) -> Result<String, String> {
    let components: Vec<serde_json::Value> =
        serde_json::from_str(components_json).map_err(|e| format!("invalid JSON array: {e}"))?;
    let mut parsed: Vec<ComponentInput> = Vec::with_capacity(components.len());
    for (i, v) in components.into_iter().enumerate() {
        match serde_json::from_value::<ComponentInput>(v) {
            Ok(c) => parsed.push(c),
            Err(e) => return Err(format!("component[{i}]: {e}")),
        }
    }
    client
        .reload(parsed)
        .await
        .map_err(|e| format!("{e:#}"))
        .and_then(|r| json_dump(&r).map_err(|e| format!("{e}")))
}
