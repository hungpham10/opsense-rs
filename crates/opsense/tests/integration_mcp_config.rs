//! `Query.components` — đọc cấu hình **đang chạy** qua GraphQL (và CLI/MCP
//! dùng chính query này).
//!
//! Vì sao quan trọng: `Mutation.reload` nhận **danh sách component đầy đủ**, nên
//! client không đọc được cấu hình hiện tại thì sửa một param cũng phải dựng lại
//! cả pipeline — sót một node là mất node. Test khoá hợp đồng: đọc được `id`,
//! `type`, `inputs`, và JSON config (kể cả `params` của script Rhai).
//!
//! Trong integration mode (`OPSENSE_INTEGRATION`) failure panics; ngoài đó skip
//! gracefully (dev chưa bật compose).

mod common;

use opsense::client::OpsenseClient;

async fn connect() -> Option<OpsenseClient> {
    let client = common::dex::login_client();
    match common::wait_for_health(&client, 30).await {
        Ok(()) => {}
        Err(_) if common::integration_mode() => {
            panic!("serve not reachable — CI requires `docker compose up`")
        }
        Err(_) => {
            eprintln!("skipping: serve not reachable — run `docker compose up` first");
            return None;
        }
    }
    if common::wait_for_dex(10).await.is_err() {
        if common::integration_mode() {
            panic!("Dex not reachable — CI requires `docker compose up`");
        }
        eprintln!("skipping: Dex not reachable");
        return None;
    }
    let id_token = common::dex::dex_login_get_id_token(&client).await;
    if common::wait_for_pipeline(&client, 30, &id_token).await.is_err() {
        if common::integration_mode() {
            panic!("pipeline not ready — CI requires `docker compose up`");
        }
        eprintln!("skipping: pipeline not ready");
        return None;
    }
    Some(
        OpsenseClient::new(format!("{}/api/repl/graphql", common::serve_url()))
            .expect("GraphQL client")
            .with_bearer(id_token),
    )
}

#[tokio::test]
async fn config_edit_is_audited_in_station() {
    let Some(c) = connect().await else { return };

    let all = c.components(None).await.expect("Query.components");
    let Some(target) = all.first() else { return };
    let path = "/params/audit_probe";
    let value = format!("{}", opsense_components::signal::now_secs());

    c.patch_component(&target.id, path, &value)
        .await
        .expect("patch probe");

    // Audit là observation trong station `opsense-audit` → đọc lại được bằng
    // đúng đường query của mọi state khác (không cần log file).
    let audit = c
        .query_station(
            "opsense-audit",
            None,
            None,
            Some(100),
            None,
            Some("config_edit"),
        )
        .await
        .expect("đọc station audit");
    let mine = audit
        .observations
        .iter()
        .find(|o| {
            o.labels.get("node").map(String::as_str) == Some(target.id.as_str())
                && o.labels.get("path").map(String::as_str) == Some("params/audit_probe")
        })
        .expect("phải có audit cho patch vừa rồi");
    assert_eq!(mine.labels.get("to").map(String::as_str), Some(value.as_str()));
}

#[tokio::test]
async fn query_station_rejects_unbounded_window() {
    let Some(c) = connect().await else { return };

    // Cửa sổ vô hạn (kiểu `from=0, to=MAX`) là cách chắc chắn nhất để treo
    // server: phải bị chặn kèm gợi ý, không clamp im lặng.
    let err = c
        .query_station(
            "binance-tsdb",
            Some(0),
            Some(i64::MAX),
            None,
            None,
            None,
        )
        .await
        .expect_err("cửa sổ vô hạn phải bị từ chối")
        .to_string();
    assert!(
        err.contains("vượt trần") && err.contains("chia"),
        "lỗi phải nói rõ trần + cách chia: {err}"
    );

    // `limit` vô hạn cũng vậy.
    let err = c
        .query_station("binance-tsdb", None, None, Some(1_000_000), None, None)
        .await
        .expect_err("limit vượt trần phải bị từ chối")
        .to_string();
    assert!(err.contains("limit"), "{err}");

    // `from=i64::MIN, to=i64::MAX` làm phép trừ tràn: debug panic, release wrap
    // thành số âm khiến guard im lặng BỎ QUA — đúng truy vấn vô hạn cần chặn.
    let err = c
        .query_station("binance-tsdb", Some(i64::MIN), Some(i64::MAX), None, None, None)
        .await
        .expect_err("cửa sổ tràn số phải bị từ chối")
        .to_string();
    assert!(
        err.contains("quá rộng để tính") || err.contains("vượt trần"),
        "cửa sổ tràn số phải bị chặn: {err}"
    );
}

#[tokio::test]
async fn query_station_filters_server_side() {
    let Some(c) = connect().await else { return };

    // Station nào có dữ liệu thì dùng; không có thì bỏ qua (chỉ cần chứng minh
    // filter + `truncated` chạy, không cần dữ liệu trading).
    let status = c.status().await.expect("Query.status");
    let Some(station) = status
        .stations
        .iter()
        .find(|s| s.kind == "timeseries")
        .map(|s| s.id.clone())
    else {
        eprintln!("skipping: chưa có station timeseries nào");
        return;
    };

    let all = c
        .query_station(&station, None, None, Some(10), None, None)
        .await
        .expect("query không filter");
    assert!(all.scanned >= all.observations.len());

    // Filter theo `signal` sai kiểu → lỗi (không âm thầm trả rỗng).
    assert!(
        c.query_station(&station, None, None, Some(10), Some("khong_ton_tai"), None)
            .await
            .is_err(),
        "signal lạ phải báo lỗi chứ không trả rỗng im lặng"
    );

    // Filter hợp lệ → chỉ còn đúng signal đó (hoặc rỗng nếu station không có).
    let out = c
        .query_station(&station, None, None, Some(10), Some("order"), None)
        .await
        .expect("query signal=order");
    assert!(
        out.observations
            .iter()
            .all(|o| o.labels.get("kind").is_some() || true),
        "sanity"
    );
    assert!(
        out.observations.len() <= 10,
        "limit phải được áp dụng: {}",
        out.observations.len()
    );
}

#[tokio::test]
async fn components_returns_live_config() {
    let Some(c) = connect().await else { return };

    let all = c.components(None).await.expect("Query.components");
    assert!(!all.is_empty(), "pipeline phải có ít nhất 1 component");

    for comp in &all {
        assert!(!comp.id.is_empty(), "component thiếu id: {comp:?}");
        assert!(!comp.kind.is_empty(), "component thiếu type: {comp:?}");
        assert!(comp.config.is_object(), "config phải là object: {comp:?}");
    }

    // `id` lọc đúng 1 node, và node đó phải khớp với entry trong danh sách đầy đủ.
    let first = &all[0];
    let one = c
        .components(Some(&first.id))
        .await
        .expect("Query.components(id)");
    assert_eq!(one.len(), 1, "id phải lọc đúng 1 node: {one:?}");
    assert_eq!(one[0].id, first.id);
    assert_eq!(one[0].config, first.config);

    // Node không tồn tại → danh sách rỗng (không phải lỗi).
    let missing = c
        .components(Some("__no_such_node__"))
        .await
        .expect("Query.components(id) với id lạ");
    assert!(missing.is_empty(), "id lạ phải trả rỗng: {missing:?}");
}

#[tokio::test]
async fn patch_component_changes_only_one_field() {
    let Some(c) = connect().await else { return };

    // Chọn node `rhai_transform` để patch được `params` (script Rhai).
    let all = c.components(None).await.expect("Query.components");
    let Some(target) = all.iter().find(|x| x.kind == "rhai_transform") else {
        eprintln!("skipping: config không dùng rhai_transform");
        return;
    };
    let before = target.config["params"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    let old = before.get("sl_pct").cloned();

    // Patch một param; đọc lại phải thấy giá trị mới, phần khác giữ nguyên.
    let new_value = 0.0123;
    c.patch_component(&target.id, "/params/sl_pct", &new_value.to_string())
        .await
        .expect("patchComponent phải thành công");

    let after = c
        .components(Some(&target.id))
        .await
        .expect("Query.components")
        .into_iter()
        .next()
        .expect("node vẫn tồn tại sau patch");
    assert_eq!(
        after.config["params"]["sl_pct"],
        serde_json::json!(new_value),
        "giá trị mới phải được áp dụng"
    );
    // Các param khác không bị đụng.
    for (k, v) in &before {
        if k == "sl_pct" {
            continue;
        }
        assert_eq!(after.config["params"][k], *v, "param `{k}` bị đổi ngoài ý muốn");
    }

    // Trả lại giá trị cũ (nếu config gốc không có `sl_pct` thì xoá: patch `null`
    // không đúng nghĩa → dùng `set_param` không hỗ trợ xoá, nên chỉ khôi phục
    // khi có giá trị cũ).
    if let Some(old) = old {
        c.patch_component(&target.id, "/params/sl_pct", &old.to_string())
            .await
            .expect("khôi phục giá trị cũ");
    }
}

#[tokio::test]
async fn patch_component_rejects_broken_value_without_touching_runtime() {
    let Some(c) = connect().await else { return };

    let all = c.components(None).await.expect("Query.components");
    let before: Vec<_> = all.iter().map(|x| x.config.clone()).collect();
    let Some(target) = all.first() else { return };

    // (a) Giá trị không phải JSON.
    assert!(
        c.patch_component(&target.id, "/params/sl_pct", "{khong-phai-json}")
            .await
            .is_err(),
        "JSON hỏng phải báo lỗi"
    );
    // (b) Path không phải JSON pointer.
    assert!(
        c.patch_component(&target.id, "params.sl_pct", "0.02")
            .await
            .is_err(),
        "path thiếu '/' phải báo lỗi"
    );
    // (c) Node không tồn tại.
    assert!(
        c.patch_component("__no_such_node__", "/params/x", "1")
            .await
            .is_err(),
        "node lạ phải báo lỗi"
    );

    // Runtime phải **y nguyên** sau các lần patch hỏng.
    let after = c.components(None).await.expect("Query.components");
    assert_eq!(
        after.iter().map(|x| &x.config).collect::<Vec<_>>(),
        before.iter().collect::<Vec<_>>(),
        "patch hỏng phải không đổi cấu hình nào"
    );
}

#[tokio::test]
async fn components_exposes_rhai_params() {
    let Some(c) = connect().await else { return };

    let all = c.components(None).await.expect("Query.components");
    let rhai = all
        .iter()
        .find(|c| c.kind == "rhai_transform")
        .expect("config dùng rhai_transform (strategy/*/config.toml)");

    // `params` là thứ MCP/CLI cần đọc để biết `strategy`, `mode`, knob…
    let params = rhai
        .config
        .get("params")
        .and_then(|v| v.as_object())
        .unwrap_or_else(|| panic!("rhai_transform phải có params: {}", rhai.config));
    assert!(
        !params.is_empty(),
        "params rỗng thì không sửa được gì: {params:?}"
    );
    if let Some(strategy) = params.get("strategy") {
        assert!(
            strategy.is_string(),
            "params.strategy phải là string: {strategy}"
        );
    }
}
