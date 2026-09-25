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
