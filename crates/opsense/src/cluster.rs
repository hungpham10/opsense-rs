//! Vận chuyển cho tầng **gossip** — quan sát: ai còn sống.
//!
//! [`opsense_mlib::gossip`] giữ logic thuần (LWW hợp nhất info, ba mức
//! `alive → suspect → dead`) và **không** I/O. File này là phần I/O: gọi
//! `/health/live` của peer mỗi vòng, rồi đưa kết quả về cho lib.
//!
//! **Không có quyết định nào ở đây.** "Ai là master" thuộc
//! [`opsense_mlib::raft`], và tuyệt đối không suy ra từ view của gossip — mạng
//! đứt đôi thì hai bên đều tin mình còn sống, nên chỉ Raft mới loại được.
//!
//! Tách I/O khỏi state để test được không cần mạng: [`Mesh::apply_probe_results`]
//! là **toàn bộ** phần quyết định của một vòng, và nó nhận `now_ms` từ ngoài
//! — test không phải ngủ thật để đi qua cửa sổ thời gian. [`Mesh::tick`] chỉ
//! là "gọi HTTP, lấy đồng hồ, đưa vào đó".

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use opsense_core::GossipConfig;
use opsense_mlib::gossip::{Gossip, Info, NodeInfo};

/// Một vòng quan sát đã xong — để test assert được mà không cần mạng.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickOutcome {
    /// Peer trả lời lần này.
    pub alive: Vec<String>,
    /// Không trả lời lần này (chưa chết — xem `suspects`/`dead`).
    pub silent: Vec<String>,
    /// Vừa chuyển sang nghi ngờ.
    pub suspects: Vec<String>,
    /// Vừa bị kết luận chết.
    pub dead: Vec<String>,
}

impl TickOutcome {
    /// Không có chuyển trạng thái nào ⇒ view đứng yên.
    #[must_use]
    pub fn is_quiet(&self) -> bool {
        self.suspects.is_empty() && self.dead.is_empty()
    }
}

/// View của node này về mesh + cấu hình để quét tiếp.
pub struct Mesh {
    cfg: GossipConfig,
    gossip: Mutex<Gossip>,
    /// Mốc 0 của đồng hồ đơn điệu. `Gossip` so sánh thời gian bằng đồng hồ
    /// này, nên **không** dùng wall clock: đổi múi giờ giữa chừng sẽ làm cả
    /// view bị coi là đã im lặng hàng giờ.
    started: Instant,
    http: reqwest::Client,
}

impl std::fmt::Debug for Mesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mesh")
            .field("node_id", &self.cfg.node_id)
            .field("own_url", &self.cfg.own_url)
            .field(
                "peers",
                &self.gossip.lock().map_or(0, |g| g.nodes().len()),
            )
            .finish()
    }
}

impl Mesh {
    /// Dựng mesh từ config **đã áp env** (`Config::resolved_gossip`).
    ///
    /// `Err` = bật mesh nhưng cấu hình hỏng; `Ok(None)` = không bật mesh. Phân
    /// biệt này quan trọng: cấu hình hỏng phải nổi lên lúc khởi động chứ không
    /// được lặng lẽ chạy chế độ đơn lẻ — lúc đó người vận hành tưởng đang có
    /// HA mà thật ra không.
    pub fn from_config(cfg: GossipConfig) -> Result<Option<Self>, String> {
        cfg.validate().map_err(|e| e.to_string())?;
        if !cfg.enabled() {
            return Ok(None);
        }
        // Cài provider trước khi dựng client: peer có thể ở `https://` mà
        // `Client::builder()` panic `No provider set` nếu chưa ai cài (xem
        // `opsense_mlib::tls`).
        opsense_mlib::tls::install_default_crypto_provider();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.tick_secs.clamp(1, 30)))
            .build()
            .map_err(|e| format!("không dựng được HTTP client cho mesh: {e}"))?;
        // Seed khai trước: node biết peer trước khi gọi health lần nào, nên
        // tick đầu tiên đã có việc để làm.
        let mut gossip = Gossip::new(&cfg.node_id, &cfg.own_url, cfg.quorum, 0);
        for seed in cfg.seed_urls() {
            gossip.add_peer(seed, node_id_of(seed), 0);
        }
        Ok(Some(Self {
            cfg,
            gossip: Mutex::new(gossip),
            started: Instant::now(),
            http,
        }))
    }

    pub fn node_id(&self) -> &str {
        &self.cfg.node_id
    }

    pub fn own_url(&self) -> &str {
        &self.cfg.own_url
    }

    /// Milli-giây kể từ lúc mesh dựng — đồng hồ đơn điệu mà `Gossip` dùng.
    fn now_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Snapshot view cho `GET /api/cluster/v1/nodes`, sắp theo `node_id`.
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.gossip.lock().map(|g| g.nodes()).unwrap_or_default()
    }

    /// View đã ổn định đủ lâu chưa — dùng để *trì hoãn* hành động, không để
    /// quyết định master.
    #[must_use]
    pub fn is_settled_at(&self, now_ms: u64) -> bool {
        self.gossip
            .lock()
            .map(|g| g.is_settled(now_ms, self.cfg.settle_secs))
            .unwrap_or(false)
    }

    /// View đã ổn định đủ `settle_secs` chưa, theo đồng hồ thật của tiến trình.
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.is_settled_at(self.now_ms())
    }

    /// Node nào đang chạy version khác với mình.
    ///
    /// Thứ *quan sát được* và hành động được ngay: hai bản build lệch nhau thì
    /// báo ra, chứ không im lặng rồi hỏng lúc đọc dữ liệu chéo.
    #[must_use]
    pub fn version_mismatches(&self) -> BTreeMap<String, String> {
        let Ok(g) = self.gossip.lock() else { return BTreeMap::new() };
        let own = g.published().version.clone();
        g.nodes()
            .into_iter()
            .filter(|n| !n.info.version.is_empty() && n.info.version != own)
            .map(|n| (n.node_id, n.info.version))
            .collect()
    }

    /// Công bố version của chính mình (để peer nhìn thấy qua `InfoState`).
    pub fn publish(&self, info: Info) {
        if let Ok(mut g) = self.gossip.lock() {
            g.publish(info);
        }
    }

    /// Cặp `(node_id, url)` cần gọi `/health/live` ở vòng tới.
    fn probe_targets(&self) -> Vec<(String, String)> {
        self.gossip
            .lock()
            .map(|g| {
                g.probe_pairs()
                    .into_iter()
                    .map(|(id, url)| (id.to_string(), url.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Gọi `/health/live` của mọi peer chưa chết, **song song**.
    ///
    /// Song song vì `tick_secs` là nhịp của cả vòng: probe tuần tự với N peer
    /// thì vòng dài ra N lần, và node im lặng bị đánh dấu chết muộn — tức đo
    /// sai ngay thứ mà cả module sinh ra để đo.
    async fn probe_all(&self) -> Vec<(String, bool)> {
        let mut set = tokio::task::JoinSet::new();
        for (id, url) in self.probe_targets() {
            let client = self.http.clone();
            set.spawn(async move {
                let ok = client
                    .get(format!("{}/health/live", url.trim_end_matches('/')))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success());
                (id, ok)
            });
        }
        let mut out = Vec::new();
        while let Some(joined) = set.join_next().await {
            if let Ok(pair) = joined {
                out.push(pair);
            }
        }
        out
    }

    /// Áp kết quả probe, rồi nghi ngờ và tái thu. Đây là **toàn bộ** phần quyết
    /// định của một vòng; `now_ms` đưa vào từ ngoài để test đi qua cửa sổ thời
    /// gian không cần ngủ.
    pub fn apply_probe_results(&self, results: &[(String, bool)], now_ms: u64) -> TickOutcome {
        let mut out = TickOutcome::default();
        let Ok(mut g) = self.gossip.lock() else { return out };
        let urls: BTreeMap<String, String> = g
            .probe_pairs()
            .into_iter()
            .map(|(id, url)| (id.to_string(), url.to_string()))
            .collect();
        for (id, ok) in results {
            let Some(url) = urls.get(id) else {
                // Kết quả của node ta không biết ⇒ bỏ qua. Ghi vào node khác
                // theo tên trùng là loại bug khó tìm nhất trong phần này.
                tracing::debug!(node = %id, "gossip: bỏ qua kết quả của node không biết");
                continue;
            };
            if *ok {
                g.mark_alive(url, id, now_ms);
                out.alive.push(id.clone());
            } else {
                out.silent.push(id.clone());
            }
        }
        out.suspects = g.tick_suspects(self.cfg.suspect_secs.saturating_mul(1_000), now_ms);
        out.dead = g.tick_reap(self.cfg.dead_secs.saturating_mul(1_000), now_ms);
        out
    }

    /// Một vòng quan sát thật: probe rồi áp kết quả.
    pub async fn tick(&self) -> TickOutcome {
        let results = self.probe_all().await;
        let out = self.apply_probe_results(&results, self.now_ms());
        for id in &out.dead {
            tracing::warn!(node = %id, "gossip: node không trả lời đủ lâu, kết luận chết");
        }
        for id in &out.suspects {
            tracing::info!(node = %id, "gossip: node im lặng, đang nghi ngờ");
        }
        if !out.is_quiet() {
            tracing::info!(suspect = ?out.suspects, dead = ?out.dead, "gossip: view đổi");
        }
        out
    }

    /// Chạy vòng quan sát mãi. Trả `JoinHandle` để caller `abort()` khi serve
    /// dừng — task này không tự biết lúc nào nên dừng.
    pub fn spawn(self: &std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        let mesh = self.clone();
        let period = Duration::from_secs(self.cfg.tick_secs.max(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            // Node chậm hơn nhịp thì đừng dồn lại thành loạt: nghĩa là ta vừa
            // bị chậm, và quét bù không làm ta kịp hơn.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                mesh.tick().await;
            }
        })
    }
}

/// `node_id` đoán từ URL seed: `http://node-b:8080` → `node-b`.
///
/// Chỉ là **phỏng đoán tốt nhất có thể** cho tới khi node đó tự đăng ký
/// (`POST /internal/join`, PR sau). Đoán sai thì mất một vòng `mark_alive` —
/// tự chữa, và tệ hơn nhiều so với việc đoán sai rồi im lặng.
fn node_id_of(url: &str) -> &str {
    let rest = url
        .split_once("://")
        .map_or(url, |(_, r)| r)
        .split('/')
        .next()
        .unwrap_or(url);
    rest.split(':').next().unwrap_or(rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_mlib::gossip::InfoState;

    /// Thời gian của **tick đầu tiên**, không phải 0: `Gossip` seed peer với
    /// `last_seen = 0` lúc khởi động, nên tick đầu chỉ cách đó `tick_secs`.
    /// Test dùng thời gian giả (100s ngay lúc đầu) sẽ thấy peer "im lặng" ngay
    /// — đúng nhưng không phản ánh đời thật.
    const T0: u64 = 5_000;

    fn cfg() -> GossipConfig {
        GossipConfig {
            node_id: "node-a".into(),
            own_url: "http://node-a:8080".into(),
            seeds: "http://node-b:8080,http://node-c:8080".into(),
            token: "s3cret".into(),
            tick_secs: 5,
            suspect_secs: 5,
            dead_secs: 30,
            settle_secs: 10,
            quorum: 1,
        }
    }

    fn mesh(cfg: GossipConfig) -> Mesh {
        Mesh::from_config(cfg).expect("config hợp lệ").expect("mesh bật")
    }

    #[test]
    fn disabled_when_node_id_is_empty() {
        let mut c = cfg();
        c.node_id = String::new();
        assert!(
            Mesh::from_config(c).ok().flatten().is_none(),
            "không có node_id thì không dựng mesh, và đó không phải lỗi"
        );
    }

    /// Cấu hình hỏng phải nổi lên lúc khởi động — không được lặng lẽ chạy đơn
    /// lẻ, vì lúc đó người vận hành tưởng đang có HA.
    #[test]
    fn broken_config_is_an_error_not_a_silent_solo_node() {
        let mut c = cfg();
        c.own_url = String::new();
        let err = Mesh::from_config(c).expect_err("thiếu own_url phải báo");
        assert!(err.contains("own_url"), "{err}");
    }

    /// URL seed chỉ có `http://host:port` ⇒ đoán `node_id` là **host**.
    #[test]
    fn node_id_is_guessed_from_the_seed_url() {
        assert_eq!(node_id_of("http://node-b:8080"), "node-b");
        assert_eq!(node_id_of("https://opsense.example.com"), "opsense.example.com");
        assert_eq!(node_id_of("http://10.0.0.5:9000/"), "10.0.0.5");
    }

    /// Seed khai trước khi gọi health lần nào — không thì tick đầu tiên không có
    /// việc gì và node không bao giờ thấy ai.
    #[test]
    fn seeds_are_peers_before_any_probe() {
        let m = mesh(cfg());
        let ids: Vec<String> = m.probe_targets().into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["node-b".to_string(), "node-c".to_string()]);
    }

    /// Vòng có trả lời ⇒ `mark_alive`; im lặng thì **chưa** được kết luận chết.
    #[test]
    fn a_silent_peer_is_not_dead_after_one_round() {
        let m = mesh(cfg());
        let out = m.apply_probe_results(
            &[("node-b".into(), true), ("node-c".into(), false)],
            T0,
        );
        assert_eq!(out.alive, vec!["node-b".to_string()]);
        assert_eq!(out.silent, vec!["node-c".to_string()]);
        assert!(out.dead.is_empty(), "một vòng im lặng không đủ kết luận chết");
        assert!(out.suspects.is_empty(), "chưa trôi qua cửa sổ nghi ngờ");
    }

    /// Ba mức đúng thứ tự, và node chết thì **không** còn trong danh sách ping.
    #[test]
    fn alive_then_suspect_then_dead_under_the_clocks_hand() {
        let m = mesh(cfg());
        // node-c không tham gia: giữ nó sống để mọi chuyển trạng thái dưới đây
        // là của node-b, không lẫn với node chưa từng được hỏi.
        m.apply_probe_results(
            &[("node-b".into(), true), ("node-c".into(), true)],
            T0,
        );

        // +6s: im lặng quá `suspect_secs=5` ⇒ nghi ngờ, chưa chết.
        let out = m.apply_probe_results(
            &[("node-b".into(), false), ("node-c".into(), true)],
            T0 + 6_000,
        );
        assert_eq!(out.suspects, vec!["node-b".to_string()]);
        assert!(out.dead.is_empty(), "`dead_secs=30` nên chưa được chết");
        assert!(
            m.probe_targets().iter().any(|(id, _)| id == "node-b"),
            "node đang nghi ngờ vẫn phải được ping"
        );

        // +31s: quá `dead_secs=30` kể từ lần nhìn thấy cuối (T0) ⇒ chết.
        let out = m.apply_probe_results(&[("node-c".into(), true)], T0 + 31_000);
        assert_eq!(out.dead, vec!["node-b".to_string()]);
        assert!(
            !m.probe_targets().iter().any(|(id, _)| id == "node-b"),
            "node chết thì bỏ khỏi danh sách ping"
        );
    }

    /// Một lần trả lời là xoá nghi ngờ — thứ giữ mạng chập chờn không giật đình.
    #[test]
    fn one_reply_clears_suspicion() {
        let m = mesh(cfg());
        // Cả hai sống ở tick đầu, rồi node-b im lặng ⇒ chỉ node-b bị nghi.
        m.apply_probe_results(&[("node-b".into(), true), ("node-c".into(), true)], T0);
        let out = m.apply_probe_results(
            &[("node-b".into(), false), ("node-c".into(), true)],
            T0 + 6_000,
        );
        assert_eq!(out.suspects, vec!["node-b".to_string()]);

        // Cùng mốc thời gian đó: node-b trả lời ⇒ nghi ngờ bị xoá ngay.
        let out = m.apply_probe_results(&[("node-b".into(), true)], T0 + 6_000);
        assert!(out.suspects.is_empty(), "một lần trả lời là xoá nghi ngờ");
        assert_eq!(out.alive, vec!["node-b".to_string()]);
        assert!(!m.is_settled_at(T0 + 6_000), "vừa xác nhận lại thì view chưa yên");
    }

    /// Node ta không biết thì kết quả probe vô nghĩa — bỏ qua, đừng ghi nhầm
    /// trạng thái của node khác.
    #[test]
    fn results_for_unknown_nodes_are_ignored() {
        let m = mesh(cfg());
        let out = m.apply_probe_results(&[("node-khong-ton-tai".into(), true)], T0);
        assert!(out.alive.is_empty() && out.silent.is_empty());
        assert_eq!(m.nodes().len(), 2, "view không được bịa thêm node");
    }

    /// Lệch version là thứ *quan sát được* — báo ra, không kết luận gì thêm.
    #[test]
    fn version_mismatch_is_reported_not_judged() {
        let m = mesh(cfg());
        m.publish(Info {
            version: "1.0.16".into(),
            ..Info::default()
        });
        assert!(m.version_mismatches().is_empty(), "chưa ai công bố gì");

        let mut remote = InfoState::new();
        remote.set_local(
            "node-b",
            Info {
                version: "1.0.15".into(),
                ..Info::default()
            },
        );
        {
            let mut g = m.gossip.lock().expect("khoá");
            assert!(g.apply(&remote), "state của peer phải được hợp nhất");
        }
        assert_eq!(
            m.version_mismatches().get("node-b").map(String::as_str),
            Some("1.0.15")
        );
    }

    /// `/health/live` phải trả 2xx mới tính là sống — và ta phải gọi **đúng
    /// path** đó, không phải chỗ nào trả 2xx cũng được.
    #[tokio::test]
    async fn probe_hits_health_live_and_only_2xx_counts_as_alive() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let asked_live = String::from_utf8_lossy(&buf).contains("/health/live");
                let (status, body) = if asked_live {
                    ("204 No Content", "")
                } else {
                    ("500 Internal Server Error", "no")
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let mut c = cfg();
        c.seeds = format!("http://{addr}");
        let m = mesh(c);

        let results = m.probe_all().await;
        assert_eq!(results.len(), 1, "chỉ có một seed");
        assert!(
            results[0].1,
            "gọi `/health/live` và nhận 2xx thì coi là sống; gọi sai path thì server trả 500"
        );

        // Server biến mất ⇒ phải coi là im lặng, không panic. `abort()` để task
        // ngừng giữ listener.
        server.abort();
        let results = m.probe_all().await;
        assert_eq!(results.len(), 1);
        assert!(!results[0].1, "peer không còn thì im lặng, không phải sống");
    }
}
