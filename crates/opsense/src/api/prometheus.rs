//! Prometheus metrics collector for opsense components.
//!
//! Thuần sync, không async, không chạm global registry — dùng chung cho
//! GraphQL resolver, handler `/metrics`, MCP tool, REPL, CLI.

use opsense_model::events::{Observation, Signal};
use opsense_core::config::StationTarget;
use opsense_mlib::vector::runtime::NodeInfo;
use std::collections::BTreeMap;

pub use axum_prometheus::Handle as PrometheusHandle;

/// Một series cần cập nhật. Thuần data — KHÔNG chạm global registry.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Series {
    /// Tên metric đã sanitize (vd `opsense_cpu_usage`).
    pub name: String,
    /// Giá trị gauge.
    pub value: f64,
    /// Labels: (key, value). Đã escape giá trị cho Prometheus text format.
    pub labels: Vec<(String, String)>,
}

/// Khoá định danh series (name + labels sort theo key) — dùng cho staleness.
pub fn series_key(s: &Series) -> String {
    let mut parts = Vec::with_capacity(1 + s.labels.len());
    parts.push(s.name.clone());
    let mut sorted_labels = s.labels.clone();
    sorted_labels.sort_by(|a, b| a.0.cmp(&b.0));
    for (k, v) in &sorted_labels {
        parts.push(format!("{k}={v}"));
    }
    parts.join(",")
}

/// Sanitize metric name theo Prometheus: `[a-zA-Z_:][a-zA-Z0-9_:]*`.
/// Ký tự không hợp lệ → `_`. Giữ `metric_id` gốc làm label để không mất thông tin.
/// Leading digits bị thay bằng một `_` duy nhất.
pub fn sanitize_metric_name(metric_id: &str) -> String {
    let mut out = String::with_capacity(metric_id.len() + 8);
    out.push_str("opsense_");
    let mut seen_alpha = false;
    let mut added_leading_digit_underscore = false;
    for ch in metric_id.chars() {
        let ok = ch.is_ascii_alphanumeric() || ch == '_' || ch == ':';
        if !seen_alpha {
            if ch.is_ascii_alphabetic() || ch == '_' || ch == ':' {
                out.push(ch);
                seen_alpha = true;
            } else if ch.is_ascii_digit() {
                if !added_leading_digit_underscore {
                    out.push('_');
                    added_leading_digit_underscore = true;
                }
            } else if ok {
                out.push(ch);
            } else {
                out.push('_');
            }
        } else if ok {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out == "opsense_" {
        out.push_str("metric");
    }
    out
}

/// Escape giá trị label cho Prometheus text format.
/// Phải escape: `\` → `\\`, `"` → `\"`, newline → `\n`.
pub fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Gom metric từ node topology (Tầng 1: health node).
pub fn collect_nodes(topology: &[NodeInfo], _now: i64) -> Vec<Series> {
    let mut out = Vec::with_capacity(topology.len() * 3);
    for n in topology {
        let comp = &n.id;
        // running: 1/0 thay vì true/false
        out.push(Series {
            name: "opsense_node_running".into(),
            value: if n.running { 1.0 } else { 0.0 },
            labels: vec![("component".into(), comp.clone())],
        });
        // faults: gauge giá trị tuyệt đối
        out.push(Series {
            name: "opsense_node_faults".into(),
            value: n.fault_count as f64,
            labels: vec![("component".into(), comp.clone())],
        });
        // info: hằng 1, để join trong PromQL
        out.push(Series {
            name: "opsense_node_info".into(),
            value: 1.0,
            labels: vec![
                ("component".into(), comp.clone()),
                ("type".into(), n.component_type.clone()),
            ],
        });
    }
    out
}

/// Gom metric từ station observations (Tầng 2: station data).
pub fn collect_station(
    obs: &[Observation],
    cfg: &StationTarget,
    now: i64,
) -> Vec<Series> {
    // Window per station (nếu station khai báo) — fallback 120s toàn cục.
    let window_secs = cfg.window_secs.unwrap_or(120).max(1);
    let cutoff = now.saturating_sub(window_secs as i64);

    // Mặc định allowlist signal nếu station không khai báo
    let allow_signals: std::collections::HashSet<&str> = if cfg.signals.is_empty() {
        ["raw", "summary"].into_iter().collect()
    } else {
        cfg.signals.iter().map(|s| s.as_str()).collect()
    };

    // Exclude kinds set
    let exclude_kinds: std::collections::HashSet<&str> = cfg
        .exclude_kinds
        .iter()
        .map(|s| s.as_str())
        .collect();

    // Dedup theo series_key: giữ obs có timestamp mới nhất.
    let mut by_key: std::collections::BTreeMap<String, &Observation> = std::collections::BTreeMap::new();

    for o in obs {
        // Gate timestamp
        if o.ts < cutoff {
            continue;
        }
        // Gate signal
        let sig = match o.signal {
            Signal::Raw => "raw",
            Signal::Summary => "summary",
            Signal::Utilization => "utilization",
            Signal::Saturation => "saturation",
            Signal::Rate => "rate",
            Signal::Errors => "errors",
            Signal::Duration => "duration",
            Signal::Order => "order",
        };
        if !allow_signals.contains(sig) {
            continue;
        }
        // Loại cursor trading_step
        if o.labels.get("kind").map(|s| s.as_str()) == Some("trading_step") {
            continue;
        }
        // Loại order
        if sig == "order" {
            continue;
        }
        // Loại exclude_kinds
        if let Some(kind) = o.labels.get("kind") {
            if exclude_kinds.contains(kind.as_str()) {
                continue;
            }
        }

        let metric_name = sanitize_metric_name(&o.metric_id);
        let mut labels = Vec::with_capacity(3 + o.labels.len());
        labels.push(("component".into(), cfg.station.clone()));
        labels.push(("signal".into(), sig.into()));
        labels.push(("metric_id".into(), o.metric_id.clone()));
        // Thêm labels gốc từ observation (đã escape)
        for (k, v) in &o.labels {
            if k == "kind" && v == "trading_step" {
                continue; // cursor không phải metric
            }
            labels.push((k.clone(), escape_label_value(v)));
        }

        let series = Series {
            name: metric_name,
            value: o.value,
            labels,
        };
        let key = series_key(&series);
        // Giữ obs có timestamp mới nhất cho mỗi key
        by_key
            .entry(key)
            .and_modify(|existing| {
                if o.ts > existing.ts {
                    *existing = o;
                }
            })
            .or_insert(o);
    }

    by_key
        .into_values()
        .map(|o| {
            let metric_name = sanitize_metric_name(&o.metric_id);
            let mut labels = Vec::with_capacity(3 + o.labels.len());
            labels.push(("component".into(), cfg.station.clone()));
            let sig = match o.signal {
                Signal::Raw => "raw",
                Signal::Summary => "summary",
                Signal::Utilization => "utilization",
                Signal::Saturation => "saturation",
                Signal::Rate => "rate",
                Signal::Errors => "errors",
                Signal::Duration => "duration",
                Signal::Order => "order",
            };
            labels.push(("signal".into(), sig.into()));
            labels.push(("metric_id".into(), o.metric_id.clone()));
            for (k, v) in &o.labels {
                if k == "kind" && v == "trading_step" {
                    continue;
                }
                labels.push((k.clone(), escape_label_value(v)));
            }
            Series {
                name: metric_name,
                value: o.value,
                labels,
            }
        })
        .collect()
}

/// Cắt series theo trần `max` (deterministic: sort theo key).
/// Trả về (series đã cắt, số bị cắt).
pub fn cap_series(mut series: Vec<Series>, max: usize) -> (Vec<Series>, u64) {
    if max == 0 || series.len() <= max {
        return (series, 0);
    }
    // Sort deterministic theo key
    series.sort_by(|a, b| series_key(a).cmp(&series_key(b)));
    let dropped = series.len() - max;
    series.truncate(max);
    (series, dropped as u64)
}

/// Render series thành Prometheus text format.
pub fn render_prometheus(
    series: &[Series],
    dropped: u64,
) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(series.len() * 80);

    // Group theo metric name để in # HELP / # TYPE 1 lần
    let mut by_name: BTreeMap<&str, Vec<&Series>> = BTreeMap::new();
    for s in series {
        by_name.entry(&s.name).or_default().push(s);
    }

    for (name, series) in by_name {
        writeln!(out, "# HELP {name} Opsense component metric").unwrap();
        writeln!(out, "# TYPE {name} gauge").unwrap();
        for s in series {
            let label_str = s.labels.iter()
                .map(|(k, v)| format!("{k}=\"{v}\""))
                .collect::<Vec<_>>()
                .join(",");
            writeln!(out, "{name}{{{label_str}}} {}", s.value).unwrap();
        }
    }

    // dropped counter
    if dropped > 0 {
        writeln!(out, "# HELP opsense_prometheus_dropped_series Total series dropped due to max_series limit").unwrap();
        writeln!(out, "# TYPE opsense_prometheus_dropped_series counter").unwrap();
        writeln!(out, "opsense_prometheus_dropped_series {}", dropped).unwrap();
    }

    out
}

/// Gom metric từ OAuth (Tầng 4).
pub fn collect_oauth(
    snap: &crate::api::oauth::metrics::OAuthMetricsSnapshot,
) -> Vec<Series> {
    vec![
        Series { name: "opsense_oauth_device_code_issued_total".into(), value: snap.device_code_issued as f64, labels: vec![] },
        Series { name: "opsense_oauth_device_code_approved_total".into(), value: snap.device_code_approved as f64, labels: vec![] },
        Series { name: "opsense_oauth_device_code_denied_total".into(), value: snap.device_code_denied as f64, labels: vec![] },
        Series { name: "opsense_oauth_access_token_issued_total".into(), value: snap.access_token_issued as f64, labels: vec![] },
        Series { name: "opsense_oauth_long_session_issued_total".into(), value: snap.long_session_issued as f64, labels: vec![] },
    ]
}

/// Gom HTTP metrics từ axum-prometheus layer (Tầng 3).
/// Layer này đã ghi vào global registry, ta chỉ cần render handle.
pub fn render_http(handler: &axum_prometheus::Handle) -> String {
    handler.0.render()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_core::config::StationTarget;
    use opsense_model::events::{Observation, Signal, TelemetryKind};
    use opsense_mlib::vector::runtime::NodeInfo;

    fn obs(ts: i64, metric: &str, signal: Signal, value: f64, labels: Vec<(&str, &str)>) -> Observation {
        let mut o = Observation::new(ts, metric.into(), TelemetryKind::Metric, signal, value);
        for (k, v) in labels {
            o.labels.insert(k.into(), v.into());
        }
        o
    }

    #[test]
    fn sanitize_metric_name_basic() {
        assert_eq!(sanitize_metric_name("cpu_usage"), "opsense_cpu_usage");
        assert_eq!(sanitize_metric_name("cpu.usage"), "opsense_cpu_usage");
        assert_eq!(sanitize_metric_name("cpu-usage"), "opsense_cpu_usage");
        assert_eq!(sanitize_metric_name("123metric"), "opsense__metric"); // digit đầu -> _
    }

    #[test]
    fn series_key_deterministic() {
        let s1 = Series { name: "a".into(), value: 1.0, labels: vec![("b".into(), "2".into()), ("a".into(), "1".into())] };
        let s2 = Series { name: "a".into(), value: 1.0, labels: vec![("a".into(), "1".into()), ("b".into(), "2".into())] };
        assert_eq!(series_key(&s1), series_key(&s2));
    }

    #[test]
    fn escape_label_value_works() {
        assert_eq!(escape_label_value("abc"), "abc");
        assert_eq!(escape_label_value("a\"b"), "a\\\"b");
        assert_eq!(escape_label_value("a\\b"), "a\\\\b");
        assert_eq!(escape_label_value("a\nb"), "a\\nb");
    }

    #[test]
    fn node_metrics_use_literal_0_and_1() {
        let _now = 1_000_000;
        let topology = vec![
            NodeInfo {
                id: "grid".into(),
                component_type: "rhai_transform".into(),
                inputs: vec![],
                outputs: vec![],
                running: true,
                fault_count: 5,
                last_error: None,
            },
            NodeInfo {
                id: "tick-candle".into(),
                component_type: "tick_2_candle".into(),
                inputs: vec!["tick-map".into()],
                outputs: vec!["grid".into()],
                running: false,
                fault_count: 0,
                last_error: None,
            },
        ];
        let series = collect_nodes(&topology, 0);
        let running_grid = series.iter().find(|s| s.name == "opsense_node_running" && s.labels.iter().any(|(k,v)| k=="component" && v=="grid")).unwrap();
        let running_tick = series.iter().find(|s| s.name == "opsense_node_running" && s.labels.iter().any(|(k,v)| k=="component" && v=="tick-candle")).unwrap();
        assert_eq!(running_grid.value, 1.0);
        assert_eq!(running_tick.value, 0.0);
    }

    #[test]
    fn signal_order_is_excluded_by_default() {
        let now = 1_000_000;
        let cfg = StationTarget::default();
        let obs = vec![
            obs(now, "test_metric", Signal::Order, 42.0, vec![("status", "closed")]),
            obs(now, "test_metric", Signal::Raw, 1.0, vec![]),
        ];
        let series = collect_station(&obs, &cfg, now);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].name, "opsense_test_metric");
    }

    #[test]
    fn trading_step_kind_is_not_a_metric() {
        let _now = 1_000_000;
        let _cfg = StationTarget::default();
        let mut obs = obs(_now, "test_metric", Signal::Summary, 1.0, vec![]);
        obs.labels.insert("kind".into(), "trading_step".into());
        let series = collect_station(&[obs], &StationTarget::default(), _now);
        assert_eq!(series.len(), 0);
    }

    /// Regression (dashboard không có thông tin về lưới): `conf/grid.local.toml`
    /// khai `exclude_kinds = ["snapshot", "trend_probe"]` nên obs `snapshot` —
    /// nơi `grid.rhai` từng nhét toàn bộ hình học lưới vào label — bị cắt khỏi
    /// Prometheus. Giải pháp là phát `kind = "grid_state"`, và kind đó **phải**
    /// sống qua đúng config thật.
    ///
    /// Nếu ai đó thêm `"grid_state"` vào `exclude_kinds` ở config (hoặc đổi
    /// macro `station` sang loại mặc định khác) thì 8 panel row 5 của dashboard
    /// Grafana im lặng rỗng — cũng chính triệu chứng gốc của ticket này.
    #[test]
    fn grid_state_survives_the_exclude_kinds_of_the_real_config() {
        let now = 1_000_000;
        // Đúng `[[prometheus.stations]]` của `conf/grid.local.toml`.
        let cfg = StationTarget {
            station: "grid".into(),
            enabled: true,
            signals: vec!["summary".into()],
            window_secs: Some(300),
            exclude_kinds: vec!["snapshot".into(), "trend_probe".into()],
        };

        // Nguồn duy nhất còn lại của hình học lưới trên Prometheus.
        let grid_state = obs(now, "grid_cells", Signal::Summary, 64.0, vec![("kind", "grid_state")]);
        // Obs mà `snapshot()` gắn grid vào label — bị cắt có chủ đích.
        let snapshot = obs(
            now,
            "BTCUSDT",
            Signal::Summary,
            106.5,
            vec![("kind", "snapshot"), ("grid_step", "0.09"), ("cells", "64")],
        );

        let series = collect_station(&[grid_state, snapshot], &cfg, now);

        let grid: Vec<&Series> = series.iter().filter(|s| s.name == "opsense_grid_cells").collect();
        assert_eq!(grid.len(), 1, "grid_cells không được exclude: {series:?}");
        assert_eq!(grid[0].value, 64.0);
        assert!(
            series.iter().all(|s| s.name != "opsense_BTCUSDT"),
            "snapshot phải vẫn bị exclude (không sống bằng cách mở rộng allowlist): {series:?}"
        );
        assert!(
            series.iter().all(|s| !s.labels.iter().any(|(k, _)| k == "grid_step")),
            "không metric số nào được nhét vào label: {series:?}"
        );
    }

    #[test]
    fn stale_observation_is_not_refreshed() {
        let now = 1_000_000;
        let cfg = StationTarget::default();
        let obs = vec![
            obs(now - 200, "test", Signal::Raw, 1.0, vec![]), // > 120s
            obs(now - 50, "test", Signal::Raw, 2.0, vec![]),   // < 120s
        ];
        let series = collect_station(&obs, &cfg, now);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].value, 2.0);
    }

    #[test]
    fn cap_series_is_deterministic() {
        let s1 = Series { name: "a".into(), value: 1.0, labels: vec![("x".into(), "1".into())] };
        let s2 = Series { name: "b".into(), value: 2.0, labels: vec![("x".into(), "2".into())] };
        let s3 = Series { name: "c".into(), value: 3.0, labels: vec![("x".into(), "3".into())] };
        let (capped, dropped) = cap_series(vec![s3.clone(), s1.clone(), s2.clone()], 2);
        assert_eq!(dropped, 1);
        assert_eq!(capped[0].name, "a"); // sort theo key
        assert_eq!(capped[1].name, "b");
    }

    #[test]
    fn cap_series_reports_dropped() {
        let s1 = Series { name: "a".into(), value: 1.0, labels: vec![] };
        let s2 = Series { name: "b".into(), value: 2.0, labels: vec![] };
        let (_, dropped) = cap_series(vec![s1, s2], 1);
        assert_eq!(dropped, 1);
    }

    #[test]
    fn metric_id_is_sanitized_but_preserved_as_label() {
        let now = 1_000_000;
        let cfg = StationTarget::default();
        let obs = vec![obs(now, "cpu.usage", Signal::Raw, 42.0, vec![])];
        let series = collect_station(&obs, &cfg, now);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].name, "opsense_cpu_usage");
        assert!(series[0].labels.iter().any(|(k,v)| k=="metric_id" && v=="cpu.usage"));
    }

    #[test]
    fn empty_station_yields_no_series() {
        let series = collect_station(&[], &StationTarget::default(), 1_000_000);
        assert_eq!(series.len(), 0);
    }

    #[test]
    fn signal_filter_allowlist_respected() {
        let now = 1_000_000;
        let mut cfg = StationTarget::default();
        cfg.signals = vec!["raw".into()]; // chỉ raw
        let obs = vec![
            obs(1_000_000, "test", Signal::Raw, 1.0, vec![]),
            obs(1_000_000, "test", Signal::Summary, 2.0, vec![]),
        ];
        let series = collect_station(&obs, &cfg, now);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].value, 1.0);
    }
}