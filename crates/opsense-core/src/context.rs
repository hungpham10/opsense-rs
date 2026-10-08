use std::collections::{BTreeMap, HashMap};
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::RwLock;

use opsense_model::secret::Secret;

use crate::config::{Config, StorageConfig};
use crate::station::{Station, StationKind};

pub type Stations = Arc<RwLock<HashMap<String, Station>>>;

#[derive(Clone)]
pub struct Context {
    /// Manage secrets
    secret: Arc<Secret>,

    /// Resolved `[attributes]` (TOML + `OPSENSE_ATTR_*` env overrides) for
    /// template rendering in fetch nodes. Mutable via GraphQL `setAttribute`.
    attributes: Arc<RwLock<BTreeMap<String, String>>>,

    /// Registry of stations this process manages, keyed by component id
    /// (`Station::Category` / `Station::Pattern` / `Station::Timeseries`).
    /// Transforms publish here; `AppState` (HTTP/MCP/Rhai) reads from here.
    stations: Stations,

    /// Mô tả tự do của từng node, khoá theo component `id`, đọc từ
    /// `description` trong `[[pipeline.components]]`.
    ///
    /// **Vì sao không phải field của component struct:** macro
    /// `#[source]/#[transform]/#[sink]` đặt `#[serde(deny_unknown_fields)]` lên
    /// mọi struct (`opsense-macros/src/configurable_component.rs:218`), nên một
    /// `description` trong TOML sẽ làm hỏng deserialize mọi node. Thêm nó vào
    /// struct thì phải sửa macro **và** mọi constructor. Tệ hơn: `get_config`
    /// trả JSON của typed struct, mà `reload`/`set_param` đi vòng qua struct —
    /// nên mô tả đặt trong struct sẽ bị xoá mỗi lần sửa cấu hình.
    ///
    /// Nó là metadata của **deployment** chứ không phải của code: node `history`
    /// trong `strategies/binance` là nến Binance 1m, còn cùng node đó trong
    /// `strategies/predict` là prometheus. Chỗ viết đúng là chỗ khai pipeline.
    descriptions: Arc<RwLock<BTreeMap<String, String>>>,

    /// `[storage]` config — quyết định backend của station mới.
    storage: StorageConfig,
}

impl Context {
    #[must_use]
    pub fn new(cfg: &Config, secret: Arc<Secret>) -> Self {
        let attributes = Arc::new(RwLock::new(cfg.resolved_attributes()));

        Self {
            attributes,
            secret,
            stations: Arc::new(RwLock::new(HashMap::new())),
            descriptions: Arc::new(RwLock::new(BTreeMap::new())),
            storage: cfg.storage.clone(),
        }
    }

    /// Thay toàn bộ mô tả node. Gọi một lần lúc boot, cùng lúc dựng pipeline.
    ///
    /// Thay **cả bảng** chứ không ghép: `[[pipeline.components]]` là nguồn duy
    /// nhất, nên node biến mất thì mô tả của nó cũng phải biến mất. Không
    /// gọi lại lúc `reload` — `reload` nhận danh sách component chứ không nhận
    /// config, và mô tả không đổi theo.
    pub async fn set_node_descriptions(&self, map: BTreeMap<String, String>) {
        *self.descriptions.write().await = map;
    }

    /// Snapshot toàn bộ mô tả node, khoá theo `id`.
    pub async fn node_descriptions(&self) -> BTreeMap<String, String> {
        self.descriptions.read().await.clone()
    }

    /// Mô tả của một node. `None` khi node không khai `description` — đó là
    /// hợp lệ, không phải lỗi cấu hình.
    pub async fn node_description(&self, id: &str) -> Option<String> {
        self.descriptions.read().await.get(id).cloned()
    }

    /// Snapshot of every attribute. Used by `Query.attributes` to expose the
    /// current state of the in-memory attribute map to REPL/MCP clients.
    pub async fn get_attributes(&self) -> BTreeMap<String, String> {
        self.attributes.read().await.clone()
    }

    /// Insert or update one attribute. Applies immediately to subsequent
    /// `Context::variable()` lookups (used by HTTP source template rendering).
    pub async fn set_attribute(&self, name: String, value: String) {
        self.attributes.write().await.insert(name, value);
    }

    /// Remove one attribute. Returns `true` when the entry existed.
    pub async fn remove_attribute(&self, name: &str) -> bool {
        self.attributes.write().await.remove(name).is_some()
    }

    pub async fn stations(&self) -> Vec<(String, StationKind)> {
        let guard = self.stations.read().await;
        guard
            .iter()
            .map(|(id, st)| (id.clone(), st.kind()))
            .collect()
    }

    /// Station `id` đã đăng ký chưa — dùng để **chờ** node tương ứng khởi động
    /// xong. `stations()` phải dựng cả `Vec` mới chỉ để kiểm tra một tên, nên
    /// cổng riêng cho việc chờ (xem `AppState::wait_for_station`).
    pub async fn has_station(&self, id: &str) -> bool {
        self.stations.read().await.contains_key(id)
    }

    /// `[storage]` config — components đọc để dựng station theo backend.
    pub fn storage(&self) -> &StorageConfig {
        &self.storage
    }

    pub async fn variable<T>(&self, name: &str) -> Result<T, Error>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        let val_str = {
            let attrs = self.attributes.read().await;
            attrs.get(name).cloned()
        };
        let val_str = match val_str {
            Some(val) => val,
            None => self.secret.get(name, "/").await.map_err(|e| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("Variable/Secret '{}' not found: {}", name, e),
                )
            })?,
        };

        val_str.parse::<T>().map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Failed to parse attribute '{}' with value '{}': {}",
                    name, val_str, e
                ),
            )
        })
    }

    /// First-wins register a [`Station`] under `id`. Fails with `AlreadyExists`
    /// when the id is already taken so duplicate registrations surface as
    /// errors instead of silently overwriting another node's data.
    ///
    /// Takes the write lock for the duration of the check-and-insert so two
    /// nodes racing for the same id can never both win.
    pub async fn registry(&self, id: &str, station: Station) -> Result<(), Error> {
        let mut stations_guard = self.stations.write().await;
        if stations_guard.contains_key(id) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("Station '{}' already registered", id),
            ));
        }
        stations_guard.insert(id.to_string(), station);
        Ok(())
    }

    pub async fn station<T>(&self, name: &str) -> Result<T, Error>
    where
        T: for<'a> TryFrom<&'a Station, Error = Error>,
    {
        let stations_guard = self.stations.read().await;

        let station = stations_guard.get(name).ok_or_else(|| {
            Error::new(ErrorKind::NotFound, format!("Station '{}' not found", name))
        })?;

        T::try_from(station)
    }

    /// Xoá sạch **một** station: RAM + storage, mọi loại station đều nhận
    /// cùng một nghĩa — dữ liệu cũ không còn đọc được, cũng không còn quay lại
    /// sau restart.
    ///
    /// `Arc` được copy ra khỏi registry **trước** khi `await`, nên không giữ
    /// read-lock của `stations` xuyên suốt thao tác clear: node khác vẫn
    /// `registry()` được trong lúc này.
    ///
    /// Trả [`StationKind`] để caller báo cáo đã xoá loại nào (GraphQL `kind`).
    pub async fn clear_station(&self, name: &str) -> Result<StationKind, Error> {
        let station = {
            let stations_guard = self.stations.read().await;
            stations_guard.get(name).cloned().ok_or_else(|| {
                Error::new(ErrorKind::NotFound, format!("Station '{name}' not found"))
            })?
        };

        match &station {
            Station::Timeseries(inner) => inner.read().await.clear().await,
            Station::Category(inner) => inner.write().await.clear().await,
            Station::Pattern(inner) => inner.read().await.clear().await,
        }?;

        Ok(station.kind())
    }

    /// Xoá sạch **mọi** station đã đăng ký.
    ///
    /// Copy cả registry ra trước rồi mới clear từng cái — cùng lý do như
    /// [`Context::clear_station`]: không giữ lock qua `await`, và một station
    /// hỏng không chặn phần còn lại (lỗi được gom lại, trả về sau cùng danh
    /// sách những cái đã xoá được).
    ///
    /// Cảnh báo: đây là **xoá không thể hoàn tác** — mất lệnh đang mở, cursor
    /// T+N, plan và toàn bộ lịch sử của mọi station trong process.
    pub async fn clear_all_stations(&self) -> Result<(Vec<(String, StationKind)>, Vec<String>), Error> {
        let stations: Vec<(String, Station)> = {
            let stations_guard = self.stations.read().await;
            stations_guard
                .iter()
                .map(|(id, station)| (id.clone(), station.clone()))
                .collect()
        };

        let mut cleared = Vec::with_capacity(stations.len());
        let mut failed = Vec::new();
        for (id, station) in stations {
            let result = match &station {
                Station::Timeseries(inner) => inner.read().await.clear().await,
                Station::Category(inner) => inner.write().await.clear().await,
                Station::Pattern(inner) => inner.read().await.clear().await,
            };
            match result {
                Ok(()) => cleared.push((id, station.kind())),
                Err(e) => failed.push(format!("{id}: {e}")),
            }
        }

        if !failed.is_empty() {
            tracing::warn!(
                failed = ?failed,
                cleared = cleared.len(),
                "clear_all_stations: một số station không xoá được"
            );
        }
        Ok((cleared, failed))
    }
}

impl opsense_mlib::vector::runtime::Context for Context {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // `use super::*` không mang `Observation`/`TimeseriesStation` vào scope:
    // module cha chỉ import `Station`/`StationKind`. Không có hai dòng này thì
    // `cargo test --no-run` fail ở `opsense-core` trước cả khi tới crate khác.
    use crate::station::TimeseriesStation;
    use opsense_model::events::{Observation, Signal, TelemetryKind};
    use opsense_model::secret::Secret;

    fn obs(ts: i64, value: f64) -> Observation {
        Observation::new(ts, "grid".into(), TelemetryKind::Metric, Signal::Order, value)
    }

    async fn ctx() -> Context {
        let cfg: Config = serde_json::from_str("{}").expect("Config mặc định từ `{}`");
        let secret = Secret::new().await.expect("secret init");
        Context::new(&cfg, Arc::new(secret))
    }

    /// Xoá theo id: station đích rỗng, station **khác** phải còn nguyên.
    ///
    /// Vế thứ hai mới là chỗ dễ sai — xoá nhầm cả registry thì lệnh xoá một
    /// station sẽ âm thầm mất dữ liệu của station kia, và điều đó chỉ lộ ra
    /// ở lần query sau.
    #[tokio::test]
    async fn clear_station_leaves_other_stations_intact() {
        let ctx = ctx().await;

        let grid = TimeseriesStation::new(8, Some(3_600));
        let candles = TimeseriesStation::new(8, Some(3_600));
        let base = 1_787_040_000i64;

        let ts_grid = Arc::new(RwLock::new(grid));
        let ts_candles = Arc::new(RwLock::new(candles));
        ts_grid.write().await.update_range(&[obs(base, 1.0)], base, base, base);
        ts_candles
            .write()
            .await
            .update_range(&[obs(base, 2.0)], base, base, base);

        ctx.registry("grid", Station::Timeseries(ts_grid.clone()))
            .await
            .expect("register grid");
        ctx.registry("tick-candle", Station::Timeseries(ts_candles.clone()))
            .await
            .expect("register tick-candle");

        let kind = ctx.clear_station("grid").await.expect("clear grid");
        assert_eq!(kind, StationKind::Timeseries);

        assert_eq!(ts_grid.read().await.query_recent(base, base).await.unwrap_or_default().len(), 0);
        assert_eq!(
            ts_candles.read().await.query_recent(base, base).await.unwrap_or_default().len(),
            1,
            "station khác không được đụng"
        );
    }

    /// Id không có thì phải là lỗi `NotFound`, không phải im lặng thành công —
    /// im lặng thành công là kiểu lỗi khó nhất khi quản trị state.
    #[tokio::test]
    async fn clear_station_unknown_id_is_not_found() {
        let ctx = ctx().await;
        let err = ctx
            .clear_station("khong-ton-tai")
            .await
            .expect_err("id lạ phải báo NotFound");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    /// `clear_all_stations` xoá hết và báo cáo đủ; registry vẫn nguyên vì node
    /// còn sống và sẽ ghi lại.
    #[tokio::test]
    async fn clear_all_stations_empties_every_station_and_keeps_registry() {
        let ctx = ctx().await;
        let base = 1_787_040_000i64;

        let a = Arc::new(RwLock::new(TimeseriesStation::new(8, Some(3_600))));
        let b = Arc::new(RwLock::new(TimeseriesStation::new(8, Some(3_600))));
        a.write().await.update_range(&[obs(base, 1.0)], base, base, base);
        b.write().await.update_range(&[obs(base, 2.0)], base, base, base);
        ctx.registry("a", Station::Timeseries(a.clone())).await.expect("register a");
        ctx.registry("b", Station::Timeseries(b.clone())).await.expect("register b");

        let (cleared, failed) = ctx
            .clear_all_stations()
            .await
            .expect("clear all stations");

        assert!(failed.is_empty(), "không station nào được phép hỏng: {failed:?}");
        let mut ids: Vec<&str> = cleared.iter().map(|(id, _)| id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["a", "b"]);
        assert!(cleared.iter().all(|(_, k)| *k == StationKind::Timeseries));

        for st in [&a, &b] {
            assert_eq!(
                st.read().await.query_recent(base, base).await.unwrap_or_default().len(),
                0,
                "mọi station phải rỗng"
            );
        }

        // Registry phải còn: node đang chạy không được mất station.
        assert_eq!(
            ctx.stations().await.len(),
            2,
            "clear dữ liệu không được gỡ station khỏi registry"
        );
    }
}
