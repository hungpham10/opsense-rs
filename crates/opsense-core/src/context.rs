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
}

impl opsense_mlib::vector::runtime::Context for Context {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
