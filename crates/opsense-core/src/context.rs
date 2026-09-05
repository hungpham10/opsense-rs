use std::collections::{BTreeMap, HashMap};
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::RwLock;

use opsense_model::secret::Secret;

use crate::config::Config;
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
}

impl Context {
    #[must_use]
    pub fn new(cfg: &Config, secret: Arc<Secret>) -> Self {
        let attributes = Arc::new(RwLock::new(cfg.resolved_attributes()));

        Self {
            attributes,
            secret,
            stations: Arc::new(RwLock::new(HashMap::new())),
        }
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

impl opsense_libs::vector::runtime::Context for Context {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
