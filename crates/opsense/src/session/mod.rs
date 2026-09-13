//! Service Account (long session) management.
//!
//! - `store`: on-disk `~/.config/opsense/sessions/<id>.json` (mode 0600).
//! - `cli`:  `opsense session {issue,list,revoke,resolve,import}` subcommand.
//! - HTTP API tới `/api/oauth/v1/session/*` sống ở `crate::client::session_api`.

pub mod cli;
pub mod store;

pub use store::{delete_session_from_disk, list_sessions_on_disk, load_session_from_disk, save_session_to_disk, sessions_dir, SessionFile};
