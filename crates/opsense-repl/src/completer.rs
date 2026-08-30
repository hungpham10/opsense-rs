//! Reedline completion: commands, station ids, session variables.

use std::sync::Arc;

use reedline::{Completer, Span, Suggestion};

use opsense_core::registry;
use opsense_session::{Session, SessionManager};

const COMMANDS: &[&str] = &[
    ":help",
    ":station",
    ":query",
    ":py",
    ":stats",
    ":plot",
    ":vars",
    ":sessions",
    ":new",
    ":use",
    ":close",
    ":save",
    ":load",
    ":quit",
];

/// Suggests `:commands`, then station ids, then `@var` names depending on
/// the word being typed.
pub struct OpsenseCompleter {
    #[allow(dead_code)] // feeds :use suggestions once multi-session lands
    manager: Arc<SessionManager>,
    current_session: Arc<Session>,
}

impl OpsenseCompleter {
    #[must_use]
    pub fn new(manager: Arc<SessionManager>, current_session: Arc<Session>) -> Self {
        Self {
            manager,
            current_session,
        }
    }

    fn candidates(&self) -> Vec<String> {
        let mut out: Vec<String> = COMMANDS.iter().map(|s| (*s).to_string()).collect();
        for id in registry::station_ids_snapshot() {
            out.push(format!(":query {id}"));
        }
        for var in self
            .current_session
            .state()
            .lock()
            .unwrap()
            .variables
            .keys()
        {
            out.push(var.clone());
        }
        out.sort();
        out.dedup();
        out
    }
}

impl Completer for OpsenseCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let prefix = &line[..pos];
        let word_start = prefix.rfind(char::is_whitespace).map_or(0, |i| i + 1);
        let word = &prefix[word_start..];

        self.candidates()
            .into_iter()
            .filter(|c| c.starts_with(word))
            .map(|c| Suggestion {
                value: c,
                description: None,
                style: None,
                extra: None,
                span: Span {
                    start: word_start,
                    end: pos,
                },
                append_whitespace: false,
            })
            .collect()
    }
}
