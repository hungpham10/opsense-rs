//! Command dispatch for the opsense REPL.
//!
//! All pipeline mutations go through `Mutation.reload(components)` on the
//! server. The REPL only has one edit operation — `:node add <json>` —
//! because the rest of the pipeline is bootstrapped from a TOML file at
//! `opsense serve` startup, and the server's `Status` query doesn't
//! expose the full config of every node (only `id`/`type`/`inputs`).
//!
//! To edit/remove a node, the operator edits the TOML and restarts
//! `opsense serve`. This is intentional — keeps the wire format minimal
//! and avoids stale-state round-trips.
//!
//! Returns `Ok(Some(text))` to print, `Ok(None)` for no output,
//! or `Err(...)` to render.

use std::collections::BTreeMap;

use crate::client::graphql::{ComponentInput, NodeSummary, OpsenseClient, Status};
use crate::repl::display::{
    format_attributes, format_edit_result, format_observations, format_stations_table,
    format_status_table,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public dispatcher
// ─────────────────────────────────────────────────────────────────────────────

pub async fn dispatch(line: &str, client: &OpsenseClient) -> anyhow::Result<Option<String>> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }

    let (cmd, rest) = split_first_word(line);

    match cmd {
        ":status" | ":s" => cmd_status(client).await,
        ":attr" | ":a" => cmd_attr(client, rest).await,
        ":node" | ":n" => cmd_node(client, rest).await,
        ":query" | ":q" => cmd_query(client, rest).await,
        ":login" => cmd_login(rest).await,

        ":help" | ":h" | ":?" => Ok(Some(HELP_TEXT.to_string())),

        c if c.starts_with(':') => anyhow::bail!("unknown command '{c}'; try :help"),

        // Bare JSON → :node add
        _ if rest.trim_start().starts_with('{') => {
            let raw = format!("{} {}", cmd, rest);
            cmd_node_add(client, &raw).await
        }

        // Bare text → :query
        _ => cmd_query(client, line).await,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Status
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_status(client: &OpsenseClient) -> anyhow::Result<Option<String>> {
    let status = client.status().await?;
    let mut out = format!("Pipeline ({} nodes):\n", status.nodes.len());
    out.push_str(&format_status_table(&status).to_string());
    out.push('\n');
    out.push_str(&format!("\nStations ({}):\n", status.stations.len()));
    out.push_str(&format_stations_table(&status.stations).to_string());
    Ok(Some(out))
}

// ─────────────────────────────────────────────────────────────────────────────
// Attributes
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_attr(client: &OpsenseClient, rest: &str) -> anyhow::Result<Option<String>> {
    let (sub, args) = split_first_word(rest);
    match sub {
        "list" | "ls" | "" => {
            let attrs: BTreeMap<String, String> = client.attributes().await?;
            if attrs.is_empty() {
                Ok(Some("(no attributes)".to_string()))
            } else {
                Ok(Some(format_attributes(&attrs).to_string()))
            }
        }
        "set" | "s" => {
            let (name, value) = split_once(args, ' ')
                .ok_or_else(|| anyhow::anyhow!("usage: :attr set <name> <value>"))?;
            let r = client.set_attribute(name, value).await?;
            let mut out = format!("ok={}", r.ok);
            if r.env_override_active {
                out.push_str("  [warn] OPSENSE_ATTR_<NAME> is set — env value takes priority");
            }
            Ok(Some(out))
        }
        "rm" | "remove" | "r" => {
            let name = args.trim();
            if name.is_empty() {
                anyhow::bail!("usage: :attr rm <name>");
            }
            let removed = client.remove_attribute(name).await?;
            Ok(Some(format!("removed={removed}")))
        }
        "get" | "g" => {
            let name = args.trim();
            if name.is_empty() {
                anyhow::bail!("usage: :attr get <name>");
            }
            let attrs: BTreeMap<String, String> = client.attributes().await?;
            match attrs.get(name) {
                Some(v) => Ok(Some(format!("{name} = {v}"))),
                None => Ok(Some(format!("{name} = <not set>"))),
            }
        }
        _ => anyhow::bail!("unknown :attr subcommand '{sub}'; try :attr list | set | rm | get"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Node — read-only inspection + add (push full component list)
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_node(client: &OpsenseClient, rest: &str) -> anyhow::Result<Option<String>> {
    let (sub, args) = split_first_word(rest);
    match sub {
        "list" | "ls" | "" => cmd_status(client).await,
        "add" | "a" => cmd_node_add(client, args).await,
        "get" | "g" => {
            let id = args.trim();
            if id.is_empty() {
                anyhow::bail!("usage: :node get <id>");
            }
            let status: Status = client.status().await?;
            let node = status
                .nodes
                .iter()
                .find(|n| n.id == id)
                .ok_or_else(|| anyhow::anyhow!("node '{id}' not found"))?;
            Ok(Some(format_node(node)))
        }
        _ => anyhow::bail!(
            "unknown :node subcommand '{sub}'; try :node list | add | get\n\
             (edit/remove: change the TOML config and restart `opsense serve`)"
        ),
    }
}

async fn cmd_node_add(client: &OpsenseClient, args: &str) -> anyhow::Result<Option<String>> {
    let json = args.trim();
    if json.is_empty() {
        anyhow::bail!("usage: :node add <json>  or  :node add <type> <id>");
    }

    let new_comp = if json.starts_with('{') {
        json_to_component(parse_json_obj(json)?)?
    } else {
        // Shorthand: <type> <id>  (config rỗng — typetag sẽ fail trên server
        // nếu type yêu cầu config bắt buộc; user dùng dạng JSON đầy đủ)
        let parts: Vec<&str> = args.splitn(2, ' ').collect();
        if parts.len() < 2 {
            anyhow::bail!("usage: :node add <type> <id>");
        }
        ComponentInput::new(parts[0], parts[1])
    };

    let status: Status = client.status().await?;
    let mut next: Vec<ComponentInput> = status
        .nodes
        .iter()
        .map(|n| ComponentInput {
            kind: n.kind.clone(),
            id: n.id.clone(),
            config: None,
            inputs: if n.inputs.is_empty() { None } else { Some(n.inputs.clone()) },
        })
        .collect();
    if next.iter().any(|c| c.id == new_comp.id) {
        anyhow::bail!("node '{}' already exists; edit the TOML config to change it", new_comp.id);
    }
    next.push(new_comp.clone());

    let result = client.reload(next).await?;
    Ok(Some(format_edit_result(&format!("added '{}'", new_comp.id), &result)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Query
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_query(client: &OpsenseClient, rest: &str) -> anyhow::Result<Option<String>> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.is_empty() {
        anyhow::bail!("usage: :query <node> [from_ts] [to_ts]");
    }
    let node = parts[0];
    let from_ts = parts.get(1).and_then(|s| s.parse::<i64>().ok());
    let to_ts = parts.get(2).and_then(|s| s.parse::<i64>().ok());
    let obs = client.query_timeseries(node, from_ts, to_ts).await?;
    if obs.is_empty() {
        Ok(Some("(no observations)".to_string()))
    } else {
        Ok(Some(format_observations(&obs).to_string()))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn json_to_component(value: serde_json::Value) -> anyhow::Result<ComponentInput> {
    let obj = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("expected JSON object"))?;
    let kind = obj
        .get("type")
        .or_else(|| obj.get("kind"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'type' field"))?
        .to_string();
    let id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'id' field"))?
        .to_string();
    let config = obj.get("config").cloned();
    let inputs = obj
        .get("inputs")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect());
    Ok(ComponentInput { kind, id, config, inputs })
}

fn parse_json(s: &str) -> anyhow::Result<serde_json::Value> {
    serde_json::from_str(s).map_err(|e| anyhow::anyhow!("invalid JSON: {e}"))
}

fn parse_json_obj(s: &str) -> anyhow::Result<serde_json::Value> {
    let v = parse_json(s)?;
    if v.is_object() {
        Ok(v)
    } else {
        anyhow::bail!("expected a JSON object, got {:?}", v)
    }
}

fn split_first_word(s: &str) -> (&str, &str) {
    let s = s.trim();
    if let Some(idx) = s.find(|c: char| c.is_whitespace()) {
        let (a, b) = s.split_at(idx);
        (a, b.trim_start())
    } else {
        (s, "")
    }
}

fn split_once(s: &str, delim: char) -> Option<(&str, &str)> {
    let s = s.trim();
    let idx = s.find(delim)?;
    Some((s[..idx].trim(), s[idx + delim.len_utf8()..].trim()))
}

fn format_node(node: &NodeSummary) -> String {
    format!(
        "id={} type={} inputs=[{}]",
        node.id,
        node.kind,
        node.inputs.join(", ")
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Auth: device flow login
// ─────────────────────────────────────────────────────────────────────────────

/// `:login [host]` — start OAuth2 device flow.
/// `host` mặc định lấy từ `OPSENSE_HOST` env var; nếu không có sẽ dùng
/// `http://127.0.0.1:8080` (UDS không phù hợp cho browser flow).
async fn cmd_login(rest: &str) -> anyhow::Result<Option<String>> {
    let host = if rest.trim().is_empty() {
        std::env::var("OPSENSE_HOST")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
    } else {
        rest.trim().to_string()
    };

    let info = crate::client::request_device_code(&host).await?;
    eprintln!(
        "Open this URL in your browser:\n  {}{}\n\nAnd enter code: {}\n",
        host, info.verification_uri, info.user_code
    );

    let token = crate::client::poll_token(&host, &info.device_code, info.interval as u64).await?;
    let path = crate::client::save_token_to_disk(&token.access_token)?;

    Ok(Some(format!(
        "Logged in. Token saved to {}\nRestart REPL (`:quit` then `opsense repl`) to use it.",
        path.display()
    )))
}

// ─────────────────────────────────────────────────────────────────────────────
// Help text
// ─────────────────────────────────────────────────────────────────────────────

const HELP_TEXT: &str = r#"opsense REPL — connected to POST /graphql

Viewing:
  :status, :s           pipeline status (nodes + stations)
  :attr list, :a         show all attributes
  :attr get <name>       show one attribute
  :node list, :n         pipeline status (shortcut for :status)
  :node get <id>         show one node

Station queries:
  :query <node> [from] [to]    query timeseries (timestamps in unix seconds)

Auth:
  :login                  start OAuth2 device flow (RFC 8628) and save token

Pipeline editing:
  :node add <json>               add a new node (full config required)
  :node add <type> <id>          shorthand add (config rỗng)
  (to edit/remove a node: change the TOML config and restart `opsense serve`)

Attributes (runtime variables for templates):
  :attr set <name> <value>       set an attribute
  :attr rm <name>                remove an attribute
  (env var OPSENSE_ATTR_<NAME> overrides in-memory value)

Misc:
  :help, :h, :?           show this text
  :quit, :q               exit
"#;
