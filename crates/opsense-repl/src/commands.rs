//! REPL command surface: `:station`, `:query`, `:py`, `:stats`, `:plot`,
//! `:vars`, `:sessions`, `:save`/`:load`.
//!
//! Every command runs against the current [`Session`]; results land in the
//! variable namespace as `@N`. `:py` code sees existing `@N` DataFrames as
//! pandas objects injected into its namespace.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use chrono::Utc;

use opsense_core::registry::{self, StationHandle};
use opsense_core::{LogLevel, Observation, Signal, Stage, TelemetryKind};
use opsense_session::{
    load_session, save_session, HistoryEntry, Session, SessionValue, SessionValueType,
};

use crate::display;
use crate::Repl;

// ---------------------------------------------------------------------------
// Arrow conversion (ported from the removed `opsense-store` crate): turn a
// `Vec<Observation>` returned by `Station::query` into a `RecordBatch` so the
// REPL can hand it to the Python bridge as a DataFrame.
// ---------------------------------------------------------------------------

fn kind_tag(kind: &TelemetryKind) -> &'static str {
    match kind {
        TelemetryKind::Metric => "metric",
        TelemetryKind::Log => "log",
        TelemetryKind::Trace => "trace",
    }
}

fn signal_tag(signal: &Signal) -> &'static str {
    match signal {
        Signal::Utilization => "utilization",
        Signal::Saturation => "saturation",
        Signal::Rate => "rate",
        Signal::Errors => "errors",
        Signal::Duration => "duration",
        Signal::Raw => "raw",
    }
}

fn severity_tag(severity: Option<&LogLevel>) -> Option<&'static str> {
    match severity {
        Some(LogLevel::Debug) => Some("debug"),
        Some(LogLevel::Info) => Some("info"),
        Some(LogLevel::Warn) => Some("warn"),
        Some(LogLevel::Error) => Some("error"),
        None => None,
    }
}

/// Convert observations into a single Arrow `RecordBatch`.
///
/// # Errors
/// Arrow column construction failures (effectively only on OOM).
pub fn observations_to_record_batch(
    observations: &[Observation],
) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("metric_id", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("signal", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        // JSON object string; null when the observation has no labels.
        Field::new("labels", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
    ]));
    let len = observations.len();

    if len == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut ts = Int64Array::builder(len);
    let mut metric_id = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut signal = StringBuilder::new();
    let mut value = Float64Array::builder(len);
    let mut labels = StringBuilder::new();
    let mut severity = StringBuilder::new();

    for obs in observations {
        ts.append_value(obs.ts);
        metric_id.append_value(&obs.metric_id);
        kind.append_value(kind_tag(&obs.kind));
        signal.append_value(signal_tag(&obs.signal));
        value.append_value(obs.value);
        labels.append_option(if obs.labels.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&obs.labels).unwrap_or_else(|_| "{}".into()))
        });
        severity.append_option(severity_tag(obs.severity.as_ref()));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts.finish()),
            Arc::new(metric_id.finish()),
            Arc::new(kind.finish()),
            Arc::new(signal.finish()),
            Arc::new(value.finish()),
            Arc::new(labels.finish()),
            Arc::new(severity.finish()),
        ],
    )
}

/// Dispatch one REPL line. Returns the text to print, or `None` for silent
/// success.
///
/// # Errors
/// Command usage errors, unknown stations/variables, Python failures.
pub async fn dispatch(line: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let started = std::time::Instant::now();
    let outcome = route(line, repl).await;
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

    repl.session.record_history(HistoryEntry {
        timestamp: Utc::now(),
        command: line.to_string(),
        result_var: None,
        success: outcome.is_ok(),
        error: outcome.as_ref().err().map(|e| format!("{e:#}")),
        duration_ms,
    });
    outcome
}

async fn route(line: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let (head, rest) = match line.split_once(char::is_whitespace) {
        Some((h, r)) => (h, r.trim()),
        None => (line, ""),
    };

    match head {
        ":help" | ":h" => Ok(Some(help(rest))),
        ":station" => station_cmd(rest).await,
        ":query" => query_cmd(rest, repl).await,
        ":py" | ":python" => py_cmd(rest, repl).await,
        ":jl" | ":julia" => jl_cmd(rest, repl).await,
        ":stats" => stats_cmd(rest, repl).await,
        ":plot" => plot_cmd(rest, repl).await,
        ":vars" | ":ls" => vars_cmd(repl),
        ":sessions" => sessions_cmd(repl),
        ":runner" => runner_cmd(rest, repl).await,
        ":kernel" => kernel_cmd(rest, repl).await,
        ":pattern" => pattern_cmd(rest, repl).await,
        ":catalog" => catalog_cmd(rest).await,
        ":new" => new_cmd(repl).await,
        ":use" => use_cmd(rest, repl),
        ":close" => close_cmd(rest, repl),
        ":save" => save_cmd(rest, repl),
        ":load" => load_cmd(rest, repl),
        other if other.starts_with(':') => Err(anyhow::anyhow!(
            "unknown command `{other}` — try :help"
        )),
        _ => Err(anyhow::anyhow!(
            "interactive Rhai expressions are not wired here yet — prefix analysis commands with `:` (see :help)"
        )),
    }
}

// ---------------------------------------------------------------------------
// :help
// ---------------------------------------------------------------------------

#[must_use]
pub fn help(topic: &str) -> String {
    match topic.trim() {
        "query" => ":query <metric> [--station <id>] [--stage raw|processed] [--from 24h] [--to 0]\n\
                    Queries a station (default: first registered); result stored as @N."
            .into(),
        "py" => ":py <code>\n\
                 Runs Python inside this session. Existing @N DataFrames referenced \
                 in the code are injected as pandas DataFrames. A variable named \
                 `result` is captured back (DataFrame -> new @N, else printed)."
            .into(),
        "stats" => ":stats describe <@var>\n\
                    :stats rolling <@var> [--window 1h] [--fn mean,std] [--col value]\n\
                    :stats quantile <@var> [--q 0.95] [--col value]\n\
                    :stats acf|adf <@var> [--lags 40] [--col value]\n\
                    :stats forecast <@var> [--order 1,1,1] [--steps 24] [--col value]"
            .into(),
        "plot" => ":plot <@var> [--type hist|line|dist|scatter|corr] [--out plot.png]"
            .into(),
        _ => concat!(
            "commands:\n",
            "  :station list | :station describe <id>\n",
            "  :query <metric> [--station <id>] [--stage raw] [--from 24h]  query a station -> @N\n",
            "  :py                                             enter Python sub-REPL\n",
            "  :py <code>                                      single-shot Python (@N in, result out)\n",
            "  :jl                                             enter Julia sub-REPL\n",
            "  :jl <code>                                      single-shot Julia\n",
            "  :stats describe|rolling|quantile|acf|adf|forecast <@var> [...]\n",
            "  :plot <@var> [--type hist] [--out f.png]\n",
            "  :vars | :sessions | :new | :use <id> | :close <id>\n",
            "  :runner list | :runner connect <host:port> [name]\n",
            "  :kernel local | :kernel <name>                  switch execution backend\n",
            "  :pattern add <node> <pattern>                   add log pattern\n",
            "  :pattern get <node> <text>                      check text against patterns\n",
            "  :catalog <node> <query>                         search catalog entries\n",
            "  :save <file> | :load <file>\n",
            "  :help <command>   :quit"
        )
        .to_string(),
    }
}

// ---------------------------------------------------------------------------
// :station
// ---------------------------------------------------------------------------

async fn station_cmd(rest: &str) -> anyhow::Result<Option<String>> {
    let (sub, arg) = match rest.split_once(char::is_whitespace) {
        Some((s, a)) => (s, a.trim()),
        None => (rest, ""),
    };
    match sub {
        "" | "list" => Ok(Some(display::station_table(&registry::station_ids().await).await)),
        "describe" => {
            let id = non_empty(arg, "station id required: :station describe <id>")?;
            match registry::describe_station(id).await {
                Some(info) => Ok(Some(
                    serde_json::to_string_pretty(&info).unwrap_or_else(|_| info.to_string()),
                )),
                None => Err(anyhow::anyhow!("no station registered with id `{id}`")),
            }
        }
        "use" => Err(anyhow::anyhow!(
            "`current station` lives on the session — use :query <metric> against a registered station id instead"
        )),
        other => Err(anyhow::anyhow!(
            "unknown :station subcommand `{other}` (list | describe)"
        )),
    }
}

// ---------------------------------------------------------------------------
// :query
// ---------------------------------------------------------------------------

/// Parse `"30s" | "10m" | "24h" | "7d" | "4w"` into seconds.
pub fn parse_duration(spec: &str) -> anyhow::Result<i64> {
    let digits_end = spec
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| anyhow::anyhow!("duration `{spec}` missing unit (s|m|h|d|w)"))?;
    let (num, unit) = spec.split_at(digits_end);
    let n: i64 = num.parse()?;
    let secs = match unit {
        "s" | "sec" | "secs" => n,
        "m" | "min" | "mins" => n * 60,
        "h" | "hr" | "hours" => n * 3600,
        "d" | "days" => n * 86_400,
        "w" | "weeks" => n * 604_800,
        other => return Err(anyhow::anyhow!("unknown duration unit `{other}`")),
    };
    Ok(secs)
}

struct Flags<'a> {
    args: Vec<&'a str>,
    values: HashMap<&'a str, &'a str>,
}

impl<'a> Flags<'a> {
    fn parse(tokens: &'a [&'a str]) -> Self {
        let mut args = Vec::new();
        let mut values = HashMap::new();
        let mut i = 0;
        while i < tokens.len() {
            let tok = tokens[i];
            if let Some(name) = tok.strip_prefix("--") {
                if let Some(next) = tokens.get(i + 1) {
                    values.insert(name, *next);
                    i += 2;
                    continue;
                }
                values.insert(name, "");
            } else {
                args.push(tok);
            }
            i += 1;
        }
        Self { args, values }
    }

    fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).copied().filter(|v| !v.is_empty())
    }
}

fn non_empty<'a>(s: &'a str, msg: &str) -> anyhow::Result<&'a str> {
    if s.trim().is_empty() {
        Err(anyhow::anyhow!(msg.to_string()))
    } else {
        Ok(s.trim())
    }
}

async fn query_cmd(rest: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let tokens: Vec<&str> = rest.split_whitespace().collect();
    let flags = Flags::parse(&tokens);
    let metric = non_empty(
        flags.args.first().copied().unwrap_or(""),
        "usage: :query <metric> [--stage raw] [--from 24h]",
    )?;

    let stage = match flags.get("stage") {
        Some("raw") => Stage::Raw,
        _ => Stage::Processed,
    };
    let now = Utc::now().timestamp();
    let from_secs = match flags.get("from") {
        Some(spec) => parse_duration(spec)?,
        None => 24 * 3600,
    };
    let to = now
        - flags
            .get("to")
            .and_then(|s| parse_duration(s).ok())
            .unwrap_or(0);
    let from = now - from_secs;

    // Station selection, most specific wins:
    // 1. explicit `--station <id>`;
    // 2. a station whose id matches the metric name (legacy convenience);
    // 3. the first registered station.
    let st: StationHandle = if let Some(id) = flags.get("station") {
        registry::station(id).await.ok_or_else(|| {
            anyhow::anyhow!("no station registered with id `{id}` — see :station list")
        })?
    } else if let Some(s) = registry::station(metric).await {
        s
    } else {
        first_station().await.ok_or_else(|| {
            anyhow::anyhow!(
                "no station registered — start the pipeline or pass an explicit station: :query <metric> --station <id>"
            )
        })?
    };

    let observations = {
        let g = st.read().await;
        g.query(stage, metric, from, to).await
    };
    let batch =
        observations_to_record_batch(&observations).map_err(|e| anyhow::anyhow!("arrow: {e}"))?;
    let rows = batch.num_rows();

    let var = repl.session.with_state(|state| {
        let name = state.next_var_name();
        state.set_variable(name.clone(), SessionValue::dataframe(batch));
        name
    });

    Ok(Some(format!(
        "{rows} rows [{metric} {stage:?} {from}..{to}] -> {var}"
    )))
}

async fn first_station() -> Option<StationHandle> {
    for id in registry::station_ids().await {
        if let Some(s) = registry::station(&id).await {
            return Some(s);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Variable plumbing shared by :py / :stats / :plot
// ---------------------------------------------------------------------------

/// Collect every `@N` DataFrame the code references so the bridge can inject
/// them as pandas objects.
/// Session variables a piece of kernel code references, in first-mention
/// order: users write `_df_1` in kernel code (mapped back to `@1`), a literal
/// `@1` mention also counts.
pub(crate) fn referenced_input_vars(code: &str) -> Vec<String> {
    let mut vars = Vec::new();
    for token in code.split(|c: char| !(c.is_alphanumeric() || c == '_') && c != '@') {
        let var = match token.strip_prefix("_df_") {
            Some(n) if n.chars().all(|c| c.is_ascii_digit()) && !n.is_empty() => format!("@{n}"),
            _ => token.to_string(),
        };
        if var.starts_with('@') && !vars.contains(&var) {
            vars.push(var);
        }
    }
    vars
}

pub(crate) fn collect_inputs(code: &str, session: &Session) -> HashMap<String, RecordBatch> {
    let mut inputs = HashMap::new();
    for var in referenced_input_vars(code) {
        let state = session.state();
        let guard = state.lock().unwrap();
        if let Some(value) = guard.get_variable(&var) {
            if let Some(batch) = value.as_dataframe() {
                inputs.insert(var.clone(), batch.clone());
            }
        }
    }
    inputs
}

/// Ensure `repl.session` points at a Python kernel session (reusing the cached
/// one) so Python-bound commands work even though the default kernel is `echo`.
async fn ensure_python_session(repl: &mut Repl) -> anyhow::Result<()> {
    if let Some(session) = &repl.python_session {
        repl.session = session.clone();
        return Ok(());
    }
    let backend = repl
        .manager
        .python_backend()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let session = repl
        .manager
        .create_session_with(backend)
        .await
        .map_err(|e| anyhow::anyhow!("python session: {e:#}"))?;
    carry_over_variables(&repl.session, &session);
    repl.python_session = Some(session.clone());
    repl.session = session;
    Ok(())
}

async fn run_python(repl: &mut Repl, code: &str) -> anyhow::Result<(String, Option<RecordBatch>)> {
    // The default kernel is `echo`; route Python execution to a real Python
    // session so analysis still works when Python is available.
    ensure_python_session(repl).await?;
    let inputs = collect_inputs(code, &repl.session);
    let started = std::time::Instant::now();
    let output = repl.session.execute_with(code, inputs).await?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

    if let Some(err) = output.error {
        anyhow::bail!("{err}");
    }
    match output.dataframe {
        Some(batch) => {
            let summary = format!(
                "{} rows x {} cols ({elapsed_ms} ms)",
                batch.num_rows(),
                batch.num_columns()
            );
            Ok((summary, Some(batch)))
        }
        None => {
            // stdout từ kernel (print, puts...) — in TRƯỚC text result.
            if !output.stdout.is_empty() {
                return Ok((output.stdout, None));
            }
            match output.text {
                Some(text) => Ok((text, None)),
                None => Ok((format!("ok ({elapsed_ms} ms)"), None)),
            }
        }
    }
}

/// Execute `code`, capture `result` into the next `@N` when it produced a
/// DataFrame, and render the printable outcome.
async fn execute_and_store(repl: &mut Repl, code: &str) -> anyhow::Result<String> {
    let (summary, batch) = run_python(repl, code).await?;
    match batch {
        Some(rb) => {
            let var = repl.session.with_state(|state| {
                let name = state.next_var_name();
                state.set_variable(name.clone(), SessionValue::dataframe(rb));
                name
            });
            Ok(format!("{summary} -> {var}"))
        }
        None => Ok(summary),
    }
}

// ---------------------------------------------------------------------------
// :py
// ---------------------------------------------------------------------------

async fn py_cmd(code: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    if code.is_empty() {
        // Enter Python sub-REPL: start a real Python kernel session (the
        // default kernel is `echo`, which does not run Python).
        let backend = repl
            .manager
            .python_backend()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        let new_session = repl
            .manager
            .create_session_with(backend)
            .await
            .map_err(|e| anyhow::anyhow!("python session: {e:#}"))?;
        repl.python_session = Some(new_session.clone());
        carry_over_variables(&repl.session, &new_session);
        repl.session = new_session.clone();
        repl.mode = crate::ReplMode::Kernel(crate::KernelLang::Python);
        repl.input_mode = crate::InputMode::Line;
        repl.block_buf.clear();
        println!(
            "Python kernel [session {}] — :block=multi-line | :line=single | exit()=back",
            new_session.id()
        );
        return Ok(None);
    }
    Ok(Some(execute_and_store(repl, code).await?))
}

/// `:jl` — enter Julia sub-REPL (creates a session on the julia kernel backend).
/// Copy the variable namespace (`@N`) from the current session into a new one.
/// Entering a sub-REPL (`:py`/`:jl`) swaps in a fresh session — without this,
/// datasets produced by `:query` would be invisible to kernel code.
fn carry_over_variables(from: &Session, to: &Session) {
    let vars: Vec<(String, SessionValue)> = {
        let state = from.state();
        let guard = state.lock().unwrap();
        guard.variables.clone().into_iter().collect()
    };
    let state = to.state();
    let mut guard = state.lock().unwrap();
    for (name, value) in vars {
        guard.set_variable(name, value);
    }
}

async fn jl_cmd(_code: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let backend = repl
        .manager
        .julia_backend()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let new_session = repl
        .manager
        .create_session_with(backend)
        .await
        .map_err(|e| anyhow::anyhow!("julia session: {e:#}"))?;

    carry_over_variables(&repl.session, &new_session);
    let old_id = repl.session.id();
    repl.session = new_session.clone();
    repl.mode = crate::ReplMode::Kernel(crate::KernelLang::Julia);
    repl.input_mode = crate::InputMode::Line;
    repl.block_buf.clear();

    println!(
        "Julia kernel [session {}] — :block=multi-line | :line=single | exit()=back",
        new_session.id()
    );
    let _ = old_id; // old python session stays alive in the manager
    Ok(None)
}

// ---------------------------------------------------------------------------
// :stats
// ---------------------------------------------------------------------------

async fn stats_cmd(rest: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let tokens: Vec<&str> = rest.split_whitespace().collect();
    let flags = Flags::parse(&tokens);
    let sub = flags.args.first().copied().unwrap_or("");
    let var = flags.args.get(1).copied().unwrap_or("");

    let col = flags.get("col").unwrap_or("value");
    let series = format!("{var}[{col:?}]");

    let code = match sub {
        "describe" => format!("result = opsense_stats.describe({var})"),
        "rolling" => {
            let window = flags.get("window").unwrap_or("1h");
            let fns = flags
                .get("fn")
                .unwrap_or("mean")
                .split(',')
                .map(|f| format!("{f:?}"))
                .collect::<Vec<_>>()
                .join(",");
            format!("result = opsense_stats.rolling({var}, {window:?}, [{fns}])")
        }
        "quantile" => {
            let q = flags.get("q").unwrap_or("0.95");
            format!("result = opsense_stats.quantile({series}, {q})")
        }
        "acf" => {
            let lags = flags.get("lags").unwrap_or("40");
            format!("result = opsense_stats.acf({series}, nlags={lags})")
        }
        "adf" | "adftest" => format!("result = opsense_stats.adf_test({series})"),
        "forecast" => {
            let order = flags
                .get("order")
                .unwrap_or("1,1,1")
                .split(',')
                .collect::<Vec<_>>()
                .join(",");
            let steps = flags.get("steps").unwrap_or("24");
            format!(
                "result = opsense_stats.arima_forecast({series}, ({order}), {steps}).to_frame('forecast')"
            )
        }
        other => {
            return Err(anyhow::anyhow!(
                "unknown :stats subcommand `{other}` — see :help stats"
            ))
        }
    };
    Ok(Some(execute_and_store(repl, &code).await?))
}

// ---------------------------------------------------------------------------
// :plot
// ---------------------------------------------------------------------------

const PLOT_TYPES: &[(&str, &str)] = &[
    ("hist", "plot_hist"),
    ("line", "plot_line"),
    ("dist", "plot_dist"),
    ("scatter", "plot_scatter"),
    ("corr", "plot_corr"),
];

async fn plot_cmd(rest: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let tokens: Vec<&str> = rest.split_whitespace().collect();
    let flags = Flags::parse(&tokens);
    let var = non_empty(
        flags.args.first().copied().unwrap_or(""),
        "usage: :plot <@var> [--type hist] [--out f.png]",
    )?;

    let kind = flags.get("type").unwrap_or("hist");
    let fn_name = PLOT_TYPES
        .iter()
        .find(|(name, _)| *name == kind)
        .map(|(_, f)| *f)
        .ok_or_else(|| {
            anyhow::anyhow!("unknown plot type `{kind}` (hist|line|dist|scatter|corr)")
        })?;

    // Python returns a data URI; the Rust side writes the file so plots work
    // even in allow_fs=false sandboxes.
    let out_path = flags
        .get("out")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(format!("opsense_plot_{kind}.png")));

    let (summary, _) =
        run_python(repl, &format!("result = opsense_plots.{fn_name}({var})")).await?;
    let uri = summary.trim();
    let b64 = uri
        .rsplit("base64,")
        .next()
        .ok_or_else(|| anyhow::anyhow!("plot returned unexpected payload: {uri:.80}"))?;
    let png = base64::engine::general_purpose::STANDARD.decode(b64.trim())?;
    std::fs::write(&out_path, &png)?;
    Ok(Some(format!(
        "saved {} bytes -> {}",
        png.len(),
        out_path.display()
    )))
}

// ---------------------------------------------------------------------------
// :runner / :kernel — execution backend routing (checklist §6)
// ---------------------------------------------------------------------------

async fn runner_cmd(rest: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let (sub, args) = match rest.split_once(char::is_whitespace) {
        Some((s, a)) => (s, a.trim()),
        None => (rest, ""),
    };
    match sub {
        "" | "list" => {
            let rows = repl
                .manager
                .list_backends()
                .into_iter()
                .map(|(name, kind)| vec![name, kind.to_string()])
                .collect();
            Ok(Some(display::table(&["backend", "kind"], rows)))
        }
        "connect" => {
            let mut parts = args.split_whitespace();
            let addr = non_empty(
                parts.next().unwrap_or(""),
                "usage: :runner connect <host:port> [name]",
            )?;
            let name = parts
                .next()
                .unwrap_or(addr.split(':').next().unwrap_or("runner"));
            let backend = repl.manager.register_runner(name, addr).await?;
            repl.session.switch_backend(backend).await?;
            Ok(Some(format!(
                "session {} now runs on runner `{name}` ({addr})",
                repl.session.id()
            )))
        }
        other => Err(anyhow::anyhow!(
            "unknown :runner subcommand `{other}` (list | connect <addr> [name])"
        )),
    }
}

async fn kernel_cmd(rest: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let target = non_empty(rest, "usage: :kernel local | :kernel <runner-name>")?;
    let backend = if target == "local" {
        repl.manager.local_backend()
    } else {
        repl.manager
            .runner(target)
            .ok_or_else(|| anyhow::anyhow!("no backend named `{target}` — see :runner list"))?
    };
    repl.session.switch_backend(backend).await?;
    Ok(Some(format!(
        "session {} switched to `{}`",
        repl.session.id(),
        target
    )))
}

// ---------------------------------------------------------------------------
// :pattern / :catalog — text index queries
// ---------------------------------------------------------------------------

async fn pattern_cmd(rest: &str, _repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let (sub, args) = match rest.split_once(char::is_whitespace) {
        Some((sub, a)) => (sub, a.trim()),
        None => (rest, ""),
    };
    let (node, text) = match args.split_once(char::is_whitespace) {
        Some((n, t)) => (n.trim(), t.trim()),
        None => (args, ""),
    };

    match sub {
        "add" => {
            if node.is_empty() || text.is_empty() {
                return Err(anyhow::anyhow!("usage: :pattern add <node> <pattern>"));
            }
            match registry::text_index(node).await {
                Some(idx) => {
                    idx.write().await.add_pattern(text).await;
                    Ok(Some(format!("pattern added to `{node}`")))
                }
                None => Err(anyhow::anyhow!("no text index `{node}` registered")),
            }
        }
        "get" => {
            if node.is_empty() || text.is_empty() {
                return Err(anyhow::anyhow!("usage: :pattern get <node> <text>"));
            }
            match registry::text_index(node).await {
                Some(idx) => {
                    let known = idx.read().await.is_known(text).await.unwrap_or(false);
                    Ok(Some(format!(
                        "{} → {}",
                        if known { "KNOWN" } else { "NEW" },
                        text
                    )))
                }
                None => Err(anyhow::anyhow!("no text index `{node}` registered")),
            }
        }
        other => Err(anyhow::anyhow!(
            "unknown :pattern subcommand `{other}` (add | get)"
        )),
    }
}

async fn catalog_cmd(rest: &str) -> anyhow::Result<Option<String>> {
    let (node, pattern) = match rest.split_once(char::is_whitespace) {
        Some((n, p)) => (n.trim(), p.trim()),
        None => (rest, ""),
    };
    if node.is_empty() {
        return Err(anyhow::anyhow!("usage: :catalog <node> <query>"));
    }
    match registry::text_index(node).await {
        Some(idx) => {
            let entries = idx.read().await.search_entries(pattern, None).await;
            if entries.is_empty() {
                return Ok(Some("(no matches)".into()));
            }
            let rows: Vec<Vec<String>> = entries
                .iter()
                .map(|(k, v)| vec![k.clone(), v.clone()])
                .collect();
            Ok(Some(display::table(&["key", "value"], rows)))
        }
        None => Err(anyhow::anyhow!("no text index `{node}` registered")),
    }
}

// ---------------------------------------------------------------------------
// :vars / :sessions / :new / :use / :close / :save / :load
// ---------------------------------------------------------------------------

fn vars_cmd(repl: &Repl) -> anyhow::Result<Option<String>> {
    let holder = repl.session.state();
    let state = holder.lock().unwrap();
    if state.variables.is_empty() {
        return Ok(Some("(no variables)".into()));
    }
    let mut rows: Vec<Vec<String>> = state
        .variables
        .iter()
        .map(|(name, v)| {
            let shape = match v.value_type {
                SessionValueType::DataFrame => v
                    .as_dataframe()
                    .map(|rb| format!("{}x{}", rb.num_rows(), rb.num_columns()))
                    .unwrap_or_default(),
                SessionValueType::Plot => format!(
                    "{} bytes {}",
                    v.as_bytes().map_or(0, <[u8]>::len),
                    v.metadata.get("format").cloned().unwrap_or_default()
                ),
                SessionValueType::Model | SessionValueType::Bytes => {
                    format!("{} bytes", v.as_bytes().map_or(0, <[u8]>::len))
                }
                SessionValueType::Scalar => {
                    v.as_scalar().map(|s| s.to_string()).unwrap_or_default()
                }
            };
            vec![
                name.clone(),
                format!("{:?}", v.value_type),
                shape,
                v.created_at.format("%H:%M:%S").to_string(),
            ]
        })
        .collect();
    rows.sort_by(|a, b| a[0].cmp(&b[0]));
    Ok(Some(display::table(
        &["var", "type", "shape/value", "created"],
        rows,
    )))
}

fn sessions_cmd(repl: &Repl) -> anyhow::Result<Option<String>> {
    let rows = repl
        .manager
        .list_sessions()
        .into_iter()
        .map(|(id, status, created)| {
            let marker = if id == repl.session.id() { "*" } else { "" };
            vec![
                format!("{id}{marker}"),
                format!("{status:?}"),
                created.to_rfc3339(),
            ]
        })
        .collect();
    Ok(Some(display::table(
        &["session", "status", "created"],
        rows,
    )))
}

async fn new_cmd(repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let session = repl.manager.create_session_async().await?;
    let id = session.id();
    repl.session = session;
    Ok(Some(format!("switched to new session {id}")))
}

fn find_by_prefix(repl: &Repl, prefix: &str) -> anyhow::Result<uuid::Uuid> {
    let matches: Vec<_> = repl
        .manager
        .list_sessions()
        .into_iter()
        .map(|(id, _, _)| id)
        .filter(|id| id.to_string().starts_with(prefix))
        .collect();
    match matches.len() {
        1 => Ok(matches[0]),
        0 => Err(anyhow::anyhow!("no session matching `{prefix}`")),
        _ => Err(anyhow::anyhow!(
            "ambiguous prefix `{prefix}` ({} matches)",
            matches.len()
        )),
    }
}

fn use_cmd(prefix: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let id = find_by_prefix(repl, non_empty(prefix, "usage: :use <session-id-prefix>")?)?;
    let session = repl
        .manager
        .get_session(id)
        .ok_or_else(|| anyhow::anyhow!("session {id} vanished"))?;
    repl.session = session;
    Ok(Some(format!("switched to session {id}")))
}

fn close_cmd(prefix: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let id = find_by_prefix(
        repl,
        non_empty(prefix, "usage: :close <session-id-prefix>")?,
    )?;
    if id == repl.session.id() {
        return Err(anyhow::anyhow!(
            "refusing to close the current session — :use another or :new first"
        ));
    }
    anyhow::ensure!(repl.manager.close_session(id), "no session {id}");
    Ok(Some(format!("closed {id}")))
}

fn save_cmd(path: &str, repl: &Repl) -> anyhow::Result<Option<String>> {
    let path = non_empty(path, "usage: :save <file>")?;
    save_session(&repl.session, std::path::Path::new(path))?;
    Ok(Some(format!("session saved -> {path}")))
}

fn load_cmd(path: &str, repl: &mut Repl) -> anyhow::Result<Option<String>> {
    let path = non_empty(path, "usage: :load <file>")?;
    let session = load_session(&repl.manager, std::path::Path::new(path))?;
    let id = session.id();
    repl.session = session;
    Ok(Some(format!("loaded session {id} <- {path}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opsense_core::config::{
        Config, EngineConfig, ReplConfig, SessionConfig, SourcesConfig, StorageConfig,
    };
    use opsense_session::{EchoBackend, SessionManager};

    // ---- parse_duration --------------------------------------------------

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
        assert_eq!(parse_duration("45sec").unwrap(), 45);
        assert_eq!(parse_duration("5secs").unwrap(), 5);
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(parse_duration("10m").unwrap(), 600);
        assert_eq!(parse_duration("2min").unwrap(), 120);
    }

    #[test]
    fn parse_duration_hours_days_weeks() {
        assert_eq!(parse_duration("24h").unwrap(), 86_400);
        assert_eq!(parse_duration("7d").unwrap(), 7 * 86_400);
        assert_eq!(parse_duration("4w").unwrap(), 4 * 604_800);
    }

    #[test]
    fn parse_duration_rejects_missing_unit() {
        assert!(parse_duration("100").is_err());
    }

    #[test]
    fn parse_duration_rejects_unknown_unit() {
        assert!(parse_duration("10x").is_err());
    }

    #[test]
    fn parse_duration_rejects_non_numeric() {
        assert!(parse_duration("abch").is_err());
    }

    // ---- help ------------------------------------------------------------

    #[test]
    fn help_default_lists_topics() {
        let out = help("");
        assert!(out.contains("commands:"));
        assert!(out.contains(":help"));
        assert!(out.contains(":quit"));
    }

    #[test]
    fn help_specific_topic_query() {
        let out = help("query");
        assert!(out.contains(":query"));
        assert!(out.contains("--stage"));
        assert!(out.contains("--from"));
    }

    #[test]
    fn help_specific_topic_py() {
        let out = help("py");
        assert!(out.contains(":py"));
        assert!(out.contains("DataFrame"));
    }

    #[test]
    fn help_specific_topic_stats() {
        let out = help("stats");
        assert!(out.contains("describe"));
        assert!(out.contains("rolling"));
        assert!(out.contains("quantile"));
        assert!(out.contains("forecast"));
    }

    #[test]
    fn help_specific_topic_plot() {
        let out = help("plot");
        assert!(out.contains(":plot"));
        assert!(out.contains("--type"));
    }

    #[test]
    fn help_trims_input() {
        assert!(help("  query  ").contains(":query"));
    }

    // ---- non_empty -------------------------------------------------------

    #[test]
    fn non_empty_returns_trimmed() {
        assert_eq!(non_empty("  hello  ", "missing").unwrap(), "hello");
    }

    #[test]
    fn non_empty_rejects_blank() {
        assert!(non_empty("", "missing").is_err());
        assert!(non_empty("   ", "missing").is_err());
        assert!(non_empty("\t", "missing").is_err());
    }

    // ---- kind_tag / signal_tag / severity_tag ----------------------------

    #[test]
    fn kind_tag_known_kinds() {
        assert_eq!(kind_tag(&TelemetryKind::Metric), "metric");
        assert_eq!(kind_tag(&TelemetryKind::Log), "log");
        assert_eq!(kind_tag(&TelemetryKind::Trace), "trace");
    }

    #[test]
    fn signal_tag_known_signals() {
        assert_eq!(signal_tag(&Signal::Utilization), "utilization");
        assert_eq!(signal_tag(&Signal::Saturation), "saturation");
        assert_eq!(signal_tag(&Signal::Errors), "errors");
    }

    #[test]
    fn severity_tag_maps_levels() {
        assert_eq!(severity_tag(Some(&LogLevel::Info)), Some("info"));
        assert_eq!(severity_tag(Some(&LogLevel::Error)), Some("error"));
        assert_eq!(severity_tag(Some(&LogLevel::Warn)), Some("warn"));
        assert_eq!(severity_tag(None), None);
    }

    // ---- Flags::parse ----------------------------------------------------

    #[test]
    fn flags_parse_bare_args() {
        let f = Flags::parse(&["metric", "--stage", "raw", "--from", "24h"]);
        assert_eq!(f.args, vec!["metric"]);
        assert_eq!(f.get("stage"), Some("raw"));
        assert_eq!(f.get("from"), Some("24h"));
    }

    #[test]
    fn flags_get_omits_empty_values() {
        let f = Flags::parse(&["--only"]);
        assert_eq!(f.get("only"), None);
    }

    #[test]
    fn flags_collects_bare_args() {
        let f = Flags::parse(&["a", "b", "c"]);
        assert_eq!(f.args, vec!["a", "b", "c"]);
        assert!(f.values.is_empty());
    }

    // ---- referenced_input_vars / collect_inputs --------------------------
    // Regression: sub-REPL code references session variables as `_df_N`, but
    // the collector only matched literal `@N` mentions — so `@N` was never
    // pushed into the kernel and `_df_1` was always UndefVarError.

    #[test]
    fn referenced_vars_maps_df_prefix() {
        assert_eq!(referenced_input_vars("nrow(_df_1)"), vec!["@1"]);
        assert_eq!(
            referenced_input_vars("describe(_df_12)"),
            vec!["@12".to_string()]
        );
    }

    #[test]
    fn referenced_vars_accepts_literal_at() {
        assert_eq!(referenced_input_vars("mean(@3)"), vec!["@3"]);
    }

    #[test]
    fn referenced_vars_dedups_and_keeps_order() {
        let code = "_df_2 + _df_1 + _df_2 + @1";
        assert_eq!(referenced_input_vars(code), vec!["@2", "@1"]);
    }

    #[test]
    fn referenced_vars_ignores_non_variables() {
        assert!(referenced_input_vars("using DataFrames, Statistics").is_empty());
        assert!(referenced_input_vars("exit()").is_empty());
        assert!(referenced_input_vars("1 + 1").is_empty());
        // `_df` without digits, `_df_abc` non-numeric, bare `_df1` — none map.
        assert!(referenced_input_vars("_df + _df_abc + _df1").is_empty());
    }

    #[test]
    fn referenced_vars_handles_underscores_in_names() {
        // `my_df_1` is a plain identifier, not a session-variable reference.
        assert!(referenced_input_vars("my_df_1").is_empty());
    }

    #[test]
    fn collect_inputs_pushes_df_referenced_datasets() {
        let config = Config {
            engine: EngineConfig::default(),
            capacity: HashMap::new(),
            sources: SourcesConfig::default(),
            attributes: HashMap::new(),
            storage: StorageConfig::default(),
            pipeline: None,
            session: SessionConfig::default(),
            repl: ReplConfig::default(),
        };
        let manager = SessionManager::new(config, Arc::new(EchoBackend));
        let session = manager
            .block_on(manager.create_session_with(Arc::new(EchoBackend)))
            .expect("echo session");

        let batch = observations_to_record_batch(&[Observation::new(
            1_788_047_156,
            "up".to_string(),
            TelemetryKind::Metric,
            Signal::Raw,
            1.0,
        )])
        .expect("batch");
        session
            .state()
            .lock()
            .unwrap()
            .set_variable("@1".into(), SessionValue::dataframe(batch));

        let inputs = collect_inputs("describe(_df_1)", &session);
        assert_eq!(inputs.len(), 1, "`_df_1` must resolve to session var `@1`");
        assert_eq!(inputs["@1"].num_rows(), 1);

        // Code that references nothing pushes nothing.
        assert!(collect_inputs("using DataFrames", &session).is_empty());
        // A referenced var that does not exist is silently skipped.
        assert!(collect_inputs("nrow(_df_9)", &session).is_empty());
    }

    #[test]
    fn carry_over_moves_variables_to_new_session() {
        let config = Config {
            engine: EngineConfig::default(),
            capacity: HashMap::new(),
            sources: SourcesConfig::default(),
            attributes: HashMap::new(),
            storage: StorageConfig::default(),
            pipeline: None,
            session: SessionConfig::default(),
            repl: ReplConfig::default(),
        };
        let backend = Arc::new(EchoBackend);
        let manager = SessionManager::new(config, backend);
        let old = manager
            .block_on(manager.create_session_with(Arc::new(EchoBackend)))
            .expect("session");
        let new = manager
            .block_on(manager.create_session_with(Arc::new(EchoBackend)))
            .expect("session");

        old.state().lock().unwrap().set_variable(
            "@1".into(),
            SessionValue::scalar(42.0f64),
        );
        // Regression: without carry-over, `@1` lives only on the old session
        // and sub-REPL code can never see it.
        assert!(new.state().lock().unwrap().get_variable("@1").is_none());

        carry_over_variables(&old, &new);
        assert!(new.state().lock().unwrap().get_variable("@1").is_some());
        // Old session keeps its variable (non-destructive copy).
        assert!(old.state().lock().unwrap().get_variable("@1").is_some());
    }
}
