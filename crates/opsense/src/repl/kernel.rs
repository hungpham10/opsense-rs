//! Kernel REPL — talk directly to a runner via gRPC.
//!
//! Two execution modes:
//! - **Inline** (default after `:py`/`:jl`): each non-empty line becomes one
//!   `execute` call.
//! - **Block** (after `:block`): lines accumulate in a buffer. An empty line
//!   triggers a single `execute` with the buffer joined by `\n`, then the buffer
//!   is cleared and the REPL stays in Block mode.
//!
//! The kernel natively handles multi-line code (Python uses `exec(compile(...))`,
//! Julia uses `Meta.parseall()`), so the client just sends the joined buffer
//! as one `CodeRequest.code` string.

use anyhow::{Context as _, Result};
use opsense_proto::pb::SessionParams;
use reedline::{DefaultPrompt, Reedline, Signal};

use crate::client::RunnerClient;

/// REPL mode (driven by user commands).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// No kernel session yet. Accepts only `:py`/`:jl` (to start) and `:exit`.
    Idle,
    /// Each non-empty line is executed immediately.
    Inline,
    /// Lines accumulate; empty line triggers execution, stays in Block.
    Block,
}

pub struct KernelRepl {
    client: Option<RunnerClient>,
    mode: Mode,
    kernel_kind: Option<String>,
    endpoint: String,
    buffer: Vec<String>,
}

impl KernelRepl {
    /// Build a new REPL. Does not connect to the runner until the user
    /// types `:py` or `:jl`.
    pub fn new(endpoint: String) -> Self {
        Self {
            client: None,
            mode: Mode::Idle,
            kernel_kind: None,
            endpoint,
            buffer: Vec::new(),
        }
    }

    /// Run the interactive kernel REPL loop.
    pub async fn run(&mut self) -> Result<()> {
        println!("opsense kernel REPL  →  {}", self.endpoint);
        println!("type :py to start a Python session, :jl for Julia, :exit to quit\n");

        let mut rl = Reedline::create();

        loop {
            let prompt = DefaultPrompt::default();
            match rl.read_line(&prompt) {
                Ok(Signal::Success(line)) => {
                    if let Err(e) = self.handle_line(&line).await {
                        eprintln!("error: {e:#}");
                    }
                }
                Ok(Signal::CtrlC) => {
                    self.buffer.clear();
                    println!("(buffer cleared)");
                }
                Ok(Signal::CtrlD) => {
                    self.shutdown().await;
                    break;
                }
                Err(e) => {
                    eprintln!("readline error: {e}");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Process one line of input.
    async fn handle_line(&mut self, line: &str) -> Result<()> {
        let line = line.trim();
        if line.is_empty() {
            return self.handle_empty_line().await;
        }

        if line.starts_with(':') {
            return self.handle_command(line).await;
        }

        // Non-command, non-empty line.
        match self.mode {
            Mode::Idle => {
                println!("no kernel session; type :py or :jl first");
            }
            Mode::Inline => {
                self.execute_line(line).await?;
            }
            Mode::Block => {
                self.buffer.push(line.to_string());
            }
        }
        Ok(())
    }

    /// Empty line: in Block mode, execute the buffer.
    async fn handle_empty_line(&mut self) -> Result<()> {
        match self.mode {
            Mode::Block if !self.buffer.is_empty() => {
                let code = std::mem::take(&mut self.buffer).join("\n");
                self.execute_code(&code).await?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Dispatch a `:` command.
    async fn handle_command(&mut self, line: &str) -> Result<()> {
        let (cmd, _rest) = split_first_word(line);
        match cmd {
            ":py" | ":python" => self.start_kernel("python").await,
            ":jl" | ":julia" => self.start_kernel("julia").await,
            ":echo" => self.start_kernel("echo").await,
            ":inline" => self.set_mode(Mode::Inline),
            ":block" => self.set_mode(Mode::Block),
            ":send" => self.flush_buffer().await,
            ":abort" => self.abort().await,
            ":exit" | ":quit" | ":q" => {
                self.shutdown().await;
                std::process::exit(0);
            }
            ":help" | ":h" | ":?" => {
                println!("{HELP_TEXT}");
                Ok(())
            }
            other => anyhow::bail!("unknown command '{other}'; type :help"),
        }
    }

    async fn start_kernel(&mut self, kind: &str) -> Result<()> {
        // If we already have a session, close it first.
        if let Some(mut c) = self.client.take() {
            let _ = c.close().await;
        }
        self.mode = Mode::Idle;
        self.kernel_kind = Some(kind.to_string());
        self.buffer.clear();

        let session_id = format!("repl-{}", uuid_v4_simple());
        let params = SessionParams {
            session_id,
            env: Default::default(),
            allow_fs: false,
            allow_net: false,
            max_memory_mb: 0,
            packages: vec![],
            require_challenge: false,
            requested_role: String::new(),
        };

        println!("connecting to {} (kernel={kind})...", self.endpoint);
        let client = RunnerClient::connect(&self.endpoint, params)
            .await
            .with_context(|| format!("failed to connect to {}", self.endpoint))?;
        println!("session ready (kernel={kind})");
        self.client = Some(client);
        self.mode = Mode::Inline;
        Ok(())
    }

    fn set_mode(&mut self, new_mode: Mode) -> Result<()> {
        if self.client.is_none() {
            println!("no kernel session; type :py or :jl first");
            return Ok(());
        }
        // If leaving Block with non-empty buffer, warn.
        if self.mode == Mode::Block
            && new_mode != Mode::Block
            && !self.buffer.is_empty()
        {
            println!("warning: discarding {} buffered line(s)", self.buffer.len());
            self.buffer.clear();
        }
        self.mode = new_mode;
        match new_mode {
            Mode::Inline => println!("switched to inline mode"),
            Mode::Block => println!("switched to block mode (empty line executes)"),
            Mode::Idle => {}
        }
        Ok(())
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            println!("(buffer is empty)");
            return Ok(());
        }
        let code = std::mem::take(&mut self.buffer).join("\n");
        self.execute_code(&code).await
    }

    async fn abort(&mut self) -> Result<()> {
        let Some(client) = self.client.as_mut() else {
            println!("no kernel session");
            return Ok(());
        };
        client.interrupt().await.context("interrupt failed")?;
        println!("(interrupt sent)");
        Ok(())
    }

    async fn execute_line(&mut self, line: &str) -> Result<()> {
        self.execute_code(line).await
    }

    async fn execute_code(&mut self, code: &str) -> Result<()> {
        let Some(client) = self.client.as_mut() else {
            println!("no kernel session; type :py or :jl first");
            return Ok(());
        };
        let outcome = client.execute(code).await.context("execute failed")?;
        print_outcome(&outcome);
        Ok(())
    }

    async fn shutdown(&mut self) {
        if let Some(mut client) = self.client.take() {
            let _ = client.close().await;
        }
        println!("Goodbye.");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn print_outcome(outcome: &crate::client::ExecOutcome) {
    let stdout = outcome.stdout();
    if !stdout.is_empty() {
        print!("{stdout}");
        if !stdout.ends_with('\n') {
            println!();
        }
    }
    let stderr = outcome.stderr();
    if !stderr.is_empty() {
        eprint!("{stderr}");
        if !stderr.ends_with('\n') {
            eprintln!();
        }
    }
    if let Some(v) = &outcome.value {
        use opsense_proto::pb::value::Kind as K;
        match &v.kind {
            Some(K::Text(t)) => println!("=> {t}"),
            Some(K::Number(n)) => println!("=> {n}"),
            Some(K::Flag(b)) => println!("=> {b}"),
            Some(K::Nothing(_)) => println!("=> None"),
            Some(K::Json(j)) => println!("=> {j}"),
            Some(K::Dataframe(df)) => {
                println!("=> DataFrame(rows={}, cols={}, columns=[{}])",
                    df.rows, df.cols, df.columns.join(", "));
            }
            Some(K::Artifact(a)) => {
                println!("=> Artifact(name={}, mime={}, {} bytes)",
                    a.name, a.mime, a.data.len());
            }
            Some(K::Raw(b)) => println!("=> Raw({} bytes)", b.len()),
            None => {}
        }
    }
    if let Some(err) = &outcome.error {
        if !outcome.timed_out {
            eprintln!("[{}] {}", err.kind, err.message);
        } else {
            eprintln!("[timeout] {}", err.message);
        }
    }
}

fn split_first_word(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    if let Some(idx) = s.find(|c: char| c.is_whitespace()) {
        let (a, b) = s.split_at(idx);
        (a, b.trim_start())
    } else {
        (s, "")
    }
}

fn uuid_v4_simple() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

const HELP_TEXT: &str = r#"opsense kernel REPL — direct gRPC to a running runner

Kernel sessions:
  :py, :python               start a Python session
  :jl, :julia                start a Julia session
  :echo                      start the echo kernel (for testing)
  (starting a new kernel closes the current session)

Execution modes:
  >>> inline  (default)      each line is sent as one execute call
  ... block                  accumulate lines; empty line executes
  :inline, :block            switch modes (discards block buffer on :inline)
  :send                      force-execute the current block buffer

Control:
  :abort                     interrupt the running request
  :exit, :quit, :q           close session and exit
  :help, :h, :?              this text
"#;
