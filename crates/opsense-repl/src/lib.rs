//! Interactive REPL shell for Opsense (`opsense serve --repl`).
//!
//! Wraps [`reedline`] with Opsense commands (`:station`, `:query`, `:py`,
//! `:stats`, …) bound to one analysis session. `:py` code runs in the
//! session's kernel process over IPC; query results cross the boundary
//! as Arrow RecordBatches.
//!
//! Sub-REPL: `:py` enters a Python sub-REPL (`python>` prompt), `:jl`
//! enters Julia (`julia>`). Inside a sub-REPL, `:block` toggles multi-line
//! mode (accumulate until empty line), `:line` returns to single-line,
//! `:exit` returns to the opsense prompt.

pub mod commands;
pub mod completer;
pub mod display;

use std::io::{BufRead, IsTerminal};
use std::sync::Arc;

use opsense_session::{Session, SessionManager};
use reedline::{DefaultPrompt, Reedline, Signal};

// ── modes ──────────────────────────────────────────────────────────────────

/// Which sub-REPL the user is in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplMode {
    /// Main opsense shell — commands dispatch normally.
    Opsense,
    /// Kernel sub-REPL — every line goes to the kernel process.
    Kernel(KernelLang),
}

/// Which kernel language the sub-REPL targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelLang {
    Python,
    Julia,
}

impl KernelLang {
    pub fn prompt(self) -> &'static str {
        match self {
            Self::Python => "python> ",
            Self::Julia => "julia> ",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Python => "Python",
            Self::Julia => "Julia",
        }
    }
}

/// How Enter behaves inside a kernel sub-REPL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    /// Enter executes immediately.
    Line,
    /// Enter accumulates; an EMPTY line sends the whole block.
    Block,
}

/// The live REPL context.
pub struct Repl {
    pub manager: Arc<SessionManager>,
    pub session: Arc<Session>,
    pub mode: ReplMode,
    pub input_mode: InputMode,
    /// Accumulated lines in Block input mode.
    pub block_buf: Vec<String>,
    /// Cached Python session so `:py`/`:stats`/`:plot` reuse one process
    /// instead of spawning a new kernel per command.
    pub python_session: Option<Arc<Session>>,
}

// ── helpers ────────────────────────────────────────────────────────────────

fn is_exit(line: &str) -> bool {
    matches!(line.trim(), ":quit" | ":q" | ":exit" | "exit()" | "exit")
}

fn is_block_toggle(line: &str) -> Option<InputMode> {
    match line.trim() {
        ":block" => Some(InputMode::Block),
        ":line" => Some(InputMode::Line),
        _ => None,
    }
}

/// Run one line in Opsense mode (command dispatch). Returns true = stop.
fn run_opsense_line(line: &str, repl: &mut Repl) -> bool {
    let line = line.trim();
    if line.is_empty() {
        return false;
    }
    if is_exit(line) {
        return true;
    }
    let manager = Arc::clone(&repl.manager);
    match manager.block_on(commands::dispatch(line, repl)) {
        Ok(Some(output)) => println!("{output}"),
        Ok(None) => {}
        Err(err) => eprintln!("error: {err:#}"),
    }
    false
}

/// Run one line inside a kernel sub-REPL (Python/Julia).
/// Returns true when the user wants to return to Opsense mode.
fn run_kernel_line(line: &str, repl: &mut Repl, _lang: KernelLang) -> bool {
    let trimmed = line.trim();

    // Exit sub-REPL.
    if is_exit(trimmed) {
        println!("← back to opsense");
        return true; // true = leave kernel mode (not quit app)
    }

    // Toggle block/line.
    if let Some(new_mode) = is_block_toggle(trimmed) {
        repl.input_mode = new_mode;
        repl.block_buf.clear();
        match new_mode {
            InputMode::Block => println!("— block mode: Enter adds a line, empty line runs —"),
            InputMode::Line => println!("— line mode: Enter executes immediately —"),
        }
        return false;
    }

    // Block mode: accumulate or flush.
    if repl.input_mode == InputMode::Block {
        if trimmed.is_empty() {
            // Empty line → execute accumulated block.
            let code = repl.block_buf.join("\n");
            repl.block_buf.clear();
            if !code.is_empty() {
                execute_kernel_code(repl, &code);
            }
        } else {
            repl.block_buf.push(line.to_string());
        }
        return false;
    }

    // Line mode: execute immediately.
    if !trimmed.is_empty() {
        execute_kernel_code(repl, line);
    }
    false
}

/// Execute one code string via the session's kernel backend.
fn execute_kernel_code(repl: &Repl, code: &str) {
    let manager = Arc::clone(&repl.manager);
    // Đẩy các `@N` mà code tham chiếu (trực tiếp hoặc qua `_df_N`) vào kernel
    // trước khi execute — nếu không thì `_df_1` chưa từng tồn tại ở sub-REPL.
    let inputs = crate::commands::collect_inputs(code, &repl.session);
    match manager.block_on(async { repl.session.execute_with(code, inputs).await }) {
        Ok(out) => {
            // stdout từ kernel (print, puts, ...) — in TRƯỚC kết quả.
            if !out.stdout.is_empty() {
                println!("{}", out.stdout);
            }
            match (&out.text, &out.dataframe, &out.error) {
                (Some(text), _, _) => println!("{text}"),
                (_, Some(df), _) => println!("{} rows × {} cols", df.num_rows(), df.num_columns()),
                (_, _, Some(e)) => eprintln!("{e}"),
                _ => {}
            }
        }
        Err(err) => eprintln!("error: {err:#}"),
    }
}

// ── main entry ─────────────────────────────────────────────────────────────

/// Run the interactive REPL loop.
///
/// # Errors
/// Propagates terminal setup failures and kernel spawn failures.
pub fn run(manager: Arc<SessionManager>) -> anyhow::Result<()> {
    let session = manager
        .create_session()
        .map_err(|err| anyhow::anyhow!("kernel session unavailable ({err:#})"))?;

    println!(
        "Opsense REPL — session {} — :help for commands, :quit to exit.",
        session.id()
    );

    let mut repl = Repl {
        manager: Arc::clone(&manager),
        session,
        mode: ReplMode::Opsense,
        input_mode: InputMode::Line,
        block_buf: Vec::new(),
        python_session: None,
    };

    if !std::io::stdin().is_terminal() {
        // Piped stdin: plain line-by-line reading.
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let line = line.map_err(|e| anyhow::anyhow!("read stdin: {e}"))?;
            if step(&line, &mut repl) {
                break;
            }
        }
    } else {
        // Interactive terminal with Reedline.
        let mut rl = Reedline::create();
        loop {
            let prompt = current_prompt(&repl);
            let signal = rl
                .read_line(&prompt)
                .map_err(|e| anyhow::anyhow!("terminal I/O failed: {e}"))?;
            match signal {
                Signal::Success(buffer) => {
                    if step(&buffer, &mut repl) {
                        break;
                    }
                }
                Signal::CtrlD | Signal::CtrlC => continue,
            }
        }
    }

    manager.close_all();
    Ok(())
}

/// Process one input line based on current mode.
/// Returns true when the app should quit (only from Opsense mode).
fn step(line: &str, repl: &mut Repl) -> bool {
    match &repl.mode {
        ReplMode::Opsense => run_opsense_line(line, repl),
        ReplMode::Kernel(lang) => {
            if run_kernel_line(line, repl, *lang) {
                // Leave kernel sub-REPL, return to opsense mode.
                repl.mode = ReplMode::Opsense;
                repl.input_mode = InputMode::Line;
                repl.block_buf.clear();
            }
            false // never quit from kernel sub-mode
        }
    }
}

/// Current prompt string based on mode.
fn current_prompt(repl: &Repl) -> DefaultPrompt {
    match &repl.mode {
        ReplMode::Opsense => DefaultPrompt::new(
            reedline::DefaultPromptSegment::Basic("opsense".into()),
            reedline::DefaultPromptSegment::Basic("❯ ".into()),
        ),
        ReplMode::Kernel(lang) => DefaultPrompt::new(
            reedline::DefaultPromptSegment::Basic(lang.name().to_string()),
            reedline::DefaultPromptSegment::Basic(lang.prompt().into()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_exit_matches_known_aliases() {
        for s in [":quit", ":q", ":exit", "exit()", "exit"] {
            assert!(is_exit(s), "expected {s:?} to be an exit");
        }
    }

    #[test]
    fn is_exit_tolerates_whitespace() {
        assert!(is_exit("  :quit  "));
        assert!(is_exit("\texit()"));
    }

    #[test]
    fn is_exit_rejects_other_commands() {
        for s in [":help", ":station", "exit; ", "noexit", ""] {
            assert!(!is_exit(s), "expected {s:?} to NOT exit");
        }
    }

    #[test]
    fn is_block_toggle_returns_mode() {
        assert_eq!(is_block_toggle(":block"), Some(InputMode::Block));
        assert_eq!(is_block_toggle(":line"), Some(InputMode::Line));
    }

    #[test]
    fn is_block_toggle_ignores_other_lines() {
        assert_eq!(is_block_toggle(""), None);
        assert_eq!(is_block_toggle(":help"), None);
        assert_eq!(is_block_toggle("  :block  "), Some(InputMode::Block));
    }

    #[test]
    fn kernel_lang_prompts_and_names() {
        assert_eq!(KernelLang::Python.prompt(), "python> ");
        assert_eq!(KernelLang::Julia.prompt(), "julia> ");
        assert_eq!(KernelLang::Python.name(), "Python");
        assert_eq!(KernelLang::Julia.name(), "Julia");
    }

    #[test]
    fn repl_mode_equality() {
        assert_eq!(ReplMode::Opsense, ReplMode::Opsense);
        assert_eq!(
            ReplMode::Kernel(KernelLang::Python),
            ReplMode::Kernel(KernelLang::Python)
        );
        assert_ne!(
            ReplMode::Kernel(KernelLang::Python),
            ReplMode::Kernel(KernelLang::Julia)
        );
        assert_ne!(ReplMode::Opsense, ReplMode::Kernel(KernelLang::Python));
    }
}
