//! `sparklint` command-line interface.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};

mod check;
mod discover;
mod output;
mod rules_cmd;

/// Exit codes (ruff-compatible).
pub const EXIT_CLEAN: u8 = 0;
pub const EXIT_FINDINGS: u8 = 1;
pub const EXIT_ERROR: u8 = 2;

#[derive(Debug, Parser)]
#[command(
    name = "sparklint",
    version,
    about = "A fast, Spark-aware linter for PySpark code and Databricks notebooks",
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Lint files and directories.
    Check(CheckArgs),
    /// Explain a rule.
    Rule {
        /// Rule code (ACT002) or name (collect-unbounded).
        code: String,
    },
    /// List all rules.
    Rules {
        #[arg(long, value_enum, default_value_t = ListFormat::Text)]
        format: ListFormat,
        /// Only show implemented rules.
        #[arg(long)]
        implemented: bool,
    },
    /// Print the version.
    Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ListFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    Github,
    Sarif,
}

#[derive(Debug, Args)]
pub struct CheckArgs {
    /// Files or directories to lint (default: current directory); `-` reads one file
    /// from standard input.
    pub paths: Vec<PathBuf>,
    /// Path of the file read from standard input: picks the config, the file kind
    /// (`.ipynb`) and the name in the output. Implies `-` when no path is given.
    #[arg(long, value_name = "PATH")]
    pub stdin_filename: Option<PathBuf>,
    /// Rule selectors to enable (codes, prefixes, `tag:x`, presets); replaces the config.
    #[arg(long, value_delimiter = ',')]
    pub select: Option<Vec<String>>,
    /// Rule selectors to add to the selection.
    #[arg(long, value_delimiter = ',')]
    pub extend_select: Vec<String>,
    /// Rule selectors to disable.
    #[arg(long, value_delimiter = ',')]
    pub ignore: Vec<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub output_format: OutputFormat,
    /// Apply safe fixes.
    #[arg(long)]
    pub fix: bool,
    /// Also apply unsafe fixes (implies --fix).
    #[arg(long)]
    pub unsafe_fixes: bool,
    /// Show counts per rule instead of individual findings.
    #[arg(long)]
    pub statistics: bool,
    /// Use this config file for every file (instead of the closest one).
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Exit with 0 even when findings are reported.
    #[arg(long)]
    pub exit_zero: bool,
    /// Do not print the summary line.
    #[arg(long, short)]
    pub quiet: bool,
}

fn run(cli: Cli) -> anyhow::Result<u8> {
    match cli.command {
        Command::Check(args) => check::run(&args),
        Command::Rule { code } => rules_cmd::explain(&code),
        Command::Rules {
            format,
            implemented,
        } => rules_cmd::list(format, implemented),
        Command::Version => {
            println!("sparklint {}", env!("CARGO_PKG_VERSION"));
            Ok(EXIT_CLEAN)
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            eprintln!("sparklint: error: {e:#}");
            ExitCode::from(EXIT_ERROR)
        }
    }
}
