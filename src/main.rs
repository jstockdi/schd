mod cli;
mod db;
mod run;
mod schedule;
mod scheduler;
mod worker;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "schd", about = "Laptop-friendly cron scheduler")]
struct Cli {
    /// Path to database file
    #[arg(long, env = "SCHD_DB")]
    db: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a new schedule
    Add {
        name: String,
        /// Cron expression (7-field: sec min hour dom month dow year)
        cron: String,
        /// Command to execute
        command: String,
    },
    /// Remove a schedule
    Remove { name: String },
    /// List all schedules
    List,
    /// Enable a schedule
    Enable { name: String },
    /// Disable a schedule
    Disable { name: String },
    /// Show recent runs
    Runs {
        /// Filter by schedule name
        name: Option<String>,
        #[arg(short = 'n', long, default_value = "20")]
        limit: usize,
    },
    /// Start the scheduler loop
    Scheduler {
        #[arg(short, long, default_value = "60")]
        interval: u64,
    },
    /// Start the worker loop
    Worker {
        #[arg(short, long, default_value = "5")]
        interval: u64,
    },
    /// Run both scheduler and worker in one process
    Daemon {
        #[arg(long, default_value = "60")]
        scheduler_interval: u64,
        #[arg(long, default_value = "5")]
        worker_interval: u64,
    },
}

fn main() {
    let args = Cli::parse();
    let db_path = cli::get_db_path(args.db);

    // Long-running commands handle their own DB connections
    match args.command {
        Commands::Scheduler { interval } => cli::cmd_scheduler(&db_path, interval),
        Commands::Worker { interval } => cli::cmd_worker(&db_path, interval),
        Commands::Daemon {
            scheduler_interval,
            worker_interval,
        } => cli::cmd_daemon(&db_path, scheduler_interval, worker_interval),
        cmd => {
            let conn = match cli::init_db(&db_path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            };
            let result = match cmd {
                Commands::Add { name, cron, command } => {
                    cli::cmd_add(&conn, &name, &cron, &command)
                }
                Commands::Remove { name } => cli::cmd_remove(&conn, &name),
                Commands::List => cli::cmd_list(&conn),
                Commands::Enable { name } => cli::cmd_set_enabled(&conn, &name, true),
                Commands::Disable { name } => cli::cmd_set_enabled(&conn, &name, false),
                Commands::Runs { name, limit } => cli::cmd_runs(&conn, name.as_deref(), limit),
                // Already handled above
                Commands::Scheduler { .. }
                | Commands::Worker { .. }
                | Commands::Daemon { .. } => unreachable!(),
            };
            match result {
                Ok(output) => println!("{output}"),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}
