use rusqlite::Connection;
use std::process::Command;

use crate::run;

/// Run one tick of the worker: claim a pending run and execute it.
/// Returns true if a run was executed.
pub fn tick(conn: &Connection) -> rusqlite::Result<bool> {
    let claimed = run::claim_pending(conn)?;
    let run = match claimed {
        Some(r) => r,
        None => return Ok(false),
    };

    let (name, command) = run::get_schedule_for_run(conn, &run)?;
    let (exit_code, output) = execute_command(&command);

    run::complete(conn, run.id, exit_code, &output)?;

    eprintln!(
        "[worker] completed run {} for '{}' (schedule {}): exit={}",
        run.id, name, run.schedule_id, exit_code
    );

    Ok(true)
}

fn execute_command(command: &str) -> (i32, String) {
    match Command::new("sh").arg("-c").arg(command).output() {
        Ok(output) => {
            let mut combined = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.is_empty() {
                if !combined.is_empty() {
                    combined.push('\n');
                }
                combined.push_str(&stderr);
            }
            let code = output.status.code().unwrap_or(-1);
            (code, combined)
        }
        Err(e) => (-1, format!("failed to execute: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::schedule;
    use chrono::Utc;
    use tempfile::TempDir;

    fn setup() -> Connection {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let conn = db::open(&path).unwrap();
        db::migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn test_tick_no_pending() {
        let conn = setup();
        assert!(!tick(&conn).unwrap());
    }

    #[test]
    fn test_tick_executes_command() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hello").unwrap();
        run::insert_pending(&conn, sched_id, Utc::now()).unwrap();

        assert!(tick(&conn).unwrap());

        let runs = run::list_recent(&conn, Some("job"), 10).unwrap();
        assert_eq!(runs[0].0.status, "success");
        assert!(runs[0].0.output.as_ref().unwrap().contains("hello"));
    }

    #[test]
    fn test_tick_failed_command() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "exit 42").unwrap();
        run::insert_pending(&conn, sched_id, Utc::now()).unwrap();

        assert!(tick(&conn).unwrap());

        let runs = run::list_recent(&conn, Some("job"), 10).unwrap();
        assert_eq!(runs[0].0.status, "failed");
        assert_eq!(runs[0].0.exit_code, Some(42));
    }

    #[test]
    fn test_execute_command_success() {
        let (code, output) = execute_command("echo hello world");
        assert_eq!(code, 0);
        assert_eq!(output.trim(), "hello world");
    }

    #[test]
    fn test_execute_command_stderr() {
        let (code, output) = execute_command("echo out && echo err >&2");
        assert_eq!(code, 0);
        assert!(output.contains("out"));
        assert!(output.contains("err"));
    }

    #[test]
    fn test_execute_command_failure() {
        let (code, _output) = execute_command("exit 1");
        assert_eq!(code, 1);
    }

    #[test]
    fn test_drains_all_pending() {
        let conn = setup();
        let id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        for i in 0..3 {
            let time = Utc::now() - chrono::Duration::hours(i);
            run::insert_pending(&conn, id, time).unwrap();
        }

        // Should drain one at a time
        assert!(tick(&conn).unwrap());
        assert!(tick(&conn).unwrap());
        assert!(tick(&conn).unwrap());
        assert!(!tick(&conn).unwrap()); // no more
    }
}
