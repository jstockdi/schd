use rusqlite::Connection;
use std::io::Read as _;
use std::os::unix::process::CommandExt;
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::run;

/// Run one tick of the worker: claim a pending run and execute it.
/// Returns true if a run was executed.
pub fn tick(conn: &Connection, timeout: Duration) -> rusqlite::Result<bool> {
    let claimed = run::claim_pending(conn)?;
    let run = match claimed {
        Some(r) => r,
        None => return Ok(false),
    };

    let (name, command) = run::get_schedule_for_run(conn, &run)?;
    let (exit_code, output) = execute_command(&command, timeout);

    run::complete(conn, run.id, exit_code, &output)?;

    eprintln!(
        "[worker] completed run {} for '{}' (schedule {}): exit={}",
        run.id, name, run.schedule_id, exit_code
    );

    Ok(true)
}

fn execute_command(command: &str, timeout: Duration) -> (i32, String) {
    let mut child = match Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return (-1, format!("failed to execute: {e}")),
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let stdout_handle = thread::spawn(move || {
        let mut buf = String::new();
        if let Some(mut out) = stdout {
            let _ = out.read_to_string(&mut buf);
        }
        buf
    });

    let stderr_handle = thread::spawn(move || {
        let mut buf = String::new();
        if let Some(mut err) = stderr {
            let _ = err.read_to_string(&mut buf);
        }
        buf
    });

    let deadline = Instant::now() + timeout;
    let status: Option<ExitStatus> = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {
                if Instant::now() >= deadline {
                    kill_process_group(&child);
                    let _ = child.wait();
                    break None;
                }
                thread::sleep(Duration::from_secs(1));
            }
            Err(e) => return (-1, format!("failed to wait: {e}")),
        }
    };

    let stdout_str = stdout_handle.join().unwrap_or_default();
    let stderr_str = stderr_handle.join().unwrap_or_default();

    let mut combined = stdout_str;
    if !stderr_str.is_empty() {
        if !combined.is_empty() {
            combined.push('\n');
        }
        combined.push_str(&stderr_str);
    }

    match status {
        Some(s) => (s.code().unwrap_or(-1), combined),
        None => {
            if !combined.is_empty() {
                combined.push('\n');
            }
            combined.push_str(&format!(
                "killed: command timed out after {}s",
                timeout.as_secs()
            ));
            (-1, combined)
        }
    }
}

fn kill_process_group(child: &std::process::Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
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
        assert!(!tick(&conn, Duration::from_secs(10)).unwrap());
    }

    #[test]
    fn test_tick_executes_command() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hello").unwrap();
        run::insert_pending(&conn, sched_id, Utc::now()).unwrap();

        assert!(tick(&conn, Duration::from_secs(10)).unwrap());

        let runs = run::list_recent(&conn, Some("job"), 10).unwrap();
        assert_eq!(runs[0].0.status, "success");
        assert!(runs[0].0.output.as_ref().unwrap().contains("hello"));
    }

    #[test]
    fn test_tick_failed_command() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "exit 42").unwrap();
        run::insert_pending(&conn, sched_id, Utc::now()).unwrap();

        assert!(tick(&conn, Duration::from_secs(10)).unwrap());

        let runs = run::list_recent(&conn, Some("job"), 10).unwrap();
        assert_eq!(runs[0].0.status, "failed");
        assert_eq!(runs[0].0.exit_code, Some(42));
    }

    #[test]
    fn test_execute_command_success() {
        let (code, output) = execute_command("echo hello world", Duration::from_secs(10));
        assert_eq!(code, 0);
        assert_eq!(output.trim(), "hello world");
    }

    #[test]
    fn test_execute_command_stderr() {
        let (code, output) = execute_command("echo out && echo err >&2", Duration::from_secs(10));
        assert_eq!(code, 0);
        assert!(output.contains("out"));
        assert!(output.contains("err"));
    }

    #[test]
    fn test_execute_command_failure() {
        let (code, _output) = execute_command("exit 1", Duration::from_secs(10));
        assert_eq!(code, 1);
    }

    #[test]
    fn test_execute_command_timeout() {
        let (code, output) = execute_command("sleep 60", Duration::from_secs(2));
        assert_eq!(code, -1);
        assert!(output.contains("killed: command timed out after 2s"));
    }

    #[test]
    fn test_execute_command_timeout_kills_children() {
        let (code, output) = execute_command("sleep 60 & sleep 60 & wait", Duration::from_secs(2));
        assert_eq!(code, -1);
        assert!(output.contains("timed out"));
    }

    #[test]
    fn test_drains_all_pending() {
        let conn = setup();
        let id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        for i in 0..3 {
            let time = Utc::now() - chrono::Duration::hours(i);
            run::insert_pending(&conn, id, time).unwrap();
        }

        let timeout = Duration::from_secs(10);
        // Should drain one at a time
        assert!(tick(&conn, timeout).unwrap());
        assert!(tick(&conn, timeout).unwrap());
        assert!(tick(&conn, timeout).unwrap());
        assert!(!tick(&conn, timeout).unwrap()); // no more
    }
}
