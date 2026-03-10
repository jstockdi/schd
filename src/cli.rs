use crate::{db, run, schedule, scheduler, worker};
use rusqlite::Connection;
use std::fmt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
pub enum Error {
    Db(rusqlite::Error),
    Validation(String),
    NotFound(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Db(e) => write!(f, "{e}"),
            Error::Validation(msg) => write!(f, "{msg}"),
            Error::NotFound(name) => write!(f, "{name} not found"),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Db(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn init_db(path: &Path) -> Result<Connection> {
    let conn = db::open(path)?;
    db::migrate(&conn)?;
    Ok(conn)
}

pub fn get_db_path(cli_path: Option<PathBuf>) -> PathBuf {
    cli_path.unwrap_or_else(db::default_db_path)
}

pub fn cmd_add(conn: &Connection, name: &str, cron: &str, command: &str) -> Result<String> {
    schedule::validate_cron(cron).map_err(Error::Validation)?;
    let id = schedule::add(conn, name, cron, command)?;
    Ok(format!("added schedule '{name}' (id={id})"))
}

pub fn cmd_remove(conn: &Connection, name: &str) -> Result<String> {
    if schedule::remove(conn, name)? {
        Ok(format!("removed schedule '{name}'"))
    } else {
        Err(Error::NotFound(format!("schedule '{name}'")))
    }
}

pub fn cmd_list(conn: &Connection) -> Result<String> {
    let schedules = schedule::list(conn)?;
    if schedules.is_empty() {
        return Ok("no schedules".to_string());
    }
    let mut out = format!(
        "{:<4} {:<20} {:<8} {:<25} {}",
        "ID", "NAME", "ENABLED", "CRON", "COMMAND"
    );
    for s in &schedules {
        out.push('\n');
        out.push_str(&format!(
            "{:<4} {:<20} {:<8} {:<25} {}",
            s.id,
            s.name,
            if s.enabled { "yes" } else { "no" },
            s.cron,
            s.command
        ));
    }
    Ok(out)
}

pub fn cmd_set_enabled(conn: &Connection, name: &str, enabled: bool) -> Result<String> {
    let action = if enabled { "enabled" } else { "disabled" };
    if schedule::set_enabled(conn, name, enabled)? {
        Ok(format!("{action} schedule '{name}'"))
    } else {
        Err(Error::NotFound(format!("schedule '{name}'")))
    }
}

pub fn cmd_trigger(conn: &Connection, name: &str) -> Result<String> {
    let sched = schedule::get_by_name(conn, name)?
        .ok_or_else(|| Error::NotFound(name.to_string()))?;
    let now = chrono::Utc::now();
    match run::insert_pending(conn, sched.id, now)? {
        Some(run_id) => Ok(format!("triggered run {run_id} for '{name}'")),
        None => Ok(format!("run already pending for '{name}'")),
    }
}

pub fn cmd_runs(conn: &Connection, name: Option<&str>, limit: usize) -> Result<String> {
    let runs = run::list_recent(conn, name, limit)?;
    if runs.is_empty() {
        return Ok("no runs".to_string());
    }
    let mut out = format!(
        "{:<4} {:<20} {:<20} {:<8} {:<6} {}",
        "ID", "SCHEDULE", "SCHEDULED FOR", "STATUS", "EXIT", "FINISHED"
    );
    for (r, sched_name) in &runs {
        out.push('\n');
        out.push_str(&format!(
            "{:<4} {:<20} {:<20} {:<8} {:<6} {}",
            r.id,
            sched_name,
            r.scheduled_for,
            r.status,
            r.exit_code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "-".to_string()),
            r.finished_at.as_deref().unwrap_or("-"),
        ));
    }
    Ok(out)
}

pub fn cmd_output(conn: &Connection, run_id: i64) -> Result<String> {
    match run::get_output(conn, run_id)? {
        Some(output) => Ok(output),
        None => Err(Error::NotFound(format!("run {run_id}"))),
    }
}

/// Run one scheduler tick against a db path. Opens its own connection.
/// Returns a message if new runs were scheduled.
pub fn scheduler_tick(db_path: &Path) -> Option<String> {
    let conn = db::open(db_path).ok()?;
    db::migrate(&conn).ok()?;
    match scheduler::tick(&conn) {
        Ok(n) if n > 0 => Some(format!("[scheduler] scheduled {n} new run(s)")),
        Ok(_) => None,
        Err(e) => {
            eprintln!("[scheduler] error: {e}");
            None
        }
    }
}

/// Run one worker tick against a db path. Opens its own connection.
/// Returns true if a job was executed (caller should immediately try again).
pub fn worker_tick(db_path: &Path) -> bool {
    let conn = match db::open(db_path) {
        Ok(c) => c,
        Err(_) => return false,
    };
    if db::migrate(&conn).is_err() {
        return false;
    }
    match worker::tick(&conn) {
        Ok(ran) => ran,
        Err(e) => {
            eprintln!("[worker] error: {e}");
            false
        }
    }
}

pub fn cmd_scheduler(db_path: &Path, interval: u64) -> ! {
    eprintln!("[scheduler] starting (interval={interval}s)");
    loop {
        if let Some(msg) = scheduler_tick(db_path) {
            eprintln!("{msg}");
        }
        thread::sleep(Duration::from_secs(interval));
    }
}

pub fn cmd_worker(db_path: &Path, interval: u64) -> ! {
    eprintln!("[worker] starting (interval={interval}s)");
    loop {
        if worker_tick(db_path) {
            continue;
        }
        thread::sleep(Duration::from_secs(interval));
    }
}

fn ensure_log_dir() {
    if let Some(home) = std::env::var_os("HOME") {
        let log_dir = PathBuf::from(home).join("Library/Logs/schd");
        let _ = std::fs::create_dir_all(log_dir);
    }
}

pub fn cmd_daemon(db_path: &Path, scheduler_interval: u64, worker_interval: u64) -> ! {
    ensure_log_dir();
    eprintln!(
        "[daemon] starting (scheduler={scheduler_interval}s, worker={worker_interval}s)"
    );

    let db_path_sched = db_path.to_path_buf();
    let _sched_handle = thread::spawn(move || loop {
        if let Some(msg) = scheduler_tick(&db_path_sched) {
            eprintln!("{msg}");
        }
        thread::sleep(Duration::from_secs(scheduler_interval));
    });

    let db_path_work = db_path.to_path_buf();
    loop {
        if worker_tick(&db_path_work) {
            continue;
        }
        thread::sleep(Duration::from_secs(worker_interval));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Connection) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let conn = init_db(&path).unwrap();
        (dir, conn)
    }

    #[test]
    fn test_get_db_path_default() {
        let path = get_db_path(None);
        assert!(path.ends_with(".schd/schd.db"));
    }

    #[test]
    fn test_get_db_path_custom() {
        let path = get_db_path(Some(PathBuf::from("/tmp/custom.db")));
        assert_eq!(path, PathBuf::from("/tmp/custom.db"));
    }

    #[test]
    fn test_init_db() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let conn = init_db(&path).unwrap();
        let list = schedule::list(&conn).unwrap();
        assert!(list.is_empty());
    }

    #[test]
    fn test_cmd_add_success() {
        let (_dir, conn) = setup();
        let result = cmd_add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        assert!(result.contains("added schedule 'job'"));
    }

    #[test]
    fn test_cmd_add_invalid_cron() {
        let (_dir, conn) = setup();
        let err = cmd_add(&conn, "job", "bad", "echo hi").unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
    }

    #[test]
    fn test_cmd_add_duplicate() {
        let (_dir, conn) = setup();
        cmd_add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        let err = cmd_add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap_err();
        assert!(matches!(err, Error::Db(_)));
    }

    #[test]
    fn test_cmd_remove_success() {
        let (_dir, conn) = setup();
        cmd_add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        let result = cmd_remove(&conn, "job").unwrap();
        assert!(result.contains("removed schedule 'job'"));
    }

    #[test]
    fn test_cmd_remove_not_found() {
        let (_dir, conn) = setup();
        let err = cmd_remove(&conn, "nope").unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn test_cmd_list_empty() {
        let (_dir, conn) = setup();
        let result = cmd_list(&conn).unwrap();
        assert_eq!(result, "no schedules");
    }

    #[test]
    fn test_cmd_list_with_schedules() {
        let (_dir, conn) = setup();
        cmd_add(&conn, "job1", "0 0 9 * * * *", "echo 1").unwrap();
        cmd_add(&conn, "job2", "0 0 10 * * * *", "echo 2").unwrap();
        let result = cmd_list(&conn).unwrap();
        assert!(result.contains("job1"));
        assert!(result.contains("job2"));
        assert!(result.contains("yes"));
    }

    #[test]
    fn test_cmd_list_with_disabled() {
        let (_dir, conn) = setup();
        cmd_add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        cmd_set_enabled(&conn, "job", false).unwrap();
        let result = cmd_list(&conn).unwrap();
        assert!(result.contains("no"));
    }

    #[test]
    fn test_cmd_enable_disable() {
        let (_dir, conn) = setup();
        cmd_add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();

        let result = cmd_set_enabled(&conn, "job", false).unwrap();
        assert!(result.contains("disabled"));

        let result = cmd_set_enabled(&conn, "job", true).unwrap();
        assert!(result.contains("enabled"));
    }

    #[test]
    fn test_cmd_enable_not_found() {
        let (_dir, conn) = setup();
        let err = cmd_set_enabled(&conn, "nope", true).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn test_cmd_runs_empty() {
        let (_dir, conn) = setup();
        let result = cmd_runs(&conn, None, 10).unwrap();
        assert_eq!(result, "no runs");
    }

    #[test]
    fn test_cmd_runs_with_data() {
        let (_dir, conn) = setup();
        cmd_add(&conn, "job", "0 * * * * * *", "echo hi").unwrap();
        scheduler::tick(&conn).unwrap();
        worker::tick(&conn).unwrap();

        let result = cmd_runs(&conn, Some("job"), 10).unwrap();
        assert!(result.contains("job"));
        assert!(result.contains("success"));
    }

    #[test]
    fn test_cmd_runs_with_pending() {
        let (_dir, conn) = setup();
        cmd_add(&conn, "job", "0 * * * * * *", "echo hi").unwrap();
        scheduler::tick(&conn).unwrap();

        let result = cmd_runs(&conn, None, 10).unwrap();
        assert!(result.contains("pending"));
    }

    #[test]
    fn test_cmd_runs_shows_exit_and_finished() {
        let (_dir, conn) = setup();
        cmd_add(&conn, "job", "0 * * * * * *", "exit 1").unwrap();
        scheduler::tick(&conn).unwrap();
        worker::tick(&conn).unwrap();

        let result = cmd_runs(&conn, None, 10).unwrap();
        assert!(result.contains("failed"));
        assert!(result.contains("1")); // exit code
    }

    #[test]
    fn test_error_display() {
        let e = Error::Validation("bad input".to_string());
        assert_eq!(format!("{e}"), "bad input");

        let e = Error::NotFound("schedule 'foo'".to_string());
        assert_eq!(format!("{e}"), "schedule 'foo' not found");

        let e = Error::Db(rusqlite::Error::QueryReturnedNoRows);
        let msg = format!("{e}");
        assert!(!msg.is_empty());
    }

    #[test]
    fn test_scheduler_tick_no_schedules() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        init_db(&db_path).unwrap();

        let result = scheduler_tick(&db_path);
        assert!(result.is_none());
    }

    #[test]
    fn test_scheduler_tick_with_schedule() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = init_db(&db_path).unwrap();
        cmd_add(&conn, "job", "0 * * * * * *", "echo hi").unwrap();
        drop(conn);

        let result = scheduler_tick(&db_path);
        assert!(result.is_some());
        assert!(result.unwrap().contains("scheduled"));
    }

    #[test]
    fn test_scheduler_tick_idempotent() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = init_db(&db_path).unwrap();
        cmd_add(&conn, "job", "0 * * * * * *", "echo hi").unwrap();
        drop(conn);

        scheduler_tick(&db_path);
        let result = scheduler_tick(&db_path);
        assert!(result.is_none()); // already scheduled
    }

    #[test]
    fn test_scheduler_tick_bad_path() {
        let result = scheduler_tick(Path::new("/nonexistent/dir/db.sqlite"));
        assert!(result.is_none());
    }

    #[test]
    fn test_worker_tick_no_work() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        init_db(&db_path).unwrap();

        assert!(!worker_tick(&db_path));
    }

    #[test]
    fn test_worker_tick_with_work() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = init_db(&db_path).unwrap();
        cmd_add(&conn, "job", "0 * * * * * *", "echo hi").unwrap();
        drop(conn);

        scheduler_tick(&db_path);
        assert!(worker_tick(&db_path));
        assert!(!worker_tick(&db_path)); // no more work
    }

    #[test]
    fn test_worker_tick_bad_path() {
        assert!(!worker_tick(Path::new("/nonexistent/dir/db.sqlite")));
    }
}
