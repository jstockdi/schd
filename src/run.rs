use chrono::{DateTime, Utc};
use rusqlite::{Connection, Result, params};

#[derive(Debug, Clone)]
pub struct Run {
    pub id: i64,
    pub schedule_id: i64,
    pub scheduled_for: String,
    pub status: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub output: Option<String>,
}

/// Insert a pending run if one doesn't already exist for this schedule+time.
/// Returns the new run ID if inserted, or None if it already existed.
pub fn insert_pending(
    conn: &Connection,
    schedule_id: i64,
    scheduled_for: DateTime<Utc>,
) -> Result<Option<i64>> {
    let time_str = scheduled_for.format("%Y-%m-%dT%H:%M:%S").to_string();
    let rows = conn.execute(
        "INSERT OR IGNORE INTO runs (schedule_id, scheduled_for, status) VALUES (?1, ?2, 'pending')",
        params![schedule_id, time_str],
    )?;
    if rows > 0 {
        Ok(Some(conn.last_insert_rowid()))
    } else {
        Ok(None)
    }
}

/// Claim a pending run for execution. Sets status to 'running' and records start time.
/// Returns the run if one was claimed.
pub fn claim_pending(conn: &Connection) -> Result<Option<Run>> {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();

    // Find the oldest pending run
    let run = {
        let mut stmt = conn.prepare(
            "SELECT id, schedule_id, scheduled_for, status, started_at, finished_at, exit_code, output
             FROM runs WHERE status = 'pending' ORDER BY scheduled_for ASC LIMIT 1",
        )?;
        let mut rows = stmt.query_map([], row_to_run)?;
        match rows.next() {
            Some(r) => r?,
            None => return Ok(None),
        }
    };

    // Mark it as running
    let updated = conn.execute(
        "UPDATE runs SET status = 'running', started_at = ?1 WHERE id = ?2 AND status = 'pending'",
        params![now, run.id],
    )?;

    if updated == 0 {
        // Another worker claimed it
        return Ok(None);
    }

    Ok(Some(Run {
        status: "running".to_string(),
        started_at: Some(now),
        ..run
    }))
}

/// Mark a run as complete.
pub fn complete(
    conn: &Connection,
    run_id: i64,
    exit_code: i32,
    output: &str,
) -> Result<()> {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    let status = if exit_code == 0 { "success" } else { "failed" };
    conn.execute(
        "UPDATE runs SET status = ?1, finished_at = ?2, exit_code = ?3, output = ?4 WHERE id = ?5",
        params![status, now, exit_code, output, run_id],
    )?;
    Ok(())
}

/// List recent runs, optionally filtered by schedule name.
pub fn list_recent(conn: &Connection, schedule_name: Option<&str>, limit: usize) -> Result<Vec<(Run, String)>> {
    let query = match schedule_name {
        Some(_) => {
            "SELECT r.id, r.schedule_id, r.scheduled_for, r.status, r.started_at, r.finished_at, r.exit_code, r.output, s.name
             FROM runs r JOIN schedules s ON r.schedule_id = s.id
             WHERE s.name = ?1
             ORDER BY r.id DESC LIMIT ?2"
        }
        None => {
            "SELECT r.id, r.schedule_id, r.scheduled_for, r.status, r.started_at, r.finished_at, r.exit_code, r.output, s.name
             FROM runs r JOIN schedules s ON r.schedule_id = s.id
             ORDER BY r.id DESC LIMIT ?2"
        }
    };

    let mut stmt = conn.prepare(query)?;

    let rows = if let Some(name) = schedule_name {
        stmt.query_map(params![name, limit as i64], |row| {
            Ok((row_to_run_inline(row)?, row.get::<_, String>(8)?))
        })?
    } else {
        // For unfiltered query, the parameter positions differ
        // We need to handle this differently since ?1 isn't used
        drop(stmt);
        let query_no_filter =
            "SELECT r.id, r.schedule_id, r.scheduled_for, r.status, r.started_at, r.finished_at, r.exit_code, r.output, s.name
             FROM runs r JOIN schedules s ON r.schedule_id = s.id
             ORDER BY r.id DESC LIMIT ?1";
        let mut stmt2 = conn.prepare(query_no_filter)?;
        let rows = stmt2.query_map(params![limit as i64], |row| {
            Ok((row_to_run_inline(row)?, row.get::<_, String>(8)?))
        })?;
        return rows.collect();
    };

    rows.collect()
}

fn row_to_run(row: &rusqlite::Row) -> rusqlite::Result<Run> {
    Ok(Run {
        id: row.get(0)?,
        schedule_id: row.get(1)?,
        scheduled_for: row.get(2)?,
        status: row.get(3)?,
        started_at: row.get(4)?,
        finished_at: row.get(5)?,
        exit_code: row.get(6)?,
        output: row.get(7)?,
    })
}

fn row_to_run_inline(row: &rusqlite::Row) -> rusqlite::Result<Run> {
    row_to_run(row)
}

/// Get the output for a run by ID.
pub fn get_output(conn: &Connection, run_id: i64) -> Result<Option<String>> {
    match conn.query_row(
        "SELECT output FROM runs WHERE id = ?1",
        params![run_id],
        |row| row.get::<_, Option<String>>(0),
    ) {
        Ok(output) => Ok(output),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Get the name and command for a run by joining with schedules.
pub fn get_schedule_for_run(conn: &Connection, run: &Run) -> Result<(String, String)> {
    conn.query_row(
        "SELECT name, command FROM schedules WHERE id = ?1",
        params![run.schedule_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::schedule;
    use tempfile::TempDir;

    fn setup() -> Connection {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let conn = db::open(&path).unwrap();
        db::migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn test_insert_pending() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        let time = Utc::now();

        assert!(insert_pending(&conn, sched_id, time).unwrap().is_some());
        // Duplicate insert should be ignored
        assert!(insert_pending(&conn, sched_id, time).unwrap().is_none());
    }

    #[test]
    fn test_claim_pending_none() {
        let conn = setup();
        assert!(claim_pending(&conn).unwrap().is_none());
    }

    #[test]
    fn test_claim_and_complete() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        let time = Utc::now();
        insert_pending(&conn, sched_id, time).unwrap();

        let run = claim_pending(&conn).unwrap().unwrap();
        assert_eq!(run.status, "running");
        assert!(run.started_at.is_some());

        complete(&conn, run.id, 0, "hello\n").unwrap();

        let recent = list_recent(&conn, Some("job"), 10).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].0.status, "success");
        assert_eq!(recent[0].0.exit_code, Some(0));
        assert_eq!(recent[0].0.output.as_deref(), Some("hello\n"));
        assert_eq!(recent[0].1, "job");
    }

    #[test]
    fn test_complete_failed() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "false").unwrap();
        insert_pending(&conn, sched_id, Utc::now()).unwrap();
        let run = claim_pending(&conn).unwrap().unwrap();
        complete(&conn, run.id, 1, "error\n").unwrap();

        let recent = list_recent(&conn, Some("job"), 10).unwrap();
        assert_eq!(recent[0].0.status, "failed");
        assert_eq!(recent[0].0.exit_code, Some(1));
    }

    #[test]
    fn test_list_recent_no_filter() {
        let conn = setup();
        let id1 = schedule::add(&conn, "job1", "0 0 9 * * * *", "echo 1").unwrap();
        let id2 = schedule::add(&conn, "job2", "0 0 10 * * * *", "echo 2").unwrap();
        insert_pending(&conn, id1, Utc::now()).unwrap();
        insert_pending(&conn, id2, Utc::now()).unwrap();

        let all = list_recent(&conn, None, 10).unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_list_recent_with_limit() {
        let conn = setup();
        let id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo 1").unwrap();
        for i in 0..5 {
            let time = Utc::now() - chrono::Duration::hours(i);
            insert_pending(&conn, id, time).unwrap();
        }

        let limited = list_recent(&conn, None, 3).unwrap();
        assert_eq!(limited.len(), 3);
    }

    #[test]
    fn test_get_schedule_for_run() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        insert_pending(&conn, sched_id, Utc::now()).unwrap();
        let run = claim_pending(&conn).unwrap().unwrap();

        let (name, cmd) = get_schedule_for_run(&conn, &run).unwrap();
        assert_eq!(name, "job");
        assert_eq!(cmd, "echo hi");
    }

    #[test]
    fn test_claim_already_claimed() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        insert_pending(&conn, sched_id, Utc::now()).unwrap();

        let run = claim_pending(&conn).unwrap().unwrap();
        assert_eq!(run.status, "running");

        // Second claim should return None
        assert!(claim_pending(&conn).unwrap().is_none());
    }

    #[test]
    fn test_cascade_delete() {
        let conn = setup();
        let sched_id = schedule::add(&conn, "job", "0 0 9 * * * *", "echo hi").unwrap();
        insert_pending(&conn, sched_id, Utc::now()).unwrap();

        schedule::remove(&conn, "job").unwrap();

        let all = list_recent(&conn, None, 10).unwrap();
        assert_eq!(all.len(), 0);
    }
}
