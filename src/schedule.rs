use chrono::{DateTime, Utc};
use cron::Schedule as CronSchedule;
use rusqlite::{Connection, Result, params};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Schedule {
    pub id: i64,
    pub name: String,
    pub cron: String,
    pub command: String,
    pub enabled: bool,
    pub created_at: String,
}

pub fn validate_cron(expr: &str) -> std::result::Result<(), String> {
    CronSchedule::from_str(expr)
        .map(|_| ())
        .map_err(|e| format!("invalid cron expression: {e}"))
}

pub fn add(conn: &Connection, name: &str, cron: &str, command: &str) -> Result<i64> {
    conn.execute(
        "INSERT INTO schedules (name, cron, command) VALUES (?1, ?2, ?3)",
        params![name, cron, command],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn remove(conn: &Connection, name: &str) -> Result<bool> {
    let rows = conn.execute("DELETE FROM schedules WHERE name = ?1", params![name])?;
    Ok(rows > 0)
}

pub fn set_enabled(conn: &Connection, name: &str, enabled: bool) -> Result<bool> {
    let rows = conn.execute(
        "UPDATE schedules SET enabled = ?1 WHERE name = ?2",
        params![enabled as i32, name],
    )?;
    Ok(rows > 0)
}

pub fn list(conn: &Connection) -> Result<Vec<Schedule>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, cron, command, enabled, created_at FROM schedules ORDER BY id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(Schedule {
            id: row.get(0)?,
            name: row.get(1)?,
            cron: row.get(2)?,
            command: row.get(3)?,
            enabled: row.get::<_, i32>(4)? != 0,
            created_at: row.get(5)?,
        })
    })?;
    rows.collect()
}

pub fn get_by_name(conn: &Connection, name: &str) -> Result<Option<Schedule>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, cron, command, enabled, created_at FROM schedules WHERE name = ?1",
    )?;
    let mut rows = stmt.query_map(params![name], |row| {
        Ok(Schedule {
            id: row.get(0)?,
            name: row.get(1)?,
            cron: row.get(2)?,
            command: row.get(3)?,
            enabled: row.get::<_, i32>(4)? != 0,
            created_at: row.get(5)?,
        })
    })?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

pub fn list_enabled(conn: &Connection) -> Result<Vec<Schedule>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, cron, command, enabled, created_at FROM schedules WHERE enabled = 1 ORDER BY id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(Schedule {
            id: row.get(0)?,
            name: row.get(1)?,
            cron: row.get(2)?,
            command: row.get(3)?,
            enabled: row.get::<_, i32>(4)? != 0,
            created_at: row.get(5)?,
        })
    })?;
    rows.collect()
}

/// Compute the most recent cron fire time at or before `now`.
/// Returns None if no fire time can be computed.
pub fn most_recent_fire_time(cron_expr: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let schedule = CronSchedule::from_str(cron_expr).ok()?;
    // Iterate forward from 2 days ago, collecting fire times until we pass `now`.
    // The last one at or before `now` is the most recent fire time.
    let mut prev = None;
    for t in schedule.after(&(now - chrono::Duration::days(2))) {
        if t > now {
            break;
        }
        prev = Some(t);
    }
    prev
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use tempfile::TempDir;

    fn setup() -> Connection {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let conn = db::open(&path).unwrap();
        db::migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn test_validate_cron_valid() {
        assert!(validate_cron("0 0 9 * * * *").is_ok());
    }

    #[test]
    fn test_validate_cron_invalid() {
        let result = validate_cron("not a cron");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid cron"));
    }

    #[test]
    fn test_add_and_list() {
        let conn = setup();
        let id = add(&conn, "test-job", "0 0 9 * * * *", "echo hello").unwrap();
        assert!(id > 0);

        let schedules = list(&conn).unwrap();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].name, "test-job");
        assert_eq!(schedules[0].cron, "0 0 9 * * * *");
        assert_eq!(schedules[0].command, "echo hello");
        assert!(schedules[0].enabled);
    }

    #[test]
    fn test_add_duplicate_name() {
        let conn = setup();
        add(&conn, "test-job", "0 0 9 * * * *", "echo hello").unwrap();
        let result = add(&conn, "test-job", "0 0 10 * * * *", "echo world");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove() {
        let conn = setup();
        add(&conn, "test-job", "0 0 9 * * * *", "echo hello").unwrap();

        assert!(remove(&conn, "test-job").unwrap());
        assert!(!remove(&conn, "test-job").unwrap()); // already removed

        let schedules = list(&conn).unwrap();
        assert_eq!(schedules.len(), 0);
    }

    #[test]
    fn test_enable_disable() {
        let conn = setup();
        add(&conn, "test-job", "0 0 9 * * * *", "echo hello").unwrap();

        assert!(set_enabled(&conn, "test-job", false).unwrap());
        let s = get_by_name(&conn, "test-job").unwrap().unwrap();
        assert!(!s.enabled);

        assert!(set_enabled(&conn, "test-job", true).unwrap());
        let s = get_by_name(&conn, "test-job").unwrap().unwrap();
        assert!(s.enabled);

        // Non-existent schedule
        assert!(!set_enabled(&conn, "no-such", false).unwrap());
    }

    #[test]
    fn test_get_by_name_not_found() {
        let conn = setup();
        assert!(get_by_name(&conn, "nope").unwrap().is_none());
    }

    #[test]
    fn test_list_enabled() {
        let conn = setup();
        add(&conn, "job1", "0 0 9 * * * *", "echo 1").unwrap();
        add(&conn, "job2", "0 0 10 * * * *", "echo 2").unwrap();
        set_enabled(&conn, "job2", false).unwrap();

        let enabled = list_enabled(&conn).unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "job1");
    }

    #[test]
    fn test_most_recent_fire_time() {
        // "every hour at minute 0" -> 0 0 * * * * *
        let now = Utc::now();
        let result = most_recent_fire_time("0 0 * * * * *", now);
        assert!(result.is_some());
        let fire = result.unwrap();
        assert!(fire <= now);
        // Should be within the last hour
        assert!(now - fire < chrono::Duration::hours(1));
    }

    #[test]
    fn test_most_recent_fire_time_invalid_cron() {
        let result = most_recent_fire_time("bad cron", Utc::now());
        assert!(result.is_none());
    }
}
