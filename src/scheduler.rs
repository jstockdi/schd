use chrono::Utc;
use rusqlite::Connection;

use crate::run;
use crate::schedule;

/// Run one tick of the scheduler: for each enabled schedule, compute the most
/// recent fire time and insert a pending run if one doesn't exist yet.
/// Returns the number of new runs scheduled.
pub fn tick(conn: &Connection) -> rusqlite::Result<usize> {
    let now = Utc::now();
    let schedules = schedule::list_enabled(conn)?;
    let mut count = 0;

    for sched in &schedules {
        if let Some(fire_time) = schedule::most_recent_fire_time(&sched.cron, now) {
            if let Some(run_id) = run::insert_pending(conn, sched.id, fire_time)? {
                eprintln!(
                    "[scheduler] scheduled run {} for '{}' (schedule {}) at {}",
                    run_id,
                    sched.name,
                    sched.id,
                    fire_time.format("%Y-%m-%dT%H:%M:%S")
                );
                count += 1;
            }
        }
    }

    Ok(count)
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
    fn test_tick_no_schedules() {
        let conn = setup();
        let count = tick(&conn).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_tick_creates_pending_run() {
        let conn = setup();
        // Every minute
        schedule::add(&conn, "every-min", "0 * * * * * *", "echo tick").unwrap();

        let count = tick(&conn).unwrap();
        assert_eq!(count, 1);

        // Second tick should not create duplicate
        let count2 = tick(&conn).unwrap();
        assert_eq!(count2, 0);
    }

    #[test]
    fn test_tick_skips_disabled() {
        let conn = setup();
        schedule::add(&conn, "disabled-job", "0 * * * * * *", "echo nope").unwrap();
        schedule::set_enabled(&conn, "disabled-job", false).unwrap();

        let count = tick(&conn).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_tick_multiple_schedules() {
        let conn = setup();
        schedule::add(&conn, "job1", "0 * * * * * *", "echo 1").unwrap();
        schedule::add(&conn, "job2", "0 * * * * * *", "echo 2").unwrap();

        let count = tick(&conn).unwrap();
        assert_eq!(count, 2);
    }
}
