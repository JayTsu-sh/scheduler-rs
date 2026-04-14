use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;

use super::Trigger;
use crate::error::SchedulerError;

#[derive(Debug, Clone)]
pub struct CronTrigger {
    schedule: Schedule,
    expression: String,
}

impl CronTrigger {
    pub fn new(expr: &str) -> Result<Self, SchedulerError> {
        let schedule = Schedule::from_str(expr)
            .map_err(|e| SchedulerError::InvalidCronExpression(format!("{}: {}", expr, e)))?;
        Ok(Self {
            schedule,
            expression: expr.to_string(),
        })
    }
}

impl Trigger for CronTrigger {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.schedule.after(after).next()
    }

    fn description(&self) -> String {
        format!("CronTrigger({})", self.expression)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_basic_cron() {
        let trigger = CronTrigger::new("0 0 12 * * * *").unwrap();
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next.hour(), 12);
    }

    #[test]
    fn test_every_second() {
        let trigger = CronTrigger::new("* * * * * * *").unwrap();
        let after = Utc.with_ymd_and_hms(2025, 6, 15, 10, 30, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert!(next > after);
        assert_eq!((next - after).num_seconds(), 1);
    }

    #[test]
    fn test_next_fire_time_ordering() {
        let trigger = CronTrigger::new("0 0 * * * * *").unwrap();
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let first = trigger.next_fire_time(&after).unwrap();
        let second = trigger.next_fire_time(&first).unwrap();
        assert!(second > first);
    }

    #[test]
    fn test_invalid_cron() {
        let result = CronTrigger::new("invalid");
        assert!(result.is_err());
    }

    use chrono::Timelike;
}
