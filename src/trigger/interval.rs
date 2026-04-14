use chrono::{DateTime, Utc};

use super::Trigger;

#[derive(Debug, Clone)]
pub struct IntervalTrigger {
    interval: chrono::Duration,
    start_time: DateTime<Utc>,
}

impl IntervalTrigger {
    pub fn every(duration: std::time::Duration) -> Self {
        Self {
            interval: chrono::Duration::from_std(duration)
                .expect("duration too large for chrono::Duration"),
            start_time: Utc::now(),
        }
    }

    pub fn starting_at(mut self, start_time: DateTime<Utc>) -> Self {
        self.start_time = start_time;
        self
    }
}

impl Trigger for IntervalTrigger {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        if *after < self.start_time {
            return Some(self.start_time);
        }

        let elapsed = *after - self.start_time;
        let interval_millis = self.interval.num_milliseconds();
        if interval_millis <= 0 {
            return None;
        }
        let elapsed_millis = elapsed.num_milliseconds();
        let periods = elapsed_millis / interval_millis + 1;
        Some(self.start_time + self.interval * periods as i32)
    }

    fn description(&self) -> String {
        format!("IntervalTrigger(every {:?})", self.interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::time::Duration;

    #[test]
    fn test_basic_interval() {
        let start = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let trigger = IntervalTrigger::every(Duration::from_secs(60)).starting_at(start);
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 30).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 0).unwrap());
    }

    #[test]
    fn test_start_time_in_future() {
        let start = Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap();
        let trigger = IntervalTrigger::every(Duration::from_secs(3600)).starting_at(start);
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, start);
    }

    #[test]
    fn test_multiple_intervals() {
        let start = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let trigger = IntervalTrigger::every(Duration::from_secs(300)).starting_at(start);
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 12, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        // 12 minutes = 720 seconds, 720/300 = 2.4, so next is at 3*300 = 900s = 15 min
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 1, 1, 0, 15, 0).unwrap());
    }
}
