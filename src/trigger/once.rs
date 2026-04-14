use chrono::{DateTime, Utc};

use super::Trigger;

#[derive(Debug, Clone)]
pub struct OnceTrigger {
    time: DateTime<Utc>,
}

impl OnceTrigger {
    pub fn at(time: DateTime<Utc>) -> Self {
        Self { time }
    }

    pub fn after(duration: std::time::Duration) -> Self {
        let time = Utc::now()
            + chrono::Duration::from_std(duration)
                .expect("duration too large for chrono::Duration");
        Self { time }
    }
}

impl Trigger for OnceTrigger {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.time > *after {
            Some(self.time)
        } else {
            None
        }
    }

    fn description(&self) -> String {
        format!("OnceTrigger(at {})", self.time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_future_time() {
        let time = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        let trigger = OnceTrigger::at(time);
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(trigger.next_fire_time(&after), Some(time));
    }

    #[test]
    fn test_past_time() {
        let time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let trigger = OnceTrigger::at(time);
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(trigger.next_fire_time(&after), None);
    }

    #[test]
    fn test_after_returns_correct() {
        let trigger = OnceTrigger::after(std::time::Duration::from_secs(3600));
        let now = Utc::now();
        let next = trigger.next_fire_time(&now);
        // The trigger was created with `after(1 hour)`, so next_fire_time(now)
        // should return a time roughly 1 hour from creation, which is > now.
        assert!(next.is_some());
        let fire_time = next.unwrap();
        assert!(fire_time > now);
    }
}
