use chrono::{DateTime, Utc};

use super::Trigger;

/// Returns the earliest `next_fire_time` from any child trigger.
#[derive(Debug)]
pub struct OrTrigger {
    triggers: Vec<Box<dyn Trigger>>,
}

impl OrTrigger {
    pub fn new(triggers: Vec<Box<dyn Trigger>>) -> Self {
        Self { triggers }
    }
}

impl Trigger for OrTrigger {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.triggers
            .iter()
            .filter_map(|t| t.next_fire_time(after))
            .min()
    }

    fn description(&self) -> String {
        let descs: Vec<String> = self.triggers.iter().map(|t| t.description()).collect();
        format!("OrTrigger({})", descs.join(" | "))
    }
}

/// Returns the latest `next_fire_time` from all child triggers.
/// All children must return `Some`; if any returns `None`, the result is `None`.
#[derive(Debug)]
pub struct AndTrigger {
    triggers: Vec<Box<dyn Trigger>>,
}

impl AndTrigger {
    pub fn new(triggers: Vec<Box<dyn Trigger>>) -> Self {
        Self { triggers }
    }
}

impl Trigger for AndTrigger {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.triggers.is_empty() {
            return None;
        }

        let mut latest: Option<DateTime<Utc>> = None;
        for trigger in &self.triggers {
            let next = trigger.next_fire_time(after)?;
            latest = Some(match latest {
                Some(current) => current.max(next),
                None => next,
            });
        }
        latest
    }

    fn description(&self) -> String {
        let descs: Vec<String> = self.triggers.iter().map(|t| t.description()).collect();
        format!("AndTrigger({})", descs.join(" & "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trigger::once::OnceTrigger;
    use chrono::TimeZone;

    #[test]
    fn test_or_picks_earliest() {
        let t1 = OnceTrigger::at(Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap());
        let t2 = OnceTrigger::at(Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap());
        let or_trigger = OrTrigger::new(vec![Box::new(t1), Box::new(t2)]);

        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let next = or_trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_and_picks_latest() {
        let t1 = OnceTrigger::at(Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap());
        let t2 = OnceTrigger::at(Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap());
        let and_trigger = AndTrigger::new(vec![Box::new(t1), Box::new(t2)]);

        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let next = and_trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_and_returns_none_if_any_none() {
        let t1 = OnceTrigger::at(Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()); // past
        let t2 = OnceTrigger::at(Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap());
        let and_trigger = AndTrigger::new(vec![Box::new(t1), Box::new(t2)]);

        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(and_trigger.next_fire_time(&after), None);
    }

    #[test]
    fn test_or_empty_vec() {
        let or_trigger = OrTrigger::new(vec![]);
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(or_trigger.next_fire_time(&after), None);
    }

    #[test]
    fn test_and_empty_vec() {
        let and_trigger = AndTrigger::new(vec![]);
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(and_trigger.next_fire_time(&after), None);
    }
}
