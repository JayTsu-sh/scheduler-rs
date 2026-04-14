use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};

use super::Trigger;

/// A trigger that fires exactly once, immediately.
///
/// On the first call to `next_fire_time`, returns `Some(now)` so the scheduler
/// picks it up as due on the very next poll. Subsequent calls return `None`,
/// making the schedule complete after one execution.
///
/// ```ignore
/// // Fire a task right now, one time only
/// scheduler.add(my_task, ImmediateTrigger::new()).await?;
/// ```
#[derive(Debug)]
pub struct ImmediateTrigger {
    fired: AtomicBool,
}

impl ImmediateTrigger {
    pub fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}

impl Default for ImmediateTrigger {
    fn default() -> Self {
        Self::new()
    }
}

impl Trigger for ImmediateTrigger {
    fn next_fire_time(&self, _after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        // First call: return current time (immediately due).
        // All subsequent calls: return None (one-shot, schedule becomes Completed).
        if self
            .fired
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            Some(Utc::now())
        } else {
            None
        }
    }

    fn description(&self) -> String {
        "ImmediateTrigger".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_immediate_fires_on_first_call() {
        let trigger = ImmediateTrigger::new();
        let now = Utc::now();
        let next = trigger.next_fire_time(&now);
        assert!(next.is_some());
        let fire = next.unwrap();
        let diff = (fire - now).num_milliseconds().abs();
        assert!(diff < 100, "Expected fire time near now, diff={diff}ms");
    }

    #[test]
    fn test_immediate_none_on_second_call() {
        let trigger = ImmediateTrigger::new();
        let now = Utc::now();

        // First call returns Some
        assert!(trigger.next_fire_time(&now).is_some());
        // Second call returns None (one-shot)
        assert!(trigger.next_fire_time(&now).is_none());
        // Third call also None
        assert!(trigger.next_fire_time(&now).is_none());
    }

    #[test]
    fn test_immediate_description() {
        let trigger = ImmediateTrigger::new();
        assert_eq!(trigger.description(), "ImmediateTrigger");
    }
}
