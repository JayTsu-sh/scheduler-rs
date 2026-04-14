use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;

use super::Trigger;

/// A wrapper trigger that converts time calculations to a specific timezone.
///
/// This allows triggers like `CronTrigger` or `MonthlyTrigger` to fire based
/// on local wall-clock time rather than UTC.
///
/// # Example
///
/// ```ignore
/// use scheduler_rs::trigger::{CronTrigger, TriggerExt};
///
/// let trigger = CronTrigger::new("0 0 9 * * MON-FRI")?
///     .in_tz(chrono_tz::Asia::Shanghai);
/// // Fires at 09:00 Shanghai time, not 09:00 UTC
/// ```
#[derive(Debug)]
pub struct TzTrigger<T: Trigger> {
    inner: T,
    tz: Tz,
}

impl<T: Trigger> TzTrigger<T> {
    pub fn new(inner: T, tz: Tz) -> Self {
        Self { inner, tz }
    }
}

impl<T: Trigger> Trigger for TzTrigger<T> {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Convert `after` from UTC to local timezone
        let local = after.with_timezone(&self.tz);
        let naive_local = local.naive_local();

        // Treat local time as pseudo-UTC for the inner trigger
        let pseudo_utc = DateTime::<Utc>::from_naive_utc_and_offset(naive_local, Utc);

        // Inner trigger computes next fire time in "local" coordinates
        let next_pseudo = self.inner.next_fire_time(&pseudo_utc)?;

        // Convert result back: interpret as local time, then to real UTC
        let next_naive = next_pseudo.naive_utc();
        let next_local = self.tz.from_local_datetime(&next_naive).earliest()?;
        Some(next_local.with_timezone(&Utc))
    }

    fn window_end_time(&self, fire_time: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Same conversion for window_end_time
        let local = fire_time.with_timezone(&self.tz);
        let naive_local = local.naive_local();
        let pseudo_utc = DateTime::<Utc>::from_naive_utc_and_offset(naive_local, Utc);

        let end_pseudo = self.inner.window_end_time(&pseudo_utc)?;

        let end_naive = end_pseudo.naive_utc();
        let end_local = self.tz.from_local_datetime(&end_naive).earliest()?;
        Some(end_local.with_timezone(&Utc))
    }

    fn description(&self) -> String {
        format!("{} (tz: {})", self.inner.description(), self.tz)
    }
}

/// Extension trait that adds `.in_tz()` to any trigger.
pub trait TriggerExt: Trigger + Sized {
    fn in_tz(self, tz: Tz) -> TzTrigger<Self> {
        TzTrigger::new(self, tz)
    }
}

// Blanket impl for all triggers
impl<T: Trigger + Sized> TriggerExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trigger::CronTrigger;
    use chrono::TimeZone;

    #[test]
    fn test_tz_trigger_converts_correctly() {
        // Cron: fire at 09:00 every day (in UTC terms for the inner trigger)
        // With Shanghai timezone (UTC+8), 09:00 Shanghai = 01:00 UTC
        let inner = CronTrigger::new("0 0 9 * * *").unwrap();
        let trigger = inner.in_tz(chrono_tz::Asia::Shanghai);

        // After 2025-06-10 00:00 UTC (= 08:00 Shanghai, before 09:00)
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();

        // Should fire at 01:00 UTC (= 09:00 Shanghai)
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 10, 1, 0, 0).unwrap());
    }

    #[test]
    fn test_tz_trigger_after_local_fire_time() {
        // 09:00 Shanghai time
        let inner = CronTrigger::new("0 0 9 * * *").unwrap();
        let trigger = inner.in_tz(chrono_tz::Asia::Shanghai);

        // After 2025-06-10 02:00 UTC (= 10:00 Shanghai, past today's 09:00)
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 2, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();

        // Should fire at 01:00 UTC next day (= 09:00 Shanghai June 11)
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 11, 1, 0, 0).unwrap());
    }

    #[test]
    fn test_tz_trigger_negative_offset() {
        // 09:00 New York time (UTC-4 in summer / EDT)
        let inner = CronTrigger::new("0 0 9 * * *").unwrap();
        let trigger = inner.in_tz(chrono_tz::America::New_York);

        // After 2025-06-10 12:00 UTC (= 08:00 EDT, before 09:00)
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 12, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();

        // 09:00 EDT = 13:00 UTC
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 10, 13, 0, 0).unwrap());
    }

    #[test]
    fn test_tz_trigger_description() {
        let inner = CronTrigger::new("0 0 9 * * *").unwrap();
        let trigger = inner.in_tz(chrono_tz::Asia::Shanghai);
        let desc = trigger.description();
        assert!(desc.contains("Asia/Shanghai"));
    }
}
