use chrono::{DateTime, Utc};
use std::fmt::Debug;

pub trait Trigger: Send + Sync + Debug {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>>;
    fn description(&self) -> String;

    /// For window-based triggers, returns the end time of the window
    /// that contains the given fire_time. Returns None for non-window triggers.
    fn window_end_time(&self, fire_time: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        let _ = fire_time;
        None
    }
}

pub mod cron;
pub mod interval;
pub mod once;
pub mod immediate;
pub mod calendar_interval;
pub mod monthly;
pub mod combining;
pub mod window;
#[cfg(feature = "timezone")]
pub mod timezone;

pub use self::cron::CronTrigger;
pub use self::interval::IntervalTrigger;
pub use self::once::OnceTrigger;
pub use self::immediate::ImmediateTrigger;
pub use self::calendar_interval::{CalendarIntervalTrigger, CalendarIntervalBuilder};
pub use self::monthly::{MonthlyTrigger, DayOfMonth};
pub use self::combining::{OrTrigger, AndTrigger};
pub use self::window::{WindowTrigger, DayPattern, WindowMode};
#[cfg(feature = "timezone")]
pub use self::timezone::{TzTrigger, TriggerExt};
