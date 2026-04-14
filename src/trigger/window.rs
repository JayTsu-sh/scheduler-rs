use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc};

use super::Trigger;

/// Pattern for which days the window applies.
#[derive(Debug, Clone)]
pub enum DayPattern {
    /// Every day.
    Daily,
    /// Specific days of the week (Monday = 1 through Sunday = 7, ISO).
    Weekdays(Vec<u32>),
}

/// How the window trigger behaves when the window opens.
#[derive(Debug, Clone)]
pub enum WindowMode {
    /// Fire once when the window opens. Task runs until window ends (via CancellationToken).
    Once,
    /// Fire repeatedly at the given interval during the window.
    Repeat(std::time::Duration),
}

/// A trigger that fires within a time window on specified days.
///
/// Supports:
/// - Same-day windows (e.g., 09:00 - 17:00)
/// - Cross-midnight windows (e.g., 20:00 - 08:00, ending next day)
/// - Day-of-week filtering
/// - Once mode (fire at window start, cancel at window end)
/// - Repeat mode (fire at interval within window)
#[derive(Debug, Clone)]
pub struct WindowTrigger {
    pub(crate) day_pattern: DayPattern,
    pub(crate) start_time: NaiveTime,
    pub(crate) end_time: NaiveTime,
    pub(crate) mode: WindowMode,
}

impl WindowTrigger {
    /// Create a daily window trigger with default midnight-to-midnight window.
    pub fn daily() -> Self {
        Self {
            day_pattern: DayPattern::Daily,
            start_time: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
            mode: WindowMode::Once,
        }
    }

    /// Set the window start time.
    pub fn start_at(mut self, time: NaiveTime) -> Self {
        self.start_time = time;
        self
    }

    /// Set the window end time.
    pub fn end_at(mut self, time: NaiveTime) -> Self {
        self.end_time = time;
        self
    }

    /// Set the trigger to repeat at the given interval within the window.
    pub fn repeat(mut self, interval: std::time::Duration) -> Self {
        self.mode = WindowMode::Repeat(interval);
        self
    }

    /// Set which days the window applies to.
    pub fn on_days(mut self, pattern: DayPattern) -> Self {
        self.day_pattern = pattern;
        self
    }

    /// Whether start_time > end_time, meaning the window spans midnight.
    fn is_cross_midnight(&self) -> bool {
        self.start_time >= self.end_time
    }

    /// Check if a given ISO weekday (Mon=1..Sun=7) matches the day pattern.
    fn is_day_match(&self, weekday_iso: u32) -> bool {
        match &self.day_pattern {
            DayPattern::Daily => true,
            DayPattern::Weekdays(days) => days.contains(&weekday_iso),
        }
    }

    /// Compute (window_start, window_end) datetimes for a window that STARTS on the given date.
    fn window_for_start_date(&self, date: NaiveDate) -> (DateTime<Utc>, DateTime<Utc>) {
        let start_dt = Utc.from_utc_datetime(&date.and_time(self.start_time));
        let end_dt = if self.is_cross_midnight() {
            // Window ends on the next day
            let next_day = date.succ_opt().unwrap();
            Utc.from_utc_datetime(&next_day.and_time(self.end_time))
        } else {
            Utc.from_utc_datetime(&date.and_time(self.end_time))
        };
        (start_dt, end_dt)
    }

    /// Find the active window containing `at`, if any.
    /// Returns (window_start, window_end) if `at` is inside a window.
    fn active_window_at(&self, at: &DateTime<Utc>) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let at_date = at.date_naive();

        // Check window starting today
        let today_weekday = at_date.weekday().number_from_monday();
        if self.is_day_match(today_weekday) {
            let (ws, we) = self.window_for_start_date(at_date);
            if *at >= ws && *at < we {
                return Some((ws, we));
            }
        }

        // For cross-midnight windows, also check if yesterday's window extends into today
        if self.is_cross_midnight()
            && let Some(yesterday) = at_date.pred_opt()
        {
            let yesterday_weekday = yesterday.weekday().number_from_monday();
            if self.is_day_match(yesterday_weekday) {
                let (ws, we) = self.window_for_start_date(yesterday);
                if *at >= ws && *at < we {
                    return Some((ws, we));
                }
            }
        }

        None
    }

    /// Find the next window start strictly after `after`.
    fn next_window_start(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        let start_date = after.date_naive();

        // Search up to 366 days to find a matching day
        for day_offset in 0..366i64 {
            let candidate_date = start_date + chrono::Duration::days(day_offset);
            let weekday_iso = candidate_date.weekday().number_from_monday();

            if !self.is_day_match(weekday_iso) {
                continue;
            }

            let (window_start, _) = self.window_for_start_date(candidate_date);
            if window_start > *after {
                return Some(window_start);
            }
        }

        None
    }

    /// For Repeat mode: find the next repeat fire time within the current window.
    fn next_repeat_in_window(
        &self,
        after: &DateTime<Utc>,
        interval: &std::time::Duration,
    ) -> Option<DateTime<Utc>> {
        let (window_start, window_end) = self.active_window_at(after)?;

        let chrono_interval = chrono::Duration::from_std(*interval).ok()?;
        if chrono_interval.is_zero() {
            return None;
        }

        let elapsed = *after - window_start;
        let interval_ms = chrono_interval.num_milliseconds();
        let elapsed_ms = elapsed.num_milliseconds();

        // Next repeat number: strictly after `after`
        let n = elapsed_ms / interval_ms + 1;
        let next = window_start + chrono_interval * n as i32;

        if next < window_end {
            Some(next)
        } else {
            None // Past window end, caller will find next window start
        }
    }
}

impl Trigger for WindowTrigger {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        // For Repeat mode, check if we're inside a window and can fire again
        if let WindowMode::Repeat(ref interval) = self.mode
            && let Some(next_repeat) = self.next_repeat_in_window(after, interval)
        {
            return Some(next_repeat);
        }

        // Otherwise, find the next window start
        self.next_window_start(after)
    }

    fn window_end_time(&self, fire_time: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Find which window this fire_time belongs to
        let fire_date = fire_time.date_naive();

        // Check window starting on fire_time's date
        let weekday = fire_date.weekday().number_from_monday();
        if self.is_day_match(weekday) {
            let (ws, we) = self.window_for_start_date(fire_date);
            if *fire_time >= ws && *fire_time < we {
                return Some(we);
            }
        }

        // For cross-midnight, check if yesterday's window contains this fire_time
        if self.is_cross_midnight()
            && let Some(yesterday) = fire_date.pred_opt()
        {
            let y_weekday = yesterday.weekday().number_from_monday();
            if self.is_day_match(y_weekday) {
                let (ws, we) = self.window_for_start_date(yesterday);
                if *fire_time >= ws && *fire_time < we {
                    return Some(we);
                }
            }
        }

        None
    }

    fn description(&self) -> String {
        format!(
            "WindowTrigger({:?}, {} - {}, {:?})",
            self.day_pattern, self.start_time, self.end_time, self.mode
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_same_day_window_next_start() {
        // Window: 09:00 - 17:00 daily
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap());

        // Before window: should return today's 09:00
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 7, 0, 0).unwrap(); // Tuesday 07:00
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 10, 9, 0, 0).unwrap());
    }

    #[test]
    fn test_same_day_window_after_end() {
        // Window: 09:00 - 17:00 daily
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap());

        // After window: should return tomorrow's 09:00
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 18, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 11, 9, 0, 0).unwrap());
    }

    #[test]
    fn test_same_day_window_once_inside() {
        // Window: 09:00 - 17:00 daily, Once mode
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap());

        // Inside window in Once mode: next should be tomorrow's window start
        // (because today's window start is already past)
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 12, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 11, 9, 0, 0).unwrap());
    }

    #[test]
    fn test_cross_midnight_window() {
        // Window: 20:00 - 08:00 (crosses midnight)
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(20, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(8, 0, 0).unwrap());

        // Before window: 15:00 → should fire at 20:00 today
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 15, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 10, 20, 0, 0).unwrap());
    }

    #[test]
    fn test_cross_midnight_window_inside_after_midnight() {
        // Window: 20:00 - 08:00 (crosses midnight), Once mode
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(20, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(8, 0, 0).unwrap());

        // At 03:00 on June 11 — inside yesterday's window (June 10 20:00 - June 11 08:00)
        // Once mode: today's window start (20:00 Jun 11) is the next fire
        // But we're also inside the Jun 10 window. In Once mode, next_fire_time
        // returns the next window_start > after, which is Jun 11 20:00
        let after = Utc.with_ymd_and_hms(2025, 6, 11, 3, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 11, 20, 0, 0).unwrap());
    }

    #[test]
    fn test_repeat_mode_inside_window() {
        // Window: 09:00 - 17:00, repeat every 2 hours
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap())
            .repeat(std::time::Duration::from_secs(7200)); // 2 hours

        // At 10:30 → next should be 11:00 (09:00 + 2h = 11:00)
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 10, 30, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 10, 11, 0, 0).unwrap());
    }

    #[test]
    fn test_repeat_mode_at_exact_boundary() {
        // Window: 09:00 - 17:00, repeat every 2 hours
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap())
            .repeat(std::time::Duration::from_secs(7200));

        // At exactly 09:00 → next should be 11:00
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 9, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 10, 11, 0, 0).unwrap());
    }

    #[test]
    fn test_repeat_mode_near_window_end() {
        // Window: 09:00 - 17:00, repeat every 2 hours
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap())
            .repeat(std::time::Duration::from_secs(7200));

        // At 16:00 → next would be 17:00 but that's the window end
        // So should skip to next day's 09:00
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 16, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 11, 9, 0, 0).unwrap());
    }

    #[test]
    fn test_cross_midnight_repeat() {
        // Window: 22:00 - 06:00, repeat every 2 hours
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(22, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(6, 0, 0).unwrap())
            .repeat(std::time::Duration::from_secs(7200));

        // At 23:30 (inside window) → next repeat = 00:00 (22:00 + 2h)
        let after = Utc.with_ymd_and_hms(2025, 6, 10, 23, 30, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 11, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_cross_midnight_repeat_after_midnight() {
        // Window: 22:00 - 06:00, repeat every 2 hours
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(22, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(6, 0, 0).unwrap())
            .repeat(std::time::Duration::from_secs(7200));

        // At 01:00 on June 11 (inside June 10's window which ends at 06:00 June 11)
        // elapsed from 22:00 = 3h, next = 22:00 + 4h = 02:00
        let after = Utc.with_ymd_and_hms(2025, 6, 11, 1, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 11, 2, 0, 0).unwrap());
    }

    #[test]
    fn test_weekdays_only() {
        // Window: 09:00 - 17:00 on weekdays (Mon-Fri)
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap())
            .on_days(DayPattern::Weekdays(vec![1, 2, 3, 4, 5]));

        // Friday June 13 2025, 18:00 → next should be Monday June 16 09:00
        let after = Utc.with_ymd_and_hms(2025, 6, 13, 18, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        // June 14 = Saturday (6), June 15 = Sunday (7), June 16 = Monday (1)
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 6, 16, 9, 0, 0).unwrap());
    }

    #[test]
    fn test_window_end_time_same_day() {
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap());

        // Fire time is window start
        let fire_time = Utc.with_ymd_and_hms(2025, 6, 10, 9, 0, 0).unwrap();
        let end = trigger.window_end_time(&fire_time).unwrap();
        assert_eq!(end, Utc.with_ymd_and_hms(2025, 6, 10, 17, 0, 0).unwrap());
    }

    #[test]
    fn test_window_end_time_cross_midnight() {
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(20, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(8, 0, 0).unwrap());

        // Fire at 20:00 → window ends at 08:00 next day
        let fire_time = Utc.with_ymd_and_hms(2025, 6, 10, 20, 0, 0).unwrap();
        let end = trigger.window_end_time(&fire_time).unwrap();
        assert_eq!(end, Utc.with_ymd_and_hms(2025, 6, 11, 8, 0, 0).unwrap());
    }

    #[test]
    fn test_window_end_time_inside_after_midnight() {
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(20, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(8, 0, 0).unwrap());

        // At 03:00 on June 11 — inside yesterday's cross-midnight window
        let fire_time = Utc.with_ymd_and_hms(2025, 6, 11, 3, 0, 0).unwrap();
        let end = trigger.window_end_time(&fire_time).unwrap();
        assert_eq!(end, Utc.with_ymd_and_hms(2025, 6, 11, 8, 0, 0).unwrap());
    }

    #[test]
    fn test_window_end_time_outside_window() {
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(9, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(17, 0, 0).unwrap());

        // 18:00 — outside window
        let fire_time = Utc.with_ymd_and_hms(2025, 6, 10, 18, 0, 0).unwrap();
        assert_eq!(trigger.window_end_time(&fire_time), None);
    }

    #[test]
    fn test_builder_api() {
        let trigger = WindowTrigger::daily()
            .start_at(NaiveTime::from_hms_opt(8, 0, 0).unwrap())
            .end_at(NaiveTime::from_hms_opt(18, 0, 0).unwrap())
            .repeat(std::time::Duration::from_secs(300))
            .on_days(DayPattern::Weekdays(vec![1, 2, 3, 4, 5]));

        assert!(matches!(trigger.day_pattern, DayPattern::Weekdays(_)));
        assert!(matches!(trigger.mode, WindowMode::Repeat(_)));
    }
}
