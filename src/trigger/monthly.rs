use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc};

use super::Trigger;

/// Represents a day within a month.
#[derive(Debug, Clone)]
pub enum DayOfMonth {
    /// A specific day (1-31). Skipped if the month doesn't have that many days.
    Day(u32),
    /// Offset from the end of month. -1 = last day, -2 = second to last, etc.
    Last(i32),
}

#[derive(Debug, Clone)]
pub struct MonthlyTrigger {
    days: Vec<DayOfMonth>,
    time: NaiveTime,
    interval_months: u32,
}

impl MonthlyTrigger {
    pub fn new() -> Self {
        Self {
            days: Vec::new(),
            time: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            interval_months: 1,
        }
    }

    pub fn on_day(mut self, day: DayOfMonth) -> Self {
        self.days.push(day);
        self
    }

    pub fn on_last_day(self) -> Self {
        self.on_day(DayOfMonth::Last(-1))
    }

    pub fn at(mut self, time: NaiveTime) -> Self {
        self.time = time;
        self
    }

    pub fn every_n_months(mut self, n: u32) -> Self {
        self.interval_months = n;
        self
    }
}

impl Default for MonthlyTrigger {
    fn default() -> Self {
        Self::new()
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    // Get the first day of the next month, then subtract one day.
    if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }
    .unwrap()
    .pred_opt()
    .unwrap()
    .day()
}

fn resolve_day(day: &DayOfMonth, year: i32, month: u32) -> Option<u32> {
    let dim = days_in_month(year, month);
    match day {
        DayOfMonth::Day(d) => {
            if *d >= 1 && *d <= dim {
                Some(*d)
            } else {
                None // skip months that don't have this day
            }
        }
        DayOfMonth::Last(n) => {
            // n is negative: -1 = last day, -2 = second to last
            let resolved = dim as i32 + n + 1;
            if resolved >= 1 {
                Some(resolved as u32)
            } else {
                None
            }
        }
    }
}

fn advance_month(year: i32, month: u32, months_to_add: u32) -> (i32, u32) {
    let total_months = (year as i64) * 12 + (month as i64 - 1) + months_to_add as i64;
    let new_year = (total_months / 12) as i32;
    let new_month = (total_months % 12 + 1) as u32;
    (new_year, new_month)
}

impl Trigger for MonthlyTrigger {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.days.is_empty() {
            return None;
        }

        let mut year = after.year();
        let mut month = after.month();

        // Search up to 120 months (10 years) to avoid infinite loop
        for _ in 0..120 {
            let mut candidates: Vec<DateTime<Utc>> = Vec::new();

            for day_spec in &self.days {
                if let Some(day) = resolve_day(day_spec, year, month)
                    && let Some(date) = NaiveDate::from_ymd_opt(year, month, day)
                {
                    let datetime = date.and_time(self.time);
                    let utc_dt = Utc.from_utc_datetime(&datetime);
                    if utc_dt > *after {
                        candidates.push(utc_dt);
                    }
                }
            }

            if let Some(earliest) = candidates.into_iter().min() {
                return Some(earliest);
            }

            let (ny, nm) = advance_month(year, month, self.interval_months);
            year = ny;
            month = nm;
        }

        None
    }

    fn description(&self) -> String {
        format!(
            "MonthlyTrigger({} day specs, at {}, every {} month(s))",
            self.days.len(),
            self.time,
            self.interval_months
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_last_day_of_month() {
        let trigger = MonthlyTrigger::new()
            .on_last_day()
            .at(NaiveTime::from_hms_opt(12, 0, 0).unwrap());

        let after = Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        // Last day of January = 31st
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 1, 31, 12, 0, 0).unwrap());
    }

    #[test]
    fn test_last_minus_3() {
        let trigger = MonthlyTrigger::new()
            .on_day(DayOfMonth::Last(-3))
            .at(NaiveTime::from_hms_opt(9, 0, 0).unwrap());

        // January has 31 days, Last(-3) => 31 + (-3) + 1 = 29
        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 1, 29, 9, 0, 0).unwrap());
    }

    #[test]
    fn test_february_handling() {
        let trigger = MonthlyTrigger::new()
            .on_last_day()
            .at(NaiveTime::from_hms_opt(0, 0, 0).unwrap());

        // After Jan 31 at midnight, next should be Feb 28 (2025 is not a leap year)
        let after = Utc.with_ymd_and_hms(2025, 1, 31, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 2, 28, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_multi_day() {
        let trigger = MonthlyTrigger::new()
            .on_day(DayOfMonth::Day(10))
            .on_day(DayOfMonth::Day(20))
            .at(NaiveTime::from_hms_opt(8, 0, 0).unwrap());

        let after = Utc.with_ymd_and_hms(2025, 3, 12, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        // Should pick March 20th (10th is already past)
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 3, 20, 8, 0, 0).unwrap());
    }

    #[test]
    fn test_day_31_skips_short_months() {
        let trigger = MonthlyTrigger::new()
            .on_day(DayOfMonth::Day(31))
            .at(NaiveTime::from_hms_opt(0, 0, 0).unwrap());

        // After Feb 1, day 31 should skip Feb (28 days) and fire on March 31
        let after = Utc.with_ymd_and_hms(2025, 2, 1, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 3, 31, 0, 0, 0).unwrap());
    }
}
