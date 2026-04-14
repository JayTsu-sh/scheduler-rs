use chrono::{DateTime, Days, Months, Utc};

use super::Trigger;

#[derive(Debug, Clone)]
pub struct CalendarIntervalTrigger {
    years: u32,
    months: u32,
    weeks: u32,
    days: u32,
    base_time: DateTime<Utc>,
}

/// Builder for constructing a `CalendarIntervalTrigger`.
#[derive(Debug)]
pub struct CalendarIntervalBuilder {
    amount: u32,
}

impl CalendarIntervalTrigger {
    pub fn every(n: u32) -> CalendarIntervalBuilder {
        CalendarIntervalBuilder { amount: n }
    }

    pub fn starting_at(mut self, base_time: DateTime<Utc>) -> Self {
        self.base_time = base_time;
        self
    }
}

impl CalendarIntervalBuilder {
    pub fn years(self) -> CalendarIntervalTrigger {
        CalendarIntervalTrigger {
            years: self.amount,
            months: 0,
            weeks: 0,
            days: 0,
            base_time: Utc::now(),
        }
    }

    pub fn months(self) -> CalendarIntervalTrigger {
        CalendarIntervalTrigger {
            years: 0,
            months: self.amount,
            weeks: 0,
            days: 0,
            base_time: Utc::now(),
        }
    }

    pub fn weeks(self) -> CalendarIntervalTrigger {
        CalendarIntervalTrigger {
            years: 0,
            months: 0,
            weeks: self.amount,
            days: 0,
            base_time: Utc::now(),
        }
    }

    pub fn days(self) -> CalendarIntervalTrigger {
        CalendarIntervalTrigger {
            years: 0,
            months: 0,
            weeks: 0,
            days: self.amount,
            base_time: Utc::now(),
        }
    }
}

impl Trigger for CalendarIntervalTrigger {
    fn next_fire_time(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut candidate = self.base_time;

        loop {
            if candidate > *after {
                return Some(candidate);
            }
            candidate = self.advance(candidate)?;
        }
    }

    fn description(&self) -> String {
        let mut parts = Vec::new();
        if self.years > 0 {
            parts.push(format!("{} year(s)", self.years));
        }
        if self.months > 0 {
            parts.push(format!("{} month(s)", self.months));
        }
        if self.weeks > 0 {
            parts.push(format!("{} week(s)", self.weeks));
        }
        if self.days > 0 {
            parts.push(format!("{} day(s)", self.days));
        }
        format!("CalendarIntervalTrigger(every {})", parts.join(", "))
    }
}

impl CalendarIntervalTrigger {
    fn advance(&self, dt: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut result = dt;

        if self.years > 0 {
            result = result.checked_add_months(Months::new(self.years * 12))?;
        }
        if self.months > 0 {
            result = result.checked_add_months(Months::new(self.months))?;
        }
        let total_days = self.weeks as u64 * 7 + self.days as u64;
        if total_days > 0 {
            result = result.checked_add_days(Days::new(total_days))?;
        }

        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_every_3_months() {
        let base = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
        let trigger = CalendarIntervalTrigger::every(3).months().starting_at(base);

        let after = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 4, 15, 10, 0, 0).unwrap());

        let next2 = trigger.next_fire_time(&next).unwrap();
        assert_eq!(next2, Utc.with_ymd_and_hms(2025, 7, 15, 10, 0, 0).unwrap());
    }

    #[test]
    fn test_every_2_weeks() {
        let base = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let trigger = CalendarIntervalTrigger::every(2).weeks().starting_at(base);

        let after = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_month_boundary_handling() {
        // Starting Jan 31, add 1 month should go to Feb 28 (2025 is not a leap year)
        let base = Utc.with_ymd_and_hms(2025, 1, 31, 12, 0, 0).unwrap();
        let trigger = CalendarIntervalTrigger::every(1).months().starting_at(base);

        let after = Utc.with_ymd_and_hms(2025, 1, 31, 12, 0, 0).unwrap();
        let next = trigger.next_fire_time(&after).unwrap();
        // chrono clamps Jan 31 + 1 month to Feb 28
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 2, 28, 12, 0, 0).unwrap());
    }
}
