//! Cron expression parsing and next-fire-time computation.
//!
//! Uses the `cron` crate internally (which depends on chrono). Chrono types
//! are confined to this module — the public API uses only `Timestamptz` (jiff).

use cron::Schedule;
use orcher_common::error::OrcherError;
use orcher_common::types::time::Timestamptz;
use std::str::FromStr;

/// Compute the next fire time for a cron expression in the given timezone.
///
/// Returns `None` if the cron expression has no future occurrences (shouldn't
/// happen for standard expressions, but possible with constrained ones).
pub fn next_fire_time(
    cron_expression: &str,
    timezone: &str,
    after: Timestamptz,
) -> Result<Option<Timestamptz>, OrcherError> {
    let schedule = parse(cron_expression)?;

    let tz: chrono_tz::Tz = timezone
        .parse()
        .map_err(|e| OrcherError::internal(format!("Invalid timezone '{}': {}", timezone, e)))?;

    // Bridge: Timestamptz (jiff) → chrono via existing From impl
    let after_utc: chrono::DateTime<chrono::Utc> = after.into();
    let after_tz = after_utc.with_timezone(&tz);

    // Find next occurrence after the given time
    let next = schedule.after(&after_tz).next();

    // Bridge back: chrono → Timestamptz (jiff) via existing From impl
    Ok(next.map(|dt| Timestamptz::from(dt.with_timezone(&chrono::Utc))))
}

/// Validate a cron expression without computing a fire time.
pub fn validate_cron(cron_expression: &str) -> Result<(), OrcherError> {
    parse(cron_expression).map(|_| ())
}

/// Parse a cron expression. The standard five fields (minute hour
/// day-of-month month day-of-week) are accepted as well as the six or seven
/// the underlying parser takes (a leading seconds field, an optional trailing
/// year): five fields fire at second 0.
fn parse(cron_expression: &str) -> Result<Schedule, OrcherError> {
    let fields = cron_expression.split_whitespace().count();
    let expression = if fields == 5 {
        format!("0 {}", cron_expression.trim())
    } else {
        cron_expression.to_string()
    };
    Schedule::from_str(&expression).map_err(|e| {
        OrcherError::internal(format!(
            "Invalid cron expression '{}': {}",
            cron_expression, e
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The standard five-field form fires at second 0 of the minute it
    /// names, as the six-field form with a leading 0 does.
    #[test]
    fn a_five_field_expression_fires_like_its_six_field_form() {
        let now = Timestamptz::now();
        assert!(validate_cron("*/5 * * * *").is_ok());
        assert_eq!(
            next_fire_time("*/5 * * * *", "UTC", now).unwrap(),
            next_fire_time("0 */5 * * * *", "UTC", now).unwrap()
        );
        assert!(validate_cron("not a cron").is_err());
    }

    #[test]
    fn test_every_minute() {
        let now = Timestamptz::now();
        let result = next_fire_time("0 * * * * *", "UTC", now);
        assert!(result.is_ok());
        let next = result.unwrap();
        assert!(next.is_some());
        let next = next.unwrap();
        // Next fire should be within 60 seconds
        assert!(next > now);
    }

    #[test]
    fn test_daily_at_nine() {
        let now = Timestamptz::now();
        let result = next_fire_time("0 0 9 * * *", "America/New_York", now);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_invalid_cron() {
        let now = Timestamptz::now();
        let result = next_fire_time("not a cron", "UTC", now);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_timezone() {
        let now = Timestamptz::now();
        let result = next_fire_time("0 * * * * *", "Invalid/Zone", now);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_valid() {
        assert!(validate_cron("0 0 9 * * MON-FRI").is_ok());
    }

    #[test]
    fn test_validate_invalid() {
        assert!(validate_cron("bad").is_err());
    }
}
