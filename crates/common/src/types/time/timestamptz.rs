//! UTC timestamp newtype wrapping `jiff::Timestamp` with serde, arithmetic, and sqlx support.

use jiff::{SignedDuration, Span, Timestamp};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Sub};
use std::time::{Duration, SystemTime};

/// Canonical UTC timestamp type backed by `jiff::Timestamp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamptz(Timestamp);

impl Timestamptz {
    #[inline]
    pub fn now() -> Self {
        Self(Timestamp::now())
    }

    #[inline]
    pub const fn from_jiff(ts: Timestamp) -> Self {
        Self(ts)
    }

    #[inline]
    pub const fn to_jiff(self) -> Timestamp {
        self.0
    }

    #[inline]
    pub fn from_second(secs: i64) -> Option<Self> {
        Timestamp::from_second(secs).ok().map(Self)
    }

    #[inline]
    pub fn from_millisecond(millis: i64) -> Option<Self> {
        Timestamp::from_millisecond(millis).ok().map(Self)
    }

    #[inline]
    pub fn from_microsecond(micros: i64) -> Option<Self> {
        Timestamp::from_microsecond(micros).ok().map(Self)
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.0.as_second()
    }

    #[inline]
    pub fn timestamp_millis(&self) -> i64 {
        self.0.as_millisecond()
    }

    #[inline]
    pub fn timestamp_micros(&self) -> i64 {
        self.0.as_microsecond()
    }

    /// Sub-second nanosecond component (0..999_999_999).
    #[inline]
    pub fn timestamp_subsec_nanos(&self) -> u32 {
        self.0.subsec_nanosecond() as u32
    }

    #[inline]
    pub fn inner(&self) -> &jiff::Timestamp {
        &self.0
    }

    /// Format as RFC 3339 string (e.g. `2024-01-15T12:30:00Z`).
    #[inline]
    pub fn to_rfc3339(&self) -> String {
        self.0.to_string()
    }

    #[inline]
    pub fn parse_rfc3339(s: &str) -> Result<Self, jiff::Error> {
        s.parse::<Timestamp>().map(Self)
    }

    #[inline]
    pub fn format(&self, fmt: &str) -> String {
        self.0.strftime(fmt).to_string()
    }

    /// Signed duration from `other` to `self` (i.e. `self - other`).
    #[inline]
    pub fn signed_duration_since(&self, other: Timestamptz) -> SignedDuration {
        self.0.duration_since(other.0)
    }

    #[inline]
    pub fn checked_add(&self, dur: SignedDuration) -> Option<Self> {
        self.0.checked_add(dur).ok().map(Self)
    }

    #[inline]
    pub fn checked_sub(&self, dur: SignedDuration) -> Option<Self> {
        self.0.checked_sub(dur).ok().map(Self)
    }

    #[inline]
    pub fn checked_add_span(&self, span: Span) -> Option<Self> {
        self.0.checked_add(span).ok().map(Self)
    }
}

impl fmt::Display for Timestamptz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Timestamp> for Timestamptz {
    #[inline]
    fn from(ts: Timestamp) -> Self {
        Self(ts)
    }
}

impl From<Timestamptz> for Timestamp {
    #[inline]
    fn from(ts: Timestamptz) -> Self {
        ts.0
    }
}

impl From<SystemTime> for Timestamptz {
    #[inline]
    fn from(st: SystemTime) -> Self {
        Self(Timestamp::try_from(st).expect("SystemTime out of jiff::Timestamp range"))
    }
}

impl From<Timestamptz> for SystemTime {
    #[inline]
    fn from(ts: Timestamptz) -> Self {
        SystemTime::from(ts.0)
    }
}

impl std::str::FromStr for Timestamptz {
    type Err = jiff::Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Timestamp>().map(Self)
    }
}

impl Add<SignedDuration> for Timestamptz {
    type Output = Self;

    #[inline]
    fn add(self, rhs: SignedDuration) -> Self {
        Self(self.0 + rhs)
    }
}

impl Sub<SignedDuration> for Timestamptz {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: SignedDuration) -> Self {
        Self(self.0 - rhs)
    }
}

impl Add<Duration> for Timestamptz {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Duration) -> Self {
        Self(self.0 + SignedDuration::try_from(rhs).expect("Duration too large"))
    }
}

impl Sub<Duration> for Timestamptz {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Duration) -> Self {
        Self(self.0 - SignedDuration::try_from(rhs).expect("Duration too large"))
    }
}

impl Add<Span> for Timestamptz {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Span) -> Self {
        Self(self.0 + rhs)
    }
}

impl Sub<Span> for Timestamptz {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Span) -> Self {
        Self(self.0 - rhs)
    }
}

impl Sub<Timestamptz> for Timestamptz {
    type Output = SignedDuration;

    #[inline]
    fn sub(self, rhs: Timestamptz) -> SignedDuration {
        self.0.duration_since(rhs.0)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for Timestamptz {
    #[inline]
    fn from(dt: chrono::DateTime<chrono::Utc>) -> Self {
        // chrono DateTime<Utc> → Unix micros → jiff Timestamp
        Self::from_microsecond(dt.timestamp_micros()).expect("chrono DateTime out of jiff range")
    }
}

impl From<Timestamptz> for chrono::DateTime<chrono::Utc> {
    #[inline]
    fn from(ts: Timestamptz) -> Self {
        chrono::DateTime::from_timestamp_micros(ts.timestamp_micros())
            .expect("Timestamptz out of chrono range")
    }
}

impl Default for Timestamptz {
    fn default() -> Self {
        Self(Timestamp::UNIX_EPOCH)
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use jiff::Span;
    use sqlx::{
        encode::IsNull,
        error::BoxDynError,
        postgres::{
            types::Oid, PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueFormat, PgValueRef,
        },
        Decode, Encode, Type,
    };

    /// PostgreSQL epoch (2000-01-01T00:00:00Z).
    fn pg_epoch() -> Timestamp {
        Timestamp::from_second(946_684_800).expect("valid pg epoch")
    }

    impl Type<sqlx::Postgres> for Timestamptz {
        fn type_info() -> PgTypeInfo {
            PgTypeInfo::with_oid(Oid(1184))
        }

        fn compatible(ty: &PgTypeInfo) -> bool {
            *ty == PgTypeInfo::with_oid(Oid(1184)) || *ty == PgTypeInfo::with_oid(Oid(1114))
        }
    }

    impl PgHasArrayType for Timestamptz {
        fn array_type_info() -> PgTypeInfo {
            PgTypeInfo::with_oid(Oid(1185))
        }
    }

    impl Encode<'_, sqlx::Postgres> for Timestamptz {
        fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
            let span = self.0.since(pg_epoch())?;
            let micros = span.total(jiff::Unit::Microsecond)? as i64;
            Encode::<sqlx::Postgres>::encode(micros, buf)
        }

        fn size_hint(&self) -> usize {
            size_of::<i64>()
        }
    }

    impl Decode<'_, sqlx::Postgres> for Timestamptz {
        fn decode(value: PgValueRef<'_>) -> Result<Self, BoxDynError> {
            Ok(match value.format() {
                PgValueFormat::Binary => {
                    let micros: i64 = <i64 as Decode<sqlx::Postgres>>::decode(value)?;
                    let span = Span::new().try_microseconds(micros)?;
                    let ts = pg_epoch().checked_add(span)?;
                    Timestamptz(ts)
                }
                PgValueFormat::Text => {
                    let s = value.as_str()?;
                    let ts: Timestamp = s.parse()?;
                    Timestamptz(ts)
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now() {
        let ts = Timestamptz::now();
        assert!(ts.timestamp() > 0);
    }

    #[test]
    fn test_serde_roundtrip() {
        let ts = Timestamptz::now();
        let json = serde_json::to_string(&ts).unwrap();
        let parsed: Timestamptz = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, parsed);
    }

    #[test]
    fn test_rfc3339_roundtrip() {
        let ts = Timestamptz::now();
        let s = ts.to_rfc3339();
        let parsed = Timestamptz::parse_rfc3339(&s).unwrap();
        assert_eq!(ts, parsed);
    }

    #[test]
    fn test_arithmetic_signed_duration() {
        let ts = Timestamptz::now();
        let later = ts + SignedDuration::from_secs(60);
        let diff = later - ts;
        assert_eq!(diff.as_secs(), 60);
    }

    #[test]
    fn test_arithmetic_std_duration() {
        let ts = Timestamptz::now();
        let later = ts + Duration::from_secs(30);
        let diff = later - ts;
        assert_eq!(diff.as_secs(), 30);
    }

    #[test]
    fn test_arithmetic_span() {
        let ts = Timestamptz::now();
        let later = ts + jiff::ToSpan::minutes(5);
        let diff = later - ts;
        assert_eq!(diff.as_secs(), 300);
    }

    #[test]
    fn test_signed_duration_since() {
        let a = Timestamptz::now();
        let b = a + SignedDuration::from_secs(10);
        let dur = b.signed_duration_since(a);
        assert_eq!(dur.as_secs(), 10);
    }

    #[test]
    fn test_ordering() {
        let a = Timestamptz::now();
        let b = a + SignedDuration::from_secs(1);
        assert!(b > a);
        assert!(a < b);
    }

    #[test]
    fn test_timestamp_methods() {
        let ts = Timestamptz::from_second(1_700_000_000).unwrap();
        assert_eq!(ts.timestamp(), 1_700_000_000);
        assert_eq!(ts.timestamp_millis(), 1_700_000_000_000);
    }

    #[test]
    fn test_display() {
        let ts = Timestamptz::from_second(0).unwrap();
        assert_eq!(ts.to_string(), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn test_from_str() {
        let ts: Timestamptz = "2024-01-15T12:30:00Z".parse().unwrap();
        assert!(ts.timestamp() > 0);
    }

    #[test]
    fn test_system_time_roundtrip() {
        let ts = Timestamptz::now();
        let st: SystemTime = ts.into();
        let back: Timestamptz = st.into();
        assert_eq!(ts, back);
    }

    #[test]
    fn test_default_is_epoch() {
        let ts = Timestamptz::default();
        assert_eq!(ts.timestamp(), 0);
    }
}
