use chrono::{DateTime, Utc};
/// Simulation/backtest clock.
///
/// Holds a mutable "current time" value that can be set or advanced
/// manually by a backtest driver. Readers always see the most recent
/// simulated timestamp.
pub struct SimClock {
    cur: parking_lot::RwLock<DateTime<Utc>>,
}
impl SimClock {
    /// Construct a new `SimClock` starting at `start`.
    pub fn new(start: DateTime<Utc>) -> Self { Self { cur: parking_lot::RwLock::new(start) } }
    /// Hard-set the simulated time to `t`.
    #[allow(dead_code)]
    pub fn set(&self, t: DateTime<Utc>) { *self.cur.write() = t; }
    #[allow(dead_code)]
    /// Advance the simulated clock forward by `s` seconds.
    pub fn advance_secs(&self, s: i64) {
        use chrono::Duration;
        let mut w = self.cur.write();
        *w = *w + Duration::seconds(s);
    }
    pub(crate) fn now(&self) -> DateTime<Utc> { *self.cur.read() }
}