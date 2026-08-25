//! Pacing the replay against a downstream node instead of the wall clock.
//!
//! `--lockstep wheel_odom:fused_odom` makes every `wheel_odom` publish wait for
//! the `fused_odom` that answers the one before it. A consumer that keeps up
//! therefore pulls the recording along faster than it was recorded, and one
//! that falls behind slows it down, with `--lockstep-timeout` as the escape
//! hatch when nothing answers at all.
//!
//! The rate that comes out of this drives more than pacing: the messages
//! *between* two gate publishes — camera frames between two odometry ticks —
//! are spread across the same interval, so their stamps stay where they belong
//! relative to the gate messages around them.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::sink::{Arrivals, Sink};
use crate::source::Stream;

/// How strongly one cycle moves the running estimate. Jeff asked for a rough
/// average, and a rough average is also what keeps a single jittery reply from
/// yanking the stamps of the frames that follow it.
const SMOOTHING: f64 = 0.3;

/// A recording second replayed in a microsecond is not a measurement, it is a
/// division by clock jitter. Bounding the estimate stops one instant reply
/// from collapsing every later stamp onto the same nanosecond.
const SLOWEST: f64 = 0.001;
const FASTEST: f64 = 1000.0;

pub struct Lockstep {
    /// Index of the replayed stream whose publishes wait to be answered.
    pub stream: usize,
    /// Replies that arrived after the deadline, reported at the end.
    pub timeouts: u64,
    timeout: Duration,
    arrivals: Arrivals,
    outstanding: Option<Outstanding>,
}

/// A gate message that has gone out and is waiting to be answered.
struct Outstanding {
    replies_before: u64,
    published_at: Instant,
    recorded_ts: f64,
}

impl Lockstep {
    pub async fn open(
        spec: &str,
        streams: &[Stream],
        sink: &Sink,
        prefix: &str,
        timeout: Duration,
    ) -> Result<Self> {
        let (gate, reply) = spec
            .split_once(':')
            .context("--lockstep wants STREAM:TOPIC, for example wheel_odom:fused_odom")?;
        let stream = streams
            .iter()
            .position(|stream| stream.name == gate)
            .with_context(|| format!("--lockstep waits on {gate}, which is not being replayed"))?;
        // The reply comes from a module in the same graph, so it is namespaced
        // the same way our own publishes are.
        let arrivals = sink.watch(&format!("{prefix}{reply}")).await?;
        Ok(Self { stream, timeouts: 0, timeout, arrivals, outstanding: None })
    }

    /// Blocks until the reply to the previous gate message lands, folding how
    /// long it took into `rate`. Returns immediately for the first one, which
    /// has nothing to wait for.
    pub async fn wait(&mut self, recorded_ts: f64, rate: &mut f64) {
        let Some(outstanding) = self.outstanding.take() else {
            return;
        };
        match self.next_reply(outstanding.replies_before).await {
            // Timing from the publish rather than from the moment we started
            // waiting keeps our own pacing decisions out of the measurement:
            // the reply may well have landed while we were still emitting the
            // frames in between.
            Some(arrived) => {
                let wall = arrived.saturating_duration_since(outstanding.published_at).as_secs_f64();
                *rate = blend(*rate, recorded_ts - outstanding.recorded_ts, wall);
            }
            None => self.timeouts += 1,
        }
    }

    /// Records that a gate message just went out, so the next `wait` knows what
    /// it is waiting for.
    pub fn sent(&mut self, recorded_ts: f64, published_at: Instant) {
        let replies_before = self.arrivals.borrow().0;
        self.outstanding = Some(Outstanding { replies_before, published_at, recorded_ts });
    }

    /// When the first reply after `replies_before` arrived, or `None` if the
    /// timeout ran out first.
    async fn next_reply(&mut self, replies_before: u64) -> Option<Instant> {
        let deadline = Instant::now() + self.timeout;
        loop {
            let (replies, arrived) = *self.arrivals.borrow_and_update();
            if replies > replies_before {
                return Some(arrived);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            // An error either way means the timeout expired or the watching
            // task is gone; neither is worth waiting on any longer.
            if tokio::time::timeout(remaining, self.arrivals.changed()).await.is_err() {
                return None;
            }
        }
    }
}

/// Folds one measured cycle into the running estimate: `recorded` seconds of
/// the recording were answered in `wall` seconds of real time.
fn blend(rate: f64, recorded: f64, wall: f64) -> f64 {
    if wall <= 0.0 || recorded <= 0.0 {
        return rate;
    }
    let observed = (recorded / wall).clamp(SLOWEST, FASTEST);
    rate + SMOOTHING * (observed - rate)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A step toward the measurement, not a jump to it, in both directions.
    #[test]
    fn the_estimate_eases_toward_what_was_measured() {
        let quicker = blend(1.0, 0.1, 0.025);
        assert!(quicker > 1.0 && quicker < 4.0, "{quicker} should be a step toward 4x");

        let slower = blend(1.0, 0.1, 0.5);
        assert!(slower < 1.0 && slower > 0.2, "{slower} should be a step toward 0.2x");
    }

    #[test]
    fn repeated_measurements_converge_on_the_measured_rate() {
        let mut rate = 1.0;
        for _ in 0..80 {
            rate = blend(rate, 0.1, 0.025);
        }
        assert!((rate - 4.0).abs() < 1e-6, "{rate} should have converged on 4x");
    }

    /// A reply that lands in the same microsecond is clock jitter, not a
    /// million-times-realtime consumer.
    #[test]
    fn an_instant_reply_cannot_drive_the_rate_past_the_bound() {
        assert_eq!(blend(FASTEST, 0.05, 1e-9), FASTEST);
    }

    /// The loop boundary of `--loop` walks the recording clock backwards.
    #[test]
    fn a_backwards_interval_leaves_the_estimate_alone() {
        assert_eq!(blend(2.5, -30.0, 0.01), 2.5);
    }

    #[test]
    fn a_pairing_needs_both_halves() {
        assert_eq!("wheel_odom:fused_odom".split_once(':'), Some(("wheel_odom", "fused_odom")));
        assert_eq!("wheel_odom".split_once(':'), None);
    }
}
