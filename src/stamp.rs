//! Rewriting `header.stamp` inside LCM wire payloads.
//!
//! LCM lays fields out in declaration order, big-endian, with no padding, after
//! an 8-byte fingerprint. Array-length fields are declared before the header in
//! the dimos `.lcm` definitions, so for every stamped type the stamp sits at a
//! constant offset: `8 + 4*leading_length_fields + 4 (header.seq)`. That lets us
//! patch 8 bytes in place instead of decoding and re-encoding a multi-megabyte
//! point cloud. `tests::offsets_match_encoder` pins every entry against the
//! real encoder in `lcm-msgs`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use lcm_msgs::tf2_msgs::TFMessage;

/// The LCM type name of a `tf` stream, the only one `TfFilter` applies to.
pub const TF_MESSAGE: &str = "tf2_msgs.TFMessage";

/// Drops named transforms from a replayed `tf` message.
///
/// Worth reaching for when the graph being replayed into publishes an edge that
/// lands on a frame the recording also parents: both reaching TF gives one frame
/// two parents, which TF resolves by whichever arrived most recently — a silent,
/// roughly fixed error in every pose derived from it. Every Alfred recording
/// parents `mid360_link` straight off `odom`, so replaying its `tf` into a graph
/// that publishes the lidar mounting off `base_link` does exactly that.
///
/// Entirely opt-in: with no `--drop-tf` rule a replay reproduces the recording.
pub struct TfFilter {
    rules: Vec<(String, String)>,
    dropped: BTreeMap<(String, String), u64>,
    /// Messages every transform was dropped from, which are not worth sending.
    emptied: u64,
}

impl TfFilter {
    /// `specs` are `PARENT:CHILD` rules; `*` matches any frame on either side.
    pub fn new(specs: &[String]) -> Result<Self> {
        let mut rules = Vec::with_capacity(specs.len());
        for spec in specs {
            let (parent, child) = spec
                .split_once(':')
                .with_context(|| format!("--drop-tf wants PARENT:CHILD, not {spec}"))?;
            if parent.is_empty() || child.is_empty() {
                bail!("--drop-tf {spec} has an empty frame; use * to match any");
            }
            rules.push((parent.to_string(), child.to_string()));
        }
        Ok(Self { rules, dropped: BTreeMap::new(), emptied: 0 })
    }

    fn matches_a_rule(&self, parent: &str, child: &str) -> bool {
        self.rules.iter().any(|(p, c)| {
            (p == "*" || p == parent) && (c == "*" || c == child)
        })
    }

    /// Rewrites `buf` without the dropped transforms. `false` means nothing
    /// survived and the message should not be published at all.
    pub fn apply(&mut self, buf: &mut Vec<u8>) -> Result<bool> {
        let message = TFMessage::decode(buf).context("invalid LCM TFMessage")?;
        let mut kept = Vec::with_capacity(message.transforms.len());
        for transform in message.transforms {
            let (parent, child) = (&transform.header.frame_id, &transform.child_frame_id);
            if self.matches_a_rule(parent, child) {
                *self.dropped.entry((parent.clone(), child.clone())).or_default() += 1;
            } else {
                kept.push(transform);
            }
        }
        if kept.is_empty() {
            self.emptied += 1;
            return Ok(false);
        }
        *buf = TFMessage { transforms: kept }.encode();
        Ok(true)
    }

    /// Named per edge rather than totalled, because the count alone does not say
    /// *which* frame was doubled — the fact that costs a decode pass to recover.
    /// Printed even when nothing matched, so the reader can tell an empty result
    /// from a filter that never ran.
    pub fn report(&self) -> String {
        let total: u64 = self.dropped.values().sum();
        let mut report = format!("dropped {total} tf transform(s)");
        if self.dropped.is_empty() {
            report.push('\n');
            return report;
        }
        report.push_str(":\n");
        for ((parent, child), count) in &self.dropped {
            report.push_str(&format!("  {parent} -> {child} ({count})\n"));
        }
        if self.emptied > 0 {
            report.push_str(&format!(
                "  {} tf message(s) had nothing left and were not published\n",
                self.emptied
            ));
        }
        report
    }
}

/// How long a defect stays quiet after being warned about.
///
/// Every defect these checks look for is a property of a stream or of a frame
/// tree, so once one message trips it every later message trips it too — the
/// lidar in `drive_2026-08-16_23-46-03.db` would print 107 lines. Each warning
/// fires on first sight and then at most once a window, carrying the count it
/// swallowed in between.
const WARN_EVERY: Duration = Duration::from_secs(5);

struct Throttle {
    last: Option<Instant>,
    suppressed: u64,
}

impl Throttle {
    fn new() -> Self {
        Self { last: None, suppressed: 0 }
    }

    /// `Some(n)` means print now, having swallowed `n` since the previous print.
    fn ready(&mut self) -> Option<u64> {
        if self.last.is_some_and(|last| last.elapsed() < WARN_EVERY) {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(Instant::now());
        Some(std::mem::take(&mut self.suppressed))
    }
}

fn and_more(suppressed: u64) -> String {
    match suppressed {
        0 => String::new(),
        n => format!(" ({n} more since the last warning)"),
    }
}

fn secs(ns: i64) -> String {
    format!("{:.3}s", ns as f64 / NANOS_PER_SEC as f64)
}

fn listed<'a>(frames: impl IntoIterator<Item = &'a str>) -> String {
    frames.into_iter().collect::<Vec<_>>().join(", ")
}

/// A payload stamp this far past the recorder's delivery time is two clocks
/// disagreeing rather than one being wrong: under a single period of a 100 Hz
/// sensor, "before" and "after" are not meaningfully different. The `lidar`
/// stream runs up to 2.27s ahead, which is not disagreement.
const AHEAD_TOLERANCE_NS: i64 = 10 * (NANOS_PER_SEC / 1000);

#[derive(Default)]
struct StreamStamps {
    name: String,
    previous_ns: Option<i64>,
    ahead: u64,
    worst_ahead_ns: i64,
    backwards: u64,
    worst_backstep_ns: i64,
}

/// Says out loud what a recording claims that cannot be true.
///
/// Nothing here is repaired: a replay puts out what the recording holds. But
/// each of these changes what a downstream node's own timing and pose come out
/// to, and all three are invisible from the subscriber's side — which is why
/// they cost whole debugging sessions before anyone thinks to decode the file.
///
/// - A payload stamped *after* the recorder took delivery describes a message
///   that had not been sent yet.
/// - A payload stamp walking backwards while arrivals walk forwards is a clock
///   that is not monotonic. Detected by order, not by magnitude: the bad stamps
///   have no characteristic offset to threshold against, and a real capture
///   latency of −0.247s is the same size as some of them.
/// - A `tf` stream that is not one tree. TF resolves a pose by walking a frame
///   to its root, so a frame with two parents resolves to whichever edge landed
///   most recently, and two roots means whole sets of frames cannot be related
///   at all. Every Alfred recording is two trees: the RealSense hangs off
///   `base_link` while `mid360_link` hangs off `odom`, with nothing joining
///   them, so nothing in the file says where the lidar is on the robot.
///
/// tf is looked at *after* `--drop-tf`, since dropping the offending edge is
/// the fix and a warning about an edge that is no longer published is noise.
/// Single-header types get the stamp checks and tf gets the tree check: a
/// `TFMessage` is a bag of transforms stamped independently by whoever
/// published each edge, so it has no one stamp to run backwards.
pub struct Audit {
    streams: Vec<StreamStamps>,
    ahead: Throttle,
    backwards: Throttle,
    /// Every parent each frame has been seen under.
    parents: BTreeMap<String, BTreeSet<String>>,
    doubled: Throttle,
    split: Throttle,
    /// When the frames last stopped forming one tree. A tree assembles an edge
    /// at a time, so a frame is briefly its own root until its parent's first
    /// message arrives; only a split that outlives a warning window is real.
    split_since: Option<Instant>,
}

impl Audit {
    pub fn new(stream_names: impl IntoIterator<Item = String>) -> Self {
        Self {
            streams: stream_names
                .into_iter()
                .map(|name| StreamStamps { name, ..Default::default() })
                .collect(),
            ahead: Throttle::new(),
            backwards: Throttle::new(),
            parents: BTreeMap::new(),
            doubled: Throttle::new(),
            split: Throttle::new(),
            split_since: None,
        }
    }

    /// `buf` is the wire payload as it will be published, before retiming;
    /// `received_ns` is when the recorder took delivery of it.
    pub fn inspect(&mut self, stream: usize, support: Support, buf: &[u8], received_ns: i64) {
        match support {
            Support::Fixed(offset) => self.check_stamp(stream, offset, buf, received_ns),
            Support::TfMessage => self.check_tree(buf),
            Support::None => {}
        }
    }

    fn check_stamp(&mut self, stream: usize, offset: usize, buf: &[u8], received_ns: i64) {
        if received_ns == 0 || buf.len() < offset + 8 {
            return;
        }
        let Some(stamp_ns) = read_nanos(buf, offset) else {
            return;
        };
        // A stamp from another clock entirely is already counted and replaced by
        // the retimer; measuring an uptime clock against the epoch says nothing.
        if !same_clock(stamp_ns, received_ns) {
            return;
        }

        let stamps = &mut self.streams[stream];
        let name = stamps.name.clone();

        let ahead_ns = stamp_ns - received_ns;
        if ahead_ns > AHEAD_TOLERANCE_NS {
            stamps.ahead += 1;
            stamps.worst_ahead_ns = stamps.worst_ahead_ns.max(ahead_ns);
        }
        let mut backstep_ns = 0;
        if let Some(previous_ns) = stamps.previous_ns.replace(stamp_ns) {
            if stamp_ns < previous_ns {
                backstep_ns = previous_ns - stamp_ns;
                stamps.backwards += 1;
                stamps.worst_backstep_ns = stamps.worst_backstep_ns.max(backstep_ns);
            }
        }

        if ahead_ns > AHEAD_TOLERANCE_NS {
            if let Some(suppressed) = self.ahead.ready() {
                eprintln!(
                    "warning: {name} is stamped {} after the recorder received it{}",
                    secs(ahead_ns),
                    and_more(suppressed)
                );
            }
        }
        if backstep_ns > 0 {
            if let Some(suppressed) = self.backwards.ready() {
                eprintln!(
                    "warning: {name} stamps went back {} while its arrivals moved forward{}",
                    secs(backstep_ns),
                    and_more(suppressed)
                );
            }
        }
    }

    fn check_tree(&mut self, buf: &[u8]) {
        // A payload the decoder rejects is the retimer's error to raise.
        let Ok(message) = TFMessage::decode(buf) else {
            return;
        };
        for transform in &message.transforms {
            let child = &transform.child_frame_id;
            let parents = self.parents.entry(child.clone()).or_default();
            let doubled = parents.insert(transform.header.frame_id.clone()) && parents.len() > 1;
            let named = match doubled {
                true => listed(parents.iter().map(String::as_str)),
                false => String::new(),
            };
            if doubled {
                if let Some(suppressed) = self.doubled.ready() {
                    eprintln!(
                        "warning: tf gives {child} more than one parent ({named}){}",
                        and_more(suppressed)
                    );
                }
            }
        }

        let roots = self.roots();
        if roots.len() < 2 {
            self.split_since = None;
            return;
        }
        let named = listed(roots.iter().copied());
        let count = roots.len();
        if self.split_since.get_or_insert_with(Instant::now).elapsed() < WARN_EVERY {
            return;
        }
        if let Some(suppressed) = self.split.ready() {
            eprintln!(
                "warning: tf holds {count} separate trees, rooted at {named}{}",
                and_more(suppressed)
            );
        }
    }

    /// Frames that have been seen as a parent but never as a child. One tree
    /// has exactly one.
    fn roots(&self) -> BTreeSet<&str> {
        self.parents
            .values()
            .flatten()
            .map(String::as_str)
            .filter(|frame| !self.parents.contains_key(*frame))
            .collect()
    }

    /// Named per stream and per frame, because the total says nothing about
    /// which sensor's clock to distrust or which frame got doubled — and that
    /// is the fact a decode pass would otherwise be needed to recover.
    pub fn report(&self) -> String {
        let mut report = String::new();

        let mut stamps = String::new();
        for stream in &self.streams {
            let mut faults = Vec::new();
            if stream.ahead > 0 {
                faults.push(format!(
                    "{} stamp(s) up to {} after arrival",
                    stream.ahead,
                    secs(stream.worst_ahead_ns)
                ));
            }
            if stream.backwards > 0 {
                faults.push(format!(
                    "{} backwards step(s) up to {}",
                    stream.backwards,
                    secs(stream.worst_backstep_ns)
                ));
            }
            if !faults.is_empty() {
                stamps.push_str(&format!("  {}: {}\n", stream.name, faults.join(", ")));
            }
        }
        if !stamps.is_empty() {
            report.push_str("stamps that cannot be right:\n");
            report.push_str(&stamps);
        }

        let mut tree = String::new();
        for (child, parents) in &self.parents {
            if parents.len() > 1 {
                tree.push_str(&format!(
                    "  {child} has {} parents: {}\n",
                    parents.len(),
                    listed(parents.iter().map(String::as_str))
                ));
            }
        }
        let roots = self.roots();
        if roots.len() > 1 {
            tree.push_str(&format!(
                "  {} separate trees, rooted at {}\n",
                roots.len(),
                listed(roots.iter().copied())
            ));
        }
        if !tree.is_empty() {
            report.push_str("tf does not describe one tree:\n");
            report.push_str(&tree);
        }

        report
    }
}

/// How a message type's timestamp can be rewritten.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Support {
    /// `stamp.sec` lives at this byte offset, `stamp.nsec` at `offset + 4`.
    Fixed(usize),
    /// Variable-length array of independently stamped transforms.
    TfMessage,
    /// No known timestamp field; payload is republished untouched.
    None,
}

pub fn support_for(msg_name: &str) -> Support {
    match msg_name {
        "tf2_msgs.TFMessage" | "geometry_msgs.Transform" => Support::TfMessage,

        "geometry_msgs.PointStamped"
        | "geometry_msgs.PoseStamped"
        | "geometry_msgs.PoseWithCovarianceStamped"
        | "geometry_msgs.TransformStamped"
        | "geometry_msgs.TwistStamped"
        | "geometry_msgs.TwistWithCovarianceStamped"
        | "geometry_msgs.WrenchStamped"
        | "nav_msgs.Odometry"
        | "sensor_msgs.Imu" => Support::Fixed(12),

        // Bare `builtin_interfaces.Time`, so no `seq` ahead of it.
        "foxglove_msgs.CompressedVideo" => Support::Fixed(12),

        "nav_msgs.LineSegments3D"
        | "nav_msgs.OccupancyGrid"
        | "nav_msgs.Path"
        | "sensor_msgs.CameraInfo"
        | "sensor_msgs.CompressedImage"
        | "sensor_msgs.Image"
        | "vision_msgs.Detection2D"
        | "vision_msgs.Detection2DArray"
        | "vision_msgs.Detection3D"
        | "vision_msgs.Detection3DArray" => Support::Fixed(16),

        "sensor_msgs.Joy" | "sensor_msgs.PointCloud2" => Support::Fixed(20),

        "sensor_msgs.JointState" => Support::Fixed(28),

        _ => Support::None,
    }
}

pub const NANOS_PER_SEC: i64 = 1_000_000_000;

/// How far a payload stamp may sit from the moment the recorder received the
/// message before we stop believing the two are on the same clock.
///
/// Some drivers stamp with system uptime rather than the epoch — `china_office.db`
/// has `color_image` frames stamped `sec=1278` next to a recorded arrival of
/// `1781260015`. Mapped as if it were a recording time, a stamp like that lands
/// before 1970 and gets flattened to zero, which is worse than useless to a
/// subscriber. An hour is far beyond any real sensor latency and still nowhere
/// near an uptime clock.
const SAME_CLOCK_NS: i64 = 3600 * NANOS_PER_SEC;

pub fn seconds_to_nanos(seconds: f64) -> i64 {
    if !seconds.is_finite() || seconds <= 0.0 {
        return 0;
    }
    (seconds * NANOS_PER_SEC as f64).round() as i64
}

/// Maps recording-clock timestamps onto the replay wall clock.
///
/// Arithmetic stays in integer nanoseconds. A Unix timestamp in `f64` *seconds*
/// only resolves to ~100 ns, so round-tripping a stamp through `f64` would
/// perturb it even when the mapping is a whole-second shift.
#[derive(Debug, Clone, Copy)]
pub enum Retimer {
    /// Stamps track emission: at `--rate 2` they end up twice as close together.
    Scaled { first_ns: i64, start_wall_ns: i64, rate: f64 },
    /// Stamps move to the present but keep their original spacing.
    Shifted { delta_ns: i64 },
    /// Payloads keep whatever the recording captured.
    Original,
}

impl Retimer {
    pub fn map(&self, ns: i64) -> i64 {
        match *self {
            // `ns - first_ns` is elapsed recording time, small enough that f64
            // represents it exactly; only the division needs floating point.
            Retimer::Scaled { first_ns, start_wall_ns, rate } => {
                start_wall_ns + ((ns - first_ns) as f64 / rate).round() as i64
            }
            Retimer::Shifted { delta_ns } => ns + delta_ns,
            Retimer::Original => ns,
        }
    }

    /// Rewrites the payload's stamps in place.
    ///
    /// `received_ns` is when the recorder took delivery of the message. It
    /// stands in whenever the payload's own stamp is unset (`sec <= 0`) or is
    /// plainly on another clock, and the return value reports that it had to.
    pub fn apply(&self, support: Support, buf: &mut Vec<u8>, received_ns: i64) -> Result<bool> {
        if matches!(self, Retimer::Original) {
            return Ok(false);
        }
        match support {
            Support::None => Ok(false),
            Support::Fixed(offset) => {
                if buf.len() < offset + 8 {
                    anyhow::bail!(
                        "payload is {} bytes, too short to hold a stamp at offset {offset}",
                        buf.len()
                    );
                }
                let recorded = read_nanos(buf, offset).filter(|ns| same_clock(*ns, received_ns));
                write_nanos(buf, offset, self.map(recorded.unwrap_or(received_ns)));
                Ok(recorded.is_none())
            }
            Support::TfMessage => {
                let mut message = TFMessage::decode(buf).context("invalid LCM TFMessage")?;
                let mut substituted = false;
                for transform in &mut message.transforms {
                    let stamp = &mut transform.header.stamp;
                    let recorded = nanos_from_parts(stamp.sec, stamp.nsec)
                        .filter(|ns| same_clock(*ns, received_ns));
                    substituted |= recorded.is_none();
                    let (sec, nsec) = nanos_to_parts(self.map(recorded.unwrap_or(received_ns)));
                    stamp.sec = sec;
                    stamp.nsec = nsec;
                }
                *buf = message.encode();
                Ok(substituted)
            }
        }
    }
}

/// Whether a payload stamp reads as coming from the clock the recorder used.
/// With no reception time to compare against, the payload gets the benefit of
/// the doubt.
fn same_clock(stamp_ns: i64, received_ns: i64) -> bool {
    received_ns == 0 || (stamp_ns - received_ns).abs() < SAME_CLOCK_NS
}

/// The payload's own timestamp in nanoseconds, or `None` when the type has no
/// known stamp field, the payload is too short, or the stamp is unset.
///
/// This is what a recorder writes as `publish_time`, and — being derived from
/// the bytes rather than from arrival — it is also what lets a message this
/// process published be recognised when it loops back.
pub fn stamp_of(support: Support, buf: &[u8]) -> Option<i64> {
    match support {
        Support::None => None,
        Support::Fixed(offset) => match buf.len() >= offset + 8 {
            true => read_nanos(buf, offset),
            false => None,
        },
        // Each transform carries its own stamp; the first stands for the
        // message, which is all an identity needs.
        Support::TfMessage => {
            let message = TFMessage::decode(buf).ok()?;
            let stamp = &message.transforms.first()?.header.stamp;
            nanos_from_parts(stamp.sec, stamp.nsec)
        }
    }
}

fn read_nanos(buf: &[u8], offset: usize) -> Option<i64> {
    let sec = i32::from_be_bytes(buf[offset..offset + 4].try_into().ok()?);
    let nsec = i32::from_be_bytes(buf[offset + 4..offset + 8].try_into().ok()?);
    nanos_from_parts(sec, nsec)
}

fn write_nanos(buf: &mut [u8], offset: usize, ns: i64) {
    let (sec, nsec) = nanos_to_parts(ns);
    buf[offset..offset + 4].copy_from_slice(&sec.to_be_bytes());
    buf[offset + 4..offset + 8].copy_from_slice(&nsec.to_be_bytes());
}

/// `sec <= 0` is dimos's "unset" marker, not a real 1970 timestamp.
fn nanos_from_parts(sec: i32, nsec: i32) -> Option<i64> {
    (sec > 0).then(|| i64::from(sec) * NANOS_PER_SEC + i64::from(nsec))
}

fn nanos_to_parts(ns: i64) -> (i32, i32) {
    if ns <= 0 {
        return (0, 0);
    }
    let sec = ns.div_euclid(NANOS_PER_SEC);
    (sec.try_into().unwrap_or(i32::MAX), ns.rem_euclid(NANOS_PER_SEC) as i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lcm_msgs::std_msgs::{Header, Time};

    const SEC: i32 = 1_781_260_015;
    const NSEC: i32 = 113_250_000;

    fn header() -> Header {
        Header { seq: 7, stamp: Time { sec: SEC, nsec: NSEC }, frame_id: "camera".into() }
    }

    /// Every `Support::Fixed` offset must agree with what `lcm-msgs` actually encodes.
    #[test]
    fn offsets_match_encoder() {
        macro_rules! check {
            ($name:literal, $encoded:expr) => {{
                let bytes = $encoded;
                let Support::Fixed(offset) = support_for($name) else {
                    panic!("{} is not registered as a fixed-offset type", $name);
                };
                assert_eq!(
                    read_nanos(&bytes, offset),
                    nanos_from_parts(SEC, NSEC),
                    "offset {offset} is wrong for {}",
                    $name
                );
            }};
        }

        use lcm_msgs::{foxglove_msgs, geometry_msgs, nav_msgs, sensor_msgs, vision_msgs};

        check!("geometry_msgs.PointStamped", geometry_msgs::PointStamped { header: header(), ..Default::default() }.encode());
        check!("geometry_msgs.PoseStamped", geometry_msgs::PoseStamped { header: header(), ..Default::default() }.encode());
        check!("geometry_msgs.PoseWithCovarianceStamped", geometry_msgs::PoseWithCovarianceStamped { header: header(), ..Default::default() }.encode());
        check!("geometry_msgs.TransformStamped", geometry_msgs::TransformStamped { header: header(), ..Default::default() }.encode());
        check!("geometry_msgs.TwistStamped", geometry_msgs::TwistStamped { header: header(), ..Default::default() }.encode());
        check!("geometry_msgs.TwistWithCovarianceStamped", geometry_msgs::TwistWithCovarianceStamped { header: header(), ..Default::default() }.encode());
        check!("geometry_msgs.WrenchStamped", geometry_msgs::WrenchStamped { header: header(), ..Default::default() }.encode());
        check!("nav_msgs.Odometry", nav_msgs::Odometry { header: header(), ..Default::default() }.encode());
        check!("sensor_msgs.Imu", sensor_msgs::Imu { header: header(), ..Default::default() }.encode());

        check!("nav_msgs.OccupancyGrid", nav_msgs::OccupancyGrid { header: header(), ..Default::default() }.encode());
        check!("nav_msgs.Path", nav_msgs::Path { header: header(), ..Default::default() }.encode());
        check!("sensor_msgs.CameraInfo", sensor_msgs::CameraInfo { header: header(), ..Default::default() }.encode());
        check!("sensor_msgs.CompressedImage", sensor_msgs::CompressedImage { header: header(), ..Default::default() }.encode());
        check!("sensor_msgs.Image", sensor_msgs::Image { header: header(), ..Default::default() }.encode());
        check!("vision_msgs.Detection2D", vision_msgs::Detection2D { header: header(), ..Default::default() }.encode());
        check!("vision_msgs.Detection2DArray", vision_msgs::Detection2DArray { header: header(), ..Default::default() }.encode());
        check!("vision_msgs.Detection3D", vision_msgs::Detection3D { header: header(), ..Default::default() }.encode());
        check!("vision_msgs.Detection3DArray", vision_msgs::Detection3DArray { header: header(), ..Default::default() }.encode());

        check!("sensor_msgs.Joy", sensor_msgs::Joy { header: header(), ..Default::default() }.encode());
        check!("sensor_msgs.PointCloud2", sensor_msgs::PointCloud2 { header: header(), ..Default::default() }.encode());

        check!("sensor_msgs.JointState", sensor_msgs::JointState { header: header(), ..Default::default() }.encode());

        check!(
            "foxglove_msgs.CompressedVideo",
            foxglove_msgs::CompressedVideo {
                timestamp: lcm_msgs::builtin_interfaces::Time { sec: SEC, nanosec: NSEC },
                ..Default::default()
            }
            .encode()
        );
    }

    /// Patching must touch only the stamp — every other byte stays identical.
    #[test]
    fn patch_leaves_the_rest_of_the_payload_alone() {
        let image = lcm_msgs::sensor_msgs::Image {
            header: header(),
            height: 480,
            width: 640,
            encoding: "jpeg".into(),
            step: 0,
            data: (0..2048u32).map(|byte| byte as u8).collect(),
            ..Default::default()
        };
        let original = image.encode();
        let mut patched = original.clone();

        let retimer = Retimer::Shifted { delta_ns: 1000 * NANOS_PER_SEC };
        retimer.apply(support_for("sensor_msgs.Image"), &mut patched, 0).unwrap();

        assert_eq!(original.len(), patched.len());
        let decoded = lcm_msgs::sensor_msgs::Image::decode(&patched).unwrap();
        assert_eq!(decoded.data, image.data);
        assert_eq!(decoded.encoding, image.encoding);
        assert_eq!(decoded.header.frame_id, image.header.frame_id);
        assert_eq!(decoded.header.stamp.sec, SEC + 1000);
        assert_eq!(decoded.header.stamp.nsec, NSEC);
    }

    #[test]
    fn tf_message_stamps_all_transforms() {
        let message = TFMessage {
            transforms: vec![
                geometry_stamped("odom", "base_link"),
                geometry_stamped("base_link", "camera"),
            ],
        };
        let mut buf = message.encode();

        let retimer = Retimer::Shifted { delta_ns: 60 * NANOS_PER_SEC };
        retimer.apply(Support::TfMessage, &mut buf, 0).unwrap();

        let decoded = TFMessage::decode(&buf).unwrap();
        assert_eq!(decoded.transforms.len(), 2);
        for transform in &decoded.transforms {
            assert_eq!(transform.header.stamp.sec, SEC + 60);
        }
        assert_eq!(decoded.transforms[1].child_frame_id, "camera");
    }

    fn filtered(filter: &mut TfFilter, edges: &[(&str, &str)]) -> Option<Vec<(String, String)>> {
        let transforms = edges.iter().map(|(p, c)| geometry_stamped(p, c)).collect();
        let mut buf = TFMessage { transforms }.encode();
        if !filter.apply(&mut buf).unwrap() {
            return None;
        }
        Some(
            TFMessage::decode(&buf)
                .unwrap()
                .transforms
                .iter()
                .map(|t| (t.header.frame_id.clone(), t.child_frame_id.clone()))
                .collect(),
        )
    }

    /// The real tree in `drive_2026-08-18_23-05-04.db`: `mid360_link` sits under
    /// both the lidar mounting and the record-time odometry edge. Keeping the
    /// mounting and dropping the odometry is the whole point of the filter.
    #[test]
    fn the_recorded_odometry_edge_goes_and_the_mounting_stays() {
        let mut filter = TfFilter::new(&["odom:mid360_link".into()]).unwrap();
        let kept = filtered(
            &mut filter,
            &[("base_link", "mid360_link"), ("odom", "mid360_link"), ("base_link", "camera_link")],
        )
        .unwrap();
        assert_eq!(
            kept,
            [
                ("base_link".to_string(), "mid360_link".to_string()),
                ("base_link".to_string(), "camera_link".to_string())
            ]
        );
        assert!(filter.report().contains("odom -> mid360_link (1)"), "{}", filter.report());
    }

    /// A replay reproduces the recording unless asked otherwise, including the
    /// odometry edges a live graph might also be publishing.
    #[test]
    fn nothing_is_dropped_without_a_rule() {
        let mut filter = TfFilter::new(&[]).unwrap();
        for edge in [("odom", "x"), ("map", "x"), ("visual_odom", "x"), ("anything", "base_link")] {
            assert_eq!(
                filtered(&mut filter, &[edge]),
                Some(vec![(edge.0.to_string(), edge.1.to_string())]),
                "{edge:?} should have been replayed"
            );
        }
    }

    /// A message with nothing left is not worth putting on the wire, and the
    /// caller needs to know rather than publishing an empty TFMessage.
    #[test]
    fn a_message_of_only_dropped_edges_is_not_published() {
        let mut filter = TfFilter::new(&["odom:base_link".into()]).unwrap();
        assert_eq!(filtered(&mut filter, &[("odom", "base_link")]), None);
        assert!(filter.report().contains("had nothing left"), "{}", filter.report());
    }

    #[test]
    fn an_extra_rule_may_wildcard_either_side() {
        let mut filter = TfFilter::new(&["*:mid360_link".into()]).unwrap();
        assert_eq!(filtered(&mut filter, &[("base_link", "mid360_link")]), None);

        let mut filter = TfFilter::new(&["camera_link:*".into()]).unwrap();
        assert_eq!(filtered(&mut filter, &[("camera_link", "camera_depth_frame")]), None);
    }

    /// Zero has to print, or an empty result reads the same as a filter that
    /// never ran — which is how the missing-prefix bug stayed hidden.
    #[test]
    fn a_clean_recording_still_reports_the_filter_ran() {
        let mut filter = TfFilter::new(&[]).unwrap();
        assert!(filtered(&mut filter, &[("base_link", "camera_link")]).is_some());
        assert_eq!(filter.report(), "dropped 0 tf transform(s)\n");
    }

    #[test]
    fn a_drop_tf_rule_without_a_colon_is_an_error() {
        assert!(TfFilter::new(&["odom".into()]).is_err());
        assert!(TfFilter::new(&["odom:".into()]).is_err());
        assert!(TfFilter::new(&[":base_link".into()]).is_err());
    }

    fn geometry_stamped(frame: &str, child: &str) -> lcm_msgs::geometry_msgs::TransformStamped {
        lcm_msgs::geometry_msgs::TransformStamped {
            header: Header {
                seq: 0,
                stamp: Time { sec: SEC, nsec: NSEC },
                frame_id: frame.into(),
            },
            child_frame_id: child.into(),
            ..Default::default()
        }
    }

    fn audit() -> Audit {
        Audit::new(["lidar".to_string()])
    }

    /// Feeds one `sensor_msgs.Image` stamped `stamp_sec` and received at
    /// `received_sec`, which is the pair the checks compare.
    fn observe(audit: &mut Audit, stamp_ns: i64, received_ns: i64) {
        let (sec, nsec) = nanos_to_parts(stamp_ns);
        let stamped = Header { stamp: Time { sec, nsec }, ..header() };
        let buf = lcm_msgs::sensor_msgs::Image { header: stamped, ..Default::default() }.encode();
        audit.inspect(0, support_for("sensor_msgs.Image"), &buf, received_ns);
    }

    fn at(sec: i64) -> i64 {
        (i64::from(SEC) + sec) * NANOS_PER_SEC
    }

    /// A message cannot have been stamped after the recorder took delivery of it.
    #[test]
    fn a_stamp_from_after_its_own_arrival_is_reported() {
        let mut audit = audit();
        observe(&mut audit, at(0) + 2 * NANOS_PER_SEC, at(0));
        assert!(
            audit.report().contains("lidar: 1 stamp(s) up to 2.000s after arrival"),
            "{}",
            audit.report()
        );
    }

    /// Two clocks a few milliseconds apart are not a broken clock, and warning
    /// about them every message would bury the ones that are.
    #[test]
    fn a_stamp_a_few_milliseconds_early_is_left_alone() {
        let mut audit = audit();
        observe(&mut audit, at(0) + NANOS_PER_SEC / 1000, at(0));
        assert_eq!(audit.report(), "");
    }

    /// Detected by order rather than magnitude: the bad stamps in
    /// `drive_2026-08-16_23-46-03.db` scatter from a few ms to 68s behind, and
    /// a real capture latency is the same size as the small ones.
    #[test]
    fn a_stamp_that_walks_backwards_is_reported() {
        let mut audit = audit();
        observe(&mut audit, at(10), at(10));
        observe(&mut audit, at(11), at(11));
        observe(&mut audit, at(9), at(12));
        assert!(
            audit.report().contains("lidar: 1 backwards step(s) up to 2.000s"),
            "{}",
            audit.report()
        );
    }

    #[test]
    fn stamps_that_only_move_forward_are_left_alone() {
        let mut audit = audit();
        for step in 0..5 {
            observe(&mut audit, at(step), at(step));
        }
        assert_eq!(audit.report(), "");
    }

    /// An uptime stamp is already counted and replaced by the retimer; measuring
    /// it against the epoch would report the same defect a second time as an
    /// implausible 56-year backwards step.
    #[test]
    fn a_stamp_from_another_clock_is_not_reported_twice() {
        let mut audit = audit();
        observe(&mut audit, at(0), at(0));
        observe(&mut audit, 1278 * NANOS_PER_SEC, at(1));
        observe(&mut audit, at(2), at(2));
        assert_eq!(audit.report(), "");
    }

    fn observe_tf(audit: &mut Audit, edges: &[(&str, &str)]) {
        let transforms = edges.iter().map(|(p, c)| geometry_stamped(p, c)).collect();
        let buf = TFMessage { transforms }.encode();
        audit.inspect(0, Support::TfMessage, &buf, at(0));
    }

    /// What replaying an Alfred `tf` stream into a live graph produces:
    /// the recording's `odom -> mid360_link` meets the graph's own mounting
    /// edge, and the lidar pose resolves to whichever arrived last.
    #[test]
    fn a_frame_with_two_parents_is_reported() {
        let mut audit = audit();
        observe_tf(&mut audit, &[("base_link", "mid360_link"), ("base_link", "camera_link")]);
        observe_tf(&mut audit, &[("odom", "mid360_link")]);
        assert!(
            audit.report().contains("mid360_link has 2 parents: base_link, odom"),
            "{}",
            audit.report()
        );
    }

    /// Frames in two trees cannot be related to each other at all.
    #[test]
    fn frames_that_do_not_reach_one_root_are_reported() {
        let mut audit = audit();
        observe_tf(&mut audit, &[("base_link", "camera_link"), ("map", "waypoint")]);
        assert!(
            audit.report().contains("2 separate trees, rooted at base_link, map"),
            "{}",
            audit.report()
        );
    }

    /// A tree arrives an edge at a time, so a frame is its own root until its
    /// parent's first message lands. Nothing may be reported for that.
    #[test]
    fn a_tree_assembled_out_of_order_is_not_a_split_tree() {
        let mut audit = audit();
        observe_tf(&mut audit, &[("base_link", "camera_link")]);
        observe_tf(&mut audit, &[("odom", "base_link")]);
        assert_eq!(audit.report(), "");
    }

    /// The count that is swallowed has to be carried, or the log understates
    /// how much of the recording is affected.
    #[test]
    fn repeat_warnings_are_throttled_but_still_counted() {
        let mut audit = audit();
        for step in 0..100 {
            observe(&mut audit, at(step) + 2 * NANOS_PER_SEC, at(step));
        }
        assert!(audit.report().contains("100 stamp(s) up to 2.000s"), "{}", audit.report());
        assert!(audit.ahead.suppressed >= 98, "{} suppressed", audit.ahead.suppressed);
    }

    /// A driver that stamps with system uptime must not drag the message back
    /// to 1970; reception time is the only usable clock in that case.
    #[test]
    fn stamps_from_another_clock_fall_back_to_reception_time() {
        let uptime = Header { stamp: Time { sec: 1278, nsec: 135_574_598 }, ..header() };
        let mut buf =
            lcm_msgs::sensor_msgs::Image { header: uptime, ..Default::default() }.encode();
        let received_ns = 1_781_260_015 * NANOS_PER_SEC;

        let substituted = Retimer::Shifted { delta_ns: 0 }
            .apply(support_for("sensor_msgs.Image"), &mut buf, received_ns)
            .unwrap();

        assert!(substituted, "an uptime stamp should be reported, not silently used");
        let decoded = lcm_msgs::sensor_msgs::Image::decode(&buf).unwrap();
        assert_eq!(decoded.header.stamp.sec, 1_781_260_015);
    }

    #[test]
    fn a_stamp_on_the_recorders_clock_is_kept() {
        let mut buf =
            lcm_msgs::sensor_msgs::Image { header: header(), ..Default::default() }.encode();
        let received_ns = i64::from(SEC) * NANOS_PER_SEC + 4 * NANOS_PER_SEC;

        let substituted = Retimer::Shifted { delta_ns: 0 }
            .apply(support_for("sensor_msgs.Image"), &mut buf, received_ns)
            .unwrap();

        assert!(!substituted);
        let decoded = lcm_msgs::sensor_msgs::Image::decode(&buf).unwrap();
        assert_eq!(decoded.header.stamp.nsec, NSEC);
    }

    #[test]
    fn unset_stamps_fall_back_to_reception_time() {
        let mut buf = lcm_msgs::nav_msgs::Odometry::default().encode();
        let retimer = Retimer::Shifted { delta_ns: 0 };
        retimer.apply(Support::Fixed(12), &mut buf, seconds_to_nanos(1234.5)).unwrap();

        let decoded = lcm_msgs::nav_msgs::Odometry::decode(&buf).unwrap();
        assert_eq!(decoded.header.stamp.sec, 1234);
        assert_eq!(decoded.header.stamp.nsec, 500_000_000);
    }

    #[test]
    fn scaled_retimer_compresses_intervals_by_rate() {
        let retimer = Retimer::Scaled {
            first_ns: 100 * NANOS_PER_SEC,
            start_wall_ns: 5000 * NANOS_PER_SEC,
            rate: 2.0,
        };
        assert_eq!(retimer.map(100 * NANOS_PER_SEC), 5000 * NANOS_PER_SEC);
        assert_eq!(retimer.map(110 * NANOS_PER_SEC), 5005 * NANOS_PER_SEC);
    }

    /// A whole-second shift must leave the nanosecond field bit-identical;
    /// going through f64 seconds would perturb it by ~100 ns.
    #[test]
    fn shifting_preserves_nanosecond_precision() {
        let mut buf = lcm_msgs::nav_msgs::Odometry {
            header: header(),
            ..Default::default()
        }
        .encode();
        Retimer::Shifted { delta_ns: 1000 * NANOS_PER_SEC }
            .apply(Support::Fixed(12), &mut buf, 0)
            .unwrap();

        let decoded = lcm_msgs::nav_msgs::Odometry::decode(&buf).unwrap();
        assert_eq!(decoded.header.stamp.sec, SEC + 1000);
        assert_eq!(decoded.header.stamp.nsec, NSEC);
    }
}
