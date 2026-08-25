//! Rewriting `header.stamp` inside LCM wire payloads.
//!
//! LCM lays fields out in declaration order, big-endian, with no padding, after
//! an 8-byte fingerprint. Array-length fields are declared before the header in
//! the dimos `.lcm` definitions, so for every stamped type the stamp sits at a
//! constant offset: `8 + 4*leading_length_fields + 4 (header.seq)`. That lets us
//! patch 8 bytes in place instead of decoding and re-encoding a multi-megabyte
//! point cloud. `tests::offsets_match_encoder` pins every entry against the
//! real encoder in `lcm-msgs`.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use lcm_msgs::tf2_msgs::TFMessage;

/// The LCM type name of a `tf` stream, the only one `TfFilter` applies to.
pub const TF_MESSAGE: &str = "tf2_msgs.TFMessage";

/// Edges a live graph publishes for itself, so a replay must not also supply
/// them. Parent or child may be `*`.
const LIVE_OWNED: [(&str, &str); 4] =
    [("odom", "*"), ("map", "*"), ("visual_odom", "*"), ("*", "base_link")];

/// Drops the transforms inside a `tf` message that the live graph owns.
///
/// A recording carries whatever odometry edges were on the wire when it was
/// made, and the graph replaying into publishes its own. Both reaching TF gives
/// one frame two parents, which TF resolves by whichever arrived most recently
/// — a silent, roughly fixed error in every pose derived from it, with nothing
/// logged anywhere. `drive_2026-08-18_23-05-04.db` carries `mid360_link` under
/// both `base_link` (the 22.57° lidar mounting) and `odom`, so this is not
/// hypothetical.
///
/// On by default for that reason: replaying `tf` is the obvious thing to try,
/// and it is the quiet failures that cost whole runs.
pub struct TfFilter {
    rules: Vec<(String, String)>,
    dropped: BTreeMap<(String, String), u64>,
    /// Messages every transform was dropped from, which are not worth sending.
    emptied: u64,
}

impl TfFilter {
    /// `specs` are extra `PARENT:CHILD` rules on top of the built-in set.
    pub fn new(specs: &[String]) -> Result<Self> {
        let mut rules: Vec<(String, String)> =
            LIVE_OWNED.iter().map(|(p, c)| (p.to_string(), c.to_string())).collect();
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

    fn owned_by_the_live_graph(&self, parent: &str, child: &str) -> bool {
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
            if self.owned_by_the_live_graph(parent, child) {
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
        let mut filter = TfFilter::new(&[]).unwrap();
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

    #[test]
    fn every_live_owned_edge_is_dropped_by_default() {
        let mut filter = TfFilter::new(&[]).unwrap();
        for edge in [("odom", "x"), ("map", "x"), ("visual_odom", "x"), ("anything", "base_link")] {
            assert_eq!(filtered(&mut filter, &[edge]), None, "{edge:?} should have been dropped");
        }
    }

    /// A message with nothing left is not worth putting on the wire, and the
    /// caller needs to know rather than publishing an empty TFMessage.
    #[test]
    fn a_message_of_only_dropped_edges_is_not_published() {
        let mut filter = TfFilter::new(&[]).unwrap();
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
