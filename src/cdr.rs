//! Turning ROS 2 CDR payloads into dimos LCM wire bytes.
//!
//! Recordings written by `db_to_mcap` (and plain rosbag2) store ROS 2 messages
//! in CDR, which no LCM or dimos subscriber understands. Each supported type is
//! read field by field and handed to the generated `lcm-msgs` struct, so the
//! fingerprint and field layout come from the same code dimos itself encodes
//! with rather than from offsets maintained here.
//!
//! ROS 2's Header has no `seq`, so the LCM `header.seq` is left at 0.

use anyhow::{bail, Context, Result};
use lcm_msgs::{geometry_msgs, nav_msgs, sensor_msgs, std_msgs, tf2_msgs};

/// Names the dimos type a ROS schema corresponds to.
///
/// rosbag2 writes `sensor_msgs/msg/Imu`; recordings taken straight off a DDS
/// wire (the Go2, for instance) carry the IDL spelling `sensor_msgs::msg::dds_::Imu_`.
pub fn msg_name_from_schema(schema: &str) -> String {
    let flattened = schema.replace("::", "/");
    let mut parts = flattened.split('/').filter(|part| !matches!(*part, "msg" | "dds_"));
    match (parts.next(), parts.next()) {
        (Some(package), Some(kind)) => format!("{package}.{}", kind.trim_end_matches('_')),
        _ => flattened.replace('/', "."),
    }
}

pub fn supports(msg_name: &str) -> bool {
    matches!(
        msg_name,
        "geometry_msgs.PoseStamped"
            | "geometry_msgs.TransformStamped"
            | "nav_msgs.Odometry"
            | "nav_msgs.Path"
            | "sensor_msgs.CameraInfo"
            | "sensor_msgs.CompressedImage"
            | "sensor_msgs.Image"
            | "sensor_msgs.Imu"
            | "sensor_msgs.JointState"
            | "sensor_msgs.LaserScan"
            | "sensor_msgs.PointCloud2"
            | "tf2_msgs.TFMessage"
    )
}

pub fn to_lcm(msg_name: &str, payload: &[u8]) -> Result<Vec<u8>> {
    let mut reader = Reader::new(payload)?;
    let wire = match msg_name {
        "geometry_msgs.PoseStamped" => pose_stamped(&mut reader)?.encode(),
        "geometry_msgs.TransformStamped" => transform_stamped(&mut reader)?.encode(),
        "nav_msgs.Odometry" => odometry(&mut reader)?.encode(),
        "nav_msgs.Path" => path(&mut reader)?.encode(),
        "sensor_msgs.CameraInfo" => camera_info(&mut reader)?.encode(),
        "sensor_msgs.CompressedImage" => compressed_image(&mut reader)?.encode(),
        "sensor_msgs.Image" => image(&mut reader)?.encode(),
        "sensor_msgs.Imu" => imu(&mut reader)?.encode(),
        "sensor_msgs.JointState" => joint_state(&mut reader)?.encode(),
        "sensor_msgs.LaserScan" => laser_scan(&mut reader)?.encode(),
        "sensor_msgs.PointCloud2" => point_cloud2(&mut reader)?.encode(),
        "tf2_msgs.TFMessage" => tf_message(&mut reader)?.encode(),
        other => bail!("no CDR transcoder for {other}"),
    };
    Ok(wire)
}

// ------------------------------------------------------------------- reading

/// A CDR body plus the endianness its encapsulation header declared.
///
/// Primitives align to their own size relative to the start of the body, which
/// is why the 4-byte encapsulation header is stripped up front.
struct Reader<'a> {
    body: &'a [u8],
    position: usize,
    little_endian: bool,
}

impl<'a> Reader<'a> {
    fn new(payload: &'a [u8]) -> Result<Self> {
        let header = payload.get(..4).context("CDR payload is too short for its header")?;
        Ok(Self { body: &payload[4..], position: 0, little_endian: header[1] & 1 == 1 })
    }

    fn take(&mut self, count: usize, alignment: usize) -> Result<&'a [u8]> {
        self.position = self.position.next_multiple_of(alignment);
        let end = self.position + count;
        let bytes = self.body.get(self.position..end).context("CDR payload ended early")?;
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1, 1)?[0])
    }

    fn bool(&mut self) -> Result<bool> {
        Ok(self.u8()? != 0)
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self.take(4, 4)?.try_into().expect("take returned 4 bytes");
        Ok(if self.little_endian { u32::from_le_bytes(bytes) } else { u32::from_be_bytes(bytes) })
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(self.u32()? as i32)
    }

    fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_bits(self.u32()?))
    }

    fn f64(&mut self) -> Result<f64> {
        let bytes: [u8; 8] = self.take(8, 8)?.try_into().expect("take returned 8 bytes");
        Ok(if self.little_endian {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }

    /// Length-prefixed and null-terminated; the null is not part of the value.
    fn string(&mut self) -> Result<String> {
        let length = self.u32()? as usize;
        let bytes = self.take(length, 1)?;
        Ok(String::from_utf8_lossy(bytes.strip_suffix(b"\0").unwrap_or(bytes)).into_owned())
    }

    fn count(&mut self) -> Result<usize> {
        Ok(self.u32()? as usize)
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.count()?;
        Ok(self.take(length, 1)?.to_vec())
    }

    fn f32_sequence(&mut self) -> Result<Vec<f32>> {
        let length = self.count()?;
        (0..length).map(|_| self.f32()).collect()
    }

    fn f64_sequence(&mut self) -> Result<Vec<f64>> {
        let length = self.count()?;
        (0..length).map(|_| self.f64()).collect()
    }

    fn f64_array<const N: usize>(&mut self) -> Result<[f64; N]> {
        let mut values = [0.0; N];
        for value in &mut values {
            *value = self.f64()?;
        }
        Ok(values)
    }
}

// ------------------------------------------------------------------ messages

fn header(reader: &mut Reader) -> Result<std_msgs::Header> {
    Ok(std_msgs::Header {
        seq: 0,
        stamp: std_msgs::Time { sec: reader.i32()?, nsec: reader.i32()? },
        frame_id: reader.string()?,
    })
}

fn vector3(reader: &mut Reader) -> Result<geometry_msgs::Vector3> {
    Ok(geometry_msgs::Vector3 { x: reader.f64()?, y: reader.f64()?, z: reader.f64()? })
}

fn point(reader: &mut Reader) -> Result<geometry_msgs::Point> {
    Ok(geometry_msgs::Point { x: reader.f64()?, y: reader.f64()?, z: reader.f64()? })
}

fn quaternion(reader: &mut Reader) -> Result<geometry_msgs::Quaternion> {
    Ok(geometry_msgs::Quaternion {
        x: reader.f64()?,
        y: reader.f64()?,
        z: reader.f64()?,
        w: reader.f64()?,
    })
}

fn pose(reader: &mut Reader) -> Result<geometry_msgs::Pose> {
    Ok(geometry_msgs::Pose { position: point(reader)?, orientation: quaternion(reader)? })
}

fn twist(reader: &mut Reader) -> Result<geometry_msgs::Twist> {
    Ok(geometry_msgs::Twist { linear: vector3(reader)?, angular: vector3(reader)? })
}

fn pose_stamped(reader: &mut Reader) -> Result<geometry_msgs::PoseStamped> {
    Ok(geometry_msgs::PoseStamped { header: header(reader)?, pose: pose(reader)? })
}

fn transform_stamped(reader: &mut Reader) -> Result<geometry_msgs::TransformStamped> {
    Ok(geometry_msgs::TransformStamped {
        header: header(reader)?,
        child_frame_id: reader.string()?,
        transform: geometry_msgs::Transform {
            translation: vector3(reader)?,
            rotation: quaternion(reader)?,
        },
    })
}

fn tf_message(reader: &mut Reader) -> Result<tf2_msgs::TFMessage> {
    let count = reader.count()?;
    let transforms = (0..count).map(|_| transform_stamped(reader)).collect::<Result<Vec<_>>>()?;
    Ok(tf2_msgs::TFMessage { transforms })
}

fn odometry(reader: &mut Reader) -> Result<nav_msgs::Odometry> {
    Ok(nav_msgs::Odometry {
        header: header(reader)?,
        child_frame_id: reader.string()?,
        pose: geometry_msgs::PoseWithCovariance {
            pose: pose(reader)?,
            covariance: reader.f64_array::<36>()?,
        },
        twist: geometry_msgs::TwistWithCovariance {
            twist: twist(reader)?,
            covariance: reader.f64_array::<36>()?,
        },
    })
}

fn path(reader: &mut Reader) -> Result<nav_msgs::Path> {
    let header = header(reader)?;
    let count = reader.count()?;
    let poses = (0..count).map(|_| pose_stamped(reader)).collect::<Result<Vec<_>>>()?;
    Ok(nav_msgs::Path { header, poses })
}

fn imu(reader: &mut Reader) -> Result<sensor_msgs::Imu> {
    Ok(sensor_msgs::Imu {
        header: header(reader)?,
        orientation: quaternion(reader)?,
        orientation_covariance: reader.f64_array::<9>()?,
        angular_velocity: vector3(reader)?,
        angular_velocity_covariance: reader.f64_array::<9>()?,
        linear_acceleration: vector3(reader)?,
        linear_acceleration_covariance: reader.f64_array::<9>()?,
    })
}

fn image(reader: &mut Reader) -> Result<sensor_msgs::Image> {
    let header = header(reader)?;
    let height = reader.i32()?;
    let width = reader.i32()?;
    let encoding = reader.string()?;
    let is_bigendian = reader.u8()?;
    let step = reader.i32()?;
    Ok(sensor_msgs::Image {
        header,
        height,
        width,
        encoding,
        is_bigendian,
        step,
        data: reader.bytes()?,
    })
}

fn compressed_image(reader: &mut Reader) -> Result<sensor_msgs::CompressedImage> {
    let header = header(reader)?;
    let format = reader.string()?;
    Ok(sensor_msgs::CompressedImage { header, format, data: reader.bytes()? })
}

fn camera_info(reader: &mut Reader) -> Result<sensor_msgs::CameraInfo> {
    let header = header(reader)?;
    let height = reader.i32()?;
    let width = reader.i32()?;
    let distortion_model = reader.string()?;
    let distortion = reader.f64_sequence()?;
    Ok(sensor_msgs::CameraInfo {
        header,
        height,
        width,
        distortion_model,
        D: distortion,
        K: reader.f64_array::<9>()?,
        R: reader.f64_array::<9>()?,
        P: reader.f64_array::<12>()?,
        binning_x: reader.i32()?,
        binning_y: reader.i32()?,
        roi: sensor_msgs::RegionOfInterest {
            x_offset: reader.i32()?,
            y_offset: reader.i32()?,
            height: reader.i32()?,
            width: reader.i32()?,
            do_rectify: reader.bool()?,
        },
    })
}

fn point_cloud2(reader: &mut Reader) -> Result<sensor_msgs::PointCloud2> {
    let header = header(reader)?;
    let height = reader.i32()?;
    let width = reader.i32()?;
    let field_count = reader.count()?;
    let fields = (0..field_count)
        .map(|_| {
            Ok(sensor_msgs::PointField {
                name: reader.string()?,
                offset: reader.i32()?,
                datatype: reader.u8()?,
                count: reader.i32()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let is_bigendian = reader.bool()?;
    let point_step = reader.i32()?;
    let row_step = reader.i32()?;
    let data = reader.bytes()?;
    let is_dense = reader.bool()?;
    Ok(sensor_msgs::PointCloud2 {
        header,
        height,
        width,
        fields,
        is_bigendian,
        point_step,
        row_step,
        data,
        is_dense,
    })
}

fn joint_state(reader: &mut Reader) -> Result<sensor_msgs::JointState> {
    let header = header(reader)?;
    let name_count = reader.count()?;
    let name = (0..name_count).map(|_| reader.string()).collect::<Result<Vec<_>>>()?;
    let position = reader.f64_sequence()?;
    let velocity = reader.f64_sequence()?;
    Ok(sensor_msgs::JointState {
        header,
        name,
        position,
        velocity,
        effort: reader.f64_sequence()?,
    })
}

fn laser_scan(reader: &mut Reader) -> Result<sensor_msgs::LaserScan> {
    let header = header(reader)?;
    let angle_min = reader.f32()?;
    let angle_max = reader.f32()?;
    let angle_increment = reader.f32()?;
    let time_increment = reader.f32()?;
    let scan_time = reader.f32()?;
    let range_min = reader.f32()?;
    let range_max = reader.f32()?;
    let ranges = reader.f32_sequence()?;
    Ok(sensor_msgs::LaserScan {
        header,
        angle_min,
        angle_max,
        angle_increment,
        time_increment,
        scan_time,
        range_min,
        range_max,
        ranges,
        intensities: reader.f32_sequence()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a little-endian CDR body with the alignment rules the reader has
    /// to honour, so the tests exercise padding rather than assuming it away.
    #[derive(Default)]
    struct Writer {
        body: Vec<u8>,
    }

    impl Writer {
        fn align(&mut self, alignment: usize) {
            while !self.body.len().is_multiple_of(alignment) {
                self.body.push(0);
            }
        }
        fn u8(&mut self, value: u8) -> &mut Self {
            self.body.push(value);
            self
        }
        fn u32(&mut self, value: u32) -> &mut Self {
            self.align(4);
            self.body.extend_from_slice(&value.to_le_bytes());
            self
        }
        fn i32(&mut self, value: i32) -> &mut Self {
            self.u32(value as u32)
        }
        fn f32(&mut self, value: f32) -> &mut Self {
            self.u32(value.to_bits())
        }
        fn f64(&mut self, value: f64) -> &mut Self {
            self.align(8);
            self.body.extend_from_slice(&value.to_le_bytes());
            self
        }
        fn string(&mut self, value: &str) -> &mut Self {
            self.u32(value.len() as u32 + 1);
            self.body.extend_from_slice(value.as_bytes());
            self.body.push(0);
            self
        }
        fn bytes(&mut self, value: &[u8]) -> &mut Self {
            self.u32(value.len() as u32);
            self.body.extend_from_slice(value);
            self
        }
        fn finish(&self) -> Vec<u8> {
            let mut payload = vec![0x00, 0x01, 0x00, 0x00];
            payload.extend_from_slice(&self.body);
            payload
        }
    }

    fn with_header<'a>(
        writer: &'a mut Writer,
        sec: i32,
        nsec: i32,
        frame: &str,
    ) -> &'a mut Writer {
        writer.i32(sec).i32(nsec).string(frame)
    }

    #[test]
    fn schema_names_lose_the_ros_msg_segment() {
        assert_eq!(msg_name_from_schema("sensor_msgs/msg/Imu"), "sensor_msgs.Imu");
        assert_eq!(msg_name_from_schema("sensor_msgs/Imu"), "sensor_msgs.Imu");
        assert_eq!(msg_name_from_schema("tf2_msgs/msg/TFMessage"), "tf2_msgs.TFMessage");
        assert_eq!(msg_name_from_schema("sensor_msgs::msg::dds_::Imu_"), "sensor_msgs.Imu");
        assert_eq!(
            msg_name_from_schema("unitree_go::msg::dds_::LowState_"),
            "unitree_go.LowState"
        );
    }

    #[test]
    fn pose_stamped_round_trips_through_the_lcm_encoder() {
        let mut writer = Writer::default();
        with_header(&mut writer, 1700000000, 250_000_000, "odom");
        for value in [1.0, 2.0, 3.0, 0.0, 0.0, 0.0, 1.0] {
            writer.f64(value);
        }

        let wire = to_lcm("geometry_msgs.PoseStamped", &writer.finish()).unwrap();
        let decoded = geometry_msgs::PoseStamped::decode(&wire).unwrap();
        assert_eq!(decoded.header.stamp.sec, 1700000000);
        assert_eq!(decoded.header.stamp.nsec, 250_000_000);
        assert_eq!(decoded.header.frame_id, "odom");
        assert_eq!(decoded.pose.position.x, 1.0);
        assert_eq!(decoded.pose.position.z, 3.0);
        assert_eq!(decoded.pose.orientation.w, 1.0);
    }

    /// `frame_id` leaves the cursor at an odd offset, so the doubles that follow
    /// only land correctly if 8-byte alignment padding is skipped.
    #[test]
    fn odd_length_frame_ids_still_align_the_doubles() {
        let mut writer = Writer::default();
        with_header(&mut writer, 5, 6, "a");
        for value in [9.5, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0] {
            writer.f64(value);
        }

        let wire = to_lcm("geometry_msgs.PoseStamped", &writer.finish()).unwrap();
        let decoded = geometry_msgs::PoseStamped::decode(&wire).unwrap();
        assert_eq!(decoded.pose.position.x, 9.5);
    }

    #[test]
    fn image_payload_survives_transcoding() {
        let pixels: Vec<u8> = (0..48).collect();
        let mut writer = Writer::default();
        with_header(&mut writer, 7, 8, "camera");
        writer.i32(4).i32(4).string("rgb8").u8(0).i32(12).bytes(&pixels);

        let wire = to_lcm("sensor_msgs.Image", &writer.finish()).unwrap();
        let decoded = sensor_msgs::Image::decode(&wire).unwrap();
        assert_eq!(decoded.height, 4);
        assert_eq!(decoded.width, 4);
        assert_eq!(decoded.encoding, "rgb8");
        assert_eq!(decoded.step, 12);
        assert_eq!(decoded.data, pixels);
    }

    #[test]
    fn tf_message_keeps_every_transform() {
        let mut writer = Writer::default();
        writer.u32(2);
        with_header(&mut writer, 1, 2, "world").string("base");
        for value in [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0] {
            writer.f64(value);
        }
        with_header(&mut writer, 3, 4, "base").string("lidar");
        for value in [0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 1.0] {
            writer.f64(value);
        }

        let wire = to_lcm("tf2_msgs.TFMessage", &writer.finish()).unwrap();
        let decoded = tf2_msgs::TFMessage::decode(&wire).unwrap();
        assert_eq!(decoded.transforms.len(), 2);
        assert_eq!(decoded.transforms[0].child_frame_id, "base");
        assert_eq!(decoded.transforms[0].transform.translation.x, 1.0);
        assert_eq!(decoded.transforms[1].header.frame_id, "base");
        assert_eq!(decoded.transforms[1].transform.translation.y, 2.0);
    }

    #[test]
    fn point_cloud2_fields_and_data_come_through() {
        let points: Vec<u8> = (0..24).collect();
        let mut writer = Writer::default();
        with_header(&mut writer, 11, 12, "lidar");
        writer.i32(1).i32(2).u32(1).string("x").u32(0).u8(7).u32(1);
        writer.u8(0).u32(12).u32(24).bytes(&points).u8(1);

        let wire = to_lcm("sensor_msgs.PointCloud2", &writer.finish()).unwrap();
        let decoded = sensor_msgs::PointCloud2::decode(&wire).unwrap();
        assert_eq!(decoded.width, 2);
        assert_eq!(decoded.fields.len(), 1);
        assert_eq!(decoded.fields[0].name, "x");
        assert_eq!(decoded.fields[0].datatype, 7);
        assert_eq!(decoded.point_step, 12);
        assert_eq!(decoded.data, points);
        assert!(decoded.is_dense);
        assert!(!decoded.is_bigendian);
    }

    #[test]
    fn big_endian_payloads_are_read_with_the_declared_endianness() {
        let mut body = Vec::new();
        body.extend_from_slice(&13i32.to_be_bytes());
        body.extend_from_slice(&14i32.to_be_bytes());
        body.extend_from_slice(&2u32.to_be_bytes());
        body.extend_from_slice(b"m\0");
        // 14 bytes so far; the doubles that follow align to 16.
        body.extend_from_slice(&[0, 0]);
        for value in [4.25f64, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0] {
            body.extend_from_slice(&value.to_be_bytes());
        }
        let mut payload = vec![0x00, 0x00, 0x00, 0x00];
        payload.extend_from_slice(&body);

        let wire = to_lcm("geometry_msgs.PoseStamped", &payload).unwrap();
        let decoded = geometry_msgs::PoseStamped::decode(&wire).unwrap();
        assert_eq!(decoded.header.stamp.sec, 13);
        assert_eq!(decoded.header.frame_id, "m");
        assert_eq!(decoded.pose.position.x, 4.25);
    }

    /// Covers the float and string sequence readers, which no other test hits.
    #[test]
    fn laser_scan_sequences_keep_their_lengths_and_values() {
        let mut writer = Writer::default();
        with_header(&mut writer, 21, 22, "laser");
        for value in [-1.5f32, 1.5, 0.25, 0.0, 0.1, 0.05, 30.0] {
            writer.f32(value);
        }
        writer.u32(3).f32(1.0).f32(2.0).f32(3.0);
        writer.u32(0);

        let wire = to_lcm("sensor_msgs.LaserScan", &writer.finish()).unwrap();
        let decoded = sensor_msgs::LaserScan::decode(&wire).unwrap();
        assert_eq!(decoded.angle_min, -1.5);
        assert_eq!(decoded.range_max, 30.0);
        assert_eq!(decoded.ranges, vec![1.0, 2.0, 3.0]);
        assert!(decoded.intensities.is_empty());
    }

    #[test]
    fn joint_state_reads_a_sequence_of_strings() {
        let mut writer = Writer::default();
        with_header(&mut writer, 31, 32, "body");
        writer.u32(2).string("hip").string("knee");
        writer.u32(2).f64(0.5).f64(-0.25);
        writer.u32(0);
        writer.u32(0);

        let wire = to_lcm("sensor_msgs.JointState", &writer.finish()).unwrap();
        let decoded = sensor_msgs::JointState::decode(&wire).unwrap();
        assert_eq!(decoded.name, vec!["hip", "knee"]);
        assert_eq!(decoded.position, vec![0.5, -0.25]);
        assert!(decoded.velocity.is_empty());
        assert!(decoded.effort.is_empty());
    }

    #[test]
    fn truncated_payloads_are_an_error_rather_than_a_panic() {
        let payload = vec![0x00, 0x01, 0x00, 0x00, 1, 2, 3];
        assert!(to_lcm("sensor_msgs.Imu", &payload).is_err());
    }
}
