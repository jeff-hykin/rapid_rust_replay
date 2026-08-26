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

// ------------------------------------------------------------------- writing

/// Builds a little-endian CDR body, padding each primitive to its own size.
///
/// Alignment is measured from the start of the body, so the 4-byte
/// encapsulation header is only prepended by `finish`.
#[derive(Default)]
pub struct Writer {
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

    fn bool(&mut self, value: bool) -> &mut Self {
        self.u8(u8::from(value))
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

    /// Length-prefixed and null-terminated; the length counts the null.
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

    fn count(&mut self, value: usize) -> &mut Self {
        self.u32(value as u32)
    }

    /// A bounded or unbounded sequence: a length, then the values.
    fn f64_sequence(&mut self, values: &[f64]) -> &mut Self {
        self.count(values.len());
        self.f64_array(values)
    }

    fn f32_sequence(&mut self, values: &[f32]) -> &mut Self {
        self.count(values.len());
        for value in values {
            self.f32(*value);
        }
        self
    }

    /// A fixed-size array, which carries no length of its own.
    fn f64_array(&mut self, values: &[f64]) -> &mut Self {
        for value in values {
            self.f64(*value);
        }
        self
    }

    fn finish(&self) -> Vec<u8> {
        // CDR_LE: the second byte's low bit is the endianness flag.
        let mut payload = vec![0x00, 0x01, 0x00, 0x00];
        payload.extend_from_slice(&self.body);
        payload
    }
}

/// Turns dimos LCM wire bytes into the ROS 2 CDR payload for the same type.
///
/// The inverse of `to_lcm`, for the recording path: Foxglove has no LCM decoder,
/// so a capture is only readable there if it goes to disk as CDR. Fields are
/// written in schema order, which is the order `to_lcm` reads them in.
///
/// ROS 2's Header has no `seq`, so the LCM `header.seq` is dropped.
///
/// The returned `msg_name` is usually the one passed in, but a `sensor_msgs.Image`
/// that is really carrying a codec stream comes back as `sensor_msgs.CompressedImage`
/// — see `container_format`.
pub fn to_cdr<'a>(msg_name: &'a str, wire: &[u8]) -> Result<Encoded<'a>> {
    let mut defect = None;
    if msg_name == "sensor_msgs.Image" {
        let image = sensor_msgs::Image::decode(wire)?;
        match container_format(&image) {
            Container::Raw => {}
            Container::Unrecognised => {
                defect = Some(format!(
                    "encoding=\"{}\" height={} step={} len={} is neither raw pixels \
                     nor a container we recognise; forwarded as-is",
                    image.encoding,
                    image.height,
                    image.step,
                    image.data.len(),
                ));
            }
            Container::Format(format) => {
                let mut writer = Writer::default();
                put_compressed_image(
                    &mut writer,
                    &sensor_msgs::CompressedImage {
                        header: image.header,
                        format: format.into(),
                        data: image.data,
                    },
                );
                return Ok(Encoded {
                    msg_name: "sensor_msgs.CompressedImage",
                    data: writer.finish(),
                    defect: None,
                });
            }
        }
    }
    let mut writer = Writer::default();
    match msg_name {
        "geometry_msgs.PoseStamped" => {
            put_pose_stamped(&mut writer, &geometry_msgs::PoseStamped::decode(wire)?)
        }
        "geometry_msgs.TransformStamped" => {
            put_transform_stamped(&mut writer, &geometry_msgs::TransformStamped::decode(wire)?)
        }
        "nav_msgs.Odometry" => put_odometry(&mut writer, &nav_msgs::Odometry::decode(wire)?),
        "nav_msgs.Path" => put_path(&mut writer, &nav_msgs::Path::decode(wire)?),
        "sensor_msgs.CameraInfo" => {
            put_camera_info(&mut writer, &sensor_msgs::CameraInfo::decode(wire)?)
        }
        "sensor_msgs.CompressedImage" => {
            put_compressed_image(&mut writer, &sensor_msgs::CompressedImage::decode(wire)?)
        }
        "sensor_msgs.Image" => put_image(&mut writer, &sensor_msgs::Image::decode(wire)?),
        "sensor_msgs.Imu" => put_imu(&mut writer, &sensor_msgs::Imu::decode(wire)?),
        "sensor_msgs.JointState" => {
            put_joint_state(&mut writer, &sensor_msgs::JointState::decode(wire)?)
        }
        "sensor_msgs.LaserScan" => {
            put_laser_scan(&mut writer, &sensor_msgs::LaserScan::decode(wire)?)
        }
        "sensor_msgs.PointCloud2" => {
            put_point_cloud2(&mut writer, &sensor_msgs::PointCloud2::decode(wire)?)
        }
        "tf2_msgs.TFMessage" => put_tf_message(&mut writer, &tf2_msgs::TFMessage::decode(wire)?),
        other => bail!("no CDR encoder for {other}"),
    }
    Ok(Encoded { msg_name, data: writer.finish(), defect })
}

/// A CDR body and the ROS type it actually ended up as.
#[derive(Debug, PartialEq, Eq)]
pub struct Encoded<'a> {
    pub msg_name: &'a str,
    pub data: Vec<u8>,
    /// Set when the payload was written out in a shape a viewer cannot render,
    /// because guessing would be worse. Counted and reported rather than hidden.
    pub defect: Option<String>,
}

/// The container an `Image` is really carrying, if it is not raw pixels.
///
/// dimos's `JpegLcmTransport` ships a whole jpeg inside a `sensor_msgs.Image`, and
/// writing that back out as an Image claims a codec stream is a pixel layout —
/// Foxglove's image panel silently refuses it and the panel stays blank forever.
///
/// The size invariant decides first: a conformant raw frame is exactly
/// `height * step` bytes and a codec stream essentially never is. Magic bytes
/// alone would not do, because a saturated mono8 row can genuinely open `ff d8 ff`.
fn container_format(image: &sensor_msgs::Image) -> Container {
    if !matches!(image.encoding.as_str(), "jpeg" | "jpg" | "png") {
        return Container::Raw;
    }
    let (Ok(height), Ok(step)) =
        (usize::try_from(image.height), usize::try_from(image.step))
    else {
        return Container::Raw;
    };
    if step != 0 && height.checked_mul(step) == Some(image.data.len()) {
        return Container::Raw;
    }
    match image.data.as_slice() {
        [0xFF, 0xD8, 0xFF, ..] => Container::Format("jpeg"),
        [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, ..] => Container::Format("png"),
        _ => Container::Unrecognised,
    }
}

enum Container {
    /// Ordinary pixels, or a shape we have no reason to doubt.
    Raw,
    Format(&'static str),
    /// `encoding` names a codec and the byte count rules out raw pixels, but no
    /// magic matches. Guessing a container here would corrupt the stream, so the
    /// message goes out unchanged and the caller reports it.
    Unrecognised,
}

fn put_header(writer: &mut Writer, header: &std_msgs::Header) {
    writer.i32(header.stamp.sec).i32(header.stamp.nsec).string(&header.frame_id);
}

fn put_vector3(writer: &mut Writer, value: &geometry_msgs::Vector3) {
    writer.f64(value.x).f64(value.y).f64(value.z);
}

fn put_point(writer: &mut Writer, value: &geometry_msgs::Point) {
    writer.f64(value.x).f64(value.y).f64(value.z);
}

fn put_quaternion(writer: &mut Writer, value: &geometry_msgs::Quaternion) {
    writer.f64(value.x).f64(value.y).f64(value.z).f64(value.w);
}

fn put_pose(writer: &mut Writer, value: &geometry_msgs::Pose) {
    put_point(writer, &value.position);
    put_quaternion(writer, &value.orientation);
}

fn put_twist(writer: &mut Writer, value: &geometry_msgs::Twist) {
    put_vector3(writer, &value.linear);
    put_vector3(writer, &value.angular);
}

fn put_pose_stamped(writer: &mut Writer, value: &geometry_msgs::PoseStamped) {
    put_header(writer, &value.header);
    put_pose(writer, &value.pose);
}

fn put_transform_stamped(writer: &mut Writer, value: &geometry_msgs::TransformStamped) {
    put_header(writer, &value.header);
    writer.string(&value.child_frame_id);
    put_vector3(writer, &value.transform.translation);
    put_quaternion(writer, &value.transform.rotation);
}

fn put_tf_message(writer: &mut Writer, value: &tf2_msgs::TFMessage) {
    writer.count(value.transforms.len());
    for transform in &value.transforms {
        put_transform_stamped(writer, transform);
    }
}

fn put_odometry(writer: &mut Writer, value: &nav_msgs::Odometry) {
    put_header(writer, &value.header);
    writer.string(&value.child_frame_id);
    put_pose(writer, &value.pose.pose);
    writer.f64_array(&value.pose.covariance);
    put_twist(writer, &value.twist.twist);
    writer.f64_array(&value.twist.covariance);
}

fn put_path(writer: &mut Writer, value: &nav_msgs::Path) {
    put_header(writer, &value.header);
    writer.count(value.poses.len());
    for pose in &value.poses {
        put_pose_stamped(writer, pose);
    }
}

fn put_imu(writer: &mut Writer, value: &sensor_msgs::Imu) {
    put_header(writer, &value.header);
    put_quaternion(writer, &value.orientation);
    writer.f64_array(&value.orientation_covariance);
    put_vector3(writer, &value.angular_velocity);
    writer.f64_array(&value.angular_velocity_covariance);
    put_vector3(writer, &value.linear_acceleration);
    writer.f64_array(&value.linear_acceleration_covariance);
}

fn put_image(writer: &mut Writer, value: &sensor_msgs::Image) {
    put_header(writer, &value.header);
    writer
        .i32(value.height)
        .i32(value.width)
        .string(&value.encoding)
        .u8(value.is_bigendian)
        .i32(value.step)
        .bytes(&value.data);
}

fn put_compressed_image(writer: &mut Writer, value: &sensor_msgs::CompressedImage) {
    put_header(writer, &value.header);
    writer.string(&value.format).bytes(&value.data);
}

fn put_camera_info(writer: &mut Writer, value: &sensor_msgs::CameraInfo) {
    put_header(writer, &value.header);
    writer
        .i32(value.height)
        .i32(value.width)
        .string(&value.distortion_model)
        .f64_sequence(&value.D)
        .f64_array(&value.K)
        .f64_array(&value.R)
        .f64_array(&value.P)
        .i32(value.binning_x)
        .i32(value.binning_y)
        .i32(value.roi.x_offset)
        .i32(value.roi.y_offset)
        .i32(value.roi.height)
        .i32(value.roi.width)
        .bool(value.roi.do_rectify);
}

fn put_point_cloud2(writer: &mut Writer, value: &sensor_msgs::PointCloud2) {
    put_header(writer, &value.header);
    writer.i32(value.height).i32(value.width).count(value.fields.len());
    for field in &value.fields {
        writer.string(&field.name).i32(field.offset).u8(field.datatype).i32(field.count);
    }
    writer
        .bool(value.is_bigendian)
        .i32(value.point_step)
        .i32(value.row_step)
        .bytes(&value.data)
        .bool(value.is_dense);
}

fn put_joint_state(writer: &mut Writer, value: &sensor_msgs::JointState) {
    put_header(writer, &value.header);
    writer.count(value.name.len());
    for name in &value.name {
        writer.string(name);
    }
    writer
        .f64_sequence(&value.position)
        .f64_sequence(&value.velocity)
        .f64_sequence(&value.effort);
}

fn put_laser_scan(writer: &mut Writer, value: &sensor_msgs::LaserScan) {
    put_header(writer, &value.header);
    writer
        .f32(value.angle_min)
        .f32(value.angle_max)
        .f32(value.angle_increment)
        .f32(value.time_increment)
        .f32(value.scan_time)
        .f32(value.range_min)
        .f32(value.range_max)
        .f32_sequence(&value.ranges)
        .f32_sequence(&value.intensities);
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

    /// The encoder has to write exactly the layout the decoder reads, so a CDR
    /// payload that survives a trip through LCM and back has to come out byte
    /// for byte identical. Each case is built by hand rather than derived from
    /// the encoder, so a matching mistake in both directions cannot hide here.
    fn round_trips(msg_name: &str, cdr: &[u8]) {
        let wire = to_lcm(msg_name, cdr).unwrap_or_else(|e| panic!("{msg_name} to_lcm: {e}"));
        let again = to_cdr(msg_name, &wire).unwrap_or_else(|e| panic!("{msg_name} to_cdr: {e}"));
        assert_eq!(again.msg_name, msg_name, "{msg_name} was retyped");
        assert_eq!(again.data, cdr, "{msg_name} did not survive the round trip");
    }

    /// Fills the covariance-style fixed arrays with distinguishable values, so
    /// a writer that emitted them in the wrong order would not still pass.
    fn ramp(writer: &mut Writer, count: usize, start: f64) {
        for step in 0..count {
            writer.f64(start + step as f64);
        }
    }

    #[test]
    fn pose_stamped_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 100, 200, "odom");
        ramp(&mut writer, 7, 1.0);
        round_trips("geometry_msgs.PoseStamped", &writer.finish());
    }

    #[test]
    fn transform_stamped_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 1, 2, "base_link").string("camera_link");
        ramp(&mut writer, 7, 3.0);
        round_trips("geometry_msgs.TransformStamped", &writer.finish());
    }

    #[test]
    fn tf_message_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        writer.count(2);
        with_header(&mut writer, 1, 2, "world").string("base");
        ramp(&mut writer, 7, 1.0);
        with_header(&mut writer, 3, 4, "base").string("lidar");
        ramp(&mut writer, 7, 8.0);
        round_trips("tf2_msgs.TFMessage", &writer.finish());
    }

    /// The two 36-value covariance blocks are the easiest thing to transpose.
    #[test]
    fn odometry_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 11, 12, "odom").string("base_link");
        ramp(&mut writer, 7, 1.0);
        ramp(&mut writer, 36, 100.0);
        ramp(&mut writer, 6, 50.0);
        ramp(&mut writer, 36, 200.0);
        round_trips("nav_msgs.Odometry", &writer.finish());
    }

    #[test]
    fn path_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 5, 6, "map");
        writer.count(2);
        with_header(&mut writer, 7, 8, "map");
        ramp(&mut writer, 7, 1.0);
        with_header(&mut writer, 9, 10, "map");
        ramp(&mut writer, 7, 20.0);
        round_trips("nav_msgs.Path", &writer.finish());
    }

    #[test]
    fn imu_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 13, 14, "imu_link");
        ramp(&mut writer, 4, 1.0);
        ramp(&mut writer, 9, 10.0);
        ramp(&mut writer, 3, 20.0);
        ramp(&mut writer, 9, 30.0);
        ramp(&mut writer, 3, 40.0);
        ramp(&mut writer, 9, 50.0);
        round_trips("sensor_msgs.Imu", &writer.finish());
    }

    #[test]
    fn image_round_trips_back_to_the_same_cdr() {
        let pixels: Vec<u8> = (0..48).collect();
        let mut writer = Writer::default();
        with_header(&mut writer, 7, 8, "camera");
        writer.i32(4).i32(4).string("rgb8").u8(0).i32(12).bytes(&pixels);
        round_trips("sensor_msgs.Image", &writer.finish());
    }

    #[test]
    fn compressed_image_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 15, 16, "camera");
        writer.string("jpeg").bytes(&[0xff, 0xd8, 0xff, 0xe0, 0x00]);
        round_trips("sensor_msgs.CompressedImage", &writer.finish());
    }

    /// Builds an LCM Image with the given shape and runs it back through the
    /// recording encoder, which is where the raw-or-container question is settled.
    fn recorded_image(encoding: &str, height: i32, step: i32, data: &[u8]) -> Encoded<'static> {
        let mut writer = Writer::default();
        with_header(&mut writer, 3, 4, "camera");
        writer.i32(height).i32(640).string(encoding).u8(0).i32(step).bytes(data);
        let wire = to_lcm("sensor_msgs.Image", &writer.finish()).expect("to_lcm");
        to_cdr("sensor_msgs.Image", &wire).expect("to_cdr")
    }

    /// `JpegLcmTransport` ships a whole jpeg inside a `sensor_msgs.Image`. Recording
    /// that verbatim hands Foxglove an Image whose `encoding` names a codec rather
    /// than a pixel layout, and its image panel silently refuses to draw it.
    #[test]
    fn a_codec_carrying_image_is_recorded_as_a_compressed_image() {
        let jpeg = [0xFFu8, 0xD8, 0xFF, 0xE0, 0x00, 0x10];
        let encoded = recorded_image("jpeg", 480, 0, &jpeg);
        assert_eq!(encoded.msg_name, "sensor_msgs.CompressedImage");
        let wire = to_lcm("sensor_msgs.CompressedImage", &encoded.data).expect("to_lcm");
        let decoded = sensor_msgs::CompressedImage::decode(&wire).expect("decode");
        assert_eq!(decoded.format, "jpeg");
        assert_eq!(decoded.data, jpeg, "the codec stream must pass through untouched");

        let png = [0x89u8, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00];
        let encoded = recorded_image("png", 480, 0, &png);
        assert_eq!(encoded.msg_name, "sensor_msgs.CompressedImage");
        let wire = to_lcm("sensor_msgs.CompressedImage", &encoded.data).expect("to_lcm");
        assert_eq!(sensor_msgs::CompressedImage::decode(&wire).expect("decode").format, "png");
    }

    /// A saturated mono8 row can genuinely open with the jpeg magic, so the size
    /// invariant has to outrank the sniff or a bright scene gets mislabelled and
    /// its pixels are handed to Foxglove as a jpeg that will never decode.
    #[test]
    fn a_full_size_raw_frame_stays_an_image_despite_jpeg_magic() {
        let pixels = [0xFFu8, 0xD8, 0xFF, 0x01, 0x02, 0x03];
        assert_eq!(recorded_image("jpeg", 2, 3, &pixels).msg_name, "sensor_msgs.Image");
    }

    /// A raw frame whose `encoding` is an ordinary pixel layout is never sniffed
    /// at all, so it cannot be retyped no matter what its first bytes look like.
    #[test]
    fn a_pixel_layout_encoding_is_never_treated_as_a_container() {
        let pixels = [0xFFu8, 0xD8, 0xFF, 0x01, 0x02, 0x03];
        assert_eq!(recorded_image("mono8", 480, 0, &pixels).msg_name, "sensor_msgs.Image");
    }

    /// `encoding` names a codec and the byte count rules out raw pixels, but no
    /// magic matches. Guessing a container would corrupt the stream, so the frame
    /// goes out unchanged — and, because it decodes cleanly and only fails at the
    /// point somebody tries to draw it, nothing downstream would ever notice.
    #[test]
    fn an_unidentifiable_container_is_forwarded_but_reported() {
        let mystery = [0x00u8, 0x01, 0x02, 0x03];
        let encoded = recorded_image("jpeg", 480, 0, &mystery);
        assert_eq!(encoded.msg_name, "sensor_msgs.Image", "we must not guess a container");
        let defect = encoded.defect.expect("an unrenderable frame has to be reported");
        assert!(defect.contains("encoding=\"jpeg\""), "{defect}");
        assert!(defect.contains("step=0"), "{defect}");
        assert!(defect.contains("len=4"), "{defect}");

        let pixels = [0xFFu8, 0xD8, 0xFF, 0x01, 0x02, 0x03];
        assert_eq!(recorded_image("jpeg", 2, 3, &pixels).defect, None, "a raw frame is fine");
        assert_eq!(recorded_image("mono8", 480, 0, &pixels).defect, None, "a raw frame is fine");
        assert_eq!(recorded_image("jpeg", 480, 0, &[0xFF, 0xD8, 0xFF]).defect, None);
    }

    /// Two real frames off Alfred's colour camera, taken from `color_image` in
    /// `web_ctrl_1787719739.mcap` (the smallest and the median of its 2361). They
    /// carry the genuine `JpegLcmTransport` defect — `encoding: "jpeg"`, `step: 0`,
    /// a full jpeg in `data` — which no synthetic frame can vouch for. Ingesting
    /// mcap runs them through `to_lcm` first, so this is the exact byte path a
    /// `--record` of that file takes, minus LCM itself.
    #[test]
    fn real_jpeg_frames_are_recorded_as_compressed_images() {
        let frames: [&[u8]; 2] = [
            include_bytes!("testdata/jpeg_image_a.cdr"),
            include_bytes!("testdata/jpeg_image_b.cdr"),
        ];
        for (index, cdr) in frames.iter().enumerate() {
            let wire = to_lcm("sensor_msgs.Image", cdr).expect("to_lcm");
            let image = sensor_msgs::Image::decode(&wire).expect("decode Image");
            assert_eq!(image.encoding, "jpeg", "frame {index} lost its encoding");
            assert_eq!(image.step, 0, "frame {index} is not the codec-carrying shape");

            let encoded = to_cdr("sensor_msgs.Image", &wire).expect("to_cdr");
            assert_eq!(encoded.msg_name, "sensor_msgs.CompressedImage", "frame {index}");
            let wire = to_lcm("sensor_msgs.CompressedImage", &encoded.data).expect("to_lcm");
            let decoded = sensor_msgs::CompressedImage::decode(&wire).expect("decode");
            assert_eq!(decoded.format, "jpeg", "frame {index}");
            assert_eq!(decoded.data, image.data, "frame {index} jpeg was altered");
            assert!(
                decoded.data.starts_with(&[0xFF, 0xD8, 0xFF])
                    && decoded.data.ends_with(&[0xFF, 0xD9]),
                "frame {index} is not a whole jpeg after the round trip",
            );
        }
    }

    /// `D` is a sequence while `K`, `R` and `P` are fixed arrays of three
    /// different lengths, so a writer that confused the two would be caught.
    #[test]
    fn camera_info_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 17, 18, "camera_optical");
        writer.i32(480).i32(640).string("plumb_bob");
        writer.count(5);
        ramp(&mut writer, 5, 0.5);
        ramp(&mut writer, 9, 10.0);
        ramp(&mut writer, 9, 30.0);
        ramp(&mut writer, 12, 50.0);
        writer.i32(1).i32(2).i32(3).i32(4).i32(5).i32(6).u8(1);
        round_trips("sensor_msgs.CameraInfo", &writer.finish());
    }

    #[test]
    fn point_cloud2_round_trips_back_to_the_same_cdr() {
        let points: Vec<u8> = (0..24).collect();
        let mut writer = Writer::default();
        with_header(&mut writer, 11, 12, "lidar");
        writer.i32(1).i32(2).count(2);
        writer.string("x").i32(0).u8(7).i32(1);
        writer.string("intensity").i32(12).u8(7).i32(1);
        writer.u8(0).i32(12).i32(24).bytes(&points).u8(1);
        round_trips("sensor_msgs.PointCloud2", &writer.finish());
    }

    #[test]
    fn laser_scan_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 21, 22, "laser");
        for value in [-1.5f32, 1.5, 0.25, 0.0, 0.1, 0.05, 30.0] {
            writer.f32(value);
        }
        writer.count(3).f32(1.0).f32(2.0).f32(3.0);
        writer.count(2).f32(9.0).f32(8.0);
        round_trips("sensor_msgs.LaserScan", &writer.finish());
    }

    #[test]
    fn joint_state_round_trips_back_to_the_same_cdr() {
        let mut writer = Writer::default();
        with_header(&mut writer, 31, 32, "body");
        writer.count(2).string("hip").string("knee");
        writer.count(2).f64(0.5).f64(-0.25);
        writer.count(2).f64(1.5).f64(-1.25);
        writer.count(0);
        round_trips("sensor_msgs.JointState", &writer.finish());
    }

    /// Every type `supports()` claims has to have both directions and a schema,
    /// so adding one to the list without wiring it up fails here rather than
    /// silently recording an unreadable channel.
    #[test]
    fn every_supported_type_can_be_encoded_and_has_a_schema() {
        for msg_name in [
            "geometry_msgs.PoseStamped",
            "geometry_msgs.TransformStamped",
            "nav_msgs.Odometry",
            "nav_msgs.Path",
            "sensor_msgs.CameraInfo",
            "sensor_msgs.CompressedImage",
            "sensor_msgs.Image",
            "sensor_msgs.Imu",
            "sensor_msgs.JointState",
            "sensor_msgs.LaserScan",
            "sensor_msgs.PointCloud2",
            "tf2_msgs.TFMessage",
        ] {
            assert!(supports(msg_name), "{msg_name} dropped out of supports()");
            assert!(crate::schema::schema_text(msg_name).is_some(), "{msg_name} has no schema");
            assert!(crate::schema::schema_name(msg_name).is_some(), "{msg_name} has no name");
            // An empty payload is not encodable, but "no encoder" is a
            // different error and is the one this is looking for.
            let complaint = to_cdr(msg_name, &[]).unwrap_err().to_string();
            assert!(!complaint.contains("no CDR encoder"), "{msg_name} has no encoder");
        }
    }

    #[test]
    fn an_unsupported_type_is_an_error_in_both_directions() {
        assert!(to_cdr("unitree_go.LowState", &[1, 2, 3]).is_err());
        assert!(to_lcm("unitree_go.LowState", &[1, 2, 3]).is_err());
    }

    #[test]
    fn truncated_payloads_are_an_error_rather_than_a_panic() {
        let payload = vec![0x00, 0x01, 0x00, 0x00, 1, 2, 3];
        assert!(to_lcm("sensor_msgs.Imu", &payload).is_err());
    }
}
