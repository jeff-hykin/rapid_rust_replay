//! ROS 2 `.msg` definitions for the types `cdr::to_cdr` writes.
//!
//! Foxglove decodes a CDR channel by parsing the schema text stored beside it;
//! it carries no ROS message definitions of its own. Each entry is the
//! concatenated form ROS 2 tooling emits: the type itself, then every nested
//! type after a line of `=`.
//!
//! Generated from the `rosbags` ROS 2 Humble typestore (`generate_msgdef`,
//! `ros_version=2`) so the text matches what `db_to_mcap` and rosbag2 write
//! rather than being retyped here.

/// The ROS schema name for a dimos type name, e.g. `sensor_msgs.Image` ->
/// `sensor_msgs/msg/Image`. This is the name Foxglove matches its renderers on.
pub fn schema_name(msg_name: &str) -> Option<&'static str> {
    Some(match msg_name {
        "geometry_msgs.PoseStamped" => "geometry_msgs/msg/PoseStamped",
        "geometry_msgs.TransformStamped" => "geometry_msgs/msg/TransformStamped",
        "nav_msgs.Odometry" => "nav_msgs/msg/Odometry",
        "nav_msgs.Path" => "nav_msgs/msg/Path",
        "sensor_msgs.CameraInfo" => "sensor_msgs/msg/CameraInfo",
        "sensor_msgs.CompressedImage" => "sensor_msgs/msg/CompressedImage",
        "sensor_msgs.Image" => "sensor_msgs/msg/Image",
        "sensor_msgs.Imu" => "sensor_msgs/msg/Imu",
        "sensor_msgs.JointState" => "sensor_msgs/msg/JointState",
        "sensor_msgs.LaserScan" => "sensor_msgs/msg/LaserScan",
        "sensor_msgs.PointCloud2" => "sensor_msgs/msg/PointCloud2",
        "tf2_msgs.TFMessage" => "tf2_msgs/msg/TFMessage",
        _ => return None,
    })
}

/// The `ros2msg` schema text for a dimos type name.
pub fn schema_text(msg_name: &str) -> Option<&'static str> {
    Some(match msg_name {
        "geometry_msgs.PoseStamped" => GEOMETRY_MSGS_POSESTAMPED,
        "geometry_msgs.TransformStamped" => GEOMETRY_MSGS_TRANSFORMSTAMPED,
        "nav_msgs.Odometry" => NAV_MSGS_ODOMETRY,
        "nav_msgs.Path" => NAV_MSGS_PATH,
        "sensor_msgs.CameraInfo" => SENSOR_MSGS_CAMERAINFO,
        "sensor_msgs.CompressedImage" => SENSOR_MSGS_COMPRESSEDIMAGE,
        "sensor_msgs.Image" => SENSOR_MSGS_IMAGE,
        "sensor_msgs.Imu" => SENSOR_MSGS_IMU,
        "sensor_msgs.JointState" => SENSOR_MSGS_JOINTSTATE,
        "sensor_msgs.LaserScan" => SENSOR_MSGS_LASERSCAN,
        "sensor_msgs.PointCloud2" => SENSOR_MSGS_POINTCLOUD2,
        "tf2_msgs.TFMessage" => TF2_MSGS_TFMESSAGE,
        _ => return None,
    })
}

const GEOMETRY_MSGS_POSESTAMPED: &str = r#"std_msgs/Header header
geometry_msgs/Pose pose
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/Pose
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"#;

const GEOMETRY_MSGS_TRANSFORMSTAMPED: &str = r#"std_msgs/Header header
string child_frame_id
geometry_msgs/Transform transform
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/Transform
geometry_msgs/Vector3 translation
geometry_msgs/Quaternion rotation
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"#;

const NAV_MSGS_ODOMETRY: &str = r#"std_msgs/Header header
string child_frame_id
geometry_msgs/PoseWithCovariance pose
geometry_msgs/TwistWithCovariance twist
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/PoseWithCovariance
geometry_msgs/Pose pose
float64[36] covariance
================================================================================
MSG: geometry_msgs/Pose
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
================================================================================
MSG: geometry_msgs/TwistWithCovariance
geometry_msgs/Twist twist
float64[36] covariance
================================================================================
MSG: geometry_msgs/Twist
geometry_msgs/Vector3 linear
geometry_msgs/Vector3 angular
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
"#;

const NAV_MSGS_PATH: &str = r#"std_msgs/Header header
geometry_msgs/PoseStamped[] poses
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/PoseStamped
std_msgs/Header header
geometry_msgs/Pose pose
================================================================================
MSG: geometry_msgs/Pose
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"#;

const SENSOR_MSGS_CAMERAINFO: &str = r#"std_msgs/Header header
uint32 height
uint32 width
string distortion_model
float64[] d
float64[9] k
float64[9] r
float64[12] p
uint32 binning_x
uint32 binning_y
sensor_msgs/RegionOfInterest roi
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: sensor_msgs/RegionOfInterest
uint32 x_offset
uint32 y_offset
uint32 height
uint32 width
bool do_rectify
"#;

const SENSOR_MSGS_COMPRESSEDIMAGE: &str = r#"std_msgs/Header header
string format
uint8[] data
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

const SENSOR_MSGS_IMAGE: &str = r#"std_msgs/Header header
uint32 height
uint32 width
string encoding
uint8 is_bigendian
uint32 step
uint8[] data
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

const SENSOR_MSGS_IMU: &str = r#"std_msgs/Header header
geometry_msgs/Quaternion orientation
float64[9] orientation_covariance
geometry_msgs/Vector3 angular_velocity
float64[9] angular_velocity_covariance
geometry_msgs/Vector3 linear_acceleration
float64[9] linear_acceleration_covariance
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
"#;

const SENSOR_MSGS_JOINTSTATE: &str = r#"std_msgs/Header header
string[] name
float64[] position
float64[] velocity
float64[] effort
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

const SENSOR_MSGS_LASERSCAN: &str = r#"std_msgs/Header header
float32 angle_min
float32 angle_max
float32 angle_increment
float32 time_increment
float32 scan_time
float32 range_min
float32 range_max
float32[] ranges
float32[] intensities
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

const SENSOR_MSGS_POINTCLOUD2: &str = r#"std_msgs/Header header
uint32 height
uint32 width
sensor_msgs/PointField[] fields
bool is_bigendian
uint32 point_step
uint32 row_step
uint8[] data
bool is_dense
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: sensor_msgs/PointField
uint8 INT8=1
uint8 UINT8=2
uint8 INT16=3
uint8 UINT16=4
uint8 INT32=5
uint8 UINT32=6
uint8 FLOAT32=7
uint8 FLOAT64=8
string name
uint32 offset
uint8 datatype
uint32 count
"#;

const TF2_MSGS_TFMESSAGE: &str = r#"geometry_msgs/TransformStamped[] transforms
================================================================================
MSG: geometry_msgs/TransformStamped
std_msgs/Header header
string child_frame_id
geometry_msgs/Transform transform
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/Transform
geometry_msgs/Vector3 translation
geometry_msgs/Quaternion rotation
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"#;
