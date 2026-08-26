//! Capturing live LCM or Zenoh traffic into an `.mcap`.
//!
//! This is the write side of the convention `source::McapSource` reads: one
//! channel per stream, LCM wire bytes as the payload, the python type in
//! `dimos.payload_type` and the stream name in `dimos.stream_name`.
//!
//! Recording is live: it takes what is on the transport, not what is in a file.
//! A replay that records at the same time therefore hears its own publishes come
//! back, and those are dropped — see `SelfPublished`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use tokio::sync::mpsc;

use crate::sink::{Sink, Transport};
use crate::stamp::{self, Support};

/// Free space below which recording is likely to end badly. A warning, not a
/// refusal: how much a capture needs depends entirely on which streams it holds.
const LOW_DISK_BYTES: u64 = 6 * 1024 * 1024 * 1024;

/// How the payload bytes of one stream are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Codec {
    /// ROS 2 CDR beside its `ros2msg` schema. The only encoding general tools
    /// read: Foxglove has no LCM decoder and no message definitions of its own,
    /// so an LCM channel shows up there as an undecodable blob.
    Cdr,
    /// Raw LCM wire bytes, exactly as they arrived.
    Lcm,
    /// LZ4 frame around the wire bytes, and unreadable outside dimos and `rrr`.
    ///
    /// Only worth it with `--record-compression none`: measured over 248 Mid-360
    /// clouds it saves 6.6% against raw LCM uncompressed, but *costs* 6.3% under
    /// the default zstd chunks, because a compressed payload leaves the chunk
    /// compressor nothing to find.
    #[value(name = "lz4+lcm")]
    Lz4Lcm,
}

impl Codec {
    fn id(self) -> &'static str {
        match self {
            Codec::Cdr => "cdr",
            Codec::Lcm => "lcm",
            Codec::Lz4Lcm => "lz4+lcm",
        }
    }

    /// What this codec becomes for a stream carrying `msg_name`.
    ///
    /// CDR only exists for types with a transcoder, so a stream of anything
    /// else — a `unitree_go` message, or a stream whose type never turned up —
    /// is stored as raw LCM rather than dropped.
    fn resolve(self, msg_name: &str) -> Codec {
        match self {
            Codec::Cdr if !crate::cdr::supports(msg_name) => Codec::Lcm,
            resolved => resolved,
        }
    }

    /// Returns the payload, the type it ended up as — only ever different from
    /// `msg_name` for a jpeg-carrying Image, see `cdr::to_cdr` — and a note when
    /// the payload had to be written in a shape a viewer cannot render.
    fn encode(self, msg_name: &str, wire: Vec<u8>) -> Result<(&str, Vec<u8>, Option<String>)> {
        match self {
            Codec::Cdr => {
                let encoded = crate::cdr::to_cdr(msg_name, &wire)?;
                Ok((encoded.msg_name, encoded.data, encoded.defect))
            }
            Codec::Lcm => Ok((msg_name, wire, None)),
            Codec::Lz4Lcm => {
                use std::io::Write;
                let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
                encoder.write_all(&wire)?;
                Ok((msg_name, encoder.finish().context("lz4 compression failed")?, None))
            }
        }
    }
}

/// How each chunk of the file is compressed, on top of the per-stream codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Compression {
    Zstd,
    Lz4,
    None,
}

impl Compression {
    fn to_mcap(self) -> Option<mcap::Compression> {
        match self {
            Compression::Zstd => Some(mcap::Compression::Zstd),
            Compression::Lz4 => Some(mcap::Compression::Lz4),
            Compression::None => None,
        }
    }
}

/// Which stream names are recorded.
///
/// `--record` is the allow list and `--record-all-but` the deny list; a name has
/// to pass both. An empty allow list means every stream, so `--record-all-but`
/// on its own reads as "everything except these".
pub struct Selection {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl Selection {
    pub fn new(include: &[String], exclude: &[String]) -> Result<Self> {
        Ok(Self { include: parse_names(include)?, exclude: parse_names(exclude)? })
    }

    pub fn wants(&self, name: &str) -> bool {
        let allowed = self.include.is_empty() || self.include.iter().any(|p| matches(p, name));
        allowed && !self.exclude.iter().any(|p| matches(p, name))
    }
}

/// Matches `-s/--stream`'s spelling, so one habit covers both flags.
fn matches(pattern: &str, name: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => name == pattern,
    }
}

/// Accepts the JSON array from the ask — `--record '["color_image"]'` — and the
/// comma-separated and repeated-flag spellings people reach for anyway.
fn parse_names(specs: &[String]) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for spec in specs {
        let spec = spec.trim();
        if spec.starts_with('[') {
            let parsed: Vec<String> = serde_json::from_str(spec)
                .with_context(|| format!("{spec} is not a JSON array of stream names"))?;
            names.extend(parsed);
        } else {
            names.extend(
                spec.split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_string),
            );
        }
    }
    Ok(names)
}

/// A message that passed selection and is on its way to the file.
struct Captured {
    stream: String,
    msg_name: String,
    payload: Vec<u8>,
    log_time_ns: i64,
    publish_time_ns: i64,
}

/// The `(channel, stamp)` pairs this process has just put on the wire.
///
/// Both transports deliver to their own subscribers, so a replay that also
/// records would otherwise write its own output straight back into the file.
/// The key is the payload's timestamp rather than a hash of the bytes: hashing
/// every frame of a 400 KB image stream costs more than the recording does.
///
/// Entries expire, because a stamp only has to survive the loopback trip. That
/// leaves a real message with the same stamp on the same channel — a second
/// publisher echoing the replay — indistinguishable from our own. Recording a
/// topic you are also replaying is asking a narrower question than it looks.
struct SelfPublished {
    seen: HashSet<(String, i64)>,
    order: VecDeque<(Instant, String, i64)>,
}

const LOOPBACK_TTL: Duration = Duration::from_secs(10);

impl SelfPublished {
    fn new() -> Self {
        Self { seen: HashSet::new(), order: VecDeque::new() }
    }

    fn note(&mut self, channel: &str, stamp_ns: i64) {
        self.expire();
        if self.seen.insert((channel.to_string(), stamp_ns)) {
            self.order.push_back((Instant::now(), channel.to_string(), stamp_ns));
        }
    }

    fn claims(&mut self, channel: &str, stamp_ns: i64) -> bool {
        self.expire();
        self.seen.contains(&(channel.to_string(), stamp_ns))
    }

    fn expire(&mut self) {
        while let Some((at, ..)) = self.order.front() {
            if at.elapsed() < LOOPBACK_TTL {
                break;
            }
            let (_, channel, stamp_ns) = self.order.pop_front().expect("head checked above");
            self.seen.remove(&(channel, stamp_ns));
        }
    }
}

pub struct Recorder {
    ours: Arc<Mutex<SelfPublished>>,
    /// Holds the other end of the writer's queue. The subscriber task holds one
    /// too, so both have to go before the writer sees the queue close.
    captured: Option<mpsc::UnboundedSender<Captured>>,
    subscriber: tokio::task::JoinHandle<()>,
    writer: tokio::task::JoinHandle<Result<Written>>,
    path: PathBuf,
}

#[derive(Default)]
struct Written {
    messages: u64,
    by_stream: BTreeMap<String, u64>,
    /// Streams whose payloads would not transcode, with how many were lost and
    /// why the first one was. Silence here would look like a quiet stream.
    unencodable: BTreeMap<String, (u64, String)>,
    /// Streams that were written but in a shape a viewer cannot draw. These are
    /// in the file and decode fine, so nothing else would ever flag them.
    unrenderable: BTreeMap<String, (u64, String)>,
}

impl Recorder {
    /// Subscribes to everything on the transport and starts writing.
    ///
    /// `prefix` is the namespace the graph publishes under, and is stripped from
    /// each channel to recover the bare stream name.
    pub async fn start(
        sink: &Sink,
        prefix: &str,
        path: &Path,
        selection: Selection,
        compression: Compression,
        codecs: HashMap<String, Codec>,
        default_codec: Codec,
    ) -> Result<Self> {
        if let Some(free) = free_bytes(path) {
            if free < LOW_DISK_BYTES {
                eprintln!(
                    "warning: only {:.1} GB free where {} is being written; \
                     a recording can fill that in minutes",
                    free as f64 / 1e9,
                    path.display()
                );
            }
        }

        let file = std::fs::File::create(path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        // The profile names what a reader should expect to find. Channels are
        // typed one at a time, so a capture holding one untranscodable stream
        // is still a ros2 file everywhere it counts.
        let ros2 = codecs.values().chain([&default_codec]).any(|c| *c == Codec::Cdr);
        let mut writer = mcap::WriteOptions::new()
            .profile(if ros2 { "ros2" } else { "dimos" })
            .compression(compression.to_mcap())
            .create(BufWriter::new(file))?;

        let (sender, mut receiver) = mpsc::unbounded_channel::<Captured>();
        // The mcap writer is synchronous and compresses on the calling thread,
        // so it gets a thread of its own rather than a slice of the runtime.
        let writing = tokio::task::spawn_blocking(move || {
            let mut written = Written::default();
            let mut channels: HashMap<String, (u16, String, u32)> = HashMap::new();
            while let Some(message) = receiver.blocking_recv() {
                let codec = codecs
                    .get(&message.stream)
                    .copied()
                    .unwrap_or(default_codec)
                    .resolve(&message.msg_name);
                // Encoding comes first because it is what settles the recorded type:
                // a jpeg-carrying Image becomes a CompressedImage, and the channel's
                // schema has to be the one the payloads actually match.
                //
                // One malformed payload should cost that message, not the rest of
                // the capture — the file is only readable once finalised.
                let (recorded, data, defect) = match codec.encode(&message.msg_name, message.payload)
                {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        let seen = written
                            .unencodable
                            .entry(message.stream)
                            .or_insert((0, error.to_string()));
                        seen.0 += 1;
                        continue;
                    }
                };
                if let Some(why) = defect {
                    let seen =
                        written.unrenderable.entry(message.stream.clone()).or_insert((0, why));
                    seen.0 += 1;
                }
                let (id, sequence) = match channels.get_mut(&message.stream) {
                    Some((id, registered, sequence)) => {
                        if registered != recorded {
                            let seen = written.unencodable.entry(message.stream).or_insert((
                                0,
                                format!("stream changed type to {recorded} mid-capture"),
                            ));
                            seen.0 += 1;
                            continue;
                        }
                        *sequence += 1;
                        (*id, *sequence)
                    }
                    None => {
                        let id = writer.add_channel(&mcap::Channel {
                            topic: message.stream.clone(),
                            schema: schema_for(codec, recorded),
                            message_encoding: codec.id().to_string(),
                            metadata: channel_metadata(&message.stream, recorded),
                        })?;
                        channels.insert(message.stream.clone(), (id, recorded.to_string(), 1));
                        (id, 1)
                    }
                };
                writer.write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id: id,
                        sequence,
                        log_time: message.log_time_ns.max(0) as u64,
                        publish_time: message.publish_time_ns.max(0) as u64,
                    },
                    &data,
                )?;
                written.messages += 1;
                *written.by_stream.entry(message.stream).or_default() += 1;
            }
            writer.finish()?;
            Ok(written)
        });

        let ours = Arc::new(Mutex::new(SelfPublished::new()));
        let mut arrivals = sink.subscribe_all(prefix).await?;
        let separator = separator(sink.transport());
        let (prefix, filter, mine) =
            (prefix.to_string(), sender.clone(), Arc::clone(&ours));
        let subscriber = tokio::spawn(async move {
            while let Some((channel, payload)) = arrivals.recv().await {
                let log_time_ns = stamp::seconds_to_nanos(unix_now());
                let (stream, msg_name) = split_channel(&channel, &prefix, separator);
                if !selection.wants(stream) {
                    continue;
                }
                let stamp_ns = stamp::stamp_of(stamp::support_for(msg_name), &payload);
                // A type with no stamp field cannot be told apart by time, so
                // the channel alone stands in for it during the loopback window.
                if mine.lock().expect("recorder lock").claims(&channel, stamp_ns.unwrap_or(0)) {
                    continue;
                }
                let sent = filter.send(Captured {
                    stream: stream.to_string(),
                    msg_name: msg_name.to_string(),
                    publish_time_ns: stamp_ns.unwrap_or(log_time_ns),
                    log_time_ns,
                    payload,
                });
                if sent.is_err() {
                    break;
                }
            }
        });

        Ok(Self {
            ours,
            captured: Some(sender),
            subscriber,
            writer: writing,
            path: path.to_path_buf(),
        })
    }

    /// Called for every message the replay publishes, so the recorder can
    /// recognise it on the way back in.
    pub fn note_published(&self, channel: &str, support: Support, payload: &[u8]) {
        let stamp_ns = stamp::stamp_of(support, payload).unwrap_or(0);
        self.ours.lock().expect("recorder lock").note(channel, stamp_ns);
    }

    /// Closes the file and returns what went into it.
    ///
    /// An mcap without its summary section cannot be enumerated at all, so this
    /// has to run even when the replay was interrupted.
    pub async fn finish(mut self) -> Result<String> {
        // Awaited, not just aborted: the subscriber's copy of the queue handle
        // only goes when its future is actually dropped, and until then the
        // writer is waiting for a message that will never come.
        self.subscriber.abort();
        let _ = (&mut self.subscriber).await;
        drop(self.captured.take());
        let written = self.writer.await.context("the recorder thread panicked")??;
        let mut report = format!(
            "recorded {} message(s) to {}\n",
            written.messages,
            self.path.display()
        );
        for (stream, count) in &written.by_stream {
            report.push_str(&format!("  {stream}: {count}\n"));
        }
        for (stream, (count, why)) in &written.unencodable {
            report.push_str(&format!("  {stream}: {count} message(s) dropped, {why}\n"));
        }
        for (stream, (count, why)) in &written.unrenderable {
            report.push_str(&format!("  {stream}: {count} message(s) unrenderable, {why}\n"));
        }
        Ok(report)
    }
}

/// The ROS message definition a CDR channel is decoded with.
///
/// A CDR channel without one is worse than an LCM channel: the bytes are in a
/// format general tools understand but nothing says what the fields are. LCM
/// channels carry no schema because there is no schema language for them —
/// `dimos.payload_type` is what names the type there.
fn schema_for(codec: Codec, msg_name: &str) -> Option<std::sync::Arc<mcap::Schema<'static>>> {
    if codec != Codec::Cdr {
        return None;
    }
    Some(std::sync::Arc::new(mcap::Schema {
        name: crate::schema::schema_name(msg_name)?.to_string(),
        encoding: "ros2msg".to_string(),
        data: crate::schema::schema_text(msg_name)?.as_bytes().into(),
    }))
}

/// The metadata `McapSource::open` reads back, and that dimos's own MCAP store
/// writes: the python module path and the bare stream name.
fn channel_metadata(stream: &str, msg_name: &str) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert("dimos.stream_name".to_string(), stream.to_string());
    if let Some(payload_type) = payload_module(msg_name) {
        metadata.insert("dimos.payload_type".to_string(), payload_type);
    }
    metadata
}

/// `sensor_msgs.Image` -> `dimos.msgs.sensor_msgs.Image.Image`, the inverse of
/// `source::msg_name_from_payload_module`.
fn payload_module(msg_name: &str) -> Option<String> {
    let class = msg_name.rsplit('.').next().filter(|class| !class.is_empty())?;
    Some(format!("dimos.msgs.{msg_name}.{class}"))
}

fn separator(transport: Transport) -> char {
    match transport {
        Transport::Lcm => '#',
        Transport::Zenoh => '/',
    }
}

/// `/color_image#sensor_msgs.Image` -> `("color_image", "sensor_msgs.Image")`.
///
/// The leading `/` is stripped even when it is not the configured prefix: a
/// dimos stream name never starts with one, and carrying it into the file would
/// put it back into every channel and key expression on replay.
fn split_channel<'a>(channel: &'a str, prefix: &str, separator: char) -> (&'a str, &'a str) {
    let rest = channel.strip_prefix(prefix).unwrap_or(channel);
    let rest = rest.strip_prefix('/').unwrap_or(rest);
    match rest.rsplit_once(separator) {
        Some((stream, msg_name)) => (stream, msg_name),
        None => (rest, ""),
    }
}

/// Parses `--record-codec NAME:CODEC`.
pub fn codec_overrides(specs: &[String]) -> Result<HashMap<String, Codec>> {
    let mut codecs = HashMap::new();
    for spec in specs {
        let (name, codec) = spec.split_once(':').with_context(|| {
            format!("--record-codec wants NAME:CODEC, for example lidar:lz4+lcm, not {spec}")
        })?;
        let codec = match codec {
            "cdr" => Codec::Cdr,
            "lcm" => Codec::Lcm,
            "lz4+lcm" => Codec::Lz4Lcm,
            other => bail!("--record-codec {spec}: {other} is not one of cdr, lcm, lz4+lcm"),
        };
        codecs.insert(name.to_string(), codec);
    }
    Ok(codecs)
}

fn unix_now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

/// Free space on the filesystem the recording will land on, or `None` if it
/// cannot be measured — in which case there is nothing worth warning about.
fn free_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let directory = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let directory = std::ffi::CString::new(directory.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(directory.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    (stat.f_bavail as u64).checked_mul(stat.f_frsize as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs(specs: &[&str]) -> Vec<String> {
        specs.iter().map(|spec| (*spec).to_string()).collect()
    }

    /// The spelling from the ask.
    #[test]
    fn record_takes_a_json_array() {
        let selection = Selection::new(&specs(&[r#"["color_image", "lidar"]"#]), &[]).unwrap();
        assert!(selection.wants("color_image"));
        assert!(selection.wants("lidar"));
        assert!(!selection.wants("wheel_odometry"));
    }

    #[test]
    fn record_also_takes_bare_and_comma_separated_names() {
        let selection = Selection::new(&specs(&["color_image,lidar"]), &[]).unwrap();
        assert!(selection.wants("lidar"));
        let selection = Selection::new(&specs(&["color_image", "lidar"]), &[]).unwrap();
        assert!(selection.wants("lidar"));
    }

    #[test]
    fn an_empty_selection_records_everything() {
        let selection = Selection::new(&specs(&["[]"]), &[]).unwrap();
        assert!(selection.wants("anything_at_all"));
    }

    #[test]
    fn record_all_but_is_a_deny_list_over_everything() {
        let selection = Selection::new(&[], &specs(&[r#"["lidar"]"#])).unwrap();
        assert!(selection.wants("color_image"));
        assert!(!selection.wants("lidar"));
    }

    /// The deny list wins, which is what makes `--record 'infrared_*'
    /// --record-all-but infrared_right` a way to say "the pair minus one".
    #[test]
    fn the_deny_list_narrows_the_allow_list() {
        let selection =
            Selection::new(&specs(&["infrared_*"]), &specs(&["infrared_right"])).unwrap();
        assert!(selection.wants("infrared_left"));
        assert!(!selection.wants("infrared_right"));
        assert!(!selection.wants("color_image"));
    }

    #[test]
    fn a_selection_that_is_not_json_is_an_error() {
        assert!(Selection::new(&specs(&["[color_image"]), &[]).is_err());
    }

    #[test]
    fn lcm_and_zenoh_channels_split_back_into_stream_and_type() {
        assert_eq!(
            split_channel("/color_image#sensor_msgs.Image", "/", '#'),
            ("color_image", "sensor_msgs.Image")
        );
        assert_eq!(
            split_channel("dimos/color_image/sensor_msgs.Image", "dimos/", '/'),
            ("color_image", "sensor_msgs.Image")
        );
    }

    /// A module publishing outside the configured namespace still has to be
    /// recorded under a name a replay can publish again.
    #[test]
    fn a_channel_outside_the_prefix_keeps_its_name_without_the_slash() {
        assert_eq!(split_channel("/lidar#sensor_msgs.PointCloud2", "dimos/", '#'), ("lidar", "sensor_msgs.PointCloud2"));
        assert_eq!(split_channel("raw", "/", '#'), ("raw", ""));
    }

    /// What goes into the file has to be what `McapSource` reads back out.
    #[test]
    fn channel_metadata_round_trips_through_the_reader() {
        let metadata = channel_metadata("color_image", "sensor_msgs.Image");
        assert_eq!(metadata["dimos.stream_name"], "color_image");
        assert_eq!(metadata["dimos.payload_type"], "dimos.msgs.sensor_msgs.Image.Image");
    }

    #[test]
    fn a_stream_with_no_known_type_gets_no_payload_type() {
        assert!(!channel_metadata("raw", "").contains_key("dimos.payload_type"));
    }

    #[test]
    fn a_published_message_is_recognised_when_it_loops_back() {
        let mut ours = SelfPublished::new();
        ours.note("/color_image#sensor_msgs.Image", 1_781_260_015_000_000_000);
        assert!(ours.claims("/color_image#sensor_msgs.Image", 1_781_260_015_000_000_000));
        // Same channel, another moment: someone else's message.
        assert!(!ours.claims("/color_image#sensor_msgs.Image", 1_781_260_016_000_000_000));
        // Same moment, another channel.
        assert!(!ours.claims("/lidar#sensor_msgs.PointCloud2", 1_781_260_015_000_000_000));
    }

    #[test]
    fn lz4_codec_output_decodes_back_to_the_wire_bytes() {
        let wire = b"lcm wire bytes, repeated repeated repeated".to_vec();
        let (_, stored, _) = Codec::Lz4Lcm.encode("sensor_msgs.Image", wire.clone()).unwrap();
        assert_ne!(stored, wire);
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(
            &mut lz4_flex::frame::FrameDecoder::new(stored.as_slice()),
            &mut decoded,
        )
        .unwrap();
        assert_eq!(decoded, wire);
        assert_eq!(Codec::Lcm.encode("sensor_msgs.Image", wire.clone()).unwrap().1, wire);
    }

    #[test]
    fn codec_overrides_are_parsed_per_stream() {
        let codecs =
            codec_overrides(&specs(&["lidar:lz4+lcm", "color_image:lcm", "odom:cdr"])).unwrap();
        assert_eq!(codecs["lidar"], Codec::Lz4Lcm);
        assert_eq!(codecs["color_image"], Codec::Lcm);
        assert_eq!(codecs["odom"], Codec::Cdr);
        assert!(codec_overrides(&specs(&["lidar:jpeg"])).is_err());
        assert!(codec_overrides(&specs(&["lidar"])).is_err());
    }

    /// A stream whose type has no transcoder still has to be recorded, just
    /// without the schema that would let a general tool read it.
    #[test]
    fn cdr_falls_back_to_lcm_for_a_type_it_cannot_transcode() {
        assert_eq!(Codec::Cdr.resolve("sensor_msgs.PointCloud2"), Codec::Cdr);
        assert_eq!(Codec::Cdr.resolve("unitree_go.LowState"), Codec::Lcm);
        // A stream whose type never turned up on the wire.
        assert_eq!(Codec::Cdr.resolve(""), Codec::Lcm);
        // An explicit choice is never second-guessed.
        assert_eq!(Codec::Lz4Lcm.resolve("sensor_msgs.PointCloud2"), Codec::Lz4Lcm);
        assert_eq!(Codec::Lcm.resolve("sensor_msgs.PointCloud2"), Codec::Lcm);
    }

    /// The whole point of the CDR path: the channel carries a definition
    /// Foxglove can parse, under the name it matches its renderers on.
    #[test]
    fn a_cdr_channel_carries_its_ros_schema() {
        let schema = schema_for(Codec::Cdr, "sensor_msgs.PointCloud2").expect("a schema");
        assert_eq!(schema.name, "sensor_msgs/msg/PointCloud2");
        assert_eq!(schema.encoding, "ros2msg");
        let text = String::from_utf8(schema.data.to_vec()).unwrap();
        assert!(text.starts_with("std_msgs/Header header"));
        assert!(text.contains("MSG: sensor_msgs/PointField"));
        // ROS 2 spells the stamp as a nested type, not ROS 1's bare `time`.
        assert!(text.contains("builtin_interfaces/Time stamp"));
    }

    #[test]
    fn lcm_channels_carry_no_schema() {
        assert!(schema_for(Codec::Lcm, "sensor_msgs.PointCloud2").is_none());
        assert!(schema_for(Codec::Lz4Lcm, "sensor_msgs.PointCloud2").is_none());
    }

    /// `resolve` runs first, so this can only be reached by asking for CDR on a
    /// type that has one — but a missing schema must never become a CDR channel
    /// with nothing to decode it.
    #[test]
    fn cdr_without_a_schema_is_not_offered_one() {
        assert!(schema_for(Codec::Cdr, "unitree_go.LowState").is_none());
    }
}
