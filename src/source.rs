//! Reading recorded messages out of a dimos `.db` or `.mcap` in timestamp order.

use std::collections::VecDeque;
use std::path::Path;

use anyhow::{bail, Context, Result};
use rusqlite::Connection;

use crate::stamp::{self, Support};

/// How a stream's stored bytes relate to its LCM wire bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    /// Stored bytes are already the wire bytes (`lcm`, and `jpeg`, which is an
    /// LCM Image whose `encoding` field says `jpeg`).
    Wire,
    /// LZ4 frame around the wire bytes.
    Lz4,
    /// ROS 2 CDR, transcoded to LCM per message.
    Cdr,
    /// Python pickle, or a CDR type with no LCM counterpart — meaningless to an
    /// LCM or Zenoh subscriber.
    Unsupported,
}

fn storage_for(codec_id: &str) -> Storage {
    match codec_id {
        "lcm" | "jpeg" => Storage::Wire,
        "lz4+lcm" | "lz4+jpeg" => Storage::Lz4,
        _ => Storage::Unsupported,
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    /// As recorded: the SQL table name, or the mcap channel topic.
    pub name: String,
    /// The name to publish under. Starts as `name`; `--rename` rewrites it.
    pub published: String,
    /// e.g. `sensor_msgs.Image`; the suffix used in LCM channels and Zenoh keys.
    pub msg_name: String,
    pub storage: Storage,
    pub support: Support,
    pub count: u64,
}

pub struct Record {
    pub stream: usize,
    pub ts: f64,
    pub data: Vec<u8>,
}

/// `dimos.msgs.sensor_msgs.Image.Image` -> `sensor_msgs.Image`.
///
/// dimos names each message module after its single exported class, so dropping
/// the trailing class segment recovers the `msg_name` the transports use.
fn msg_name_from_payload_module(payload_module: &str) -> String {
    let path = payload_module.strip_prefix("dimos.msgs.").unwrap_or(payload_module);
    match path.rsplit_once('.') {
        Some((module, class)) if module.ends_with(class) => module.to_string(),
        _ => path.to_string(),
    }
}

pub enum Source {
    Db(Box<DbSource>),
    Mcap(Box<McapSource>),
}

impl Source {
    pub fn open(path: &Path, selector: &dyn Fn(&str) -> bool) -> Result<Self> {
        let extension = path.extension().and_then(|e| e.to_str()).unwrap_or_default();
        if extension.eq_ignore_ascii_case("mcap") {
            Ok(Source::Mcap(Box::new(McapSource::open(path, selector)?)))
        } else {
            Ok(Source::Db(Box::new(DbSource::open(path, selector)?)))
        }
    }

    pub fn streams(&self) -> &[Stream] {
        match self {
            Source::Db(source) => &source.streams,
            Source::Mcap(source) => &source.streams,
        }
    }

    pub fn next(&mut self) -> Result<Option<Record>> {
        match self {
            Source::Db(source) => source.next(),
            Source::Mcap(source) => source.next(),
        }
    }

    pub fn rewind(&mut self) -> Result<()> {
        match self {
            Source::Db(source) => source.rewind(),
            Source::Mcap(source) => source.rewind(),
        }
    }
}

// ---------------------------------------------------------------- dimos .db

/// Pulls `(id, ts)` a page at a time so a multi-gigabyte recording never has to
/// be indexed in memory; blobs are fetched individually as each message is due.
const INDEX_PAGE: usize = 4096;

struct Cursor {
    stream: usize,
    table: String,
    blob_table: String,
    page: VecDeque<(i64, f64)>,
    last_ts: f64,
    done: bool,
}

pub struct DbSource {
    connection: Connection,
    pub streams: Vec<Stream>,
    cursors: Vec<Cursor>,
}

/// SQLite identifiers are quoted, not bound, so the stream name has to be escaped.
fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

impl DbSource {
    fn open(path: &Path, selector: &dyn Fn(&str) -> bool) -> Result<Self> {
        let connection = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .with_context(|| format!("failed to open {}", path.display()))?;

        let mut statement = connection
            .prepare("SELECT name, config FROM _streams")
            .context("no _streams table — is this a dimos memory2 recording?")?;
        let rows = statement
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);

        let mut streams = Vec::new();
        for (name, config) in rows {
            if !selector(&name) {
                continue;
            }
            let config: serde_json::Value =
                serde_json::from_str(&config).with_context(|| format!("bad config for {name}"))?;
            let payload_module = config["payload_module"].as_str().unwrap_or_default();
            let codec_id = config["codec_id"].as_str().unwrap_or_default();
            let msg_name = msg_name_from_payload_module(payload_module);
            let count: u64 = connection
                .query_row(&format!("SELECT COUNT(*) FROM {}", quote(&name)), [], |row| row.get(0))
                .unwrap_or(0);
            streams.push(Stream {
                support: stamp::support_for(&msg_name),
                msg_name,
                storage: storage_for(codec_id),
                published: name.clone(),
                name,
                count,
            });
        }
        if streams.is_empty() {
            bail!("no matching streams in {}", path.display());
        }

        let cursors = new_cursors(&streams);
        Ok(Self { connection, streams, cursors })
    }

    fn fill(&mut self, index: usize) -> Result<()> {
        let cursor = &mut self.cursors[index];
        let sql = format!(
            "SELECT id, ts FROM {} WHERE ts > ?1 ORDER BY ts LIMIT ?2",
            cursor.table
        );
        let mut statement = self.connection.prepare_cached(&sql)?;
        let rows = statement
            .query_map(rusqlite::params![cursor.last_ts, INDEX_PAGE as i64], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if rows.is_empty() {
            cursor.done = true;
        } else {
            cursor.last_ts = rows[rows.len() - 1].1;
            cursor.page.extend(rows);
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Record>> {
        for index in 0..self.cursors.len() {
            if self.cursors[index].page.is_empty() && !self.cursors[index].done {
                self.fill(index)?;
            }
        }
        let Some(index) = self
            .cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| Some((index, cursor.page.front()?.1)))
            .min_by(|left, right| left.1.total_cmp(&right.1))
            .map(|(index, _)| index)
        else {
            return Ok(None);
        };

        let cursor = &mut self.cursors[index];
        let (id, ts) = cursor.page.pop_front().expect("head checked above");
        let stream = cursor.stream;
        let sql = format!("SELECT data FROM {} WHERE id = ?1", cursor.blob_table);

        let mut statement = self.connection.prepare_cached(&sql)?;
        let data: Vec<u8> = statement
            .query_row([id], |row| row.get(0))
            .with_context(|| format!("missing blob {id} in {}", self.streams[stream].name))?;

        Ok(Some(Record { stream, ts, data }))
    }

    fn rewind(&mut self) -> Result<()> {
        self.cursors = new_cursors(&self.streams);
        Ok(())
    }
}

fn new_cursors(streams: &[Stream]) -> Vec<Cursor> {
    streams
        .iter()
        .enumerate()
        .map(|(index, stream)| Cursor {
            stream: index,
            table: quote(&stream.name),
            blob_table: quote(&format!("{}_blob", stream.name)),
            page: VecDeque::new(),
            last_ts: f64::NEG_INFINITY,
            done: false,
        })
        .collect()
}

// -------------------------------------------------------------------- .mcap

pub struct McapSource {
    /// Leaked so the message iterator below can borrow it for `'static`. The
    /// mapping lives as long as the process, which for a replay CLI is exactly
    /// the file's useful lifetime.
    mapped: &'static memmap2::Mmap,
    pub streams: Vec<Stream>,
    /// Channel topic -> index into `streams`; absent for channels filtered out.
    by_topic: std::collections::HashMap<String, usize>,
    messages: mcap::MessageStream<'static>,
}

impl McapSource {
    fn open(path: &Path, selector: &dyn Fn(&str) -> bool) -> Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        // Safety: replay is read-only; a concurrent writer truncating the file
        // would be a problem, but recordings are finalized before replay.
        let mapped = unsafe { memmap2::Mmap::map(&file) }
            .with_context(|| format!("failed to mmap {}", path.display()))?;
        let mapped: &'static memmap2::Mmap = Box::leak(Box::new(mapped));

        let summary = mcap::Summary::read(mapped)?
            .context("mcap has no summary section; cannot enumerate channels")?;

        let counts = summary.stats.as_ref().map(|stats| &stats.channel_message_counts);
        let mut streams = Vec::new();
        let mut by_topic = std::collections::HashMap::new();
        for (id, channel) in &summary.channels {
            // ROS topics lead with `/`; dimos stream names never do, and the
            // slash would otherwise show up in every channel and key expression.
            let name = channel.metadata.get("dimos.stream_name").cloned().unwrap_or_else(|| {
                channel.topic.strip_prefix('/').unwrap_or(&channel.topic).to_string()
            });
            if !selector(&name) {
                continue;
            }
            // dimos's own recorder stores LCM bytes and names the python type in
            // channel metadata; rosbag2 and `db_to_mcap` store CDR under a ROS
            // schema name.
            let (msg_name, storage) = match channel.message_encoding.as_str() {
                "lcm" => {
                    let payload_module = channel
                        .metadata
                        .get("dimos.payload_type")
                        .map(String::as_str)
                        .unwrap_or_default();
                    (msg_name_from_payload_module(payload_module), Storage::Wire)
                }
                "cdr" => {
                    let schema =
                        channel.schema.as_ref().map(|schema| schema.name.clone()).unwrap_or_default();
                    let msg_name = crate::cdr::msg_name_from_schema(&schema);
                    let storage = if crate::cdr::supports(&msg_name) {
                        Storage::Cdr
                    } else {
                        Storage::Unsupported
                    };
                    (msg_name, storage)
                }
                _ => (String::new(), Storage::Unsupported),
            };
            by_topic.insert(channel.topic.clone(), streams.len());
            streams.push(Stream {
                support: stamp::support_for(&msg_name),
                msg_name,
                storage,
                published: name.clone(),
                name,
                count: counts.and_then(|counts| counts.get(id)).copied().unwrap_or(0),
            });
        }
        if streams.is_empty() {
            bail!("no matching channels in {}", path.display());
        }

        let messages = mcap::MessageStream::new(mapped)?;
        Ok(Self { mapped, streams, by_topic, messages })
    }

    fn next(&mut self) -> Result<Option<Record>> {
        loop {
            let Some(message) = self.messages.next() else { return Ok(None) };
            let message = message?;
            let Some(&index) = self.by_topic.get(&message.channel.topic) else { continue };
            return Ok(Some(Record {
                stream: index,
                // publish_time carries the source timestamp; log_time is when
                // the recorder received it.
                ts: message.publish_time as f64 / 1e9,
                data: message.data.into_owned(),
            }));
        }
    }

    fn rewind(&mut self) -> Result<()> {
        self.messages = mcap::MessageStream::new(self.mapped)?;
        Ok(())
    }
}

/// Turns stored bytes into LCM wire bytes.
pub fn to_wire(stream: &Stream, data: Vec<u8>) -> Result<Vec<u8>> {
    match stream.storage {
        Storage::Wire => Ok(data),
        Storage::Lz4 => lz4_flex::frame::FrameDecoder::new(data.as_slice())
            .pipe_read_to_end()
            .context("lz4 decompression failed"),
        Storage::Cdr => crate::cdr::to_lcm(&stream.msg_name, &data)
            .with_context(|| format!("failed to transcode {} from CDR", stream.name)),
        Storage::Unsupported => bail!("stream is not stored in an LCM-compatible codec"),
    }
}

trait PipeReadToEnd {
    fn pipe_read_to_end(self) -> std::io::Result<Vec<u8>>;
}

impl<R: std::io::Read> PipeReadToEnd for R {
    fn pipe_read_to_end(mut self) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::new();
        self.read_to_end(&mut out)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_name_drops_the_duplicated_class_segment() {
        assert_eq!(
            msg_name_from_payload_module("dimos.msgs.sensor_msgs.Image.Image"),
            "sensor_msgs.Image"
        );
        assert_eq!(
            msg_name_from_payload_module("dimos.msgs.nav_msgs.Odometry.Odometry"),
            "nav_msgs.Odometry"
        );
        assert_eq!(
            msg_name_from_payload_module("dimos.msgs.tf2_msgs.TFMessage.TFMessage"),
            "tf2_msgs.TFMessage"
        );
    }

    #[test]
    fn msg_name_passes_through_names_that_are_already_short() {
        assert_eq!(msg_name_from_payload_module("sensor_msgs.Image"), "sensor_msgs.Image");
        assert_eq!(msg_name_from_payload_module(""), "");
    }

    #[test]
    fn quote_escapes_embedded_quotes() {
        assert_eq!(quote(r#"we"ird"#), r#""we""ird""#);
    }

    #[test]
    fn lz4_round_trips_to_the_original_wire_bytes() {
        let wire = b"lcm wire bytes".to_vec();
        let compressed = lz4_flex::frame::FrameEncoder::new(Vec::new())
            .pipe_write_all(&wire)
            .unwrap();
        let stream = Stream {
            name: "s".into(),
            published: "s".into(),
            msg_name: String::new(),
            storage: Storage::Lz4,
            support: Support::None,
            count: 0,
        };
        assert_eq!(to_wire(&stream, compressed).unwrap(), wire);
    }

    trait PipeWriteAll {
        fn pipe_write_all(self, data: &[u8]) -> std::io::Result<Vec<u8>>;
    }

    impl PipeWriteAll for lz4_flex::frame::FrameEncoder<Vec<u8>> {
        fn pipe_write_all(mut self, data: &[u8]) -> std::io::Result<Vec<u8>> {
            use std::io::Write;
            self.write_all(data)?;
            self.finish().map_err(std::io::Error::other)
        }
    }
}
