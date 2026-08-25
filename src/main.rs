//! Replay a dimos recording (`.db` or `.mcap`) onto LCM or Zenoh.

mod cdr;
mod lockstep;
mod sink;
mod source;
mod stamp;

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use clap::Parser;

use lockstep::Lockstep;
use sink::{Sink, Transport};
use source::{Source, Storage};
use stamp::{Retimer, Support};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Stamps {
    /// Rewrite stamps so they track emission: `--rate 2` halves their spacing.
    Scaled,
    /// Move stamps to the present but keep their recorded spacing.
    Shifted,
    /// Leave recorded stamps untouched.
    Original,
}

#[derive(Parser, Debug)]
#[command(
    name = "rrr",
    about = "Replay a dimos .db or .mcap recording onto LCM or Zenoh",
    version
)]
struct Args {
    /// Recording to replay (`.db` or `.mcap`).
    input: PathBuf,

    /// Transport to publish on.
    #[arg(short, long, value_enum, default_value_t = Transport::Lcm)]
    transport: Transport,

    /// Stream to replay; repeat for several. Trailing `*` matches a prefix.
    /// Defaults to every stream.
    #[arg(short, long = "stream")]
    streams: Vec<String>,

    /// Publish a stream under a different name: `OLD:NEW`, repeat for several.
    /// Several streams may share one NEW, which is how a stereo pair reaches a
    /// consumer that expects both imagers on one topic.
    #[arg(long = "rename", value_name = "OLD:NEW")]
    renames: Vec<String>,

    /// List the streams in the recording and exit.
    #[arg(long)]
    list: bool,

    /// Playback speed: 2 is twice realtime, 0.5 half. Use 0 for no pacing.
    #[arg(short, long, default_value_t = 1.0)]
    rate: f64,

    /// How recorded timestamps inside each payload are rewritten.
    #[arg(long, value_enum, default_value_t = Stamps::Scaled)]
    stamps: Stamps,

    /// Prepended to every channel or key expression, e.g. `dimos/`.
    #[arg(long, default_value = "")]
    prefix: String,

    /// Skip this many seconds from the start of the recording.
    #[arg(long, default_value_t = 0.0)]
    start: f64,

    /// Stop after this many seconds of recording time.
    #[arg(long)]
    duration: Option<f64>,

    /// Replay forever, restarting at the end.
    #[arg(long = "loop")]
    repeat: bool,

    /// Let a downstream node set the pace: `STREAM:TOPIC`, e.g.
    /// `wheel_odom:fused_odom`. Each publish of the stream waits to be answered
    /// on the topic, so a consumer that keeps up replays faster than realtime.
    #[arg(long, value_name = "STREAM:TOPIC")]
    lockstep: Option<String>,

    /// How long a lockstep reply may take before the replay moves on without it.
    #[arg(long, default_value_t = 1.0, value_name = "SECONDS")]
    lockstep_timeout: f64,

    /// Only report errors.
    #[arg(short, long)]
    quiet: bool,
}

/// Applies `--rename OLD:NEW` to the names streams are published under.
///
/// Each spec is matched against the *recorded* name, so renames never chain and
/// their order does not matter. Several may share one NEW: the sink is indexed
/// by `Record.stream`, so two streams pointing at one channel is not a conflict.
fn rename(streams: &mut [source::Stream], specs: &[String]) -> Result<()> {
    for spec in specs {
        let (old, new) = spec.split_once(':').with_context(|| {
            format!(
                "--rename wants OLD:NEW, for example wheel_odometry:source_odometry, not {spec}"
            )
        })?;
        if new.is_empty() {
            bail!("--rename {spec} leaves an empty name");
        }
        let stream = streams
            .iter_mut()
            .find(|stream| stream.name == old)
            .with_context(|| format!("--rename {spec}: no stream named {old} is being replayed"))?;
        stream.published = new.to_string();
    }
    Ok(())
}

fn now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.rate < 0.0 {
        bail!("--rate cannot be negative");
    }

    let patterns = args.streams.clone();
    let selector = move |name: &str| {
        patterns.is_empty()
            || patterns.iter().any(|pattern| match pattern.strip_suffix('*') {
                Some(prefix) => name.starts_with(prefix),
                None => name == pattern,
            })
    };

    let mut source = Source::open(&args.input, &selector)?;
    let mut streams = source.streams().to_vec();
    rename(&mut streams, &args.renames)?;

    if args.list {
        let separator = if args.transport == Transport::Lcm { '#' } else { '/' };
        for stream in &streams {
            println!(
                "{:<32} {:>9}  {:<28} {}",
                stream.name,
                stream.count,
                stream.msg_name,
                sink::name(stream, &args.prefix, separator)
            );
        }
        return Ok(());
    }

    for stream in &streams {
        if stream.storage == Storage::Unsupported {
            eprintln!(
                "warning: skipping {} — {} has no LCM wire form",
                stream.name,
                if stream.msg_name.is_empty() { "its codec" } else { &stream.msg_name }
            );
        } else if stream.support == Support::None && args.stamps != Stamps::Original {
            eprintln!(
                "warning: {} has no known timestamp field ({}); payloads replay unmodified",
                stream.name,
                if stream.msg_name.is_empty() { "unknown type" } else { &stream.msg_name }
            );
        }
    }

    let sink = Sink::open(args.transport, &streams, &args.prefix).await?;
    let mut lockstep = match &args.lockstep {
        Some(spec) => Some(
            Lockstep::open(
                spec,
                &streams,
                &sink,
                Duration::from_secs_f64(args.lockstep_timeout),
            )
            .await?,
        ),
        None => None,
    };

    if !args.quiet {
        eprintln!(
            "replaying {} stream(s) from {} at {}",
            streams.len(),
            args.input.display(),
            if args.rate == 0.0 { "max speed".into() } else { format!("{}x", args.rate) }
        );
        for name in sink.names() {
            eprintln!("  -> {name}");
        }
        if let Some(spec) = &args.lockstep {
            eprintln!("lockstep: {spec} (up to {}s per reply)", args.lockstep_timeout);
        }
    }

    let mut total = Tally::default();
    loop {
        let pass = replay_once(&mut source, &streams, &sink, &args, lockstep.as_mut()).await?;
        total.published += pass.published;
        total.skipped += pass.skipped;
        total.restamped += pass.restamped;
        if !args.repeat {
            break;
        }
        source.rewind()?;
    }

    if !args.quiet {
        eprintln!("published {} message(s)", total.published);
        if total.skipped > 0 {
            eprintln!("skipped {} message(s)", total.skipped);
        }
        if total.restamped > 0 {
            eprintln!(
                "{} message(s) carried a stamp from another clock; \
                 used the recorded arrival time instead",
                total.restamped
            );
        }
        if let Some(lockstep) = &lockstep {
            if lockstep.timeouts > 0 {
                eprintln!("{} lockstep reply(s) never arrived", lockstep.timeouts);
            }
        }
    }
    Ok(())
}

/// Where the recording clock was last pinned to the replay clock.
///
/// Without lockstep this is set once and never moves, which is the plain
/// `elapsed / rate` pacing. Lockstep re-pins it at every gate publish, so both
/// the emission times and the stamps of the messages that follow are measured
/// from a point that actually happened rather than from one predicted at the
/// start of the file.
struct Anchor {
    recorded: f64,
    wall: Instant,
    wall_ns: i64,
}

#[derive(Default)]
struct Tally {
    published: u64,
    /// Messages whose payload had no LCM wire form.
    skipped: u64,
    /// Messages whose own stamp was unusable, so recorded arrival time stood in.
    restamped: u64,
}

async fn replay_once(
    source: &mut Source,
    streams: &[source::Stream],
    sink: &Sink,
    args: &Args,
    mut lockstep: Option<&mut Lockstep>,
) -> Result<Tally> {
    let mut base: Option<f64> = None;
    let mut anchor: Option<Anchor> = None;
    let mut rate = if args.rate == 0.0 { 1.0 } else { args.rate };
    let mut retimer = Retimer::Original;
    let mut tally = Tally::default();

    while let Some(record) = source.next()? {
        let stream = &streams[record.stream];
        if stream.storage == Storage::Unsupported {
            tally.skipped += 1;
            continue;
        }

        // The first message that survives filtering anchors both clocks.
        let base = *base.get_or_insert(record.ts + args.start);
        let anchor = anchor.get_or_insert_with(|| {
            let wall_ns = stamp::seconds_to_nanos(now());
            let base_ns = stamp::seconds_to_nanos(base);
            retimer = match args.stamps {
                Stamps::Original => Retimer::Original,
                Stamps::Shifted => Retimer::Shifted { delta_ns: wall_ns - base_ns },
                Stamps::Scaled => {
                    Retimer::Scaled { first_ns: base_ns, start_wall_ns: wall_ns, rate }
                }
            };
            Anchor { recorded: base, wall: Instant::now(), wall_ns }
        });

        let elapsed = record.ts - base;
        if elapsed < 0.0 {
            tally.skipped += 1;
            continue;
        }
        if args.duration.is_some_and(|limit| elapsed > limit) {
            break;
        }

        let gate = lockstep.as_ref().is_some_and(|lockstep| lockstep.stream == record.stream);
        if gate {
            // A gate message is released by the downstream node, not by the
            // clock, so it skips its wall deadline entirely — that is what lets
            // a consumer that keeps up pull the recording along faster.
            if let Some(lockstep) = lockstep.as_mut() {
                lockstep.wait(record.ts, &mut rate).await;
            }
            anchor.recorded = record.ts;
            anchor.wall = Instant::now();
            anchor.wall_ns = stamp::seconds_to_nanos(now());
            if let Retimer::Scaled { first_ns, start_wall_ns, rate: scaled } = &mut retimer {
                *first_ns = stamp::seconds_to_nanos(anchor.recorded);
                *start_wall_ns = anchor.wall_ns;
                *scaled = rate;
            }
        } else if args.rate > 0.0 {
            let ahead = (record.ts - anchor.recorded).max(0.0);
            sleep_until(anchor.wall + Duration::from_secs_f64(ahead / rate)).await;
        }

        let mut data = source::to_wire(stream, record.data)?;
        if retimer.apply(stream.support, &mut data, stamp::seconds_to_nanos(record.ts))? {
            tally.restamped += 1;
        }
        sink.publish(record.stream, data).await?;
        tally.published += 1;

        if gate {
            if let Some(lockstep) = lockstep.as_mut() {
                lockstep.sent(record.ts, Instant::now());
            }
        }
    }

    Ok(tally)
}

/// Sleeps to just short of `target`, then yields until it arrives.
///
/// macOS coalesces timers for background processes, so a bare sleep routinely
/// overshoots by milliseconds — enough to smear a 100 Hz IMU stream. Giving the
/// last millisecond to a yield loop keeps pacing tight without spinning through
/// the whole interval.
async fn sleep_until(target: Instant) {
    const SPIN: Duration = Duration::from_millis(1);
    let remaining = target.saturating_duration_since(Instant::now());
    if remaining > SPIN {
        tokio::time::sleep(remaining - SPIN).await;
    }
    while Instant::now() < target {
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use source::{Storage, Stream};

    fn streams(names: &[&str]) -> Vec<Stream> {
        names
            .iter()
            .map(|name| Stream {
                name: (*name).into(),
                published: (*name).into(),
                msg_name: "sensor_msgs.Image".into(),
                storage: Storage::Wire,
                support: Support::None,
                count: 0,
            })
            .collect()
    }

    fn specs(specs: &[&str]) -> Vec<String> {
        specs.iter().map(|spec| (*spec).to_string()).collect()
    }

    /// DimSlam tells its cameras apart by `frame_id`, so both imagers have to
    /// arrive on one topic. A shared target is the point, not a conflict.
    #[test]
    fn several_streams_may_share_one_published_name() {
        let mut streams = streams(&["infrared_left", "infrared_right"]);
        rename(&mut streams, &specs(&["infrared_left:image", "infrared_right:image"])).unwrap();

        let channels: Vec<_> = streams.iter().map(|stream| sink::name(stream, "", '#')).collect();
        assert_eq!(channels, ["image#sensor_msgs.Image", "image#sensor_msgs.Image"]);
    }

    /// The recorded name stays put, so `-s` and `--lockstep` keep matching the
    /// file rather than the wire.
    #[test]
    fn renaming_leaves_the_recorded_name_alone() {
        let mut streams = streams(&["wheel_odometry"]);
        rename(&mut streams, &specs(&["wheel_odometry:source_odometry"])).unwrap();
        assert_eq!(streams[0].name, "wheel_odometry");
        assert_eq!(streams[0].published, "source_odometry");
    }

    /// Matching on the recorded name means `a:b` then `b:c` cannot pick up its
    /// own output, so the specs may be given in any order.
    #[test]
    fn renames_do_not_chain() {
        let mut streams = streams(&["camera_info", "infrared_left_camera_info"]);
        rename(
            &mut streams,
            &specs(&["camera_info:color_camera_info", "infrared_left_camera_info:camera_info"]),
        )
        .unwrap();
        assert_eq!(streams[0].published, "color_camera_info");
        assert_eq!(streams[1].published, "camera_info");
    }

    #[test]
    fn the_prefix_is_applied_after_the_rename() {
        let mut streams = streams(&["infrared_left"]);
        rename(&mut streams, &specs(&["infrared_left:image"])).unwrap();
        assert_eq!(sink::name(&streams[0], "dimos/", '/'), "dimos/image/sensor_msgs.Image");
    }

    #[test]
    fn a_rename_of_a_stream_that_is_not_replayed_is_an_error() {
        let mut streams = streams(&["color_image"]);
        let error = rename(&mut streams, &specs(&["lidar:points"])).unwrap_err().to_string();
        assert!(error.contains("no stream named lidar"), "{error}");
    }

    #[test]
    fn a_rename_without_a_colon_is_an_error() {
        let mut streams = streams(&["color_image"]);
        assert!(rename(&mut streams, &specs(&["color_image"])).is_err());
        assert!(rename(&mut streams, &specs(&["color_image:"])).is_err());
    }
}
