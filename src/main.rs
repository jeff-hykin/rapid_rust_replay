//! Replay a dimos recording (`.db` or `.mcap`) onto LCM or Zenoh.

mod cdr;
mod sink;
mod source;
mod stamp;

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use clap::Parser;

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

    /// Only report errors.
    #[arg(short, long)]
    quiet: bool,
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
    let streams = source.streams().to_vec();

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
    }

    let mut published: u64 = 0;
    let mut skipped: u64 = 0;
    loop {
        let (sent, dropped) = replay_once(&mut source, &streams, &sink, &args).await?;
        published += sent;
        skipped += dropped;
        if !args.repeat {
            break;
        }
        source.rewind()?;
    }

    if !args.quiet {
        eprintln!("published {published} message(s)");
        if skipped > 0 {
            eprintln!("skipped {skipped} message(s)");
        }
    }
    Ok(())
}

async fn replay_once(
    source: &mut Source,
    streams: &[source::Stream],
    sink: &Sink,
    args: &Args,
) -> Result<(u64, u64)> {
    let mut base: Option<f64> = None;
    let mut start_wall = Instant::now();
    let mut retimer = Retimer::Original;
    let mut published = 0;
    let mut skipped = 0;

    while let Some(record) = source.next()? {
        let stream = &streams[record.stream];
        if stream.storage == Storage::Unsupported {
            skipped += 1;
            continue;
        }

        // The first message that survives filtering anchors both clocks.
        let base = *base.get_or_insert_with(|| {
            start_wall = Instant::now();
            let base = record.ts + args.start;
            let base_ns = stamp::seconds_to_nanos(base);
            let now_ns = stamp::seconds_to_nanos(now());
            retimer = match args.stamps {
                Stamps::Original => Retimer::Original,
                Stamps::Shifted => Retimer::Shifted { delta_ns: now_ns - base_ns },
                Stamps::Scaled => Retimer::Scaled {
                    first_ns: base_ns,
                    start_wall_ns: now_ns,
                    rate: if args.rate == 0.0 { 1.0 } else { args.rate },
                },
            };
            base
        });

        let elapsed = record.ts - base;
        if elapsed < 0.0 {
            skipped += 1;
            continue;
        }
        if args.duration.is_some_and(|limit| elapsed > limit) {
            break;
        }

        if args.rate > 0.0 {
            sleep_until(start_wall + Duration::from_secs_f64(elapsed / args.rate)).await;
        }

        let mut data = source::to_wire(stream, record.data)?;
        retimer.apply(stream.support, &mut data, stamp::seconds_to_nanos(record.ts))?;
        sink.publish(record.stream, data).await?;
        published += 1;
    }

    Ok((published, skipped))
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
