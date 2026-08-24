# rapid_rust_replay

Replays a DimOS recording — a memory2 `.db` or an `.mcap` — onto LCM or Zenoh,
re-stamping each message so subscribers see it as live data.

```
rrr recording.db                       # replay every stream on LCM at 1x
rrr recording.db --list                # what is in the file
rrr recording.db -s color_image -r 0.5 # one stream, half speed
rrr recording.mcap -t zenoh --loop     # onto Zenoh, forever
```

## Install

```
nix profile install github:jeff-hykin/rapid_rust_replay
```

Or run it without installing anything:

```
nix run github:jeff-hykin/rapid_rust_replay -- recording.db --list
```

From a checkout, `cargo install --path .` works too.

## What it reads

| Source | Payload | Notes |
| --- | --- | --- |
| memory2 `.db` | `lcm`, `jpeg`, `lz4+lcm`, `lz4+jpeg` | stored bytes are already LCM wire bytes |
| `.mcap`, `message_encoding: lcm` | LCM | written by the DimOS native recorder |
| `.mcap`, `message_encoding: cdr` | ROS 2 | transcoded to LCM per message |

Streams stored as Python pickles, and CDR types with no LCM counterpart, are
reported and skipped rather than failing the run.

Transcoding covers `Image`, `CompressedImage`, `CameraInfo`, `Imu`,
`PointCloud2`, `LaserScan`, `JointState`, `Odometry`, `Path`, `PoseStamped`,
`TransformStamped` and `TFMessage`. ROS 2 has no `header.seq`, so the LCM one is
left at 0.

## What it writes

The name follows the DimOS conventions: `topic#msg_name` on LCM,
`topic/msg_name` on Zenoh. A leading `/` on a ROS topic is dropped, since DimOS
stream names never carry one. `--prefix` is prepended to both.

Zenoh sessions honour `ZENOH_CONFIG`, so endpoints and scouting can be pointed
somewhere specific without this tool growing a flag per knob:

```
ZENOH_CONFIG=peer.json5 rrr recording.db -t zenoh
```

## Timestamps

`--stamps` decides what happens to the timestamp inside each payload:

- `scaled` (default) — stamps track emission, so `--rate 2` halves their spacing
  as well as the wall-clock spacing.
- `shifted` — moved to the present, recorded spacing preserved.
- `original` — left alone.

Rewriting patches the stamp in place at a known byte offset, so a multi-megabyte
point cloud costs eight bytes of work instead of a decode and re-encode. The
offsets are pinned against the real encoder by a unit test rather than trusted
from arithmetic. Messages whose type has no known stamp field are republished
untouched, with a warning.

All of it is integer nanoseconds; a float Unix timestamp only resolves to about
100 ns, which is enough to perturb the field it is meant to preserve.

## Options

```
-t, --transport <lcm|zenoh>   transport to publish on [default: lcm]
-s, --stream <NAME>           stream to replay, repeatable; trailing * matches a prefix
-r, --rate <RATE>             2 is twice realtime, 0.5 half, 0 disables pacing [default: 1]
    --stamps <MODE>           scaled | shifted | original [default: scaled]
    --prefix <PREFIX>         prepended to every channel or key expression
    --start <SECONDS>         skip this far into the recording
    --duration <SECONDS>      stop after this much recording time
    --loop                    restart at the end
    --list                    list streams and exit
-q, --quiet                   only report errors
```

## Pacing

macOS coalesces timers for background processes, so a bare sleep overshoots by
milliseconds — enough to smear a 100 Hz IMU stream. Replay sleeps to a
millisecond short of each deadline and yields through the rest.
