# rapid_rust_replay

Replays a DimOS recording — a memory2 `.db` or an `.mcap` — onto LCM or Zenoh,
re-stamping each message so subscribers see it as live data.

```sh
rrr recording.db                       # replay every stream on LCM at 1x
rrr recording.db --list                # what is in the file
rrr recording.db -s color_image -r 0.5 # one stream, half speed
rrr recording.mcap -t zenoh --loop     # onto Zenoh, forever

rrr recording.db --lockstep wheel_odom:fused_odom   # let the consumer set the pace
```

## Install

```sh
nix profile install github:jeff-hykin/rapid_rust_replay
```

Or run it without installing anything:

```sh
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
`topic/msg_name` on Zenoh.

DimOS also namespaces every topic, and the two transports do it differently: an
LCM channel is a leading-slash path (`/wheel_odometry#nav_msgs.Odometry`), while
a Zenoh key expression cannot start with `/` and lives under `dimos`
(`dimos/wheel_odometry/nav_msgs.Odometry`). `--prefix` therefore defaults to `/`
on LCM and `dimos/` on Zenoh, which is what a live DimOS graph subscribes to.
Pass `--prefix ''` for unqualified names.

Getting this wrong is silent in both directions — nothing subscribes, so nothing
complains, and the replay still reports every message as published. `--list`
prints the exact wire name, which is the only way to see it before a run.

Zenoh sessions honour `ZENOH_CONFIG`, so endpoints and scouting can be pointed
somewhere specific without this tool growing a flag per knob:

```sh
ZENOH_CONFIG=peer.json5 rrr recording.db -t zenoh
```

## Renaming streams

A recording stores each stream under the name of the module that produced it,
which is not always the name the consumer expects. `--rename OLD:NEW` changes
the name it is published under:

```sh
rrr recording.db --rename wheel_odometry:source_odometry
```

Several streams may share one `NEW`. That is not a conflict — it is how a stereo
pair reaches a consumer that expects both imagers on one topic and tells them
apart by `frame_id`:

```sh
rrr recording.db \
  --rename infrared_left:image \
  --rename infrared_right:image \
  --rename infrared_left_camera_info:camera_info \
  --rename infrared_right_camera_info:camera_info \
  --rename camera_info:color_camera_info
```

Each `OLD` is matched against the *recorded* name, so renames never chain and
their order does not matter — the last example moves `camera_info` out of the way
and the infrared pair into it in one pass. For the same reason `-s/--stream` and
`--lockstep` still name streams as the recording spells them. `--prefix` is
applied after the rename. `--list` shows the recorded name alongside the channel
it will actually publish on, which is the quickest way to check a mapping.

## Replaying `tf`

Replay `tf`. Every transform, exactly as recorded — that is the default and it
is almost always what you want. The recorded tree is what the hardware actually
reported that day; anything else is a guess. Synthesizing the mount tree from a
URDF instead scores whatever calibration happens to be committed today, and on
`drive_2026-08-18_23-05-04.db` that meant handing cuVSLAM a D435's 50 mm stereo
baseline for a D455 that reports 95 mm. Triangulation scales linearly with
baseline, so the trajectory came out 1.9x off — 3.21 m RMSE against point-lio
instead of 0.90 m. Nothing errored. Recorded tf is not optional decoration.

The one hazard is a double parent: replay an edge that a live module also
publishes and TF resolves the frame by whichever arrived most recently, a silent
and roughly fixed error in every pose derived from it. `drive_2026-08-18_23-05-04.db`
parents `mid360_link` straight off `odom`, so replaying it alongside a live
point-lio, which publishes the lidar under `base_link`, would collide.

Fix that at the source, not in the replay. If the live module and the recording
claim the same edge, move the live one — dim_slam takes its output frame from
config. If the recording's tf is wrong or incomplete, correct the database.

`--drop-tf PARENT:CHILD` is the escape hatch when neither is possible. It is
repeatable and `*` matches any frame on either side:

```sh
rrr recording.db --drop-tf 'odom:mid360_link' --drop-tf 'camera_link:*'
```

With a rule in play, every dropped edge is reported by name at the end of the
run, and a message left with no transforms at all is not published:

```
published 7190 message(s)
dropped 594 tf transform(s):
  odom -> mid360_link (594)
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

Some drivers stamp with system uptime rather than the epoch, and a recording can
mix the two within one stream. A stamp more than an hour from when the recorder
took delivery of the message is treated as coming from another clock, and the
recorded arrival time is used instead — mapping it as-is would land the message
before 1970. The count is reported at the end of the run rather than passed
along silently.

## What the recording gets wrong

Nothing below is repaired — a replay puts out what the recording holds — but all
of it is invisible from the subscriber's side, which is how it costs people whole
days. So it is said out loud, per stream and per frame, because a bare total does
not tell you which sensor to distrust.

- A payload stamped *after* the recorder took delivery of it describes a message
  that had not been sent yet. Anything under 10 ms is two clocks disagreeing
  rather than one being wrong, and is ignored.
- A payload stamp walking backwards while arrivals walk forwards is a clock that
  is not monotonic. Caught by order rather than magnitude: the bad stamps have no
  characteristic offset to threshold against, and a real capture latency is the
  same size as the small ones.
- A `tf` stream that is not one tree — a frame with two parents, or frames that
  never reach a common root. This is checked *after* `--drop-tf`, since dropping
  the offending edge is the fix.

These repeat on every message once they start, so each warning prints on first
sight and then at most once every five seconds, carrying the count it swallowed:

```
warning: lidar is stamped 2.273s after the recorder received it
warning: lidar stamps went back 2.371s while its arrivals moved forward
warning: tf holds 2 separate trees, rooted at base_link, odom (1781 more since the last warning)
```

The totals land at the end of the run, and survive `--quiet`:

```
stamps that cannot be right:
  lidar: 3 stamp(s) up to 2.273s after arrival, 88 backwards step(s) up to 68.724s
tf does not describe one tree:
  2 separate trees, rooted at base_link, odom
```

That is a real reading of `drive_2026-08-16_23-46-03.db`. The Mid-360 driver
copies its own arrival into the payload for most messages and produces a garbage
stamp for the rest; every RealSense stream in the same file is exact. The two
trees are in every Alfred recording: the camera hangs off `base_link` and the
lidar off `odom`, with nothing joining them, so nothing in the file says where
the lidar sits on the robot.

## Lockstep

`--lockstep wheel_odom:fused_odom` hands the pace to whatever is consuming the
replay. Every `wheel_odom` publish waits for the `fused_odom` that answers the
one before it, so a consumer that keeps up pulls the recording along faster than
realtime, and one that falls behind slows it down. `--lockstep-timeout` (1s by
default) is the escape hatch when nothing answers; those are counted and
reported.

The reply topic is matched as a family, so `fused_odom` also accepts
`fused_odom#nav_msgs.Odometry` on LCM and `fused_odom/nav_msgs.Odometry` on
Zenoh. `--prefix` is applied to it, since the reply comes from a module in the
same graph and is namespaced the same way our own publishes are.

How fast the replies come back is measured from each publish, so it reflects the
consumer rather than this tool's own pacing, and it is smoothed rather than
taken one cycle at a time. That rate then spreads the messages *between* two
gate publishes — the camera frames between two odometry ticks — across the same
interval, so their stamps stay where they belong relative to the gate messages
around them. `--rate` sets the starting estimate; `--rate 0` gates on the
replies alone and emits everything in between as fast as it can.

Measured against a 4-second slice of `china_office.db`, gating `go2_odom` on a
consumer answering in ~40 ms per cycle: it ran at 1.22x, gate spacing compressed
from 53.4 ms to 43.8 ms, and the `livox_imu` messages in between compressed by
the same factor, landing within 2 ms of their own stamps.

## Options

```
-t, --transport <lcm|zenoh>   transport to publish on [default: lcm]
-s, --stream <NAME>           stream to replay, repeatable; trailing * matches a prefix
    --rename <OLD:NEW>        publish a stream under another name, repeatable
    --drop-tf <PARENT:CHILD>  drop this tf edge, repeatable; * matches any frame
-r, --rate <RATE>             2 is twice realtime, 0.5 half, 0 disables pacing [default: 1]
    --stamps <MODE>           scaled | shifted | original [default: scaled]
    --prefix <PREFIX>         prepended to every name [default: / on lcm, dimos/ on zenoh]
    --start <SECONDS>         skip this far into the recording
    --duration <SECONDS>      stop after this much recording time
    --loop                    restart at the end
    --lockstep <STREAM:TOPIC> gate the stream on a reply from the topic
    --lockstep-timeout <SECS> how long a reply may take [default: 1]
    --list                    list streams and exit
-q, --quiet                   only report errors
```

## Pacing

macOS coalesces timers for background processes, so a bare sleep overshoots by
milliseconds — enough to smear a 100 Hz IMU stream. Replay sleeps to a
millisecond short of each deadline and yields through the rest.
