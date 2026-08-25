//! Publishing wire bytes onto LCM or Zenoh.
//!
//! Both transports carry the LCM payload unchanged; only the name differs.
//! dimos builds an LCM channel as `topic#msg_name` and a Zenoh key expression
//! as `topic/msg_name`.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use dimos_lcm::Lcm;
use tokio::sync::watch;
use zenoh::pubsub::Publisher;
use zenoh::Session;

use crate::source::Stream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Transport {
    Lcm,
    Zenoh,
}

pub enum Sink {
    Lcm { lcm: Arc<Lcm>, channels: Vec<String> },
    Zenoh { session: Session, publishers: Vec<Publisher<'static>> },
}

/// A running count of messages seen on a watched topic, and when the latest
/// one landed.
pub type Arrivals = watch::Receiver<(u64, Instant)>;

impl Sink {
    pub async fn open(transport: Transport, streams: &[Stream], prefix: &str) -> Result<Self> {
        match transport {
            Transport::Lcm => {
                let lcm = Lcm::new().await.context("failed to join the LCM multicast group")?;
                let channels = streams.iter().map(|stream| name(stream, prefix, '#')).collect();
                Ok(Sink::Lcm { lcm: Arc::new(lcm), channels })
            }
            Transport::Zenoh => {
                // Honours `ZENOH_CONFIG`, so endpoints and scouting can be set
                // without this tool growing a flag for every zenoh knob.
                let config = zenoh::Config::from_env().unwrap_or_default();
                let session = zenoh::open(config)
                    .await
                    .map_err(|error| anyhow::anyhow!("failed to open a zenoh session: {error}"))?;
                let mut publishers = Vec::with_capacity(streams.len());
                for stream in streams {
                    let key = name(stream, prefix, '/');
                    let publisher = session
                        .declare_publisher(key.clone())
                        .await
                        .map_err(|error| anyhow::anyhow!("failed to declare {key}: {error}"))?;
                    publishers.push(publisher);
                }
                Ok(Sink::Zenoh { session, publishers })
            }
        }
    }

    pub async fn publish(&self, stream: usize, data: Vec<u8>) -> Result<()> {
        match self {
            Sink::Lcm { lcm, channels } => lcm
                .publish(&channels[stream], &data)
                .await
                .with_context(|| format!("failed to publish on {}", channels[stream])),
            Sink::Zenoh { publishers, .. } => publishers[stream]
                .put(data)
                .await
                .map_err(|error| anyhow::anyhow!("failed to publish: {error}")),
        }
    }

    /// Counts messages arriving on `topic` and timestamps each arrival, so
    /// lockstep can measure how long a downstream node took to answer.
    pub async fn watch(&self, topic: &str) -> Result<Arrivals> {
        let (arrivals, receiver) = watch::channel((0u64, Instant::now()));
        match self {
            Sink::Lcm { lcm, .. } => {
                let (lcm, topic) = (Arc::clone(lcm), topic.to_string());
                tokio::spawn(async move {
                    let mut seen = 0u64;
                    while let Ok(message) = lcm.recv().await {
                        if channel_answers(&message.channel, &topic) {
                            seen += 1;
                            if arrivals.send((seen, Instant::now())).is_err() {
                                break;
                            }
                        }
                    }
                });
            }
            Sink::Zenoh { session, .. } => {
                let key = key_expr(topic);
                let subscriber = session
                    .declare_subscriber(key.clone())
                    .await
                    .map_err(|error| anyhow::anyhow!("failed to subscribe to {key}: {error}"))?;
                tokio::spawn(async move {
                    let mut seen = 0u64;
                    while subscriber.recv_async().await.is_ok() {
                        seen += 1;
                        if arrivals.send((seen, Instant::now())).is_err() {
                            break;
                        }
                    }
                });
            }
        }
        Ok(receiver)
    }

    /// The channel or key each stream publishes on, for `--list` and logging.
    pub fn names(&self) -> Vec<String> {
        match self {
            Sink::Lcm { channels, .. } => channels.clone(),
            Sink::Zenoh { publishers, .. } => {
                publishers.iter().map(|p| p.key_expr().to_string()).collect()
            }
        }
    }
}

/// A watched topic is named the way a person says it — `fused_odom` — but the
/// publisher appends its message type, so a bare name has to match the family.
fn channel_answers(channel: &str, topic: &str) -> bool {
    channel.strip_prefix(topic).is_some_and(|rest| rest.is_empty() || rest.starts_with('#'))
}

/// The Zenoh spelling of the same idea: `**` also matches zero chunks, so
/// `fused_odom/**` covers the bare key as well as the typed one.
fn key_expr(topic: &str) -> String {
    if topic.contains('*') {
        topic.to_string()
    } else {
        format!("{}/**", topic.trim_end_matches('/'))
    }
}

/// Builds the published name; streams with no known message type get no suffix.
pub fn name(stream: &Stream, prefix: &str, separator: char) -> String {
    let topic = format!("{prefix}{}", stream.name);
    if stream.msg_name.is_empty() {
        topic
    } else {
        format!("{topic}{separator}{}", stream.msg_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Storage;
    use crate::stamp::Support;

    fn stream(name: &str, msg_name: &str) -> Stream {
        Stream {
            name: name.into(),
            msg_name: msg_name.into(),
            storage: Storage::Wire,
            support: Support::None,
            count: 0,
        }
    }

    #[test]
    fn lcm_and_zenoh_names_match_the_dimos_conventions() {
        let color = stream("color_image", "sensor_msgs.Image");
        assert_eq!(name(&color, "", '#'), "color_image#sensor_msgs.Image");
        assert_eq!(name(&color, "", '/'), "color_image/sensor_msgs.Image");
        assert_eq!(name(&color, "dimos/", '/'), "dimos/color_image/sensor_msgs.Image");
    }

    #[test]
    fn streams_without_a_known_type_get_no_suffix() {
        assert_eq!(name(&stream("raw", ""), "", '#'), "raw");
    }

    #[test]
    fn a_watched_topic_matches_the_typed_lcm_channel() {
        assert!(channel_answers("fused_odom", "fused_odom"));
        assert!(channel_answers("fused_odom#nav_msgs.Odometry", "fused_odom"));
        assert!(!channel_answers("fused_odometry", "fused_odom"));
        assert!(!channel_answers("wheel_odom#nav_msgs.Odometry", "fused_odom"));
    }

    /// `topic/**` has to cover the bare key too, or an untyped publisher would
    /// never satisfy the gate.
    #[test]
    fn a_watched_topic_matches_the_typed_zenoh_key() {
        let declared = zenoh::key_expr::KeyExpr::new(key_expr("fused_odom")).unwrap();
        for reply in ["fused_odom", "fused_odom/nav_msgs.Odometry"] {
            let key = zenoh::key_expr::KeyExpr::new(reply).unwrap();
            assert!(declared.intersects(&key), "{declared} should cover {key}");
        }
        assert!(!declared.intersects(&zenoh::key_expr::KeyExpr::new("wheel_odom").unwrap()));
    }

    #[test]
    fn an_explicit_wildcard_is_left_as_written() {
        assert_eq!(key_expr("odom/**"), "odom/**");
    }
}
