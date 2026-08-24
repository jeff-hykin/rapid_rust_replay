//! Publishing wire bytes onto LCM or Zenoh.
//!
//! Both transports carry the LCM payload unchanged; only the name differs.
//! dimos builds an LCM channel as `topic#msg_name` and a Zenoh key expression
//! as `topic/msg_name`.

use anyhow::{Context, Result};
use dimos_lcm::Lcm;
use zenoh::pubsub::Publisher;
use zenoh::Session;

use crate::source::Stream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Transport {
    Lcm,
    Zenoh,
}

pub enum Sink {
    Lcm { lcm: Lcm, channels: Vec<String> },
    Zenoh { _session: Session, publishers: Vec<Publisher<'static>> },
}

impl Sink {
    pub async fn open(transport: Transport, streams: &[Stream], prefix: &str) -> Result<Self> {
        match transport {
            Transport::Lcm => {
                let lcm = Lcm::new().await.context("failed to join the LCM multicast group")?;
                let channels = streams.iter().map(|stream| name(stream, prefix, '#')).collect();
                Ok(Sink::Lcm { lcm, channels })
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
                Ok(Sink::Zenoh { _session: session, publishers })
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
}
