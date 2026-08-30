//! Length-prefixed frame codec shared by every host↔kernel hop.
//!
//! Frame layout: `[tag u8][len u32 BE][payload]` — `CONTROL` payloads are
//! protobuf-encoded [`crate::pb::Envelope`] messages, `ARROW` payloads are one
//! Arrow IPC stream segment (schema + batches). The same codec serves the
//! local stdio transport and the runner's kernel connections.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message as _;
use tokio_util::codec::{Decoder, Encoder};

/// CONTROL: protobuf `Envelope`.
pub const TAG_CONTROL: u8 = 0x01;
/// ARROW: one Arrow IPC stream segment.
pub const TAG_ARROW: u8 = 0x02;

/// Hard cap for a single frame (512 MiB) so a corrupt stream cannot make the
/// host allocate without bound.
pub const MAX_FRAME_LEN: u32 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameTag {
    Control,
    Arrow,
}

impl FrameTag {
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            FrameTag::Control => TAG_CONTROL,
            FrameTag::Arrow => TAG_ARROW,
        }
    }

    #[must_use]
    pub const fn from_u8(raw: u8) -> Option<Self> {
        match raw {
            TAG_CONTROL => Some(FrameTag::Control),
            TAG_ARROW => Some(FrameTag::Arrow),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub tag: FrameTag,
    pub payload: Bytes,
}

impl Frame {
    #[must_use]
    pub fn control<M: prost::Message>(msg: &M) -> Self {
        Self {
            tag: FrameTag::Control,
            payload: Bytes::from(msg.encode_to_vec()),
        }
    }

    #[must_use]
    pub fn arrow(payload: Bytes) -> Self {
        Self {
            tag: FrameTag::Arrow,
            payload,
        }
    }

    /// Decode a CONTROL payload into a [`crate::pb::Envelope`].
    ///
    /// # Errors
    /// Wrong tag or protobuf decode failure.
    pub fn envelope(&self) -> anyhow::Result<crate::pb::Envelope> {
        anyhow::ensure!(
            self.tag == FrameTag::Control,
            "expected a CONTROL frame, got tag {:#04x}",
            self.tag.as_u8()
        );
        Ok(crate::pb::Envelope::decode(self.payload.as_ref())?)
    }
}

/// Header size: tag byte + u32 length.
const HEADER_LEN: usize = 5;

#[derive(Debug, Clone, Copy, Default)]
pub struct FrameCodec;

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> io::Result<()> {
        let len = u32::try_from(item.payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame exceeds u32 length"))?;
        dst.reserve(HEADER_LEN + item.payload.len());
        dst.put_u8(item.tag.as_u8());
        dst.put_u32(len);
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Frame>> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }
        let tag = src[0];
        let len = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;
        if len > MAX_FRAME_LEN as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("declared frame length {len} exceeds cap {MAX_FRAME_LEN}"),
            ));
        }
        if src.len() < HEADER_LEN + len {
            src.reserve(HEADER_LEN + len - src.len());
            return Ok(None);
        }
        let mut full = src.split_to(HEADER_LEN + len);
        let payload = full.split_off(HEADER_LEN).freeze();
        let tag = FrameTag::from_u8(tag).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown frame tag {tag:#04x}"),
            )
        })?;
        Ok(Some(Frame { tag, payload }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::{envelope, Envelope, Hello, Welcome};
    use crate::PROTOCOL_VERSION;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::duplex;
    use tokio_util::codec::{FramedRead, FramedWrite};

    async fn roundtrip(frames: Vec<Frame>) -> Vec<Frame> {
        let (mut client, mut server) = duplex(64);
        let expected = frames.clone();
        let writer = tokio::spawn(async move {
            let mut sink = FramedWrite::new(&mut client, FrameCodec);
            for frame in &expected {
                sink.send(frame.clone()).await.unwrap();
            }
        });
        let reader = tokio::spawn(async move {
            FramedRead::new(&mut server, FrameCodec)
                .map(|r| r.expect("frame decode"))
                .collect::<Vec<_>>()
                .await
        });
        writer.await.unwrap();
        reader.await.unwrap()
    }

    #[tokio::test]
    async fn control_frame_roundtrips_envelope() {
        let env = Envelope {
            msg: Some(envelope::Msg::Welcome(Welcome {
                protocol_version: PROTOCOL_VERSION,
                kernel_name: "echo".into(),
                kernel_version: "0.1.0".into(),
            })),
        };
        let back = roundtrip(vec![Frame::control(&env)]).await;
        assert_eq!(back.len(), 1);
        match back[0].envelope().unwrap().msg {
            Some(envelope::Msg::Welcome(w)) => {
                assert_eq!(w.kernel_name, "echo");
                assert_eq!(w.protocol_version, PROTOCOL_VERSION);
            }
            other => panic!("unexpected envelope: {other:?}"),
        }
    }

    #[tokio::test]
    async fn arrow_frame_roundtrips_payload() {
        let payload = Bytes::from_static(b"arrow-ipc-bytes");
        let back = roundtrip(vec![Frame::arrow(payload.clone())]).await;
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].tag, FrameTag::Arrow);
        assert_eq!(back[0].payload, payload);
    }

    #[tokio::test]
    async fn mixed_frames_keep_order_and_type() {
        let hello = Envelope {
            msg: Some(envelope::Msg::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                client: "harness".into(),
            })),
        };
        let frames = vec![
            Frame::control(&hello),
            Frame::arrow(Bytes::from_static(b"a")),
            Frame::arrow(Bytes::from_static(b"bb")),
            Frame::control(&hello),
        ];
        let back = roundtrip(frames).await;
        assert_eq!(back.len(), 4);
        assert_eq!(back[0].tag, FrameTag::Control);
        assert_eq!(back[1].tag, FrameTag::Arrow);
        assert_eq!(&back[1].payload[..], b"a");
        assert_eq!(&back[2].payload[..], b"bb");
        assert_eq!(back[3].tag, FrameTag::Control);
        assert!(matches!(
            back[3].envelope().unwrap().msg,
            Some(envelope::Msg::Hello(_))
        ));
    }

    #[test]
    fn decoder_rejects_unknown_tag_and_oversize_len() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0x7f, 0, 0, 0, 0]);
        let err = FrameCodec.decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[TAG_CONTROL, 0xff, 0xff, 0xff, 0xff]);
        let err = FrameCodec.decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decoder_waits_for_full_payload() {
        let env = Envelope {
            msg: Some(envelope::Msg::Hello(Hello {
                protocol_version: 1,
                client: "x".into(),
            })),
        };
        let frame = Frame::control(&env);
        let mut wire = BytesMut::new();
        FrameCodec.encode(frame.clone(), &mut wire).unwrap();

        // Truncated header -> nothing yet.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&wire[..4]);
        assert!(FrameCodec.decode(&mut buf).unwrap().is_none());

        // Rest of the frame arrives -> yields exactly one control frame.
        buf.extend_from_slice(&wire[4..]);
        let got = FrameCodec
            .decode(&mut buf)
            .unwrap()
            .expect("complete frame yields");
        assert_eq!(got, frame);
        assert!(FrameCodec.decode(&mut buf).unwrap().is_none());
    }
}
