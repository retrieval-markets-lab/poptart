use crate::prefix::Prefix;
use anyhow::Result;
use asynchronous_codec::{Decoder, Encoder, Framed};
use bytes::{Bytes, BytesMut};
use futures::future::{self, BoxFuture};
use futures::io::{AsyncRead, AsyncWrite};
use libipld::Cid;
use libp2p::core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::{from_slice, to_vec};
use serde_repr::*;
use serde_tuple::*;
use std::{io, iter};
use unsigned_varint::codec;

pub trait Store: std::fmt::Debug + Clone + Send + Sync + 'static {
    fn get(&self, cid: &Cid) -> Result<Bytes>;
    fn put<T: AsRef<[u8]>, L: IntoIterator<Item = Cid>>(
        &self,
        cid: Cid,
        bytes: T,
        links: L,
    ) -> Result<()>;
}

const MAX_BUF_SIZE: usize = 1024 * 1024 * 2;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub root: Cid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct Block {
    pub prefix: Bytes,
    pub data: Bytes,
}

impl Block {
    pub fn new(cid: Cid, data: Bytes) -> Self {
        let prefix = Bytes::from(Prefix::from(&cid).to_bytes());
        Block { prefix, data }
    }
}

#[derive(PartialEq, Clone, Copy, Eq, Debug, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum StatusCode {
    Partial = 14,
    Completed = 20,
    NotFound = 34,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub status: StatusCode,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    #[serde(rename = "req", skip_serializing_if = "Option::is_none")]
    pub request: Option<Request>,
    #[serde(rename = "resp", skip_serializing_if = "Option::is_none")]
    pub response: Option<Response>,
}

impl Message {
    pub fn complete_response(blocks: Vec<Block>) -> Self {
        Message {
            request: None,
            response: Some(Response {
                status: StatusCode::Completed,
                blocks,
            }),
        }
    }
}

impl From<Request> for Message {
    fn from(req: Request) -> Message {
        Message {
            request: Some(req),
            response: None,
        }
    }
}

impl From<Cid> for Message {
    fn from(cid: Cid) -> Message {
        Message {
            request: Some(Request { root: cid }),
            response: None,
        }
    }
}

pub struct TorkCodec {
    /// Codec to encode/decode the Unsigned varint length prefix of the frames.
    pub length_codec: codec::UviBytes,
}

impl TorkCodec {
    pub fn new(length_codec: codec::UviBytes) -> Self {
        TorkCodec { length_codec }
    }
}

impl Encoder for TorkCodec {
    type Item = Message;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let buf = to_vec(&item).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        // length prefix the protobuf message, ensuring the max limit is not hit
        self.length_codec
            .encode(Bytes::from(buf), dst)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
    }
}

impl Decoder for TorkCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let packet = match self.length_codec.decode(src)? {
            Some(p) => p,
            None => return Ok(None),
        };

        let message: Message = from_slice(&packet.freeze())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        Ok(Some(message))
    }
}

pub const PROTOCOL_NAME: &[u8; 16] = b"/ipfs/tork/0.1.0";

#[derive(Debug, Clone)]
pub struct TorkProtocol();

impl UpgradeInfo for TorkProtocol {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL_NAME)
    }
}

impl<C> InboundUpgrade<C> for TorkProtocol
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Framed<C, TorkCodec>;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, socket: C, _: Self::Info) -> Self::Future {
        let mut length_codec = codec::UviBytes::default();
        length_codec.set_max_len(MAX_BUF_SIZE);
        Box::pin(future::ok(Framed::new(
            socket,
            TorkCodec::new(length_codec),
        )))
    }
}

impl<C> OutboundUpgrade<C> for TorkProtocol
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Framed<C, TorkCodec>;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, socket: C, _: Self::Info) -> Self::Future {
        let mut length_codec = codec::UviBytes::default();
        length_codec.set_max_len(MAX_BUF_SIZE);
        Box::pin(future::ok(Framed::new(
            socket,
            TorkCodec::new(length_codec),
        )))
    }
}

pub mod tests {
    use super::*;
    use futures::channel::oneshot;
    use futures::prelude::*;
    use libipld::cid::multihash::{Code, MultihashDigest};
    use libipld::codec_impl::IpldCodec;
    use libipld::{codec::Encode, Ipld};
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::Boxed;
    use libp2p::core::upgrade::{apply_inbound, apply_outbound, Version};
    use libp2p::identity::Keypair;
    use libp2p::{noise, tcp, yamux, PeerId, Transport};
    use rand::prelude::*;
    use std::{
        collections::HashMap,
        io::{Error, ErrorKind},
        sync::Arc,
        time::Duration,
    };
    use tokio::sync::RwLock;

    pub const RAW: u64 = 0x55;
    pub const CBOR: u64 = 0x71;

    pub fn create_random_block<const S: usize>() -> (Cid, Bytes, Ipld) {
        let mut bytes = BytesMut::with_capacity(S);
        bytes.resize(S, 0);
        thread_rng().fill(&mut bytes[..]);
        create_block(bytes, RAW)
    }

    pub fn create_block<B: Into<Bytes>>(bytes: B, codec: u64) -> (Cid, Bytes, Ipld) {
        let bytes = bytes.into();
        let digest = Code::Sha2_256.digest(&bytes);
        let cid = Cid::new_v1(codec, digest);
        (cid, bytes.clone(), Ipld::Bytes(bytes.into()))
    }

    pub fn encode_ipld(codec: u64, n: &Ipld) -> impl Into<Bytes> {
        let codec = IpldCodec::try_from(codec).unwrap();
        let mut buf = Vec::new();
        Ipld::encode(n, codec, &mut buf).unwrap();
        buf
    }

    #[derive(Debug, Clone, Default)]
    pub struct TestStore {
        store: Arc<RwLock<HashMap<Cid, Bytes>>>,
    }

    impl Store for TestStore {
        fn get(&self, cid: &Cid) -> Result<Bytes> {
            self.store
                .try_read()?
                .get(cid)
                .cloned()
                .ok_or_else(|| anyhow::format_err!("missing"))
        }

        fn put<T: AsRef<[u8]>, L: IntoIterator<Item = Cid>>(
            &self,
            cid: Cid,
            bytes: T,
            _links: L,
        ) -> Result<()> {
            self.store
                .try_write()?
                .insert(cid, bytes.as_ref().to_vec().into());
            Ok(())
        }
    }

    pub fn mk_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox)>) {
        let local_key = Keypair::generate_ed25519();

        let auth_config = {
            let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
                .into_authentic(&local_key)
                .expect("Noise key generation failed");

            noise::NoiseConfig::xx(dh_keys).into_authenticated()
        };

        let peer_id = local_key.public().to_peer_id();
        let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
            .upgrade(Version::V1)
            .authenticate(auth_config)
            .multiplex(yamux::YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|err| Error::new(ErrorKind::Other, err))
            .boxed();
        (peer_id, transport)
    }

    #[tokio::test]
    async fn test_protocol() {
        let (tx, rx) = oneshot::channel();

        let cid: Cid = "bafyreifjthbkzdxj2baze2d2xqs2f73jcu7asubv42elz2vvuw6fdqv4py"
            .parse()
            .unwrap();

        let bg_task = tokio::task::spawn(async move {
            let mut transport =
                tcp::tokio::Transport::new(tcp::Config::default().nodelay(true)).boxed();

            transport
                .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
                .unwrap();

            let addr = transport
                .next()
                .await
                .expect("new address event")
                .into_new_address()
                .expect("listen address");

            tx.send(addr).unwrap();

            let socket = transport
                .next()
                .await
                .expect("request event")
                .into_incoming()
                .unwrap()
                .0
                .await
                .unwrap();

            let msg = apply_inbound(socket, TorkProtocol())
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap();

            assert_eq!(msg.request.expect("to be request").root, cid);
        });
        let client = tokio::task::spawn(async move {
            let mut transport =
                tcp::tokio::Transport::new(tcp::Config::default().nodelay(true)).boxed();

            let socket = transport.dial(rx.await.unwrap()).unwrap().await.unwrap();

            let mut framed = apply_outbound(socket, TorkProtocol(), Version::V1)
                .await
                .unwrap();

            let msg: Message = Request { root: cid }.into();

            framed.send(msg).await.unwrap();
            framed.flush().await.unwrap();

            bg_task.await.unwrap();
        });

        client.await.unwrap();
    }
}
