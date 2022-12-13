use anyhow::Result;
use asynchronous_codec::Framed;
use futures::prelude::*;
use libipld::Ipld;
use libp2p::core::{
    connection::{ConnectedPoint, ConnectionId},
    InboundUpgrade, OutboundUpgrade,
};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, ConnectionHandlerUpgrErr, IntoConnectionHandler,
    KeepAlive, NegotiatedSubstream, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler,
    PollParameters, SubstreamProtocol,
};
use libp2p::{Multiaddr, PeerId};
use smallvec::SmallVec;
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tracing::info;

mod prefix;
mod protocol;
mod session;

pub use self::protocol::{tests::*, Store};
use self::protocol::{Block, Message, TorkCodec, TorkProtocol};
use self::session::{NetEvent, NetworkSender, ResponseSender, Session};

enum ConnState {
    Connected(ConnectionId),
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct Tork<S: Store> {
    store: S,
    self_id: PeerId,
    conns: Arc<Mutex<HashMap<PeerId, ConnectionId>>>,
    network_receiver: async_channel::Receiver<NetEvent>,
    network_sender: NetworkSender,
}

impl<S: Store> Tork<S> {
    pub fn new(self_id: PeerId, store: S) -> Self {
        let (network_sender, network_receiver) = async_channel::bounded(1024);
        Tork {
            store,
            self_id,
            conns: Default::default(),
            network_sender,
            network_receiver,
        }
    }

    pub fn new_session(&self) -> Result<Session<S>> {
        let conns = self.conns.lock().unwrap();
        if conns.len() == 0 {
            return Err(anyhow::format_err!("no connected providers"));
        }

        Ok(Session::new(
            self.store.clone(),
            self.network_sender.clone(),
            conns.iter(),
        ))
    }

    // fn get_conn_id(&self, peer: &PeerId) -> Option<ConnectionId> {
    //     self.conns.lock().unwrap().get(peer).copied()
    // }

    fn set_conn_state(&self, peer: &PeerId, new_state: ConnState) {
        let peers = &mut *self.conns.lock().unwrap();
        let peer = *peer;
        match peers.entry(peer) {
            std::collections::hash_map::Entry::Occupied(mut entry) => match new_state {
                ConnState::Connected(id) => {
                    *entry.get_mut() = id;
                }
                ConnState::Disconnected => {
                    entry.remove();
                }
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                if let ConnState::Connected(id) = new_state {
                    entry.insert(id);
                }
            }
        }
    }
}

impl<S: Store> NetworkBehaviour for Tork<S> {
    type ConnectionHandler = TorkHandler;
    type OutEvent = ();

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        TorkHandler::new()
    }

    fn addresses_of_peer(&mut self, _peer_id: &PeerId) -> Vec<Multiaddr> {
        Default::default()
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        connection: &ConnectionId,
        _endpoint: &ConnectedPoint,
        _failed_addresses: Option<&Vec<Multiaddr>>,
        _other_established: usize,
    ) {
        self.set_conn_state(peer_id, ConnState::Connected(*connection));
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        _conn: &ConnectionId,
        _endpoint: &ConnectedPoint,
        _handler: <Self::ConnectionHandler as IntoConnectionHandler>::Handler,
        _remaining_established: usize,
    ) {
        // TODO: handle multiple connection if that happens?
        // not sure why a peer would need to keep multiple connections open with the same peer...
        self.set_conn_state(peer_id, ConnState::Disconnected)
    }

    fn inject_event(&mut self, peer: PeerId, connection_id: ConnectionId, event: Message) {
        if let Some(req) = event.request {
            info!("{:} received a request", self.self_id);
            tokio::task::spawn({
                let store = self.store.clone();
                let sender = self.network_sender.clone();
                async move {
                    let msg = if let Ok(bytes) = store.get(&req.root) {
                        let blk = Block::new(req.root, bytes);
                        Message::complete_response(vec![blk])
                    } else {
                        Message::not_found()
                    };
                    let _ = sender
                        .send(NetEvent::SendMessage {
                            peer,
                            message: msg,
                            connection_id,
                        })
                        .await;
                }
            });
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        match Pin::new(&mut self.network_receiver).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Pending,
            Poll::Ready(Some(ev)) => match ev {
                NetEvent::SendMessage {
                    peer,
                    message,
                    connection_id,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id: peer,
                        handler: NotifyHandler::One(connection_id),
                        event: HandlerInEvent::Response(message),
                    });
                }
                NetEvent::SendRequest {
                    peer,
                    message,
                    connection_id,
                    response,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id: peer,
                        handler: NotifyHandler::One(connection_id),
                        event: HandlerInEvent::Request(message, response),
                    });
                }
            },
        }
    }
}

enum SubstreamState {
    /// Waiting for a message from the remote. The idle state for an inbound substream.
    WaitingInput(Framed<NegotiatedSubstream, TorkCodec>),
    /// Waiting for the user to send a message. The idle state for an outbound substream.
    WaitingOutput(Framed<NegotiatedSubstream, TorkCodec>),
    /// Waiting to send a message to the remote with a future response.
    PendingSend(Framed<NegotiatedSubstream, TorkCodec>, Message),
    PendingSendWithResponse(
        Framed<NegotiatedSubstream, TorkCodec>,
        (Message, ResponseSender),
    ),
    /// Waiting to flush the substream so that the data arrives to the remote.
    PendingFlush(Framed<NegotiatedSubstream, TorkCodec>),
    PendingFlushWithResponse(Framed<NegotiatedSubstream, TorkCodec>, ResponseSender),
    /// Waiting to read the response from the remote
    PendingResponse(Framed<NegotiatedSubstream, TorkCodec>, ResponseSender),
    /// The substream is being closed.
    Closing(Framed<NegotiatedSubstream, TorkCodec>),
    /// An error occurred during processing.
    Poisoned,
}
// time in sec
const INITIAL_KEEP_ALIVE: u64 = 30;

pub struct TorkHandler {
    /// Upgrade configuration for the bitswap protocol.
    listen_protocol: SubstreamProtocol<TorkProtocol, ()>,
    /// The single long-lived substream.
    substream: Option<SubstreamState>,
    send_queue: SmallVec<[HandlerInEvent; 16]>,
    outbound_substream_establishing: bool,
    keep_alive: KeepAlive,
    /// The amount of time we allow idle connections before disconnecting.
    idle_timeout: Duration,
}

impl TorkHandler {
    fn new() -> Self {
        TorkHandler {
            listen_protocol: SubstreamProtocol::new(TorkProtocol(), ()),
            substream: None,
            send_queue: SmallVec::new(),
            outbound_substream_establishing: false,
            keep_alive: KeepAlive::Until(Instant::now() + Duration::from_secs(INITIAL_KEEP_ALIVE)),
            idle_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub enum HandlerInEvent {
    Request(Message, ResponseSender),
    Response(Message),
}

impl ConnectionHandler for TorkHandler {
    type InEvent = HandlerInEvent;
    type OutEvent = Message;
    type Error = io::Error;
    type InboundOpenInfo = ();
    type InboundProtocol = TorkProtocol;
    type OutboundOpenInfo = (Message, ResponseSender);
    type OutboundProtocol = TorkProtocol;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        self.listen_protocol.clone()
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        protocol: <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
        _info: Self::InboundOpenInfo,
    ) {
        self.substream = Some(SubstreamState::WaitingInput(protocol));
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Output,
        message: Self::OutboundOpenInfo,
    ) {
        info!("negociated outbound");
        self.substream = Some(SubstreamState::PendingSendWithResponse(protocol, message));
    }

    fn inject_event(&mut self, event: HandlerInEvent) {
        self.send_queue.push(event);
        self.keep_alive = KeepAlive::Yes;
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _: Self::OutboundOpenInfo,
        e: ConnectionHandlerUpgrErr<
            <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Error,
        >,
    ) {
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ConnectionHandlerEvent<TorkProtocol, (Message, ResponseSender), Message, io::Error>>
    {
        if !self.send_queue.is_empty()
            && self.substream.is_none()
            && !self.outbound_substream_establishing
        {
            let message = self.send_queue.remove(0);
            self.send_queue.shrink_to_fit();
            self.outbound_substream_establishing = true;

            if let HandlerInEvent::Request(msg, sender) = message {
                info!("sending outbound request");
                return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                    protocol: self.listen_protocol.clone().map_info(|()| (msg, sender)),
                });
            } else {
                self.send_queue.push(message);
            }
        }
        // read messages from inbound substream first
        loop {
            match std::mem::replace(&mut self.substream, Some(SubstreamState::Poisoned)) {
                Some(SubstreamState::WaitingInput(mut substream)) => {
                    match substream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(message))) => {
                            // reset keep alive idle timeout
                            self.keep_alive = KeepAlive::Until(Instant::now() + self.idle_timeout);

                            // we've received an inbound, now we'll wait to send a message on that
                            // stream.
                            self.substream = Some(SubstreamState::WaitingOutput(substream));
                            return Poll::Ready(ConnectionHandlerEvent::Custom(message));
                        }
                        Poll::Ready(Some(Err(error))) => {
                            // More serious errors, close this side of the stream. If the
                            // peer is still around, they will re-establish their connection
                            self.substream = Some(SubstreamState::Closing(substream));
                        }
                        // peer closed the stream
                        Poll::Ready(None) => {
                            self.substream = Some(SubstreamState::Closing(substream));
                        }
                        Poll::Pending => {
                            self.substream = Some(SubstreamState::WaitingInput(substream));
                            break;
                        }
                    }
                }
                Some(SubstreamState::WaitingOutput(substream)) => {
                    if !self.send_queue.is_empty() {
                        let message = self.send_queue.remove(0);
                        self.send_queue.shrink_to_fit();

                        match message {
                            HandlerInEvent::Response(message) => {
                                self.substream =
                                    Some(SubstreamState::PendingSend(substream, message));
                            }
                            HandlerInEvent::Request(message, sender) => {
                                self.substream = Some(SubstreamState::PendingSendWithResponse(
                                    substream,
                                    (message, sender),
                                ));
                            }
                        };
                    } else {
                        self.substream = Some(SubstreamState::WaitingOutput(substream));
                        break;
                    }
                }
                Some(SubstreamState::PendingSend(mut substream, message)) => {
                    match Sink::poll_ready(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            match Sink::start_send(Pin::new(&mut substream), message) {
                                Ok(()) => {
                                    self.substream = Some(SubstreamState::PendingFlush(substream))
                                }
                                Err(e) => {
                                    return Poll::Ready(ConnectionHandlerEvent::Close(e));
                                }
                            }
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionHandlerEvent::Close(e));
                        }
                        Poll::Pending => {
                            self.keep_alive = KeepAlive::Yes;
                            self.substream = Some(SubstreamState::PendingSend(substream, message));
                            break;
                        }
                    }
                }
                Some(SubstreamState::PendingSendWithResponse(mut substream, (message, sender))) => {
                    match Sink::poll_ready(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            match Sink::start_send(Pin::new(&mut substream), message) {
                                Ok(()) => {
                                    self.substream = Some(SubstreamState::PendingFlushWithResponse(
                                        substream, sender,
                                    ))
                                }
                                Err(e) => {
                                    return Poll::Ready(ConnectionHandlerEvent::Close(e));
                                }
                            }
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionHandlerEvent::Close(e));
                        }
                        Poll::Pending => {
                            self.keep_alive = KeepAlive::Yes;
                            self.substream = Some(SubstreamState::PendingSendWithResponse(
                                substream,
                                (message, sender),
                            ));
                            break;
                        }
                    }
                }
                Some(SubstreamState::PendingFlush(mut substream)) => {
                    match Sink::poll_flush(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            // reset the idle timeout
                            self.keep_alive = KeepAlive::Until(Instant::now() + self.idle_timeout);

                            self.substream = Some(SubstreamState::WaitingInput(substream))
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionHandlerEvent::Close(e))
                        }
                        Poll::Pending => {
                            self.keep_alive = KeepAlive::Yes;
                            self.substream = Some(SubstreamState::PendingFlush(substream));
                            break;
                        }
                    }
                }
                Some(SubstreamState::PendingFlushWithResponse(mut substream, sender)) => {
                    match Sink::poll_flush(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            // reset the idle timeout
                            self.keep_alive = KeepAlive::Until(Instant::now() + self.idle_timeout);

                            self.substream =
                                Some(SubstreamState::PendingResponse(substream, sender))
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionHandlerEvent::Close(e))
                        }
                        Poll::Pending => {
                            self.keep_alive = KeepAlive::Yes;
                            self.substream =
                                Some(SubstreamState::PendingFlushWithResponse(substream, sender));
                            break;
                        }
                    }
                }
                Some(SubstreamState::PendingResponse(mut substream, sender)) => {
                    match substream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(message))) => {
                            sender.send(Ok(message)).ok();
                            // reset keep alive idle timeout
                            self.keep_alive = KeepAlive::Until(Instant::now() + self.idle_timeout);

                            self.substream = Some(SubstreamState::WaitingOutput(substream));
                            break;
                        }
                        Poll::Ready(Some(Err(error))) => {
                            return Poll::Ready(ConnectionHandlerEvent::Close(error));
                        }
                        // peer closed the stream
                        Poll::Ready(None) => {
                            // self.inbound_substream =
                            // Some(InboundSubstreamState::Closing(substream));
                            break;
                        }
                        Poll::Pending => {
                            self.substream =
                                Some(SubstreamState::PendingResponse(substream, sender));
                            break;
                        }
                    }
                }
                Some(SubstreamState::Closing(mut substream)) => {
                    match Sink::poll_close(Pin::new(&mut substream), cx) {
                        Poll::Ready(res) => {
                            if let Err(e) = res {
                                // Don't close the connection but just drop the inbound substream.
                                // In case the remote has more to send, they will open up a new
                                // substream.
                            }
                            self.substream = None;
                            self.keep_alive = KeepAlive::No;
                            break;
                        }
                        Poll::Pending => {
                            self.substream = Some(SubstreamState::Closing(substream));
                            break;
                        }
                    }
                }
                None => {
                    self.substream = None;
                    break;
                }
                Some(SubstreamState::Poisoned) => {
                    unreachable!("Error occurred during inbound stream processing")
                }
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use libipld::ipld;
    use libp2p::swarm::{Swarm, SwarmEvent};
    use tokio::sync::mpsc;
    use tracing::{info, trace, Level};
    use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*};

    use super::*;

    fn assert_send<T: Send + Sync>() {}

    #[test]
    fn test_traits() {
        assert_send::<Tork<TestStore>>();
        assert_send::<&Tork<TestStore>>();
    }

    // Get content for a given number of blocks and providers
    async fn get_content<const NB: usize, const NP: usize>() {
        let blocks = (1..NB)
            .map(|_| create_random_block::<64>())
            .collect::<Vec<_>>();

        let mut links: Vec<Ipld> = Vec::new();
        for (cid, bytes, _) in &blocks {
            links.push(ipld!({
                "Hash": cid,
                "Tsize": bytes.len(),
            }));
        }

        let root_node = ipld!({
            "Links": links,
        });

        let buf = encode_ipld(CBOR, &root_node);
        let (root, bytes, _) = create_block(buf, CBOR);

        let blocks = {
            let mut rt_block = vec![(root, bytes, root_node)];
            rt_block.extend(blocks);
            rt_block
        };

        let mut providers = Vec::new();
        let (tx, mut rx) = mpsc::channel::<Multiaddr>(1);

        for _ in 0..NP {
            let (peer_id, trans) = mk_transport();
            let store = TestStore::default();
            let t = Tork::new(peer_id, store.clone());
            let mut swarm = Swarm::with_tokio_executor(trans, t, peer_id);

            for (cid, bytes, _) in &blocks {
                store.put(*cid, bytes.clone(), vec![]).unwrap();
            }

            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

            let sender = tx.clone();
            providers.push(tokio::task::spawn(async move {
                while swarm.next().now_or_never().is_some() {}
                let listeners: Vec<_> = Swarm::listeners(&swarm).collect();
                for l in listeners {
                    sender.send(l.clone()).await.unwrap();
                }

                loop {
                    let ev = swarm.next().await;
                    trace!("peer1: {:?}", ev);
                }
            }));
        }

        let (peer2_id, trans) = mk_transport();
        let store2 = TestStore::default();

        let t2 = Tork::new(peer2_id, store2.clone());
        let mut swarm2 = Swarm::with_tokio_executor(trans, t2, peer2_id);

        let swarm2_tk = swarm2.behaviour().clone();

        for i in 0..NP {
            let addr = rx.recv().await.unwrap();
            info!("client: dialing peer{} at {}", i, addr);
            Swarm::dial(&mut swarm2, addr).unwrap();
        }

        let mut conns = 0;

        loop {
            match swarm2.next().await {
                Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                    trace!("client: connected to {}", peer_id);
                    conns += 1;

                    if conns == NP {
                        break;
                    }
                }
                _ => (),
            }
        }

        let client = tokio::task::spawn(async move {
            loop {
                let ev = swarm2.next().await;
                trace!("peer2: {:?}", ev);
            }
        });

        let session = swarm2_tk.new_session().unwrap();
        let content = session.resolve_all(root).await;

        let results = content.try_collect::<Vec<Ipld>>().await.unwrap();

        assert_eq!(results.len(), NB);

        for (cid, bytes, _) in blocks.into_iter() {
            let stored = store2.get(&cid).unwrap();
            assert_eq!(bytes, stored);
        }

        for provider in providers {
            provider.abort();
            provider.await.ok();
        }

        client.abort();
        client.await.ok();
    }

    #[tokio::test]
    async fn test_get_1_block_1_provider() {
        get_content::<1, 1>().await;
    }

    #[tokio::test]
    async fn test_get_4_blocks_1_provider() {
        get_content::<4, 1>().await;
    }

    #[tokio::test]
    async fn test_get_4_blocks_2_providers() {
        // tracing_subscriber::registry()
        //     .with(fmt::layer().pretty())
        //     .with(LevelFilter::from_level(Level::DEBUG))
        //     .init();

        get_content::<4, 2>().await;
    }

    #[tokio::test]
    async fn test_get_64_blocks_4_providers() {
        get_content::<64, 4>().await;
    }

    #[tokio::test]
    async fn test_get_128_blocks_4_providers() {
        get_content::<128, 4>().await;
    }

    #[tokio::test]
    async fn test_get_600_blocks_4_providers() {
        get_content::<600, 4>().await;
    }
}
