use crate::prefix::Prefix;
use crate::protocol::{Message, StatusCode, Store};
use anyhow::Result;
use async_stream::try_stream;
use futures::{prelude::*, stream::FuturesUnordered};
use libipld::codec::Codec;
use libipld::codec_impl::IpldCodec;
use libipld::{Cid, Ipld};
use libp2p::{core::connection::ConnectionId, PeerId};
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub type BlockSender = oneshot::Sender<Result<(Ipld, Vec<Cid>)>>;
pub type ResponseSender = oneshot::Sender<Result<Message>>;
pub type NetworkSender = async_channel::Sender<NetEvent>;

#[derive(Debug)]
pub enum NetEvent {
    SendMessage {
        peer: PeerId,
        message: Message,
        connection_id: ConnectionId,
    },
    SendRequest {
        peer: PeerId,
        message: Message,
        connection_id: ConnectionId,
        response: ResponseSender,
    },
}

pub struct Task {
    cid: Cid,
    result: BlockSender,
}

#[derive(Debug)]
pub struct Session<S: Store> {
    store: S,
    // links: deque::Worker<Task>,
    links: async_channel::Sender<Task>,
    workers: Arc<Vec<JoinHandle<()>>>,
}

impl<S: Store> Session<S> {
    pub fn new<'a, P>(store: S, net: NetworkSender, providers: P) -> Self
    where
        P: Iterator<Item = (&'a PeerId, &'a ConnectionId)>,
    {
        let (wx, sx) = async_channel::bounded(128);
        // let (w, s) = deque::new::<Task>();
        let mut workers = Vec::new();
        for (p, c) in providers {
            let network_sender = net.clone();
            let peer = *p;
            let conn_id = *c;
            let mut stealer: async_channel::Receiver<Task> = sx.clone(); // s.clone();
            let store = store.clone();

            // TODO: workers should keep track of peer connectivity, stats etc.
            workers.push(tokio::task::spawn(async move {
                while let Some(task) = stealer.next().await {
                    let (s, r) = oneshot::channel();
                    let _ = network_sender
                        .send(NetEvent::SendRequest {
                            peer,
                            message: task.cid.into(),
                            connection_id: conn_id,
                            response: s,
                        })
                        .await;

                    match r.await {
                        Ok(Ok(res)) => {
                            let result = res
                                .response
                                .as_ref()
                                .ok_or_else(|| anyhow::format_err!("not a response message"))
                                .and_then(|res| {
                                    if let Some(blk) = res.blocks.iter().next() {
                                        return Ok(blk);
                                    }
                                    if let StatusCode::NotFound = res.status {
                                        return Err(anyhow::format_err!("block not found"));
                                    }
                                    Err(anyhow::format_err!("empty message"))
                                })
                                .and_then(|blk| {
                                    let cid =
                                        Prefix::new_from_bytes(&blk.prefix)?.to_cid(&blk.data)?;
                                    let codec = IpldCodec::try_from(cid.codec())?;
                                    let node: Ipld = codec.decode(&blk.data)?;

                                    let mut linkset = BTreeSet::new();
                                    node.references(&mut linkset);
                                    let links = linkset.into_iter().collect::<Vec<Cid>>();

                                    store.put(cid, &blk.data, links.clone())?;
                                    Ok((node, links))
                                });
                            if let Err(err) = task.result.send(result) {
                                warn!("task result sender: {:?}", err);
                            }
                        }
                        Ok(Err(other)) => {
                            warn!("send:{}: failed: {:?}", peer, other);
                        }
                        _ => {}
                    };
                }
            }));
        }
        Session {
            store,
            links: wx,
            workers: Arc::new(workers),
        }
    }

    pub async fn resolve_all(&self, root: Cid) -> impl Stream<Item = Result<Ipld>> + '_ {
        let mut jobs = FuturesUnordered::new();
        jobs.push(self.load_link(root));

        try_stream! {

            while let Some(Ok((node, links))) = jobs.next().await {

                for l in links.into_iter() {
                    jobs.push(self.load_link(l));
                }

                yield node;
            }
        }
    }

    pub async fn load_link(&self, cid: Cid) -> Result<(Ipld, Vec<Cid>)> {
        let (s, r) = oneshot::channel();
        self.links.send(Task { cid, result: s }).await?;
        match r.await {
            Ok(res) => res,
            _ => Err(anyhow::format_err!("sender was dropped")),
        }
    }

    pub fn stop(self) {
        for handle in self.workers.iter() {
            handle.abort();
        }
    }
}

mod tests {
    use super::*;
    use crate::protocol::{
        tests::{create_block, create_random_block, encode_ipld, TestStore, CBOR},
        Block,
    };
    use libipld::ipld;
    use std::collections::HashMap;
    use tracing::info;
    // use tracing_subscriber::{filter::LevelFilter, fmt, prelude::*};

    fn assert_send<T: Send>() {}

    #[test]
    fn test_traits() {
        assert_send::<Session<TestStore>>();
        assert_send::<&Session<TestStore>>();
    }

    async fn get_content() {
        let blocks = (0..16)
            .map(|_| create_random_block::<1024>())
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

        let store = TestStore::default();

        for (cid, bytes, _) in &blocks {
            store.put(*cid, bytes.clone(), vec![]).unwrap();
        }

        let (network_sender, mut network_receiver) = async_channel::bounded(1024);

        tokio::task::spawn(async move {
            while let Some(NetEvent::SendRequest {
                message,
                response,
                connection_id,
                ..
            }) = network_receiver.next().await
            {
                tokio::task::spawn({
                    let store = store.clone();
                    let req = message.request.unwrap();
                    async move {
                        info!("conn {:?} handling request", connection_id);
                        if let Ok(bytes) = store.get(&req.root) {
                            let blk = Block::new(req.root, bytes);
                            let msg = Message::complete_response(vec![blk]);
                            response.send(Ok(msg)).ok();
                        }
                    }
                });
            }
        });

        let providers: HashMap<PeerId, ConnectionId> = HashMap::from([
            (PeerId::random(), ConnectionId::new(1)),
            (PeerId::random(), ConnectionId::new(2)),
            (PeerId::random(), ConnectionId::new(3)),
            (PeerId::random(), ConnectionId::new(4)),
        ]);
        let session = Session::new(TestStore::default(), network_sender, providers.iter());

        let content = session.resolve_all(root).await;

        let results = content.try_collect::<Vec<Ipld>>().await.unwrap();
        assert_eq!(results.len(), blocks.len());
    }

    #[tokio::test]
    async fn test_session() {
        // tracing_subscriber::registry()
        //     .with(fmt::layer().pretty())
        //     .with(LevelFilter::from_level(Level::DEBUG))
        //     .init();

        get_content().await;
    }
}
