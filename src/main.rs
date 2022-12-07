use ahash::HashMap;
use bytes::Bytes;
use cid::Cid;
use clap::Parser;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use iroh_bitswap::Block;
use iroh_car::CarReader;
use libipld::prelude::Codec as _;
use libipld::{Ipld, IpldCodec};
use libp2p::{
    identify,
    identify::Event as IdentifyEvent,
    identity::Keypair,
    multiaddr::Protocol,
    swarm::{ConnectionHandler, IntoConnectionHandler, NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, Swarm,
};
use log::{debug, info, warn};
use smallvec::SmallVec;
use std::path::PathBuf;
use std::time::Instant;
use std::{collections::BTreeSet, str::FromStr};
use std::{env, vec};
use tokio::fs::File;
use tokio::io::BufReader;

mod behaviour;
mod metrics;
mod store;
mod transport;

use behaviour::*;
use store::RockStore;
use transport::*;

fn parse_links(cid: &Cid, bytes: &[u8]) -> anyhow::Result<Vec<Cid>> {
    let mut cids = BTreeSet::new();
    let codec = match cid.codec() {
        0x71 => IpldCodec::DagCbor,
        0x70 => IpldCodec::DagPb,
        0x0129 => IpldCodec::DagJson,
        0x55 => IpldCodec::Raw,
        codec => anyhow::bail!("unsupported codec {:?}", codec),
    };
    codec.references::<Ipld, _>(bytes, &mut cids)?;
    let links = cids.into_iter().collect();
    Ok(links)
}

const POPTART_DIR: &str = ".poptart";
fn poptart_data_root() -> anyhow::Result<PathBuf> {
    if let Some(val) = env::var_os("POPTART_DATA_DIR") {
        return Ok(PathBuf::from(val));
    }
    let path = dirs_next::data_dir().ok_or_else(|| {
        anyhow::format_err!("operating environment provides no directory for application data")
    })?;
    Ok(path.join(POPTART_DIR))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    colog::init();

    info!("starting poptart 🍭 ...");
    let opt = Opt::parse();
    let mut config = PopTartConfig::default();
    config.transfer_protocol = TransferProtocol::from_str(&opt.transfer_protocol)
        .expect("failed to parse protocol string");
    if let CliArgument::Relay { .. } = opt.argument {
        info!("you are a hole-punching 🧃 relay. thank you for your service 🫡.");
        config.is_relay = true;
    };
    if let Some(_) = opt.relay {
        info!("we'll be punching 🧃 through NATs today.");
        config.is_relay_client = true;
    };
    let keys = Keypair::generate_ed25519();
    let peer_id = keys.public().to_peer_id();

    let store_path = if opt.tempstore {
        tempfile::tempdir()?.path().into()
    } else {
        poptart_data_root()?
    };
    let store = if store_path.exists() {
        info!("found existing blockstore, opening...");
        RockStore::open(store_path).await?
    } else {
        info!("creating new blockstore at path {:?}...", store_path);
        RockStore::create(store_path).await?
    };

    info!("setting up transport 🏎️ ...");
    let (transport, relay_client) = build_transport(&keys, config.is_relay_client).await;

    let behaviour = PopTartBehaviour::new(&keys, store.clone(), config, relay_client).await;

    let swarm = Swarm::with_tokio_executor(transport, behaviour, peer_id);

    let (cmd_sender, cmd_receiver) = mpsc::channel(0);
    let (evt_sender, mut evt_receiver) = mpsc::channel(0);

    let ev_loop = EventLoop::new(swarm, store.clone(), cmd_receiver, evt_sender);

    tokio::task::spawn(ev_loop.run());

    let mut client = Client::new(cmd_sender);

    match opt.argument {
        CliArgument::Provide { path, port } => {
            client
                .start(format!("/ip4/0.0.0.0/tcp/{:}", port).parse()?)
                .await
                .expect("swarm to start listening");

            if let Some(addr) = opt.relay {
                client.dial(addr).await?;
            }

            let file = File::open(&path).await?;
            let buf_reader = BufReader::new(file);

            let car_reader = CarReader::new(buf_reader).await?;
            let root = car_reader.header().roots()[0];
            let mut stream = car_reader.stream().boxed();

            while let Some(Ok((cid, data))) = stream.next().await {
                let data = Bytes::from(data);
                let blk = Block::new(data, cid);
                let links = parse_links(&cid, blk.data()).unwrap_or_default();
                store.put(cid, blk.data(), links)?;
                if let TransferProtocol::Bitswap = config.transfer_protocol {
                    client.notify_new_blocks_bitswap(vec![blk]).await?;
                }
            }
            info!("imported car file with root {:?} into store...", root);

            loop {
                match evt_receiver.next().await {
                    _ => (),
                }
            }
        }
        CliArgument::Resolve { peers, root, port } => {
            client
                .start(format!("/ip4/0.0.0.0/tcp/{:}", port).parse()?)
                .await
                .expect("swarm to start listening");

            for peer in peers {
                client.dial(peer).await?;
            }
            client.resolve(root).await?;
            info!("resolved content");
        }
        CliArgument::Relay { port } => {
            client
                .start(format!("/ip4/0.0.0.0/tcp/{:}", port).parse()?)
                .await
                .expect("swarm to start listening");

            loop {
                match evt_receiver.next().await {
                    _ => (),
                }
            }
        }
    };

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "poptart")]
struct Opt {
    #[clap(long)]
    tempstore: bool,
    #[clap(long)]
    relay: Option<Multiaddr>,
    #[clap(long)]
    transfer_protocol: String,
    #[clap(subcommand)]
    argument: CliArgument,
}

#[derive(Debug, Parser)]
enum CliArgument {
    Relay {
        #[arg(default_value_t = 2001)]
        port: u16,
    },
    Provide {
        #[clap(long)]
        path: PathBuf,
        #[arg(default_value_t = 2001)]
        port: u16,
    },
    Resolve {
        #[clap(long)]
        root: Cid,
        #[clap(long)]
        peers: Vec<Multiaddr>,
        #[arg(default_value_t = 2002)]
        port: u16,
    },
}

#[derive(Debug)]
enum Command {
    Dial {
        maddr: Multiaddr,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    Start {
        addr: Multiaddr,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    Resolve {
        root: Cid,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    NotifyNewBlocks {
        blocks: Vec<Block>,
    },
}

#[derive(Clone)]
struct Client {
    sender: mpsc::Sender<Command>,
}

impl Client {
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Client { sender }
    }

    /// Listen for incoming connections on the given address
    pub async fn start(&mut self, addr: Multiaddr) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Start { addr, sender })
            .await
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }

    /// Resolves IPFS content via Bitswap
    pub async fn resolve(&mut self, root: Cid) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Resolve { root, sender })
            .await
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }

    /// Tries to open a connection with the peer at the given address. Must be a p2p address,
    /// so a multiaddrs containing a peer ID. The receiver will return the error if the dial fails.
    pub async fn dial(&mut self, maddr: Multiaddr) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Dial { maddr, sender })
            .await
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }

    /// Notify bitswap about new blocks. Don't really wait for it to complete.
    pub async fn notify_new_blocks_bitswap(&mut self, blocks: Vec<Block>) -> anyhow::Result<()> {
        self.sender
            .send(Command::NotifyNewBlocks { blocks })
            .await
            .expect("Command receiver not to be dropped");
        Ok(())
    }
}

struct EventLoop {
    swarm: Swarm<PopTartBehaviour>,
    store: RockStore,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    dial_requests: HashMap<PeerId, oneshot::Sender<anyhow::Result<()>>>,
}

impl EventLoop {
    fn new(
        swarm: Swarm<PopTartBehaviour>,
        store: RockStore,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            store,
            command_receiver,
            event_sender,
            dial_requests: Default::default(),
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => self.handle_swarm_event(event),
                command = self.command_receiver.next() => match command {
                    Some(c) => self.handle_command(c),
                    None => return,
                },
            }
        }
    }

    fn handle_swarm_event(
        &mut self,
        event: SwarmEvent<
            <PopTartBehaviour as NetworkBehaviour>::OutEvent,
            <<<PopTartBehaviour as NetworkBehaviour>::ConnectionHandler as IntoConnectionHandler>::Handler as ConnectionHandler>::Error>,
    ) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = *self.swarm.local_peer_id();
                info!(
                    "==> node listening on {:?}",
                    address.with(Protocol::P2p(local_peer_id.into()))
                );
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                if let Some(peer_id) = peer_id {
                    self.dial_requests
                        .remove(&peer_id)
                        .and_then(|sender| sender.send(Err(error.into())).ok());
                }
            }
            SwarmEvent::Behaviour(event) => self.handle_behaviour_event(event),
            _ => {}
        }
    }

    fn handle_behaviour_event(&mut self, event: Event) {
        match event {
            Event::Dcutr(event) => {
                debug!("dcutr event: {:?}", event);
            }
            Event::Relay(event) => {
                debug!("relay event: {:?}", event)
            }
            Event::RelayClient(event) => {
                debug!("relay client event: {:?}", event)
            }
            Event::Identify(e) => {
                if let IdentifyEvent::Received {
                    peer_id,
                    info:
                        identify::Info {
                            observed_addr,
                            protocols,
                            ..
                        },
                } = e
                {
                    info!("peer told us our public address: {:?}", observed_addr);
                    if let Some(bitswap) = self.swarm.behaviour().bitswap.as_ref() {
                        bitswap.on_identify(&peer_id, &protocols);
                    }
                    self.dial_requests
                        .remove(&peer_id)
                        .and_then(|sender| sender.send(Ok(())).ok());
                }
            }
            Event::TorkEvent => {}
            Event::Bitswap(e) => {
                debug!("bitswap event: {:?}", e);
                let _ = self.event_sender.send(Event::Bitswap(e));
            }
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Start { addr, sender } => {
                let _ = match self.swarm.listen_on(addr) {
                    Ok(_) => sender.send(Ok(())),
                    Err(e) => sender.send(Err(e.into())),
                };
            }
            Command::Dial { maddr, sender } => {
                if let Some(Protocol::P2p(hash)) = maddr.iter().last() {
                    let pid = PeerId::from_multihash(hash).expect("invalid multihash");
                    self.dial_requests.insert(pid, sender);
                    if let Err(err) = self.swarm.dial(maddr) {
                        warn!("failed to dial peer: {err:?}");
                        let _ = self
                            .dial_requests
                            .remove(&pid)
                            .expect("to have a peer id")
                            .send(Err(err.into()));
                    }
                } else {
                    let _ =
                        sender.send(Err(anyhow::format_err!("multiaddr was not a p2p address")));
                }
            }
            Command::NotifyNewBlocks { blocks } => {
                if let Some(bitswap) = self.swarm.behaviour().bitswap.as_ref() {
                    let client = bitswap.client().clone();
                    // avoid blocking other events
                    tokio::task::spawn(async move {
                        if let Err(err) = client.notify_new_blocks(&blocks).await {
                            warn!("failed to notify bitswap about blocks: {:?}", err);
                        }
                    });
                } else {
                    warn!("tried to notify bitswap of new blocks, but bitswap is not enabled.")
                }
            }
            // Currently all we do here is follow every link we can find and store the blocks.
            Command::Resolve { root, sender } => {
                let behaviour = self.swarm.behaviour();
                let store = self.store.clone();

                if let Some(bitswap) = behaviour.bitswap.as_ref() {
                    let client = bitswap.client().clone();
                    tokio::task::spawn(async move {
                        let start = Instant::now();

                        let session = client.new_session().await;

                        let mut stack: SmallVec<[vec::IntoIter<Cid>; 8]> = SmallVec::new();
                        stack.push(vec![root].into_iter());

                        while !stack.is_empty() {
                            let next = stack.last_mut().expect("stack should be non-empty").next();

                            match next {
                                None => {
                                    stack.pop();
                                }
                                Some(cid) => {
                                    if let Ok(blk) = session.get_block(&cid).await {
                                        if let Ok(links) = parse_links(blk.cid(), blk.data()) {
                                            stack.push(links.clone().into_iter());
                                            let _ = store.put(cid, blk.data(), links);
                                        }
                                    };
                                }
                            }
                        }
                        info!("completed transfer in {}ms", start.elapsed().as_millis());

                        let _ = session.stop().await;
                        let _ = sender.send(Ok(()));
                    });
                } else if let Some(tork) = behaviour.tork.as_ref() {
                    let session = match tork.new_session() {
                        Ok(session) => session,
                        Err(err) => {
                            sender.send(Err(err)).ok();
                            return;
                        }
                    };

                    tokio::task::spawn(async move {
                        let start = Instant::now();

                        let stream = session.resolve_all(root).await;

                        if let Err(err) = stream.try_collect::<Vec<_>>().await {
                            sender.send(Err(err)).ok();
                        } else {
                            sender.send(Ok(())).ok();
                        }
                        info!("completed transfer in {}ms", start.elapsed().as_millis());

                        session.stop();
                    });
                }
            }
        }
    }
}
