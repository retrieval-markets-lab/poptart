use ahash::AHashMap;
use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use clap::Parser;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use iroh_bitswap::{Block, Store};
use iroh_car::CarReader;
use libp2p::{identity::Keypair, multiaddr::Protocol, swarm::SwarmEvent, Multiaddr, Swarm};
use log::{debug, info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::sync::RwLock;
mod behaviour;
mod transport;

pub use behaviour::*;
pub use transport::*;

#[derive(Debug, Clone, Default)]
struct MemStore {
    store: Arc<RwLock<AHashMap<Cid, Block>>>,
}

#[async_trait]
impl Store for MemStore {
    async fn get_size(&self, cid: &Cid) -> anyhow::Result<usize> {
        self.store
            .read()
            .await
            .get(cid)
            .map(|block| block.data().len())
            .ok_or_else(|| anyhow::format_err!("missing"))
    }

    async fn get(&self, cid: &Cid) -> anyhow::Result<Block> {
        self.store
            .read()
            .await
            .get(cid)
            .cloned()
            .ok_or_else(|| anyhow::format_err!("missing"))
    }

    async fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        Ok(self.store.read().await.contains_key(cid))
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    colog::init();
    info!("starting poptart 🍭 ...");
    let opt = Opt::parse();
    let is_relay_client = if let CliArgument::Relay = opt.argument {
        false
    } else {
        info!("we'll be punching 🧃 through NATs today.");
        true
    };
    let keys = Keypair::generate_ed25519();
    let peer_id = keys.public().to_peer_id();

    let store = MemStore::default();
    info!("setting up transport 🏎️ ...");
    let (transport, relay_client) = build_transport(&keys, is_relay_client).await;

    let behaviour =
        PopTartBehaviour::new(&peer_id, store.clone(), is_relay_client, relay_client).await;

    let swarm = Swarm::with_tokio_executor(transport, behaviour, peer_id);

    let (cmd_sender, cmd_receiver) = mpsc::channel(0);
    let (evt_sender, mut evt_receiver) = mpsc::channel(0);

    let ev_loop = EventLoop::new(swarm, cmd_receiver, evt_sender);

    tokio::task::spawn(ev_loop.run());

    let mut client = Client::new(cmd_sender);

    client
        .start("/ip4/0.0.0.0/tcp/0".parse()?)
        .await
        .expect("swarm to start listening");

    match opt.argument {
        CliArgument::Provide { relay, path } => {
            client.connect_to_relay(relay).await?;
            loop {
                let file = File::open(&path).await?;
                let buf_reader = BufReader::new(file);

                let car_reader = CarReader::new(buf_reader).await?;
                let stream = car_reader.stream().boxed();

                let store_clone = store.clone();
                stream
                    .try_for_each(move |(cid, data)| {
                        let store = store_clone.clone();
                        async move {
                            let bytes = Bytes::from(data);
                            store
                                .store
                                .write()
                                .await
                                .insert(cid, Block::new(bytes.into(), cid));
                            Ok(())
                        }
                    })
                    .await?;
                match evt_receiver.next().await {
                    _ => (),
                }
            }
        }
        CliArgument::Resolve { peers, root, relay } => {
            client.connect_to_relay(relay).await?;
            client.resolve(peers, root).await?;
        }
        CliArgument::Relay => match evt_receiver.next().await {
            _ => (),
        },
    };

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(name = "poptart")]
struct Opt {
    #[clap(subcommand)]
    argument: CliArgument,
}

#[derive(Debug, Parser)]
enum CliArgument {
    Relay,
    Provide {
        #[clap(long)]
        path: PathBuf,
        #[clap(long)]
        relay: Multiaddr,
    },
    Resolve {
        #[clap(long)]
        root: Cid,
        #[clap(long)]
        peers: Vec<Multiaddr>,
        #[clap(long)]
        relay: Multiaddr,
    },
}

#[derive(Debug)]
enum Command {
    ConnectRelay {
        relay: Multiaddr,
    },
    Start {
        addr: Multiaddr,
        sender: oneshot::Sender<anyhow::Result<()>>,
    },
    Resolve {
        providers: Vec<Multiaddr>,
        root: Cid,
        sender: oneshot::Sender<anyhow::Result<()>>,
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

    /// Opens a connection with some peers and resolves IPFS content via Bitswap
    pub async fn resolve(&mut self, addrs: Vec<Multiaddr>, root: Cid) -> anyhow::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Resolve {
                providers: addrs,
                root,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped");
        receiver.await.expect("Sender not to be dropped")
    }
    pub async fn connect_to_relay(&mut self, relay: Multiaddr) -> anyhow::Result<()> {
        self.sender
            .send(Command::ConnectRelay { relay })
            .await
            .expect("Command receiver not to be dropped");
        Ok(())
    }
}

struct EventLoop {
    swarm: Swarm<PopTartBehaviour<MemStore>>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
}

impl EventLoop {
    fn new(
        swarm: Swarm<PopTartBehaviour<MemStore>>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            let local_peer_id = *self.swarm.local_peer_id();
                            println!(
                                "==> node listening on {:?}",
                                address.with(Protocol::P2p(local_peer_id.into()))
                            );
                        }
                        SwarmEvent::Behaviour(Event::Dcutr(event)) => {
                            debug!("dcutr event: {:?}", event);
                        }
                        SwarmEvent::Behaviour(Event::Relay(event)) => {
                            debug!("relay event: {:?}", event)
                        }
                        SwarmEvent::Behaviour(Event::RelayClient(event)) => {
                            debug!("relay client event: {:?}", event)
                        }
                        _ => {}
                    }
                }
                command = self.command_receiver.next() => match command {
                    Some(c) => self.handle_command(c).await,
                    None => return,
                },
            }
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Start { addr, sender } => {
                let _ = match self.swarm.listen_on(addr) {
                    Ok(_) => sender.send(Ok(())),
                    Err(e) => sender.send(Err(e.into())),
                };
            }
            Command::Resolve {
                providers,
                root,
                sender,
            } => {
                for addr in providers {
                    if let Err(err) = self.swarm.dial(addr) {
                        warn!("failed to dial peer: {err:?}");
                    }
                }
                let behaviour = self.swarm.behaviour().clone();
                let _ = match behaviour.bitswap.client().get_block(&root).await {
                    Ok(_) => sender.send(Ok(())),
                    Err(err) => sender.send(Err(err.into())),
                };
            }
            Command::ConnectRelay { relay } => {
                if let Err(err) = self.swarm.dial(relay) {
                    warn!("failed to dial relay: {err:?}");
                }
            }
        }
    }
}
