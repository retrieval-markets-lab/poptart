use cid::Cid;
use clap::Parser;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use iroh_car::CarReader;
use libipld::prelude::Codec as _;
use libipld::{Ipld, IpldCodec};
use libp2p::{
    identify, identify::Event as IdentifyEvent, identity::Keypair, multiaddr::Protocol,
    swarm::SwarmEvent, Multiaddr, Swarm,
};
use log::{debug, info, warn};
use std::collections::BTreeSet;
use std::env;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;

mod behaviour;
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
    let is_relay_client = if let CliArgument::Relay = opt.argument {
        info!("you are a hole-punching 🧃 relay. thank you for your service 🫡.");
        false
    } else {
        info!("we'll be punching 🧃 through NATs today.");
        true
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
    let (transport, relay_client) = build_transport(&keys, is_relay_client).await;

    let behaviour =
        PopTartBehaviour::new(&keys, store.clone(), is_relay_client, relay_client).await;

    let swarm = Swarm::with_tokio_executor(transport, behaviour, peer_id);

    let (cmd_sender, cmd_receiver) = mpsc::channel(0);
    let (evt_sender, mut evt_receiver) = mpsc::channel(0);

    let ev_loop = EventLoop::new(swarm, cmd_receiver, evt_sender);

    tokio::task::spawn(ev_loop.run());

    let mut client = Client::new(cmd_sender);

    client
        .start("/ip4/0.0.0.0/tcp/2001".parse()?)
        .await
        .expect("swarm to start listening");

    match opt.argument {
        CliArgument::Provide { relay, path } => {
            client.connect_to_relay(relay).await?;
            loop {
                let file = File::open(&path).await?;
                let buf_reader = BufReader::new(file);

                let car_reader = CarReader::new(buf_reader).await?;
                let root = car_reader.header().roots()[0];
                let stream = car_reader.stream().boxed();

                let store_clone = store.clone();
                stream
                    .try_for_each(move |(cid, data)| {
                        let store = store_clone.clone();
                        async move {
                            let links = parse_links(&cid, &data).unwrap_or_default();
                            store.put(cid, &data, links)?;
                            Ok(())
                        }
                    })
                    .await?;
                info!("imported car file with root {:?} into store...", root);
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
    #[clap(long)]
    tempstore: bool,
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
    swarm: Swarm<PopTartBehaviour<RockStore>>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
}

impl EventLoop {
    fn new(
        swarm: Swarm<PopTartBehaviour<RockStore>>,
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
                            info!(
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
                let behaviour = self.swarm.behaviour();
                let _ = match behaviour.bitswap.client().get_block(&root).await {
                    Ok(_) => sender.send(Ok(())),
                    Err(err) => sender.send(Err(err)),
                };
            }
            Command::ConnectRelay { relay } => {
                if let Err(err) = self.swarm.dial(relay) {
                    warn!("failed to dial relay: {err:?}");
                }
                let mut learned_observed_addr = false;
                let mut told_relay_observed_addr = false;

                loop {
                    match self.swarm.next().await.unwrap() {
                        SwarmEvent::Behaviour(Event::Identify(IdentifyEvent::Sent { .. })) => {
                            info!("told relay its public address 🙊");
                            told_relay_observed_addr = true;
                        }
                        SwarmEvent::Behaviour(Event::Identify(IdentifyEvent::Received {
                            info: identify::Info { observed_addr, .. },
                            ..
                        })) => {
                            info!("relay told us our public address: {:?}", observed_addr);
                            learned_observed_addr = true;
                        }
                        event => debug!("{:?}", event),
                    }

                    if learned_observed_addr && told_relay_observed_addr {
                        break;
                    }
                }
            }
        }
    }
}
