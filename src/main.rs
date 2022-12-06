use ahash::AHashMap;
use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use clap::Parser;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use iroh_bitswap::{Bitswap, Block, Config, Store};
use iroh_car::CarReader;
use libp2p::{
    core::{self, muxing::StreamMuxerBox, transport::OrTransport},
    dns,
    identity::Keypair,
    multiaddr::Protocol,
    noise, quic,
    swarm::{derive_prelude::EitherOutput, SwarmEvent},
    tcp, websocket,
    yamux::{self, WindowUpdateMode},
    Multiaddr, PeerId, Swarm, Transport,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::sync::RwLock;

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
    let opt = Opt::parse();

    let keys = Keypair::generate_ed25519();

    let tcp_config = tcp::Config::default().port_reuse(true);
    let tcp_transport = tcp::tokio::Transport::new(tcp_config.clone());

    let ws_tcp = websocket::WsConfig::new(tcp::tokio::Transport::new(tcp_config));
    let tcp_ws_transport = tcp_transport.or_transport(ws_tcp);

    let quic_config = quic::Config::new(&keys);
    let quic_transport = quic::tokio::Transport::new(quic_config);

    let auth_config = {
        let dh_keys = noise::Keypair::<noise::X25519Spec>::new().into_authentic(&keys)?;
        noise::NoiseConfig::xx(dh_keys).into_authenticated()
    };

    let mut yamux_config = yamux::YamuxConfig::default();
    yamux_config.set_max_buffer_size(16 * 1024 * 1024);
    yamux_config.set_receive_window_size(16 * 1024 * 1024);
    yamux_config.set_window_update_mode(WindowUpdateMode::on_receive());

    let tcp_ws_transport = tcp_ws_transport
        .upgrade(core::upgrade::Version::V1Lazy)
        .authenticate(auth_config)
        .multiplex(yamux_config)
        .boxed();

    let transport = OrTransport::new(quic_transport, tcp_ws_transport)
        .map(|o, _| match o {
            EitherOutput::First((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            EitherOutput::Second((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    let dns_cfg = dns::ResolverConfig::cloudflare();
    let dns_opts = dns::ResolverOpts::default();
    let transport = dns::TokioDnsConfig::custom(transport, dns_cfg, dns_opts)
        .unwrap()
        .boxed();

    let peer_id = keys.public().to_peer_id();

    let store = MemStore::default();

    let bs = Bitswap::new(peer_id, store.clone(), Config::default()).await;

    let swarm = Swarm::with_tokio_executor(transport, bs, peer_id);

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
        CliArgument::Provide { path } => loop {
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
        },
        CliArgument::Resolve { peers, root } => {
            client.resolve(peers, root).await?;
        }
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
    Provide {
        #[clap(long)]
        path: PathBuf,
    },
    Resolve {
        #[clap(long)]
        root: Cid,
        #[clap(long)]
        peers: Vec<Multiaddr>,
    },
}

#[derive(Debug)]
enum Command {
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
}

#[derive(Debug)]
pub enum Event {
    Provide { key: Cid },
    FindProviders { key: Cid },
    Ping { peer: PeerId },
}

struct EventLoop {
    swarm: Swarm<Bitswap<MemStore>>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
}

impl EventLoop {
    fn new(
        swarm: Swarm<Bitswap<MemStore>>,
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
                        println!("failed to dial peer: {err:?}");
                    }
                }
                let bitswap = self.swarm.behaviour().clone();
                let _ = match bitswap.client().get_block(&root).await {
                    Ok(_) => sender.send(Ok(())),
                    Err(err) => sender.send(Err(err.into())),
                };
            }
        }
    }
}
